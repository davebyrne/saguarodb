use std::collections::HashSet;

use catalog::CatalogManager;
use common::{
    BindingId, ColumnDef, ColumnId, ColumnInfo, DataType, DbError, Result, SqlState, TableId,
    TableSchema, Value,
};
use parser::{
    Assignment, Expr, FromItem, FunctionArg, InsertSource, OrderByItem, SelectItem,
    SelectStatement, Statement,
};

use crate::{
    AggregateFunc, BinOp, BoundExpr, BoundFrom, BoundInsertSource, BoundOrderByItem, BoundSelect,
    BoundSelectItem, BoundStatement, JoinType, UnaryOp,
};

#[derive(Clone, Debug)]
struct Binding {
    id: BindingId,
    table_id: TableId,
    table_name: String,
    visible_name: String,
    columns: Vec<ColumnDef>,
    slot_start: usize,
}

#[derive(Default)]
struct BindContext {
    bindings: Vec<Binding>,
    next_binding: BindingId,
    next_slot: usize,
}

pub fn bind(statement: &Statement, catalog: &dyn CatalogManager) -> Result<BoundStatement> {
    match statement {
        Statement::CreateTable {
            name,
            columns,
            primary_key,
        } => {
            let mut seen_primary_key_names = HashSet::new();
            for primary_key_name in primary_key {
                if !seen_primary_key_names.insert(primary_key_name) {
                    return Err(plan_error(
                        SqlState::SyntaxError,
                        format!("duplicate primary key column {primary_key_name}"),
                    ));
                }
            }
            if primary_key.len() != 1 {
                return Err(plan_error(
                    SqlState::DatatypeMismatch,
                    "v1 requires exactly one primary key column",
                ));
            }
            Ok(BoundStatement::CreateTable {
                name: name.clone(),
                columns: columns.clone(),
                primary_key: primary_key.clone(),
            })
        }
        Statement::DropTable { name } => {
            let table = require_table(catalog, name)?;
            Ok(BoundStatement::DropTable { table: table.id })
        }
        Statement::Insert {
            table,
            columns,
            source,
        } => bind_insert(catalog, table, columns, source),
        Statement::Select(select) => bind_select(catalog, select).map(BoundStatement::Select),
        Statement::Update {
            table,
            assignments,
            filter,
        } => bind_update(catalog, table, assignments, filter.as_ref()),
        Statement::Delete { table, filter } => bind_delete(catalog, table, filter.as_ref()),
        Statement::Explain(inner) => Ok(BoundStatement::Explain(Box::new(bind(inner, catalog)?))),
    }
}

fn bind_insert(
    catalog: &dyn CatalogManager,
    table_name: &str,
    column_names: &[String],
    source: &InsertSource,
) -> Result<BoundStatement> {
    let table = require_table(catalog, table_name)?;
    let columns = insert_columns(&table, column_names)?;
    validate_insert_omissions(&table, &columns)?;

    let source = match source {
        InsertSource::Values(rows) => bind_insert_values(&table, &columns, rows)?,
        InsertSource::Query(select) => bind_insert_query(catalog, &table, &columns, select)?,
    };

    Ok(BoundStatement::Insert {
        table: table.id,
        columns,
        source,
    })
}

fn bind_insert_values(
    table: &TableSchema,
    columns: &[ColumnId],
    rows: &[Vec<Expr>],
) -> Result<BoundInsertSource> {
    let mut bound_rows = Vec::with_capacity(rows.len());
    for row in rows {
        if row.len() != columns.len() {
            return Err(plan_error(
                SqlState::DatatypeMismatch,
                "INSERT row has wrong number of values",
            ));
        }

        let mut bound_row = Vec::with_capacity(row.len());
        for (expr, column_id) in row.iter().zip(columns) {
            let column = column_by_id(table, *column_id)?;
            let bound = bind_expr(
                &mut BindContext::default(),
                expr,
                Some(column.data_type.clone()),
            )?;
            reject_aggregate(&bound)?;
            validate_assignable(&bound, column)?;
            bound_row.push(bound);
        }
        bound_rows.push(bound_row);
    }

    let output_schema = columns
        .iter()
        .map(|column_id| {
            let column = column_by_id(table, *column_id)?;
            Ok(column_info_for_column(table, column))
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(BoundInsertSource::Values {
        rows: bound_rows,
        output_schema,
    })
}

fn bind_insert_query(
    catalog: &dyn CatalogManager,
    table: &TableSchema,
    columns: &[ColumnId],
    select: &SelectStatement,
) -> Result<BoundInsertSource> {
    let bound = bind_select(catalog, select)?;
    if bound.columns.len() != columns.len() {
        return Err(plan_error(
            SqlState::DatatypeMismatch,
            "INSERT ... SELECT query produces a different number of columns than the target",
        ));
    }
    for (item, column_id) in bound.columns.iter().zip(columns) {
        let column = column_by_id(table, *column_id)?;
        validate_assignable(&item.expr, column)?;
    }
    Ok(BoundInsertSource::Query(Box::new(bound)))
}

fn bind_update(
    catalog: &dyn CatalogManager,
    table_name: &str,
    assignments: &[Assignment],
    filter: Option<&Expr>,
) -> Result<BoundStatement> {
    let table = require_table(catalog, table_name)?;
    let mut ctx = BindContext::default();
    let from = bind_table_from_schema(&mut ctx, table.clone(), None);
    let source_filter = filter
        .map(|expr| bind_boolean_expr(&mut ctx, expr))
        .transpose()?;
    if let Some(filter) = &source_filter {
        reject_aggregate(filter)?;
    }

    let mut seen = HashSet::new();
    let mut bound_assignments = Vec::with_capacity(assignments.len());
    for assignment in assignments {
        let column = column_by_name(&table, &assignment.column)?;
        if table.primary_key.contains(&column.id) {
            return Err(plan_error(
                SqlState::DatatypeMismatch,
                format!("cannot update primary key column {}", column.name),
            ));
        }
        if !seen.insert(column.id) {
            return Err(plan_error(
                SqlState::DatatypeMismatch,
                format!("duplicate assignment for column {}", column.name),
            ));
        }
        let value = bind_expr(&mut ctx, &assignment.value, Some(column.data_type.clone()))?;
        reject_aggregate(&value)?;
        validate_assignable(&value, column)?;
        bound_assignments.push((column.id, value));
    }

    Ok(BoundStatement::Update {
        table: table.id,
        assignments: bound_assignments,
        source: BoundSelect {
            columns: table_select_items(&table, &ctx.bindings[0]),
            from,
            filter: source_filter,
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
            output_schema: table_output_schema(&table),
        },
    })
}

fn bind_delete(
    catalog: &dyn CatalogManager,
    table_name: &str,
    filter: Option<&Expr>,
) -> Result<BoundStatement> {
    let table = require_table(catalog, table_name)?;
    let mut ctx = BindContext::default();
    let from = bind_table_from_schema(&mut ctx, table.clone(), None);
    let source_filter = filter
        .map(|expr| bind_boolean_expr(&mut ctx, expr))
        .transpose()?;
    if let Some(filter) = &source_filter {
        reject_aggregate(filter)?;
    }

    Ok(BoundStatement::Delete {
        table: table.id,
        source: BoundSelect {
            columns: table_select_items(&table, &ctx.bindings[0]),
            from,
            filter: source_filter,
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
            output_schema: table_output_schema(&table),
        },
    })
}

fn bind_select(catalog: &dyn CatalogManager, select: &SelectStatement) -> Result<BoundSelect> {
    if select.from.is_empty() {
        return Err(plan_error(
            SqlState::UndefinedTable,
            "SELECT requires FROM in v1",
        ));
    }

    let mut ctx = BindContext::default();
    let from = bind_from_items(catalog, &mut ctx, &select.from)?;
    let filter = select
        .filter
        .as_ref()
        .map(|expr| bind_boolean_expr(&mut ctx, expr))
        .transpose()?;
    if let Some(filter) = &filter {
        reject_aggregate(filter)?;
    }

    let group_by = select
        .group_by
        .iter()
        .map(|expr| {
            let bound = bind_expr(&mut ctx, expr, None)?;
            reject_aggregate(&bound)?;
            Ok(bound)
        })
        .collect::<Result<Vec<_>>>()?;

    let mut columns = Vec::new();
    for item in &select.columns {
        bind_select_item(&mut ctx, item, &mut columns)?;
    }

    let having = select
        .having
        .as_ref()
        .map(|expr| bind_boolean_expr(&mut ctx, expr))
        .transpose()?;
    let order_by = bind_order_by(&mut ctx, &select.order_by, &columns)?;

    validate_aggregate_usage(&columns, &group_by, having.as_ref(), &order_by)?;

    let output_schema = columns
        .iter()
        .map(|item| ColumnInfo {
            name: item.alias.clone(),
            data_type: item.expr.data_type(),
            table_id: output_table_id(&ctx, &item.expr),
            column_id: output_column_id(&item.expr),
        })
        .collect();

    Ok(BoundSelect {
        columns,
        from,
        filter,
        group_by,
        having,
        order_by,
        limit: select.limit,
        offset: select.offset,
        output_schema,
    })
}

fn bind_from_items(
    catalog: &dyn CatalogManager,
    ctx: &mut BindContext,
    items: &[FromItem],
) -> Result<BoundFrom> {
    let mut bound = bind_from_item(catalog, ctx, &items[0])?;
    for item in &items[1..] {
        let right = bind_from_item(catalog, ctx, item)?;
        bound = BoundFrom::Join {
            left: Box::new(bound),
            right: Box::new(right),
            join_type: JoinType::Cross,
            condition: None,
        };
    }
    Ok(bound)
}

fn bind_from_item(
    catalog: &dyn CatalogManager,
    ctx: &mut BindContext,
    item: &FromItem,
) -> Result<BoundFrom> {
    match item {
        FromItem::Table { name, alias } => {
            let table = require_table(catalog, name)?;
            Ok(bind_table_from_schema(ctx, table, alias.clone()))
        }
        FromItem::Join {
            left,
            right,
            join_type,
            condition,
        } => {
            let left = bind_from_item(catalog, ctx, left)?;
            let right = bind_from_item(catalog, ctx, right)?;
            let join_type = convert_join_type(join_type.clone());
            let condition = match (join_type, condition) {
                (JoinType::Cross, None) => None,
                (JoinType::Cross, Some(_)) => {
                    return Err(plan_error(
                        SqlState::SyntaxError,
                        "CROSS JOIN cannot have an ON predicate in v1",
                    ));
                }
                (_, Some(expr)) => Some(bind_boolean_expr(ctx, expr)?),
                (_, None) => {
                    return Err(plan_error(
                        SqlState::SyntaxError,
                        "non-CROSS joins require an ON predicate",
                    ));
                }
            };
            if let Some(condition) = &condition {
                reject_aggregate(condition)?;
            }
            Ok(BoundFrom::Join {
                left: Box::new(left),
                right: Box::new(right),
                join_type,
                condition,
            })
        }
    }
}

fn bind_table_from_schema(
    ctx: &mut BindContext,
    table: TableSchema,
    alias: Option<String>,
) -> BoundFrom {
    let binding = ctx.next_binding;
    ctx.next_binding += 1;
    let slot_start = ctx.next_slot;
    ctx.next_slot += table.columns.len();
    ctx.bindings.push(Binding {
        id: binding,
        table_id: table.id,
        table_name: table.name.clone(),
        visible_name: alias.clone().unwrap_or_else(|| table.name.clone()),
        columns: table.columns.clone(),
        slot_start,
    });
    BoundFrom::Table {
        table: table.id,
        binding,
        alias,
        schema: table.columns,
    }
}

fn bind_select_item(
    ctx: &mut BindContext,
    item: &SelectItem,
    output: &mut Vec<BoundSelectItem>,
) -> Result<()> {
    match item {
        SelectItem::Wildcard => {
            for binding in &ctx.bindings {
                for column in &binding.columns {
                    output.push(BoundSelectItem {
                        expr: input_ref(binding, column),
                        alias: column.name.clone(),
                    });
                }
            }
        }
        SelectItem::QualifiedWildcard(qualifier) => {
            let binding = resolve_binding(ctx, qualifier)?;
            for column in &binding.columns {
                output.push(BoundSelectItem {
                    expr: input_ref(binding, column),
                    alias: column.name.clone(),
                });
            }
        }
        SelectItem::Expression { expr, alias } => {
            let bound = bind_expr(ctx, expr, None)?;
            let alias = alias.clone().unwrap_or_else(|| derive_alias(expr));
            output.push(BoundSelectItem { expr: bound, alias });
        }
    }
    Ok(())
}

fn bind_order_by(
    ctx: &mut BindContext,
    order_by: &[OrderByItem],
    columns: &[BoundSelectItem],
) -> Result<Vec<BoundOrderByItem>> {
    order_by
        .iter()
        .map(|item| {
            let expr = match &item.expr {
                // `ORDER BY <n>`: a bare positive integer literal selects the
                // nth output column (1-based), matching PostgreSQL.
                Expr::Literal(Value::Integer(position)) => {
                    let index = order_by_position_index(*position, columns.len())?;
                    columns[index].expr.clone()
                }
                Expr::ColumnRef {
                    table: None,
                    column,
                } => columns
                    .iter()
                    .find(|select_item| select_item.alias == *column)
                    .map(|select_item| select_item.expr.clone())
                    .map(Ok)
                    .unwrap_or_else(|| bind_expr(ctx, &item.expr, None))?,
                _ => bind_expr(ctx, &item.expr, None)?,
            };
            Ok(BoundOrderByItem {
                expr,
                ascending: item.ascending,
                nulls_first: item.nulls_first,
            })
        })
        .collect()
}

/// Resolve a 1-based `ORDER BY` position into a zero-based output-column index.
fn order_by_position_index(position: i64, column_count: usize) -> Result<usize> {
    let in_range = position >= 1 && usize::try_from(position).is_ok_and(|p| p <= column_count);
    if !in_range {
        return Err(plan_error(
            SqlState::SyntaxError,
            format!("ORDER BY position {position} is out of range (1..{column_count})"),
        ));
    }
    Ok(position as usize - 1)
}

fn bind_boolean_expr(ctx: &mut BindContext, expr: &Expr) -> Result<BoundExpr> {
    let bound = bind_expr(ctx, expr, Some(DataType::Boolean))?;
    require_type(&bound, DataType::Boolean)?;
    Ok(bound)
}

fn bind_expr(ctx: &mut BindContext, expr: &Expr, expected: Option<DataType>) -> Result<BoundExpr> {
    match expr {
        Expr::Literal(value) => bind_literal(value, expected),
        Expr::ColumnRef { table, column } => resolve_column(ctx, table.as_deref(), column),
        Expr::BinaryOp { left, op, right } => bind_binary_op(ctx, left, op.clone(), right),
        Expr::UnaryOp { op, expr } => bind_unary_op(ctx, op.clone(), expr),
        Expr::Function {
            name,
            args,
            distinct,
        } => bind_function(ctx, name, args, *distinct),
        Expr::IsNull(expr) => {
            let expr = Box::new(bind_expr(ctx, expr, None)?);
            Ok(BoundExpr::IsNull {
                expr,
                data_type: DataType::Boolean,
                nullable: false,
            })
        }
        Expr::IsNotNull(expr) => {
            let expr = Box::new(bind_expr(ctx, expr, None)?);
            Ok(BoundExpr::IsNotNull {
                expr,
                data_type: DataType::Boolean,
                nullable: false,
            })
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => bind_in_list(ctx, expr, list, *negated),
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => bind_between(ctx, expr, low, high, *negated),
        Expr::Like {
            expr,
            pattern,
            negated,
        } => bind_like(ctx, expr, pattern, *negated),
        Expr::Case {
            operand,
            when_clauses,
            else_clause,
        } => bind_case(
            ctx,
            operand.as_deref(),
            when_clauses,
            else_clause.as_deref(),
        ),
        Expr::Cast { expr, data_type } => {
            let expr = Box::new(bind_expr(ctx, expr, Some(data_type.clone()))?);
            Ok(BoundExpr::Cast {
                nullable: expr.nullable(),
                expr,
                data_type: data_type.clone(),
            })
        }
    }
}

fn bind_literal(value: &Value, expected: Option<DataType>) -> Result<BoundExpr> {
    let (data_type, nullable) = match value {
        Value::Null => (
            expected.ok_or_else(|| {
                plan_error(
                    SqlState::DatatypeMismatch,
                    "NULL literal requires a type context",
                )
            })?,
            true,
        ),
        Value::Boolean(_) => (DataType::Boolean, false),
        Value::Integer(_) => (DataType::Integer, false),
        Value::Text(_) => (DataType::Text, false),
    };
    Ok(BoundExpr::Literal {
        value: value.clone(),
        data_type,
        nullable,
    })
}

fn bind_binary_op(
    ctx: &mut BindContext,
    left: &Expr,
    op: parser::BinOp,
    right: &Expr,
) -> Result<BoundExpr> {
    let op = convert_bin_op(op);
    match op {
        BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div | BinOp::Mod => {
            let left = bind_expr(ctx, left, Some(DataType::Integer))?;
            let right = bind_expr(ctx, right, Some(DataType::Integer))?;
            require_type(&left, DataType::Integer)?;
            require_type(&right, DataType::Integer)?;
            let nullable = left.nullable() || right.nullable();
            Ok(BoundExpr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
                data_type: DataType::Integer,
                nullable,
            })
        }
        BinOp::And | BinOp::Or => {
            let left = bind_expr(ctx, left, Some(DataType::Boolean))?;
            let right = bind_expr(ctx, right, Some(DataType::Boolean))?;
            require_type(&left, DataType::Boolean)?;
            require_type(&right, DataType::Boolean)?;
            let nullable = left.nullable() || right.nullable();
            Ok(BoundExpr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
                data_type: DataType::Boolean,
                nullable,
            })
        }
        BinOp::Concat => {
            let left = bind_expr(ctx, left, Some(DataType::Text))?;
            let right = bind_expr(ctx, right, Some(DataType::Text))?;
            require_type(&left, DataType::Text)?;
            require_type(&right, DataType::Text)?;
            let nullable = left.nullable() || right.nullable();
            Ok(BoundExpr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
                data_type: DataType::Text,
                nullable,
            })
        }
        BinOp::Eq | BinOp::Neq | BinOp::Lt | BinOp::LtEq | BinOp::Gt | BinOp::GtEq => {
            let (left, right) = bind_comparison_operands(ctx, left, right)?;
            let nullable = left.nullable() || right.nullable();
            Ok(BoundExpr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
                data_type: DataType::Boolean,
                nullable,
            })
        }
    }
}

fn bind_comparison_operands(
    ctx: &mut BindContext,
    left: &Expr,
    right: &Expr,
) -> Result<(BoundExpr, BoundExpr)> {
    match (is_null_literal(left), is_null_literal(right)) {
        (true, true) => Err(plan_error(
            SqlState::DatatypeMismatch,
            "NULL comparison requires a non-NULL type context",
        )),
        (true, false) => {
            let right = bind_expr(ctx, right, None)?;
            let left = bind_expr(ctx, left, Some(right.data_type()))?;
            Ok((left, right))
        }
        (false, true) => {
            let left = bind_expr(ctx, left, None)?;
            let right = bind_expr(ctx, right, Some(left.data_type()))?;
            Ok((left, right))
        }
        (false, false) => {
            let left = bind_expr(ctx, left, None)?;
            let right = bind_expr(ctx, right, Some(left.data_type()))?;
            require_type(&right, left.data_type())?;
            Ok((left, right))
        }
    }
}

fn bind_unary_op(ctx: &mut BindContext, op: parser::UnaryOp, expr: &Expr) -> Result<BoundExpr> {
    let op = convert_unary_op(op);
    match op {
        UnaryOp::Neg => {
            let expr = bind_expr(ctx, expr, Some(DataType::Integer))?;
            require_type(&expr, DataType::Integer)?;
            Ok(BoundExpr::UnaryOp {
                nullable: expr.nullable(),
                op,
                expr: Box::new(expr),
                data_type: DataType::Integer,
            })
        }
        UnaryOp::Not => {
            let expr = bind_expr(ctx, expr, Some(DataType::Boolean))?;
            require_type(&expr, DataType::Boolean)?;
            Ok(BoundExpr::UnaryOp {
                nullable: expr.nullable(),
                op,
                expr: Box::new(expr),
                data_type: DataType::Boolean,
            })
        }
    }
}

fn bind_function(
    ctx: &mut BindContext,
    name: &str,
    args: &[FunctionArg],
    distinct: bool,
) -> Result<BoundExpr> {
    let name = name.to_ascii_lowercase();
    if let Some(func) = aggregate_func(&name) {
        return bind_aggregate(ctx, func, args, distinct);
    }
    if distinct {
        return Err(plan_error(
            SqlState::SyntaxError,
            format!("function {name} does not support DISTINCT"),
        ));
    }
    bind_scalar_function(ctx, &name, args)
}

fn bind_aggregate(
    ctx: &mut BindContext,
    func: AggregateFunc,
    args: &[FunctionArg],
    distinct: bool,
) -> Result<BoundExpr> {
    if distinct {
        return Err(plan_error(
            SqlState::SyntaxError,
            "aggregate DISTINCT is not supported in v1",
        ));
    }

    let arg = match args {
        [FunctionArg::Wildcard] if func == AggregateFunc::Count => None,
        [FunctionArg::Wildcard] => {
            return Err(plan_error(
                SqlState::SyntaxError,
                "only COUNT supports wildcard aggregate argument",
            ));
        }
        [FunctionArg::Expr(expr)] => {
            let arg = bind_expr(ctx, expr, None)?;
            reject_aggregate(&arg)?;
            Some(Box::new(arg))
        }
        _ => {
            return Err(plan_error(
                SqlState::SyntaxError,
                "aggregate functions require exactly one argument",
            ));
        }
    };

    let (data_type, nullable) = match func {
        AggregateFunc::Count => (DataType::Integer, false),
        AggregateFunc::Sum | AggregateFunc::Avg => {
            let Some(arg) = &arg else {
                return Err(plan_error(
                    SqlState::SyntaxError,
                    "SUM and AVG require an expression argument",
                ));
            };
            require_type(arg, DataType::Integer)?;
            (DataType::Integer, true)
        }
        AggregateFunc::Min | AggregateFunc::Max => {
            let Some(arg) = &arg else {
                return Err(plan_error(
                    SqlState::SyntaxError,
                    "MIN and MAX require an expression argument",
                ));
            };
            (arg.data_type(), true)
        }
    };

    Ok(BoundExpr::AggregateCall {
        func,
        arg,
        distinct: false,
        data_type,
        nullable,
    })
}

fn bind_scalar_function(
    ctx: &mut BindContext,
    name: &str,
    args: &[FunctionArg],
) -> Result<BoundExpr> {
    let mut bound_args = Vec::with_capacity(args.len());
    for arg in args {
        match arg {
            FunctionArg::Expr(expr) => bound_args.push(bind_expr(ctx, expr, None)?),
            FunctionArg::Wildcard => {
                return Err(plan_error(
                    SqlState::SyntaxError,
                    format!("function {name} does not accept a wildcard argument"),
                ));
            }
        }
    }

    let (data_type, nullable) = scalar_signature(name, &bound_args)?;
    Ok(BoundExpr::Function {
        name: name.to_string(),
        args: bound_args,
        data_type,
        nullable,
    })
}

/// Validates a scalar function's arity and argument types, returning its result
/// type and nullability. All v1 scalar functions are NULL-propagating, so the
/// result is nullable when any argument is.
fn scalar_signature(name: &str, args: &[BoundExpr]) -> Result<(DataType, bool)> {
    let nullable = args.iter().any(BoundExpr::nullable);
    match name {
        "upper" | "lower" | "trim" => {
            expect_arity(name, args, 1)?;
            require_type(&args[0], DataType::Text)?;
            Ok((DataType::Text, nullable))
        }
        "length" => {
            expect_arity(name, args, 1)?;
            require_type(&args[0], DataType::Text)?;
            Ok((DataType::Integer, nullable))
        }
        "abs" => {
            expect_arity(name, args, 1)?;
            require_type(&args[0], DataType::Integer)?;
            Ok((DataType::Integer, nullable))
        }
        "substring" => {
            if args.len() != 2 && args.len() != 3 {
                return Err(plan_error(
                    SqlState::SyntaxError,
                    "substring expects 2 or 3 arguments",
                ));
            }
            require_type(&args[0], DataType::Text)?;
            require_type(&args[1], DataType::Integer)?;
            if let Some(length) = args.get(2) {
                require_type(length, DataType::Integer)?;
            }
            Ok((DataType::Text, nullable))
        }
        _ => Err(plan_error(
            SqlState::SyntaxError,
            format!("function {name} is not supported in v1"),
        )),
    }
}

fn expect_arity(name: &str, args: &[BoundExpr], arity: usize) -> Result<()> {
    if args.len() != arity {
        return Err(plan_error(
            SqlState::SyntaxError,
            format!("function {name} expects {arity} argument(s)"),
        ));
    }
    Ok(())
}

fn bind_in_list(
    ctx: &mut BindContext,
    expr: &Expr,
    list: &[Expr],
    negated: bool,
) -> Result<BoundExpr> {
    if matches!(expr, Expr::Literal(Value::Null)) {
        return bind_null_in_list(ctx, expr, list, negated);
    }
    let expr = bind_expr(ctx, expr, None)?;
    let mut nullable = expr.nullable();
    let mut bound_list = Vec::with_capacity(list.len());
    for item in list {
        let item = bind_expr(ctx, item, Some(expr.data_type()))?;
        require_type(&item, expr.data_type())?;
        nullable |= item.nullable();
        bound_list.push(item);
    }
    Ok(BoundExpr::InList {
        expr: Box::new(expr),
        list: bound_list,
        negated,
        data_type: DataType::Boolean,
        nullable,
    })
}

fn bind_null_in_list(
    ctx: &mut BindContext,
    expr: &Expr,
    list: &[Expr],
    negated: bool,
) -> Result<BoundExpr> {
    let mut inferred_type = None;
    let mut nullable = true;
    let mut bound_list = vec![None; list.len()];

    for (index, item) in list.iter().enumerate() {
        if matches!(item, Expr::Literal(Value::Null)) && inferred_type.is_none() {
            continue;
        }
        let item = bind_expr(ctx, item, inferred_type.clone())?;
        if let Some(data_type) = &inferred_type {
            require_type(&item, data_type.clone())?;
        } else {
            inferred_type = Some(item.data_type());
        }
        nullable |= item.nullable();
        bound_list[index] = Some(item);
    }

    let data_type = inferred_type.ok_or_else(|| {
        plan_error(
            SqlState::DatatypeMismatch,
            "NULL literal requires a type context",
        )
    })?;
    let expr = bind_expr(ctx, expr, Some(data_type.clone()))?;
    let bound_list = bound_list
        .into_iter()
        .enumerate()
        .map(|(index, item)| {
            item.map(Ok)
                .unwrap_or_else(|| bind_expr(ctx, &list[index], Some(data_type.clone())))
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(BoundExpr::InList {
        expr: Box::new(expr),
        list: bound_list,
        negated,
        data_type: DataType::Boolean,
        nullable,
    })
}

fn bind_between(
    ctx: &mut BindContext,
    expr: &Expr,
    low: &Expr,
    high: &Expr,
    negated: bool,
) -> Result<BoundExpr> {
    let expr = bind_expr(ctx, expr, None)?;
    let low = bind_expr(ctx, low, Some(expr.data_type()))?;
    let high = bind_expr(ctx, high, Some(expr.data_type()))?;
    require_type(&low, expr.data_type())?;
    require_type(&high, expr.data_type())?;
    let nullable = expr.nullable() || low.nullable() || high.nullable();
    Ok(BoundExpr::Between {
        expr: Box::new(expr),
        low: Box::new(low),
        high: Box::new(high),
        negated,
        data_type: DataType::Boolean,
        nullable,
    })
}

fn bind_like(
    ctx: &mut BindContext,
    expr: &Expr,
    pattern: &Expr,
    negated: bool,
) -> Result<BoundExpr> {
    let expr = bind_expr(ctx, expr, Some(DataType::Text))?;
    let pattern = bind_expr(ctx, pattern, Some(DataType::Text))?;
    require_type(&expr, DataType::Text)?;
    require_type(&pattern, DataType::Text)?;
    let nullable = expr.nullable() || pattern.nullable();
    Ok(BoundExpr::Like {
        expr: Box::new(expr),
        pattern: Box::new(pattern),
        negated,
        data_type: DataType::Boolean,
        nullable,
    })
}

fn bind_case(
    ctx: &mut BindContext,
    operand: Option<&Expr>,
    when_clauses: &[(Expr, Expr)],
    else_clause: Option<&Expr>,
) -> Result<BoundExpr> {
    let operand = operand
        .map(|expr| bind_expr(ctx, expr, None).map(Box::new))
        .transpose()?;

    let inferred_result_type = infer_case_result_type(ctx, when_clauses, else_clause)?;
    let mut result_type = Some(inferred_result_type);
    let mut nullable = else_clause.is_none();
    let mut bound_when = Vec::with_capacity(when_clauses.len());

    for (when, then) in when_clauses {
        let when = if let Some(operand) = &operand {
            let when = bind_expr(ctx, when, Some(operand.data_type()))?;
            require_type(&when, operand.data_type())?;
            when
        } else {
            bind_boolean_expr(ctx, when)?
        };
        let then = bind_expr(ctx, then, result_type.clone())?;
        update_case_type(&then, &mut result_type, &mut nullable)?;
        bound_when.push((when, then));
    }

    let else_clause = else_clause
        .map(|expr| {
            let bound = bind_expr(ctx, expr, result_type.clone())?;
            update_case_type(&bound, &mut result_type, &mut nullable)?;
            Ok(Box::new(bound))
        })
        .transpose()?;

    let data_type = result_type.ok_or_else(|| {
        plan_error(
            SqlState::DatatypeMismatch,
            "CASE result expressions cannot all be NULL",
        )
    })?;

    Ok(BoundExpr::Case {
        operand,
        when_clauses: bound_when,
        else_clause,
        data_type,
        nullable,
    })
}

fn infer_case_result_type(
    ctx: &mut BindContext,
    when_clauses: &[(Expr, Expr)],
    else_clause: Option<&Expr>,
) -> Result<DataType> {
    let mut result_type = None;

    for (_, then) in when_clauses {
        update_inferred_case_type(ctx, then, &mut result_type)?;
    }
    if let Some(else_clause) = else_clause {
        update_inferred_case_type(ctx, else_clause, &mut result_type)?;
    }

    result_type.ok_or_else(|| {
        plan_error(
            SqlState::DatatypeMismatch,
            "CASE result expressions cannot all be NULL",
        )
    })
}

fn update_inferred_case_type(
    ctx: &mut BindContext,
    expr: &Expr,
    result_type: &mut Option<DataType>,
) -> Result<()> {
    if is_null_literal(expr) {
        return Ok(());
    }
    let bound = bind_expr(ctx, expr, result_type.clone())?;
    match result_type {
        Some(data_type) if *data_type != bound.data_type() => Err(plan_error(
            SqlState::DatatypeMismatch,
            "CASE result expressions must have the same type",
        )),
        Some(_) => Ok(()),
        None => {
            *result_type = Some(bound.data_type());
            Ok(())
        }
    }
}

fn update_case_type(
    expr: &BoundExpr,
    result_type: &mut Option<DataType>,
    nullable: &mut bool,
) -> Result<()> {
    if expr.is_null_literal() {
        *nullable = true;
        return Ok(());
    }
    if expr.nullable() {
        *nullable = true;
    }
    match result_type {
        Some(data_type) if *data_type != expr.data_type() => Err(plan_error(
            SqlState::DatatypeMismatch,
            "CASE result expressions must have the same type",
        )),
        Some(_) => Ok(()),
        None => {
            *result_type = Some(expr.data_type());
            Ok(())
        }
    }
}

fn validate_aggregate_usage(
    columns: &[BoundSelectItem],
    group_by: &[BoundExpr],
    having: Option<&BoundExpr>,
    order_by: &[BoundOrderByItem],
) -> Result<()> {
    let aggregate_context = !group_by.is_empty()
        || columns.iter().any(|item| contains_aggregate(&item.expr))
        || having.is_some()
        || order_by.iter().any(|item| contains_aggregate(&item.expr));

    if !aggregate_context {
        return Ok(());
    }

    for item in columns {
        validate_grouped_expr(&item.expr, group_by)?;
    }
    if let Some(having) = having {
        validate_grouped_expr(having, group_by)?;
    }
    for item in order_by {
        validate_grouped_expr(&item.expr, group_by)?;
    }
    Ok(())
}

fn validate_grouped_expr(expr: &BoundExpr, group_by: &[BoundExpr]) -> Result<()> {
    if matches!(expr, BoundExpr::AggregateCall { .. }) {
        return Ok(());
    }
    if !contains_aggregate(expr) {
        if !references_input(expr) || group_by.iter().any(|group| group == expr) {
            return Ok(());
        }
        return Err(plan_error(
            SqlState::DatatypeMismatch,
            "non-aggregate expression must appear exactly in GROUP BY",
        ));
    }

    match expr {
        BoundExpr::BinaryOp { left, right, .. } => {
            validate_grouped_expr(left, group_by)?;
            validate_grouped_expr(right, group_by)
        }
        BoundExpr::UnaryOp { expr, .. }
        | BoundExpr::IsNull { expr, .. }
        | BoundExpr::IsNotNull { expr, .. }
        | BoundExpr::Cast { expr, .. } => validate_grouped_expr(expr, group_by),
        BoundExpr::Function { args, .. } => {
            for arg in args {
                validate_grouped_expr(arg, group_by)?;
            }
            Ok(())
        }
        BoundExpr::InList { expr, list, .. } => {
            validate_grouped_expr(expr, group_by)?;
            for item in list {
                validate_grouped_expr(item, group_by)?;
            }
            Ok(())
        }
        BoundExpr::Between {
            expr, low, high, ..
        } => {
            validate_grouped_expr(expr, group_by)?;
            validate_grouped_expr(low, group_by)?;
            validate_grouped_expr(high, group_by)
        }
        BoundExpr::Like { expr, pattern, .. } => {
            validate_grouped_expr(expr, group_by)?;
            validate_grouped_expr(pattern, group_by)
        }
        BoundExpr::Case {
            operand,
            when_clauses,
            else_clause,
            ..
        } => {
            if let Some(operand) = operand {
                validate_grouped_expr(operand, group_by)?;
            }
            for (when, then) in when_clauses {
                validate_grouped_expr(when, group_by)?;
                validate_grouped_expr(then, group_by)?;
            }
            if let Some(else_clause) = else_clause {
                validate_grouped_expr(else_clause, group_by)?;
            }
            Ok(())
        }
        BoundExpr::Literal { .. } | BoundExpr::InputRef { .. } | BoundExpr::LocalRef { .. } => {
            validate_grouped_expr(expr, group_by)
        }
        BoundExpr::AggregateCall { .. } => Ok(()),
    }
}

fn contains_aggregate(expr: &BoundExpr) -> bool {
    match expr {
        BoundExpr::AggregateCall { .. } => true,
        BoundExpr::BinaryOp { left, right, .. } => {
            contains_aggregate(left) || contains_aggregate(right)
        }
        BoundExpr::UnaryOp { expr, .. }
        | BoundExpr::IsNull { expr, .. }
        | BoundExpr::IsNotNull { expr, .. }
        | BoundExpr::Cast { expr, .. } => contains_aggregate(expr),
        BoundExpr::Function { args, .. } => args.iter().any(contains_aggregate),
        BoundExpr::InList { expr, list, .. } => {
            contains_aggregate(expr) || list.iter().any(contains_aggregate)
        }
        BoundExpr::Between {
            expr, low, high, ..
        } => contains_aggregate(expr) || contains_aggregate(low) || contains_aggregate(high),
        BoundExpr::Like { expr, pattern, .. } => {
            contains_aggregate(expr) || contains_aggregate(pattern)
        }
        BoundExpr::Case {
            operand,
            when_clauses,
            else_clause,
            ..
        } => {
            operand.as_deref().is_some_and(contains_aggregate)
                || when_clauses
                    .iter()
                    .any(|(when, then)| contains_aggregate(when) || contains_aggregate(then))
                || else_clause.as_deref().is_some_and(contains_aggregate)
        }
        BoundExpr::Literal { .. } | BoundExpr::InputRef { .. } | BoundExpr::LocalRef { .. } => {
            false
        }
    }
}

fn reject_aggregate(expr: &BoundExpr) -> Result<()> {
    if contains_aggregate(expr) {
        return Err(plan_error(
            SqlState::DatatypeMismatch,
            "aggregate calls are not allowed here",
        ));
    }
    Ok(())
}

fn references_input(expr: &BoundExpr) -> bool {
    match expr {
        BoundExpr::InputRef { .. } => true,
        BoundExpr::BinaryOp { left, right, .. } => {
            references_input(left) || references_input(right)
        }
        BoundExpr::UnaryOp { expr, .. }
        | BoundExpr::IsNull { expr, .. }
        | BoundExpr::IsNotNull { expr, .. }
        | BoundExpr::Cast { expr, .. } => references_input(expr),
        BoundExpr::Function { args, .. } => args.iter().any(references_input),
        BoundExpr::AggregateCall { arg, .. } => arg.as_deref().is_some_and(references_input),
        BoundExpr::InList { expr, list, .. } => {
            references_input(expr) || list.iter().any(references_input)
        }
        BoundExpr::Between {
            expr, low, high, ..
        } => references_input(expr) || references_input(low) || references_input(high),
        BoundExpr::Like { expr, pattern, .. } => {
            references_input(expr) || references_input(pattern)
        }
        BoundExpr::Case {
            operand,
            when_clauses,
            else_clause,
            ..
        } => {
            operand.as_deref().is_some_and(references_input)
                || when_clauses
                    .iter()
                    .any(|(when, then)| references_input(when) || references_input(then))
                || else_clause.as_deref().is_some_and(references_input)
        }
        BoundExpr::Literal { .. } | BoundExpr::LocalRef { .. } => false,
    }
}

fn resolve_column(ctx: &BindContext, table: Option<&str>, column: &str) -> Result<BoundExpr> {
    let mut matches = Vec::new();
    for binding in &ctx.bindings {
        if let Some(table) = table
            && binding.visible_name != table
            && (binding.visible_name != binding.table_name || binding.table_name != table)
        {
            continue;
        }
        for column_def in &binding.columns {
            if column_def.name == column {
                matches.push((binding, column_def));
            }
        }
    }

    match matches.as_slice() {
        [(binding, column)] => Ok(input_ref(binding, column)),
        [] => Err(plan_error(
            SqlState::UndefinedColumn,
            format!("column {column} does not exist"),
        )),
        _ => Err(plan_error(
            SqlState::UndefinedColumn,
            format!("column {column} is ambiguous"),
        )),
    }
}

fn resolve_binding<'a>(ctx: &'a BindContext, qualifier: &str) -> Result<&'a Binding> {
    let matches = ctx
        .bindings
        .iter()
        .filter(|binding| binding.visible_name == qualifier)
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [binding] => Ok(binding),
        [] => Err(plan_error(
            SqlState::UndefinedTable,
            format!("table binding {qualifier} does not exist"),
        )),
        _ => Err(plan_error(
            SqlState::UndefinedTable,
            format!("table binding {qualifier} is ambiguous"),
        )),
    }
}

fn input_ref(binding: &Binding, column: &ColumnDef) -> BoundExpr {
    BoundExpr::InputRef {
        input: binding.id,
        column: column.id,
        slot: binding.slot_start + usize::from(column.id),
        data_type: column.data_type.clone(),
        nullable: column.nullable,
    }
}

fn require_table(catalog: &dyn CatalogManager, name: &str) -> Result<TableSchema> {
    catalog.get_table_by_name(name)?.ok_or_else(|| {
        plan_error(
            SqlState::UndefinedTable,
            format!("table {name} does not exist"),
        )
    })
}

fn insert_columns(table: &TableSchema, column_names: &[String]) -> Result<Vec<ColumnId>> {
    if column_names.is_empty() {
        return Ok(table.columns.iter().map(|column| column.id).collect());
    }
    let mut seen = HashSet::new();
    column_names
        .iter()
        .map(|name| {
            let column = column_by_name(table, name)?;
            if !seen.insert(column.id) {
                return Err(plan_error(
                    SqlState::DatatypeMismatch,
                    format!("duplicate insert column {}", column.name),
                ));
            }
            Ok(column.id)
        })
        .collect()
}

fn validate_insert_omissions(table: &TableSchema, columns: &[ColumnId]) -> Result<()> {
    let provided: HashSet<_> = columns.iter().copied().collect();
    for column in &table.columns {
        if !column.nullable && !provided.contains(&column.id) {
            return Err(plan_error(
                SqlState::NotNullViolation,
                format!("column {} cannot be omitted", column.name),
            ));
        }
    }
    Ok(())
}

fn column_by_name<'a>(table: &'a TableSchema, name: &str) -> Result<&'a ColumnDef> {
    table
        .columns
        .iter()
        .find(|column| column.name == name)
        .ok_or_else(|| {
            plan_error(
                SqlState::UndefinedColumn,
                format!("column {name} does not exist"),
            )
        })
}

fn column_by_id(table: &TableSchema, id: ColumnId) -> Result<&ColumnDef> {
    table
        .columns
        .iter()
        .find(|column| column.id == id)
        .ok_or_else(|| {
            DbError::internal(format!(
                "catalog table {} is missing column id {id}",
                table.name
            ))
        })
}

fn validate_assignable(expr: &BoundExpr, column: &ColumnDef) -> Result<()> {
    require_type(expr, column.data_type.clone())?;
    if !column.nullable && expr.nullable() {
        return Err(plan_error(
            SqlState::NotNullViolation,
            format!("column {} cannot be NULL", column.name),
        ));
    }
    Ok(())
}

fn require_type(expr: &BoundExpr, expected: DataType) -> Result<()> {
    if expr.data_type() != expected {
        return Err(plan_error(
            SqlState::DatatypeMismatch,
            format!(
                "expected expression type {:?}, got {:?}",
                expected,
                expr.data_type()
            ),
        ));
    }
    Ok(())
}

fn table_select_items(table: &TableSchema, binding: &Binding) -> Vec<BoundSelectItem> {
    table
        .columns
        .iter()
        .map(|column| BoundSelectItem {
            expr: input_ref(binding, column),
            alias: column.name.clone(),
        })
        .collect()
}

fn table_output_schema(table: &TableSchema) -> Vec<ColumnInfo> {
    table
        .columns
        .iter()
        .map(|column| column_info_for_column(table, column))
        .collect()
}

fn column_info_for_column(table: &TableSchema, column: &ColumnDef) -> ColumnInfo {
    ColumnInfo {
        name: column.name.clone(),
        data_type: column.data_type.clone(),
        table_id: Some(table.id),
        column_id: Some(column.id),
    }
}

fn output_table_id(ctx: &BindContext, expr: &BoundExpr) -> Option<TableId> {
    match expr {
        BoundExpr::InputRef { input, .. } => ctx
            .bindings
            .iter()
            .find(|binding| binding.id == *input)
            .map(|binding| binding.table_id),
        _ => None,
    }
}

fn output_column_id(expr: &BoundExpr) -> Option<ColumnId> {
    match expr {
        BoundExpr::InputRef { column, .. } => Some(*column),
        _ => None,
    }
}

fn is_null_literal(expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(Value::Null))
}

fn aggregate_func(name: &str) -> Option<AggregateFunc> {
    match name {
        "count" => Some(AggregateFunc::Count),
        "sum" => Some(AggregateFunc::Sum),
        "avg" => Some(AggregateFunc::Avg),
        "min" => Some(AggregateFunc::Min),
        "max" => Some(AggregateFunc::Max),
        _ => None,
    }
}

fn derive_alias(expr: &Expr) -> String {
    match expr {
        Expr::ColumnRef { column, .. } => column.clone(),
        Expr::Function { name, .. } => name.clone(),
        _ => "?column?".to_string(),
    }
}

fn convert_bin_op(op: parser::BinOp) -> BinOp {
    match op {
        parser::BinOp::Add => BinOp::Add,
        parser::BinOp::Sub => BinOp::Sub,
        parser::BinOp::Mul => BinOp::Mul,
        parser::BinOp::Div => BinOp::Div,
        parser::BinOp::Mod => BinOp::Mod,
        parser::BinOp::Eq => BinOp::Eq,
        parser::BinOp::Neq => BinOp::Neq,
        parser::BinOp::Lt => BinOp::Lt,
        parser::BinOp::LtEq => BinOp::LtEq,
        parser::BinOp::Gt => BinOp::Gt,
        parser::BinOp::GtEq => BinOp::GtEq,
        parser::BinOp::And => BinOp::And,
        parser::BinOp::Or => BinOp::Or,
        parser::BinOp::Concat => BinOp::Concat,
    }
}

fn convert_unary_op(op: parser::UnaryOp) -> UnaryOp {
    match op {
        parser::UnaryOp::Neg => UnaryOp::Neg,
        parser::UnaryOp::Not => UnaryOp::Not,
    }
}

fn convert_join_type(join_type: parser::JoinType) -> JoinType {
    match join_type {
        parser::JoinType::Inner => JoinType::Inner,
        parser::JoinType::Left => JoinType::Left,
        parser::JoinType::Right => JoinType::Right,
        parser::JoinType::Full => JoinType::Full,
        parser::JoinType::Cross => JoinType::Cross,
    }
}

fn plan_error(code: SqlState, message: impl Into<String>) -> DbError {
    DbError::plan(code, message)
}
