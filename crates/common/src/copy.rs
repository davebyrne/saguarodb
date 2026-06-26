//! Shared `COPY` configuration types. These live in `common` (the leaf crate)
//! because the parser AST, the binder's `BoundStatement`, the executor's format
//! engine, and the server's COPY loop all need them, and `executor` may not
//! depend on `parser`. See `docs/specs/copy.md`.

/// Direction of a `COPY` statement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CopyDirection {
    /// `COPY <table> FROM STDIN` — bulk import.
    From,
    /// `COPY <table> TO STDOUT` — bulk export.
    To,
}

/// `COPY` data format.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CopyFormat {
    Text,
    Csv,
}

/// Fully resolved `COPY` options, with format-specific defaults applied. The
/// parser normalizes both the modern (`WITH (FORMAT csv, ...)`) and legacy
/// (`WITH CSV ...`) option syntaxes into this struct.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CopyOptions {
    pub format: CopyFormat,
    pub delimiter: char,
    pub null_string: String,
    pub header: bool,
    /// CSV only; ignored by the text format engine.
    pub quote: char,
    /// CSV only; defaults to `quote`. Ignored by the text format engine.
    pub escape: char,
}

impl CopyOptions {
    /// The PostgreSQL defaults for a format: text uses TAB / `\N`; CSV uses
    /// comma / empty-string NULL / double-quote. The caller then overrides
    /// individual fields from the explicit `WITH` options.
    pub fn defaults_for(format: CopyFormat) -> Self {
        match format {
            CopyFormat::Text => Self {
                format,
                delimiter: '\t',
                null_string: "\\N".to_string(),
                header: false,
                quote: '"',
                escape: '"',
            },
            CopyFormat::Csv => Self {
                format,
                delimiter: ',',
                null_string: String::new(),
                header: false,
                quote: '"',
                escape: '"',
            },
        }
    }
}
