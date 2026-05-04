mod traits;

pub use traits::{RecoveryOperations, RowIterator, SchemaOperations, StorageEngine};

#[cfg(test)]
mod tests {
    use crate::{RecoveryOperations, SchemaOperations, StorageEngine};

    #[test]
    fn storage_traits_are_object_safe() {
        fn assert_engine<T: StorageEngine + ?Sized>() {}
        fn assert_schema<T: SchemaOperations + ?Sized>() {}
        fn assert_recovery<T: RecoveryOperations + ?Sized>() {}

        assert_engine::<dyn StorageEngine>();
        assert_schema::<dyn SchemaOperations>();
        assert_recovery::<dyn RecoveryOperations>();
    }
}
