/// Declares alias for TABLE keyword for statements that support `CREATE COLUMN FAMILY ...` where
/// TABLE would be used in a `CREATE TABLE ...` statement.
#[derive(Debug, PartialEq)]
pub enum TableAlias {
    ColumnFamily,
}
