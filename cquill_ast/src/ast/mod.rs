mod ddl;
mod definitions;
mod dml;
mod table;
mod token;

#[cfg(test)]
mod token_test;

pub use ddl::*;
pub use definitions::*;
pub use dml::*;
pub use table::*;
pub use token::*;

#[derive(Debug, PartialEq)]
pub enum CqlStatement {
    Alter,
    Batch,
    Create(CreateStatement),
    Delete,
    Drop(DropStatement),
    Insert,
    Select,
    Update(UpdateStatement),
    Truncate(TruncateTableStatement),
}
