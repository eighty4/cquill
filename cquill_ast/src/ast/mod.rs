mod ddl;
mod dml;
mod table;
mod token;

#[cfg(test)]
mod token_test;

pub use ddl::*;
pub use dml::*;
pub use table::*;
pub use token::*;

#[derive(Debug, PartialEq)]
pub enum CqlStatement {
    Create(CreateStatement),
    Delete,
    Drop(DropStatement),
    Insert,
    Select,
    Update(UpdateStatement),
}
