mod create;
mod drop;
mod table;
mod token;

pub use create::*;
pub use drop::*;
pub use token::*;

use std::fmt::Display;

#[derive(Debug, PartialEq)]
pub enum CqlStatement {
    Create(CreateStatement),
    Delete,
    Drop(DropStatement),
    Insert,
    Select,
    Update,
}
