pub use create_aggregate::*;
pub use create_function::*;
pub use drop_aggregate::*;
pub use drop_function::*;

#[derive(Debug, PartialEq)]
pub enum CreateIfExistsBehavior {
    /// Specifies `if not exists`
    DoNotError,
    /// Does not specify `or replace` or `if not exists`
    Error,
    /// Specifies `or replace`
    Replace,
}

#[derive(Debug, PartialEq)]
pub enum OnNullInput {
    Called,
    ReturnsNull,
}

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/functions.html#create-function-statement
mod create_function {
    use crate::ast::{CqlDataType, CreateIfExistsBehavior, OnNullInput, StringView, TokenView};

    #[derive(Debug, PartialEq)]
    pub struct CreateFunctionStatement {
        pub if_exists_behavior: CreateIfExistsBehavior,
        pub function_name: TokenView,
        pub function_args: Vec<(TokenView, CqlDataType)>,
        pub on_null_input: OnNullInput,
        pub returns: CqlDataType,
        pub language: TokenView,
        pub function_body: StringView,
    }
}

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/functions.html#drop-function-statement
mod drop_function {
    use crate::ast::{CqlDataType, TokenView};

    #[derive(Debug, PartialEq)]
    pub struct DropFunctionStatement {
        pub function_name: TokenView,
        pub if_exists: bool,
        pub keyspace_name: Option<TokenView>,
        pub signature: Option<Vec<CqlDataType>>,
    }
}

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/functions.html#create-aggregate-statement
mod create_aggregate {
    use crate::ast::{CqlDataType, CreateIfExistsBehavior, TokenView};

    // todo represent init_condition with an AST node
    #[derive(Debug, PartialEq)]
    pub struct CreateAggregateStatement {
        pub if_exists_behavior: CreateIfExistsBehavior,
        pub function_name: TokenView,
        pub function_arg: CqlDataType,
        pub state_function: TokenView,
        pub state_type: CqlDataType,
        pub final_function: Option<TokenView>,
        pub init_condition: bool,
    }
}

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/functions.html#drop-aggregate-statement
mod drop_aggregate {
    use crate::ast::{CqlDataType, TokenView};

    #[derive(Debug, PartialEq)]
    pub struct DropAggregateStatement {
        pub aggregate_name: TokenView,
        pub if_exists: bool,
        pub keyspace_name: Option<TokenView>,
        pub signature: Option<Vec<CqlDataType>>,
    }
}
