pub use batch::*;
pub use delete::*;
pub use insert::*;
pub use select::*;
pub use update::*;

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/dml.html#select-statement
mod select {
    #[derive(Debug, PartialEq)]
    pub struct SelectStatement {}
}

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/dml.html#insert-statement
mod insert {
    #[derive(Debug, PartialEq)]
    pub struct InsertStatement {}
}

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/dml.html#update-statement
mod update {
    use crate::ast::{StringView, TokenView};

    #[derive(Debug, PartialEq)]
    pub struct UpdateStatement {
        pub table_name: TokenView,
        pub assignments: Vec<UpdateAssignment>,
        pub where_clause: UpdateWhereClause,
        pub if_behavior: Option<UpdateIfBehavior>,
    }

    // todo operator
    #[derive(Debug, PartialEq)]
    pub struct UpdateAssignment {
        pub selection: UpdateAssignmentSelection,
        pub expr_term: UpdateExpressionTerm,
    }

    #[derive(Debug, PartialEq)]
    pub enum UpdateAssignmentSelection {
        Column {
            column_name: TokenView,
        },
        ColumnAccess {
            column_name: TokenView,
            expr_term: UpdateExpressionTerm,
        },
        ColumnField {
            column_name: TokenView,
            field_name: TokenView,
        },
    }

    #[derive(Debug, PartialEq)]
    pub enum UpdateExpressionTerm {
        Number(TokenView),
        String(StringView),
    }

    #[derive(Debug, PartialEq)]
    pub struct UpdateWhereClause {
        pub relations: Vec<UpdateWhereClauseRelation>,
    }

    #[derive(Debug, PartialEq)]
    pub struct UpdateWhereClauseRelation {
        pub column_name: TokenView,
        // todo TokenView does not have TokenName to resolve kind of operator
        // pub operator: TokenView,
        pub expr_term: UpdateExpressionTerm,
    }

    #[derive(Debug, PartialEq)]
    pub enum UpdateIfBehavior {
        Conditional(Vec<UpdateIfCondition>),
        Exists,
    }

    // todo operator
    #[derive(Debug, PartialEq)]
    pub struct UpdateIfCondition {
        pub selection: UpdateAssignmentSelection,
        pub expr_term: UpdateExpressionTerm,
    }
}

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/dml.html#delete_statement
mod delete {
    #[derive(Debug, PartialEq)]
    pub struct DeleteStatement {}
}

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/dml.html#batch_statement
mod batch {
    #[derive(Debug, PartialEq)]
    pub struct BatchStatement {}
}
