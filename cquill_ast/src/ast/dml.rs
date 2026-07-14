pub use update::*;

mod update {
    use crate::ast::{BindMarker, StringView, TokenView};

    #[derive(Debug, PartialEq)]
    pub struct UpdateStatement {
        pub table_name: TokenView,
        pub using_params: Option<Vec<UpdateParameter>>,
        pub assignments: Vec<Assignment>,
        pub where_clause: WhereClause,
        pub if_behavior: Option<UpdateIfBehavior>,
    }

    #[derive(Debug, PartialEq)]
    pub enum UpdateParameter {
        Timestamp(TimestampValue),
        TimeToLive(TimeToLiveValue),
    }

    #[derive(Debug, PartialEq)]
    pub enum TimestampValue {
        BindMarker(BindMarker),
        Integer(TokenView),
        String(TokenView),
        Unspecified,
    }

    #[derive(Debug, PartialEq)]
    pub enum TimeToLiveValue {
        BindMarker(BindMarker),
        Integer(TokenView),
        Null,
    }

    // todo operator
    #[derive(Debug, PartialEq)]
    pub struct Assignment {
        pub selection: AssignmentSelection,
        pub expr_term: ExpressionTerm,
    }

    #[derive(Debug, PartialEq)]
    pub enum AssignmentSelection {
        Column {
            column_name: TokenView,
        },
        ColumnAccess {
            column_name: TokenView,
            expr_term: ExpressionTerm,
        },
        ColumnField {
            column_name: TokenView,
            field_name: TokenView,
        },
    }

    #[derive(Debug, PartialEq)]
    pub enum ExpressionTerm {
        Number(TokenView),
        String(StringView),
    }

    #[derive(Debug, PartialEq)]
    pub struct WhereClause {
        pub relations: Vec<WhereClauseRelation>,
    }

    #[derive(Debug, PartialEq)]
    pub struct WhereClauseRelation {
        pub column_name: TokenView,
        // todo TokenView does not have TokenName to resolve kind of operator
        // pub operator: TokenView,
        pub expr_term: ExpressionTerm,
    }

    #[derive(Debug, PartialEq)]
    pub enum UpdateIfBehavior {
        Conditional(Vec<UpdateIfCondition>),
        Exists,
    }

    // todo operator
    #[derive(Debug, PartialEq)]
    pub struct UpdateIfCondition {
        pub selection: AssignmentSelection,
        pub expr_term: ExpressionTerm,
    }
}
