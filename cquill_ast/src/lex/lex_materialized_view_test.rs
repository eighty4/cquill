use crate::ast::*;
use crate::lex::TokenName::*;
use crate::lex::testing::tokenize_expect;
use crate::test_cql::*;

mod create_materialized_view {
    use super::*;

    #[test]
    fn test_create_materialized_view() {
        tokenize_expect(
            CREATE_MATERIALIZED_VIEW,
            vec![
                (CreateKeyword, "create"),
                (MaterializedKeyword, "materialized"),
                (ViewKeyword, "view"),
                (Identifier, "big_data_view"),
                (AsKeyword, "as"),
                (SelectKeyword, "select"),
                (Star, "*"),
                (FromKeyword, "from"),
                (Identifier, "big_data_table"),
                (WhereKeyword, "where"),
                (Identifier, "int_column"),
                (GreaterThan, ">"),
                (NumberLiteral, "4"),
                (PrimaryKeyword, "primary"),
                (KeyKeyword, "key"),
                (LeftParenthesis, "("),
                (Identifier, "int_column"),
                (RightParenthesis, ")"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_create_materialized_view_if_not_exists() {
        tokenize_expect(
            CREATE_MATERIALIZED_VIEW_IF_NOT_EXISTS,
            vec![
                (CreateKeyword, "create"),
                (MaterializedKeyword, "materialized"),
                (ViewKeyword, "view"),
                (IfKeyword, "if"),
                (NotKeyword, "not"),
                (ExistsKeyword, "exists"),
                (Identifier, "big_data_view"),
                (AsKeyword, "as"),
                (SelectKeyword, "select"),
                (Star, "*"),
                (FromKeyword, "from"),
                (Identifier, "big_data_table"),
                (WhereKeyword, "where"),
                (Identifier, "int_column"),
                (GreaterThan, ">"),
                (NumberLiteral, "4"),
                (PrimaryKeyword, "primary"),
                (KeyKeyword, "key"),
                (LeftParenthesis, "("),
                (Identifier, "int_column"),
                (RightParenthesis, ")"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_create_materialized_view_with_options() {
        tokenize_expect(
            CREATE_MATERIALIZED_VIEW_WITH_OPTIONS,
            vec![
                (CreateKeyword, "create"),
                (MaterializedKeyword, "materialized"),
                (ViewKeyword, "view"),
                (Identifier, "big_data_view"),
                (AsKeyword, "as"),
                (SelectKeyword, "select"),
                (Star, "*"),
                (FromKeyword, "from"),
                (Identifier, "big_data_table"),
                (WhereKeyword, "where"),
                (Identifier, "int_column"),
                (GreaterThan, ">"),
                (NumberLiteral, "4"),
                (PrimaryKeyword, "primary"),
                (KeyKeyword, "key"),
                (LeftParenthesis, "("),
                (Identifier, "int_column"),
                (RightParenthesis, ")"),
                (WithKeyword, "with"),
                (Identifier, "comment"),
                (Equal, "="),
                (StringLiteral(StringStyle::SingleQuote), "'comment ca va'"),
                (Semicolon, ";"),
            ],
        );
    }
}

mod alter_materialized_view {
    use super::*;

    #[test]
    fn test_alter_materialized_view() {
        tokenize_expect(
            ALTER_MATERIALIZED_VIEW,
            vec![
                (AlterKeyword, "alter"),
                (MaterializedKeyword, "materialized"),
                (ViewKeyword, "view"),
                (Identifier, "big_data_view"),
                (WithKeyword, "with"),
                (Identifier, "comment"),
                (Equal, "="),
                (StringLiteral(StringStyle::SingleQuote), "'quoi de neuf'"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_alter_materialized_view_if_exists() {
        tokenize_expect(
            ALTER_MATERIALIZED_VIEW_IF_EXISTS,
            vec![
                (AlterKeyword, "alter"),
                (MaterializedKeyword, "materialized"),
                (ViewKeyword, "view"),
                (IfKeyword, "if"),
                (ExistsKeyword, "exists"),
                (Identifier, "big_data_view"),
                (WithKeyword, "with"),
                (Identifier, "comment"),
                (Equal, "="),
                (StringLiteral(StringStyle::SingleQuote), "'quoi de neuf'"),
                (Semicolon, ";"),
            ],
        );
    }
}

mod drop_materialized_view {
    use super::*;

    #[test]
    fn test_drop_materialized_view() {
        tokenize_expect(
            DROP_MATERIALIZED_VIEW_DEFAULT_KEYSPACE,
            vec![
                (DropKeyword, "drop"),
                (MaterializedKeyword, "materialized"),
                (ViewKeyword, "view"),
                (Identifier, "big_data_view"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_drop_materialized_view_if_exists() {
        tokenize_expect(
            DROP_MATERIALIZED_VIEW_DEFAULT_KEYSPACE_IF_EXISTS,
            vec![
                (DropKeyword, "drop"),
                (MaterializedKeyword, "materialized"),
                (ViewKeyword, "view"),
                (IfKeyword, "if"),
                (ExistsKeyword, "exists"),
                (Identifier, "big_data_view"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_drop_materialized_view_explicit_keyspace() {
        tokenize_expect(
            DROP_MATERIALIZED_VIEW_EXPLICIT_KEYSPACE,
            vec![
                (DropKeyword, "drop"),
                (MaterializedKeyword, "materialized"),
                (ViewKeyword, "view"),
                (Identifier, "big_data_keyspace"),
                (Dot, "."),
                (Identifier, "big_data_view"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_drop_materialized_view_if_exists_explicit_keyspace() {
        tokenize_expect(
            DROP_MATERIALIZED_VIEW_EXPLICIT_KEYSPACE_IF_EXISTS,
            vec![
                (DropKeyword, "drop"),
                (MaterializedKeyword, "materialized"),
                (ViewKeyword, "view"),
                (IfKeyword, "if"),
                (ExistsKeyword, "exists"),
                (Identifier, "big_data_keyspace"),
                (Dot, "."),
                (Identifier, "big_data_view"),
                (Semicolon, ";"),
            ],
        );
    }
}
