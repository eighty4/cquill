use crate::ast::*;
use crate::lex::TokenName::*;
use crate::lex::testing::tokenize_expect;
use crate::test_cql::*;

mod create_index {
    use super::*;

    #[test]
    fn test_create_index() {
        tokenize_expect(
            CREATE_INDEX,
            vec![
                (CreateKeyword, "create"),
                (IndexKeyword, "index"),
                (Identifier, "big_data_index"),
                (OnKeyword, "on"),
                (Identifier, "big_data_table"),
                (LeftParenthesis, "("),
                (Identifier, "text_column"),
                (RightParenthesis, ")"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_create_index_if_not_exists() {
        tokenize_expect(
            CREATE_INDEX_IF_NOT_EXISTS,
            vec![
                (CreateKeyword, "create"),
                (IndexKeyword, "index"),
                (IfKeyword, "if"),
                (NotKeyword, "not"),
                (ExistsKeyword, "exists"),
                (Identifier, "big_data_index"),
                (OnKeyword, "on"),
                (Identifier, "big_data_table"),
                (LeftParenthesis, "("),
                (Identifier, "text_column"),
                (RightParenthesis, ")"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_create_index_on_keys() {
        tokenize_expect(
            CREATE_INDEX_ON_KEYS,
            vec![
                (CreateKeyword, "create"),
                (IndexKeyword, "index"),
                (Identifier, "big_data_index"),
                (OnKeyword, "on"),
                (Identifier, "big_data_table"),
                (LeftParenthesis, "("),
                (KeysKeyword, "keys"),
                (LeftParenthesis, "("),
                (Identifier, "map_column"),
                (RightParenthesis, ")"),
                (RightParenthesis, ")"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_create_index_on_values() {
        tokenize_expect(
            CREATE_INDEX_ON_VALUES,
            vec![
                (CreateKeyword, "create"),
                (IndexKeyword, "index"),
                (Identifier, "big_data_index"),
                (OnKeyword, "on"),
                (Identifier, "big_data_table"),
                (LeftParenthesis, "("),
                (ValuesKeyword, "values"),
                (LeftParenthesis, "("),
                (Identifier, "map_column"),
                (RightParenthesis, ")"),
                (RightParenthesis, ")"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_create_index_on_entries() {
        tokenize_expect(
            CREATE_INDEX_ON_ENTRIES,
            vec![
                (CreateKeyword, "create"),
                (IndexKeyword, "index"),
                (Identifier, "big_data_index"),
                (OnKeyword, "on"),
                (Identifier, "big_data_table"),
                (LeftParenthesis, "("),
                (EntriesKeyword, "entries"),
                (LeftParenthesis, "("),
                (Identifier, "map_column"),
                (RightParenthesis, ")"),
                (RightParenthesis, ")"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_create_index_on_full() {
        tokenize_expect(
            CREATE_INDEX_ON_FULL,
            vec![
                (CreateKeyword, "create"),
                (IndexKeyword, "index"),
                (Identifier, "big_data_index"),
                (OnKeyword, "on"),
                (Identifier, "big_data_table"),
                (LeftParenthesis, "("),
                (FullKeyword, "full"),
                (LeftParenthesis, "("),
                (Identifier, "map_column"),
                (RightParenthesis, ")"),
                (RightParenthesis, ")"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_create_custom_index() {
        tokenize_expect(
            CREATE_CUSTOM_INDEX,
            vec![
                (CreateKeyword, "create"),
                (CustomKeyword, "custom"),
                (IndexKeyword, "index"),
                (Identifier, "big_data_index"),
                (OnKeyword, "on"),
                (Identifier, "big_data_table"),
                (LeftParenthesis, "("),
                (Identifier, "text_column"),
                (RightParenthesis, ")"),
                (UsingKeyword, "using"),
                (StringLiteral(StringStyle::SingleQuote), "'fqpn.IndexClass'"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_create_custom_index_with_options() {
        tokenize_expect(
            CREATE_CUSTOM_INDEX_WITH_OPTIONS,
            vec![
                (CreateKeyword, "create"),
                (CustomKeyword, "custom"),
                (IndexKeyword, "index"),
                (Identifier, "big_data_index"),
                (OnKeyword, "on"),
                (Identifier, "big_data_table"),
                (LeftParenthesis, "("),
                (Identifier, "text_column"),
                (RightParenthesis, ")"),
                (UsingKeyword, "using"),
                (StringLiteral(StringStyle::SingleQuote), "'fqpn.IndexClass'"),
                (WithKeyword, "with"),
                (OptionsKeyword, "options"),
                (Equal, "="),
                (LeftCurvedBracket, "{"),
                (StringLiteral(StringStyle::SingleQuote), "'option'"),
                (Colon, ":"),
                (StringLiteral(StringStyle::SingleQuote), "'value'"),
                (RightCurvedBracket, "}"),
                (Semicolon, ";"),
            ],
        );
    }
}

mod drop_index {
    use super::*;

    #[test]
    fn test_drop_index() {
        tokenize_expect(
            DROP_INDEX_DEFAULT_KEYSPACE,
            vec![
                (DropKeyword, "drop"),
                (IndexKeyword, "index"),
                (Identifier, "big_data_index"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_drop_index_if_exists() {
        tokenize_expect(
            DROP_INDEX_DEFAULT_KEYSPACE_IF_EXISTS,
            vec![
                (DropKeyword, "drop"),
                (IndexKeyword, "index"),
                (IfKeyword, "if"),
                (ExistsKeyword, "exists"),
                (Identifier, "big_data_index"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_drop_index_explicit_keyspace() {
        tokenize_expect(
            DROP_INDEX_EXPLICIT_KEYSPACE,
            vec![
                (DropKeyword, "drop"),
                (IndexKeyword, "index"),
                (Identifier, "big_data_keyspace"),
                (Dot, "."),
                (Identifier, "big_data_index"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_drop_index_explicit_keyspace_if_exists() {
        tokenize_expect(
            DROP_INDEX_EXPLICIT_KEYSPACE_IF_EXISTS,
            vec![
                (DropKeyword, "drop"),
                (IndexKeyword, "index"),
                (IfKeyword, "if"),
                (ExistsKeyword, "exists"),
                (Identifier, "big_data_keyspace"),
                (Dot, "."),
                (Identifier, "big_data_index"),
                (Semicolon, ";"),
            ],
        );
    }
}
