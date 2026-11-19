use crate::ast::*;
use crate::lex::TokenName::*;
use crate::lex::testing::tokenize_expect;
use crate::test_cql::*;

mod create_trigger {
    use super::*;

    #[test]
    fn test_create_trigger() {
        tokenize_expect(
            CREATE_TRIGGER_DEFAULT_KEYSPACE,
            vec![
                (CreateKeyword, "create"),
                (TriggerKeyword, "trigger"),
                (Identifier, "big_data_trigger"),
                (OnKeyword, "on"),
                (Identifier, "big_data_table"),
                (UsingKeyword, "using"),
                (StringLiteral(StringStyle::SingleQuote), "'trigger name'"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_create_trigger_if_not_exists() {
        tokenize_expect(
            CREATE_TRIGGER_IF_NOT_EXISTS_DEFAULT_KEYSPACE,
            vec![
                (CreateKeyword, "create"),
                (TriggerKeyword, "trigger"),
                (IfKeyword, "if"),
                (NotKeyword, "not"),
                (ExistsKeyword, "exists"),
                (Identifier, "big_data_trigger"),
                (OnKeyword, "on"),
                (Identifier, "big_data_table"),
                (UsingKeyword, "using"),
                (StringLiteral(StringStyle::SingleQuote), "'trigger name'"),
                (Semicolon, ";"),
            ],
        );
    }
}

mod drop_trigger {
    use super::*;

    #[test]
    fn test_drop_trigger() {
        tokenize_expect(
            DROP_TRIGGER_DEFAULT_KEYSPACE,
            vec![
                (DropKeyword, "drop"),
                (TriggerKeyword, "trigger"),
                (Identifier, "big_data_trigger"),
                (OnKeyword, "on"),
                (Identifier, "big_data_table"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_drop_trigger_if_not_exists() {
        tokenize_expect(
            DROP_TRIGGER_DEFAULT_KEYSPACE_IF_EXISTS,
            vec![
                (DropKeyword, "drop"),
                (TriggerKeyword, "trigger"),
                (IfKeyword, "if"),
                (ExistsKeyword, "exists"),
                (Identifier, "big_data_trigger"),
                (OnKeyword, "on"),
                (Identifier, "big_data_table"),
                (Semicolon, ";"),
            ],
        );
    }
}
