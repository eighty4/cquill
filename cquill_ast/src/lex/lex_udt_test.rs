use crate::lex::TokenName::*;
use crate::lex::testing::tokenize_expect;
use crate::test_cql::*;

mod create_type {
    use super::*;

    #[test]
    fn test_create_type() {
        tokenize_expect(
            CREATE_DEFAULT_KEYSPACE_UDT_WITH_SINGLE_ATTRIBUTE,
            vec![
                (CreateKeyword, "create"),
                (TypeKeyword, "type"),
                (Identifier, "big_data_udt"),
                (LeftParenthesis, "("),
                (Identifier, "int_attribute"),
                (IntKeyword, "int"),
                (RightParenthesis, ")"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_create_type_with_multiple_attributes() {
        tokenize_expect(
            CREATE_DEFAULT_KEYSPACE_UDT_WITH_MULTIPLE_ATTRIBUTES,
            vec![
                (CreateKeyword, "create"),
                (TypeKeyword, "type"),
                (Identifier, "big_data_udt"),
                (LeftParenthesis, "("),
                (Identifier, "int_attr"),
                (IntKeyword, "int"),
                (Comma, ","),
                (Identifier, "text_attr"),
                (TextKeyword, "text"),
                (RightParenthesis, ")"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_create_type_if_not_exists() {
        tokenize_expect(
            CREATE_DEFAULT_KEYSPACE_UDT_IF_NOT_EXISTS,
            vec![
                (CreateKeyword, "create"),
                (TypeKeyword, "type"),
                (IfKeyword, "if"),
                (NotKeyword, "not"),
                (ExistsKeyword, "exists"),
                (Identifier, "big_data_udt"),
                (LeftParenthesis, "("),
                (Identifier, "int_attr"),
                (IntKeyword, "int"),
                (RightParenthesis, ")"),
                (Semicolon, ";"),
            ],
        );
    }
}

mod alter_type {
    use super::*;

    #[test]
    fn test_alter_type_add_field() {
        tokenize_expect(
            ALTER_UDT_ADD_FIELD,
            vec![
                (AlterKeyword, "alter"),
                (TypeKeyword, "type"),
                (Identifier, "big_data_udt"),
                (AddKeyword, "add"),
                (Identifier, "big_data_text_column"),
                (TextKeyword, "text"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_alter_type_if_exists() {
        tokenize_expect(
            ALTER_UDT_IF_EXISTS,
            vec![
                (AlterKeyword, "alter"),
                (TypeKeyword, "type"),
                (IfKeyword, "if"),
                (ExistsKeyword, "exists"),
                (Identifier, "big_data_udt"),
                (AddKeyword, "add"),
                (Identifier, "big_data_text_column"),
                (TextKeyword, "text"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_alter_type_add_field_if_not_exists() {
        tokenize_expect(
            ALTER_UDT_ADD_FIELD_IF_NOT_EXISTS,
            vec![
                (AlterKeyword, "alter"),
                (TypeKeyword, "type"),
                (Identifier, "big_data_udt"),
                (AddKeyword, "add"),
                (IfKeyword, "if"),
                (NotKeyword, "not"),
                (ExistsKeyword, "exists"),
                (Identifier, "big_data_text_column"),
                (TextKeyword, "text"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_alter_type_rename_field() {
        tokenize_expect(
            ALTER_UDT_RENAME_FIELD,
            vec![
                (AlterKeyword, "alter"),
                (TypeKeyword, "type"),
                (Identifier, "big_data_udt"),
                (RenameKeyword, "rename"),
                (Identifier, "big_data_text_column"),
                (ToKeyword, "to"),
                (Identifier, "modest_data_text_column"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_alter_type_rename_multiple_fields() {
        tokenize_expect(
            ALTER_UDT_RENAME_MULTIPLE_FIELDS,
            vec![
                (AlterKeyword, "alter"),
                (TypeKeyword, "type"),
                (Identifier, "big_data_udt"),
                (RenameKeyword, "rename"),
                (Identifier, "big_data_text_column"),
                (ToKeyword, "to"),
                (Identifier, "modest_data_text_column"),
                (AndKeyword, "and"),
                (Identifier, "big_data_int_column"),
                (ToKeyword, "to"),
                (Identifier, "gargantuan_int_column"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_alter_type_rename_field_if_exists() {
        tokenize_expect(
            ALTER_UDT_RENAME_FIELD_IF_EXISTS,
            vec![
                (AlterKeyword, "alter"),
                (TypeKeyword, "type"),
                (Identifier, "big_data_udt"),
                (RenameKeyword, "rename"),
                (IfKeyword, "if"),
                (ExistsKeyword, "exists"),
                (Identifier, "big_data_text_column"),
                (ToKeyword, "to"),
                (Identifier, "modest_data_text_column"),
                (Semicolon, ";"),
            ],
        );
    }
}

mod drop_type {
    use super::*;

    #[test]
    fn test_drop_type() {
        tokenize_expect(
            DROP_UDT_DEFAULT_KEYSPACE,
            vec![
                (DropKeyword, "drop"),
                (TypeKeyword, "type"),
                (Identifier, "big_data_udt"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_drop_type_if_exists() {
        tokenize_expect(
            DROP_UDT_DEFAULT_KEYSPACE_IF_EXISTS,
            vec![
                (DropKeyword, "drop"),
                (TypeKeyword, "type"),
                (IfKeyword, "if"),
                (ExistsKeyword, "exists"),
                (Identifier, "big_data_udt"),
                (Semicolon, ";"),
            ],
        );
    }
}
