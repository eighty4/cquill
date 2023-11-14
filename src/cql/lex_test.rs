use super::lex::*;
use crate::cql::test_cql::*;
use TokenName::*;

fn tokenize_expect(cql: &'static str, expected: Vec<(TokenName, &str)>) {
    let result = Tokenizer::new(cql).tokenize().unwrap();
    assert_eq!(result.len(), expected.len());
    for i in 0..expected.len() {
        assert_eq!(result[i].name, expected[i].0);
        assert_eq!(result[i].range.splice(cql), expected[i].1);
    }
}

mod data_types {
    use super::*;

    #[test]
    fn test_digit() {
        tokenize_expect("4.2", vec![(NumberLiteral, "4.2")]);
        tokenize_expect("-1", vec![(Minus, "-"), (NumberLiteral, "1")]);
        tokenize_expect("1234", vec![(NumberLiteral, "1234")]);
        tokenize_expect("86400", vec![(NumberLiteral, "86400")]);
        tokenize_expect(
            "1234 86400",
            vec![(NumberLiteral, "1234"), (NumberLiteral, "86400")],
        );
    }

    #[test]
    fn test_decimal_does_not_misread_keyspace_qualified_table_name() {
        tokenize_expect(
            "my_keyspace.my_table",
            vec![
                (Identifier, "my_keyspace"),
                (Dot, "."),
                (Identifier, "my_table"),
            ],
        );
    }

    #[test]
    fn test_invalid_digits_decimal_and_identifier() {
        tokenize_expect("1234.my_table", vec![]);
    }

    #[test]
    fn test_blob_literal() {
        tokenize_expect("0xaaaa", vec![(BlobLiteral, "0xaaaa")]);
        tokenize_expect("0xo", vec![]);
    }

    #[test]
    fn test_uuid_literal() {
        tokenize_expect(
            "89b7aa7a-8776-460b-8e1a-60cb4bcd523c",
            vec![(UuidLiteral, "89b7aa7a-8776-460b-8e1a-60cb4bcd523c")],
        );
        tokenize_expect(
            "89B7AA7A-8776-460B-8E1A-60CB4BCD523C",
            vec![(UuidLiteral, "89B7AA7A-8776-460B-8E1A-60CB4BCD523C")],
        );
    }
}

mod data_definition {
    use super::*;

    mod create_keyspace {
        use super::*;

        #[test]
        fn test_create_keyspace_if_not_exists_statement() {
            tokenize_expect(
                CREATE_KEYSPACE_IF_NOT_EXISTS,
                vec![
                    (CreateKeyword, "create"),
                    (KeyspaceKeyword, "keyspace"),
                    (IfKeyword, "if"),
                    (NotKeyword, "not"),
                    (ExistsKeyword, "exists"),
                    (Identifier, "big_data_keyspace"),
                    (WithKeyword, "with"),
                    (ReplicationKeyword, "replication"),
                    (Equal, "="),
                    (LeftCurvedBracket, "{"),
                    (StringLiteral, "'class'"),
                    (Colon, ":"),
                    (StringLiteral, "'SimpleStrategy'"),
                    (Comma, ","),
                    (StringLiteral, "'replication_factor'"),
                    (Colon, ":"),
                    (NumberLiteral, "1"),
                    (RightCurvedBracket, "}"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_create_keyspace_with_durable_writes_option_statement() {
            tokenize_expect(
                CREATE_KEYSPACE_WITH_DURABLE_WRITES_OPTION_EXISTS,
                vec![
                    (CreateKeyword, "create"),
                    (KeyspaceKeyword, "keyspace"),
                    (Identifier, "big_data_keyspace"),
                    (WithKeyword, "with"),
                    (ReplicationKeyword, "replication"),
                    (Equal, "="),
                    (LeftCurvedBracket, "{"),
                    (StringLiteral, "'class'"),
                    (Colon, ":"),
                    (StringLiteral, "'SimpleStrategy'"),
                    (Comma, ","),
                    (StringLiteral, "'replication_factor'"),
                    (Colon, ":"),
                    (NumberLiteral, "1"),
                    (RightCurvedBracket, "}"),
                    (AndKeyword, "and"),
                    (Identifier, "durable_writes"),
                    (Equal, "="),
                    (FalseKeyword, "false"),
                    (Semicolon, ";"),
                ],
            );
        }
    }

    mod use_keyspace {
        use super::*;

        #[test]
        fn test_tokenize_use_keyspace_statement() {
            tokenize_expect(
                USE_KEYSPACE,
                vec![
                    (UseKeyword, "use"),
                    (Identifier, "big_data_keyspace"),
                    (Semicolon, ";"),
                ],
            );
        }
    }

    mod alter_keyspace {
        use super::*;

        #[test]
        fn test_alter_keyspace_with_durable_writes() {
            tokenize_expect(
                ALTER_KEYSPACE_WITH_DURABLE_WRITES,
                vec![
                    (AlterKeyword, "alter"),
                    (KeyspaceKeyword, "keyspace"),
                    (Identifier, "big_data_keyspace"),
                    (WithKeyword, "with"),
                    (Identifier, "durable_writes"),
                    (Equal, "="),
                    (FalseKeyword, "false"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_alter_keyspace_with_replication() {
            tokenize_expect(
                ALTER_KEYSPACE_WITH_REPLICATION,
                vec![
                    (AlterKeyword, "alter"),
                    (KeyspaceKeyword, "keyspace"),
                    (Identifier, "big_data_keyspace"),
                    (WithKeyword, "with"),
                    (ReplicationKeyword, "replication"),
                    (Equal, "="),
                    (LeftCurvedBracket, "{"),
                    (StringLiteral, "'class'"),
                    (Colon, ":"),
                    (StringLiteral, "'SimpleStrategy'"),
                    (Comma, ","),
                    (StringLiteral, "'replication_factor'"),
                    (Colon, ":"),
                    (NumberLiteral, "1"),
                    (RightCurvedBracket, "}"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_alter_keyspace_if_exists() {
            tokenize_expect(
                ALTER_KEYSPACE_IF_EXISTS,
                vec![
                    (AlterKeyword, "alter"),
                    (KeyspaceKeyword, "keyspace"),
                    (IfKeyword, "if"),
                    (ExistsKeyword, "exists"),
                    (Identifier, "big_data_keyspace"),
                    (WithKeyword, "with"),
                    (Identifier, "durable_writes"),
                    (Equal, "="),
                    (FalseKeyword, "false"),
                    (Semicolon, ";"),
                ],
            );
        }
    }

    mod drop_keyspace {
        use super::*;

        #[test]
        fn test_drop_keyspace() {
            tokenize_expect(
                DROP_KEYSPACE,
                vec![
                    (DropKeyword, "drop"),
                    (KeyspaceKeyword, "keyspace"),
                    (Identifier, "big_data_keyspace"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_drop_keyspace_if_exists() {
            tokenize_expect(
                DROP_KEYSPACE_IF_EXISTS,
                vec![
                    (DropKeyword, "drop"),
                    (KeyspaceKeyword, "keyspace"),
                    (IfKeyword, "if"),
                    (ExistsKeyword, "exists"),
                    (Identifier, "big_data_keyspace"),
                    (Semicolon, ";"),
                ],
            );
        }
    }

    mod create_table {
        use super::*;

        #[test]
        fn test_create_table_statement() {
            tokenize_expect(
                CREATE_TABLE_WITH_ALL_DATA_TYPES,
                vec![
                    (CreateKeyword, "create"),
                    (TableKeyword, "table"),
                    (Identifier, "big_data_table"),
                    (LeftParenthesis, "("),
                    (Identifier, "ascii_column"),
                    (AsciiKeyword, "ascii"),
                    (Comma, ","),
                    (Identifier, "bigint_column"),
                    (BigIntKeyword, "bigint"),
                    (Comma, ","),
                    (Identifier, "blob_column"),
                    (BlobKeyword, "blob"),
                    (Comma, ","),
                    (Identifier, "boolean_column"),
                    (BooleanKeyword, "boolean"),
                    (Comma, ","),
                    (Identifier, "counter_column"),
                    (CounterKeyword, "counter"),
                    (Comma, ","),
                    (Identifier, "date_column"),
                    (DateKeyword, "date"),
                    (Comma, ","),
                    (Identifier, "decimal_column"),
                    (DecimalKeyword, "decimal"),
                    (Comma, ","),
                    (Identifier, "double_column"),
                    (DoubleKeyword, "double"),
                    (Comma, ","),
                    (Identifier, "duration_column"),
                    (DurationKeyword, "duration"),
                    (Comma, ","),
                    (Identifier, "float_column"),
                    (FloatKeyword, "float"),
                    (Comma, ","),
                    (Identifier, "inet_column"),
                    (InetKeyword, "inet"),
                    (Comma, ","),
                    (Identifier, "int_column"),
                    (IntKeyword, "int"),
                    (Comma, ","),
                    (Identifier, "smallint_column"),
                    (SmallIntKeyword, "smallint"),
                    (Comma, ","),
                    (Identifier, "text_column"),
                    (TextKeyword, "text"),
                    (Comma, ","),
                    (Identifier, "time_column"),
                    (TimeKeyword, "time"),
                    (Comma, ","),
                    (Identifier, "timestamp_column"),
                    (TimestampKeyword, "timestamp"),
                    (Comma, ","),
                    (Identifier, "timeuuid_column"),
                    (TimeUuidKeyword, "timeuuid"),
                    (Comma, ","),
                    (Identifier, "tinyint_column"),
                    (TinyIntKeyword, "tinyint"),
                    (Comma, ","),
                    (Identifier, "uuid_column"),
                    (UuidKeyword, "uuid"),
                    (Comma, ","),
                    (Identifier, "varchar_column"),
                    (VarCharKeyword, "varchar"),
                    (Comma, ","),
                    (Identifier, "varint_column"),
                    (VarIntKeyword, "varint"),
                    (Comma, ","),
                    (PrimaryKeyword, "primary"),
                    (KeyKeyword, "key"),
                    (LeftParenthesis, "("),
                    (Identifier, "text_column"),
                    (RightParenthesis, ")"),
                    (RightParenthesis, ")"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_create_table_if_not_exists() {
            tokenize_expect(
                CREATE_TABLE_IF_NOT_EXISTS,
                vec![
                    (CreateKeyword, "create"),
                    (TableKeyword, "table"),
                    (IfKeyword, "if"),
                    (NotKeyword, "not"),
                    (ExistsKeyword, "exists"),
                    (Identifier, "big_data_table"),
                    (LeftParenthesis, "("),
                    (Identifier, "uuid_column"),
                    (UuidKeyword, "uuid"),
                    (PrimaryKeyword, "primary"),
                    (KeyKeyword, "key"),
                    (RightParenthesis, ")"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_create_table_with_explicit_keyspace() {
            tokenize_expect(
                CREATE_TABLE_WITH_EXPLICIT_KEYSPACE,
                vec![
                    (CreateKeyword, "create"),
                    (TableKeyword, "table"),
                    (Identifier, "big_data_keyspace"),
                    (Dot, "."),
                    (Identifier, "big_data_table"),
                    (LeftParenthesis, "("),
                    (Identifier, "uuid_column"),
                    (UuidKeyword, "uuid"),
                    (PrimaryKeyword, "primary"),
                    (KeyKeyword, "key"),
                    (RightParenthesis, ")"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_create_table_with_composite_primary_key() {
            tokenize_expect(
                CREATE_TABLE_WITH_COMPOSITE_PRIMARY_KEY,
                vec![
                    (CreateKeyword, "create"),
                    (TableKeyword, "table"),
                    (Identifier, "big_data_table"),
                    (LeftParenthesis, "("),
                    (Identifier, "uuid_column"),
                    (UuidKeyword, "uuid"),
                    (Comma, ","),
                    (Identifier, "timeuuid_column"),
                    (TimeUuidKeyword, "timeuuid"),
                    (Comma, ","),
                    (PrimaryKeyword, "primary"),
                    (KeyKeyword, "key"),
                    (LeftParenthesis, "("),
                    (Identifier, "timeuuid_column"),
                    (Comma, ","),
                    (Identifier, "uuid_column"),
                    (RightParenthesis, ")"),
                    (RightParenthesis, ")"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_create_table_with_comment() {
            tokenize_expect(
                CREATE_TABLE_WITH_COMMENT,
                vec![
                    (CreateKeyword, "create"),
                    (TableKeyword, "table"),
                    (Identifier, "big_data_table"),
                    (LeftParenthesis, "("),
                    (Identifier, "uuid_column"),
                    (UuidKeyword, "uuid"),
                    (PrimaryKeyword, "primary"),
                    (KeyKeyword, "key"),
                    (RightParenthesis, ")"),
                    (WithKeyword, "with"),
                    (Identifier, "comment"),
                    (Equal, "="),
                    (StringLiteral, "'big data!'"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_create_table_with_compact_storage() {
            tokenize_expect(
                CREATE_TABLE_WITH_COMPACT_STORAGE,
                vec![
                    (CreateKeyword, "create"),
                    (TableKeyword, "table"),
                    (Identifier, "big_data_table"),
                    (LeftParenthesis, "("),
                    (Identifier, "uuid_column"),
                    (UuidKeyword, "uuid"),
                    (PrimaryKeyword, "primary"),
                    (KeyKeyword, "key"),
                    (RightParenthesis, ")"),
                    (WithKeyword, "with"),
                    (CompactKeyword, "compact"),
                    (StorageKeyword, "storage"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_create_table_with_compaction() {
            tokenize_expect(
                CREATE_TABLE_WITH_COMPACTION,
                vec![
                    (CreateKeyword, "create"),
                    (TableKeyword, "table"),
                    (Identifier, "big_data_table"),
                    (LeftParenthesis, "("),
                    (Identifier, "uuid_column"),
                    (UuidKeyword, "uuid"),
                    (PrimaryKeyword, "primary"),
                    (KeyKeyword, "key"),
                    (RightParenthesis, ")"),
                    (WithKeyword, "with"),
                    (Identifier, "compaction"),
                    (Equal, "="),
                    (LeftCurvedBracket, "{"),
                    (StringLiteral, "'class'"),
                    (Colon, ":"),
                    (StringLiteral, "'LeveledCompactionStrategy'"),
                    (RightCurvedBracket, "}"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_create_table_with_implicit_clustering_order_desc_statement() {
            tokenize_expect(
                CREATE_TABLE_WITH_IMPLICIT_DESC_CLUSTERING_ORDER,
                vec![
                    (CreateKeyword, "create"),
                    (TableKeyword, "table"),
                    (Identifier, "big_data_table"),
                    (LeftParenthesis, "("),
                    (Identifier, "text_column"),
                    (TextKeyword, "text"),
                    (Comma, ","),
                    (Identifier, "uuid_column"),
                    (UuidKeyword, "uuid"),
                    (Comma, ","),
                    (Identifier, "time_column"),
                    (TimeUuidKeyword, "timeuuid"),
                    (Comma, ","),
                    (PrimaryKeyword, "primary"),
                    (KeyKeyword, "key"),
                    (LeftParenthesis, "("),
                    (Identifier, "text_column"),
                    (Comma, ","),
                    (Identifier, "time_column"),
                    (RightParenthesis, ")"),
                    (RightParenthesis, ")"),
                    (WithKeyword, "with"),
                    (ClusteringKeyword, "clustering"),
                    (OrderKeyword, "order"),
                    (ByKeyword, "by"),
                    (LeftParenthesis, "("),
                    (RightParenthesis, ")"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_create_table_with_clustering_order_desc_statement() {
            tokenize_expect(
                CREATE_TABLE_WITH_DESC_CLUSTERING_ORDER,
                vec![
                    (CreateKeyword, "create"),
                    (TableKeyword, "table"),
                    (Identifier, "big_data_table"),
                    (LeftParenthesis, "("),
                    (Identifier, "text_column"),
                    (TextKeyword, "text"),
                    (Comma, ","),
                    (Identifier, "uuid_column"),
                    (UuidKeyword, "uuid"),
                    (Comma, ","),
                    (Identifier, "time_column"),
                    (TimeUuidKeyword, "timeuuid"),
                    (Comma, ","),
                    (PrimaryKeyword, "primary"),
                    (KeyKeyword, "key"),
                    (LeftParenthesis, "("),
                    (Identifier, "text_column"),
                    (Comma, ","),
                    (Identifier, "time_column"),
                    (RightParenthesis, ")"),
                    (RightParenthesis, ")"),
                    (WithKeyword, "with"),
                    (ClusteringKeyword, "clustering"),
                    (OrderKeyword, "order"),
                    (ByKeyword, "by"),
                    (LeftParenthesis, "("),
                    (Identifier, "time_column"),
                    (DescKeyword, "desc"),
                    (RightParenthesis, ")"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_create_table_with_clustering_order_asc_statement() {
            tokenize_expect(
                CREATE_TABLE_WITH_ASC_CLUSTERING_ORDER,
                vec![
                    (CreateKeyword, "create"),
                    (TableKeyword, "table"),
                    (Identifier, "big_data_table"),
                    (LeftParenthesis, "("),
                    (Identifier, "text_column"),
                    (TextKeyword, "text"),
                    (Comma, ","),
                    (Identifier, "uuid_column"),
                    (UuidKeyword, "uuid"),
                    (Comma, ","),
                    (Identifier, "time_column"),
                    (TimeUuidKeyword, "timeuuid"),
                    (Comma, ","),
                    (PrimaryKeyword, "primary"),
                    (KeyKeyword, "key"),
                    (LeftParenthesis, "("),
                    (Identifier, "text_column"),
                    (Comma, ","),
                    (Identifier, "time_column"),
                    (RightParenthesis, ")"),
                    (RightParenthesis, ")"),
                    (WithKeyword, "with"),
                    (ClusteringKeyword, "clustering"),
                    (OrderKeyword, "order"),
                    (ByKeyword, "by"),
                    (LeftParenthesis, "("),
                    (Identifier, "time_column"),
                    (AscKeyword, "asc"),
                    (RightParenthesis, ")"),
                    (Semicolon, ";"),
                ],
            );
        }
    }

    mod alter_table {
        use super::*;

        #[test]
        fn test_alter_table_if_exists() {
            tokenize_expect(
                ALTER_TABLE_IF_EXISTS,
                vec![
                    (AlterKeyword, "alter"),
                    (TableKeyword, "table"),
                    (IfKeyword, "if"),
                    (ExistsKeyword, "exists"),
                    (Identifier, "big_data_table"),
                    (AddKeyword, "add"),
                    (Identifier, "text_column"),
                    (TextKeyword, "text"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_alter_table_add_column() {
            tokenize_expect(
                ALTER_TABLE_ADD_COLUMN,
                vec![
                    (AlterKeyword, "alter"),
                    (TableKeyword, "table"),
                    (Identifier, "big_data_table"),
                    (AddKeyword, "add"),
                    (Identifier, "text_column"),
                    (TextKeyword, "text"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_alter_table_add_multiple_columns() {
            tokenize_expect(
                ALTER_TABLE_ADD_MULTIPLE_COLUMNS,
                vec![
                    (AlterKeyword, "alter"),
                    (TableKeyword, "table"),
                    (Identifier, "big_data_table"),
                    (AddKeyword, "add"),
                    (Identifier, "text_column"),
                    (TextKeyword, "text"),
                    (Comma, ","),
                    (Identifier, "uuid_column"),
                    (UuidKeyword, "uuid"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_alter_table_add_column_if_not_exists() {
            tokenize_expect(
                ALTER_TABLE_ADD_COLUMN_IF_NOT_EXISTS,
                vec![
                    (AlterKeyword, "alter"),
                    (TableKeyword, "table"),
                    (Identifier, "big_data_table"),
                    (AddKeyword, "add"),
                    (IfKeyword, "if"),
                    (NotKeyword, "not"),
                    (ExistsKeyword, "exists"),
                    (Identifier, "text_column"),
                    (TextKeyword, "text"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_alter_table_with_comment() {
            tokenize_expect(
                ALTER_TABLE_WITH_COMMENT,
                vec![
                    (AlterKeyword, "alter"),
                    (TableKeyword, "table"),
                    (Identifier, "big_data_table"),
                    (WithKeyword, "with"),
                    (Identifier, "comment"),
                    (Equal, "="),
                    (StringLiteral, "'big data!'"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_alter_table_drop_column() {
            tokenize_expect(
                ALTER_TABLE_DROP_COLUMN,
                vec![
                    (AlterKeyword, "alter"),
                    (TableKeyword, "table"),
                    (Identifier, "big_data_table"),
                    (DropKeyword, "drop"),
                    (Identifier, "text_column"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_alter_table_drop_multiple_columns() {
            tokenize_expect(
                ALTER_TABLE_DROP_MULTIPLE_COLUMNS,
                vec![
                    (AlterKeyword, "alter"),
                    (TableKeyword, "table"),
                    (Identifier, "big_data_table"),
                    (DropKeyword, "drop"),
                    (Identifier, "text_column"),
                    (Identifier, "uuid_column"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_alter_table_drop_column_if_not_exists() {
            tokenize_expect(
                ALTER_TABLE_DROP_COLUMN_IF_EXISTS,
                vec![
                    (AlterKeyword, "alter"),
                    (TableKeyword, "table"),
                    (Identifier, "big_data_table"),
                    (DropKeyword, "drop"),
                    (IfKeyword, "if"),
                    (ExistsKeyword, "exists"),
                    (Identifier, "text_column"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_alter_table_rename_column() {
            tokenize_expect(
                ALTER_TABLE_RENAME_COLUMN,
                vec![
                    (AlterKeyword, "alter"),
                    (TableKeyword, "table"),
                    (Identifier, "big_data_table"),
                    (RenameKeyword, "rename"),
                    (Identifier, "text_column"),
                    (ToKeyword, "to"),
                    (Identifier, "text_col"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_alter_table_rename_multiple_columns() {
            tokenize_expect(
                ALTER_TABLE_RENAME_MULTIPLE_COLUMNS,
                vec![
                    (AlterKeyword, "alter"),
                    (TableKeyword, "table"),
                    (Identifier, "big_data_table"),
                    (RenameKeyword, "rename"),
                    (Identifier, "text_column"),
                    (ToKeyword, "to"),
                    (Identifier, "text_col"),
                    (AndKeyword, "and"),
                    (Identifier, "uuid_column"),
                    (ToKeyword, "to"),
                    (Identifier, "uuid_col"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_alter_table_rename_column_if_not_exists() {
            tokenize_expect(
                ALTER_TABLE_RENAME_COLUMN_IF_EXISTS,
                vec![
                    (AlterKeyword, "alter"),
                    (TableKeyword, "table"),
                    (Identifier, "big_data_table"),
                    (RenameKeyword, "rename"),
                    (IfKeyword, "if"),
                    (ExistsKeyword, "exists"),
                    (Identifier, "text_column"),
                    (ToKeyword, "to"),
                    (Identifier, "text_col"),
                    (Semicolon, ";"),
                ],
            );
        }
    }

    mod drop_table {
        use super::*;

        #[test]
        fn test_drop_table() {
            tokenize_expect(
                DROP_TABLE,
                vec![
                    (DropKeyword, "drop"),
                    (TableKeyword, "table"),
                    (Identifier, "big_data_table"),
                    (Semicolon, ";"),
                ],
            )
        }

        #[test]
        fn test_drop_table_if_exists() {
            tokenize_expect(
                DROP_TABLE_IF_EXISTS,
                vec![
                    (DropKeyword, "drop"),
                    (TableKeyword, "table"),
                    (IfKeyword, "if"),
                    (ExistsKeyword, "exists"),
                    (Identifier, "big_data_table"),
                    (Semicolon, ";"),
                ],
            )
        }
    }

    mod truncate {
        use super::*;

        #[test]
        fn test_truncate() {
            tokenize_expect(
                TRUNCATE,
                vec![
                    (TruncateKeyword, "truncate"),
                    (Identifier, "big_data_table"),
                    (Semicolon, ";"),
                ],
            )
        }

        #[test]
        fn test_truncate_table() {
            tokenize_expect(
                TRUNCATE_TABLE,
                vec![
                    (TruncateKeyword, "truncate"),
                    (TableKeyword, "table"),
                    (Identifier, "big_data_table"),
                    (Semicolon, ";"),
                ],
            )
        }
    }
}

mod data_manipulation {
    use super::*;

    mod select {
        use super::*;

        #[test]
        fn test_select_explicit_columns() {
            tokenize_expect(
                SELECT_EXPLICIT_COLUMNS,
                vec![
                    (SelectKeyword, "select"),
                    (Identifier, "text_column"),
                    (Comma, ","),
                    (Identifier, "uuid_column"),
                    (FromKeyword, "from"),
                    (Identifier, "big_data_table"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_select_column_as() {
            tokenize_expect(
                SELECT_COLUMN_AS,
                vec![
                    (SelectKeyword, "select"),
                    (Identifier, "text_column"),
                    (AsKeyword, "as"),
                    (Identifier, "text_col"),
                    (FromKeyword, "from"),
                    (Identifier, "big_data_table"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_select_column_cast() {
            tokenize_expect(
                SELECT_COLUMN_CAST,
                vec![
                    (SelectKeyword, "select"),
                    (Identifier, "cast"),
                    (LeftParenthesis, "("),
                    (Identifier, "uuid_column"),
                    (AsKeyword, "as"),
                    (TextKeyword, "text"),
                    (RightParenthesis, ")"),
                    (FromKeyword, "from"),
                    (Identifier, "big_data_table"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_select_count() {
            tokenize_expect(
                SELECT_COUNT,
                vec![
                    (SelectKeyword, "select"),
                    (Identifier, "count"),
                    (LeftParenthesis, "("),
                    (Star, "*"),
                    (RightParenthesis, ")"),
                    (FromKeyword, "from"),
                    (Identifier, "big_data_table"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_select_json() {
            tokenize_expect(
                SELECT_JSON,
                vec![
                    (SelectKeyword, "select"),
                    (JsonKeyword, "json"),
                    (Star, "*"),
                    (FromKeyword, "from"),
                    (Identifier, "big_data_table"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_select_distinct() {
            tokenize_expect(
                SELECT_DISTINCT,
                vec![
                    (SelectKeyword, "select"),
                    (DistinctKeyword, "distinct"),
                    (Star, "*"),
                    (FromKeyword, "from"),
                    (Identifier, "big_data_table"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_select_where_equal() {
            tokenize_expect(
                SELECT_WHERE_EQ,
                vec![
                    (SelectKeyword, "select"),
                    (Star, "*"),
                    (FromKeyword, "from"),
                    (Identifier, "big_data_table"),
                    (WhereKeyword, "where"),
                    (Identifier, "text_column"),
                    (Equal, "="),
                    (StringLiteral, "'big data!'"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_select_where_not_equal() {
            tokenize_expect(
                SELECT_WHERE_NEQ,
                vec![
                    (SelectKeyword, "select"),
                    (Star, "*"),
                    (FromKeyword, "from"),
                    (Identifier, "big_data_table"),
                    (WhereKeyword, "where"),
                    (Identifier, "text_column"),
                    (NotEqual, "!="),
                    (StringLiteral, "'big data!'"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_select_where_less_than() {
            tokenize_expect(
                SELECT_WHERE_LT,
                vec![
                    (SelectKeyword, "select"),
                    (Star, "*"),
                    (FromKeyword, "from"),
                    (Identifier, "big_data_table"),
                    (WhereKeyword, "where"),
                    (Identifier, "int_column"),
                    (LessThan, "<"),
                    (NumberLiteral, "3"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_select_where_less_than_or_equal() {
            tokenize_expect(
                SELECT_WHERE_LTE,
                vec![
                    (SelectKeyword, "select"),
                    (Star, "*"),
                    (FromKeyword, "from"),
                    (Identifier, "big_data_table"),
                    (WhereKeyword, "where"),
                    (Identifier, "int_column"),
                    (LessThanEqual, "<="),
                    (NumberLiteral, "3"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_select_where_greater_than() {
            tokenize_expect(
                SELECT_WHERE_GT,
                vec![
                    (SelectKeyword, "select"),
                    (Star, "*"),
                    (FromKeyword, "from"),
                    (Identifier, "big_data_table"),
                    (WhereKeyword, "where"),
                    (Identifier, "int_column"),
                    (GreaterThan, ">"),
                    (NumberLiteral, "3"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_select_where_greater_than_equal() {
            tokenize_expect(
                SELECT_WHERE_GTE,
                vec![
                    (SelectKeyword, "select"),
                    (Star, "*"),
                    (FromKeyword, "from"),
                    (Identifier, "big_data_table"),
                    (WhereKeyword, "where"),
                    (Identifier, "int_column"),
                    (GreaterThanEqual, ">="),
                    (NumberLiteral, "3"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_select_where_in() {
            tokenize_expect(
                SELECT_WHERE_IN,
                vec![
                    (SelectKeyword, "select"),
                    (Star, "*"),
                    (FromKeyword, "from"),
                    (Identifier, "big_data_table"),
                    (WhereKeyword, "where"),
                    (Identifier, "partition_col"),
                    (Equal, "="),
                    (StringLiteral, "'big data!'"),
                    (AndKeyword, "and"),
                    (Identifier, "clustering_col"),
                    (InKeyword, "in"),
                    (LeftParenthesis, "("),
                    (StringLiteral, "'abc'"),
                    (Comma, ","),
                    (StringLiteral, "'def'"),
                    (RightParenthesis, ")"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_select_where_in_tuple() {
            tokenize_expect(
                SELECT_WHERE_IN_TUPLE,
                vec![
                    (SelectKeyword, "select"),
                    (Star, "*"),
                    (FromKeyword, "from"),
                    (Identifier, "big_data_table"),
                    (WhereKeyword, "where"),
                    (Identifier, "partition_col"),
                    (Equal, "="),
                    (StringLiteral, "'big data!'"),
                    (AndKeyword, "and"),
                    (LeftParenthesis, "("),
                    (Identifier, "clustering_col1"),
                    (Comma, ","),
                    (Identifier, "clustering_col2"),
                    (RightParenthesis, ")"),
                    (InKeyword, "in"),
                    (LeftParenthesis, "("),
                    (LeftParenthesis, "("),
                    (StringLiteral, "'abc'"),
                    (Comma, ","),
                    (NumberLiteral, "123"),
                    (RightParenthesis, ")"),
                    (Comma, ","),
                    (LeftParenthesis, "("),
                    (StringLiteral, "'def'"),
                    (Comma, ","),
                    (NumberLiteral, "456"),
                    (RightParenthesis, ")"),
                    (RightParenthesis, ")"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_select_where_contains() {
            tokenize_expect(
                SELECT_WHERE_CONTAINS,
                vec![
                    (SelectKeyword, "select"),
                    (Star, "*"),
                    (FromKeyword, "from"),
                    (Identifier, "big_data_table"),
                    (WhereKeyword, "where"),
                    (Identifier, "list_column"),
                    (ContainsKeyword, "contains"),
                    (StringLiteral, "'big data!'"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_select_where_contains_key() {
            tokenize_expect(
                SELECT_WHERE_CONTAINS_KEY,
                vec![
                    (SelectKeyword, "select"),
                    (Star, "*"),
                    (FromKeyword, "from"),
                    (Identifier, "big_data_table"),
                    (WhereKeyword, "where"),
                    (Identifier, "map_column"),
                    (ContainsKeyword, "contains"),
                    (KeyKeyword, "key"),
                    (StringLiteral, "'big data!'"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_select_multiple_where_clauses() {
            tokenize_expect(
                SELECT_WHERE_AND_WHERE,
                vec![
                    (SelectKeyword, "select"),
                    (Star, "*"),
                    (FromKeyword, "from"),
                    (Identifier, "big_data_table"),
                    (WhereKeyword, "where"),
                    (Identifier, "partition_col"),
                    (Equal, "="),
                    (StringLiteral, "'big data!'"),
                    (AndKeyword, "and"),
                    (Identifier, "clustering_col"),
                    (Equal, "="),
                    (StringLiteral, "'more data!'"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_select_group_by_column() {
            tokenize_expect(
                SELECT_GROUP_BY_COLUMN,
                vec![
                    (SelectKeyword, "select"),
                    (Star, "*"),
                    (FromKeyword, "from"),
                    (Identifier, "big_data_table"),
                    (GroupKeyword, "group"),
                    (ByKeyword, "by"),
                    (Identifier, "text_column"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_select_group_by_multiple_columns() {
            tokenize_expect(
                SELECT_GROUP_BY_MULTIPLE_COLUMNS,
                vec![
                    (SelectKeyword, "select"),
                    (Star, "*"),
                    (FromKeyword, "from"),
                    (Identifier, "big_data_table"),
                    (GroupKeyword, "group"),
                    (ByKeyword, "by"),
                    (Identifier, "text_column"),
                    (Comma, ","),
                    (Identifier, "uuid_column"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_select_order_by_column() {
            tokenize_expect(
                SELECT_ORDER_BY_COLUMN,
                vec![
                    (SelectKeyword, "select"),
                    (Star, "*"),
                    (FromKeyword, "from"),
                    (Identifier, "big_data_table"),
                    (OrderKeyword, "order"),
                    (ByKeyword, "by"),
                    (Identifier, "text_column"),
                    (AscKeyword, "asc"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_select_order_by_multiple_columns() {
            tokenize_expect(
                SELECT_ORDER_BY_MULTIPLE_COLUMNS,
                vec![
                    (SelectKeyword, "select"),
                    (Star, "*"),
                    (FromKeyword, "from"),
                    (Identifier, "big_data_table"),
                    (OrderKeyword, "order"),
                    (ByKeyword, "by"),
                    (Identifier, "text_column"),
                    (AscKeyword, "asc"),
                    (Comma, ","),
                    (Identifier, "uuid_column"),
                    (DescKeyword, "desc"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_select_per_partition_limit() {
            tokenize_expect(
                SELECT_PER_PARTITION_LIMIT,
                vec![
                    (SelectKeyword, "select"),
                    (Star, "*"),
                    (FromKeyword, "from"),
                    (Identifier, "big_data_table"),
                    (PerKeyword, "per"),
                    (PartitionKeyword, "partition"),
                    (LimitKeyword, "limit"),
                    (NumberLiteral, "1"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_select_limit() {
            tokenize_expect(
                SELECT_LIMIT,
                vec![
                    (SelectKeyword, "select"),
                    (Star, "*"),
                    (FromKeyword, "from"),
                    (Identifier, "big_data_table"),
                    (LimitKeyword, "limit"),
                    (NumberLiteral, "5"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_select_allow_filtering() {
            tokenize_expect(
                SELECT_ALLOW_FILTERING,
                vec![
                    (SelectKeyword, "select"),
                    (Star, "*"),
                    (FromKeyword, "from"),
                    (Identifier, "big_data_table"),
                    (AllowKeyword, "allow"),
                    (FilteringKeyword, "filtering"),
                    (Semicolon, ";"),
                ],
            );
        }
    }

    mod insert {
        use super::*;

        #[test]
        fn test_insert_single_value() {
            tokenize_expect(
                INSERT_SINGLE_VALUE,
                vec![
                    (InsertKeyword, "insert"),
                    (IntoKeyword, "into"),
                    (Identifier, "big_data_table"),
                    (LeftParenthesis, "("),
                    (Identifier, "text_column"),
                    (RightParenthesis, ")"),
                    (ValuesKeyword, "values"),
                    (LeftParenthesis, "("),
                    (StringLiteral, "'big data!'"),
                    (RightParenthesis, ")"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_insert_multiple_values() {
            tokenize_expect(
                INSERT_MULTIPLE_VALUES,
                vec![
                    (InsertKeyword, "insert"),
                    (IntoKeyword, "into"),
                    (Identifier, "big_data_table"),
                    (LeftParenthesis, "("),
                    (Identifier, "uuid_column"),
                    (Comma, ","),
                    (Identifier, "text_column"),
                    (RightParenthesis, ")"),
                    (ValuesKeyword, "values"),
                    (LeftParenthesis, "("),
                    (UuidLiteral, "89b7aa7a-8776-460b-8e1a-60cb4bcd523c"),
                    (Comma, ","),
                    (StringLiteral, "'big data!'"),
                    (RightParenthesis, ")"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_insert_if_not_exists() {
            tokenize_expect(
                INSERT_IF_NOT_EXISTS,
                vec![
                    (InsertKeyword, "insert"),
                    (IntoKeyword, "into"),
                    (Identifier, "big_data_table"),
                    (LeftParenthesis, "("),
                    (Identifier, "text_column"),
                    (RightParenthesis, ")"),
                    (ValuesKeyword, "values"),
                    (LeftParenthesis, "("),
                    (StringLiteral, "'big data!'"),
                    (RightParenthesis, ")"),
                    (IfKeyword, "if"),
                    (NotKeyword, "not"),
                    (ExistsKeyword, "exists"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_insert_using_ttl() {
            tokenize_expect(
                INSERT_USING_TTL,
                vec![
                    (InsertKeyword, "insert"),
                    (IntoKeyword, "into"),
                    (Identifier, "big_data_table"),
                    (LeftParenthesis, "("),
                    (Identifier, "text_column"),
                    (RightParenthesis, ")"),
                    (ValuesKeyword, "values"),
                    (LeftParenthesis, "("),
                    (StringLiteral, "'big data!'"),
                    (RightParenthesis, ")"),
                    (UsingKeyword, "using"),
                    (TtlKeyword, "ttl"),
                    (NumberLiteral, "86400"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_insert_using_timestamp() {
            tokenize_expect(
                INSERT_USING_TIMESTAMP,
                vec![
                    (InsertKeyword, "insert"),
                    (IntoKeyword, "into"),
                    (Identifier, "big_data_table"),
                    (LeftParenthesis, "("),
                    (Identifier, "text_column"),
                    (RightParenthesis, ")"),
                    (ValuesKeyword, "values"),
                    (LeftParenthesis, "("),
                    (StringLiteral, "'big data!'"),
                    (RightParenthesis, ")"),
                    (UsingKeyword, "using"),
                    (TimestampKeyword, "timestamp"),
                    (StringLiteral, "'2023-11-14T04:05+0000'"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_insert_json() {
            tokenize_expect(
                INSERT_JSON,
                vec![
                    (InsertKeyword, "insert"),
                    (IntoKeyword, "into"),
                    (Identifier, "big_data_table"),
                    (JsonKeyword, "json"),
                    (StringLiteral, "'{\"text_column\": \"big data!\"}'"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_insert_json_default_null() {
            tokenize_expect(
                INSERT_JSON_DEFAULT_NULL,
                vec![
                    (InsertKeyword, "insert"),
                    (IntoKeyword, "into"),
                    (Identifier, "big_data_table"),
                    (JsonKeyword, "json"),
                    (StringLiteral, "'{\"text_column\": \"big data!\"}'"),
                    (DefaultKeyword, "default"),
                    (NullKeyword, "null"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_insert_json_default_unset() {
            tokenize_expect(
                INSERT_JSON_DEFAULT_UNSET,
                vec![
                    (InsertKeyword, "insert"),
                    (IntoKeyword, "into"),
                    (Identifier, "big_data_table"),
                    (JsonKeyword, "json"),
                    (StringLiteral, "'{\"text_column\": \"big data!\"}'"),
                    (DefaultKeyword, "default"),
                    (UnsetKeyword, "unset"),
                    (Semicolon, ";"),
                ],
            );
        }
    }

    mod update {
        use super::*;

        #[test]
        fn test_update_single_value() {
            tokenize_expect(
                UPDATE_SINGLE_COLUMN,
                vec![
                    (UpdateKeyword, "update"),
                    (Identifier, "big_data_table"),
                    (SetKeyword, "set"),
                    (Identifier, "int_column"),
                    (Equal, "="),
                    (NumberLiteral, "1"),
                    (WhereKeyword, "where"),
                    (Identifier, "text_column"),
                    (Equal, "="),
                    (StringLiteral, "'big data!'"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_update_multiple_columns() {
            tokenize_expect(
                UPDATE_MULTIPLE_COLUMNS,
                vec![
                    (UpdateKeyword, "update"),
                    (Identifier, "big_data_table"),
                    (SetKeyword, "set"),
                    (Identifier, "int_column"),
                    (Equal, "="),
                    (NumberLiteral, "1"),
                    (Comma, ","),
                    (Identifier, "float_column"),
                    (Equal, "="),
                    (NumberLiteral, "1.1"),
                    (WhereKeyword, "where"),
                    (Identifier, "text_column"),
                    (Equal, "="),
                    (StringLiteral, "'big data!'"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_update_if_exists() {
            tokenize_expect(
                UPDATE_IF_EXISTS,
                vec![
                    (UpdateKeyword, "update"),
                    (Identifier, "big_data_table"),
                    (SetKeyword, "set"),
                    (Identifier, "int_column"),
                    (Equal, "="),
                    (NumberLiteral, "1"),
                    (WhereKeyword, "where"),
                    (Identifier, "text_column"),
                    (Equal, "="),
                    (StringLiteral, "'big data!'"),
                    (IfKeyword, "if"),
                    (ExistsKeyword, "exists"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_update_if_condition() {
            tokenize_expect(
                UPDATE_IF_CONDITION,
                vec![
                    (UpdateKeyword, "update"),
                    (Identifier, "big_data_table"),
                    (SetKeyword, "set"),
                    (Identifier, "int_column"),
                    (Equal, "="),
                    (NumberLiteral, "1"),
                    (WhereKeyword, "where"),
                    (Identifier, "text_column"),
                    (Equal, "="),
                    (StringLiteral, "'big data!'"),
                    (IfKeyword, "if"),
                    (Identifier, "int_column"),
                    (GreaterThan, ">"),
                    (NumberLiteral, "6"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_update_using_ttl() {
            tokenize_expect(
                UPDATE_USING_TTL,
                vec![
                    (UpdateKeyword, "update"),
                    (Identifier, "big_data_table"),
                    (UsingKeyword, "using"),
                    (TtlKeyword, "ttl"),
                    (NumberLiteral, "86400"),
                    (SetKeyword, "set"),
                    (Identifier, "int_column"),
                    (Equal, "="),
                    (NumberLiteral, "1"),
                    (WhereKeyword, "where"),
                    (Identifier, "text_column"),
                    (Equal, "="),
                    (StringLiteral, "'big data!'"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_update_using_timestamp() {
            tokenize_expect(
                UPDATE_USING_TIMESTAMP,
                vec![
                    (UpdateKeyword, "update"),
                    (Identifier, "big_data_table"),
                    (UsingKeyword, "using"),
                    (TimestampKeyword, "timestamp"),
                    (StringLiteral, "'2023-11-14T04:05+0000'"),
                    (SetKeyword, "set"),
                    (Identifier, "int_column"),
                    (Equal, "="),
                    (NumberLiteral, "1"),
                    (WhereKeyword, "where"),
                    (Identifier, "text_column"),
                    (Equal, "="),
                    (StringLiteral, "'big data!'"),
                    (Semicolon, ";"),
                ],
            );
        }
    }

    mod delete {
        use super::*;

        #[test]
        fn test_delete_single_column() {
            tokenize_expect(
                DELETE_SINGLE_COLUMN,
                vec![
                    (DeleteKeyword, "delete"),
                    (Identifier, "uuid_column"),
                    (FromKeyword, "from"),
                    (Identifier, "big_data_table"),
                    (WhereKeyword, "where"),
                    (Identifier, "text_column"),
                    (Equal, "="),
                    (StringLiteral, "'big data!'"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_delete_multiple_columns() {
            tokenize_expect(
                DELETE_MULTIPLE_COLUMNS,
                vec![
                    (DeleteKeyword, "delete"),
                    (Identifier, "uuid_column"),
                    (Comma, ","),
                    (Identifier, "int_column"),
                    (FromKeyword, "from"),
                    (Identifier, "big_data_table"),
                    (WhereKeyword, "where"),
                    (Identifier, "text_column"),
                    (Equal, "="),
                    (StringLiteral, "'big data!'"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_delete_if_exists() {
            tokenize_expect(
                DELETE_IF_EXISTS,
                vec![
                    (DeleteKeyword, "delete"),
                    (Identifier, "uuid_column"),
                    (FromKeyword, "from"),
                    (Identifier, "big_data_table"),
                    (WhereKeyword, "where"),
                    (Identifier, "text_column"),
                    (Equal, "="),
                    (StringLiteral, "'big data!'"),
                    (IfKeyword, "if"),
                    (ExistsKeyword, "exists"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_delete_if_condition() {
            tokenize_expect(
                DELETE_IF_CONDITION,
                vec![
                    (DeleteKeyword, "delete"),
                    (Identifier, "uuid_column"),
                    (FromKeyword, "from"),
                    (Identifier, "big_data_table"),
                    (WhereKeyword, "where"),
                    (Identifier, "text_column"),
                    (Equal, "="),
                    (StringLiteral, "'big data!'"),
                    (IfKeyword, "if"),
                    (Identifier, "uuid_column"),
                    (NotEqual, "!="),
                    (UuidLiteral, "89b7aa7a-8776-460b-8e1a-60cb4bcd523c"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_delete_using_timestamp() {
            tokenize_expect(
                DELETE_USING_TIMESTAMP,
                vec![
                    (DeleteKeyword, "delete"),
                    (Identifier, "uuid_column"),
                    (FromKeyword, "from"),
                    (Identifier, "big_data_table"),
                    (UsingKeyword, "using"),
                    (TimestampKeyword, "timestamp"),
                    (StringLiteral, "'2023-11-14T04:05+0000'"),
                    (WhereKeyword, "where"),
                    (Identifier, "text_column"),
                    (Equal, "="),
                    (StringLiteral, "'big data!'"),
                    (Semicolon, ";"),
                ],
            );
        }
    }

    mod batch {
        use super::*;

        #[test]
        fn test_batch() {
            tokenize_expect(
                BATCH,
                vec![
                    (BeginKeyword, "begin"),
                    (BatchKeyword, "batch"),
                    (InsertKeyword, "insert"),
                    (IntoKeyword, "into"),
                    (Identifier, "big_data_table"),
                    (LeftParenthesis, "("),
                    (Identifier, "text_col1"),
                    (RightParenthesis, ")"),
                    (ValuesKeyword, "values"),
                    (LeftParenthesis, "("),
                    (StringLiteral, "'big data!'"),
                    (RightParenthesis, ")"),
                    (Semicolon, ";"),
                    (InsertKeyword, "insert"),
                    (IntoKeyword, "into"),
                    (Identifier, "big_data_table"),
                    (LeftParenthesis, "("),
                    (Identifier, "text_col2"),
                    (RightParenthesis, ")"),
                    (ValuesKeyword, "values"),
                    (LeftParenthesis, "("),
                    (StringLiteral, "'more data!'"),
                    (RightParenthesis, ")"),
                    (Semicolon, ";"),
                    (ApplyKeyword, "apply"),
                    (BatchKeyword, "batch"),
                    (Semicolon, ";"),
                ],
            )
        }

        #[test]
        fn test_batch_unlogged() {
            tokenize_expect(
                BATCH_UNLOGGED,
                vec![
                    (BeginKeyword, "begin"),
                    (BatchKeyword, "batch"),
                    (UnloggedKeyword, "unlogged"),
                    (InsertKeyword, "insert"),
                    (IntoKeyword, "into"),
                    (Identifier, "big_data_table"),
                    (LeftParenthesis, "("),
                    (Identifier, "text_col1"),
                    (RightParenthesis, ")"),
                    (ValuesKeyword, "values"),
                    (LeftParenthesis, "("),
                    (StringLiteral, "'big data!'"),
                    (RightParenthesis, ")"),
                    (Semicolon, ";"),
                    (InsertKeyword, "insert"),
                    (IntoKeyword, "into"),
                    (Identifier, "big_data_table"),
                    (LeftParenthesis, "("),
                    (Identifier, "text_col2"),
                    (RightParenthesis, ")"),
                    (ValuesKeyword, "values"),
                    (LeftParenthesis, "("),
                    (StringLiteral, "'more data!'"),
                    (RightParenthesis, ")"),
                    (Semicolon, ";"),
                    (ApplyKeyword, "apply"),
                    (BatchKeyword, "batch"),
                    (Semicolon, ";"),
                ],
            )
        }

        #[test]
        fn test_batch_counter() {
            tokenize_expect(
                BATCH_COUNTER,
                vec![
                    (BeginKeyword, "begin"),
                    (BatchKeyword, "batch"),
                    (CounterKeyword, "counter"),
                    (InsertKeyword, "insert"),
                    (IntoKeyword, "into"),
                    (Identifier, "big_data_table"),
                    (LeftParenthesis, "("),
                    (Identifier, "text_col1"),
                    (RightParenthesis, ")"),
                    (ValuesKeyword, "values"),
                    (LeftParenthesis, "("),
                    (StringLiteral, "'big data!'"),
                    (RightParenthesis, ")"),
                    (Semicolon, ";"),
                    (InsertKeyword, "insert"),
                    (IntoKeyword, "into"),
                    (Identifier, "big_data_table"),
                    (LeftParenthesis, "("),
                    (Identifier, "text_col2"),
                    (RightParenthesis, ")"),
                    (ValuesKeyword, "values"),
                    (LeftParenthesis, "("),
                    (StringLiteral, "'more data!'"),
                    (RightParenthesis, ")"),
                    (Semicolon, ";"),
                    (ApplyKeyword, "apply"),
                    (BatchKeyword, "batch"),
                    (Semicolon, ";"),
                ],
            )
        }

        #[test]
        fn test_batch_using_timestamp() {
            tokenize_expect(
                BATCH_USING_TIMESTAMP,
                vec![
                    (BeginKeyword, "begin"),
                    (BatchKeyword, "batch"),
                    (UsingKeyword, "using"),
                    (TimestampKeyword, "timestamp"),
                    (StringLiteral, "'2023-11-14T04:05+0000'"),
                    (InsertKeyword, "insert"),
                    (IntoKeyword, "into"),
                    (Identifier, "big_data_table"),
                    (LeftParenthesis, "("),
                    (Identifier, "text_col1"),
                    (RightParenthesis, ")"),
                    (ValuesKeyword, "values"),
                    (LeftParenthesis, "("),
                    (StringLiteral, "'big data!'"),
                    (RightParenthesis, ")"),
                    (Semicolon, ";"),
                    (InsertKeyword, "insert"),
                    (IntoKeyword, "into"),
                    (Identifier, "big_data_table"),
                    (LeftParenthesis, "("),
                    (Identifier, "text_col2"),
                    (RightParenthesis, ")"),
                    (ValuesKeyword, "values"),
                    (LeftParenthesis, "("),
                    (StringLiteral, "'more data!'"),
                    (RightParenthesis, ")"),
                    (Semicolon, ";"),
                    (ApplyKeyword, "apply"),
                    (BatchKeyword, "batch"),
                    (Semicolon, ";"),
                ],
            )
        }
    }
}
