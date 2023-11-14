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
                    (LeftRoundBracket, "("),
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
                    (LeftRoundBracket, "("),
                    (Identifier, "text_column"),
                    (RightRoundBracket, ")"),
                    (RightRoundBracket, ")"),
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
                    (LeftRoundBracket, "("),
                    (Identifier, "uuid_column"),
                    (UuidKeyword, "uuid"),
                    (PrimaryKeyword, "primary"),
                    (KeyKeyword, "key"),
                    (RightRoundBracket, ")"),
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
                    (LeftRoundBracket, "("),
                    (Identifier, "uuid_column"),
                    (UuidKeyword, "uuid"),
                    (PrimaryKeyword, "primary"),
                    (KeyKeyword, "key"),
                    (RightRoundBracket, ")"),
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
                    (LeftRoundBracket, "("),
                    (Identifier, "uuid_column"),
                    (UuidKeyword, "uuid"),
                    (Comma, ","),
                    (Identifier, "timeuuid_column"),
                    (TimeUuidKeyword, "timeuuid"),
                    (Comma, ","),
                    (PrimaryKeyword, "primary"),
                    (KeyKeyword, "key"),
                    (LeftRoundBracket, "("),
                    (Identifier, "timeuuid_column"),
                    (Comma, ","),
                    (Identifier, "uuid_column"),
                    (RightRoundBracket, ")"),
                    (RightRoundBracket, ")"),
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
                    (LeftRoundBracket, "("),
                    (Identifier, "uuid_column"),
                    (UuidKeyword, "uuid"),
                    (PrimaryKeyword, "primary"),
                    (KeyKeyword, "key"),
                    (RightRoundBracket, ")"),
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
                    (LeftRoundBracket, "("),
                    (Identifier, "uuid_column"),
                    (UuidKeyword, "uuid"),
                    (PrimaryKeyword, "primary"),
                    (KeyKeyword, "key"),
                    (RightRoundBracket, ")"),
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
                    (LeftRoundBracket, "("),
                    (Identifier, "uuid_column"),
                    (UuidKeyword, "uuid"),
                    (PrimaryKeyword, "primary"),
                    (KeyKeyword, "key"),
                    (RightRoundBracket, ")"),
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
                    (LeftRoundBracket, "("),
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
                    (LeftRoundBracket, "("),
                    (Identifier, "text_column"),
                    (Comma, ","),
                    (Identifier, "time_column"),
                    (RightRoundBracket, ")"),
                    (RightRoundBracket, ")"),
                    (WithKeyword, "with"),
                    (ClusteringKeyword, "clustering"),
                    (OrderKeyword, "order"),
                    (ByKeyword, "by"),
                    (LeftRoundBracket, "("),
                    (RightRoundBracket, ")"),
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
                    (LeftRoundBracket, "("),
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
                    (LeftRoundBracket, "("),
                    (Identifier, "text_column"),
                    (Comma, ","),
                    (Identifier, "time_column"),
                    (RightRoundBracket, ")"),
                    (RightRoundBracket, ")"),
                    (WithKeyword, "with"),
                    (ClusteringKeyword, "clustering"),
                    (OrderKeyword, "order"),
                    (ByKeyword, "by"),
                    (LeftRoundBracket, "("),
                    (Identifier, "time_column"),
                    (DescKeyword, "desc"),
                    (RightRoundBracket, ")"),
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
                    (LeftRoundBracket, "("),
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
                    (LeftRoundBracket, "("),
                    (Identifier, "text_column"),
                    (Comma, ","),
                    (Identifier, "time_column"),
                    (RightRoundBracket, ")"),
                    (RightRoundBracket, ")"),
                    (WithKeyword, "with"),
                    (ClusteringKeyword, "clustering"),
                    (OrderKeyword, "order"),
                    (ByKeyword, "by"),
                    (LeftRoundBracket, "("),
                    (Identifier, "time_column"),
                    (AscKeyword, "asc"),
                    (RightRoundBracket, ")"),
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
