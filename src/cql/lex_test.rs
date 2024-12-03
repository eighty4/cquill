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

mod comments {
    use super::*;

    #[test]
    fn test_dash_line_comment() {
        let cql = "select -- commented out \n * from big_data_table;";
        tokenize_expect(
            cql,
            vec![
                (SelectKeyword, "select"),
                (Star, "*"),
                (FromKeyword, "from"),
                (Identifier, "big_data_table"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_dash_line_comment_butts_up_against_identifier() {
        let cql = "select-- commented out \n * from big_data_table;";
        tokenize_expect(
            cql,
            vec![
                (SelectKeyword, "select"),
                (Star, "*"),
                (FromKeyword, "from"),
                (Identifier, "big_data_table"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_slash_line_comment() {
        let cql = "select // commented out \n * from big_data_table;";
        tokenize_expect(
            cql,
            vec![
                (SelectKeyword, "select"),
                (Star, "*"),
                (FromKeyword, "from"),
                (Identifier, "big_data_table"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_multiline_comment() {
        let cql = "select /* commented out \n on multiple lines */ * from big_data_table;";
        tokenize_expect(
            cql,
            vec![
                (SelectKeyword, "select"),
                (Star, "*"),
                (FromKeyword, "from"),
                (Identifier, "big_data_table"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_empty_multiline_comment() {
        let cql = "select /**/ * from big_data_table;";
        tokenize_expect(
            cql,
            vec![
                (SelectKeyword, "select"),
                (Star, "*"),
                (FromKeyword, "from"),
                (Identifier, "big_data_table"),
                (Semicolon, ";"),
            ],
        );
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
    fn test_single_quote_string_literal_escaping_single_quotes_with_successive_single_quotes() {
        tokenize_expect(
            "'tiffany''s breakfest'",
            vec![(
                StringLiteral(StringStyle::SingleQuote),
                "'tiffany''s breakfest'",
            )],
        );
    }

    #[test]
    fn test_triple_quote_string() {
        tokenize_expect(
            "'''tiffany's breakfest'''",
            vec![(
                StringLiteral(StringStyle::TripleQuote),
                "'''tiffany's breakfest'''",
            )],
        );
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
                    (StringLiteral(StringStyle::SingleQuote), "'class'"),
                    (Colon, ":"),
                    (StringLiteral(StringStyle::SingleQuote), "'SimpleStrategy'"),
                    (Comma, ","),
                    (
                        StringLiteral(StringStyle::SingleQuote),
                        "'replication_factor'",
                    ),
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
                    (StringLiteral(StringStyle::SingleQuote), "'class'"),
                    (Colon, ":"),
                    (StringLiteral(StringStyle::SingleQuote), "'SimpleStrategy'"),
                    (Comma, ","),
                    (
                        StringLiteral(StringStyle::SingleQuote),
                        "'replication_factor'",
                    ),
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
                    (StringLiteral(StringStyle::SingleQuote), "'class'"),
                    (Colon, ":"),
                    (StringLiteral(StringStyle::SingleQuote), "'SimpleStrategy'"),
                    (Comma, ","),
                    (
                        StringLiteral(StringStyle::SingleQuote),
                        "'replication_factor'",
                    ),
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
                    (StringLiteral(StringStyle::SingleQuote), "'big data!'"),
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
                    (StringLiteral(StringStyle::SingleQuote), "'class'"),
                    (Colon, ":"),
                    (
                        StringLiteral(StringStyle::SingleQuote),
                        "'LeveledCompactionStrategy'",
                    ),
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
                    (StringLiteral(StringStyle::SingleQuote), "'big data!'"),
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
                    (StringLiteral(StringStyle::SingleQuote), "'big data!'"),
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
                    (StringLiteral(StringStyle::SingleQuote), "'big data!'"),
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
                    (StringLiteral(StringStyle::SingleQuote), "'big data!'"),
                    (AndKeyword, "and"),
                    (Identifier, "clustering_col"),
                    (InKeyword, "in"),
                    (LeftParenthesis, "("),
                    (StringLiteral(StringStyle::SingleQuote), "'abc'"),
                    (Comma, ","),
                    (StringLiteral(StringStyle::SingleQuote), "'def'"),
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
                    (StringLiteral(StringStyle::SingleQuote), "'big data!'"),
                    (AndKeyword, "and"),
                    (LeftParenthesis, "("),
                    (Identifier, "clustering_col1"),
                    (Comma, ","),
                    (Identifier, "clustering_col2"),
                    (RightParenthesis, ")"),
                    (InKeyword, "in"),
                    (LeftParenthesis, "("),
                    (LeftParenthesis, "("),
                    (StringLiteral(StringStyle::SingleQuote), "'abc'"),
                    (Comma, ","),
                    (NumberLiteral, "123"),
                    (RightParenthesis, ")"),
                    (Comma, ","),
                    (LeftParenthesis, "("),
                    (StringLiteral(StringStyle::SingleQuote), "'def'"),
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
                    (StringLiteral(StringStyle::SingleQuote), "'big data!'"),
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
                    (StringLiteral(StringStyle::SingleQuote), "'big data!'"),
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
                    (StringLiteral(StringStyle::SingleQuote), "'big data!'"),
                    (AndKeyword, "and"),
                    (Identifier, "clustering_col"),
                    (Equal, "="),
                    (StringLiteral(StringStyle::SingleQuote), "'more data!'"),
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
                    (StringLiteral(StringStyle::SingleQuote), "'big data!'"),
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
                    (StringLiteral(StringStyle::SingleQuote), "'big data!'"),
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
                    (StringLiteral(StringStyle::SingleQuote), "'big data!'"),
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
                    (StringLiteral(StringStyle::SingleQuote), "'big data!'"),
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
                    (StringLiteral(StringStyle::SingleQuote), "'big data!'"),
                    (RightParenthesis, ")"),
                    (UsingKeyword, "using"),
                    (TimestampKeyword, "timestamp"),
                    (
                        StringLiteral(StringStyle::SingleQuote),
                        "'2023-11-14T04:05+0000'",
                    ),
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
                    (
                        StringLiteral(StringStyle::SingleQuote),
                        "'{\"text_column\": \"big data!\"}'",
                    ),
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
                    (
                        StringLiteral(StringStyle::SingleQuote),
                        "'{\"text_column\": \"big data!\"}'",
                    ),
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
                    (
                        StringLiteral(StringStyle::SingleQuote),
                        "'{\"text_column\": \"big data!\"}'",
                    ),
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
                    (StringLiteral(StringStyle::SingleQuote), "'big data!'"),
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
                    (StringLiteral(StringStyle::SingleQuote), "'big data!'"),
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
                    (StringLiteral(StringStyle::SingleQuote), "'big data!'"),
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
                    (StringLiteral(StringStyle::SingleQuote), "'big data!'"),
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
                    (StringLiteral(StringStyle::SingleQuote), "'big data!'"),
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
                    (
                        StringLiteral(StringStyle::SingleQuote),
                        "'2023-11-14T04:05+0000'",
                    ),
                    (SetKeyword, "set"),
                    (Identifier, "int_column"),
                    (Equal, "="),
                    (NumberLiteral, "1"),
                    (WhereKeyword, "where"),
                    (Identifier, "text_column"),
                    (Equal, "="),
                    (StringLiteral(StringStyle::SingleQuote), "'big data!'"),
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
                    (StringLiteral(StringStyle::SingleQuote), "'big data!'"),
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
                    (StringLiteral(StringStyle::SingleQuote), "'big data!'"),
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
                    (StringLiteral(StringStyle::SingleQuote), "'big data!'"),
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
                    (StringLiteral(StringStyle::SingleQuote), "'big data!'"),
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
                    (
                        StringLiteral(StringStyle::SingleQuote),
                        "'2023-11-14T04:05+0000'",
                    ),
                    (WhereKeyword, "where"),
                    (Identifier, "text_column"),
                    (Equal, "="),
                    (StringLiteral(StringStyle::SingleQuote), "'big data!'"),
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
                    (StringLiteral(StringStyle::SingleQuote), "'big data!'"),
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
                    (StringLiteral(StringStyle::SingleQuote), "'more data!'"),
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
                    (StringLiteral(StringStyle::SingleQuote), "'big data!'"),
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
                    (StringLiteral(StringStyle::SingleQuote), "'more data!'"),
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
                    (StringLiteral(StringStyle::SingleQuote), "'big data!'"),
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
                    (StringLiteral(StringStyle::SingleQuote), "'more data!'"),
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
                    (
                        StringLiteral(StringStyle::SingleQuote),
                        "'2023-11-14T04:05+0000'",
                    ),
                    (InsertKeyword, "insert"),
                    (IntoKeyword, "into"),
                    (Identifier, "big_data_table"),
                    (LeftParenthesis, "("),
                    (Identifier, "text_col1"),
                    (RightParenthesis, ")"),
                    (ValuesKeyword, "values"),
                    (LeftParenthesis, "("),
                    (StringLiteral(StringStyle::SingleQuote), "'big data!'"),
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
                    (StringLiteral(StringStyle::SingleQuote), "'more data!'"),
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

mod secondary_indexes {
    use super::*;

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
                    (Identifier, "keys"),
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
                    (Identifier, "entries"),
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
                    (Identifier, "full"),
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
}

mod materialized_views {
    use super::*;

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
}

mod user_defined_functions {
    use super::*;

    mod create_function {
        use super::*;

        #[test]
        fn test_create_function_with_string_literal() {
            tokenize_expect(
                CREATE_FUNCTION_WITH_STRING_LITERAL,
                vec![
                    (CreateKeyword, "create"),
                    (FunctionKeyword, "function"),
                    (Identifier, "big_data_fn"),
                    (LeftParenthesis, "("),
                    (Identifier, "fn_arg"),
                    (IntKeyword, "int"),
                    (RightParenthesis, ")"),
                    (CalledKeyword, "called"),
                    (OnKeyword, "on"),
                    (NullKeyword, "null"),
                    (InputKeyword, "input"),
                    (ReturnsKeyword, "returns"),
                    (TextKeyword, "text"),
                    (LanguageKeyword, "language"),
                    (Identifier, "java"),
                    (AsKeyword, "as"),
                    (
                        StringLiteral(StringStyle::SingleQuote),
                        "'return fn_arg.toString();'",
                    ),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_create_function_called_on_null_input() {
            tokenize_expect(
                CREATE_FUNCTION_WITH_STRING_LITERAL,
                vec![
                    (CreateKeyword, "create"),
                    (FunctionKeyword, "function"),
                    (Identifier, "big_data_fn"),
                    (LeftParenthesis, "("),
                    (Identifier, "fn_arg"),
                    (IntKeyword, "int"),
                    (RightParenthesis, ")"),
                    (CalledKeyword, "called"),
                    (OnKeyword, "on"),
                    (NullKeyword, "null"),
                    (InputKeyword, "input"),
                    (ReturnsKeyword, "returns"),
                    (TextKeyword, "text"),
                    (LanguageKeyword, "language"),
                    (Identifier, "java"),
                    (AsKeyword, "as"),
                    (
                        StringLiteral(StringStyle::SingleQuote),
                        "'return fn_arg.toString();'",
                    ),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_create_function_returns_null_on_null_input() {
            tokenize_expect(
                CREATE_FUNCTION_RETURNS_NULL_ON_NULL_INPUT,
                vec![
                    (CreateKeyword, "create"),
                    (FunctionKeyword, "function"),
                    (Identifier, "big_data_fn"),
                    (LeftParenthesis, "("),
                    (Identifier, "fn_arg"),
                    (IntKeyword, "int"),
                    (RightParenthesis, ")"),
                    (ReturnsKeyword, "returns"),
                    (NullKeyword, "null"),
                    (OnKeyword, "on"),
                    (NullKeyword, "null"),
                    (InputKeyword, "input"),
                    (ReturnsKeyword, "returns"),
                    (TextKeyword, "text"),
                    (LanguageKeyword, "language"),
                    (Identifier, "java"),
                    (AsKeyword, "as"),
                    (
                        StringLiteral(StringStyle::DollarSign),
                        "$$\n        return fn_arg.toString();\n    $$",
                    ),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_create_or_replace_function() {
            tokenize_expect(
                CREATE_OR_REPLACE_FUNCTION,
                vec![
                    (CreateKeyword, "create"),
                    (OrKeyword, "or"),
                    (ReplaceKeyword, "replace"),
                    (FunctionKeyword, "function"),
                    (Identifier, "big_data_fn"),
                    (LeftParenthesis, "("),
                    (Identifier, "fn_arg"),
                    (IntKeyword, "int"),
                    (RightParenthesis, ")"),
                    (CalledKeyword, "called"),
                    (OnKeyword, "on"),
                    (NullKeyword, "null"),
                    (InputKeyword, "input"),
                    (ReturnsKeyword, "returns"),
                    (TextKeyword, "text"),
                    (LanguageKeyword, "language"),
                    (Identifier, "java"),
                    (AsKeyword, "as"),
                    (
                        StringLiteral(StringStyle::DollarSign),
                        "$$\n        return fn_arg.toString();\n    $$",
                    ),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_create_function_if_not_exists() {
            tokenize_expect(
                CREATE_FUNCTION_IF_NOT_EXISTS,
                vec![
                    (CreateKeyword, "create"),
                    (FunctionKeyword, "function"),
                    (IfKeyword, "if"),
                    (NotKeyword, "not"),
                    (ExistsKeyword, "exists"),
                    (Identifier, "big_data_fn"),
                    (LeftParenthesis, "("),
                    (Identifier, "fn_arg"),
                    (IntKeyword, "int"),
                    (RightParenthesis, ")"),
                    (CalledKeyword, "called"),
                    (OnKeyword, "on"),
                    (NullKeyword, "null"),
                    (InputKeyword, "input"),
                    (ReturnsKeyword, "returns"),
                    (TextKeyword, "text"),
                    (LanguageKeyword, "language"),
                    (Identifier, "java"),
                    (AsKeyword, "as"),
                    (
                        StringLiteral(StringStyle::DollarSign),
                        "$$\n        return fn_arg.toString();\n    $$",
                    ),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_create_function_with_multiple_args() {
            tokenize_expect(
                CREATE_FUNCTION_WITH_MULTIPLE_ARGS,
                vec![
                    (CreateKeyword, "create"),
                    (FunctionKeyword, "function"),
                    (Identifier, "big_data_fn"),
                    (LeftParenthesis, "("),
                    (Identifier, "fn_arg1"),
                    (IntKeyword, "int"),
                    (Comma, ","),
                    (Identifier, "fn_arg2"),
                    (TextKeyword, "text"),
                    (RightParenthesis, ")"),
                    (CalledKeyword, "called"),
                    (OnKeyword, "on"),
                    (NullKeyword, "null"),
                    (InputKeyword, "input"),
                    (ReturnsKeyword, "returns"),
                    (TextKeyword, "text"),
                    (LanguageKeyword, "language"),
                    (Identifier, "java"),
                    (AsKeyword, "as"),
                    (
                        StringLiteral(StringStyle::DollarSign),
                        "$$\n        return fn_arg1.toString();\n    $$",
                    ),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_create_function_with_frozen_arg() {
            tokenize_expect(
                CREATE_FUNCTION_WITH_FROZEN_ARG,
                vec![
                    (CreateKeyword, "create"),
                    (FunctionKeyword, "function"),
                    (Identifier, "big_data_fn"),
                    (LeftParenthesis, "("),
                    (Identifier, "fn_arg"),
                    (Identifier, "frozen"),
                    (LessThan, "<"),
                    (Identifier, "some_udt"),
                    (GreaterThan, ">"),
                    (RightParenthesis, ")"),
                    (CalledKeyword, "called"),
                    (OnKeyword, "on"),
                    (NullKeyword, "null"),
                    (InputKeyword, "input"),
                    (ReturnsKeyword, "returns"),
                    (TextKeyword, "text"),
                    (LanguageKeyword, "language"),
                    (Identifier, "java"),
                    (AsKeyword, "as"),
                    (
                        StringLiteral(StringStyle::DollarSign),
                        "$$\n        return fn_arg.toString();\n    $$",
                    ),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_create_function_returns_user_defined_type() {
            tokenize_expect(
                CREATE_FUNCTION_RETURNS_USER_DEFINED_TYPE,
                vec![
                    (CreateKeyword, "create"),
                    (FunctionKeyword, "function"),
                    (Identifier, "big_data_fn"),
                    (LeftParenthesis, "("),
                    (Identifier, "fn_arg"),
                    (IntKeyword, "int"),
                    (RightParenthesis, ")"),
                    (CalledKeyword, "called"),
                    (OnKeyword, "on"),
                    (NullKeyword, "null"),
                    (InputKeyword, "input"),
                    (ReturnsKeyword, "returns"),
                    (Identifier, "some_udt"),
                    (LanguageKeyword, "language"),
                    (Identifier, "java"),
                    (AsKeyword, "as"),
                    (
                        StringLiteral(StringStyle::DollarSign),
                        "$$\n        return fn_arg.toString();\n    $$",
                    ),
                    (Semicolon, ";"),
                ],
            );
        }
    }

    mod drop_function {
        use super::*;

        #[test]
        fn test_drop_function_without_args() {
            tokenize_expect(
                DROP_FUNCTION_WITHOUT_ARGS,
                vec![
                    (DropKeyword, "drop"),
                    (FunctionKeyword, "function"),
                    (Identifier, "big_data_fn"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_drop_function_with_explicit_keyspace() {
            tokenize_expect(
                DROP_FUNCTION_WITH_EXPLICIT_KEYSPACE,
                vec![
                    (DropKeyword, "drop"),
                    (FunctionKeyword, "function"),
                    (Identifier, "big_data_keyspace"),
                    (Dot, "."),
                    (Identifier, "big_data_fn"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_drop_function_with_single_arg() {
            tokenize_expect(
                DROP_FUNCTION_WITH_SINGLE_ARG,
                vec![
                    (DropKeyword, "drop"),
                    (FunctionKeyword, "function"),
                    (Identifier, "big_data_fn"),
                    (LeftParenthesis, "("),
                    (IntKeyword, "int"),
                    (RightParenthesis, ")"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_drop_function_with_multiple_args() {
            tokenize_expect(
                DROP_FUNCTION_WITH_MULTIPLE_ARGS,
                vec![
                    (DropKeyword, "drop"),
                    (FunctionKeyword, "function"),
                    (Identifier, "big_data_fn"),
                    (LeftParenthesis, "("),
                    (IntKeyword, "int"),
                    (Comma, ","),
                    (TextKeyword, "text"),
                    (RightParenthesis, ")"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_drop_function_and_the_kitchen_sink() {
            tokenize_expect(
                DROP_FUNCTION_WITH_EXPLICIT_KEYSPACE_AND_MULTIPLE_ARGS,
                vec![
                    (DropKeyword, "drop"),
                    (FunctionKeyword, "function"),
                    (Identifier, "big_data_keyspace"),
                    (Dot, "."),
                    (Identifier, "big_data_fn"),
                    (LeftParenthesis, "("),
                    (IntKeyword, "int"),
                    (Comma, ","),
                    (TextKeyword, "text"),
                    (RightParenthesis, ")"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_drop_function_if_exists() {
            tokenize_expect(
                DROP_FUNCTION_IF_EXISTS,
                vec![
                    (DropKeyword, "drop"),
                    (FunctionKeyword, "function"),
                    (IfKeyword, "if"),
                    (ExistsKeyword, "exists"),
                    (Identifier, "big_data_fn"),
                    (Semicolon, ";"),
                ],
            );
        }
    }
}

mod user_defined_aggregates {
    use super::*;

    mod create_aggregate {
        use super::*;

        #[test]
        fn test_create_aggregate_with_single_arg() {
            tokenize_expect(
                CREATE_AGGREGATE_WITH_SINGLE_ARG,
                vec![
                    (CreateKeyword, "create"),
                    (AggregateKeyword, "aggregate"),
                    (Identifier, "big_data_agg"),
                    (LeftParenthesis, "("),
                    (IntKeyword, "int"),
                    (RightParenthesis, ")"),
                    (SFuncKeyword, "sfunc"),
                    (Identifier, "fn_name"),
                    (STypeKeyword, "stype"),
                    (ListKeyword, "list"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_create_aggregate_with_multiple_args() {
            tokenize_expect(
                CREATE_AGGREGATE_WITH_MULTIPLE_ARGS,
                vec![
                    (CreateKeyword, "create"),
                    (AggregateKeyword, "aggregate"),
                    (Identifier, "big_data_agg"),
                    (LeftParenthesis, "("),
                    (IntKeyword, "int"),
                    (Comma, ","),
                    (DoubleKeyword, "double"),
                    (RightParenthesis, ")"),
                    (SFuncKeyword, "sfunc"),
                    (Identifier, "fn_name"),
                    (STypeKeyword, "stype"),
                    (ListKeyword, "list"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_create_aggregate_with_udt_stype() {
            tokenize_expect(
                CREATE_AGGREGATE_WITH_UDT_STYPE,
                vec![
                    (CreateKeyword, "create"),
                    (AggregateKeyword, "aggregate"),
                    (Identifier, "big_data_agg"),
                    (LeftParenthesis, "("),
                    (IntKeyword, "int"),
                    (RightParenthesis, ")"),
                    (SFuncKeyword, "sfunc"),
                    (Identifier, "fn_name"),
                    (STypeKeyword, "stype"),
                    (Identifier, "some_udt"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_create_or_replace_aggregate() {
            tokenize_expect(
                CREATE_OR_REPLACE_AGGREGATE,
                vec![
                    (CreateKeyword, "create"),
                    (OrKeyword, "or"),
                    (ReplaceKeyword, "replace"),
                    (AggregateKeyword, "aggregate"),
                    (Identifier, "big_data_agg"),
                    (LeftParenthesis, "("),
                    (IntKeyword, "int"),
                    (RightParenthesis, ")"),
                    (SFuncKeyword, "sfunc"),
                    (Identifier, "fn_name"),
                    (STypeKeyword, "stype"),
                    (ListKeyword, "list"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_create_aggregate_if_not_exists() {
            tokenize_expect(
                CREATE_AGGREGATE_IF_NOT_EXISTS,
                vec![
                    (CreateKeyword, "create"),
                    (AggregateKeyword, "aggregate"),
                    (IfKeyword, "if"),
                    (NotKeyword, "not"),
                    (ExistsKeyword, "exists"),
                    (Identifier, "big_data_agg"),
                    (LeftParenthesis, "("),
                    (IntKeyword, "int"),
                    (RightParenthesis, ")"),
                    (SFuncKeyword, "sfunc"),
                    (Identifier, "fn_name"),
                    (STypeKeyword, "stype"),
                    (ListKeyword, "list"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_create_aggregate_with_finalfunc() {
            tokenize_expect(
                CREATE_AGGREGATE_WITH_FINALFUNC,
                vec![
                    (CreateKeyword, "create"),
                    (AggregateKeyword, "aggregate"),
                    (Identifier, "big_data_agg"),
                    (LeftParenthesis, "("),
                    (IntKeyword, "int"),
                    (RightParenthesis, ")"),
                    (SFuncKeyword, "sfunc"),
                    (Identifier, "fn_name"),
                    (STypeKeyword, "stype"),
                    (ListKeyword, "list"),
                    (FinalFuncKeyword, "finalfunc"),
                    (Identifier, "ffn_name"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_create_aggregate_with_initcond() {
            tokenize_expect(
                CREATE_AGGREGATE_WITH_INITCOND,
                vec![
                    (CreateKeyword, "create"),
                    (AggregateKeyword, "aggregate"),
                    (Identifier, "big_data_agg"),
                    (LeftParenthesis, "("),
                    (IntKeyword, "int"),
                    (RightParenthesis, ")"),
                    (SFuncKeyword, "sfunc"),
                    (Identifier, "fn_name"),
                    (STypeKeyword, "stype"),
                    (ListKeyword, "list"),
                    (InitCondKeyword, "initcond"),
                    (StringLiteral(StringStyle::SingleQuote), "'state value'"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_create_aggregate_with_finalfunc_and_initcond() {
            tokenize_expect(
                CREATE_AGGREGATE_WITH_FINALFUNC_AND_INITCOND,
                vec![
                    (CreateKeyword, "create"),
                    (AggregateKeyword, "aggregate"),
                    (Identifier, "big_data_agg"),
                    (LeftParenthesis, "("),
                    (IntKeyword, "int"),
                    (RightParenthesis, ")"),
                    (SFuncKeyword, "sfunc"),
                    (Identifier, "fn_name"),
                    (STypeKeyword, "stype"),
                    (ListKeyword, "list"),
                    (FinalFuncKeyword, "finalfunc"),
                    (Identifier, "ffn_name"),
                    (InitCondKeyword, "initcond"),
                    (StringLiteral(StringStyle::SingleQuote), "'state value'"),
                    (Semicolon, ";"),
                ],
            );
        }
    }

    mod drop_aggregate {
        use super::*;

        #[test]
        fn test_drop_aggregate() {
            tokenize_expect(
                DROP_AGGREGATE_WITHOUT_ARGS,
                vec![
                    (DropKeyword, "drop"),
                    (AggregateKeyword, "aggregate"),
                    (Identifier, "big_data_agg"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_drop_aggregate_with_explicit_keyspace() {
            tokenize_expect(
                DROP_AGGREGATE_WITH_EXPLICIT_KEYSPACE,
                vec![
                    (DropKeyword, "drop"),
                    (AggregateKeyword, "aggregate"),
                    (Identifier, "big_data_keyspace"),
                    (Dot, "."),
                    (Identifier, "big_data_agg"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_drop_aggregate_with_single_arg() {
            tokenize_expect(
                DROP_AGGREGATE_WITH_SINGLE_ARG,
                vec![
                    (DropKeyword, "drop"),
                    (AggregateKeyword, "aggregate"),
                    (Identifier, "big_data_agg"),
                    (LeftParenthesis, "("),
                    (IntKeyword, "int"),
                    (RightParenthesis, ")"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_drop_aggregate_with_multiple_args() {
            tokenize_expect(
                DROP_AGGREGATE_WITH_MULTIPLE_ARGS,
                vec![
                    (DropKeyword, "drop"),
                    (AggregateKeyword, "aggregate"),
                    (Identifier, "big_data_agg"),
                    (LeftParenthesis, "("),
                    (IntKeyword, "int"),
                    (Comma, ","),
                    (TextKeyword, "text"),
                    (RightParenthesis, ")"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_drop_aggregate_and_the_kitchen_sink() {
            tokenize_expect(
                DROP_AGGREGATE_WITH_EXPLICIT_KEYSPACE_AND_MULTIPLE_ARGS,
                vec![
                    (DropKeyword, "drop"),
                    (AggregateKeyword, "aggregate"),
                    (Identifier, "big_data_keyspace"),
                    (Dot, "."),
                    (Identifier, "big_data_agg"),
                    (LeftParenthesis, "("),
                    (IntKeyword, "int"),
                    (Comma, ","),
                    (TextKeyword, "text"),
                    (RightParenthesis, ")"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_drop_aggregate_if_exists() {
            tokenize_expect(
                DROP_AGGREGATE_IF_EXISTS,
                vec![
                    (DropKeyword, "drop"),
                    (AggregateKeyword, "aggregate"),
                    (IfKeyword, "if"),
                    (ExistsKeyword, "exists"),
                    (Identifier, "big_data_agg"),
                    (Semicolon, ";"),
                ],
            );
        }
    }
}

mod user_defined_types {
    use super::*;

    mod create_type {
        use super::*;

        #[test]
        fn test_create_type() {
            tokenize_expect(
                CREATE_UDT_WITH_SINGLE_ATTRIBUTE,
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
                CREATE_UDT_WITH_MULTIPLE_ATTRIBUTES,
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
                CREATE_UDT_IF_NOT_EXISTS,
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
                DROP_UDT,
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
                DROP_UDT_IF_EXISTS,
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
}

mod security {
    use super::*;

    mod create_role {
        use super::*;

        #[test]
        fn test_create_role() {
            tokenize_expect(
                CREATE_ROLE_WITH_PASSWORD,
                vec![
                    (CreateKeyword, "create"),
                    (RoleKeyword, "role"),
                    (Identifier, "big_data_role"),
                    (WithKeyword, "with"),
                    (PasswordKeyword, "password"),
                    (Equal, "="),
                    (StringLiteral(StringStyle::SingleQuote), "'asdf'"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                CREATE_ROLE_WITH_HASHED_PASSWORD,
                vec![
                    (CreateKeyword, "create"),
                    (RoleKeyword, "role"),
                    (Identifier, "big_data_role"),
                    (WithKeyword, "with"),
                    (HashedKeyword, "hashed"),
                    (PasswordKeyword, "password"),
                    (Equal, "="),
                    (StringLiteral(StringStyle::SingleQuote), "'aassddff'"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                CREATE_ROLE_WITH_LOGIN_TRUE,
                vec![
                    (CreateKeyword, "create"),
                    (RoleKeyword, "role"),
                    (Identifier, "big_data_role"),
                    (WithKeyword, "with"),
                    (LoginKeyword, "login"),
                    (Equal, "="),
                    (TrueKeyword, "true"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                CREATE_ROLE_WITH_LOGIN_FALSE,
                vec![
                    (CreateKeyword, "create"),
                    (RoleKeyword, "role"),
                    (Identifier, "big_data_role"),
                    (WithKeyword, "with"),
                    (LoginKeyword, "login"),
                    (Equal, "="),
                    (FalseKeyword, "false"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                CREATE_ROLE_WITH_SUPERUSER_TRUE,
                vec![
                    (CreateKeyword, "create"),
                    (RoleKeyword, "role"),
                    (Identifier, "big_data_role"),
                    (WithKeyword, "with"),
                    (SuperUserKeyword, "superuser"),
                    (Equal, "="),
                    (TrueKeyword, "true"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                CREATE_ROLE_WITH_SUPERUSER_FALSE,
                vec![
                    (CreateKeyword, "create"),
                    (RoleKeyword, "role"),
                    (Identifier, "big_data_role"),
                    (WithKeyword, "with"),
                    (SuperUserKeyword, "superuser"),
                    (Equal, "="),
                    (FalseKeyword, "false"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                CREATE_ROLE_WITH_OPTIONS_MAP,
                vec![
                    (CreateKeyword, "create"),
                    (RoleKeyword, "role"),
                    (Identifier, "big_data_role"),
                    (WithKeyword, "with"),
                    (OptionsKeyword, "options"),
                    (Equal, "="),
                    (LeftCurvedBracket, "{"),
                    (StringLiteral(StringStyle::SingleQuote), "'opt1'"),
                    (Colon, ":"),
                    (StringLiteral(StringStyle::SingleQuote), "'val'"),
                    (Comma, ","),
                    (StringLiteral(StringStyle::SingleQuote), "'opt2'"),
                    (Colon, ":"),
                    (NumberLiteral, "99"),
                    (RightCurvedBracket, "}"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                CREATE_ROLE_WITH_ACCESS_TO_DATACENTERS_SET,
                vec![
                    (CreateKeyword, "create"),
                    (RoleKeyword, "role"),
                    (Identifier, "big_data_role"),
                    (WithKeyword, "with"),
                    (AccessKeyword, "access"),
                    (ToKeyword, "to"),
                    (DatacentersKeyword, "datacenters"),
                    (LeftCurvedBracket, "{"),
                    (StringLiteral(StringStyle::SingleQuote), "'dc1'"),
                    (Comma, ","),
                    (StringLiteral(StringStyle::SingleQuote), "'dc2'"),
                    (RightCurvedBracket, "}"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                CREATE_ROLE_WITH_ACCESS_TO_ALL_DATACENTERS,
                vec![
                    (CreateKeyword, "create"),
                    (RoleKeyword, "role"),
                    (Identifier, "big_data_role"),
                    (WithKeyword, "with"),
                    (AccessKeyword, "access"),
                    (ToKeyword, "to"),
                    (AllKeyword, "all"),
                    (DatacentersKeyword, "datacenters"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                CREATE_ROLE_WITH_MULTIPLE_ROLE_OPTIONS,
                vec![
                    (CreateKeyword, "create"),
                    (RoleKeyword, "role"),
                    (Identifier, "big_data_role"),
                    (WithKeyword, "with"),
                    (PasswordKeyword, "password"),
                    (Equal, "="),
                    (StringLiteral(StringStyle::SingleQuote), "'asdf'"),
                    (AndKeyword, "and"),
                    (LoginKeyword, "login"),
                    (Equal, "="),
                    (TrueKeyword, "true"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                CREATE_ROLE_IF_NOT_EXISTS,
                vec![
                    (CreateKeyword, "create"),
                    (RoleKeyword, "role"),
                    (IfKeyword, "if"),
                    (NotKeyword, "not"),
                    (ExistsKeyword, "exists"),
                    (Identifier, "big_data_role"),
                    (WithKeyword, "with"),
                    (PasswordKeyword, "password"),
                    (Equal, "="),
                    (StringLiteral(StringStyle::SingleQuote), "'asdf'"),
                    (Semicolon, ";"),
                ],
            );
        }
    }

    mod alter_role {
        use super::*;

        #[test]
        fn test_alter_role() {
            tokenize_expect(
                ALTER_ROLE_WITH_PASSWORD,
                vec![
                    (AlterKeyword, "alter"),
                    (RoleKeyword, "role"),
                    (Identifier, "big_data_role"),
                    (WithKeyword, "with"),
                    (PasswordKeyword, "password"),
                    (Equal, "="),
                    (StringLiteral(StringStyle::SingleQuote), "'asdf'"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                ALTER_ROLE_WITH_HASHED_PASSWORD,
                vec![
                    (AlterKeyword, "alter"),
                    (RoleKeyword, "role"),
                    (Identifier, "big_data_role"),
                    (WithKeyword, "with"),
                    (HashedKeyword, "hashed"),
                    (PasswordKeyword, "password"),
                    (Equal, "="),
                    (StringLiteral(StringStyle::SingleQuote), "'aassddff'"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                ALTER_ROLE_WITH_LOGIN_TRUE,
                vec![
                    (AlterKeyword, "alter"),
                    (RoleKeyword, "role"),
                    (Identifier, "big_data_role"),
                    (WithKeyword, "with"),
                    (LoginKeyword, "login"),
                    (Equal, "="),
                    (TrueKeyword, "true"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                ALTER_ROLE_WITH_LOGIN_FALSE,
                vec![
                    (AlterKeyword, "alter"),
                    (RoleKeyword, "role"),
                    (Identifier, "big_data_role"),
                    (WithKeyword, "with"),
                    (LoginKeyword, "login"),
                    (Equal, "="),
                    (FalseKeyword, "false"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                ALTER_ROLE_WITH_SUPERUSER_TRUE,
                vec![
                    (AlterKeyword, "alter"),
                    (RoleKeyword, "role"),
                    (Identifier, "big_data_role"),
                    (WithKeyword, "with"),
                    (SuperUserKeyword, "superuser"),
                    (Equal, "="),
                    (TrueKeyword, "true"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                ALTER_ROLE_WITH_SUPERUSER_FALSE,
                vec![
                    (AlterKeyword, "alter"),
                    (RoleKeyword, "role"),
                    (Identifier, "big_data_role"),
                    (WithKeyword, "with"),
                    (SuperUserKeyword, "superuser"),
                    (Equal, "="),
                    (FalseKeyword, "false"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                ALTER_ROLE_WITH_OPTIONS_MAP,
                vec![
                    (AlterKeyword, "alter"),
                    (RoleKeyword, "role"),
                    (Identifier, "big_data_role"),
                    (WithKeyword, "with"),
                    (OptionsKeyword, "options"),
                    (Equal, "="),
                    (LeftCurvedBracket, "{"),
                    (StringLiteral(StringStyle::SingleQuote), "'opt1'"),
                    (Colon, ":"),
                    (StringLiteral(StringStyle::SingleQuote), "'val'"),
                    (Comma, ","),
                    (StringLiteral(StringStyle::SingleQuote), "'opt2'"),
                    (Colon, ":"),
                    (NumberLiteral, "99"),
                    (RightCurvedBracket, "}"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                ALTER_ROLE_WITH_ACCESS_TO_DATACENTERS_SET,
                vec![
                    (AlterKeyword, "alter"),
                    (RoleKeyword, "role"),
                    (Identifier, "big_data_role"),
                    (WithKeyword, "with"),
                    (AccessKeyword, "access"),
                    (ToKeyword, "to"),
                    (DatacentersKeyword, "datacenters"),
                    (LeftCurvedBracket, "{"),
                    (StringLiteral(StringStyle::SingleQuote), "'dc1'"),
                    (Comma, ","),
                    (StringLiteral(StringStyle::SingleQuote), "'dc2'"),
                    (RightCurvedBracket, "}"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                ALTER_ROLE_WITH_ACCESS_TO_ALL_DATACENTERS,
                vec![
                    (AlterKeyword, "alter"),
                    (RoleKeyword, "role"),
                    (Identifier, "big_data_role"),
                    (WithKeyword, "with"),
                    (AccessKeyword, "access"),
                    (ToKeyword, "to"),
                    (AllKeyword, "all"),
                    (DatacentersKeyword, "datacenters"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                ALTER_ROLE_WITH_MULTIPLE_ROLE_OPTIONS,
                vec![
                    (AlterKeyword, "alter"),
                    (RoleKeyword, "role"),
                    (Identifier, "big_data_role"),
                    (WithKeyword, "with"),
                    (PasswordKeyword, "password"),
                    (Equal, "="),
                    (StringLiteral(StringStyle::SingleQuote), "'asdf'"),
                    (AndKeyword, "and"),
                    (LoginKeyword, "login"),
                    (Equal, "="),
                    (TrueKeyword, "true"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                ALTER_ROLE_IF_EXISTS,
                vec![
                    (AlterKeyword, "alter"),
                    (RoleKeyword, "role"),
                    (IfKeyword, "if"),
                    (ExistsKeyword, "exists"),
                    (Identifier, "big_data_role"),
                    (WithKeyword, "with"),
                    (PasswordKeyword, "password"),
                    (Equal, "="),
                    (StringLiteral(StringStyle::SingleQuote), "'asdf'"),
                    (Semicolon, ";"),
                ],
            );
        }
    }

    mod drop_role {
        use super::*;

        #[test]
        fn test_drop_role() {
            tokenize_expect(
                DROP_ROLE,
                vec![
                    (DropKeyword, "drop"),
                    (RoleKeyword, "role"),
                    (Identifier, "big_data_role"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_drop_role_if_exists() {
            tokenize_expect(
                DROP_ROLE_IF_EXISTS,
                vec![
                    (DropKeyword, "drop"),
                    (RoleKeyword, "role"),
                    (IfKeyword, "if"),
                    (ExistsKeyword, "exists"),
                    (Identifier, "big_data_role"),
                    (Semicolon, ";"),
                ],
            );
        }
    }

    mod grant_role {
        use super::*;

        #[test]
        fn test_grant_role() {
            tokenize_expect(
                GRANT_ROLE,
                vec![
                    (GrantKeyword, "grant"),
                    (RoleKeyword, "role"),
                    (Identifier, "big_data_role"),
                    (ToKeyword, "to"),
                    (Identifier, "other_big_data_role"),
                    (Semicolon, ";"),
                ],
            );
        }
    }

    mod revoke_role {
        use super::*;

        #[test]
        fn test_revoke_role() {
            tokenize_expect(
                REVOKE_ROLE,
                vec![
                    (RevokeKeyword, "revoke"),
                    (RoleKeyword, "role"),
                    (Identifier, "big_data_role"),
                    (FromKeyword, "from"),
                    (Identifier, "other_big_data_role"),
                    (Semicolon, ";"),
                ],
            );
        }
    }

    mod list_roles {
        use super::*;

        #[test]
        fn test_list_roles() {
            tokenize_expect(
                LIST_ROLES,
                vec![
                    (ListKeyword, "list"),
                    (RolesKeyword, "roles"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                LIST_ROLES_NOT_RECURSIVELY,
                vec![
                    (ListKeyword, "list"),
                    (RolesKeyword, "roles"),
                    (NoRecursiveKeyword, "norecursive"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_list_roles_of_role() {
            tokenize_expect(
                LIST_ROLES_OF_ROLE,
                vec![
                    (ListKeyword, "list"),
                    (RolesKeyword, "roles"),
                    (OfKeyword, "of"),
                    (Identifier, "big_data_role"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                LIST_ROLES_OF_ROLE_NOT_RECURSIVELY,
                vec![
                    (ListKeyword, "list"),
                    (RolesKeyword, "roles"),
                    (OfKeyword, "of"),
                    (Identifier, "big_data_role"),
                    (NoRecursiveKeyword, "norecursive"),
                    (Semicolon, ";"),
                ],
            );
        }
    }

    mod create_user {
        use super::*;

        #[test]
        fn test_create_user() {
            tokenize_expect(
                CREATE_USER_IF_NOT_EXISTS,
                vec![
                    (CreateKeyword, "create"),
                    (UserKeyword, "user"),
                    (IfKeyword, "if"),
                    (NotKeyword, "not"),
                    (ExistsKeyword, "exists"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                CREATE_USER,
                vec![
                    (CreateKeyword, "create"),
                    (UserKeyword, "user"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                CREATE_USER_SUPERUSER,
                vec![
                    (CreateKeyword, "create"),
                    (UserKeyword, "user"),
                    (Identifier, "big_data_user"),
                    (SuperUserKeyword, "superuser"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                CREATE_USER_NOT_SUPERUSER,
                vec![
                    (CreateKeyword, "create"),
                    (UserKeyword, "user"),
                    (Identifier, "big_data_user"),
                    (NoSuperUserKeyword, "nosuperuser"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                CREATE_USER_WITH_PASSWORD,
                vec![
                    (CreateKeyword, "create"),
                    (UserKeyword, "user"),
                    (Identifier, "big_data_user"),
                    (WithKeyword, "with"),
                    (PasswordKeyword, "password"),
                    (StringLiteral(StringStyle::SingleQuote), "'asdf'"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                CREATE_USER_WITH_PASSWORD_SUPERUSER,
                vec![
                    (CreateKeyword, "create"),
                    (UserKeyword, "user"),
                    (Identifier, "big_data_user"),
                    (WithKeyword, "with"),
                    (PasswordKeyword, "password"),
                    (StringLiteral(StringStyle::SingleQuote), "'asdf'"),
                    (SuperUserKeyword, "superuser"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                CREATE_USER_WITH_PASSWORD_NOT_SUPERUSER,
                vec![
                    (CreateKeyword, "create"),
                    (UserKeyword, "user"),
                    (Identifier, "big_data_user"),
                    (WithKeyword, "with"),
                    (PasswordKeyword, "password"),
                    (StringLiteral(StringStyle::SingleQuote), "'asdf'"),
                    (NoSuperUserKeyword, "nosuperuser"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                CREATE_USER_WITH_HASHED_PASSWORD,
                vec![
                    (CreateKeyword, "create"),
                    (UserKeyword, "user"),
                    (Identifier, "big_data_user"),
                    (WithKeyword, "with"),
                    (HashedKeyword, "hashed"),
                    (PasswordKeyword, "password"),
                    (StringLiteral(StringStyle::SingleQuote), "'aassddff'"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                CREATE_USER_WITH_HASHED_PASSWORD_SUPERUSER,
                vec![
                    (CreateKeyword, "create"),
                    (UserKeyword, "user"),
                    (Identifier, "big_data_user"),
                    (WithKeyword, "with"),
                    (HashedKeyword, "hashed"),
                    (PasswordKeyword, "password"),
                    (StringLiteral(StringStyle::SingleQuote), "'aassddff'"),
                    (SuperUserKeyword, "superuser"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                CREATE_USER_WITH_HASHED_PASSWORD_NOT_SUPERUSER,
                vec![
                    (CreateKeyword, "create"),
                    (UserKeyword, "user"),
                    (Identifier, "big_data_user"),
                    (WithKeyword, "with"),
                    (HashedKeyword, "hashed"),
                    (PasswordKeyword, "password"),
                    (StringLiteral(StringStyle::SingleQuote), "'aassddff'"),
                    (NoSuperUserKeyword, "nosuperuser"),
                    (Semicolon, ";"),
                ],
            );
        }
    }

    mod alter_user {
        use super::*;

        #[test]
        fn test_alter_user() {
            tokenize_expect(
                ALTER_USER_IF_EXISTS,
                vec![
                    (AlterKeyword, "alter"),
                    (UserKeyword, "user"),
                    (IfKeyword, "if"),
                    (ExistsKeyword, "exists"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                ALTER_USER,
                vec![
                    (AlterKeyword, "alter"),
                    (UserKeyword, "user"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                ALTER_USER_SUPERUSER,
                vec![
                    (AlterKeyword, "alter"),
                    (UserKeyword, "user"),
                    (Identifier, "big_data_user"),
                    (SuperUserKeyword, "superuser"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                ALTER_USER_NOT_SUPERUSER,
                vec![
                    (AlterKeyword, "alter"),
                    (UserKeyword, "user"),
                    (Identifier, "big_data_user"),
                    (NoSuperUserKeyword, "nosuperuser"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                ALTER_USER_WITH_PASSWORD,
                vec![
                    (AlterKeyword, "alter"),
                    (UserKeyword, "user"),
                    (Identifier, "big_data_user"),
                    (WithKeyword, "with"),
                    (PasswordKeyword, "password"),
                    (StringLiteral(StringStyle::SingleQuote), "'asdf'"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                ALTER_USER_WITH_PASSWORD_SUPERUSER,
                vec![
                    (AlterKeyword, "alter"),
                    (UserKeyword, "user"),
                    (Identifier, "big_data_user"),
                    (WithKeyword, "with"),
                    (PasswordKeyword, "password"),
                    (StringLiteral(StringStyle::SingleQuote), "'asdf'"),
                    (SuperUserKeyword, "superuser"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                ALTER_USER_WITH_PASSWORD_NOT_SUPERUSER,
                vec![
                    (AlterKeyword, "alter"),
                    (UserKeyword, "user"),
                    (Identifier, "big_data_user"),
                    (WithKeyword, "with"),
                    (PasswordKeyword, "password"),
                    (StringLiteral(StringStyle::SingleQuote), "'asdf'"),
                    (NoSuperUserKeyword, "nosuperuser"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                ALTER_USER_WITH_HASHED_PASSWORD,
                vec![
                    (AlterKeyword, "alter"),
                    (UserKeyword, "user"),
                    (Identifier, "big_data_user"),
                    (WithKeyword, "with"),
                    (HashedKeyword, "hashed"),
                    (PasswordKeyword, "password"),
                    (StringLiteral(StringStyle::SingleQuote), "'aassddff'"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                ALTER_USER_WITH_HASHED_PASSWORD_SUPERUSER,
                vec![
                    (AlterKeyword, "alter"),
                    (UserKeyword, "user"),
                    (Identifier, "big_data_user"),
                    (WithKeyword, "with"),
                    (HashedKeyword, "hashed"),
                    (PasswordKeyword, "password"),
                    (StringLiteral(StringStyle::SingleQuote), "'aassddff'"),
                    (SuperUserKeyword, "superuser"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                ALTER_USER_WITH_HASHED_PASSWORD_NOT_SUPERUSER,
                vec![
                    (AlterKeyword, "alter"),
                    (UserKeyword, "user"),
                    (Identifier, "big_data_user"),
                    (WithKeyword, "with"),
                    (HashedKeyword, "hashed"),
                    (PasswordKeyword, "password"),
                    (StringLiteral(StringStyle::SingleQuote), "'aassddff'"),
                    (NoSuperUserKeyword, "nosuperuser"),
                    (Semicolon, ";"),
                ],
            );
        }
    }

    mod drop_user {
        use super::*;

        #[test]
        fn test_drop_user() {
            tokenize_expect(
                DROP_USER,
                vec![
                    (DropKeyword, "drop"),
                    (UserKeyword, "user"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_drop_user_if_exists() {
            tokenize_expect(
                DROP_USER_IF_EXISTS,
                vec![
                    (DropKeyword, "drop"),
                    (UserKeyword, "user"),
                    (IfKeyword, "if"),
                    (ExistsKeyword, "exists"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
        }
    }

    mod list_users {
        use super::*;

        #[test]
        fn test_list_users() {
            tokenize_expect(
                LIST_USERS,
                vec![
                    (ListKeyword, "list"),
                    (UsersKeyword, "users"),
                    (Semicolon, ";"),
                ],
            );
        }
    }

    mod grant_permission {
        use super::*;

        #[test]
        fn test_grant_permissions_on_roles() {
            tokenize_expect(
                GRANT_ALL_PERMISSIONS_ON_ALL_KEYSPACES,
                vec![
                    (GrantKeyword, "grant"),
                    (AllKeyword, "all"),
                    (PermissionsKeyword, "permissions"),
                    (OnKeyword, "on"),
                    (AllKeyword, "all"),
                    (KeyspacesKeyword, "keyspaces"),
                    (ToKeyword, "to"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                GRANT_ALL_ON_ALL_KEYSPACES,
                vec![
                    (GrantKeyword, "grant"),
                    (AllKeyword, "all"),
                    (OnKeyword, "on"),
                    (AllKeyword, "all"),
                    (KeyspacesKeyword, "keyspaces"),
                    (ToKeyword, "to"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                GRANT_ALL_ON_KEYSPACE,
                vec![
                    (GrantKeyword, "grant"),
                    (AllKeyword, "all"),
                    (OnKeyword, "on"),
                    (KeyspaceKeyword, "keyspace"),
                    (Identifier, "big_data_keyspace"),
                    (ToKeyword, "to"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                GRANT_ALL_ON_TABLE_IMPLICITLY,
                vec![
                    (GrantKeyword, "grant"),
                    (AllKeyword, "all"),
                    (OnKeyword, "on"),
                    (Identifier, "big_data_table"),
                    (ToKeyword, "to"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                GRANT_ALL_ON_TABLE_WITH_EXPLICIT_KEYSPACE_IMPLICITLY,
                vec![
                    (GrantKeyword, "grant"),
                    (AllKeyword, "all"),
                    (OnKeyword, "on"),
                    (Identifier, "big_data_keyspace"),
                    (Dot, "."),
                    (Identifier, "big_data_table"),
                    (ToKeyword, "to"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                GRANT_ALL_ON_TABLE_EXPLICITLY,
                vec![
                    (GrantKeyword, "grant"),
                    (AllKeyword, "all"),
                    (OnKeyword, "on"),
                    (TableKeyword, "table"),
                    (Identifier, "big_data_table"),
                    (ToKeyword, "to"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                GRANT_ALL_ON_TABLE_WITH_EXPLICIT_KEYSPACE_EXPLICITLY,
                vec![
                    (GrantKeyword, "grant"),
                    (AllKeyword, "all"),
                    (OnKeyword, "on"),
                    (TableKeyword, "table"),
                    (Identifier, "big_data_keyspace"),
                    (Dot, "."),
                    (Identifier, "big_data_table"),
                    (ToKeyword, "to"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                GRANT_ALL_ON_ALL_ROLES,
                vec![
                    (GrantKeyword, "grant"),
                    (AllKeyword, "all"),
                    (OnKeyword, "on"),
                    (AllKeyword, "all"),
                    (RolesKeyword, "roles"),
                    (ToKeyword, "to"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                GRANT_ALL_ON_ROLE,
                vec![
                    (GrantKeyword, "grant"),
                    (AllKeyword, "all"),
                    (OnKeyword, "on"),
                    (RoleKeyword, "role"),
                    (Identifier, "big_data_role"),
                    (ToKeyword, "to"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                GRANT_ALL_ON_ALL_FUNCTIONS,
                vec![
                    (GrantKeyword, "grant"),
                    (AllKeyword, "all"),
                    (OnKeyword, "on"),
                    (AllKeyword, "all"),
                    (FunctionsKeyword, "functions"),
                    (ToKeyword, "to"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                GRANT_ALL_ON_FUNCTION,
                vec![
                    (GrantKeyword, "grant"),
                    (AllKeyword, "all"),
                    (OnKeyword, "on"),
                    (FunctionKeyword, "function"),
                    (Identifier, "big_data_function"),
                    (LeftParenthesis, "("),
                    (IntKeyword, "int"),
                    (RightParenthesis, ")"),
                    (ToKeyword, "to"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                GRANT_ALL_ON_FUNCTION_WITH_EXPLICIT_KEYSPACE,
                vec![
                    (GrantKeyword, "grant"),
                    (AllKeyword, "all"),
                    (OnKeyword, "on"),
                    (FunctionKeyword, "function"),
                    (Identifier, "big_data_keyspace"),
                    (Dot, "."),
                    (Identifier, "big_data_function"),
                    (LeftParenthesis, "("),
                    (IntKeyword, "int"),
                    (RightParenthesis, ")"),
                    (ToKeyword, "to"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                GRANT_ALL_ON_ALL_MBEANS,
                vec![
                    (GrantKeyword, "grant"),
                    (AllKeyword, "all"),
                    (OnKeyword, "on"),
                    (AllKeyword, "all"),
                    (MBeansKeyword, "mbeans"),
                    (ToKeyword, "to"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                GRANT_ALL_ON_MBEANS,
                vec![
                    (GrantKeyword, "grant"),
                    (AllKeyword, "all"),
                    (OnKeyword, "on"),
                    (MBeansKeyword, "mbeans"),
                    (Identifier, "big_data_mbean"),
                    (ToKeyword, "to"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                GRANT_ALL_ON_MBEAN,
                vec![
                    (GrantKeyword, "grant"),
                    (AllKeyword, "all"),
                    (OnKeyword, "on"),
                    (MBeanKeyword, "mbean"),
                    (Identifier, "big_data_mbean"),
                    (ToKeyword, "to"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_grant_permissions() {
            tokenize_expect(
                GRANT_CREATE_PERMISSION_ON_ALL_KEYSPACES,
                vec![
                    (GrantKeyword, "grant"),
                    (CreateKeyword, "create"),
                    (PermissionKeyword, "permission"),
                    (OnKeyword, "on"),
                    (AllKeyword, "all"),
                    (KeyspacesKeyword, "keyspaces"),
                    (ToKeyword, "to"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                GRANT_CREATE_ON_ALL_KEYSPACES,
                vec![
                    (GrantKeyword, "grant"),
                    (CreateKeyword, "create"),
                    (OnKeyword, "on"),
                    (AllKeyword, "all"),
                    (KeyspacesKeyword, "keyspaces"),
                    (ToKeyword, "to"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                GRANT_ALTER_ON_ALL_KEYSPACES,
                vec![
                    (GrantKeyword, "grant"),
                    (AlterKeyword, "alter"),
                    (OnKeyword, "on"),
                    (AllKeyword, "all"),
                    (KeyspacesKeyword, "keyspaces"),
                    (ToKeyword, "to"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                GRANT_DROP_ON_ALL_KEYSPACES,
                vec![
                    (GrantKeyword, "grant"),
                    (DropKeyword, "drop"),
                    (OnKeyword, "on"),
                    (AllKeyword, "all"),
                    (KeyspacesKeyword, "keyspaces"),
                    (ToKeyword, "to"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                GRANT_SELECT_ON_ALL_KEYSPACES,
                vec![
                    (GrantKeyword, "grant"),
                    (SelectKeyword, "select"),
                    (OnKeyword, "on"),
                    (AllKeyword, "all"),
                    (KeyspacesKeyword, "keyspaces"),
                    (ToKeyword, "to"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                GRANT_MODIFY_ON_ALL_KEYSPACES,
                vec![
                    (GrantKeyword, "grant"),
                    (ModifyKeyword, "modify"),
                    (OnKeyword, "on"),
                    (AllKeyword, "all"),
                    (KeyspacesKeyword, "keyspaces"),
                    (ToKeyword, "to"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                GRANT_AUTHORIZE_ON_ALL_KEYSPACES,
                vec![
                    (GrantKeyword, "grant"),
                    (AuthorizeKeyword, "authorize"),
                    (OnKeyword, "on"),
                    (AllKeyword, "all"),
                    (KeyspacesKeyword, "keyspaces"),
                    (ToKeyword, "to"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                GRANT_DESCRIBE_ON_ALL_KEYSPACES,
                vec![
                    (GrantKeyword, "grant"),
                    (DescribeKeyword, "describe"),
                    (OnKeyword, "on"),
                    (AllKeyword, "all"),
                    (KeyspacesKeyword, "keyspaces"),
                    (ToKeyword, "to"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                GRANT_EXECUTE_ON_ALL_KEYSPACES,
                vec![
                    (GrantKeyword, "grant"),
                    (ExecuteKeyword, "execute"),
                    (OnKeyword, "on"),
                    (AllKeyword, "all"),
                    (KeyspacesKeyword, "keyspaces"),
                    (ToKeyword, "to"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
        }
    }

    mod revoke_permission {
        use super::*;

        #[test]
        fn test_revoke_permissions_on_roles() {
            tokenize_expect(
                REVOKE_ALL_PERMISSIONS_ON_ALL_KEYSPACES,
                vec![
                    (RevokeKeyword, "revoke"),
                    (AllKeyword, "all"),
                    (PermissionsKeyword, "permissions"),
                    (OnKeyword, "on"),
                    (AllKeyword, "all"),
                    (KeyspacesKeyword, "keyspaces"),
                    (FromKeyword, "from"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                REVOKE_ALL_ON_ALL_KEYSPACES,
                vec![
                    (RevokeKeyword, "revoke"),
                    (AllKeyword, "all"),
                    (OnKeyword, "on"),
                    (AllKeyword, "all"),
                    (KeyspacesKeyword, "keyspaces"),
                    (FromKeyword, "from"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                REVOKE_ALL_ON_KEYSPACE,
                vec![
                    (RevokeKeyword, "revoke"),
                    (AllKeyword, "all"),
                    (OnKeyword, "on"),
                    (KeyspaceKeyword, "keyspace"),
                    (Identifier, "big_data_keyspace"),
                    (FromKeyword, "from"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                REVOKE_ALL_ON_TABLE_IMPLICITLY,
                vec![
                    (RevokeKeyword, "revoke"),
                    (AllKeyword, "all"),
                    (OnKeyword, "on"),
                    (Identifier, "big_data_table"),
                    (FromKeyword, "from"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                REVOKE_ALL_ON_TABLE_WITH_EXPLICIT_KEYSPACE_IMPLICITLY,
                vec![
                    (RevokeKeyword, "revoke"),
                    (AllKeyword, "all"),
                    (OnKeyword, "on"),
                    (Identifier, "big_data_keyspace"),
                    (Dot, "."),
                    (Identifier, "big_data_table"),
                    (FromKeyword, "from"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                REVOKE_ALL_ON_TABLE_EXPLICITLY,
                vec![
                    (RevokeKeyword, "revoke"),
                    (AllKeyword, "all"),
                    (OnKeyword, "on"),
                    (TableKeyword, "table"),
                    (Identifier, "big_data_table"),
                    (FromKeyword, "from"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                REVOKE_ALL_ON_TABLE_WITH_EXPLICIT_KEYSPACE_EXPLICITLY,
                vec![
                    (RevokeKeyword, "revoke"),
                    (AllKeyword, "all"),
                    (OnKeyword, "on"),
                    (TableKeyword, "table"),
                    (Identifier, "big_data_keyspace"),
                    (Dot, "."),
                    (Identifier, "big_data_table"),
                    (FromKeyword, "from"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                REVOKE_ALL_ON_ALL_ROLES,
                vec![
                    (RevokeKeyword, "revoke"),
                    (AllKeyword, "all"),
                    (OnKeyword, "on"),
                    (AllKeyword, "all"),
                    (RolesKeyword, "roles"),
                    (FromKeyword, "from"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                REVOKE_ALL_ON_ROLE,
                vec![
                    (RevokeKeyword, "revoke"),
                    (AllKeyword, "all"),
                    (OnKeyword, "on"),
                    (RoleKeyword, "role"),
                    (Identifier, "big_data_role"),
                    (FromKeyword, "from"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                REVOKE_ALL_ON_ALL_FUNCTIONS,
                vec![
                    (RevokeKeyword, "revoke"),
                    (AllKeyword, "all"),
                    (OnKeyword, "on"),
                    (AllKeyword, "all"),
                    (FunctionsKeyword, "functions"),
                    (FromKeyword, "from"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                REVOKE_ALL_ON_FUNCTION,
                vec![
                    (RevokeKeyword, "revoke"),
                    (AllKeyword, "all"),
                    (OnKeyword, "on"),
                    (FunctionKeyword, "function"),
                    (Identifier, "big_data_function"),
                    (LeftParenthesis, "("),
                    (IntKeyword, "int"),
                    (RightParenthesis, ")"),
                    (FromKeyword, "from"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                REVOKE_ALL_ON_FUNCTION_WITH_EXPLICIT_KEYSPACE,
                vec![
                    (RevokeKeyword, "revoke"),
                    (AllKeyword, "all"),
                    (OnKeyword, "on"),
                    (FunctionKeyword, "function"),
                    (Identifier, "big_data_keyspace"),
                    (Dot, "."),
                    (Identifier, "big_data_function"),
                    (LeftParenthesis, "("),
                    (IntKeyword, "int"),
                    (RightParenthesis, ")"),
                    (FromKeyword, "from"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                REVOKE_ALL_ON_ALL_MBEANS,
                vec![
                    (RevokeKeyword, "revoke"),
                    (AllKeyword, "all"),
                    (OnKeyword, "on"),
                    (AllKeyword, "all"),
                    (MBeansKeyword, "mbeans"),
                    (FromKeyword, "from"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                REVOKE_ALL_ON_MBEANS,
                vec![
                    (RevokeKeyword, "revoke"),
                    (AllKeyword, "all"),
                    (OnKeyword, "on"),
                    (MBeansKeyword, "mbeans"),
                    (Identifier, "big_data_mbean"),
                    (FromKeyword, "from"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                REVOKE_ALL_ON_MBEAN,
                vec![
                    (RevokeKeyword, "revoke"),
                    (AllKeyword, "all"),
                    (OnKeyword, "on"),
                    (MBeanKeyword, "mbean"),
                    (Identifier, "big_data_mbean"),
                    (FromKeyword, "from"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_revoke_permissions() {
            tokenize_expect(
                REVOKE_CREATE_PERMISSION_ON_ALL_KEYSPACES,
                vec![
                    (RevokeKeyword, "revoke"),
                    (CreateKeyword, "create"),
                    (PermissionKeyword, "permission"),
                    (OnKeyword, "on"),
                    (AllKeyword, "all"),
                    (KeyspacesKeyword, "keyspaces"),
                    (FromKeyword, "from"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                REVOKE_CREATE_ON_ALL_KEYSPACES,
                vec![
                    (RevokeKeyword, "revoke"),
                    (CreateKeyword, "create"),
                    (OnKeyword, "on"),
                    (AllKeyword, "all"),
                    (KeyspacesKeyword, "keyspaces"),
                    (FromKeyword, "from"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                REVOKE_ALTER_ON_ALL_KEYSPACES,
                vec![
                    (RevokeKeyword, "revoke"),
                    (AlterKeyword, "alter"),
                    (OnKeyword, "on"),
                    (AllKeyword, "all"),
                    (KeyspacesKeyword, "keyspaces"),
                    (FromKeyword, "from"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                REVOKE_DROP_ON_ALL_KEYSPACES,
                vec![
                    (RevokeKeyword, "revoke"),
                    (DropKeyword, "drop"),
                    (OnKeyword, "on"),
                    (AllKeyword, "all"),
                    (KeyspacesKeyword, "keyspaces"),
                    (FromKeyword, "from"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                REVOKE_SELECT_ON_ALL_KEYSPACES,
                vec![
                    (RevokeKeyword, "revoke"),
                    (SelectKeyword, "select"),
                    (OnKeyword, "on"),
                    (AllKeyword, "all"),
                    (KeyspacesKeyword, "keyspaces"),
                    (FromKeyword, "from"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                REVOKE_MODIFY_ON_ALL_KEYSPACES,
                vec![
                    (RevokeKeyword, "revoke"),
                    (ModifyKeyword, "modify"),
                    (OnKeyword, "on"),
                    (AllKeyword, "all"),
                    (KeyspacesKeyword, "keyspaces"),
                    (FromKeyword, "from"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                REVOKE_AUTHORIZE_ON_ALL_KEYSPACES,
                vec![
                    (RevokeKeyword, "revoke"),
                    (AuthorizeKeyword, "authorize"),
                    (OnKeyword, "on"),
                    (AllKeyword, "all"),
                    (KeyspacesKeyword, "keyspaces"),
                    (FromKeyword, "from"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                REVOKE_DESCRIBE_ON_ALL_KEYSPACES,
                vec![
                    (RevokeKeyword, "revoke"),
                    (DescribeKeyword, "describe"),
                    (OnKeyword, "on"),
                    (AllKeyword, "all"),
                    (KeyspacesKeyword, "keyspaces"),
                    (FromKeyword, "from"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                REVOKE_EXECUTE_ON_ALL_KEYSPACES,
                vec![
                    (RevokeKeyword, "revoke"),
                    (ExecuteKeyword, "execute"),
                    (OnKeyword, "on"),
                    (AllKeyword, "all"),
                    (KeyspacesKeyword, "keyspaces"),
                    (FromKeyword, "from"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
        }
    }

    mod list_permissions {
        use super::*;

        #[test]
        fn test_list_permissions_on_roles() {
            tokenize_expect(
                LIST_ALL_PERMISSIONS_ON_ALL_KEYSPACES,
                vec![
                    (ListKeyword, "list"),
                    (AllKeyword, "all"),
                    (PermissionsKeyword, "permissions"),
                    (OnKeyword, "on"),
                    (AllKeyword, "all"),
                    (KeyspacesKeyword, "keyspaces"),
                    (OfKeyword, "of"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                LIST_ALL_ON_ALL_KEYSPACES,
                vec![
                    (ListKeyword, "list"),
                    (AllKeyword, "all"),
                    (OnKeyword, "on"),
                    (AllKeyword, "all"),
                    (KeyspacesKeyword, "keyspaces"),
                    (OfKeyword, "of"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                LIST_ALL_ON_KEYSPACE,
                vec![
                    (ListKeyword, "list"),
                    (AllKeyword, "all"),
                    (OnKeyword, "on"),
                    (KeyspaceKeyword, "keyspace"),
                    (Identifier, "big_data_keyspace"),
                    (OfKeyword, "of"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                LIST_ALL_ON_TABLE_IMPLICITLY,
                vec![
                    (ListKeyword, "list"),
                    (AllKeyword, "all"),
                    (OnKeyword, "on"),
                    (Identifier, "big_data_table"),
                    (OfKeyword, "of"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                LIST_ALL_ON_TABLE_WITH_EXPLICIT_KEYSPACE_IMPLICITLY,
                vec![
                    (ListKeyword, "list"),
                    (AllKeyword, "all"),
                    (OnKeyword, "on"),
                    (Identifier, "big_data_keyspace"),
                    (Dot, "."),
                    (Identifier, "big_data_table"),
                    (OfKeyword, "of"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                LIST_ALL_ON_TABLE_EXPLICITLY,
                vec![
                    (ListKeyword, "list"),
                    (AllKeyword, "all"),
                    (OnKeyword, "on"),
                    (TableKeyword, "table"),
                    (Identifier, "big_data_table"),
                    (OfKeyword, "of"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                LIST_ALL_ON_TABLE_WITH_EXPLICIT_KEYSPACE_EXPLICITLY,
                vec![
                    (ListKeyword, "list"),
                    (AllKeyword, "all"),
                    (OnKeyword, "on"),
                    (TableKeyword, "table"),
                    (Identifier, "big_data_keyspace"),
                    (Dot, "."),
                    (Identifier, "big_data_table"),
                    (OfKeyword, "of"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                LIST_ALL_ON_ALL_ROLES,
                vec![
                    (ListKeyword, "list"),
                    (AllKeyword, "all"),
                    (OnKeyword, "on"),
                    (AllKeyword, "all"),
                    (RolesKeyword, "roles"),
                    (OfKeyword, "of"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                LIST_ALL_ON_ROLE,
                vec![
                    (ListKeyword, "list"),
                    (AllKeyword, "all"),
                    (OnKeyword, "on"),
                    (RoleKeyword, "role"),
                    (Identifier, "big_data_role"),
                    (OfKeyword, "of"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                LIST_ALL_ON_ALL_FUNCTIONS,
                vec![
                    (ListKeyword, "list"),
                    (AllKeyword, "all"),
                    (OnKeyword, "on"),
                    (AllKeyword, "all"),
                    (FunctionsKeyword, "functions"),
                    (OfKeyword, "of"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                LIST_ALL_ON_FUNCTION,
                vec![
                    (ListKeyword, "list"),
                    (AllKeyword, "all"),
                    (OnKeyword, "on"),
                    (FunctionKeyword, "function"),
                    (Identifier, "big_data_function"),
                    (LeftParenthesis, "("),
                    (IntKeyword, "int"),
                    (RightParenthesis, ")"),
                    (OfKeyword, "of"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                LIST_ALL_ON_FUNCTION_WITH_EXPLICIT_KEYSPACE,
                vec![
                    (ListKeyword, "list"),
                    (AllKeyword, "all"),
                    (OnKeyword, "on"),
                    (FunctionKeyword, "function"),
                    (Identifier, "big_data_keyspace"),
                    (Dot, "."),
                    (Identifier, "big_data_function"),
                    (LeftParenthesis, "("),
                    (IntKeyword, "int"),
                    (RightParenthesis, ")"),
                    (OfKeyword, "of"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                LIST_ALL_ON_ALL_MBEANS,
                vec![
                    (ListKeyword, "list"),
                    (AllKeyword, "all"),
                    (OnKeyword, "on"),
                    (AllKeyword, "all"),
                    (MBeansKeyword, "mbeans"),
                    (OfKeyword, "of"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                LIST_ALL_ON_MBEANS,
                vec![
                    (ListKeyword, "list"),
                    (AllKeyword, "all"),
                    (OnKeyword, "on"),
                    (MBeansKeyword, "mbeans"),
                    (Identifier, "big_data_mbean"),
                    (OfKeyword, "of"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                LIST_ALL_ON_MBEAN,
                vec![
                    (ListKeyword, "list"),
                    (AllKeyword, "all"),
                    (OnKeyword, "on"),
                    (MBeanKeyword, "mbean"),
                    (Identifier, "big_data_mbean"),
                    (OfKeyword, "of"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
        }

        #[test]
        fn test_list_permissions() {
            tokenize_expect(
                LIST_CREATE_PERMISSION_ON_ALL_KEYSPACES,
                vec![
                    (ListKeyword, "list"),
                    (CreateKeyword, "create"),
                    (PermissionKeyword, "permission"),
                    (OnKeyword, "on"),
                    (AllKeyword, "all"),
                    (KeyspacesKeyword, "keyspaces"),
                    (OfKeyword, "of"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                LIST_CREATE_ON_ALL_KEYSPACES,
                vec![
                    (ListKeyword, "list"),
                    (CreateKeyword, "create"),
                    (OnKeyword, "on"),
                    (AllKeyword, "all"),
                    (KeyspacesKeyword, "keyspaces"),
                    (OfKeyword, "of"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                LIST_ALTER_ON_ALL_KEYSPACES,
                vec![
                    (ListKeyword, "list"),
                    (AlterKeyword, "alter"),
                    (OnKeyword, "on"),
                    (AllKeyword, "all"),
                    (KeyspacesKeyword, "keyspaces"),
                    (OfKeyword, "of"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                LIST_DROP_ON_ALL_KEYSPACES,
                vec![
                    (ListKeyword, "list"),
                    (DropKeyword, "drop"),
                    (OnKeyword, "on"),
                    (AllKeyword, "all"),
                    (KeyspacesKeyword, "keyspaces"),
                    (OfKeyword, "of"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                LIST_SELECT_ON_ALL_KEYSPACES,
                vec![
                    (ListKeyword, "list"),
                    (SelectKeyword, "select"),
                    (OnKeyword, "on"),
                    (AllKeyword, "all"),
                    (KeyspacesKeyword, "keyspaces"),
                    (OfKeyword, "of"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                LIST_MODIFY_ON_ALL_KEYSPACES,
                vec![
                    (ListKeyword, "list"),
                    (ModifyKeyword, "modify"),
                    (OnKeyword, "on"),
                    (AllKeyword, "all"),
                    (KeyspacesKeyword, "keyspaces"),
                    (OfKeyword, "of"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                LIST_AUTHORIZE_ON_ALL_KEYSPACES,
                vec![
                    (ListKeyword, "list"),
                    (AuthorizeKeyword, "authorize"),
                    (OnKeyword, "on"),
                    (AllKeyword, "all"),
                    (KeyspacesKeyword, "keyspaces"),
                    (OfKeyword, "of"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                LIST_DESCRIBE_ON_ALL_KEYSPACES,
                vec![
                    (ListKeyword, "list"),
                    (DescribeKeyword, "describe"),
                    (OnKeyword, "on"),
                    (AllKeyword, "all"),
                    (KeyspacesKeyword, "keyspaces"),
                    (OfKeyword, "of"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
            tokenize_expect(
                LIST_EXECUTE_ON_ALL_KEYSPACES,
                vec![
                    (ListKeyword, "list"),
                    (ExecuteKeyword, "execute"),
                    (OnKeyword, "on"),
                    (AllKeyword, "all"),
                    (KeyspacesKeyword, "keyspaces"),
                    (OfKeyword, "of"),
                    (Identifier, "big_data_user"),
                    (Semicolon, ";"),
                ],
            );
        }
    }
}

mod triggers {
    use super::*;

    mod create_trigger {
        use super::*;

        #[test]
        fn test_create_trigger() {
            tokenize_expect(
                CREATE_TRIGGER,
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
                CREATE_TRIGGER_IF_NOT_EXISTS,
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
                DROP_TRIGGER,
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
                DROP_TRIGGER_IF_EXISTS,
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
}
