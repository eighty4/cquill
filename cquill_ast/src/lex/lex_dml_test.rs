use crate::ast::*;
use crate::lex::TokenName::*;
use crate::lex::testing::tokenize_expect;
use crate::test_cql::*;

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
