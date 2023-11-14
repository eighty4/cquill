use super::lex::*;
use TokenName::*;

fn tokenize_expect(cql: &'static str, expected: Vec<(TokenName, &str)>) {
    let result = Tokenizer::new(cql).tokenize().unwrap();
    assert_eq!(result.len(), expected.len());
    for i in 0..expected.len() {
        assert_eq!(result[i].name, expected[i].0);
        assert_eq!(result[i].range.splice(cql), expected[i].1);
    }
}

#[test]
fn test_tokenize_use_keyspace_statement() {
    tokenize_expect(
        "use big_data_keyspace;",
        vec![
            (UseKeyword, "use"),
            (Identifier, "big_data_keyspace"),
            (Semicolon, ";"),
        ],
    );
}

#[test]
fn test_create_keyspace_statement() {
    let cql = "create keyspace big_data_keyspace with replication = {'class': 'SimpleStrategy', 'replication_factor': 1};";
    tokenize_expect(
        cql,
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
            (Semicolon, ";"),
        ],
    );
}

#[test]
fn test_create_table_statement() {
    let cql = "create table big_data_keyspace.big_data_table
(
    ascii_column   ascii,
    bigint_column  bigint,
    blob_column    blob,
    boolean_column boolean,
    counter_column counter,
    date_column    date,
    decimal_column decimal,
    double_column  double,
    duration_column duration,
    float_column    float,
    inet_column    inet,
    int_column     int,
    smallint_column smallint,
    text_column    text,
    time_column    time,
    timestamp_column timestamp,
    timeuuid_column  timeuuid,
    tinyint_column tinyint,
    uuid_column    uuid,
    varchar_column varchar,
    varint_column varint,
    primary key (text_column)
);";
    tokenize_expect(
        cql,
        vec![
            (CreateKeyword, "create"),
            (TableKeyword, "table"),
            (Identifier, "big_data_keyspace"),
            (Dot, "."),
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
fn test_create_table_primary_key_as_attribute_statement() {
    let cql = "create table big_data_table (uuid_column uuid primary key);";
    tokenize_expect(
        cql,
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
            (Semicolon, ";"),
        ],
    );
}

#[test]
fn test_create_table_with_clustering_order_desc_statement() {
    let cql = "create table big_data_keyspace.big_data_table
(
    text_column text,
    uuid_column uuid,
    time_column timeuuid,
    primary key (text_column, time_column)
) with clustering order by (time_column desc);";
    tokenize_expect(
        cql,
        vec![
            (CreateKeyword, "create"),
            (TableKeyword, "table"),
            (Identifier, "big_data_keyspace"),
            (Dot, "."),
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
    let cql = "create table big_data_keyspace.big_data_table
(
    text_column text,
    uuid_column uuid,
    time_column timeuuid,
    primary key (text_column, time_column)
) with clustering order by (time_column asc);";
    tokenize_expect(
        cql,
        vec![
            (CreateKeyword, "create"),
            (TableKeyword, "table"),
            (Identifier, "big_data_keyspace"),
            (Dot, "."),
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
