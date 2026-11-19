use std::collections::HashMap;

use crate::ast::*;
use crate::parse_cql;
use crate::parser::testing::*;
use crate::test_cql::*;

use pretty_assertions::assert_eq;

#[test]
fn test_parsing_create_table_with_all_native_data_types() {
    let cql = CREATE_TABLE_WITH_ALL_NATIVE_DATA_TYPES;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Table(
            CreateTableStatement {
                if_not_exists: false,
                keyspace_name: None,
                table_name: find_token(cql, "big_data_table"),
                column_definitions: [
                    ("uuid_column", CqlNativeType::Uuid, None),
                    ("int_column", CqlNativeType::Int, None),
                    ("ascii_column", CqlNativeType::Ascii, None),
                    ("bigint_column", CqlNativeType::BigInt, None),
                    ("blob_column", CqlNativeType::Blob, None),
                    ("boolean_column", CqlNativeType::Boolean, None),
                    ("date_column", CqlNativeType::Date, None),
                    ("decimal_column", CqlNativeType::Decimal, None),
                    ("double_column", CqlNativeType::Double, None),
                    ("duration_column", CqlNativeType::Duration, None),
                    ("float_column", CqlNativeType::Float, None),
                    ("inet_column", CqlNativeType::INet, None),
                    ("smallint_column", CqlNativeType::SmallInt, None),
                    (
                        "text_column",
                        CqlNativeType::Text,
                        Some(ColumnDefinitionAttribute::PrimaryKey)
                    ),
                    ("time_column", CqlNativeType::Time, None),
                    ("timestamp_column", CqlNativeType::Timestamp, None),
                    ("timeuuid_column", CqlNativeType::TimeUuid, None),
                    ("tinyint_column", CqlNativeType::TinyInt, None),
                    ("varchar_column", CqlNativeType::VarChar, None),
                    ("varint_column", CqlNativeType::VarInt, None),
                ]
                .into_iter()
                .map(|(cn, dt, attribute)| ColumnDefinition::Column {
                    column_name: find_token(cql, cn),
                    data_type: CqlDataType::ValueType(CqlValueType::NativeType(dt)),
                    attribute,
                })
                .collect(),
                attributes: None,
                table_alias: None,
            }
        )))
    );
}

#[test]
fn test_parsing_create_table_with_counter_data_type() {
    let cql = CREATE_TABLE_WITH_COUNTER_DATA_TYPE;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Table(
            CreateTableStatement {
                if_not_exists: false,
                keyspace_name: None,
                table_name: find_token(cql, "big_data_table"),
                column_definitions: vec!(
                    ColumnDefinition::Column {
                        column_name: find_token(cql, "text_column"),
                        data_type: CqlDataType::ValueType(CqlValueType::NativeType(
                            CqlNativeType::Text
                        )),
                        attribute: Some(ColumnDefinitionAttribute::PrimaryKey),
                    },
                    ColumnDefinition::Column {
                        column_name: find_token(cql, "counter_column"),
                        data_type: CqlDataType::ValueType(CqlValueType::NativeType(
                            CqlNativeType::Counter
                        )),
                        attribute: None,
                    },
                ),
                attributes: None,
                table_alias: None,
            }
        )))
    );
}

#[test]
fn test_parsing_create_table_if_not_exists() {
    let cql = CREATE_TABLE_IF_NOT_EXISTS;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Table(
            CreateTableStatement {
                if_not_exists: true,
                keyspace_name: None,
                table_name: find_token(cql, "big_data_table"),
                column_definitions: vec!(ColumnDefinition::Column {
                    column_name: find_token(cql, "uuid_column"),
                    data_type: CqlDataType::ValueType(CqlValueType::NativeType(
                        CqlNativeType::Uuid
                    )),
                    attribute: Some(ColumnDefinitionAttribute::PrimaryKey),
                },),
                attributes: None,
                table_alias: None,
            }
        )))
    );
}

#[test]
fn test_parsing_create_table_with_explicit_keyspace() {
    let cql = CREATE_TABLE_WITH_EXPLICIT_KEYSPACE;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Table(
            CreateTableStatement {
                if_not_exists: false,
                keyspace_name: Some(find_token(cql, "big_data_keyspace")),
                table_name: find_token(cql, "big_data_table"),
                column_definitions: vec!(ColumnDefinition::Column {
                    column_name: find_token(cql, "uuid_column"),
                    data_type: CqlDataType::ValueType(CqlValueType::NativeType(
                        CqlNativeType::Uuid
                    )),
                    attribute: Some(ColumnDefinitionAttribute::PrimaryKey),
                },),
                attributes: None,
                table_alias: None,
            }
        )))
    );
}

#[test]
fn test_parsing_create_table_with_compound_primary_key() {
    let cql = CREATE_TABLE_WITH_COMPOUND_PRIMARY_KEY;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Table(
            CreateTableStatement {
                if_not_exists: false,
                keyspace_name: None,
                table_name: find_token(cql, "big_data_table"),
                column_definitions: vec!(
                    ColumnDefinition::Column {
                        column_name: find_token(cql, "text_column"),
                        data_type: CqlDataType::ValueType(CqlValueType::NativeType(
                            CqlNativeType::Text
                        )),
                        attribute: None,
                    },
                    ColumnDefinition::Column {
                        column_name: find_token(cql, "uuid_column"),
                        data_type: CqlDataType::ValueType(CqlValueType::NativeType(
                            CqlNativeType::Uuid
                        )),
                        attribute: None,
                    },
                    ColumnDefinition::PrimaryKey(PrimaryKeyDefinition::Compound {
                        partition: rfind_token(cql, "text_column"),
                        clustering: vec!(rfind_token(cql, "uuid_column")),
                    }),
                ),
                attributes: None,
                table_alias: None,
            }
        )))
    );
}

#[test]
fn test_parsing_create_table_with_composite_partition_primary_key() {
    let cql = CREATE_TABLE_WITH_COMPOSITE_PARTITION_PRIMARY_KEY;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Table(
            CreateTableStatement {
                if_not_exists: false,
                keyspace_name: None,
                table_name: find_token(cql, "big_data_table"),
                column_definitions: vec!(
                    ColumnDefinition::Column {
                        column_name: find_token(cql, "text_column"),
                        data_type: CqlDataType::ValueType(CqlValueType::NativeType(
                            CqlNativeType::Text
                        )),
                        attribute: None,
                    },
                    ColumnDefinition::Column {
                        column_name: find_token(cql, "uuid_column"),
                        data_type: CqlDataType::ValueType(CqlValueType::NativeType(
                            CqlNativeType::Uuid
                        )),
                        attribute: None,
                    },
                    ColumnDefinition::Column {
                        column_name: find_token(cql, "timestamp_column"),
                        data_type: CqlDataType::ValueType(CqlValueType::NativeType(
                            CqlNativeType::Timestamp
                        )),
                        attribute: None,
                    },
                    ColumnDefinition::PrimaryKey(PrimaryKeyDefinition::CompositePartition {
                        partition: vec!(
                            rfind_token(cql, "text_column"),
                            rfind_token(cql, "uuid_column")
                        ),
                        clustering: vec!(rfind_token(cql, "timestamp_column")),
                    }),
                ),
                attributes: None,
                table_alias: None,
            }
        )))
    );
}

#[test]
fn test_parsing_create_table_with_composite_partition_primary_key_missing_clustering_keys() {
    let cql = CREATE_TABLE_WITH_COMPOSITE_PARTITION_PRIMARY_KEY_MISSING_CLUSTERING_KEYS;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Table(
            CreateTableStatement {
                if_not_exists: false,
                keyspace_name: None,
                table_name: find_token(cql, "big_data_table"),
                column_definitions: vec!(
                    ColumnDefinition::Column {
                        column_name: find_token(cql, "text_column"),
                        data_type: CqlDataType::ValueType(CqlValueType::NativeType(
                            CqlNativeType::Text
                        )),
                        attribute: None,
                    },
                    ColumnDefinition::Column {
                        column_name: find_token(cql, "uuid_column"),
                        data_type: CqlDataType::ValueType(CqlValueType::NativeType(
                            CqlNativeType::Uuid
                        )),
                        attribute: None,
                    },
                    ColumnDefinition::Column {
                        column_name: find_token(cql, "timestamp_column"),
                        data_type: CqlDataType::ValueType(CqlValueType::NativeType(
                            CqlNativeType::Timestamp
                        )),
                        attribute: None,
                    },
                    ColumnDefinition::PrimaryKey(PrimaryKeyDefinition::CompositePartition {
                        partition: vec!(
                            rfind_token(cql, "text_column"),
                            rfind_token(cql, "uuid_column")
                        ),
                        clustering: vec!(),
                    }),
                ),
                attributes: None,
                table_alias: None,
            }
        )))
    );
}

#[test]
fn test_parsing_create_table_with_comment() {
    let cql = CREATE_TABLE_WITH_COMMENT;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Table(
            CreateTableStatement {
                if_not_exists: false,
                keyspace_name: None,
                table_name: find_token(cql, "big_data_table"),
                column_definitions: vec!(ColumnDefinition::Column {
                    column_name: find_token(cql, "uuid_column"),
                    data_type: CqlDataType::ValueType(CqlValueType::NativeType(
                        CqlNativeType::Uuid
                    )),
                    attribute: Some(ColumnDefinitionAttribute::PrimaryKey),
                },),
                attributes: Some(vec!(TableDefinitionAttribute::Comment(
                    find_string_literal(cql, "'big data!'")
                ))),
                table_alias: None,
            }
        )))
    );
}

#[test]
fn test_parsing_create_table_with_compact_storage() {
    let cql = CREATE_TABLE_WITH_COMPACT_STORAGE;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Table(
            CreateTableStatement {
                if_not_exists: false,
                keyspace_name: None,
                table_name: find_token(cql, "big_data_table"),
                column_definitions: vec!(ColumnDefinition::Column {
                    column_name: find_token(cql, "uuid_column"),
                    data_type: CqlDataType::ValueType(CqlValueType::NativeType(
                        CqlNativeType::Uuid
                    )),
                    attribute: Some(ColumnDefinitionAttribute::PrimaryKey),
                },),
                attributes: Some(vec!(TableDefinitionAttribute::CompactStorage)),
                table_alias: None,
            }
        )))
    );
}

#[test]
fn test_parsing_create_table_with_compaction() {
    let cql = CREATE_TABLE_WITH_COMPACTION;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Table(
            CreateTableStatement {
                if_not_exists: false,
                keyspace_name: None,
                table_name: find_token(cql, "big_data_table"),
                column_definitions: vec!(ColumnDefinition::Column {
                    column_name: find_token(cql, "uuid_column"),
                    data_type: CqlDataType::ValueType(CqlValueType::NativeType(
                        CqlNativeType::Uuid
                    )),
                    attribute: Some(ColumnDefinitionAttribute::PrimaryKey),
                },),
                attributes: Some(vec!(TableDefinitionAttribute::Compaction(HashMap::from([
                    (
                        find_string_literal(cql, "'class'"),
                        find_token(cql, "'LeveledCompactionStrategy'")
                    ),
                ])))),
                table_alias: None,
            }
        )))
    );
}

#[test]
fn test_parsing_create_table_with_clustering_order() {
    for (cql, clustering_order) in [
        (CREATE_TABLE_WITH_ASC_CLUSTERING_ORDER, ClusteringOrder::Asc),
        (
            CREATE_TABLE_WITH_DESC_CLUSTERING_ORDER,
            ClusteringOrder::Desc,
        ),
    ] {
        assert_eq!(
            parse_cql(cql.to_string()).unwrap(),
            vec!(CqlStatement::Create(CreateStatement::Table(
                CreateTableStatement {
                    if_not_exists: false,
                    keyspace_name: None,
                    table_name: find_token(cql, "big_data_table"),
                    column_definitions: vec!(
                        ColumnDefinition::Column {
                            column_name: find_token(cql, "text_column"),
                            data_type: CqlDataType::ValueType(CqlValueType::NativeType(
                                CqlNativeType::Text
                            )),
                            attribute: None,
                        },
                        ColumnDefinition::Column {
                            column_name: find_token(cql, "uuid_column"),
                            data_type: CqlDataType::ValueType(CqlValueType::NativeType(
                                CqlNativeType::Uuid
                            )),
                            attribute: None,
                        },
                        ColumnDefinition::Column {
                            column_name: find_token(cql, "time_column"),
                            data_type: CqlDataType::ValueType(CqlValueType::NativeType(
                                CqlNativeType::Timestamp
                            )),
                            attribute: None,
                        },
                        ColumnDefinition::PrimaryKey(PrimaryKeyDefinition::Compound {
                            partition: rfind_token(cql, "text_column"),
                            clustering: vec!(find_nth_token(cql, 1, "time_column")),
                        }),
                    ),
                    attributes: Some(vec!(TableDefinitionAttribute::ClusteringOrderBy(vec!(
                        ClusteringOrderDefinition {
                            column_name: rfind_token(cql, "time_column"),
                            order: Some(clustering_order),
                        }
                    )))),
                    table_alias: None,
                }
            )))
        );
    }
}

#[test]
fn test_parsing_create_table_with_multiple_clustering_orders() {
    let cql = CREATE_TABLE_WITH_MULTIPLE_CLUSTERING_ORDERS;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Table(
            CreateTableStatement {
                if_not_exists: false,
                keyspace_name: None,
                table_name: find_token(cql, "big_data_table"),
                column_definitions: vec!(
                    ColumnDefinition::Column {
                        column_name: find_token(cql, "text_column"),
                        data_type: CqlDataType::ValueType(CqlValueType::NativeType(
                            CqlNativeType::Text
                        )),
                        attribute: None,
                    },
                    ColumnDefinition::Column {
                        column_name: find_token(cql, "uuid_column"),
                        data_type: CqlDataType::ValueType(CqlValueType::NativeType(
                            CqlNativeType::Uuid
                        )),
                        attribute: None,
                    },
                    ColumnDefinition::Column {
                        column_name: find_token(cql, "time_column"),
                        data_type: CqlDataType::ValueType(CqlValueType::NativeType(
                            CqlNativeType::Timestamp
                        )),
                        attribute: None,
                    },
                    ColumnDefinition::PrimaryKey(PrimaryKeyDefinition::Compound {
                        partition: rfind_token(cql, "text_column"),
                        clustering: vec!(
                            find_nth_token(cql, 1, "time_column"),
                            find_nth_token(cql, 1, "uuid_column")
                        ),
                    }),
                ),
                attributes: Some(vec!(TableDefinitionAttribute::ClusteringOrderBy(vec!(
                    ClusteringOrderDefinition {
                        column_name: rfind_token(cql, "time_column"),
                        order: Some(ClusteringOrder::Desc),
                    },
                    ClusteringOrderDefinition {
                        column_name: rfind_token(cql, "uuid_column"),
                        order: Some(ClusteringOrder::Asc),
                    },
                )))),
                table_alias: None,
            }
        )))
    );
}
