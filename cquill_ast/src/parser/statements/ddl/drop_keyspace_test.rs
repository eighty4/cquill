use crate::ast::*;
use crate::parse_cql;
use crate::parser::testing::*;
use crate::test_cql::*;

use pretty_assertions::assert_eq;

#[test]
fn test_parsing_drop_keyspace() {
    assert_eq!(
        parse_cql(DROP_KEYSPACE.to_string()).unwrap(),
        vec!(CqlStatement::Drop(DropStatement::Keyspace(
            DropKeyspaceStatement {
                keyspace_name: find_token(DROP_KEYSPACE, "big_data_keyspace"),
                if_exists: false,
            }
        )))
    );
}

#[test]
fn test_parsing_drop_keyspace_if_exists() {
    assert_eq!(
        parse_cql(DROP_KEYSPACE_IF_EXISTS.to_string()).unwrap(),
        vec!(CqlStatement::Drop(DropStatement::Keyspace(
            DropKeyspaceStatement {
                keyspace_name: find_token(DROP_KEYSPACE_IF_EXISTS, "big_data_keyspace"),
                if_exists: true,
            }
        )))
    );
}
