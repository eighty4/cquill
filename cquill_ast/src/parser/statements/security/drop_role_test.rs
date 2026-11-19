use crate::ast::*;
use crate::parse_cql;
use crate::parser::testing::*;
use crate::test_cql::*;

use pretty_assertions::assert_eq;

#[test]
fn test_parsing_drop_role() {
    assert_eq!(
        parse_cql(DROP_ROLE.to_string()).unwrap(),
        vec!(CqlStatement::Drop(DropStatement::Role(DropRoleStatement {
            role_name: find_token(DROP_ROLE, "big_data_role"),
            if_exists: false,
        })))
    );
}

#[test]
fn test_parsing_drop_role_if_exists() {
    assert_eq!(
        parse_cql(DROP_ROLE_IF_EXISTS.to_string()).unwrap(),
        vec!(CqlStatement::Drop(DropStatement::Role(DropRoleStatement {
            role_name: find_token(DROP_ROLE_IF_EXISTS, "big_data_role"),
            if_exists: true,
        })))
    );
}
