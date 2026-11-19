use crate::ast::*;
use crate::parse_cql;
use crate::parser::testing::*;
use crate::test_cql::*;

use pretty_assertions::assert_eq;

#[test]
fn test_parsing_drop_user() {
    assert_eq!(
        parse_cql(DROP_USER.to_string()).unwrap(),
        vec!(CqlStatement::Drop(DropStatement::User(DropUserStatement {
            user_name: find_token(DROP_USER, "big_data_user"),
            if_exists: false,
        })))
    );
}

#[test]
fn test_parsing_drop_user_if_exists() {
    assert_eq!(
        parse_cql(DROP_USER_IF_EXISTS.to_string()).unwrap(),
        vec!(CqlStatement::Drop(DropStatement::User(DropUserStatement {
            user_name: find_token(DROP_USER_IF_EXISTS, "big_data_user"),
            if_exists: true,
        })))
    );
}
