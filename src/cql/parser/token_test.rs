use crate::cql::ast::DropStatement;
use crate::cql::test_cql::DROP_KEYSPACE;

#[test]
fn test_token_view() {
    let ast = crate::cql::parser::parse_cql(DROP_KEYSPACE.to_string()).unwrap();
    assert_eq!(
        match ast.first() {
            Some(crate::cql::ast::CqlStatement::Drop(DropStatement::Keyspace(dks))) =>
                dks.keyspace_name.value(),
            _ => panic!(),
        },
        "big_data_keyspace".to_string()
    );
}
