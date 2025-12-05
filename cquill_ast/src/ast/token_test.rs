use crate::ast::DropStatement;
use crate::parse_cql;
use crate::test_cql::DROP_KEYSPACE;

#[test]
fn test_token_view() {
    let ast = parse_cql(DROP_KEYSPACE.to_string()).unwrap();
    assert_eq!(
        match ast.first() {
            Some(crate::ast::CqlStatement::Drop(DropStatement::Keyspace(dks))) =>
                dks.keyspace_name.value(),
            _ => panic!(),
        },
        "big_data_keyspace".to_string()
    );
}
