use crate::cql::parser::parse_cql;

#[test]
fn test_parser() {
    let cql = r#"
CREATE TABLE cycling.race_winners (
race_name text, 
race_position int, 
cyclist_name FROZEN<fullname>, 
PRIMARY KEY (race_name, race_position));"#;
    let ast = parse_cql(cql.to_string());
}
