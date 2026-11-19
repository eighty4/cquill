use crate::lex::*;

pub fn tokenize_expect(cql: &'static str, expected: Vec<(TokenName, &str)>) {
    let result = Tokenizer::new(cql).tokenize().unwrap();
    assert_eq!(result.len(), expected.len());
    for i in 0..expected.len() {
        assert_eq!(result[i].name, expected[i].0);
        assert_eq!(result[i].range.splice(cql), expected[i].1);
    }
}

pub fn tokenize_expect_err(cql: &'static str) {
    assert!(Tokenizer::new(cql).tokenize().is_err());
}
