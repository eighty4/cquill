use super::testing::*;
use super::*;
use TokenName::*;

mod lexing_errors {
    use super::*;

    #[test]
    fn test_unclosed_single_quote_string() {
        let cql = "CREATE KEYSPACE some_keyspace WITH replication = {'class': 'NetworkTopologyStrategy', 'DC1' : '3/1'', 'DC2' : '5/2'}";
        tokenize_expect_err(cql);
    }
}

mod comments {
    use super::*;

    #[test]
    fn test_dash_line_comment() {
        let cql = "select -- commented out \n * from big_data_table;";
        tokenize_expect(
            cql,
            vec![
                (SelectKeyword, "select"),
                (Star, "*"),
                (FromKeyword, "from"),
                (Identifier, "big_data_table"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_dash_line_comment_butts_up_against_identifier() {
        let cql = "select-- commented out \n * from big_data_table;";
        tokenize_expect(
            cql,
            vec![
                (SelectKeyword, "select"),
                (Star, "*"),
                (FromKeyword, "from"),
                (Identifier, "big_data_table"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_slash_line_comment() {
        let cql = "select // commented out \n * from big_data_table;";
        tokenize_expect(
            cql,
            vec![
                (SelectKeyword, "select"),
                (Star, "*"),
                (FromKeyword, "from"),
                (Identifier, "big_data_table"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_multiline_comment() {
        let cql = "select /* commented out \n on multiple lines */ * from big_data_table;";
        tokenize_expect(
            cql,
            vec![
                (SelectKeyword, "select"),
                (Star, "*"),
                (FromKeyword, "from"),
                (Identifier, "big_data_table"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_empty_multiline_comment() {
        let cql = "select /**/ * from big_data_table;";
        tokenize_expect(
            cql,
            vec![
                (SelectKeyword, "select"),
                (Star, "*"),
                (FromKeyword, "from"),
                (Identifier, "big_data_table"),
                (Semicolon, ";"),
            ],
        );
    }
}

mod data_types {
    use super::*;
    use crate::ast::StringStyle;

    #[test]
    fn test_digit() {
        tokenize_expect("4.2", vec![(NumberLiteral, "4.2")]);
        tokenize_expect("-1", vec![(Minus, "-"), (NumberLiteral, "1")]);
        tokenize_expect("1234", vec![(NumberLiteral, "1234")]);
        tokenize_expect("86400", vec![(NumberLiteral, "86400")]);
        tokenize_expect(
            "1234 86400",
            vec![(NumberLiteral, "1234"), (NumberLiteral, "86400")],
        );
    }

    #[test]
    fn test_decimal_does_not_misread_keyspace_qualified_table_name() {
        tokenize_expect(
            "my_keyspace.my_table",
            vec![
                (Identifier, "my_keyspace"),
                (Dot, "."),
                (Identifier, "my_table"),
            ],
        );
    }

    #[test]
    fn test_invalid_digits_decimal_and_identifier() {
        tokenize_expect("1234.my_table", vec![]);
    }

    #[test]
    fn test_single_quote_string_literal_escaping_single_quotes_with_successive_single_quotes() {
        tokenize_expect(
            "'tiffany''s breakfest'",
            vec![(
                StringLiteral(StringStyle::SingleQuote),
                "'tiffany''s breakfest'",
            )],
        );
    }

    #[test]
    fn test_triple_quote_string() {
        tokenize_expect(
            "'''tiffany's breakfest'''",
            vec![(
                StringLiteral(StringStyle::TripleQuote),
                "'''tiffany's breakfest'''",
            )],
        );
    }

    #[test]
    fn test_blob_literal() {
        tokenize_expect("0xaaaa", vec![(BlobLiteral, "0xaaaa")]);
        tokenize_expect("0xo", vec![]);
    }

    #[test]
    fn test_uuid_literal() {
        tokenize_expect(
            "89b7aa7a-8776-460b-8e1a-60cb4bcd523c",
            vec![(UuidLiteral, "89b7aa7a-8776-460b-8e1a-60cb4bcd523c")],
        );
        tokenize_expect(
            "89B7AA7A-8776-460B-8E1A-60CB4BCD523C",
            vec![(UuidLiteral, "89B7AA7A-8776-460B-8E1A-60CB4BCD523C")],
        );
    }
}
