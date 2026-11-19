use crate::ast::*;
use crate::lex::TokenName::*;
use crate::lex::testing::tokenize_expect;
use crate::test_cql::*;

mod create_function {
    use super::*;

    #[test]
    fn test_create_function_with_string_literal() {
        tokenize_expect(
            CREATE_FUNCTION_CALLED_ON_NULL_AS_SINGLE_QUOTE_STRING,
            vec![
                (CreateKeyword, "create"),
                (FunctionKeyword, "function"),
                (Identifier, "big_data_fn"),
                (LeftParenthesis, "("),
                (Identifier, "fn_arg"),
                (IntKeyword, "int"),
                (RightParenthesis, ")"),
                (CalledKeyword, "called"),
                (OnKeyword, "on"),
                (NullKeyword, "null"),
                (InputKeyword, "input"),
                (ReturnsKeyword, "returns"),
                (TextKeyword, "text"),
                (LanguageKeyword, "language"),
                (Identifier, "java"),
                (AsKeyword, "as"),
                (
                    StringLiteral(StringStyle::SingleQuote),
                    "'return fn_arg.toString();'",
                ),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_create_function_called_on_null_input() {
        tokenize_expect(
            CREATE_FUNCTION_CALLED_ON_NULL_AS_SINGLE_QUOTE_STRING,
            vec![
                (CreateKeyword, "create"),
                (FunctionKeyword, "function"),
                (Identifier, "big_data_fn"),
                (LeftParenthesis, "("),
                (Identifier, "fn_arg"),
                (IntKeyword, "int"),
                (RightParenthesis, ")"),
                (CalledKeyword, "called"),
                (OnKeyword, "on"),
                (NullKeyword, "null"),
                (InputKeyword, "input"),
                (ReturnsKeyword, "returns"),
                (TextKeyword, "text"),
                (LanguageKeyword, "language"),
                (Identifier, "java"),
                (AsKeyword, "as"),
                (
                    StringLiteral(StringStyle::SingleQuote),
                    "'return fn_arg.toString();'",
                ),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_create_function_returns_null_on_null_input() {
        tokenize_expect(
            CREATE_FUNCTION_RETURNS_NULL_ON_NULL_INPUT,
            vec![
                (CreateKeyword, "create"),
                (FunctionKeyword, "function"),
                (Identifier, "big_data_fn"),
                (LeftParenthesis, "("),
                (Identifier, "fn_arg"),
                (IntKeyword, "int"),
                (RightParenthesis, ")"),
                (ReturnsKeyword, "returns"),
                (NullKeyword, "null"),
                (OnKeyword, "on"),
                (NullKeyword, "null"),
                (InputKeyword, "input"),
                (ReturnsKeyword, "returns"),
                (TextKeyword, "text"),
                (LanguageKeyword, "language"),
                (Identifier, "java"),
                (AsKeyword, "as"),
                (
                    StringLiteral(StringStyle::DollarSign),
                    "$$\n        return fn_arg.toString();\n    $$",
                ),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_create_or_replace_function() {
        tokenize_expect(
            CREATE_OR_REPLACE_FUNCTION,
            vec![
                (CreateKeyword, "create"),
                (OrKeyword, "or"),
                (ReplaceKeyword, "replace"),
                (FunctionKeyword, "function"),
                (Identifier, "big_data_fn"),
                (LeftParenthesis, "("),
                (Identifier, "fn_arg"),
                (IntKeyword, "int"),
                (RightParenthesis, ")"),
                (CalledKeyword, "called"),
                (OnKeyword, "on"),
                (NullKeyword, "null"),
                (InputKeyword, "input"),
                (ReturnsKeyword, "returns"),
                (TextKeyword, "text"),
                (LanguageKeyword, "language"),
                (Identifier, "java"),
                (AsKeyword, "as"),
                (
                    StringLiteral(StringStyle::DollarSign),
                    "$$\n        return fn_arg.toString();\n    $$",
                ),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_create_function_if_not_exists() {
        tokenize_expect(
            CREATE_FUNCTION_IF_NOT_EXISTS,
            vec![
                (CreateKeyword, "create"),
                (FunctionKeyword, "function"),
                (IfKeyword, "if"),
                (NotKeyword, "not"),
                (ExistsKeyword, "exists"),
                (Identifier, "big_data_fn"),
                (LeftParenthesis, "("),
                (Identifier, "fn_arg"),
                (IntKeyword, "int"),
                (RightParenthesis, ")"),
                (CalledKeyword, "called"),
                (OnKeyword, "on"),
                (NullKeyword, "null"),
                (InputKeyword, "input"),
                (ReturnsKeyword, "returns"),
                (TextKeyword, "text"),
                (LanguageKeyword, "language"),
                (Identifier, "java"),
                (AsKeyword, "as"),
                (
                    StringLiteral(StringStyle::DollarSign),
                    "$$\n        return fn_arg.toString();\n    $$",
                ),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_create_function_with_multiple_args() {
        tokenize_expect(
            CREATE_FUNCTION_WITH_MULTIPLE_ARGS,
            vec![
                (CreateKeyword, "create"),
                (FunctionKeyword, "function"),
                (Identifier, "big_data_fn"),
                (LeftParenthesis, "("),
                (Identifier, "fn_arg1"),
                (IntKeyword, "int"),
                (Comma, ","),
                (Identifier, "fn_arg2"),
                (TextKeyword, "text"),
                (RightParenthesis, ")"),
                (CalledKeyword, "called"),
                (OnKeyword, "on"),
                (NullKeyword, "null"),
                (InputKeyword, "input"),
                (ReturnsKeyword, "returns"),
                (TextKeyword, "text"),
                (LanguageKeyword, "language"),
                (Identifier, "java"),
                (AsKeyword, "as"),
                (
                    StringLiteral(StringStyle::DollarSign),
                    "$$\n        return fn_arg1.toString();\n    $$",
                ),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_create_function_with_frozen_arg() {
        tokenize_expect(
            CREATE_FUNCTION_WITH_FROZEN_UDT_ARG,
            vec![
                (CreateKeyword, "create"),
                (FunctionKeyword, "function"),
                (Identifier, "big_data_fn"),
                (LeftParenthesis, "("),
                (Identifier, "fn_arg"),
                (FrozenKeyword, "frozen"),
                (LessThan, "<"),
                (Identifier, "some_udt"),
                (GreaterThan, ">"),
                (RightParenthesis, ")"),
                (CalledKeyword, "called"),
                (OnKeyword, "on"),
                (NullKeyword, "null"),
                (InputKeyword, "input"),
                (ReturnsKeyword, "returns"),
                (TextKeyword, "text"),
                (LanguageKeyword, "language"),
                (Identifier, "java"),
                (AsKeyword, "as"),
                (
                    StringLiteral(StringStyle::DollarSign),
                    "$$\n        return fn_arg.toString();\n    $$",
                ),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_create_function_returns_user_defined_type() {
        tokenize_expect(
            CREATE_FUNCTION_RETURNS_USER_DEFINED_TYPE,
            vec![
                (CreateKeyword, "create"),
                (FunctionKeyword, "function"),
                (Identifier, "big_data_fn"),
                (LeftParenthesis, "("),
                (Identifier, "fn_arg"),
                (IntKeyword, "int"),
                (RightParenthesis, ")"),
                (CalledKeyword, "called"),
                (OnKeyword, "on"),
                (NullKeyword, "null"),
                (InputKeyword, "input"),
                (ReturnsKeyword, "returns"),
                (Identifier, "some_udt"),
                (LanguageKeyword, "language"),
                (Identifier, "java"),
                (AsKeyword, "as"),
                (
                    StringLiteral(StringStyle::DollarSign),
                    "$$\n        return fn_arg.toString();\n    $$",
                ),
                (Semicolon, ";"),
            ],
        );
    }
}

mod drop_function {
    use super::*;

    #[test]
    fn test_drop_function_without_args() {
        tokenize_expect(
            DROP_FUNCTION_WITHOUT_ARGS,
            vec![
                (DropKeyword, "drop"),
                (FunctionKeyword, "function"),
                (Identifier, "big_data_fn"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_drop_function_with_explicit_keyspace() {
        tokenize_expect(
            DROP_FUNCTION_WITH_EXPLICIT_KEYSPACE,
            vec![
                (DropKeyword, "drop"),
                (FunctionKeyword, "function"),
                (Identifier, "big_data_keyspace"),
                (Dot, "."),
                (Identifier, "big_data_fn"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_drop_function_with_single_arg() {
        tokenize_expect(
            DROP_FUNCTION_WITH_SINGLE_ARG,
            vec![
                (DropKeyword, "drop"),
                (FunctionKeyword, "function"),
                (Identifier, "big_data_fn"),
                (LeftParenthesis, "("),
                (IntKeyword, "int"),
                (RightParenthesis, ")"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_drop_function_with_multiple_args() {
        tokenize_expect(
            DROP_FUNCTION_WITH_MULTIPLE_ARGS,
            vec![
                (DropKeyword, "drop"),
                (FunctionKeyword, "function"),
                (Identifier, "big_data_fn"),
                (LeftParenthesis, "("),
                (IntKeyword, "int"),
                (Comma, ","),
                (TextKeyword, "text"),
                (RightParenthesis, ")"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_drop_function_and_the_kitchen_sink() {
        tokenize_expect(
            DROP_FUNCTION_WITH_EXPLICIT_KEYSPACE_AND_MULTIPLE_ARGS,
            vec![
                (DropKeyword, "drop"),
                (FunctionKeyword, "function"),
                (Identifier, "big_data_keyspace"),
                (Dot, "."),
                (Identifier, "big_data_fn"),
                (LeftParenthesis, "("),
                (IntKeyword, "int"),
                (Comma, ","),
                (TextKeyword, "text"),
                (RightParenthesis, ")"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_drop_function_if_exists() {
        tokenize_expect(
            DROP_FUNCTION_IF_EXISTS,
            vec![
                (DropKeyword, "drop"),
                (FunctionKeyword, "function"),
                (IfKeyword, "if"),
                (ExistsKeyword, "exists"),
                (Identifier, "big_data_fn"),
                (Semicolon, ";"),
            ],
        );
    }
}

mod create_aggregate {
    use super::*;

    #[test]
    fn test_create_aggregate_with_single_arg() {
        tokenize_expect(
            CREATE_AGGREGATE_WITH_COLLECTION_STYPE,
            vec![
                (CreateKeyword, "create"),
                (AggregateKeyword, "aggregate"),
                (Identifier, "big_data_agg"),
                (LeftParenthesis, "("),
                (IntKeyword, "int"),
                (RightParenthesis, ")"),
                (SFuncKeyword, "sfunc"),
                (Identifier, "fn_name"),
                (STypeKeyword, "stype"),
                (ListKeyword, "list"),
                (LessThan, "<"),
                (TextKeyword, "text"),
                (GreaterThan, ">"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_create_aggregate_with_udt_stype() {
        tokenize_expect(
            CREATE_AGGREGATE_WITH_UDT_STYPE,
            vec![
                (CreateKeyword, "create"),
                (AggregateKeyword, "aggregate"),
                (Identifier, "big_data_agg"),
                (LeftParenthesis, "("),
                (IntKeyword, "int"),
                (RightParenthesis, ")"),
                (SFuncKeyword, "sfunc"),
                (Identifier, "fn_name"),
                (STypeKeyword, "stype"),
                (Identifier, "some_udt"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_create_or_replace_aggregate() {
        tokenize_expect(
            CREATE_OR_REPLACE_AGGREGATE,
            vec![
                (CreateKeyword, "create"),
                (OrKeyword, "or"),
                (ReplaceKeyword, "replace"),
                (AggregateKeyword, "aggregate"),
                (Identifier, "big_data_agg"),
                (LeftParenthesis, "("),
                (IntKeyword, "int"),
                (RightParenthesis, ")"),
                (SFuncKeyword, "sfunc"),
                (Identifier, "fn_name"),
                (STypeKeyword, "stype"),
                (ListKeyword, "list"),
                (LessThan, "<"),
                (TextKeyword, "text"),
                (GreaterThan, ">"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_create_aggregate_if_not_exists() {
        tokenize_expect(
            CREATE_AGGREGATE_IF_NOT_EXISTS,
            vec![
                (CreateKeyword, "create"),
                (AggregateKeyword, "aggregate"),
                (IfKeyword, "if"),
                (NotKeyword, "not"),
                (ExistsKeyword, "exists"),
                (Identifier, "big_data_agg"),
                (LeftParenthesis, "("),
                (IntKeyword, "int"),
                (RightParenthesis, ")"),
                (SFuncKeyword, "sfunc"),
                (Identifier, "fn_name"),
                (STypeKeyword, "stype"),
                (ListKeyword, "list"),
                (LessThan, "<"),
                (TextKeyword, "text"),
                (GreaterThan, ">"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_create_aggregate_with_finalfunc() {
        tokenize_expect(
            CREATE_AGGREGATE_WITH_FINALFUNC,
            vec![
                (CreateKeyword, "create"),
                (AggregateKeyword, "aggregate"),
                (Identifier, "big_data_agg"),
                (LeftParenthesis, "("),
                (IntKeyword, "int"),
                (RightParenthesis, ")"),
                (SFuncKeyword, "sfunc"),
                (Identifier, "fn_name"),
                (STypeKeyword, "stype"),
                (ListKeyword, "list"),
                (LessThan, "<"),
                (TextKeyword, "text"),
                (GreaterThan, ">"),
                (FinalFuncKeyword, "finalfunc"),
                (Identifier, "ffn_name"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_create_aggregate_with_initcond() {
        tokenize_expect(
            CREATE_AGGREGATE_WITH_INITCOND,
            vec![
                (CreateKeyword, "create"),
                (AggregateKeyword, "aggregate"),
                (Identifier, "big_data_agg"),
                (LeftParenthesis, "("),
                (IntKeyword, "int"),
                (RightParenthesis, ")"),
                (SFuncKeyword, "sfunc"),
                (Identifier, "fn_name"),
                (STypeKeyword, "stype"),
                (ListKeyword, "list"),
                (LessThan, "<"),
                (TextKeyword, "text"),
                (GreaterThan, ">"),
                (InitCondKeyword, "initcond"),
                (StringLiteral(StringStyle::SingleQuote), "'state value'"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_create_aggregate_with_finalfunc_and_initcond() {
        tokenize_expect(
            CREATE_AGGREGATE_WITH_FINALFUNC_AND_INITCOND,
            vec![
                (CreateKeyword, "create"),
                (AggregateKeyword, "aggregate"),
                (Identifier, "big_data_agg"),
                (LeftParenthesis, "("),
                (IntKeyword, "int"),
                (RightParenthesis, ")"),
                (SFuncKeyword, "sfunc"),
                (Identifier, "fn_name"),
                (STypeKeyword, "stype"),
                (ListKeyword, "list"),
                (LessThan, "<"),
                (TextKeyword, "text"),
                (GreaterThan, ">"),
                (FinalFuncKeyword, "finalfunc"),
                (Identifier, "ffn_name"),
                (InitCondKeyword, "initcond"),
                (StringLiteral(StringStyle::SingleQuote), "'state value'"),
                (Semicolon, ";"),
            ],
        );
    }
}

mod drop_aggregate {
    use super::*;

    #[test]
    fn test_drop_aggregate() {
        tokenize_expect(
            DROP_AGGREGATE_WITHOUT_ARGS,
            vec![
                (DropKeyword, "drop"),
                (AggregateKeyword, "aggregate"),
                (Identifier, "big_data_agg"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_drop_aggregate_with_explicit_keyspace() {
        tokenize_expect(
            DROP_AGGREGATE_WITH_EXPLICIT_KEYSPACE,
            vec![
                (DropKeyword, "drop"),
                (AggregateKeyword, "aggregate"),
                (Identifier, "big_data_keyspace"),
                (Dot, "."),
                (Identifier, "big_data_agg"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_drop_aggregate_with_single_arg() {
        tokenize_expect(
            DROP_AGGREGATE_WITH_SINGLE_ARG,
            vec![
                (DropKeyword, "drop"),
                (AggregateKeyword, "aggregate"),
                (Identifier, "big_data_agg"),
                (LeftParenthesis, "("),
                (IntKeyword, "int"),
                (RightParenthesis, ")"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_drop_aggregate_with_multiple_args() {
        tokenize_expect(
            DROP_AGGREGATE_WITH_MULTIPLE_ARGS,
            vec![
                (DropKeyword, "drop"),
                (AggregateKeyword, "aggregate"),
                (Identifier, "big_data_agg"),
                (LeftParenthesis, "("),
                (IntKeyword, "int"),
                (Comma, ","),
                (TextKeyword, "text"),
                (RightParenthesis, ")"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_drop_aggregate_and_the_kitchen_sink() {
        tokenize_expect(
            DROP_AGGREGATE_WITH_EXPLICIT_KEYSPACE_AND_MULTIPLE_ARGS,
            vec![
                (DropKeyword, "drop"),
                (AggregateKeyword, "aggregate"),
                (Identifier, "big_data_keyspace"),
                (Dot, "."),
                (Identifier, "big_data_agg"),
                (LeftParenthesis, "("),
                (IntKeyword, "int"),
                (Comma, ","),
                (TextKeyword, "text"),
                (RightParenthesis, ")"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_drop_aggregate_if_exists() {
        tokenize_expect(
            DROP_AGGREGATE_IF_EXISTS,
            vec![
                (DropKeyword, "drop"),
                (AggregateKeyword, "aggregate"),
                (IfKeyword, "if"),
                (ExistsKeyword, "exists"),
                (Identifier, "big_data_agg"),
                (Semicolon, ";"),
            ],
        );
    }
}
