use crate::ast::*;
use crate::parse_cql;
use crate::parser::testing::*;
use crate::test_cql::*;

use pretty_assertions::assert_eq;

#[test]
fn test_create_function_called_on_null_as_single_quote_string() {
    let cql = CREATE_FUNCTION_CALLED_ON_NULL_AS_SINGLE_QUOTE_STRING;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Function(
            CreateFunctionStatement {
                function_body: find_string_literal(cql, "'return fn_arg.toString();'"),
                returns: CqlDataType::ValueType(CqlValueType::NativeType(CqlNativeType::Text)),
                function_name: find_token(cql, "big_data_fn"),
                if_exists_behavior: CreateIfExistsBehavior::Error,
                language: find_token(cql, "java"),
                on_null_input: OnNullInput::Called,
                function_args: vec!((
                    find_token(cql, "fn_arg"),
                    CqlDataType::ValueType(CqlValueType::NativeType(CqlNativeType::Int)),
                )),
            }
        )))
    );
}

#[test]
fn test_create_function_called_on_null_as_dollar_dollar() {
    let cql = CREATE_FUNCTION_CALLED_ON_NULL_AS_DOLLAR_DOLLAR;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Function(
            CreateFunctionStatement {
                function_body: find_string_literal(
                    cql,
                    "$$\n        return fn_arg.toString();\n    $$"
                ),
                returns: CqlDataType::ValueType(CqlValueType::NativeType(CqlNativeType::Text)),
                function_name: find_token(cql, "big_data_fn"),
                if_exists_behavior: CreateIfExistsBehavior::Error,
                language: find_token(cql, "java"),
                on_null_input: OnNullInput::Called,
                function_args: vec![(
                    find_token(cql, "fn_arg"),
                    CqlDataType::ValueType(CqlValueType::NativeType(CqlNativeType::Int)),
                )],
            }
        )))
    );
}

#[test]
fn test_create_function_returns_null_on_null() {
    let cql = CREATE_FUNCTION_RETURNS_NULL_ON_NULL_INPUT;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Function(
            CreateFunctionStatement {
                function_body: find_string_literal(
                    cql,
                    "$$\n        return fn_arg.toString();\n    $$"
                ),
                returns: CqlDataType::ValueType(CqlValueType::NativeType(CqlNativeType::Text)),
                function_name: find_token(cql, "big_data_fn"),
                if_exists_behavior: CreateIfExistsBehavior::Error,
                language: find_token(cql, "java"),
                on_null_input: OnNullInput::ReturnsNull,
                function_args: vec![(
                    find_token(cql, "fn_arg"),
                    CqlDataType::ValueType(CqlValueType::NativeType(CqlNativeType::Int)),
                )],
            }
        )))
    );
}

#[test]
fn test_create_function_or_replace() {
    let cql = CREATE_OR_REPLACE_FUNCTION;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Function(
            CreateFunctionStatement {
                function_body: find_string_literal(
                    cql,
                    "$$\n        return fn_arg.toString();\n    $$"
                ),
                returns: CqlDataType::ValueType(CqlValueType::NativeType(CqlNativeType::Text)),
                function_name: find_token(cql, "big_data_fn"),
                if_exists_behavior: CreateIfExistsBehavior::Replace,
                language: find_token(cql, "java"),
                on_null_input: OnNullInput::Called,
                function_args: vec![(
                    find_token(cql, "fn_arg"),
                    CqlDataType::ValueType(CqlValueType::NativeType(CqlNativeType::Int)),
                )],
            }
        )))
    );
}

#[test]
fn test_create_function_if_not_exists() {
    let cql = CREATE_FUNCTION_IF_NOT_EXISTS;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Function(
            CreateFunctionStatement {
                function_body: find_string_literal(
                    cql,
                    "$$\n        return fn_arg.toString();\n    $$"
                ),
                returns: CqlDataType::ValueType(CqlValueType::NativeType(CqlNativeType::Text)),
                function_name: find_token(cql, "big_data_fn"),
                if_exists_behavior: CreateIfExistsBehavior::DoNotError,
                language: find_token(cql, "java"),
                on_null_input: OnNullInput::Called,
                function_args: vec![(
                    find_token(cql, "fn_arg"),
                    CqlDataType::ValueType(CqlValueType::NativeType(CqlNativeType::Int)),
                )],
            }
        )))
    );
}

#[test]
fn test_create_function_with_multiple_args() {
    let cql = CREATE_FUNCTION_WITH_MULTIPLE_ARGS;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Function(
            CreateFunctionStatement {
                if_exists_behavior: CreateIfExistsBehavior::Error,
                function_name: find_token(cql, "big_data_fn"),
                function_args: vec!(
                    (
                        find_token(cql, "fn_arg1"),
                        CqlDataType::ValueType(CqlValueType::NativeType(CqlNativeType::Int)),
                    ),
                    (
                        find_token(cql, "fn_arg2"),
                        CqlDataType::ValueType(CqlValueType::NativeType(CqlNativeType::Text)),
                    )
                ),
                on_null_input: OnNullInput::Called,
                returns: CqlDataType::ValueType(CqlValueType::NativeType(CqlNativeType::Text)),
                language: find_token(cql, "java"),
                function_body: find_string_literal(
                    cql,
                    "$$\n        return fn_arg1.toString();\n    $$"
                ),
            }
        )))
    );
}

#[test]
fn test_create_function_with_frozen_udt_arg() {
    let cql = CREATE_FUNCTION_WITH_FROZEN_UDT_ARG;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Function(
            CreateFunctionStatement {
                if_exists_behavior: CreateIfExistsBehavior::Error,
                function_name: find_token(cql, "big_data_fn"),
                function_args: vec!((
                    find_token(cql, "fn_arg"),
                    CqlDataType::Frozen(Box::new(CqlDataType::ValueType(
                        CqlValueType::UserDefinedType(find_token(cql, "some_udt"))
                    ))),
                )),
                on_null_input: OnNullInput::Called,
                returns: CqlDataType::ValueType(CqlValueType::NativeType(CqlNativeType::Text)),
                language: find_token(cql, "java"),
                function_body: find_string_literal(
                    cql,
                    "$$\n        return fn_arg.toString();\n    $$"
                ),
            }
        )))
    );
}

#[test]
fn test_create_function_returns_udt() {
    let cql = CREATE_FUNCTION_RETURNS_USER_DEFINED_TYPE;
    assert_eq!(
        parse_cql(cql.to_string()).unwrap(),
        vec!(CqlStatement::Create(CreateStatement::Function(
            CreateFunctionStatement {
                if_exists_behavior: CreateIfExistsBehavior::Error,
                function_name: find_token(cql, "big_data_fn"),
                function_args: vec!((
                    find_token(cql, "fn_arg"),
                    CqlDataType::ValueType(CqlValueType::NativeType(CqlNativeType::Int)),
                )),
                on_null_input: OnNullInput::Called,
                returns: CqlDataType::ValueType(CqlValueType::UserDefinedType(find_token(
                    cql, "some_udt"
                ))),
                language: find_token(cql, "java"),
                function_body: find_string_literal(
                    cql,
                    "$$\n        return fn_arg.toString();\n    $$"
                ),
            }
        )))
    );
}
