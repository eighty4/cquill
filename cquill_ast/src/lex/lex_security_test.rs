use crate::ast::*;
use crate::lex::TokenName::*;
use crate::lex::testing::tokenize_expect;
use crate::test_cql::*;

mod create_role {
    use super::*;

    #[test]
    fn test_create_role() {
        tokenize_expect(
            CREATE_ROLE_WITH_PASSWORD,
            vec![
                (CreateKeyword, "create"),
                (RoleKeyword, "role"),
                (Identifier, "big_data_role"),
                (WithKeyword, "with"),
                (PasswordKeyword, "password"),
                (Equal, "="),
                (StringLiteral(StringStyle::SingleQuote), "'asdf'"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            CREATE_ROLE_WITH_HASHED_PASSWORD,
            vec![
                (CreateKeyword, "create"),
                (RoleKeyword, "role"),
                (Identifier, "big_data_role"),
                (WithKeyword, "with"),
                (HashedKeyword, "hashed"),
                (PasswordKeyword, "password"),
                (Equal, "="),
                (StringLiteral(StringStyle::SingleQuote), "'aassddff'"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            CREATE_ROLE_WITH_LOGIN_TRUE,
            vec![
                (CreateKeyword, "create"),
                (RoleKeyword, "role"),
                (Identifier, "big_data_role"),
                (WithKeyword, "with"),
                (LoginKeyword, "login"),
                (Equal, "="),
                (TrueKeyword, "true"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            CREATE_ROLE_WITH_LOGIN_FALSE,
            vec![
                (CreateKeyword, "create"),
                (RoleKeyword, "role"),
                (Identifier, "big_data_role"),
                (WithKeyword, "with"),
                (LoginKeyword, "login"),
                (Equal, "="),
                (FalseKeyword, "false"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            CREATE_ROLE_WITH_SUPERUSER_TRUE,
            vec![
                (CreateKeyword, "create"),
                (RoleKeyword, "role"),
                (Identifier, "big_data_role"),
                (WithKeyword, "with"),
                (SuperUserKeyword, "superuser"),
                (Equal, "="),
                (TrueKeyword, "true"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            CREATE_ROLE_WITH_SUPERUSER_FALSE,
            vec![
                (CreateKeyword, "create"),
                (RoleKeyword, "role"),
                (Identifier, "big_data_role"),
                (WithKeyword, "with"),
                (SuperUserKeyword, "superuser"),
                (Equal, "="),
                (FalseKeyword, "false"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            CREATE_ROLE_WITH_OPTIONS_MAP,
            vec![
                (CreateKeyword, "create"),
                (RoleKeyword, "role"),
                (Identifier, "big_data_role"),
                (WithKeyword, "with"),
                (OptionsKeyword, "options"),
                (Equal, "="),
                (LeftCurvedBracket, "{"),
                (StringLiteral(StringStyle::SingleQuote), "'opt1'"),
                (Colon, ":"),
                (StringLiteral(StringStyle::SingleQuote), "'val'"),
                (Comma, ","),
                (StringLiteral(StringStyle::SingleQuote), "'opt2'"),
                (Colon, ":"),
                (NumberLiteral, "99"),
                (RightCurvedBracket, "}"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            CREATE_ROLE_WITH_ACCESS_TO_DATACENTERS_SET,
            vec![
                (CreateKeyword, "create"),
                (RoleKeyword, "role"),
                (Identifier, "big_data_role"),
                (WithKeyword, "with"),
                (AccessKeyword, "access"),
                (ToKeyword, "to"),
                (DatacentersKeyword, "datacenters"),
                (LeftCurvedBracket, "{"),
                (StringLiteral(StringStyle::SingleQuote), "'dc1'"),
                (Comma, ","),
                (StringLiteral(StringStyle::SingleQuote), "'dc2'"),
                (RightCurvedBracket, "}"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            CREATE_ROLE_WITH_ACCESS_TO_ALL_DATACENTERS,
            vec![
                (CreateKeyword, "create"),
                (RoleKeyword, "role"),
                (Identifier, "big_data_role"),
                (WithKeyword, "with"),
                (AccessKeyword, "access"),
                (ToKeyword, "to"),
                (AllKeyword, "all"),
                (DatacentersKeyword, "datacenters"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            CREATE_ROLE_WITH_MULTIPLE_ROLE_OPTIONS,
            vec![
                (CreateKeyword, "create"),
                (RoleKeyword, "role"),
                (Identifier, "big_data_role"),
                (WithKeyword, "with"),
                (PasswordKeyword, "password"),
                (Equal, "="),
                (StringLiteral(StringStyle::SingleQuote), "'asdf'"),
                (AndKeyword, "and"),
                (LoginKeyword, "login"),
                (Equal, "="),
                (TrueKeyword, "true"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            CREATE_ROLE_WITH_PASSWORD_IF_NOT_EXISTS,
            vec![
                (CreateKeyword, "create"),
                (RoleKeyword, "role"),
                (IfKeyword, "if"),
                (NotKeyword, "not"),
                (ExistsKeyword, "exists"),
                (Identifier, "big_data_role"),
                (WithKeyword, "with"),
                (PasswordKeyword, "password"),
                (Equal, "="),
                (StringLiteral(StringStyle::SingleQuote), "'asdf'"),
                (Semicolon, ";"),
            ],
        );
    }
}

mod alter_role {
    use super::*;

    #[test]
    fn test_alter_role() {
        tokenize_expect(
            ALTER_ROLE_WITH_PASSWORD,
            vec![
                (AlterKeyword, "alter"),
                (RoleKeyword, "role"),
                (Identifier, "big_data_role"),
                (WithKeyword, "with"),
                (PasswordKeyword, "password"),
                (Equal, "="),
                (StringLiteral(StringStyle::SingleQuote), "'asdf'"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            ALTER_ROLE_WITH_HASHED_PASSWORD,
            vec![
                (AlterKeyword, "alter"),
                (RoleKeyword, "role"),
                (Identifier, "big_data_role"),
                (WithKeyword, "with"),
                (HashedKeyword, "hashed"),
                (PasswordKeyword, "password"),
                (Equal, "="),
                (StringLiteral(StringStyle::SingleQuote), "'aassddff'"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            ALTER_ROLE_WITH_LOGIN_TRUE,
            vec![
                (AlterKeyword, "alter"),
                (RoleKeyword, "role"),
                (Identifier, "big_data_role"),
                (WithKeyword, "with"),
                (LoginKeyword, "login"),
                (Equal, "="),
                (TrueKeyword, "true"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            ALTER_ROLE_WITH_LOGIN_FALSE,
            vec![
                (AlterKeyword, "alter"),
                (RoleKeyword, "role"),
                (Identifier, "big_data_role"),
                (WithKeyword, "with"),
                (LoginKeyword, "login"),
                (Equal, "="),
                (FalseKeyword, "false"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            ALTER_ROLE_WITH_SUPERUSER_TRUE,
            vec![
                (AlterKeyword, "alter"),
                (RoleKeyword, "role"),
                (Identifier, "big_data_role"),
                (WithKeyword, "with"),
                (SuperUserKeyword, "superuser"),
                (Equal, "="),
                (TrueKeyword, "true"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            ALTER_ROLE_WITH_SUPERUSER_FALSE,
            vec![
                (AlterKeyword, "alter"),
                (RoleKeyword, "role"),
                (Identifier, "big_data_role"),
                (WithKeyword, "with"),
                (SuperUserKeyword, "superuser"),
                (Equal, "="),
                (FalseKeyword, "false"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            ALTER_ROLE_WITH_OPTIONS_MAP,
            vec![
                (AlterKeyword, "alter"),
                (RoleKeyword, "role"),
                (Identifier, "big_data_role"),
                (WithKeyword, "with"),
                (OptionsKeyword, "options"),
                (Equal, "="),
                (LeftCurvedBracket, "{"),
                (StringLiteral(StringStyle::SingleQuote), "'opt1'"),
                (Colon, ":"),
                (StringLiteral(StringStyle::SingleQuote), "'val'"),
                (Comma, ","),
                (StringLiteral(StringStyle::SingleQuote), "'opt2'"),
                (Colon, ":"),
                (NumberLiteral, "99"),
                (RightCurvedBracket, "}"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            ALTER_ROLE_WITH_ACCESS_TO_DATACENTERS_SET,
            vec![
                (AlterKeyword, "alter"),
                (RoleKeyword, "role"),
                (Identifier, "big_data_role"),
                (WithKeyword, "with"),
                (AccessKeyword, "access"),
                (ToKeyword, "to"),
                (DatacentersKeyword, "datacenters"),
                (LeftCurvedBracket, "{"),
                (StringLiteral(StringStyle::SingleQuote), "'dc1'"),
                (Comma, ","),
                (StringLiteral(StringStyle::SingleQuote), "'dc2'"),
                (RightCurvedBracket, "}"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            ALTER_ROLE_WITH_ACCESS_TO_ALL_DATACENTERS,
            vec![
                (AlterKeyword, "alter"),
                (RoleKeyword, "role"),
                (Identifier, "big_data_role"),
                (WithKeyword, "with"),
                (AccessKeyword, "access"),
                (ToKeyword, "to"),
                (AllKeyword, "all"),
                (DatacentersKeyword, "datacenters"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            ALTER_ROLE_WITH_MULTIPLE_ROLE_OPTIONS,
            vec![
                (AlterKeyword, "alter"),
                (RoleKeyword, "role"),
                (Identifier, "big_data_role"),
                (WithKeyword, "with"),
                (PasswordKeyword, "password"),
                (Equal, "="),
                (StringLiteral(StringStyle::SingleQuote), "'asdf'"),
                (AndKeyword, "and"),
                (LoginKeyword, "login"),
                (Equal, "="),
                (TrueKeyword, "true"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            ALTER_ROLE_IF_EXISTS,
            vec![
                (AlterKeyword, "alter"),
                (RoleKeyword, "role"),
                (IfKeyword, "if"),
                (ExistsKeyword, "exists"),
                (Identifier, "big_data_role"),
                (WithKeyword, "with"),
                (PasswordKeyword, "password"),
                (Equal, "="),
                (StringLiteral(StringStyle::SingleQuote), "'asdf'"),
                (Semicolon, ";"),
            ],
        );
    }
}

mod drop_role {
    use super::*;

    #[test]
    fn test_drop_role() {
        tokenize_expect(
            DROP_ROLE,
            vec![
                (DropKeyword, "drop"),
                (RoleKeyword, "role"),
                (Identifier, "big_data_role"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_drop_role_if_exists() {
        tokenize_expect(
            DROP_ROLE_IF_EXISTS,
            vec![
                (DropKeyword, "drop"),
                (RoleKeyword, "role"),
                (IfKeyword, "if"),
                (ExistsKeyword, "exists"),
                (Identifier, "big_data_role"),
                (Semicolon, ";"),
            ],
        );
    }
}

mod grant_role {
    use super::*;

    #[test]
    fn test_grant_role() {
        tokenize_expect(
            GRANT_ROLE,
            vec![
                (GrantKeyword, "grant"),
                (RoleKeyword, "role"),
                (Identifier, "big_data_role"),
                (ToKeyword, "to"),
                (Identifier, "other_big_data_role"),
                (Semicolon, ";"),
            ],
        );
    }
}

mod revoke_role {
    use super::*;

    #[test]
    fn test_revoke_role() {
        tokenize_expect(
            REVOKE_ROLE,
            vec![
                (RevokeKeyword, "revoke"),
                (RoleKeyword, "role"),
                (Identifier, "big_data_role"),
                (FromKeyword, "from"),
                (Identifier, "other_big_data_role"),
                (Semicolon, ";"),
            ],
        );
    }
}

mod list_roles {
    use super::*;

    #[test]
    fn test_list_roles() {
        tokenize_expect(
            LIST_ROLES,
            vec![
                (ListKeyword, "list"),
                (RolesKeyword, "roles"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            LIST_ROLES_NOT_RECURSIVELY,
            vec![
                (ListKeyword, "list"),
                (RolesKeyword, "roles"),
                (NoRecursiveKeyword, "norecursive"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_list_roles_of_role() {
        tokenize_expect(
            LIST_ROLES_OF_ROLE,
            vec![
                (ListKeyword, "list"),
                (RolesKeyword, "roles"),
                (OfKeyword, "of"),
                (Identifier, "big_data_role"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            LIST_ROLES_OF_ROLE_NOT_RECURSIVELY,
            vec![
                (ListKeyword, "list"),
                (RolesKeyword, "roles"),
                (OfKeyword, "of"),
                (Identifier, "big_data_role"),
                (NoRecursiveKeyword, "norecursive"),
                (Semicolon, ";"),
            ],
        );
    }
}

mod create_user {
    use super::*;

    #[test]
    fn test_create_user() {
        tokenize_expect(
            CREATE_USER_IF_NOT_EXISTS,
            vec![
                (CreateKeyword, "create"),
                (UserKeyword, "user"),
                (IfKeyword, "if"),
                (NotKeyword, "not"),
                (ExistsKeyword, "exists"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            CREATE_USER_WITHOUT_PASSWORD,
            vec![
                (CreateKeyword, "create"),
                (UserKeyword, "user"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            CREATE_USER_SUPERUSER,
            vec![
                (CreateKeyword, "create"),
                (UserKeyword, "user"),
                (Identifier, "big_data_user"),
                (SuperUserKeyword, "superuser"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            CREATE_USER_NOT_SUPERUSER,
            vec![
                (CreateKeyword, "create"),
                (UserKeyword, "user"),
                (Identifier, "big_data_user"),
                (NoSuperUserKeyword, "nosuperuser"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            CREATE_USER_WITH_PASSWORD,
            vec![
                (CreateKeyword, "create"),
                (UserKeyword, "user"),
                (Identifier, "big_data_user"),
                (WithKeyword, "with"),
                (PasswordKeyword, "password"),
                (StringLiteral(StringStyle::SingleQuote), "'asdf'"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            CREATE_USER_WITH_PASSWORD_SUPERUSER,
            vec![
                (CreateKeyword, "create"),
                (UserKeyword, "user"),
                (Identifier, "big_data_user"),
                (WithKeyword, "with"),
                (PasswordKeyword, "password"),
                (StringLiteral(StringStyle::SingleQuote), "'asdf'"),
                (SuperUserKeyword, "superuser"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            CREATE_USER_WITH_PASSWORD_NOT_SUPERUSER,
            vec![
                (CreateKeyword, "create"),
                (UserKeyword, "user"),
                (Identifier, "big_data_user"),
                (WithKeyword, "with"),
                (PasswordKeyword, "password"),
                (StringLiteral(StringStyle::SingleQuote), "'asdf'"),
                (NoSuperUserKeyword, "nosuperuser"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            CREATE_USER_WITH_HASHED_PASSWORD,
            vec![
                (CreateKeyword, "create"),
                (UserKeyword, "user"),
                (Identifier, "big_data_user"),
                (WithKeyword, "with"),
                (HashedKeyword, "hashed"),
                (PasswordKeyword, "password"),
                (StringLiteral(StringStyle::SingleQuote), "'aassddff'"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            CREATE_USER_WITH_HASHED_PASSWORD_SUPERUSER,
            vec![
                (CreateKeyword, "create"),
                (UserKeyword, "user"),
                (Identifier, "big_data_user"),
                (WithKeyword, "with"),
                (HashedKeyword, "hashed"),
                (PasswordKeyword, "password"),
                (StringLiteral(StringStyle::SingleQuote), "'aassddff'"),
                (SuperUserKeyword, "superuser"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            CREATE_USER_WITH_HASHED_PASSWORD_NOT_SUPERUSER,
            vec![
                (CreateKeyword, "create"),
                (UserKeyword, "user"),
                (Identifier, "big_data_user"),
                (WithKeyword, "with"),
                (HashedKeyword, "hashed"),
                (PasswordKeyword, "password"),
                (StringLiteral(StringStyle::SingleQuote), "'aassddff'"),
                (NoSuperUserKeyword, "nosuperuser"),
                (Semicolon, ";"),
            ],
        );
    }
}

mod alter_user {
    use super::*;

    #[test]
    fn test_alter_user() {
        tokenize_expect(
            ALTER_USER_IF_EXISTS,
            vec![
                (AlterKeyword, "alter"),
                (UserKeyword, "user"),
                (IfKeyword, "if"),
                (ExistsKeyword, "exists"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            ALTER_USER,
            vec![
                (AlterKeyword, "alter"),
                (UserKeyword, "user"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            ALTER_USER_SUPERUSER,
            vec![
                (AlterKeyword, "alter"),
                (UserKeyword, "user"),
                (Identifier, "big_data_user"),
                (SuperUserKeyword, "superuser"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            ALTER_USER_NOT_SUPERUSER,
            vec![
                (AlterKeyword, "alter"),
                (UserKeyword, "user"),
                (Identifier, "big_data_user"),
                (NoSuperUserKeyword, "nosuperuser"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            ALTER_USER_WITH_PASSWORD,
            vec![
                (AlterKeyword, "alter"),
                (UserKeyword, "user"),
                (Identifier, "big_data_user"),
                (WithKeyword, "with"),
                (PasswordKeyword, "password"),
                (StringLiteral(StringStyle::SingleQuote), "'asdf'"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            ALTER_USER_WITH_PASSWORD_SUPERUSER,
            vec![
                (AlterKeyword, "alter"),
                (UserKeyword, "user"),
                (Identifier, "big_data_user"),
                (WithKeyword, "with"),
                (PasswordKeyword, "password"),
                (StringLiteral(StringStyle::SingleQuote), "'asdf'"),
                (SuperUserKeyword, "superuser"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            ALTER_USER_WITH_PASSWORD_NOT_SUPERUSER,
            vec![
                (AlterKeyword, "alter"),
                (UserKeyword, "user"),
                (Identifier, "big_data_user"),
                (WithKeyword, "with"),
                (PasswordKeyword, "password"),
                (StringLiteral(StringStyle::SingleQuote), "'asdf'"),
                (NoSuperUserKeyword, "nosuperuser"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            ALTER_USER_WITH_HASHED_PASSWORD,
            vec![
                (AlterKeyword, "alter"),
                (UserKeyword, "user"),
                (Identifier, "big_data_user"),
                (WithKeyword, "with"),
                (HashedKeyword, "hashed"),
                (PasswordKeyword, "password"),
                (StringLiteral(StringStyle::SingleQuote), "'aassddff'"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            ALTER_USER_WITH_HASHED_PASSWORD_SUPERUSER,
            vec![
                (AlterKeyword, "alter"),
                (UserKeyword, "user"),
                (Identifier, "big_data_user"),
                (WithKeyword, "with"),
                (HashedKeyword, "hashed"),
                (PasswordKeyword, "password"),
                (StringLiteral(StringStyle::SingleQuote), "'aassddff'"),
                (SuperUserKeyword, "superuser"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            ALTER_USER_WITH_HASHED_PASSWORD_NOT_SUPERUSER,
            vec![
                (AlterKeyword, "alter"),
                (UserKeyword, "user"),
                (Identifier, "big_data_user"),
                (WithKeyword, "with"),
                (HashedKeyword, "hashed"),
                (PasswordKeyword, "password"),
                (StringLiteral(StringStyle::SingleQuote), "'aassddff'"),
                (NoSuperUserKeyword, "nosuperuser"),
                (Semicolon, ";"),
            ],
        );
    }
}

mod drop_user {
    use super::*;

    #[test]
    fn test_drop_user() {
        tokenize_expect(
            DROP_USER,
            vec![
                (DropKeyword, "drop"),
                (UserKeyword, "user"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_drop_user_if_exists() {
        tokenize_expect(
            DROP_USER_IF_EXISTS,
            vec![
                (DropKeyword, "drop"),
                (UserKeyword, "user"),
                (IfKeyword, "if"),
                (ExistsKeyword, "exists"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
    }
}

mod list_users {
    use super::*;

    #[test]
    fn test_list_users() {
        tokenize_expect(
            LIST_USERS,
            vec![
                (ListKeyword, "list"),
                (UsersKeyword, "users"),
                (Semicolon, ";"),
            ],
        );
    }
}

mod grant_permission {
    use super::*;

    #[test]
    fn test_grant_permissions_on_roles() {
        tokenize_expect(
            GRANT_ALL_PERMISSIONS_ON_ALL_KEYSPACES,
            vec![
                (GrantKeyword, "grant"),
                (AllKeyword, "all"),
                (PermissionsKeyword, "permissions"),
                (OnKeyword, "on"),
                (AllKeyword, "all"),
                (KeyspacesKeyword, "keyspaces"),
                (ToKeyword, "to"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            GRANT_ALL_ON_ALL_KEYSPACES,
            vec![
                (GrantKeyword, "grant"),
                (AllKeyword, "all"),
                (OnKeyword, "on"),
                (AllKeyword, "all"),
                (KeyspacesKeyword, "keyspaces"),
                (ToKeyword, "to"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            GRANT_ALL_ON_KEYSPACE,
            vec![
                (GrantKeyword, "grant"),
                (AllKeyword, "all"),
                (OnKeyword, "on"),
                (KeyspaceKeyword, "keyspace"),
                (Identifier, "big_data_keyspace"),
                (ToKeyword, "to"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            GRANT_ALL_ON_TABLE_IMPLICITLY,
            vec![
                (GrantKeyword, "grant"),
                (AllKeyword, "all"),
                (OnKeyword, "on"),
                (Identifier, "big_data_table"),
                (ToKeyword, "to"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            GRANT_ALL_ON_TABLE_WITH_EXPLICIT_KEYSPACE_IMPLICITLY,
            vec![
                (GrantKeyword, "grant"),
                (AllKeyword, "all"),
                (OnKeyword, "on"),
                (Identifier, "big_data_keyspace"),
                (Dot, "."),
                (Identifier, "big_data_table"),
                (ToKeyword, "to"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            GRANT_ALL_ON_TABLE_EXPLICITLY,
            vec![
                (GrantKeyword, "grant"),
                (AllKeyword, "all"),
                (OnKeyword, "on"),
                (TableKeyword, "table"),
                (Identifier, "big_data_table"),
                (ToKeyword, "to"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            GRANT_ALL_ON_TABLE_WITH_EXPLICIT_KEYSPACE_EXPLICITLY,
            vec![
                (GrantKeyword, "grant"),
                (AllKeyword, "all"),
                (OnKeyword, "on"),
                (TableKeyword, "table"),
                (Identifier, "big_data_keyspace"),
                (Dot, "."),
                (Identifier, "big_data_table"),
                (ToKeyword, "to"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            GRANT_ALL_ON_ALL_ROLES,
            vec![
                (GrantKeyword, "grant"),
                (AllKeyword, "all"),
                (OnKeyword, "on"),
                (AllKeyword, "all"),
                (RolesKeyword, "roles"),
                (ToKeyword, "to"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            GRANT_ALL_ON_ROLE,
            vec![
                (GrantKeyword, "grant"),
                (AllKeyword, "all"),
                (OnKeyword, "on"),
                (RoleKeyword, "role"),
                (Identifier, "big_data_role"),
                (ToKeyword, "to"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            GRANT_ALL_ON_ALL_FUNCTIONS,
            vec![
                (GrantKeyword, "grant"),
                (AllKeyword, "all"),
                (OnKeyword, "on"),
                (AllKeyword, "all"),
                (FunctionsKeyword, "functions"),
                (ToKeyword, "to"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            GRANT_ALL_ON_FUNCTION,
            vec![
                (GrantKeyword, "grant"),
                (AllKeyword, "all"),
                (OnKeyword, "on"),
                (FunctionKeyword, "function"),
                (Identifier, "big_data_function"),
                (LeftParenthesis, "("),
                (IntKeyword, "int"),
                (RightParenthesis, ")"),
                (ToKeyword, "to"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            GRANT_ALL_ON_FUNCTION_WITH_EXPLICIT_KEYSPACE,
            vec![
                (GrantKeyword, "grant"),
                (AllKeyword, "all"),
                (OnKeyword, "on"),
                (FunctionKeyword, "function"),
                (Identifier, "big_data_keyspace"),
                (Dot, "."),
                (Identifier, "big_data_function"),
                (LeftParenthesis, "("),
                (IntKeyword, "int"),
                (RightParenthesis, ")"),
                (ToKeyword, "to"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            GRANT_ALL_ON_ALL_MBEANS,
            vec![
                (GrantKeyword, "grant"),
                (AllKeyword, "all"),
                (OnKeyword, "on"),
                (AllKeyword, "all"),
                (MBeansKeyword, "mbeans"),
                (ToKeyword, "to"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            GRANT_ALL_ON_MBEANS,
            vec![
                (GrantKeyword, "grant"),
                (AllKeyword, "all"),
                (OnKeyword, "on"),
                (MBeansKeyword, "mbeans"),
                (Identifier, "big_data_mbean"),
                (ToKeyword, "to"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            GRANT_ALL_ON_MBEAN,
            vec![
                (GrantKeyword, "grant"),
                (AllKeyword, "all"),
                (OnKeyword, "on"),
                (MBeanKeyword, "mbean"),
                (Identifier, "big_data_mbean"),
                (ToKeyword, "to"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_grant_permissions() {
        tokenize_expect(
            GRANT_CREATE_PERMISSION_ON_ALL_KEYSPACES,
            vec![
                (GrantKeyword, "grant"),
                (CreateKeyword, "create"),
                (PermissionKeyword, "permission"),
                (OnKeyword, "on"),
                (AllKeyword, "all"),
                (KeyspacesKeyword, "keyspaces"),
                (ToKeyword, "to"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            GRANT_CREATE_ON_ALL_KEYSPACES,
            vec![
                (GrantKeyword, "grant"),
                (CreateKeyword, "create"),
                (OnKeyword, "on"),
                (AllKeyword, "all"),
                (KeyspacesKeyword, "keyspaces"),
                (ToKeyword, "to"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            GRANT_ALTER_ON_ALL_KEYSPACES,
            vec![
                (GrantKeyword, "grant"),
                (AlterKeyword, "alter"),
                (OnKeyword, "on"),
                (AllKeyword, "all"),
                (KeyspacesKeyword, "keyspaces"),
                (ToKeyword, "to"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            GRANT_DROP_ON_ALL_KEYSPACES,
            vec![
                (GrantKeyword, "grant"),
                (DropKeyword, "drop"),
                (OnKeyword, "on"),
                (AllKeyword, "all"),
                (KeyspacesKeyword, "keyspaces"),
                (ToKeyword, "to"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            GRANT_SELECT_ON_ALL_KEYSPACES,
            vec![
                (GrantKeyword, "grant"),
                (SelectKeyword, "select"),
                (OnKeyword, "on"),
                (AllKeyword, "all"),
                (KeyspacesKeyword, "keyspaces"),
                (ToKeyword, "to"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            GRANT_MODIFY_ON_ALL_KEYSPACES,
            vec![
                (GrantKeyword, "grant"),
                (ModifyKeyword, "modify"),
                (OnKeyword, "on"),
                (AllKeyword, "all"),
                (KeyspacesKeyword, "keyspaces"),
                (ToKeyword, "to"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            GRANT_AUTHORIZE_ON_ALL_KEYSPACES,
            vec![
                (GrantKeyword, "grant"),
                (AuthorizeKeyword, "authorize"),
                (OnKeyword, "on"),
                (AllKeyword, "all"),
                (KeyspacesKeyword, "keyspaces"),
                (ToKeyword, "to"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            GRANT_DESCRIBE_ON_ALL_KEYSPACES,
            vec![
                (GrantKeyword, "grant"),
                (DescribeKeyword, "describe"),
                (OnKeyword, "on"),
                (AllKeyword, "all"),
                (KeyspacesKeyword, "keyspaces"),
                (ToKeyword, "to"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            GRANT_EXECUTE_ON_ALL_KEYSPACES,
            vec![
                (GrantKeyword, "grant"),
                (ExecuteKeyword, "execute"),
                (OnKeyword, "on"),
                (AllKeyword, "all"),
                (KeyspacesKeyword, "keyspaces"),
                (ToKeyword, "to"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
    }
}

mod list_permissions {
    use super::*;

    #[test]
    fn test_list_permissions_on_roles() {
        tokenize_expect(
            LIST_ALL_PERMISSIONS_ON_ALL_KEYSPACES,
            vec![
                (ListKeyword, "list"),
                (AllKeyword, "all"),
                (PermissionsKeyword, "permissions"),
                (OnKeyword, "on"),
                (AllKeyword, "all"),
                (KeyspacesKeyword, "keyspaces"),
                (OfKeyword, "of"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            LIST_ALL_ON_ALL_KEYSPACES,
            vec![
                (ListKeyword, "list"),
                (AllKeyword, "all"),
                (OnKeyword, "on"),
                (AllKeyword, "all"),
                (KeyspacesKeyword, "keyspaces"),
                (OfKeyword, "of"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            LIST_ALL_ON_KEYSPACE,
            vec![
                (ListKeyword, "list"),
                (AllKeyword, "all"),
                (OnKeyword, "on"),
                (KeyspaceKeyword, "keyspace"),
                (Identifier, "big_data_keyspace"),
                (OfKeyword, "of"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            LIST_ALL_ON_TABLE_IMPLICITLY,
            vec![
                (ListKeyword, "list"),
                (AllKeyword, "all"),
                (OnKeyword, "on"),
                (Identifier, "big_data_table"),
                (OfKeyword, "of"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            LIST_ALL_ON_TABLE_WITH_EXPLICIT_KEYSPACE_IMPLICITLY,
            vec![
                (ListKeyword, "list"),
                (AllKeyword, "all"),
                (OnKeyword, "on"),
                (Identifier, "big_data_keyspace"),
                (Dot, "."),
                (Identifier, "big_data_table"),
                (OfKeyword, "of"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            LIST_ALL_ON_TABLE_EXPLICITLY,
            vec![
                (ListKeyword, "list"),
                (AllKeyword, "all"),
                (OnKeyword, "on"),
                (TableKeyword, "table"),
                (Identifier, "big_data_table"),
                (OfKeyword, "of"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            LIST_ALL_ON_TABLE_WITH_EXPLICIT_KEYSPACE_EXPLICITLY,
            vec![
                (ListKeyword, "list"),
                (AllKeyword, "all"),
                (OnKeyword, "on"),
                (TableKeyword, "table"),
                (Identifier, "big_data_keyspace"),
                (Dot, "."),
                (Identifier, "big_data_table"),
                (OfKeyword, "of"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            LIST_ALL_ON_ALL_ROLES,
            vec![
                (ListKeyword, "list"),
                (AllKeyword, "all"),
                (OnKeyword, "on"),
                (AllKeyword, "all"),
                (RolesKeyword, "roles"),
                (OfKeyword, "of"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            LIST_ALL_ON_ROLE,
            vec![
                (ListKeyword, "list"),
                (AllKeyword, "all"),
                (OnKeyword, "on"),
                (RoleKeyword, "role"),
                (Identifier, "big_data_role"),
                (OfKeyword, "of"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            LIST_ALL_ON_ALL_FUNCTIONS,
            vec![
                (ListKeyword, "list"),
                (AllKeyword, "all"),
                (OnKeyword, "on"),
                (AllKeyword, "all"),
                (FunctionsKeyword, "functions"),
                (OfKeyword, "of"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            LIST_ALL_ON_FUNCTION,
            vec![
                (ListKeyword, "list"),
                (AllKeyword, "all"),
                (OnKeyword, "on"),
                (FunctionKeyword, "function"),
                (Identifier, "big_data_function"),
                (LeftParenthesis, "("),
                (IntKeyword, "int"),
                (RightParenthesis, ")"),
                (OfKeyword, "of"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            LIST_ALL_ON_FUNCTION_WITH_EXPLICIT_KEYSPACE,
            vec![
                (ListKeyword, "list"),
                (AllKeyword, "all"),
                (OnKeyword, "on"),
                (FunctionKeyword, "function"),
                (Identifier, "big_data_keyspace"),
                (Dot, "."),
                (Identifier, "big_data_function"),
                (LeftParenthesis, "("),
                (IntKeyword, "int"),
                (RightParenthesis, ")"),
                (OfKeyword, "of"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            LIST_ALL_ON_ALL_MBEANS,
            vec![
                (ListKeyword, "list"),
                (AllKeyword, "all"),
                (OnKeyword, "on"),
                (AllKeyword, "all"),
                (MBeansKeyword, "mbeans"),
                (OfKeyword, "of"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            LIST_ALL_ON_MBEANS,
            vec![
                (ListKeyword, "list"),
                (AllKeyword, "all"),
                (OnKeyword, "on"),
                (MBeansKeyword, "mbeans"),
                (Identifier, "big_data_mbean"),
                (OfKeyword, "of"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            LIST_ALL_ON_MBEAN,
            vec![
                (ListKeyword, "list"),
                (AllKeyword, "all"),
                (OnKeyword, "on"),
                (MBeanKeyword, "mbean"),
                (Identifier, "big_data_mbean"),
                (OfKeyword, "of"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_list_permissions() {
        tokenize_expect(
            LIST_CREATE_PERMISSION_ON_ALL_KEYSPACES,
            vec![
                (ListKeyword, "list"),
                (CreateKeyword, "create"),
                (PermissionKeyword, "permission"),
                (OnKeyword, "on"),
                (AllKeyword, "all"),
                (KeyspacesKeyword, "keyspaces"),
                (OfKeyword, "of"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            LIST_CREATE_ON_ALL_KEYSPACES,
            vec![
                (ListKeyword, "list"),
                (CreateKeyword, "create"),
                (OnKeyword, "on"),
                (AllKeyword, "all"),
                (KeyspacesKeyword, "keyspaces"),
                (OfKeyword, "of"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            LIST_ALTER_ON_ALL_KEYSPACES,
            vec![
                (ListKeyword, "list"),
                (AlterKeyword, "alter"),
                (OnKeyword, "on"),
                (AllKeyword, "all"),
                (KeyspacesKeyword, "keyspaces"),
                (OfKeyword, "of"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            LIST_DROP_ON_ALL_KEYSPACES,
            vec![
                (ListKeyword, "list"),
                (DropKeyword, "drop"),
                (OnKeyword, "on"),
                (AllKeyword, "all"),
                (KeyspacesKeyword, "keyspaces"),
                (OfKeyword, "of"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            LIST_SELECT_ON_ALL_KEYSPACES,
            vec![
                (ListKeyword, "list"),
                (SelectKeyword, "select"),
                (OnKeyword, "on"),
                (AllKeyword, "all"),
                (KeyspacesKeyword, "keyspaces"),
                (OfKeyword, "of"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            LIST_MODIFY_ON_ALL_KEYSPACES,
            vec![
                (ListKeyword, "list"),
                (ModifyKeyword, "modify"),
                (OnKeyword, "on"),
                (AllKeyword, "all"),
                (KeyspacesKeyword, "keyspaces"),
                (OfKeyword, "of"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            LIST_AUTHORIZE_ON_ALL_KEYSPACES,
            vec![
                (ListKeyword, "list"),
                (AuthorizeKeyword, "authorize"),
                (OnKeyword, "on"),
                (AllKeyword, "all"),
                (KeyspacesKeyword, "keyspaces"),
                (OfKeyword, "of"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            LIST_DESCRIBE_ON_ALL_KEYSPACES,
            vec![
                (ListKeyword, "list"),
                (DescribeKeyword, "describe"),
                (OnKeyword, "on"),
                (AllKeyword, "all"),
                (KeyspacesKeyword, "keyspaces"),
                (OfKeyword, "of"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            LIST_EXECUTE_ON_ALL_KEYSPACES,
            vec![
                (ListKeyword, "list"),
                (ExecuteKeyword, "execute"),
                (OnKeyword, "on"),
                (AllKeyword, "all"),
                (KeyspacesKeyword, "keyspaces"),
                (OfKeyword, "of"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
    }
}

mod revoke_permission {
    use super::*;

    #[test]
    fn test_revoke_permissions_on_roles() {
        tokenize_expect(
            REVOKE_ALL_PERMISSIONS_ON_ALL_KEYSPACES,
            vec![
                (RevokeKeyword, "revoke"),
                (AllKeyword, "all"),
                (PermissionsKeyword, "permissions"),
                (OnKeyword, "on"),
                (AllKeyword, "all"),
                (KeyspacesKeyword, "keyspaces"),
                (FromKeyword, "from"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            REVOKE_ALL_ON_ALL_KEYSPACES,
            vec![
                (RevokeKeyword, "revoke"),
                (AllKeyword, "all"),
                (OnKeyword, "on"),
                (AllKeyword, "all"),
                (KeyspacesKeyword, "keyspaces"),
                (FromKeyword, "from"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            REVOKE_ALL_ON_KEYSPACE,
            vec![
                (RevokeKeyword, "revoke"),
                (AllKeyword, "all"),
                (OnKeyword, "on"),
                (KeyspaceKeyword, "keyspace"),
                (Identifier, "big_data_keyspace"),
                (FromKeyword, "from"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            REVOKE_ALL_ON_TABLE_IMPLICITLY,
            vec![
                (RevokeKeyword, "revoke"),
                (AllKeyword, "all"),
                (OnKeyword, "on"),
                (Identifier, "big_data_table"),
                (FromKeyword, "from"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            REVOKE_ALL_ON_TABLE_WITH_EXPLICIT_KEYSPACE_IMPLICITLY,
            vec![
                (RevokeKeyword, "revoke"),
                (AllKeyword, "all"),
                (OnKeyword, "on"),
                (Identifier, "big_data_keyspace"),
                (Dot, "."),
                (Identifier, "big_data_table"),
                (FromKeyword, "from"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            REVOKE_ALL_ON_TABLE_EXPLICITLY,
            vec![
                (RevokeKeyword, "revoke"),
                (AllKeyword, "all"),
                (OnKeyword, "on"),
                (TableKeyword, "table"),
                (Identifier, "big_data_table"),
                (FromKeyword, "from"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            REVOKE_ALL_ON_TABLE_WITH_EXPLICIT_KEYSPACE_EXPLICITLY,
            vec![
                (RevokeKeyword, "revoke"),
                (AllKeyword, "all"),
                (OnKeyword, "on"),
                (TableKeyword, "table"),
                (Identifier, "big_data_keyspace"),
                (Dot, "."),
                (Identifier, "big_data_table"),
                (FromKeyword, "from"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            REVOKE_ALL_ON_ALL_ROLES,
            vec![
                (RevokeKeyword, "revoke"),
                (AllKeyword, "all"),
                (OnKeyword, "on"),
                (AllKeyword, "all"),
                (RolesKeyword, "roles"),
                (FromKeyword, "from"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            REVOKE_ALL_ON_ROLE,
            vec![
                (RevokeKeyword, "revoke"),
                (AllKeyword, "all"),
                (OnKeyword, "on"),
                (RoleKeyword, "role"),
                (Identifier, "big_data_role"),
                (FromKeyword, "from"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            REVOKE_ALL_ON_ALL_FUNCTIONS,
            vec![
                (RevokeKeyword, "revoke"),
                (AllKeyword, "all"),
                (OnKeyword, "on"),
                (AllKeyword, "all"),
                (FunctionsKeyword, "functions"),
                (FromKeyword, "from"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            REVOKE_ALL_ON_FUNCTION,
            vec![
                (RevokeKeyword, "revoke"),
                (AllKeyword, "all"),
                (OnKeyword, "on"),
                (FunctionKeyword, "function"),
                (Identifier, "big_data_function"),
                (LeftParenthesis, "("),
                (IntKeyword, "int"),
                (RightParenthesis, ")"),
                (FromKeyword, "from"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            REVOKE_ALL_ON_FUNCTION_WITH_EXPLICIT_KEYSPACE,
            vec![
                (RevokeKeyword, "revoke"),
                (AllKeyword, "all"),
                (OnKeyword, "on"),
                (FunctionKeyword, "function"),
                (Identifier, "big_data_keyspace"),
                (Dot, "."),
                (Identifier, "big_data_function"),
                (LeftParenthesis, "("),
                (IntKeyword, "int"),
                (RightParenthesis, ")"),
                (FromKeyword, "from"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            REVOKE_ALL_ON_ALL_MBEANS,
            vec![
                (RevokeKeyword, "revoke"),
                (AllKeyword, "all"),
                (OnKeyword, "on"),
                (AllKeyword, "all"),
                (MBeansKeyword, "mbeans"),
                (FromKeyword, "from"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            REVOKE_ALL_ON_MBEANS,
            vec![
                (RevokeKeyword, "revoke"),
                (AllKeyword, "all"),
                (OnKeyword, "on"),
                (MBeansKeyword, "mbeans"),
                (Identifier, "big_data_mbean"),
                (FromKeyword, "from"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            REVOKE_ALL_ON_MBEAN,
            vec![
                (RevokeKeyword, "revoke"),
                (AllKeyword, "all"),
                (OnKeyword, "on"),
                (MBeanKeyword, "mbean"),
                (Identifier, "big_data_mbean"),
                (FromKeyword, "from"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
    }

    #[test]
    fn test_revoke_permissions() {
        tokenize_expect(
            REVOKE_CREATE_PERMISSION_ON_ALL_KEYSPACES,
            vec![
                (RevokeKeyword, "revoke"),
                (CreateKeyword, "create"),
                (PermissionKeyword, "permission"),
                (OnKeyword, "on"),
                (AllKeyword, "all"),
                (KeyspacesKeyword, "keyspaces"),
                (FromKeyword, "from"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            REVOKE_CREATE_ON_ALL_KEYSPACES,
            vec![
                (RevokeKeyword, "revoke"),
                (CreateKeyword, "create"),
                (OnKeyword, "on"),
                (AllKeyword, "all"),
                (KeyspacesKeyword, "keyspaces"),
                (FromKeyword, "from"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            REVOKE_ALTER_ON_ALL_KEYSPACES,
            vec![
                (RevokeKeyword, "revoke"),
                (AlterKeyword, "alter"),
                (OnKeyword, "on"),
                (AllKeyword, "all"),
                (KeyspacesKeyword, "keyspaces"),
                (FromKeyword, "from"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            REVOKE_DROP_ON_ALL_KEYSPACES,
            vec![
                (RevokeKeyword, "revoke"),
                (DropKeyword, "drop"),
                (OnKeyword, "on"),
                (AllKeyword, "all"),
                (KeyspacesKeyword, "keyspaces"),
                (FromKeyword, "from"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            REVOKE_SELECT_ON_ALL_KEYSPACES,
            vec![
                (RevokeKeyword, "revoke"),
                (SelectKeyword, "select"),
                (OnKeyword, "on"),
                (AllKeyword, "all"),
                (KeyspacesKeyword, "keyspaces"),
                (FromKeyword, "from"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            REVOKE_MODIFY_ON_ALL_KEYSPACES,
            vec![
                (RevokeKeyword, "revoke"),
                (ModifyKeyword, "modify"),
                (OnKeyword, "on"),
                (AllKeyword, "all"),
                (KeyspacesKeyword, "keyspaces"),
                (FromKeyword, "from"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            REVOKE_AUTHORIZE_ON_ALL_KEYSPACES,
            vec![
                (RevokeKeyword, "revoke"),
                (AuthorizeKeyword, "authorize"),
                (OnKeyword, "on"),
                (AllKeyword, "all"),
                (KeyspacesKeyword, "keyspaces"),
                (FromKeyword, "from"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            REVOKE_DESCRIBE_ON_ALL_KEYSPACES,
            vec![
                (RevokeKeyword, "revoke"),
                (DescribeKeyword, "describe"),
                (OnKeyword, "on"),
                (AllKeyword, "all"),
                (KeyspacesKeyword, "keyspaces"),
                (FromKeyword, "from"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
        tokenize_expect(
            REVOKE_EXECUTE_ON_ALL_KEYSPACES,
            vec![
                (RevokeKeyword, "revoke"),
                (ExecuteKeyword, "execute"),
                (OnKeyword, "on"),
                (AllKeyword, "all"),
                (KeyspacesKeyword, "keyspaces"),
                (FromKeyword, "from"),
                (Identifier, "big_data_user"),
                (Semicolon, ";"),
            ],
        );
    }
}
