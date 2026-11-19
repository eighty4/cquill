// pub use alter_role::*;
// pub use alter_user::*;
pub use create_role::*;
pub use create_user::*;
pub use drop_role::*;
pub use drop_user::*;
// pub use grant_permission::*;
// pub use grant_role::*;
// pub use list_permissions::*;
// pub use list_roles::*;
// pub use list_users::*;
// pub use revoke_permission::*;
// pub use revoke_role::*;

use crate::ast::StringView;

#[derive(Debug, PartialEq)]
pub enum AuthPassword {
    Hashed(StringView),
    PlainText(StringView),
}

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/security.html#create-role-statement
mod create_role {
    use std::collections::HashMap;

    use crate::ast::{AuthPassword, StringView, TokenView};

    #[derive(Debug, PartialEq)]
    pub struct CreateRoleStatement {
        pub if_not_exists: bool,
        pub role_name: TokenView,
        pub attributes: Option<Vec<RoleConfigAttribute>>,
    }

    #[derive(Debug, PartialEq)]
    pub enum RoleConfigAttribute {
        Superuser(bool),
        Login(bool),
        Password(AuthPassword),
        // todo proper map literal type
        Options(HashMap<StringView, TokenView>),
        Access(Datacenters),
    }

    #[derive(Debug, PartialEq)]
    pub enum Datacenters {
        All,
        Explicit(Vec<StringView>),
    }
}

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/security.html#alter-role-statement
mod alter_role {}

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/security.html#drop-role-statement
mod drop_role {
    use crate::ast::TokenView;

    #[derive(Debug, PartialEq)]
    pub struct DropRoleStatement {
        pub role_name: TokenView,
        pub if_exists: bool,
    }
}

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/security.html#grant-role-statement
mod grant_role {}

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/security.html#revoke-role-statement
mod revoke_role {}

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/security.html#list-roles-statement
mod list_roles {}

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/security.html#create-user-statement
mod create_user {
    use crate::ast::{AuthPassword, TokenView};

    #[derive(Debug, PartialEq)]
    pub struct CreateUserStatement {
        pub user_name: TokenView,
        pub if_not_exists: bool,
        pub password: Option<AuthPassword>,
        pub user_status: Option<CreateUserStatus>,
    }

    #[derive(Debug, PartialEq)]
    pub enum CreateUserStatus {
        NoSuperuser,
        Superuser,
    }
}

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/security.html#alter-user-statement
mod alter_user {}

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/security.html#drop-user-statement
mod drop_user {
    use crate::ast::TokenView;

    #[derive(Debug, PartialEq)]
    pub struct DropUserStatement {
        pub user_name: TokenView,
        pub if_exists: bool,
    }
}

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/security.html#list-users-statement
mod list_users {}

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/security.html#grant-permission-statement
mod grant_permission {}

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/security.html#revoke-permission-statement
mod revoke_permission {}

/// https://cassandra.apache.org/doc/5.0.9/cassandra/developing/cql/security.html#list-permissions-statement
mod list_permissions {}
