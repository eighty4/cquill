#[derive(Eq, Hash, PartialEq)]
pub enum DatabaseSecurity {
    PasswordAuth,
    Tls { password_auth: bool },
    Mtls,
}

impl DatabaseSecurity {
    pub fn uses_ssl(&self) -> bool {
        matches!(self, DatabaseSecurity::Tls { .. } | DatabaseSecurity::Mtls)
    }
}

#[derive(Default, Eq, Hash, PartialEq)]
pub struct DatabaseRequest {
    pub engine: DatabaseEngine,
    pub security: Option<DatabaseSecurity>,
}

impl DatabaseRequest {
    pub fn is_os_macos(&self) -> bool {
        cfg!(target_os = "macos")
    }

    pub fn use_password_auth(&self) -> bool {
        matches!(
            self.security,
            Some(DatabaseSecurity::PasswordAuth)
                | Some(DatabaseSecurity::Tls {
                    password_auth: true,
                })
        )
    }
}

#[derive(Eq, Hash, PartialEq)]
pub enum DatabaseEngine {
    Cassandra(CassandraVersion),
    Scylla(ScyllaVersion),
}

impl DatabaseEngine {
    pub fn is_cassandra(&self) -> bool {
        matches!(self, DatabaseEngine::Cassandra(_))
    }

    pub fn is_scylla(&self) -> bool {
        matches!(self, DatabaseEngine::Scylla(_))
    }

    pub fn image_repo(&self) -> String {
        match self {
            DatabaseEngine::Cassandra(_) => "cassandra",
            DatabaseEngine::Scylla(_) => "scylladb/scylla",
        }
        .into()
    }

    pub fn image_tag(&self) -> String {
        match self {
            DatabaseEngine::Cassandra(v) => v.image_tag(),
            DatabaseEngine::Scylla(v) => v.image_tag(),
        }
    }
}

impl Default for DatabaseEngine {
    fn default() -> Self {
        DatabaseEngine::Scylla(Default::default())
    }
}

#[derive(Default, Eq, Hash, PartialEq)]
pub enum CassandraVersion {
    Six,
    #[default]
    Five,
    Four,
}

impl CassandraVersion {
    fn image_tag(&self) -> String {
        match self {
            CassandraVersion::Six => "6.0",
            CassandraVersion::Five => "5.0",
            CassandraVersion::Four => "4.1",
        }
        .into()
    }
}

#[derive(Default, Eq, Hash, PartialEq)]
pub enum ScyllaVersion {
    #[default]
    TwentyTwentySix,
    TwentyTwentyFive,
    Six,
    Five,
}

impl ScyllaVersion {
    fn image_tag(&self) -> String {
        match self {
            ScyllaVersion::TwentyTwentySix => "2026.1",
            ScyllaVersion::TwentyTwentyFive => "2025.1",
            ScyllaVersion::Six => "6.2",
            ScyllaVersion::Five => "5.4",
        }
        .into()
    }
}
