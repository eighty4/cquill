use std::{
    env::{VarError, var},
    path::PathBuf,
};

use cquill::{ConnectionInit, ConnectionOpts, MigrateOpts, migrate_cql};
use cquill_dev_database::{
    CassandraVersion, DatabaseCerts, DatabaseEngine, DatabaseRequest, DatabaseSecurity,
    ScyllaVersion, request_container,
};

#[tokio::test]
async fn test_migrate_anon_tcp() {
    let details = request_container(DatabaseRequest {
        engine: test_database_engine(),
        security: None,
    })
    .await;

    let result = migrate_cql(MigrateOpts {
        cql_dir: example_cql_dir(),
        connection_init: Some(ConnectionInit::NewSession(Some(ConnectionOpts {
            port: Some(details.port()),
            ..Default::default()
        }))),
        ..Default::default()
    })
    .await;

    match result {
        Ok(_) => {}
        Err(err) => panic!("{err:#}"),
    }
}

#[tokio::test]
async fn test_migrate_password_auth() {
    let details = request_container(DatabaseRequest {
        engine: test_database_engine(),
        security: Some(DatabaseSecurity::PasswordAuth),
    })
    .await;

    let result = migrate_cql(MigrateOpts {
        cql_dir: example_cql_dir(),
        connection_init: Some(ConnectionInit::NewSession(Some(ConnectionOpts {
            port: Some(details.port()),
            username: Some("cassandra".into()),
            password: Some("cassandra".into()),
            ..Default::default()
        }))),
        ..Default::default()
    })
    .await;

    match result {
        Ok(_) => {}
        Err(err) => panic!("{err:#}"),
    }
}

#[tokio::test]
async fn test_migrate_tls_no_password() {
    let details = request_container(DatabaseRequest {
        engine: test_database_engine(),
        security: Some(DatabaseSecurity::Tls {
            password_auth: false,
        }),
    })
    .await;

    let ca_cert = if let Some(DatabaseCerts::StandardTls(tls_certs)) = details.certs() {
        tls_certs.ca_cert.clone()
    } else {
        panic!();
    };

    let result = migrate_cql(MigrateOpts {
        cql_dir: example_cql_dir(),
        connection_init: Some(ConnectionInit::NewSession(Some(ConnectionOpts {
            port: Some(details.port()),
            ssl_certfile: Some(ca_cert),
            use_ssl: true,
            validate_ssl: Some(true),
            ..Default::default()
        }))),
        ..Default::default()
    })
    .await;

    match result {
        Ok(_) => {}
        Err(err) => panic!("{err:#}"),
    }
}

#[tokio::test]
async fn test_migrate_tls_password_auth() {
    let details = request_container(DatabaseRequest {
        engine: test_database_engine(),
        security: Some(DatabaseSecurity::Tls {
            password_auth: true,
        }),
    })
    .await;

    let ca_cert = if let Some(DatabaseCerts::StandardTls(tls_certs)) = details.certs() {
        tls_certs.ca_cert.clone()
    } else {
        panic!();
    };

    let result = migrate_cql(MigrateOpts {
        cql_dir: example_cql_dir(),
        connection_init: Some(ConnectionInit::NewSession(Some(ConnectionOpts {
            port: Some(details.port()),
            username: Some("cassandra".into()),
            password: Some("cassandra".into()),
            ssl_certfile: Some(ca_cert),
            use_ssl: true,
            validate_ssl: Some(true),
            ..Default::default()
        }))),
        ..Default::default()
    })
    .await;

    match result {
        Ok(_) => {}
        Err(err) => panic!("{err:#}"),
    }
}

#[tokio::test]
async fn test_migrate_mtls() {
    let details = request_container(DatabaseRequest {
        engine: test_database_engine(),
        security: Some(DatabaseSecurity::Mtls),
    })
    .await;

    let connection_opts = if let Some(DatabaseCerts::MutualTls(mtls_certs)) = details.certs() {
        ConnectionOpts {
            port: Some(details.port()),
            // username: Some("cassandra".into()),
            // password: Some("cassandra".into()),
            ssl_usercert: Some(mtls_certs.client_cert.clone()),
            ssl_userkey: Some(mtls_certs.client_key.clone()),
            use_ssl: true,
            validate_ssl: Some(true),
            ..Default::default()
        }
    } else {
        panic!();
    };

    let result = migrate_cql(MigrateOpts {
        cql_dir: example_cql_dir(),
        connection_init: Some(ConnectionInit::NewSession(Some(connection_opts))),
        ..Default::default()
    })
    .await;

    match result {
        Ok(_) => {}
        Err(err) => panic!("{err:#}"),
    }
}

fn example_cql_dir() -> PathBuf {
    PathBuf::from(var("CARGO_MANIFEST_DIR").unwrap())
        .join("examples")
        .join("cql")
}

pub fn test_database_engine() -> DatabaseEngine {
    match test_var("CQUILL_TEST_DB").as_deref() {
        Some("cassandra") => DatabaseEngine::Cassandra(cassandra_version()),
        Some("scylla") => DatabaseEngine::Scylla(scylla_version()),
        None => Default::default(),
        _ => panic!(),
    }
}

fn cassandra_version() -> CassandraVersion {
    match test_var("CQUILL_TEST_DB_VERSION").as_deref() {
        Some("6") => CassandraVersion::Six,
        Some("5") => CassandraVersion::Five,
        Some("4") => CassandraVersion::Four,
        None => Default::default(),
        _ => panic!(),
    }
}

fn scylla_version() -> ScyllaVersion {
    match test_var("CQUILL_TEST_DB_VERSION").as_deref() {
        Some("2026") => ScyllaVersion::TwentyTwentySix,
        Some("2025") => ScyllaVersion::TwentyTwentyFive,
        Some("6") => ScyllaVersion::Six,
        Some("5") => ScyllaVersion::Five,
        None => Default::default(),
        _ => panic!(),
    }
}

fn test_var(k: &str) -> Option<String> {
    match var(k) {
        Ok(v) => Some(v),
        Err(VarError::NotPresent) => None,
        _ => panic!(),
    }
}
