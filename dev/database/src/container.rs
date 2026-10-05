use std::{
    collections::HashMap,
    env::var,
    path::{Path, PathBuf},
};

use bollard::plugin::{ContainerCreateBody, HealthConfig, HostConfig, PortBinding};

use crate::{DatabaseCerts, DatabaseRequest, DatabaseSecurity, macros::vec_of_strings};

pub const CQL_PORT: &str = "9042/tcp";

const CQLSHRC_TLS: &str = "/cqlshrc.tls";
const CQLSHRC_MTLS: &str = "/cqlshrc.mtls";

const CA_CERT: &str = "ca_cert.pem";
const SERVER_CERT: &str = "server_cert.pem";
const SERVER_KEY: &str = "server_key.pem";
const CLIENT_TRUSTSTORE: &str = "truststore.pem";
const CLIENT_CERT: &str = "client_cert.pem";
const CLIENT_KEY: &str = "client_key.pem";

fn cert_volume(path_from: &Path, filename_to: &str) -> String {
    format!(
        "{}:{}:ro",
        path_from.to_string_lossy(),
        cert_container_path(filename_to)
    )
}

fn cert_container_path(filename: &str) -> String {
    format!("/{filename}")
}

fn cqlshrc_host_path(cqlshrc: &str) -> PathBuf {
    PathBuf::from(var("CARGO_MANIFEST_DIR").unwrap()).join(match cqlshrc {
        CQLSHRC_TLS => "tests/cqlshrc/tls.ini",
        CQLSHRC_MTLS => "tests/cqlshrc/mtls.ini",
        _ => panic!(),
    })
}

fn cqlshrc_volume(cqlshrc: &str) -> String {
    format!(
        "{}:{cqlshrc}:ro",
        cqlshrc_host_path(cqlshrc).to_string_lossy()
    )
}

pub fn make_container_create_body(
    req: &DatabaseRequest,
    certs: &Option<DatabaseCerts>,
) -> ContainerCreateBody {
    let mut port_bindings = HashMap::new();
    port_bindings.insert(
        CQL_PORT.to_string(),
        Some(vec![PortBinding {
            host_ip: Some("0.0.0.0".to_string()),
            host_port: None,
        }]),
    );
    ContainerCreateBody {
        image: Some(container_image(req)),
        host_config: Some(HostConfig {
            port_bindings: Some(port_bindings),
            binds: container_volumes(certs),
            ..Default::default()
        }),
        hostname: Some("apacheway".into()),
        network_disabled: Some(false),
        exposed_ports: Some(vec_of_strings![CQL_PORT]),
        healthcheck: Some(container_healthcheck(req)),
        env: container_env(req),
        cmd: container_cmd(req),
        ..Default::default()
    }
}

fn container_image(req: &DatabaseRequest) -> String {
    format!("{}:{}", req.engine.image_repo(), req.engine.image_tag())
}

fn container_cmd(req: &DatabaseRequest) -> Option<Vec<String>> {
    if req.engine.is_scylla() {
        let mut scylla_flags: Vec<String> = if req.is_os_macos() {
            vec_of_strings!["--reactor-backend=epoll"]
        } else {
            Vec::new()
        };
        scylla_flags.append(&mut vec_of_strings![
            "--listen-address",
            "0.0.0.0",
            "--broadcast-rpc-address",
            "127.0.0.1",
            "--default-log-level=error",
        ]);
        if req.use_password_auth() {
            scylla_flags.append(&mut vec_of_strings![
                "--authenticator",
                "PasswordAuthenticator",
            ]);
        }
        if req.security.as_ref().is_some_and(|s| s.uses_ssl()) {
            scylla_flags.append(&mut vec_of_strings!["--native-transport-port-ssl", "9042",]);
        }
        if let Some(DatabaseSecurity::Tls { .. }) = req.security {
            scylla_flags.append(&mut vec_of_strings![
                "--client-encryption-options",
                format!(
                    "enabled=true:optional=false:require_client_auth=false:certificate={}:keyfile={}",
                    cert_container_path(SERVER_CERT),
                    cert_container_path(SERVER_KEY),
                ),
            ]);
        }
        if let Some(DatabaseSecurity::Mtls) = req.security {
            scylla_flags.append(&mut vec_of_strings![
                "--client-encryption-options",
                format!(
                    "enabled=true:optional=false:require_client_auth=true:certificate={}:keyfile={}:truststore={}",
                    cert_container_path(SERVER_CERT),
                    cert_container_path(SERVER_KEY),
                    cert_container_path(CLIENT_TRUSTSTORE),
                ),
            ]);
        }
        Some(scylla_flags)
    } else {
        None
    }
}

fn container_healthcheck(req: &DatabaseRequest) -> HealthConfig {
    let mut test = vec_of_strings!["CMD", "cqlsh"];
    if req.use_password_auth() {
        test.append(&mut vec_of_strings!["-u", "cassandra", "-p", "cassandra"]);
    }
    let ssl_cqlshrc = match req.security {
        Some(DatabaseSecurity::Tls { .. }) => Some(CQLSHRC_TLS),
        Some(DatabaseSecurity::Mtls) => Some(CQLSHRC_MTLS),
        _ => None,
    };
    if let Some(cqlshrc) = ssl_cqlshrc {
        test.append(&mut vec_of_strings!["--ssl", "--cqlshrc", cqlshrc]);
    };
    test.append(&mut vec_of_strings!["-e", "describe cluster"]);
    let nps = 1_000_000_000;
    HealthConfig {
        test: Some(test),
        start_period: Some(10 * nps),
        start_interval: Some(nps),
        interval: Some(5 * nps),
        timeout: Some(5 * nps),
        retries: Some(2 * nps),
    }
}

fn container_env(req: &DatabaseRequest) -> Option<Vec<String>> {
    if req.use_password_auth() && req.engine.is_cassandra() {
        Some(vec_of_strings![
            "CASSANDRA_BROADCAST_ADDRESS=127.0.0.1",
            "-Dcassandra.settings.authenticator=PasswordAuthenticator",
            "-Dcassandra.settings.role_manager=CassandraRoleManager"
        ])
    } else {
        None
    }
}

fn container_volumes(certs: &Option<DatabaseCerts>) -> Option<Vec<String>> {
    match certs {
        Some(DatabaseCerts::StandardTls(tls_certs)) => Some(vec_of_strings![
            cqlshrc_volume(CQLSHRC_TLS),
            cert_volume(&tls_certs.ca_cert, CA_CERT),
            cert_volume(&tls_certs.server_cert, SERVER_CERT),
            cert_volume(&tls_certs.server_key, SERVER_KEY),
        ]),
        Some(DatabaseCerts::MutualTls(mtls_certs)) => Some(vec_of_strings![
            cqlshrc_volume(CQLSHRC_MTLS),
            cert_volume(&mtls_certs.ca_cert, CA_CERT),
            cert_volume(&mtls_certs.server_cert, SERVER_CERT),
            cert_volume(&mtls_certs.server_key, SERVER_KEY),
            cert_volume(&mtls_certs.client_truststore, CLIENT_TRUSTSTORE),
            cert_volume(&mtls_certs.client_cert, CLIENT_CERT),
            cert_volume(&mtls_certs.client_key, CLIENT_KEY),
        ]),
        _ => None,
    }
}
