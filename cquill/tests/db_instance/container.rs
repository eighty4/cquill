use std::{
    collections::HashMap,
    env::var,
    path::{Path, PathBuf},
};

use bollard::plugin::{ContainerCreateBody, HealthConfig, HostConfig, PortBinding};

use crate::{
    DatabaseCerts, DatabaseEngine, DatabaseRequest, DatabaseSecurity,
    db_instance::macros::vec_of_strings,
};

pub const CQL_PORT: &str = "9042/tcp";

const CQLSHRC_TLS: &str = "/cqlshrc.tls";
const CQLSHRC_MTLS: &str = "/cqlshrc.mtls";

const CA_CERT: &str = "ca_cert.pem";
const SERVER_CERT: &str = "server_cert.pem";
const SERVER_KEY: &str = "server_key.pem";
const SERVER_KEYSTORE: &str = "server_keystore.pem";
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

struct DatabaseContainerConfig {
    cmd: Option<Vec<String>>,
    env: Option<Vec<String>>,
    volumes: Option<Vec<String>>,
}

pub fn make_container_create_body(
    req: &DatabaseRequest,
    certs: &Option<DatabaseCerts>,
) -> ContainerCreateBody {
    let container_config = match req.engine {
        DatabaseEngine::Cassandra(_) => cassandra::container_config(req),
        DatabaseEngine::Scylla(_) => scylla::container_config(req),
    };
    ContainerCreateBody {
        image: Some(image(req)),
        host_config: Some(HostConfig {
            port_bindings: Some(port_bindings()),
            binds: merge_volumes(container_config.volumes, volumes(certs)),
            ..Default::default()
        }),
        hostname: Some("apacheway".into()),
        network_disabled: Some(false),
        exposed_ports: Some(vec_of_strings![CQL_PORT]),
        healthcheck: Some(healthcheck(req)),
        env: container_config.env,
        cmd: container_config.cmd,
        ..Default::default()
    }
}

fn merge_volumes(a: Option<Vec<String>>, b: Option<Vec<String>>) -> Option<Vec<String>> {
    match (a, b) {
        (Some(mut a), Some(mut b)) => {
            let mut merged = Vec::new();
            merged.append(&mut a);
            merged.append(&mut b);
            Some(merged)
        }
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        _ => None,
    }
}

fn image(req: &DatabaseRequest) -> String {
    format!("{}:{}", req.engine.image_repo(), req.engine.image_tag())
}

fn port_bindings() -> HashMap<String, Option<Vec<PortBinding>>> {
    HashMap::from([(
        CQL_PORT.to_string(),
        Some(vec![PortBinding {
            host_ip: Some("0.0.0.0".to_string()),
            host_port: None,
        }]),
    )])
}

fn healthcheck(req: &DatabaseRequest) -> HealthConfig {
    let mut test = vec_of_strings!["CMD", "cqlsh"];
    if req.use_password_auth() {
        test.append(&mut vec_of_strings!["-u", "cassandra", "-p", "cassandra"]);
    }
    let tls_cqlshrc = match req.security {
        Some(DatabaseSecurity::Tls { .. }) => Some(CQLSHRC_TLS),
        Some(DatabaseSecurity::Mtls) => Some(CQLSHRC_MTLS),
        _ => None,
    };
    if let Some(cqlshrc) = tls_cqlshrc {
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

fn volumes(certs: &Option<DatabaseCerts>) -> Option<Vec<String>> {
    match certs {
        Some(DatabaseCerts::StandardTls(tls_certs)) => Some(vec_of_strings![
            cqlshrc_volume(CQLSHRC_TLS),
            cert_volume(&tls_certs.ca_cert, CA_CERT),
            cert_volume(&tls_certs.server_cert, SERVER_CERT),
            cert_volume(&tls_certs.server_key, SERVER_KEY),
            cert_volume(&tls_certs.server_keystore, SERVER_KEYSTORE),
        ]),
        Some(DatabaseCerts::MutualTls(mtls_certs)) => Some(vec_of_strings![
            cqlshrc_volume(CQLSHRC_MTLS),
            cert_volume(&mtls_certs.ca_cert, CA_CERT),
            cert_volume(&mtls_certs.server_cert, SERVER_CERT),
            cert_volume(&mtls_certs.server_key, SERVER_KEY),
            cert_volume(&mtls_certs.server_keystore, SERVER_KEYSTORE),
            cert_volume(&mtls_certs.client_truststore, CLIENT_TRUSTSTORE),
            cert_volume(&mtls_certs.client_cert, CLIENT_CERT),
            cert_volume(&mtls_certs.client_key, CLIENT_KEY),
        ]),
        _ => None,
    }
}

mod cassandra {

    use crate::CassandraVersion;

    use super::*;

    pub fn container_config(req: &DatabaseRequest) -> DatabaseContainerConfig {
        DatabaseContainerConfig {
            cmd: cmd(req),
            env: Some(vec_of_strings!["CASSANDRA_BROADCAST_ADDRESS=127.0.0.1"]),
            volumes: None,
        }
    }

    fn cmd(req: &DatabaseRequest) -> Option<Vec<String>> {
        let sed_commands = cassandra_yaml_sed_commands(req);
        if sed_commands.is_empty() {
            None
        } else {
            let cassandra_command = "docker-entrypoint.sh cassandra -f";
            let shell_command = if sed_commands.is_empty() {
                cassandra_command.to_string()
            } else {
                format!("{} && {cassandra_command}", sed_commands.join(" && "))
            };
            Some(vec_of_strings!["sh", "-c", shell_command])
        }
    }

    fn cassandra_yaml_sed_commands(req: &DatabaseRequest) -> Vec<String> {
        let is_cassandra_six =
            matches!(req.engine, DatabaseEngine::Cassandra(CassandraVersion::Six));
        let mut sed_commands: Vec<String> = Vec::new();
        if req.use_password_auth() {
            if is_cassandra_six {
                sed_commands.push(
                    "sed -z -i 's/authenticator:\\n  class_name:[^\\n]*/authenticator:\\n  class_name: PasswordAuthenticator/' /etc/cassandra/cassandra.yaml".into(),
                );
            } else {
                sed_commands.push(
                    "sed -i 's/^authenticator:.*/authenticator: PasswordAuthenticator/' /etc/cassandra/cassandra.yaml".into(),
                );
            }
        }
        match req.security {
            Some(DatabaseSecurity::Tls { .. }) | Some(DatabaseSecurity::Mtls) => {
                let (truststore, require_client_auth) =
                    match matches!(req.security, Some(DatabaseSecurity::Mtls)) {
                        true => (CLIENT_TRUSTSTORE, "true"),
                        false => (SERVER_KEYSTORE, "false"),
                    };
                let replacement =
                    format!("\\n{}\\n", [
                  "  ssl_context_factory:",
                  "    class_name: org.apache.cassandra.security.PEMBasedSslContextFactory",
                  format!("  keystore: \\/{SERVER_KEYSTORE}").as_str(),
                  format!("  truststore: \\/{truststore}").as_str(),
                  "  enabled: true",
                  format!("  require_client_auth: {require_client_auth}").as_str(),
                  ].join("\\n"));
                sed_commands.push(format!("sed -i '/client_encryption_options:/{{:a;N;/\\n$/!ba;s/:.*/:{replacement}/}}' /etc/cassandra/cassandra.yaml"))
            }
            _ => {}
        }
        sed_commands
    }
}

mod scylla {
    use crate::ScyllaVersion;

    use super::*;

    pub fn container_config(req: &DatabaseRequest) -> DatabaseContainerConfig {
        DatabaseContainerConfig {
            cmd: Some(cmd(req)),
            env: None,
            volumes: None,
        }
    }

    fn cmd(req: &DatabaseRequest) -> Vec<String> {
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
        ]);
        let is_scylla_5 = matches!(req.engine, DatabaseEngine::Scylla(ScyllaVersion::Five));
        if !is_scylla_5 {
            scylla_flags.push("--default-log-level=error".into());
        }
        if req.use_password_auth() {
            scylla_flags.append(&mut vec_of_strings![
                "--authenticator",
                "PasswordAuthenticator",
            ]);
        }
        if req.security.as_ref().is_some_and(|s| s.uses_tls()) {
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
        scylla_flags
    }
}
