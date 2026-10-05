use std::{
    env::var,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpStream},
    path::PathBuf,
    process::{ExitCode, exit},
    time::Duration,
};

use cquill::{CqlFile, MigrateError};

#[allow(dead_code)]
pub enum MigrateExample {
    Default,
    PasswordAuth,
}

pub fn ensure_local_db_running(example: MigrateExample) {
    let address = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 9042));
    let timeout = Duration::from_secs(1);
    if !TcpStream::connect_timeout(&address, timeout).is_ok() {
        println!("this example can be ran with a Docker container from `docker-compose.yaml`:\n");
        println!(
            "    docker compose up {} -d --wait\n",
            match example {
                MigrateExample::Default => "scylla_2026",
                MigrateExample::PasswordAuth => "password_auth",
            }
        );
        exit(1);
    }
}

pub fn example_cql_dir() -> PathBuf {
    example_dir_path("cql")
}

pub fn example_dir_path(p: &str) -> PathBuf {
    PathBuf::from(var("CARGO_MANIFEST_DIR").unwrap())
        .join("examples")
        .join(p)
}

pub fn on_migrate_result(result: Result<Vec<CqlFile>, MigrateError>) -> ExitCode {
    match result {
        Err(err) => {
            println!("EXAMPLE ERRORED: {}", err);
            ExitCode::FAILURE
        }
        Ok(migrated_cql_files) => {
            if migrated_cql_files.is_empty() {
                println!("✔ already up-to-date!");
            } else {
                println!(
                    "✔ {} cql file(s) migrated: {}",
                    migrated_cql_files.len(),
                    migrated_cql_files
                        .iter()
                        .map(|f| f.filename.clone())
                        .collect::<Vec<_>>()
                        .join(", ")
                );
            }
            ExitCode::SUCCESS
        }
    }
}
