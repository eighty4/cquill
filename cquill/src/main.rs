use std::env;
use std::ops::Deref;
use std::path::PathBuf;

use clap::{Parser, Subcommand};

use cquill::MigrateError::HistoryUpdateFailed;
use cquill::{
    ConnectionInit, ConnectionOpts, CqlFile, CqlshrcOpts, MigrateError,
    MigrateError::PartialMigration, MigrateErrorState, MigrateOpts, keyspace::*, migrate_cql,
};
use regex::Regex;

#[derive(Parser)]
#[command(author, version, about)]
struct CquillCli {
    #[command(subcommand)]
    command: CquillCommand,
}

#[derive(Subcommand)]
enum CquillCommand {
    Migrate(MigrateCliArgs),
}

#[derive(Parser)]
struct MigrateCliArgs {
    #[clap(short = 'd', long, value_name = "CQL_DIR", default_value = "./cql")]
    cql_dir: PathBuf,
    /// [default: ~/.cassandra/cqlshrc]
    #[clap(long, num_args(0..=1), value_name = "CQLSHRC_PATH", default_value = None)]
    cqlshrc: Option<Option<PathBuf>>,
    #[clap(long, value_name = "HISTORY_KEYSPACE", default_value = cquill::KEYSPACE)]
    history_keyspace: String,
    #[clap(long, value_name = "HISTORY_REPLICATION", default_value = cquill::keyspace::REPLICATION)]
    history_replication: String,
    #[clap(long, value_name = "HISTORY_TABLE", default_value = cquill::TABLE)]
    history_table: String,
    /// [default: 127.0.0.1:9042]
    #[clap(short = 'a', long, value_name = "ADDRESS", value_parser = validate_address)]
    address: Option<String>,
    #[clap(short = 'u', long, value_name = "USERNAME")]
    username: Option<String>,
    #[clap(short = 'p', long, value_name = "PASSWORD")]
    password: Option<String>,
}

fn validate_address(s: &str) -> Result<String, String> {
    let pattern = r"^[a-zA-Z-_\.\d]+(:\d{1,5})?$";
    if Regex::new(pattern).unwrap().is_match(s) {
        Ok(s.to_string())
    } else {
        Err("--address must be a valid hostname or ipv4 address with optional port".into())
    }
}

impl TryFrom<MigrateCliArgs> for MigrateOpts {
    type Error = MigrateError;

    fn try_from(cli_args: MigrateCliArgs) -> Result<Self, Self::Error> {
        let replication_factor = match cli_args.history_replication.parse::<ReplicationFactor>() {
            Ok(replication_factor) => replication_factor,
            Err(err) => return Err(MigrateError::from(err)),
        };
        let (hostname, port) = match cli_args.address {
            None => (None, None),
            Some(address) => match address.split_once(':') {
                None => (Some(address), None),
                Some((hostname, port)) => (Some(hostname.into()), Some(port.parse().unwrap())),
            },
        };
        let connection_opts = ConnectionOpts {
            hostname,
            port,
            connection_timeout: None,
            username: cli_args.username,
            password: cli_args.password,
        };
        let connection_init = Some(match cli_args.cqlshrc {
            None => ConnectionInit::SimpleTcp(Some(connection_opts)),
            Some(cqlshrc) => ConnectionInit::Cqlshrc(CqlshrcOpts {
                path: cqlshrc,
                overrides: connection_opts,
            }),
        });
        Ok(MigrateOpts {
            connection_init,
            cql_dir: cli_args.cql_dir,
            history_keyspace: Some(KeyspaceOpts {
                name: cli_args.history_keyspace,
                replication: Some(replication_factor),
            }),
            history_table: Some(cli_args.history_table),
        })
    }
}

#[tokio::main]
async fn main() {
    let cquill_cli = CquillCli::parse();
    match cquill_cli.command {
        CquillCommand::Migrate(args) => migrate(args).await,
    };
}

async fn migrate(args: MigrateCliArgs) {
    let opts = match MigrateOpts::try_from(args) {
        Ok(opts) => opts,
        Err(err) => error_exit(err),
    };
    let version = env!("CARGO_PKG_VERSION");
    let cql_dir = opts.cql_dir.to_string_lossy();
    println!("CQuill {version}\nMigrating CQL files from {cql_dir}");
    match migrate_cql(opts).await {
        Ok(migrated_cql) => print_migrated_cql(&migrated_cql),
        Err(err) => match err {
            HistoryUpdateFailed {
                error_state,
                cquill_keyspace,
                cquill_table,
            } => history_update_failed_exit(error_state.deref(), cquill_keyspace, cquill_table),
            PartialMigration { error_state } => partial_migrate_error_exit(error_state.deref()),
            _ => error_exit(err),
        },
    }
}

fn print_migrated_cql(migrated_cql: &[CqlFile]) {
    if migrated_cql.is_empty() {
        println!("✔ already up to date");
    } else if migrated_cql.len() == 1 {
        println!("✔ 1 cql file migrated: {}", migrated_cql[0].filename);
    } else {
        println!("✔ {} cql files migrated:", migrated_cql.len());
        migrated_cql.iter().for_each(|p| {
            println!("  {}", p.filename);
        });
    }
}

fn error_prefix() -> String {
    // hex \x1b -> octal \033
    //        0 -> reset
    //       31 -> red foreground
    //        1 -> bold
    "\x1b[0;31;1merror:\x1b[0m".to_string()
}

fn history_update_failed_exit(
    error_state: &MigrateErrorState,
    cquill_keyspace: String,
    cquill_table: String,
) {
    if !error_state.migrated.is_empty() {
        print_migrated_cql(&error_state.migrated);
    }
    println!(
        "\nUpdating CQuill's migration history table failed after executing the CQL from {}.",
        error_state.failed_file
    );
    println!("{} {}", error_prefix(), error_state.error);
    println!("\n===IMPORTANT===");
    println!(
        "`cquill migrate` must not be run until {} is added to the {}.{} history table.",
        error_state.failed_file, cquill_keyspace, cquill_table,
    );
    println!("===============");
}

fn partial_migrate_error_exit(error_state: &MigrateErrorState) {
    if !error_state.migrated.is_empty() {
        print_migrated_cql(&error_state.migrated);
    }
    match &error_state.failed_cql {
        None => println!("Migrate failed during {}", error_state.failed_file),
        Some(failed_cql) => {
            println!(
                "\nMigrate failed during {} ({}) on the CQL statement:\n    {}",
                error_state.failed_file,
                if failed_cql.lines.0 == failed_cql.lines.1 {
                    format!("line {}", failed_cql.lines.0)
                } else if failed_cql.lines.1 - failed_cql.lines.0 == 1 {
                    format!("lines {} and {}", failed_cql.lines.0, failed_cql.lines.1)
                } else {
                    format!("lines {} to {}", failed_cql.lines.0, failed_cql.lines.1)
                },
                failed_cql.cql
            );
        }
    }
    println!("{} {}", error_prefix(), error_state.error);
    println!("\n===IMPORTANT===");
    println!(
        "CQL statements before this statement in {} were successfully executed.",
        error_state.failed_file
    );
    println!(
        "The remaining statements will need to be manually executed and {} must be added to CQuill's history table with the CQL file's content hash.",
        error_state.failed_file
    );
    println!("===============");
    std::process::exit(1);
}

fn error_exit(err: MigrateError) -> ! {
    println!("{} {err}", error_prefix());
    std::process::exit(1);
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn test_cli_parse_cql_dir() {
        assert_eq!(
            MigrateCliArgs::try_parse_from(["migrate"]).unwrap().cql_dir,
            PathBuf::from("./cql")
        );
        assert_eq!(
            MigrateCliArgs::try_parse_from(["migrate", "--cql-dir", "/da/root/cql"])
                .unwrap()
                .cql_dir,
            PathBuf::from("/da/root/cql")
        );
    }

    #[test]
    fn test_cli_parse_history_keyspace() {
        assert_eq!(
            MigrateCliArgs::try_parse_from(["migrate"])
                .unwrap()
                .history_keyspace,
            String::from("cquill")
        );
        assert_eq!(
            MigrateCliArgs::try_parse_from(["migrate", "--history-keyspace", "o"])
                .unwrap()
                .history_keyspace,
            PathBuf::from("o")
        );
    }

    #[test]
    fn test_cli_parse_history_table() {
        assert_eq!(
            MigrateCliArgs::try_parse_from(["migrate"])
                .unwrap()
                .history_table,
            String::from("migrated_cql")
        );
        assert_eq!(
            MigrateCliArgs::try_parse_from(["migrate", "--history-table", "black_mesa"])
                .unwrap()
                .history_table,
            PathBuf::from("black_mesa")
        );
    }

    #[test]
    fn test_cli_parse_history_replication() {
        assert_eq!(
            MigrateCliArgs::try_parse_from(["migrate"])
                .unwrap()
                .history_replication,
            String::from("{ 'class': 'SimpleStrategy', 'replication_factor': 1 }")
        );
        assert_eq!(
            MigrateCliArgs::try_parse_from(["migrate", "--history-replication", "{}"])
                .unwrap()
                .history_replication,
            PathBuf::from("{}")
        );
    }

    #[test]
    fn test_cli_parse_address() {
        assert!(
            MigrateCliArgs::try_parse_from(["migrate", "-a", "127.0.0.1:benedictCumberbatch"])
                .is_err()
        );
        assert!(MigrateCliArgs::try_parse_from(["migrate", "-a", "127.0.0.1:9042"]).is_ok());
    }

    #[test]
    fn test_cli_parse_cqlshrc_path() {
        assert_eq!(
            MigrateCliArgs::try_parse_from(["migrate"]).unwrap().cqlshrc,
            None
        );
        assert_eq!(
            MigrateCliArgs::try_parse_from(["migrate", "--cqlshrc"])
                .unwrap()
                .cqlshrc,
            Some(None)
        );
        assert_eq!(
            MigrateCliArgs::try_parse_from(["migrate", "--cqlshrc", "/da/root/cqlshrc"])
                .unwrap()
                .cqlshrc,
            Some(Some("/da/root/cqlshrc".into()))
        );
    }

    #[test]
    fn test_cli_args_into_migrate_ops_without_address() {
        let connection_init =
            MigrateOpts::try_from(MigrateCliArgs::try_parse_from(["migrate"]).unwrap())
                .unwrap()
                .connection_init;
        match connection_init {
            Some(ConnectionInit::SimpleTcp(Some(connection_opts))) => {
                assert!(connection_opts.hostname.is_none());
                assert!(connection_opts.port.is_none());
            }
            _ => panic!(),
        };
    }

    #[test]
    fn test_cli_args_into_migrate_ops_with_hostname() {
        let connection_init = MigrateOpts::try_from(
            MigrateCliArgs::try_parse_from(["migrate", "-a", "swissfjord"]).unwrap(),
        )
        .unwrap()
        .connection_init;
        match connection_init {
            Some(ConnectionInit::SimpleTcp(Some(connection_opts))) => {
                assert_eq!(connection_opts.hostname, Some("swissfjord".into()));
                assert!(connection_opts.port.is_none());
            }
            _ => panic!(),
        };
    }

    #[test]
    fn test_cli_args_into_migrate_ops_with_hostname_and_port() {
        let connection_init = MigrateOpts::try_from(
            MigrateCliArgs::try_parse_from(["migrate", "-a", "swissfjord:31735"]).unwrap(),
        )
        .unwrap()
        .connection_init;
        match connection_init {
            Some(ConnectionInit::SimpleTcp(Some(connection_opts))) => {
                assert_eq!(connection_opts.hostname, Some("swissfjord".into()));
                assert_eq!(connection_opts.port, Some(31735));
            }
            _ => panic!(),
        };
    }

    #[test]
    fn test_cli_args_into_migrate_ops_invalid_replication_errors() {
        assert!(
            MigrateOpts::try_from(
                MigrateCliArgs::try_parse_from([
                    "migrate",
                    "--history-replication",
                    "baldwinBrothers"
                ])
                .unwrap()
            )
            .is_err()
        );
    }

    #[test]
    fn test_cli_args_into_migrate_ops_transforms_simple_replication() {
        match MigrateOpts::try_from(
            MigrateCliArgs::try_parse_from([
                "migrate",
                "--history-replication",
                cquill::keyspace::REPLICATION,
            ])
            .unwrap(),
        )
        .unwrap()
        .history_keyspace
        .unwrap()
        .replication
        .unwrap()
        {
            ReplicationFactor::SimpleStrategy { factor: 1 } => (),
            _ => panic!(),
        };
    }

    #[test]
    fn test_cli_args_into_migrate_ops_transforms_datacenter_replication() {
        match MigrateOpts::try_from(
            MigrateCliArgs::try_parse_from([
                "migrate",
                "--history-replication",
                "{'class': 'NetworkTopologyStrategy', 'dc1': 2, 'dc2': 3}",
            ])
            .unwrap(),
        )
        .unwrap()
        .history_keyspace
        .unwrap()
        .replication
        .unwrap()
        {
            ReplicationFactor::NetworkTopologyStrategy { datacenter_factors } => {
                assert_eq!(
                    datacenter_factors,
                    HashMap::from([("dc1".into(), 2), ("dc2".into(), 3)])
                );
            }
            _ => panic!(),
        };
    }

    #[test]
    fn test_validate_address_returns_valid() {
        assert_eq!(validate_address("127.0.0.1"), Ok("127.0.0.1".into()));
    }

    #[test]
    fn test_validate_address_address_port() {
        assert!(validate_address("127.0.0.1:90kevinBacon").is_err());
        assert!(validate_address("127.0.0.1:9042").is_ok());
        assert!(validate_address("127.0.0.1").is_ok());
    }

    #[test]
    fn test_validate_address_dns_hostname_address() {
        assert!(validate_address("us-east-1.test.scylla.swissfjord").is_ok());
        assert!(validate_address("us-east-1.test.scylla.swissfjord:9042").is_ok());
        assert!(validate_address("swissfjord").is_ok());
        assert!(validate_address("swissfjord:9042").is_ok());
    }
}
