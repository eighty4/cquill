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
    /// Explicit opt-in [default: ~/.cassandra/cqlshrc]
    #[clap(long, num_args(0..=1), value_name = "CQLSHRC_PATH", default_value = None)]
    cqlshrc: Option<Option<PathBuf>>,
    /// [default: 5]
    #[clap(long, value_name = "SECONDS")]
    connection_timeout: Option<u16>,
    #[clap(long, value_name = "KEYSPACE", default_value = cquill::KEYSPACE, value_parser = validate_keyspace)]
    history_keyspace: String,
    /// [default: {'class':'SimpleStrategy','factor':1}]
    #[clap(long, value_name = "REPLICATION", value_parser = parse_replication)]
    history_replication: Option<ReplicationFactor>,
    #[clap(long, value_name = "TABLE", default_value = cquill::TABLE, value_parser = validate_table)]
    history_table: String,
    /// [default: 127.0.0.1:9042]
    #[clap(short = 'a', long, value_name = "ADDRESS", value_parser = validate_address)]
    address: Option<String>,
    #[clap(short = 'u', long, value_name = "USERNAME", value_parser = validate_username)]
    username: Option<String>,
    #[clap(short = 'p', long, value_name = "PASSWORD")]
    password: Option<String>,
    /// Explicit opt-in required for SSL [default: false]
    #[clap(long = "ssl", default_value_t = false)]
    ssl: bool,
    /// [default: true (or value from cqlshrc)]
    #[clap(
        long = "ssl-validate",
        value_name = "true|false",
        hide_possible_values = true
    )]
    ssl_validate: Option<bool>,
    #[clap(long, value_name = "CERTFILE")]
    ssl_cert: Option<PathBuf>,
    #[clap(long, value_name = "USERCERT")]
    ssl_usercert: Option<PathBuf>,
    #[clap(long, value_name = "USERKEY")]
    ssl_userkey: Option<PathBuf>,
}

fn parse_replication(s: &str) -> Result<ReplicationFactor, String> {
    s.parse::<ReplicationFactor>()
        .map_err(|err| err.to_string())
}

fn validate_address(s: &str) -> Result<String, String> {
    let pattern = r"^[a-zA-Z-_\.\d]+(:\d{1,5})?$";
    if Regex::new(pattern).unwrap().is_match(s) {
        Ok(s.to_string())
    } else {
        Err("--address must be a valid hostname or ipv4 address with optional port".into())
    }
}

fn validate_keyspace(s: &str) -> Result<String, String> {
    let p = if s.starts_with('"') {
        r#"^"[a-zA-Z\d][a-zA-Z\d_]{0,47}"$"#
    } else {
        r"^[a-z\d][a-z\d_]{0,47}$"
    };
    if Regex::new(p).unwrap().is_match(s) {
        Ok(s.to_string())
    } else {
        Err("--history-keyspace must be a valid keyspace identifier".into())
    }
}

fn validate_table(s: &str) -> Result<String, String> {
    let p = if s.starts_with('"') {
        r#"^"[a-zA-Z\d][a-zA-Z\d_]{0,221}"$"#
    } else {
        r"^[a-z\d][a-z\d_]{0,221}$"
    };
    if Regex::new(p).unwrap().is_match(s) {
        Ok(s.to_string())
    } else {
        Err("--history-table must be a valid table identifier".into())
    }
}

fn validate_username(s: &str) -> Result<String, String> {
    let p = if s.starts_with('\'') {
        r"^'.{1,256}'$"
    } else if s.starts_with('"') {
        r#"^"[a-zA-Z\d][a-zA-Z\d_]{0,255}"$"#
    } else {
        r"^[a-z\d][a-z\d_]{0,255}$"
    };
    if Regex::new(p).unwrap().is_match(s) {
        Ok(s.to_string())
    } else {
        Err("--username must be a valid username identifier".into())
    }
}

impl From<MigrateCliArgs> for MigrateOpts {
    fn from(cli_args: MigrateCliArgs) -> Self {
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
            connection_timeout: cli_args.connection_timeout,
            username: cli_args.username,
            password: cli_args.password,
            ssl_certfile: cli_args.ssl_cert,
            ssl_usercert: cli_args.ssl_usercert,
            ssl_userkey: cli_args.ssl_userkey,
            use_ssl: cli_args.ssl,
            validate_ssl: cli_args.ssl_validate,
        };
        let connection_init = Some(match cli_args.cqlshrc {
            None => ConnectionInit::NewSession(Some(connection_opts)),
            Some(cqlshrc) => ConnectionInit::Cqlshrc(CqlshrcOpts {
                path: cqlshrc,
                overrides: connection_opts,
            }),
        });
        MigrateOpts {
            connection_init,
            cql_dir: cli_args.cql_dir,
            history_keyspace: Some(KeyspaceOpts {
                name: cli_args.history_keyspace,
                replication: cli_args.history_replication.unwrap_or_default(),
            }),
            history_table: Some(cli_args.history_table),
        }
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
    let opts = MigrateOpts::from(args);
    println!(
        "cquill {}\nmigrating CQL files from directory `{}`",
        env!("CARGO_PKG_VERSION"),
        opts.cql_dir.to_string_lossy()
    );
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
    red_bold("error:")
}

fn exclamation_prefix() -> String {
    red_bold("!")
}

fn red_bold(s: &str) -> String {
    // hex \x1b -> octal \033
    //        0 -> reset
    //       31 -> red foreground
    //        1 -> bold
    format!("\x1b[0;31;1m{s}\x1b[0m")
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
        "\nUpdating CQuill's migration history table failed after executing the CQL from `{}`.",
        error_state.failed_file
    );
    println!("{} {}", error_prefix(), error_state.error);
    println!("\n===IMPORTANT===");
    println!(
        "`cquill migrate` must not be run until `{}` is added to the `{}.{}` history table.",
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
                "\nMigrate failed during `{}` ({}) on the CQL statement:\n    {}",
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
        "CQL statements before this statement in `{}` were successfully executed.",
        error_state.failed_file
    );
    println!(
        "The remaining statements will need to be manually executed and `{}` must be added to CQuill's history table with the CQL file's content hash.",
        error_state.failed_file
    );
    println!("===============");
    std::process::exit(1);
}

fn error_exit(err: MigrateError) -> ! {
    if let MigrateError::Other { source } = err {
        println!("{} {source}", error_prefix());
        if source.chain().count() > 1 {
            println!();
            for cause in source.chain().skip(1) {
                println!("     {} {cause}", exclamation_prefix());
            }
            println!();
        }
    } else {
        println!("{} {err:#}", error_prefix());
    }
    std::process::exit(1);
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::iter::repeat_n;

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
            String::from("o")
        );
        assert!(
            MigrateCliArgs::try_parse_from(["migrate", "--history-keyspace", "_emperor_of_mexico"])
                .is_err()
        );
        assert_eq!(
            MigrateCliArgs::try_parse_from([
                "migrate",
                "--history-keyspace",
                r#""EmperorOfMexico""#
            ])
            .unwrap()
            .history_keyspace,
            String::from(r#""EmperorOfMexico""#)
        );
        assert!(
            MigrateCliArgs::try_parse_from([
                "migrate",
                "--history-keyspace",
                r#""_EmperorOfMexico""#
            ])
            .is_err()
        );
    }

    #[test]
    fn test_cli_parse_username() {
        for valid in [
            "'$'",
            r#""2_Chainz""#,
            "2_chainz",
            &repeat_n('x', 256).collect::<String>(),
            &format!("\"{}\"", repeat_n('X', 256).collect::<String>()),
            &format!("'{}'", repeat_n('$', 256).collect::<String>()),
        ] {
            assert_eq!(
                MigrateCliArgs::try_parse_from(["migrate", "-u", valid])
                    .unwrap()
                    .username,
                Some(String::from(valid))
            );
        }
        for error in [
            "$",
            "2_Chainz",
            "_2_chainz",
            "2_Chain$",
            r#""_2_chainz""#,
            r#""_2_chain$""#,
            &repeat_n('x', 257).collect::<String>(),
            &format!("\"{}\"", repeat_n('X', 257).collect::<String>()),
            &format!("'{}'", repeat_n('$', 257).collect::<String>()),
        ] {
            assert!(MigrateCliArgs::try_parse_from(["migrate", "-u", error]).is_err());
        }
    }

    #[test]
    fn test_cli_parse_history_table() {
        assert_eq!(
            MigrateCliArgs::try_parse_from(["migrate"])
                .unwrap()
                .history_table,
            String::from("migrated_cql")
        );
        for valid in [
            "black_mesa",
            r#""blackMesa""#,
            &repeat_n('x', 222).collect::<String>(),
            &format!("\"{}\"", repeat_n('X', 222).collect::<String>()),
        ] {
            assert_eq!(
                MigrateCliArgs::try_parse_from(["migrate", "--history-table", valid])
                    .unwrap()
                    .history_table,
                String::from(valid)
            );
        }
        for error in [
            "black mesa",
            "blackMesa",
            "_black_mesa",
            r#""black_me$a""#,
            &repeat_n('x', 223).collect::<String>(),
            &format!("\"{}\"", repeat_n('X', 223).collect::<String>()),
        ] {
            assert!(MigrateCliArgs::try_parse_from(["migrate", "--history-table", error]).is_err());
        }
    }

    #[test]
    fn test_cli_parse_history_replication() {
        assert!(
            MigrateCliArgs::try_parse_from(["migrate"])
                .unwrap()
                .history_replication
                .is_none()
        );
        assert!(
            MigrateCliArgs::try_parse_from(["migrate", "--history-replication", "{}"]).is_err()
        );
        assert!(
            MigrateCliArgs::try_parse_from([
                "migrate",
                "--history-replication",
                "{'class':'SimpleStrategy', 'replication_factor': 1}"
            ])
            .is_ok()
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
            MigrateOpts::from(MigrateCliArgs::try_parse_from(["migrate"]).unwrap()).connection_init;
        match connection_init {
            Some(ConnectionInit::NewSession(Some(connection_opts))) => {
                assert!(connection_opts.hostname.is_none());
                assert!(connection_opts.port.is_none());
            }
            _ => panic!(),
        };
    }

    #[test]
    fn test_cli_args_into_migrate_ops_with_hostname() {
        let connection_init = MigrateOpts::from(
            MigrateCliArgs::try_parse_from(["migrate", "-a", "swissfjord"]).unwrap(),
        )
        .connection_init;
        match connection_init {
            Some(ConnectionInit::NewSession(Some(connection_opts))) => {
                assert_eq!(connection_opts.hostname, Some("swissfjord".into()));
                assert!(connection_opts.port.is_none());
            }
            _ => panic!(),
        };
    }

    #[test]
    fn test_cli_args_into_migrate_ops_with_hostname_and_port() {
        let connection_init = MigrateOpts::from(
            MigrateCliArgs::try_parse_from(["migrate", "-a", "swissfjord:31735"]).unwrap(),
        )
        .connection_init;
        match connection_init {
            Some(ConnectionInit::NewSession(Some(connection_opts))) => {
                assert_eq!(connection_opts.hostname, Some("swissfjord".into()));
                assert_eq!(connection_opts.port, Some(31735));
            }
            _ => panic!(),
        };
    }

    #[test]
    fn test_cli_args_into_migrate_ops_with_connection_timeout() {
        let connection_init = MigrateOpts::from(
            MigrateCliArgs::try_parse_from(["migrate", "--connection-timeout", "4"]).unwrap(),
        )
        .connection_init;
        match connection_init {
            Some(ConnectionInit::NewSession(Some(connection_opts))) => {
                assert_eq!(connection_opts.connection_timeout, Some(4));
            }
            _ => panic!(),
        };
    }

    #[test]
    fn test_cli_args_into_migrate_ops_with_ssl_opts() {
        let connection_init = MigrateOpts::from(
            MigrateCliArgs::try_parse_from([
                "migrate",
                "--ssl",
                "--ssl-cert",
                "/certfile",
                "--ssl-usercert",
                "/usercert",
                "--ssl-userkey",
                "/userkey",
            ])
            .unwrap(),
        )
        .connection_init;
        match connection_init {
            Some(ConnectionInit::NewSession(Some(connection_opts))) => {
                assert!(connection_opts.use_ssl);
                assert_eq!(connection_opts.validate_ssl, None);
                assert_eq!(connection_opts.ssl_certfile, Some("/certfile".into()));
                assert_eq!(connection_opts.ssl_usercert, Some("/usercert".into()));
                assert_eq!(connection_opts.ssl_userkey, Some("/userkey".into()));
            }
            _ => panic!(),
        };
    }

    #[test]
    fn test_cli_args_into_migrate_ops_with_ssl_no_validate() {
        let connection_init = MigrateOpts::from(
            MigrateCliArgs::try_parse_from([
                "migrate",
                "--ssl",
                "--ssl-validate",
                "false",
                "--ssl-cert",
                "/certfile",
                "--ssl-usercert",
                "/usercert",
                "--ssl-userkey",
                "/userkey",
            ])
            .unwrap(),
        )
        .connection_init;
        match connection_init {
            Some(ConnectionInit::NewSession(Some(connection_opts))) => {
                assert!(connection_opts.use_ssl);
                assert_eq!(connection_opts.validate_ssl, Some(false));
                assert_eq!(connection_opts.ssl_certfile, Some("/certfile".into()));
                assert_eq!(connection_opts.ssl_usercert, Some("/usercert".into()));
                assert_eq!(connection_opts.ssl_userkey, Some("/userkey".into()));
            }
            _ => panic!(),
        };
    }

    #[test]
    fn test_cli_args_into_migrate_ops_with_explicit_ssl_validate() {
        let connection_init = MigrateOpts::from(
            MigrateCliArgs::try_parse_from([
                "migrate",
                "--ssl",
                "--ssl-validate",
                "true",
                "--ssl-cert",
                "/certfile",
                "--ssl-usercert",
                "/usercert",
                "--ssl-userkey",
                "/userkey",
            ])
            .unwrap(),
        )
        .connection_init;
        match connection_init {
            Some(ConnectionInit::NewSession(Some(connection_opts))) => {
                assert!(connection_opts.use_ssl);
                assert_eq!(connection_opts.validate_ssl, Some(true));
                assert_eq!(connection_opts.ssl_certfile, Some("/certfile".into()));
                assert_eq!(connection_opts.ssl_usercert, Some("/usercert".into()));
                assert_eq!(connection_opts.ssl_userkey, Some("/userkey".into()));
            }
            _ => panic!(),
        };
    }

    #[test]
    fn test_cli_args_into_migrate_ops_transforms_simple_replication() {
        match MigrateOpts::from(
            MigrateCliArgs::try_parse_from([
                "migrate",
                "--history-replication",
                "{'class':'SimpleStrategy','replication_factor':1}",
            ])
            .unwrap(),
        )
        .history_keyspace
        .unwrap()
        .replication
        {
            ReplicationFactor::SimpleStrategy { factor: 1 } => (),
            _ => panic!(),
        };
    }

    #[test]
    fn test_cli_args_into_migrate_ops_transforms_datacenter_replication() {
        match MigrateOpts::from(
            MigrateCliArgs::try_parse_from([
                "migrate",
                "--history-replication",
                "{'class': 'NetworkTopologyStrategy', 'dc1': 2, 'dc2': 3}",
            ])
            .unwrap(),
        )
        .history_keyspace
        .unwrap()
        .replication
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
