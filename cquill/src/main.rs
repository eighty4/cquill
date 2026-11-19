use std::env;
use std::ffi::OsString;
use std::iter::Peekable;
use std::ops::Deref;
use std::path::PathBuf;

use cquill::MigrateError::HistoryUpdateFailed;
use cquill::{
    ConnectionInit, ConnectionOpts, CqlFile, CqlshrcOpts, MigrateError,
    MigrateError::PartialMigration, MigrateErrorState, MigrateOpts, keyspace::*, migrate_cql,
};
use regex::Regex;

const USAGE: &str = "Usage: cquill <COMMAND> [OPTIONS]\n\nCommands:\n  migrate                 Migrate CQL files\n  help [COMMAND]          Show help\n\nOptions:\n  -h, --help              Show help\n  -V, --version           Show version\n\nRun `cquill migrate --help` for migrate options.";
const MIGRATE_USAGE: &str = "Usage: cquill migrate [OPTIONS]\n\nOptions:\n  -d, --cql-dir <CQL_DIR>                         CQL directory [default: ./cql]\n      --cqlshrc [<CQLSHRC_PATH>]                  Explicitly use cqlshrc [default: ~/.cassandra/cqlshrc]\n      --connection-timeout <SECONDS>              Connection timeout\n      --history-keyspace <KEYSPACE>               History keyspace [default: cquill]\n      --history-replication <REPLICATION>         History keyspace replication\n      --history-table <TABLE>                     History table [default: migrated_cql]\n  -a, --address <ADDRESS>                         Hostname or IPv4 address, optionally with port\n  -u, --username <USERNAME>                       Username\n  -p, --password <PASSWORD>                       Password\n      --tls                                       Enable TLS\n      --tls-validate <true|false>                  Validate TLS certificates\n      --tls-cert <CERTFILE>                       CA certificate file\n      --tls-usercert <USERCERT>                   Client certificate for mTLS\n      --tls-userkey <USERKEY>                     Client key for mTLS\n  -h, --help                                      Show help\n  -V, --version                                   Show version\n\nEach option can also be set with an environment variable by prefixing its name with CQUILL_\n(e.g. CQUILL_CQL_DIR, CQUILL_TLS, CQUILL_TLS_VALIDATE). Command-line values take precedence.";

#[derive(Debug, Default)]
struct CliValues {
    cql_dir: Option<PathBuf>,
    cqlshrc: Option<Option<PathBuf>>,
    connection_timeout: Option<u16>,
    history_keyspace: Option<String>,
    history_replication: Option<ReplicationFactor>,
    history_table: Option<String>,
    address: Option<String>,
    username: Option<String>,
    password: Option<String>,
    tls: Option<bool>,
    tls_validate: Option<bool>,
    tls_cert: Option<PathBuf>,
    tls_usercert: Option<PathBuf>,
    tls_userkey: Option<PathBuf>,
}

struct MigrateCliArgs {
    cql_dir: PathBuf,
    cqlshrc: Option<Option<PathBuf>>,
    connection_timeout: Option<u16>,
    history_keyspace: String,
    history_replication: Option<ReplicationFactor>,
    history_table: String,
    address: Option<String>,
    username: Option<String>,
    password: Option<String>,
    tls: bool,
    tls_validate: Option<bool>,
    tls_cert: Option<PathBuf>,
    tls_usercert: Option<PathBuf>,
    tls_userkey: Option<PathBuf>,
}

enum CliAction {
    Migrate(MigrateCliArgs),
    Help(bool),
    Version,
}

#[tokio::main]
async fn main() {
    match parse_cli(env::args_os().skip(1), |name| env::var_os(name)) {
        Ok(CliAction::Migrate(args)) => migrate(args).await,
        Ok(CliAction::Help(migrate)) => println!("{}", if migrate { MIGRATE_USAGE } else { USAGE }),
        Ok(CliAction::Version) => println!("cquill {}", env!("CARGO_PKG_VERSION")),
        Err(error) => {
            eprintln!("{error}\n\n{MIGRATE_USAGE}");
            std::process::exit(2);
        }
    }
}

fn parse_cli<I, F>(args: I, mut get_env: F) -> Result<CliAction, String>
where
    I: IntoIterator<Item = OsString>,
    F: FnMut(&str) -> Option<OsString>,
{
    let mut args = args.into_iter().peekable();
    let command = match args.next() {
        None => return Err("a command is required (expected `migrate`)".into()),
        Some(command) => command
            .into_string()
            .map_err(|_| "command must be valid UTF-8".to_string())?,
    };
    if is_help(&command) {
        return Ok(CliAction::Help(false));
    }
    if is_version(&command) {
        return Ok(CliAction::Version);
    }
    if command == "help" {
        return match args.next() {
            None => Ok(CliAction::Help(false)),
            Some(subcommand) if subcommand == "migrate" => Ok(CliAction::Help(true)),
            Some(other) => Err(format!("unknown command `{}`", other.to_string_lossy())),
        };
    }
    if command != "migrate" {
        return Err(format!("unknown command `{command}` (expected `migrate`)"));
    }

    let mut values = CliValues::default();
    while let Some(argument) = args.next() {
        let argument = argument
            .into_string()
            .map_err(|_| "option names must be valid UTF-8".to_string())?;
        if is_help(&argument) {
            return Ok(CliAction::Help(true));
        }
        if is_version(&argument) {
            return Ok(CliAction::Version);
        }
        let (option, inline_value) = split_option(&argument);
        match option {
            "-d" | "--cql-dir" => {
                values.cql_dir = Some(PathBuf::from(required_value(option, inline_value, &mut args)?));
            }
            "--cqlshrc" => {
                let path = match inline_value {
                    Some(value) => Some(PathBuf::from(value)),
                    None => match args.peek() {
                        Some(next) if !next.to_string_lossy().starts_with('-') => {
                            Some(PathBuf::from(args.next().expect("peeked argument")))
                        }
                        _ => None,
                    },
                };
                values.cqlshrc = Some(path);
            }
            "--connection-timeout" => {
                values.connection_timeout = Some(parse_value(option, inline_value, &mut args)?);
            }
            "--history-keyspace" => {
                values.history_keyspace = Some(parse_value(option, inline_value, &mut args)?);
            }
            "--history-replication" => {
                let value: String = parse_value(option, inline_value, &mut args)?;
                values.history_replication = Some(parse_replication(&value)?);
            }
            "--history-table" => {
                values.history_table = Some(parse_value(option, inline_value, &mut args)?);
            }
            "-a" | "--address" => {
                values.address = Some(parse_value(option, inline_value, &mut args)?);
            }
            "-u" | "--username" => {
                values.username = Some(parse_value(option, inline_value, &mut args)?);
            }
            "-p" | "--password" => {
                values.password = Some(parse_value(option, inline_value, &mut args)?);
            }
            "--tls" if inline_value.is_none() => values.tls = Some(true),
            "--tls-validate" => {
                values.tls_validate = Some(parse_value(option, inline_value, &mut args)?);
            }
            "--tls-cert" => {
                values.tls_cert = Some(PathBuf::from(required_value(option, inline_value, &mut args)?));
            }
            "--tls-usercert" => {
                values.tls_usercert = Some(PathBuf::from(required_value(option, inline_value, &mut args)?));
            }
            "--tls-userkey" => {
                values.tls_userkey = Some(PathBuf::from(required_value(option, inline_value, &mut args)?));
            }
            _ => return Err(format!("unknown or invalid option `{argument}`")),
        }
    }

    apply_environment(&mut values, &mut get_env)?;
    let cql_dir = values.cql_dir.unwrap_or_else(|| PathBuf::from("./cql"));
    let history_keyspace = values
        .history_keyspace
        .unwrap_or_else(|| cquill::KEYSPACE.to_string());
    let history_keyspace = validate_keyspace(&history_keyspace)?;
    let history_table = values
        .history_table
        .unwrap_or_else(|| cquill::TABLE.to_string());
    let history_table = validate_table(&history_table)?;
    let address = values.address.map(|address| validate_address(&address)).transpose()?;
    let username = values.username.map(|username| validate_username(&username)).transpose()?;

    Ok(CliAction::Migrate(MigrateCliArgs {
        cql_dir,
        cqlshrc: values.cqlshrc,
        connection_timeout: values.connection_timeout,
        history_keyspace,
        history_replication: values.history_replication,
        history_table,
        address,
        username,
        password: values.password,
        tls: values.tls.unwrap_or(false),
        tls_validate: values.tls_validate,
        tls_cert: values.tls_cert,
        tls_usercert: values.tls_usercert,
        tls_userkey: values.tls_userkey,
    }))
}

fn is_help(argument: &str) -> bool {
    matches!(argument, "-h" | "--help")
}

fn is_version(argument: &str) -> bool {
    matches!(argument, "-V" | "--version")
}

fn split_option(argument: &str) -> (&str, Option<&str>) {
    if let Some((option, value)) = argument.split_once('=') {
        return (option, Some(value));
    }
    if argument.len() > 2 && argument.starts_with('-') && !argument.starts_with("--") {
        return (&argument[..2], Some(&argument[2..]));
    }
    (argument, None)
}

fn required_value<I>(
    option: &str,
    inline_value: Option<&str>,
    args: &mut Peekable<I>,
) -> Result<OsString, String>
where
    I: Iterator<Item = OsString>,
{
    match inline_value {
        Some(value) => Ok(OsString::from(value)),
        None => args
            .next()
            .ok_or_else(|| format!("option `{option}` requires a value")),
    }
}

fn parse_value<T, I>(
    option: &str,
    inline_value: Option<&str>,
    args: &mut Peekable<I>,
) -> Result<T, String>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
    I: Iterator<Item = OsString>,
{
    let value = required_value(option, inline_value, args)?;
    let value = value
        .into_string()
        .map_err(|_| format!("value for `{option}` must be valid UTF-8"))?;
    value
        .parse::<T>()
        .map_err(|error| format!("invalid value `{value}` for `{option}`: {error}"))
}

fn env_string<F>(get_env: &mut F, name: &str) -> Result<Option<String>, String>
where
    F: FnMut(&str) -> Option<OsString>,
{
    get_env(name)
        .map(|value| {
            value
                .into_string()
                .map_err(|_| format!("environment variable {name} must be valid UTF-8"))
        })
        .transpose()
}

fn apply_environment<F>(values: &mut CliValues, get_env: &mut F) -> Result<(), String>
where
    F: FnMut(&str) -> Option<OsString>,
{
    macro_rules! env_option {
        ($field:ident, $name:literal, $ty:ty) => {
            if values.$field.is_none() {
                values.$field = env_string(get_env, concat!("CQUILL_", $name))?
                    .map(|value| {
                        value.parse::<$ty>().map_err(|error| {
                            format!("invalid value for CQUILL_{}: {error}", $name)
                        })
                    })
                    .transpose()?;
            }
        };
    }
    macro_rules! env_path {
        ($field:ident, $name:literal) => {
            if values.$field.is_none() {
                values.$field = get_env(concat!("CQUILL_", $name)).map(PathBuf::from);
            }
        };
    }

    env_path!(cql_dir, "CQL_DIR");
    if values.cqlshrc.is_none() {
        values.cqlshrc = get_env("CQUILL_CQLSHRC").map(|value| {
            if value.is_empty() {
                None
            } else {
                Some(PathBuf::from(value))
            }
        });
    }
    env_option!(connection_timeout, "CONNECTION_TIMEOUT", u16);
    if values.history_keyspace.is_none() {
        values.history_keyspace = env_string(get_env, "CQUILL_HISTORY_KEYSPACE")?;
    }
    if values.history_replication.is_none() {
        if let Some(value) = env_string(get_env, "CQUILL_HISTORY_REPLICATION")? {
            values.history_replication = Some(parse_replication(&value)?);
        }
    }
    if values.history_table.is_none() {
        values.history_table = env_string(get_env, "CQUILL_HISTORY_TABLE")?;
    }
    if values.address.is_none() {
        values.address = env_string(get_env, "CQUILL_ADDRESS")?;
    }
    if values.username.is_none() {
        values.username = env_string(get_env, "CQUILL_USERNAME")?;
    }
    if values.password.is_none() {
        values.password = env_string(get_env, "CQUILL_PASSWORD")?;
    }
    env_option!(tls, "TLS", bool);
    env_option!(tls_validate, "TLS_VALIDATE", bool);
    env_path!(tls_cert, "TLS_CERT");
    env_path!(tls_usercert, "TLS_USERCERT");
    env_path!(tls_userkey, "TLS_USERKEY");
    Ok(())
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
            tls_certfile: cli_args.tls_cert,
            tls_usercert: cli_args.tls_usercert,
            tls_userkey: cli_args.tls_userkey,
            use_tls: cli_args.tls,
            validate_tls: cli_args.tls_validate,
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
    use std::ffi::OsString;
    use std::iter::repeat_n;

    use super::*;

    fn parse(arguments: &[&str]) -> Result<CliAction, String> {
        parse_cli(
            arguments.iter().map(OsString::from),
            |_| None,
        )
    }

    fn migrate_args(arguments: &[&str]) -> MigrateCliArgs {
        match parse(arguments).expect("CLI args should parse") {
            CliAction::Migrate(args) => args,
            _ => panic!("expected migrate command"),
        }
    }

    #[test]
    fn defaults_and_subcommand() {
        let args = migrate_args(&["migrate"]);
        assert_eq!(args.cql_dir, PathBuf::from("./cql"));
        assert_eq!(args.history_keyspace, cquill::KEYSPACE);
        assert_eq!(args.history_table, cquill::TABLE);
        assert!(!args.tls);
        assert!(args.connection_timeout.is_none());
        assert!(args.cqlshrc.is_none());
    }

    #[test]
    fn parses_short_long_and_equals_forms() {
        let args = migrate_args(&[
            "migrate", "-d", "/tmp/cql", "--history-table=events", "-a", "db.example:9042",
            "-u", "alice", "-psecret", "--tls", "--tls-validate=false", "--tls-cert", "/ca.pem",
            "--tls-usercert=/client.pem", "--tls-userkey", "/client.key",
        ]);
        assert_eq!(args.cql_dir, PathBuf::from("/tmp/cql"));
        assert_eq!(args.history_table, "events");
        assert_eq!(args.address.as_deref(), Some("db.example:9042"));
        assert_eq!(args.username.as_deref(), Some("alice"));
        assert_eq!(args.password.as_deref(), Some("secret"));
        assert!(args.tls);
        assert_eq!(args.tls_validate, Some(false));
        assert_eq!(args.tls_cert, Some(PathBuf::from("/ca.pem")));
        assert_eq!(args.tls_usercert, Some(PathBuf::from("/client.pem")));
        assert_eq!(args.tls_userkey, Some(PathBuf::from("/client.key")));
    }

    #[test]
    fn cqlshrc_is_an_optional_argument_and_can_be_explicitly_empty() {
        assert!(migrate_args(&["migrate", "--cqlshrc"]).cqlshrc == Some(None));
        assert_eq!(
            migrate_args(&["migrate", "--cqlshrc", "/tmp/cqlshrc"]).cqlshrc,
            Some(Some(PathBuf::from("/tmp/cqlshrc")))
        );
        assert_eq!(
            migrate_args(&["migrate", "--cqlshrc=/tmp/cqlshrc"]).cqlshrc,
            Some(Some(PathBuf::from("/tmp/cqlshrc")))
        );
        assert!(migrate_args(&["migrate", "--cqlshrc", "--tls"]).tls);
    }

    #[test]
    fn environment_values_are_used_and_cli_values_win() {
        let args = parse_cli(
            ["migrate", "--cql-dir", "/cli"].map(OsString::from),
            |name| match name {
                "CQUILL_CQL_DIR" => Some("/env".into()),
                "CQUILL_HISTORY_KEYSPACE" => Some("envks".into()),
                "CQUILL_TLS" => Some("true".into()),
                "CQUILL_TLS_VALIDATE" => Some("false".into()),
                "CQUILL_CQLSHRC" => Some("".into()),
                _ => None,
            },
        )
        .expect("env values should parse");
        let CliAction::Migrate(args) = args else { panic!("expected migrate command") };
        assert_eq!(args.cql_dir, PathBuf::from("/cli"));
        assert_eq!(args.history_keyspace, "envks");
        assert!(args.tls);
        assert_eq!(args.tls_validate, Some(false));
        assert_eq!(args.cqlshrc, Some(None));
    }

    #[test]
    fn environment_supports_all_option_names() {
        let args = parse_cli(["migrate"].map(OsString::from), |name| {
            let value = match name {
                "CQUILL_CQL_DIR" => "/env/cql",
                "CQUILL_CQLSHRC" => "/env/cqlshrc",
                "CQUILL_CONNECTION_TIMEOUT" => "4",
                "CQUILL_HISTORY_KEYSPACE" => "envks",
                "CQUILL_HISTORY_REPLICATION" => "{'class':'SimpleStrategy','replication_factor':2}",
                "CQUILL_HISTORY_TABLE" => "env_table",
                "CQUILL_ADDRESS" => "db.example:9042",
                "CQUILL_USERNAME" => "alice",
                "CQUILL_PASSWORD" => "secret",
                "CQUILL_TLS" => "true",
                "CQUILL_TLS_VALIDATE" => "false",
                "CQUILL_TLS_CERT" => "/env/ca.pem",
                "CQUILL_TLS_USERCERT" => "/env/client.pem",
                "CQUILL_TLS_USERKEY" => "/env/client.key",
                _ => return None,
            };
            Some(OsString::from(value))
        })
        .expect("all supported env vars should parse");
        let CliAction::Migrate(args) = args else { panic!("expected migrate command") };
        assert_eq!(args.cql_dir, PathBuf::from("/env/cql"));
        assert_eq!(args.cqlshrc, Some(Some(PathBuf::from("/env/cqlshrc"))));
        assert_eq!(args.connection_timeout, Some(4));
        assert_eq!(args.history_keyspace, "envks");
        assert_eq!(args.history_table, "env_table");
        assert_eq!(args.address.as_deref(), Some("db.example:9042"));
        assert_eq!(args.username.as_deref(), Some("alice"));
        assert_eq!(args.password.as_deref(), Some("secret"));
        assert!(args.tls);
        assert_eq!(args.tls_validate, Some(false));
        assert_eq!(args.tls_cert, Some(PathBuf::from("/env/ca.pem")));
        assert_eq!(args.tls_usercert, Some(PathBuf::from("/env/client.pem")));
        assert_eq!(args.tls_userkey, Some(PathBuf::from("/env/client.key")));
        match args.history_replication.expect("replication should be set") {
            ReplicationFactor::SimpleStrategy { factor } => assert_eq!(factor, 2),
            _ => panic!("expected simple strategy"),
        }
    }

    #[test]
    fn cli_validation_rejects_bad_values() {
        assert!(parse(&["migrate", "--connection-timeout", "not-a-number"]).is_err());
        assert!(parse(&["migrate", "--history-keyspace", "_invalid"]).is_err());
        assert!(parse(&["migrate", "--history-table", "UpperCase"]).is_err());
        assert!(parse(&["migrate", "--username", "bad name"]).is_err());
        assert!(parse(&["migrate", "--address", "host:port"]).is_err());
        assert!(parse(&["migrate", "--tls"]).is_ok());
        assert!(parse(&["migrate", "--unknown"]).is_err());
        assert!(parse(&["migrate", "--history-replication", "{}"]).is_err());
    }

    #[test]
    fn environment_validation_rejects_invalid_values_but_cli_overrides_them() {
        let invalid_env = |name| (name == "CQUILL_TLS").then(|| OsString::from("yes"));
        assert!(parse_cli(["migrate"].map(OsString::from), invalid_env).is_err());
        let valid_cli = parse_cli(
            ["migrate", "--tls"].map(OsString::from),
            |name| (name == "CQUILL_TLS").then(|| OsString::from("yes")),
        )
        .expect("CLI flag should take precedence over invalid env value");
        assert!(matches!(valid_cli, CliAction::Migrate(args) if args.tls));
    }

    #[test]
    fn help_and_version_are_available() {
        assert!(matches!(parse(&["--help"]), Ok(CliAction::Help(false))));
        assert!(matches!(parse(&["help", "migrate"]), Ok(CliAction::Help(true))));
        assert!(matches!(parse(&["migrate", "--help"]), Ok(CliAction::Help(true))));
        assert!(matches!(parse(&["--version"]), Ok(CliAction::Version)));
    }

    #[test]
    fn validation_limits_and_replication_parsing() {
        assert!(validate_address("us-east-1.scylla.example:9042").is_ok());
        assert!(validate_address("host:benedictCumberbatch").is_err());
        assert!(validate_keyspace(&format!("\"{}\"", repeat_n('X', 48).collect::<String>())).is_ok());
        assert!(validate_keyspace(&format!("\"{}\"", repeat_n('X', 49).collect::<String>())).is_err());
        assert!(validate_table(&format!("\"{}\"", repeat_n('X', 222).collect::<String>())).is_ok());
        assert!(validate_table(&format!("\"{}\"", repeat_n('X', 223).collect::<String>())).is_err());
        assert!(validate_username(&format!("'{}'", repeat_n('$', 256).collect::<String>())).is_ok());
        assert!(validate_username(&format!("'{}'", repeat_n('$', 257).collect::<String>())).is_err());
        assert!(parse_replication("{'class':'SimpleStrategy', 'replication_factor':1}").is_ok());
    }
}
