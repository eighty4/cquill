use std::path::PathBuf;

use clap::{Parser, Subcommand};

use cquill::{keyspace::*, migrate_cql, CassandraOpts, CqlFile, MigrateError, MigrateOpts};

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

#[derive(Parser, Debug)]
struct MigrateCliArgs {
    #[clap(short = 'd', long, value_name = "CQL_DIR", default_value = "./cql")]
    cql_dir: PathBuf,
    #[clap(long, value_name = "HISTORY_KEYSPACE", default_value = cquill::KEYSPACE)]
    history_keyspace: String,
    #[clap(long, value_name = "HISTORY_REPLICATION", default_value = cquill::keyspace::REPLICATION)]
    history_replication: String,
    #[clap(long, value_name = "HISTORY_TABLE", default_value = cquill::TABLE)]
    history_table: String,
}

impl MigrateCliArgs {
    fn to_opts(&self) -> MigrateOpts {
        let replication_factor = match self.history_replication.parse::<ReplicationFactor>() {
            Ok(replication_factor) => replication_factor,
            Err(err) => error_exit(MigrateError::from(err)),
        };
        MigrateOpts {
            cassandra_opts: Some(CassandraOpts::default()),
            cql_dir: self.cql_dir.clone(),
            history_keyspace: Some(KeyspaceOpts {
                name: self.history_keyspace.clone(),
                replication: Some(replication_factor),
            }),
            history_table: Some(self.history_table.clone()),
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
    let opts = args.to_opts();
    let version = env!("CARGO_PKG_VERSION");
    let cql_dir = opts.cql_dir.to_string_lossy();
    println!("CQuill {version}\nMigrating CQL files from {cql_dir}");
    match migrate_cql(opts).await {
        Ok(migrated_cql) => print_migrated_cql(&migrated_cql),
        Err(err) => error_exit(err),
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
    "\x1b[0;31;1merror\x1b[0m".to_string()
}

fn error_exit(err: MigrateError) -> ! {
    println!("{} {err}", error_prefix());
    std::process::exit(1);
}
