use std::path::PathBuf;

use anyhow::Error;
use clap::{Parser, Subcommand};

use cquill::{keyspace::*, migrate_cql, CassandraOpts, MigrateOpts};

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
            Err(err) => error_exit(err),
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
    let version = std::env::var("CARGO_PKG_VERSION").unwrap_or_default();
    let cql_dir = opts.cql_dir.to_string_lossy();
    println!("CQuill {version}\nMigrating CQL files from {cql_dir}");
    match migrate_cql(opts).await {
        Ok(migrated_cql) => {
            if migrated_cql.is_empty() {
                println!("cql migration already up to date");
            } else if migrated_cql.len() == 1 {
                println!("migrated 1 cql file: {}", migrated_cql[0].filename);
            } else {
                println!("migrated {} cql files:", migrated_cql.len());
                migrated_cql.iter().for_each(|p| {
                    println!("  {}", p.filename);
                });
            }
        }
        Err(err) => error_exit(err),
    }
}

fn error_prefix() -> String {
    // hex \x1b -> octal \033
    //        0 -> reset
    //       31 -> red foreground
    //        1 -> bold
    "\x1b[0;31;1merror\x1b[0m".to_string()
}

fn error_exit(err: Error) -> ! {
    println!("{} {err}", error_prefix());
    std::process::exit(1);
}
