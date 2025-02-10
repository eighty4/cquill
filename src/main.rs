use std::ops::Deref;
use std::path::PathBuf;

use clap::{Parser, Subcommand};

use cquill::MigrateError::HistoryUpdateFailed;
use cquill::{
    keyspace::*, CqlFile, MigrateError, MigrateError::PartialMigration, MigrateErrorState, Migrator,
};
use scylla::SessionBuilder;

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
    #[arg(short = 'd', long, env = "CQL_DIR", default_value = "./cql")]
    cql_dir: PathBuf,
    #[arg(long, env = "HISTORY_KEYSPACE", default_value = cquill::KEYSPACE)]
    history_keyspace: String,
    #[arg(long, env = "HISTORY_REPLICATION", default_value = cquill::keyspace::REPLICATION)]
    history_replication: String,
    #[arg(long, env = "HISTORY_TABLE", default_value = cquill::TABLE)]
    history_table: String,
    #[arg(long, env = "CQL_HOST")]
    cql_host: String,
}

impl MigrateCliArgs {
    async fn into_migrator(self) -> Migrator {
        let replication_factor = match self.history_replication.parse::<ReplicationFactor>() {
            Ok(replication_factor) => replication_factor,
            Err(err) => error_exit(MigrateError::from(err)),
        };

        let session = SessionBuilder::new()
            .known_node(self.cql_host)
            .build()
            .await
            .expect("Unable to create Scylla session");

        Migrator::new(session.into(), self.cql_dir)
            .with_keyspace(KeyspaceOpts {
                name: self.history_keyspace,
                replication: Some(replication_factor),
            })
            .with_table_name(self.history_table)
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
    let migrator = args.into_migrator().await;
    let version = env!("CARGO_PKG_VERSION");
    let cql_dir = migrator.migrations_dir.to_string_lossy();
    println!("CQuill {version}\nMigrating CQL files from {cql_dir}");
    match migrator.run_pending().await {
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
    println!("The remaining statements will need to be manually executed and {} must be added to CQuill's history table with the CQL file's content hash.", error_state.failed_file);
    println!("===============");
    std::process::exit(1);
}

fn error_exit(err: MigrateError) -> ! {
    println!("{} {err}", error_prefix());
    std::process::exit(1);
}
