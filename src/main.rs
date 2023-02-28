extern crate clap;

use clap::{Parser, Subcommand};
use cquill::MigrateOpts;
use std::path::PathBuf;

#[derive(Parser)]
#[command(author, version, about)]
struct CquillCli {
    #[command(subcommand)]
    command: CquillCommand,
}

#[derive(Subcommand)]
enum CquillCommand {
    Migrate(MigrateArgs),
}

#[derive(Parser, Debug)]
struct MigrateArgs {
    #[arg(short = 'd', long, value_name = "CQL_DIR", default_value = "./cql")]
    cql_dir: PathBuf,
}

impl MigrateArgs {
    fn to_opts(&self) -> MigrateOpts {
        MigrateOpts {
            cql_dir: self.cql_dir.clone(),
        }
    }
}

fn main() {
    let cquill_cli = CquillCli::parse();
    match cquill_cli.command {
        CquillCommand::Migrate(args) => migrate(args),
    };
}

fn migrate(args: MigrateArgs) {
    match cquill::migrate_cql(args.to_opts()) {
        Ok(migrated_cql) => {
            if migrated_cql.is_empty() {
                println!("cql migration already up to date");
            } else if migrated_cql.len() == 1 {
                println!(
                    "migrated {} cql file: {}",
                    migrated_cql.len(),
                    migrated_cql[0].to_string_lossy()
                );
            } else {
                println!("migrated {} cql files:", migrated_cql.len());
                migrated_cql.iter().for_each(|p| {
                    println!("  {}", p.file_name().unwrap().to_string_lossy());
                });
            }
        }
        Err(err) => println!("{} {err}", error_prefix()),
    }
}

fn error_prefix() -> String {
    // hex \x1b -> octal \033
    //        0 -> reset
    //       31 -> red foreground
    //        4 -> underline
    //        1 -> bold
    "\x1b[0;31;4;1merror\x1b[0m".to_string()
}
