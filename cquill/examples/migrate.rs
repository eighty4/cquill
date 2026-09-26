#[path = "util/mod.rs"]
mod util;

use std::process::ExitCode;

use cquill::*;
use util::*;

#[tokio::main]
async fn main() -> ExitCode {
    let opts = MigrateOpts {
        connection_init: None,
        cql_dir: example_cql_dir(),
        history_keyspace: None,
        history_table: None,
    };
    let result = migrate_cql(opts).await;

    on_migrate_result(result)
}
