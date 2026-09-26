#[path = "util/mod.rs"]
mod util;

use std::process::ExitCode;

use cquill::*;
use util::*;

#[tokio::main]
async fn main() -> ExitCode {
    println!(
        "before running this example, you must start the example's docker service:\n\n    `docker compose up password_auth -d --wait`\n\n"
    );

    let cql_dir = example_cql_dir();
    let cqlshrc = cql_dir.parent().unwrap().join("cqlshrc_password_auth.ini");

    let opts = MigrateOpts {
        connection_init: Some(ConnectionInit::Cqlshrc(CqlshrcOpts {
            path: Some(cqlshrc),
            overrides: ConnectionOpts::default(),
        })),
        cql_dir,
        history_keyspace: None,
        history_table: None,
    };
    let result = migrate_cql(opts).await;

    on_migrate_result(result)
}
