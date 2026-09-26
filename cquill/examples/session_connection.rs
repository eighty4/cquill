#[path = "util/mod.rs"]
mod util;

use std::{process::ExitCode, sync::Arc};

use cquill::*;
use scylla::client::session_builder::SessionBuilder;
use util::*;

#[tokio::main]
async fn main() -> ExitCode {
    let session = SessionBuilder::new()
        .known_node("127.0.0.1")
        .build()
        .await
        .unwrap();
    let opts = MigrateOpts {
        connection_init: Some(ConnectionInit::Session(Arc::new(session))),
        cql_dir: example_cql_dir(),
        history_keyspace: None,
        history_table: None,
    };
    let result = migrate_cql(opts).await;

    on_migrate_result(result)
}
