use std::collections::HashMap;
use std::fmt::Display;
use std::fs;
use std::path::PathBuf;

use anyhow::Result;
use rand::Rng;
use scylla::Session;
use temp_dir::TempDir;

use crate::cql::CqlFile;
use crate::keyspace::KeyspaceOpts;
use crate::{cql, queries, TABLE};

pub(crate) fn make_file(path: PathBuf) {
    fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(path)
        .expect("could not write file");
}

pub(crate) fn error_panic(err: &dyn Display) -> ! {
    println!("{err}");
    panic!();
}

pub(crate) async fn cql_session() -> Session {
    let node_address = "127.0.0.1:9042";
    scylla::SessionBuilder::new()
        .known_node(node_address)
        .build()
        .await
        .expect("cql session")
}

fn alphanumeric_str(len: u8) -> String {
    let mut rng = rand::thread_rng();
    (0..len)
        .map(|_| rng.sample(rand::distributions::Alphanumeric) as char)
        .map(|c| c.to_ascii_lowercase())
        .collect()
}

pub(crate) fn keyspace_name() -> String {
    format!("cquill_test_{}", alphanumeric_str(6))
}

pub(crate) struct TestHarness {
    pub directory: TempDir,
    pub cquill_keyspace: KeyspaceOpts,
    pub cquill_table: String,
    pub cql_files: Vec<CqlFile>,
}

impl TestHarness {
    pub fn builder() -> TestHarnessBuilder {
        TestHarnessBuilder::default()
    }

    pub async fn setup(&self) -> Result<Session> {
        let session = cql_session().await;
        queries::keyspace::create(&session, &self.cquill_keyspace)
            .await
            .expect("create keyspace");
        queries::migrated::table::create(&session, &self.cquill_keyspace.name, &self.cquill_table)
            .await
            .expect("create table");
        Ok(session)
    }
}

pub(crate) struct TestHarnessBuilder {
    directory: TempDir,
    cquill_keyspace: Option<KeyspaceOpts>,
    cquill_table: Option<String>,
    cql_files: HashMap<PathBuf, String>,
}

impl TestHarnessBuilder {
    pub fn cql_file(mut self, filename: &str, content: &str) -> Self {
        self.cql_files
            .insert(self.directory.path().join(filename), content.to_string());
        self
    }

    pub fn cquill_history(mut self, keyspace_name: &str, table_name: &str) -> Self {
        self.cquill_keyspace = Some(KeyspaceOpts::simple(keyspace_name.to_string(), 1));
        self.cquill_table = Some(table_name.to_string());
        self
    }

    pub fn build(self) -> TestHarness {
        self.cql_files
            .keys()
            .for_each(|f| make_file(self.directory.path().join(f)));
        let temp_dir_path = self.directory.path().canonicalize().unwrap();
        let cql_files = cql::files_from_dir(&temp_dir_path).unwrap_or_default();
        TestHarness {
            directory: self.directory,
            cquill_keyspace: self
                .cquill_keyspace
                .unwrap_or_else(|| KeyspaceOpts::simple(keyspace_name(), 1)),
            cquill_table: String::from(TABLE),
            cql_files,
        }
    }
}

impl Default for TestHarnessBuilder {
    fn default() -> Self {
        TestHarnessBuilder {
            directory: TempDir::new().expect("make temp dir"),
            cquill_keyspace: None,
            cquill_table: None,
            cql_files: HashMap::new(),
        }
    }
}
