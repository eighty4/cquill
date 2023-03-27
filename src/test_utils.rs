use std::collections::HashMap;
use std::fmt::Display;
use std::fs;
use std::io::Write;
use std::path::PathBuf;

use rand::Rng;
use scylla::Session;
use temp_dir::TempDir;

use crate::cql::CqlFile;
use crate::keyspace::KeyspaceOpts;
use crate::{cql, queries, TABLE};

pub(crate) fn make_file(path: PathBuf, content: &str) {
    let mut f = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(path)
        .expect("create file");
    f.write_all(content.as_bytes())
        .expect("write bytes to file");
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

pub(crate) async fn create_keyspace(session: &Session) -> KeyspaceOpts {
    let keyspace_opts = KeyspaceOpts::simple(keyspace_name(), 1);
    create_keyspace_from_opts(session, &keyspace_opts).await;
    keyspace_opts
}

pub(crate) async fn create_keyspace_from_opts(session: &Session, keyspace_opts: &KeyspaceOpts) {
    queries::keyspace::create(session, keyspace_opts)
        .await
        .expect("create keyspace");
}

pub(crate) async fn drop_keyspace(session: &Session, keyspace_name: &String) {
    session
        .query(format!("drop keyspace {}", keyspace_name), ())
        .await
        .expect("drop keyspace");
}

pub(crate) async fn drop_table(session: &Session, keyspace_name: &String, table_name: &String) {
    session
        .query(format!("drop table {keyspace_name}.{table_name}"), ())
        .await
        .expect("drop table");
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
    pub session: Session,
    pub cql_dir: PathBuf,
    #[allow(dead_code)]
    directory: TempDir,
    pub cquill_keyspace: String,
    pub cquill_table: String,
    pub cql_files: Vec<CqlFile>,
}

impl TestHarness {
    pub fn builder() -> TestHarnessBuilder {
        TestHarnessBuilder::default()
    }

    pub fn cql_file_path(&self, filename: &str) -> PathBuf {
        self.cql_dir.join(filename)
    }

    pub async fn drop_keyspace(&self) {
        drop_keyspace(&self.session, &self.cquill_keyspace).await;
    }
}

pub(crate) struct TestHarnessBuilder {
    directory: TempDir,
    cquill_keyspace: Option<KeyspaceOpts>,
    cquill_table: Option<String>,
    cql_files: HashMap<PathBuf, String>,
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

impl TestHarnessBuilder {
    pub fn cql_file(mut self, filename: &str, content: &str) -> Self {
        self.cql_files
            .insert(self.directory.path().join(filename), content.to_string());
        self
    }

    #[allow(dead_code)]
    pub fn cquill_history(mut self, keyspace_name: &str, table_name: &str) -> Self {
        self.cquill_keyspace = Some(KeyspaceOpts::simple(keyspace_name.to_string(), 1));
        self.cquill_table = Some(table_name.to_string());
        self
    }

    pub async fn initialize(self) -> TestHarness {
        for (filename, content) in self.cql_files.iter() {
            make_file(self.directory.path().join(filename), content);
        }
        let temp_dir_path = self.directory.path().canonicalize().unwrap();
        let cql_files = cql::files_from_dir(&temp_dir_path).unwrap_or_default();
        let cquill_keyspace = self
            .cquill_keyspace
            .unwrap_or_else(|| KeyspaceOpts::simple(keyspace_name(), 1));
        let cquill_table = self.cquill_table.unwrap_or_else(|| String::from(TABLE));
        let session = cql_session().await;
        create_keyspace_from_opts(&session, &cquill_keyspace).await;
        queries::migrated::table::create(&session, &cquill_keyspace.name, &cquill_table)
            .await
            .expect("create table");
        TestHarness {
            session,
            cql_dir: self.directory.path().to_path_buf(),
            directory: self.directory,
            cquill_keyspace: cquill_keyspace.name,
            cquill_table,
            cql_files,
        }
    }
}
