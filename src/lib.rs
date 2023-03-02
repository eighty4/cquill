extern crate anyhow;
extern crate lazy_static;
extern crate regex;
extern crate scylla;

use crate::ReplicationFactor::*;
use anyhow::{anyhow, Result};
use lazy_static::lazy_static;
use regex::Regex;
use scylla::Session;
use std::collections::HashMap;
use std::str::Split;
use std::{fs, path::PathBuf, str};

pub const KEYSPACE: &str = "cquill";

pub const REPLICATION: &str = "{ 'class': 'SimpleStrategy', 'replication_factor': 1 }";

pub const TABLE: &str = "migrated_cql";

pub struct MigrateOpts {
    pub cql_dir: PathBuf,
    pub history_keyspace: Option<KeyspaceOpts>,
    pub history_table: Option<String>,
}

/// KeyspaceOpts describes a keyspace managed by cquill with a keyspace name and
/// [ReplicationFactor].
pub struct KeyspaceOpts {
    pub name: String,
    /// The keyspace [ReplicationFactor] will default to a development environment setting using
    /// SimpleStrategy with a replication factor of 1.
    pub replication: Option<ReplicationFactor>,
}

impl KeyspaceOpts {
    pub fn simple(name: String, factor: u8) -> Self {
        KeyspaceOpts {
            name,
            replication: Some(SimpleStrategy { factor }),
        }
    }
}

/// ReplicationFactor represents the strategy and data replication factor for a keyspace.
pub enum ReplicationFactor {
    /// NetworkTopologyStrategy specifies how many replications will be placed in specific
    /// datacenters within the cluster.
    NetworkTopologyStrategy {
        datacenter_factors: HashMap<String, u8>,
    },
    /// SimpleStrategy specifies a single number of replications distributed throughout any nodes
    /// within the cluster. This strategy does not provide sufficient resiliency and fault tolerance
    /// and should not be used with production systems.
    SimpleStrategy { factor: u8 },
}

impl str::FromStr for ReplicationFactor {
    type Err = anyhow::Error;

    /// from_str performs a manual deserialization of a `CREATE KEYSPACE` statement's replication
    /// settings from the CQL key-value hash object. Valid input from the CLI default can be seen
    /// in [REPLICATION].
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        if s == REPLICATION {
            return Ok(SimpleStrategy { factor: 1 });
        }
        let trimmed = s.trim();
        if !trimmed.starts_with('{') || !trimmed.ends_with('}') {
            return Err(anyhow!("not a valid keyspace replication object"));
        }
        // collect all key value pairs from {} object into a HashMap
        let mut fields: HashMap<String, String> = HashMap::new();
        let fields_split = trimmed[1..trimmed.len() - 1].split(',');
        for value_pair in fields_split {
            let mut key_value_split = value_pair.split(':');
            let next_from_key_value_split = |key_value_split: &mut Split<char>| -> Option<String> {
                key_value_split
                    .next()
                    .map(|s| s.trim().trim_matches('"').trim_matches('\'').to_string())
            };
            let maybe_key = next_from_key_value_split(&mut key_value_split);
            let maybe_value = next_from_key_value_split(&mut key_value_split);
            match (maybe_key, maybe_value) {
                (Some(key), Some(value)) => {
                    if fields.insert(key.clone(), value).is_some() {
                        return Err(anyhow!(
                            "replication object duplicates key-value pair {key}"
                        ));
                    }
                }
                (_, _) => {
                    return Err(anyhow!(
                        "not a valid key-value pair in keyspace replication object"
                    ))
                }
            }
        }
        match fields.remove("class") {
            None => Err(anyhow!("replication object missing class field")),
            Some(replication_class) => match replication_class.as_str() {
                "NetworkTopologyStrategy" => {
                    if fields.is_empty() {
                        return Err(anyhow!("network replication must specify at least one datacenter's replication factor"));
                    }
                    let mut datacenter_factors: HashMap<String, u8> = HashMap::new();
                    lazy_static! {
                        static ref DATACENTER_REGEX: Regex =
                            regex::Regex::new(r"^[a-z\d_]{2,}$").unwrap();
                    }
                    for (datacenter, factor_string) in fields.iter() {
                        if !DATACENTER_REGEX.is_match(datacenter) {
                            return Err(anyhow!("datacenter {datacenter} is not a valid name"));
                        }
                        match factor_string.parse::<u8>() {
                            Ok(factor) => {
                                datacenter_factors.insert(datacenter.clone(), factor);
                            }
                            Err(_) => return Err(anyhow!("replication factor {datacenter} for datacenter {factor_string} must be a number"))
                        }
                    }
                    Ok(NetworkTopologyStrategy { datacenter_factors })
                }
                "SimpleStrategy" => match fields.get("replication_factor") {
                    Some(factor_string) => match factor_string.parse::<u8>() {
                        Ok(factor) => Ok(SimpleStrategy { factor }),
                        Err(_) => Err(anyhow!(
                            "replication factor {factor_string} must be a number"
                        )),
                    },
                    None => Err(anyhow!(
                        "replication object missing replication_factor field"
                    )),
                },
                _ => Err(anyhow!(
                    "replication class {replication_class} field is an unsupported type"
                )),
            },
        }
    }
}

/// migrate_cql performs a migration of all newly added cql scripts in [MigrateOpts::cql_dir]
/// since its last invocation. Migrated scripts are tracked in a cquill keyspace and history table
/// specified with [MigrateOpts::history_keyspace] and [MigrateOpts::history_table]. A successful
/// method result contains a vec of the cql script paths executed during this invocation.
pub async fn migrate_cql(opts: MigrateOpts) -> Result<Vec<PathBuf>> {
    // if cql_files_from_dir(&opts.cql_dir)?.is_empty() {
    //     return Ok(Vec::new());
    // }
    let session = cql_session().await?;

    let history_keyspace = opts
        .history_keyspace
        .unwrap_or_else(|| KeyspaceOpts::simple(String::from(KEYSPACE), 1));
    let history_table = opts.history_table.unwrap_or_else(|| String::from(TABLE));
    prepare_cquill_keyspace(&session, history_keyspace, history_table).await?;

    Ok(Vec::new())
}

// todo
//  check if keyspace already exists
//  drop and recreate dev mode
//  add keyspace composite key
async fn prepare_cquill_keyspace(
    session: &Session,
    keyspace: KeyspaceOpts,
    table: String,
) -> Result<()> {
    session.query(create_keyspace_cql(&keyspace)?, ()).await?;
    let create_table_cql = format!(
        "create table {}.{} (id timeuuid primary key, ver int, name varchar)",
        keyspace.name, table
    );
    session.query(create_table_cql, ()).await?;
    Ok(())
}

#[allow(dead_code)]
fn cql_files_from_dir(cql_dir: &PathBuf) -> Result<Vec<PathBuf>> {
    return match fs::read_dir(cql_dir) {
        Ok(read_dir) => {
            let mut result = Vec::new();
            for dir_entry in read_dir {
                let path = dir_entry?.path();
                if path.is_file() {
                    if let Some(extension) = path.extension() {
                        if extension == "cql" {
                            result.push(path);
                        }
                    }
                }
            }
            if result.is_empty() {
                return Err(anyhow!(
                    "no cql files found in directory '{}'",
                    cql_dir.to_string_lossy()
                ));
            }
            result.sort();
            Ok(result)
        }
        Err(_) => Err(anyhow!(
            "could not find directory '{}'",
            cql_dir.to_string_lossy()
        )),
    };
}

async fn cql_session() -> Result<Session> {
    let node_address = "127.0.0.1:9042";
    let connecting = scylla::SessionBuilder::new()
        .known_node(node_address)
        .build()
        .await;
    match connecting {
        Ok(session) => Ok(session),
        Err(_) => Err(anyhow!("could not connect to {}", node_address)),
    }
}

fn create_keyspace_cql(keyspace_opts: &KeyspaceOpts) -> Result<String> {
    if keyspace_opts.name.is_empty() {
        return Err(anyhow!("keyspace has empty name"));
    }
    let replication = match &keyspace_opts.replication {
        Some(r) => match r {
            NetworkTopologyStrategy { datacenter_factors } => {
                create_network_topology_strategy_keyspace_replication_map_str(datacenter_factors)
            }
            SimpleStrategy { factor } => {
                Ok(create_simple_strategy_keyspace_replication_map_str(factor))
            }
        },
        None => Ok(create_simple_strategy_keyspace_replication_map_str(&1)),
    };
    match replication {
        Ok(r) => Ok(format!(
            "create keyspace {} with replication = {}",
            keyspace_opts.name, r
        )),
        Err(e) => Err(anyhow!("keyspace {} {}", keyspace_opts.name, e.to_string())),
    }
}

fn create_simple_strategy_keyspace_replication_map_str(replication_factor: &u8) -> String {
    format!("{{ 'class': 'SimpleStrategy', 'replication_factor': {replication_factor} }}")
}

fn create_network_topology_strategy_keyspace_replication_map_str(
    datacenter_factors: &HashMap<String, u8>,
) -> Result<String> {
    if datacenter_factors.is_empty() {
        return Err(anyhow!(
            "network topology replication has no datacenter replication factors"
        ));
    }
    let mut v = Vec::with_capacity(datacenter_factors.len());
    for (dc, rf) in datacenter_factors {
        if dc.is_empty() {
            return Err(anyhow!(
                "network topology replication factor has empty datacenter name"
            ));
        } else if *rf == 0 {
            return Err(anyhow!(
                "network topology replication factor for datacenter {} is not a positive number",
                dc
            ));
        }
        v.push(format!("'{dc}': {rf}"));
    }
    Ok(format!(
        "{{ 'class': 'NetworkTopologyStrategy', {} }}",
        v.join(", ")
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use temp_dir::TempDir;

    #[test]
    fn test_replication_factory_from_str_simple_default() {
        let result = REPLICATION.parse::<ReplicationFactor>();
        assert!(result.is_ok());
        let rep_factor = result.unwrap();
        match rep_factor {
            NetworkTopologyStrategy { .. } => panic!(),
            SimpleStrategy { factor } => assert_eq!(factor, 1),
        }
    }

    #[test]
    fn test_replication_factory_from_str_simple_custom() {
        let replication_factor = "{ 'class': 'SimpleStrategy', 'replication_factor': 3 }";
        let result = replication_factor.parse::<ReplicationFactor>();
        assert!(result.is_ok());
        let rep_factor = result.unwrap();
        match rep_factor {
            NetworkTopologyStrategy { .. } => panic!(),
            SimpleStrategy { factor } => assert_eq!(factor, 3),
        }
    }

    #[test]
    fn test_replication_factory_from_str_network() {
        let replication_factor = "{ 'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 5 }";
        let result = replication_factor.parse::<ReplicationFactor>();
        match result {
            Ok(_) => {
                let rep_factor = result.unwrap();
                match rep_factor {
                    NetworkTopologyStrategy { datacenter_factors } => {
                        assert_eq!(datacenter_factors.get("dc1").unwrap().clone(), 3);
                        assert_eq!(datacenter_factors.get("dc2").unwrap().clone(), 5);
                    }
                    SimpleStrategy { .. } => panic!(),
                }
            }
            Err(_) => panic!(),
        }
    }

    fn test_replication_factory_from_str_error(input: &str, err_msg: &str) {
        let result = input.parse::<ReplicationFactor>();
        match result {
            Ok(_) => panic!(),
            Err(err) => assert_eq!(err.to_string(), err_msg),
        }
    }

    #[test]
    fn test_replication_factory_from_str_error_not_key_value_object() {
        test_replication_factory_from_str_error(
            "you're killing me, smalls",
            "not a valid keyspace replication object",
        );
    }

    #[test]
    fn test_replication_factory_from_str_error_no_key_value_pairs_in_object() {
        test_replication_factory_from_str_error(
            "{not, valid}",
            "not a valid key-value pair in keyspace replication object",
        );
    }

    #[test]
    fn test_replication_factory_from_str_error_no_replication_class() {
        test_replication_factory_from_str_error(
            "{something: else}",
            "replication object missing class field",
        );
    }

    #[test]
    fn test_replication_factory_from_str_error_unsupported_replication_class() {
        test_replication_factory_from_str_error(
            "{'class': 'FooStrategy'}",
            "replication class FooStrategy field is an unsupported type",
        );
    }

    #[test]
    fn test_replication_factory_from_str_error_simple_without_factor() {
        test_replication_factory_from_str_error(
            "{'class': 'SimpleStrategy'}",
            "replication object missing replication_factor field",
        );
    }

    #[test]
    fn test_replication_factory_from_str_error_simple_factor_not_a_number() {
        test_replication_factory_from_str_error(
            "{'class': 'SimpleStrategy', 'replication_factor': 'abc'}",
            "replication factor abc must be a number",
        );
    }

    #[test]
    fn test_replication_factory_from_str_error_network_without_factor() {
        test_replication_factory_from_str_error(
            "{'class': 'NetworkTopologyStrategy'}",
            "network replication must specify at least one datacenter's replication factor",
        );
    }

    #[test]
    fn test_replication_factory_from_str_error_duplicates_key_value_pair() {
        test_replication_factory_from_str_error(
            "{'class': 'NetworkTopologyStrategy', 'dc1': 1, 'dc1': 1}",
            "replication object duplicates key-value pair dc1",
        );
    }

    #[test]
    fn test_replication_factory_from_str_error_network_factor_bad_dc_name() {
        test_replication_factory_from_str_error(
            "{'class': 'NetworkTopologyStrategy', 'my datacenter': 3}",
            "datacenter my datacenter is not a valid name",
        );
    }

    #[test]
    fn test_cql_files_from_dir() {
        let temp_dir = TempDir::new().unwrap();
        ["foo.cql", "foo.sh", "foo.sql"].iter().for_each(|f| {
            fs::OpenOptions::new()
                .write(true)
                .create(true)
                .open(temp_dir.path().join(f))
                .expect("could not write file");
        });
        let temp_dir_path = temp_dir.path().canonicalize().unwrap();
        println!("{}", temp_dir_path.to_string_lossy());
        let result = cql_files_from_dir(&temp_dir_path);
        assert!(result.is_ok());
        let cql_files = result.unwrap();
        assert_eq!(cql_files.len(), 1);
        assert!(cql_files
            .iter()
            .any(|p| { p.file_name().unwrap() == "foo.cql" }));
    }

    #[test]
    fn test_create_keyspace_cql_errors_with_empty_keyspace_name() {
        let opts = KeyspaceOpts {
            name: "".to_string(),
            replication: None,
        };
        let result = create_keyspace_cql(&opts);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "keyspace has empty name".to_string()
        );
    }

    #[test]
    fn test_create_keyspace_cql_with_default_replication() {
        let opts = KeyspaceOpts {
            name: "cquill_migration".to_string(),
            replication: None,
        };
        let result = create_keyspace_cql(&opts);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "create keyspace cquill_migration with replication = { 'class': 'SimpleStrategy', 'replication_factor': 1 }".to_string());
    }

    #[test]
    fn test_create_keyspace_cql_with_simple_strategy_replication() {
        let replication = Some(SimpleStrategy { factor: 3 });
        let opts = KeyspaceOpts {
            name: "cquill_migration".to_string(),
            replication,
        };
        let result = create_keyspace_cql(&opts);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "create keyspace cquill_migration with replication = { 'class': 'SimpleStrategy', 'replication_factor': 3 }".to_string());
    }

    #[test]
    fn test_create_keyspace_cql_with_single_network_topology_replication_factor() {
        let datacenter_factors = HashMap::from([(String::from("dc1"), 7)]);
        let replication = Some(NetworkTopologyStrategy { datacenter_factors });
        let opts = KeyspaceOpts {
            name: "cquill_migration".to_string(),
            replication,
        };
        let result = create_keyspace_cql(&opts);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "create keyspace cquill_migration with replication = { 'class': 'NetworkTopologyStrategy', 'dc1': 7 }".to_string());
    }

    #[test]
    fn test_create_keyspace_cql_with_multiple_network_topology_replication_factors() {
        let datacenter_factors =
            HashMap::from([(String::from("dc1"), 7), (String::from("dc2"), 2)]);
        let replication = Some(NetworkTopologyStrategy { datacenter_factors });
        let opts = KeyspaceOpts {
            name: "cquill_migration".to_string(),
            replication,
        };
        let result = create_keyspace_cql(&opts);
        assert!(result.is_ok());
        let cql = result.unwrap();
        let expect_begin = "create keyspace cquill_migration with replication = {";
        assert!(cql.starts_with(expect_begin));
        assert!(cql.ends_with('}'));
        let mut split: Vec<&str> = cql[expect_begin.len()..cql.len() - 1].split(',').collect();
        split.sort();
        assert_eq!(split.len(), 3);
        assert_eq!(split[0].trim(), "'class': 'NetworkTopologyStrategy'");
        assert_eq!(split[1].trim(), "'dc1': 7");
        assert_eq!(split[2].trim(), "'dc2': 2");
    }

    #[test]
    fn test_create_keyspace_cql_errors_without_datacenter_factors() {
        let datacenter_factors = HashMap::new();
        let replication = Some(NetworkTopologyStrategy { datacenter_factors });
        let opts = KeyspaceOpts {
            name: "cquill_migration".to_string(),
            replication,
        };
        let result = create_keyspace_cql(&opts);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "keyspace cquill_migration network topology replication has no datacenter replication factors");
    }

    #[test]
    fn test_create_keyspace_cql_errors_without_datacenter_name() {
        let datacenter_factors = HashMap::from([(String::from(""), 7)]);
        let replication = Some(NetworkTopologyStrategy { datacenter_factors });
        let opts = KeyspaceOpts {
            name: "cquill_migration".to_string(),
            replication,
        };
        let result = create_keyspace_cql(&opts);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "keyspace cquill_migration network topology replication factor has empty datacenter name");
    }

    #[test]
    fn test_create_keyspace_cql_errors_with_zero_replication_factor() {
        let datacenter_factors = HashMap::from([(String::from("dc1"), 0)]);
        let replication = Some(NetworkTopologyStrategy { datacenter_factors });
        let opts = KeyspaceOpts {
            name: "cquill_migration".to_string(),
            replication,
        };
        let result = create_keyspace_cql(&opts);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "keyspace cquill_migration network topology replication factor for datacenter dc1 is not a positive number");
    }
}
