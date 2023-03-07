use crate::keyspace::{ReplicationFactor::*, *};
use anyhow::{anyhow, Result};
use lazy_static::lazy_static;
use regex::Regex;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

lazy_static! {
    static ref FILENAME_REGEX: Regex =
        regex::Regex::new(r"^v(?P<version>[\d]{3})(?:[-_\da-zA-Z]*)?.cql$")
            .expect("cql filename regex");
}

#[allow(dead_code)]
pub(crate) struct CqlFile {
    pub filename: String,
    pub hash: String,
    pub version: i16,
}

impl CqlFile {
    #[allow(dead_code)]
    pub fn from(path: PathBuf) -> Result<CqlFile> {
        let filename = path.file_name().unwrap().to_string_lossy().to_string();
        if !FILENAME_REGEX.is_match(filename.as_str()) {
            return Err(anyhow!("{filename} is not a valid cql file name"));
        }
        let hash = match fs::read(&path) {
            Err(err) => return Err(anyhow!("failed reading file {}: {err}", filename)),
            Ok(file_content) => format!("{:x}", md5::compute(file_content)),
        };
        let version = FILENAME_REGEX
            .captures(filename.as_str())
            .unwrap()
            .name("version")
            .unwrap()
            .as_str()
            .parse::<i16>()
            .unwrap();
        Ok(CqlFile {
            filename,
            hash,
            version,
        })
    }
}

pub(crate) fn create_keyspace_cql(keyspace_opts: &KeyspaceOpts) -> Result<String> {
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
    use std::fs;
    use std::io::Write;
    use temp_dir::TempDir;

    #[test]
    fn test_cql_file() {
        let temp_dir = TempDir::new().unwrap();
        let cql_file_path = temp_dir.path().join("v073-more_tables.cql");
        let mut file = fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(&cql_file_path)
            .expect("could not write file");
        let cql_file_content = "create table big_business_data (id timeuuid primary key)";
        file.write_all(cql_file_content.as_bytes())
            .expect("write to file");

        match CqlFile::from(cql_file_path) {
            Err(_) => panic!(),
            Ok(cql_file) => {
                assert_eq!(cql_file.filename, String::from("v073-more_tables.cql"));
                assert_eq!(cql_file.version, 73);
                assert_eq!(cql_file.hash, "7f5b4bdccd3863f31be5c257ff497704");
            }
        }
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
