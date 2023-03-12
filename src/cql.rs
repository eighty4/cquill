use std::fs;
use std::path::PathBuf;

use anyhow::{anyhow, Result};
use lazy_static::lazy_static;
use regex::Regex;

lazy_static! {
    static ref FILENAME_REGEX: Regex =
        regex::Regex::new(r"^[Vv](?P<version>[\d]{3})(?:[-_\da-zA-Z]*)?.cql$")
            .expect("cql filename regex");
}

pub(crate) struct CqlFile {
    pub filename: String,
    pub hash: String,
    pub version: i16,
}

impl CqlFile {
    pub fn from(path: &PathBuf) -> Result<CqlFile> {
        let filename = path.file_name().unwrap().to_string_lossy().to_string();
        if !FILENAME_REGEX.is_match(filename.as_str()) {
            return Err(anyhow!("{filename} is not a valid cql file name"));
        }
        let hash = match fs::read(path) {
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

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::Write;

    use temp_dir::TempDir;

    use super::*;

    #[test]
    fn test_cql_filename_regex() {
        assert!(FILENAME_REGEX.is_match("v000.cql"));
        assert!(FILENAME_REGEX.is_match("v001-init-schema.cql"));
        assert!(FILENAME_REGEX.is_match("V002_add_column_families.cql"));
        assert!(!FILENAME_REGEX.is_match("init-schema.cql"));
    }

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

        match CqlFile::from(&cql_file_path) {
            Err(_) => panic!(),
            Ok(cql_file) => {
                assert_eq!(cql_file.filename, String::from("v073-more_tables.cql"));
                assert_eq!(cql_file.version, 73);
                assert_eq!(cql_file.hash, "7f5b4bdccd3863f31be5c257ff497704");
            }
        }
    }
}
