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

pub struct CqlFile {
    pub filename: String,
    pub hash: String,
    pub path: PathBuf,
    pub version: i16,
}

impl CqlFile {
    pub fn from_path(path: PathBuf) -> Result<CqlFile> {
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
            path,
            version,
        })
    }
}

pub(crate) fn files_from_dir(cql_dir: &PathBuf) -> Result<Vec<CqlFile>> {
    match fs::read_dir(cql_dir) {
        Err(_) => Err(anyhow!(
            "could not find directory '{}'",
            cql_dir.to_string_lossy()
        )),
        Ok(read_dir) => {
            let mut cql_file_paths = Vec::new();
            for dir_entry in read_dir {
                let path = dir_entry?.path();
                if path.is_file() && path.file_name().is_some() {
                    if let Some(extension) = path.extension() {
                        if extension == "cql" {
                            cql_file_paths.push(path);
                        }
                    }
                }
            }
            if cql_file_paths.is_empty() {
                return Err(anyhow!(
                    "no cql files found in directory '{}'",
                    cql_dir.to_string_lossy()
                ));
            }
            cql_file_paths.sort();
            let mut cql_files = Vec::with_capacity(cql_file_paths.len());
            for path in cql_file_paths {
                cql_files.push(CqlFile::from_path(path)?);
            }
            Ok(cql_files)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::Write;

    use temp_dir::TempDir;

    use crate::test_utils::make_file;

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

        match CqlFile::from_path(cql_file_path) {
            Err(_) => panic!(),
            Ok(cql_file) => {
                assert_eq!(cql_file.filename, String::from("v073-more_tables.cql"));
                assert_eq!(cql_file.version, 73);
                assert_eq!(cql_file.hash, "7f5b4bdccd3863f31be5c257ff497704");
            }
        }
    }

    #[test]
    fn test_files_from_dir() {
        let temp_dir = TempDir::new().unwrap();
        ["v001.cql", "foo.sh", "foo.sql"]
            .iter()
            .for_each(|f| make_file(temp_dir.path().join(f)));
        let temp_dir_path = temp_dir.path().canonicalize().unwrap();

        match files_from_dir(&temp_dir_path) {
            Err(err) => {
                println!("{err}");
                panic!();
            }
            Ok(cql_files) => {
                assert_eq!(cql_files.len(), 1);
                assert!(cql_files.iter().any(|p| { p.filename == "v001.cql" }));
            }
        }
    }

    #[test]
    fn test_files_from_dir_errors_with_cql_name() {
        let temp_dir = TempDir::new().unwrap();
        ["foo.cql", "foo.sh", "foo.sql"]
            .iter()
            .for_each(|f| make_file(temp_dir.path().join(f)));
        let temp_dir_path = temp_dir.path().canonicalize().unwrap();

        match files_from_dir(&temp_dir_path) {
            Ok(_) => panic!(),
            Err(err) => {
                assert_eq!(err.to_string(), "foo.cql is not a valid cql file name");
            }
        }
    }
}
