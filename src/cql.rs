use std::fmt::{Debug, Display, Formatter};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Result};
use lazy_static::lazy_static;
use regex::Regex;

use crate::MigrateError;

lazy_static! {
    static ref FILENAME_REGEX: Regex =
        regex::Regex::new(r"^[Vv](?P<version>[\d]{3})(?:[-_\da-zA-Z]*)?.cql$")
            .expect("cql filename regex");
}

#[derive(Clone, Debug)]
pub struct CqlFile {
    pub filename: String,
    pub hash: String,
    pub path: PathBuf,
    pub version: i16,
}

impl Display for CqlFile {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.filename)
    }
}

impl CqlFile {
    pub fn from_path(path: PathBuf) -> Result<CqlFile> {
        let filename = path.file_name().unwrap().to_string_lossy().to_string();
        if !FILENAME_REGEX.is_match(filename.as_str()) {
            // todo use MigrateError for main.rs to handle error with
            //  info about _ for .cql files to be omitted from migrate
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

    pub(crate) fn read_statements(&self) -> Result<Vec<String>, MigrateError> {
        let cql = match fs::read_to_string(&self.path) {
            Err(err) => {
                return Err(MigrateError::CqlFileReadError {
                    filename: self.filename.clone(),
                    error: err.to_string(),
                })
            }
            Ok(cql) => cql
                .split(';')
                .map(|s| s.replace('\n', "").trim().to_string())
                .filter(|s| !s.is_empty())
                .collect(),
        };
        Ok(cql)
    }
}

pub(crate) fn files_from_dir(cql_dir: &PathBuf) -> Result<Vec<CqlFile>> {
    let cql_file_paths = read_cql_file_paths(cql_dir)?;
    let mut cql_files: Vec<CqlFile> = Vec::with_capacity(cql_file_paths.len());
    let mut expected_version: i16 = 1;
    for path in cql_file_paths {
        let cql_file = CqlFile::from_path(path)?;
        if cql_file.version != expected_version {
            return if cql_file.version == expected_version - 1 {
                let previous_index = usize::try_from(expected_version - 2)?;
                let previous_filename = &cql_files.get(previous_index).unwrap().filename;
                Err(anyhow!(
                    "{} and {} repeat versions instead of incrementing to v{:0>3}",
                    previous_filename,
                    cql_file.filename,
                    expected_version
                ))
            } else {
                Err(anyhow!(
                    "{} found without a preceding v{:0>3} version cql file",
                    cql_file.filename,
                    expected_version
                ))
            };
        }
        cql_files.push(cql_file);
        expected_version += 1;
    }
    Ok(cql_files)
}

fn read_cql_file_paths(cql_dir: &PathBuf) -> Result<Vec<PathBuf>> {
    let dir_read = match fs::read_dir(cql_dir) {
        Err(_) => {
            return Err(anyhow!(
                "could not find directory '{}'",
                cql_dir.to_string_lossy()
            ))
        }
        Ok(dir_read) => dir_read,
    };
    let mut cql_file_paths = Vec::new();
    for dir_entry in dir_read {
        let path = dir_entry?.path();
        if is_inclusive_cql_filename(&path) {
            cql_file_paths.push(path);
        }
    }
    cql_file_paths.sort();
    if cql_file_paths.is_empty() {
        return Err(anyhow!(
            "no cql files found in directory '{}'",
            cql_dir.to_string_lossy()
        ));
    }
    Ok(cql_file_paths)
}

fn is_inclusive_cql_filename(path: &Path) -> bool {
    if path.is_file() {
        if let Some(file_name) = path.file_name() {
            if let Some(extension) = path.extension() {
                if extension == "cql" && !file_name.to_string_lossy().starts_with('_') {
                    return true;
                }
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
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
        make_file(
            cql_file_path.clone(),
            "create table big_business_data (id timeuuid primary key)",
        );

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
            .for_each(|f| make_file(temp_dir.path().join(f), ""));
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
        ["foo.cql"]
            .iter()
            .for_each(|f| make_file(temp_dir.path().join(f), ""));
        let temp_dir_path = temp_dir.path().canonicalize().unwrap();

        match files_from_dir(&temp_dir_path) {
            Ok(_) => panic!(),
            Err(err) => {
                assert_eq!(err.to_string(), "foo.cql is not a valid cql file name");
            }
        }
    }

    #[test]
    fn test_files_from_dir_allows_non_migrating_cql() {
        let temp_dir = TempDir::new().unwrap();
        ["v001-foo.cql", "_foo.cql"]
            .iter()
            .for_each(|f| make_file(temp_dir.path().join(f), ""));
        let temp_dir_path = temp_dir.path().canonicalize().unwrap();

        match files_from_dir(&temp_dir_path) {
            Ok(cql_files) => {
                assert_eq!(cql_files.len(), 1);
                assert_eq!(
                    cql_files.get(0).unwrap().filename,
                    "v001-foo.cql".to_string()
                );
            }
            Err(err) => panic!("should not have errored with: {err}"),
        }
    }

    #[test]
    fn test_files_from_dir_errors_with_out_of_order_versions() {
        let temp_dir = TempDir::new().unwrap();
        ["v001-foo.cql", "v002-foo.cql", "v004-foo.cql"]
            .iter()
            .for_each(|f| make_file(temp_dir.path().join(f), ""));
        let temp_dir_path = temp_dir.path().canonicalize().unwrap();

        match files_from_dir(&temp_dir_path) {
            Ok(_) => panic!("cql::files_from_dir should have errored"),
            Err(err) => {
                assert_eq!(
                    err.to_string(),
                    "v004-foo.cql found without a preceding v003 version cql file"
                );
            }
        }
    }

    #[test]
    fn test_files_from_dir_errors_with_repeating_versions() {
        let temp_dir = TempDir::new().unwrap();
        ["v001-foo.cql", "v001-bar.cql"]
            .iter()
            .for_each(|f| make_file(temp_dir.path().join(f), ""));
        let temp_dir_path = temp_dir.path().canonicalize().unwrap();

        match files_from_dir(&temp_dir_path) {
            Ok(_) => panic!("cql::files_from_dir should have errored"),
            Err(err) => {
                assert_eq!(
                    err.to_string(),
                    "v001-bar.cql and v001-foo.cql repeat versions instead of incrementing to v002"
                );
            }
        }
    }
}
