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

#[derive(Debug)]
pub struct CqlStatement {
    pub cql: String,
    pub lines: (usize, usize),
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

    pub(crate) fn read_statements(&self) -> Result<Vec<CqlStatement>, MigrateError> {
        let cql = match fs::read_to_string(&self.path) {
            Err(err) => {
                return Err(MigrateError::CqlFileReadError {
                    filename: self.filename.clone(),
                    error: err.to_string(),
                });
            }
            Ok(cql) => cql,
        };

        // todo parse an ast bc this is no bueno
        let mut block_comment_begin: Option<usize> = None;
        let mut comments: Vec<(usize, usize)> = Vec::new();
        let mut line_comment_begin: Option<usize> = None;
        let mut line_index = 0;
        let mut prev_c: char = ' ';
        let mut statement_begin_index: usize = 0;
        let mut statement_begin_line: usize = 0;
        let mut statements: Vec<CqlStatement> = Vec::new();
        for (char_index, c) in cql.chars().enumerate() {
            if c == '/' && prev_c == '*' {
                if let Some(i) = block_comment_begin {
                    comments.push((i, char_index + 1));
                    block_comment_begin = None;
                }
            } else if c == '*' && prev_c == '/' && block_comment_begin.is_none() {
                block_comment_begin = Some(char_index - 1);
            } else if (c == '-' && prev_c == '-') || (c == '/' && prev_c == '/') {
                line_comment_begin = Some(char_index - 1);
            } else if c == '\n' {
                line_index += 1;
                if let Some(i) = line_comment_begin {
                    line_comment_begin = None;
                    comments.push((i, char_index));
                }
                let commented_line = if !comments.is_empty() {
                    let mut uncommented_cql = false;
                    let mut cursor = statement_begin_index;
                    for (comment_start, comment_end) in &comments {
                        if !cql[cursor..*comment_start].trim().to_string().is_empty() {
                            uncommented_cql = true;
                            break;
                        }
                        cursor = *comment_end;
                    }
                    if !uncommented_cql && !cql[cursor..char_index].trim().to_string().is_empty() {
                        uncommented_cql = true;
                    }
                    if uncommented_cql {
                        false
                    } else {
                        comments = Vec::new();
                        true
                    }
                } else {
                    cql[statement_begin_index..char_index].trim().is_empty()
                };
                if commented_line {
                    statement_begin_index = char_index;
                    statement_begin_line = line_index;
                }
            } else if c == ';' && block_comment_begin.is_none() && line_comment_begin.is_none() {
                let statement = if comments.is_empty() {
                    cql[statement_begin_index..char_index].to_string()
                } else {
                    let mut parts: Vec<String> = Vec::with_capacity(comments.len() + 1);
                    let mut cursor = statement_begin_index;
                    for (comment_start, comment_end) in &comments {
                        parts.push(cql[cursor..*comment_start].trim().to_string());
                        cursor = *comment_end;
                    }
                    parts.push(cql[cursor..char_index].trim().to_string());
                    comments = Vec::new();
                    parts.join(" ")
                };
                statements.push(CqlStatement {
                    cql: statement,
                    lines: (statement_begin_line + 1, line_index + 1),
                });
                statement_begin_index = char_index + 1;
                statement_begin_line = line_index;
            }
            prev_c = c;
        }

        Ok(statements
            .iter()
            .map(|statement| CqlStatement {
                cql: statement.cql.lines().map(|l| l.trim()).collect(),
                lines: statement.lines,
            })
            .collect())
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
            ));
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
            "  create table big_business_data (id timeuuid primary key)  ;",
        );

        match CqlFile::from_path(cql_file_path) {
            Err(_) => panic!(),
            Ok(cql_file) => {
                assert_eq!(cql_file.filename, String::from("v073-more_tables.cql"));
                assert_eq!(cql_file.version, 73);
                assert_eq!(cql_file.hash, "e995c628cf1a06863dc86760020ecb43");
                let statements_result = cql_file.read_statements();
                assert!(statements_result.is_ok());
                let statements = statements_result.unwrap();
                assert_eq!(statements.len(), 1);
                assert_eq!(
                    statements.get(0).unwrap().cql,
                    "create table big_business_data (id timeuuid primary key)"
                );
            }
        }
    }

    fn read_statements_test(cql: &'static str, expected: Vec<CqlStatement>) {
        let temp_dir = TempDir::new().unwrap();
        let cql_file_path = temp_dir.path().join("v001-no_more_tests.cql");
        make_file(cql_file_path.clone(), cql);
        let cql_file = CqlFile::from_path(cql_file_path).expect("cql file");
        let statements_result = cql_file.read_statements();
        assert!(statements_result.is_ok());
        let statements = statements_result.unwrap();
        assert_eq!(statements.len(), expected.len());
        for (i, statement) in statements.iter().enumerate() {
            let other = expected.get(i).unwrap();
            assert_eq!(statement.cql, other.cql);
            assert_eq!(
                statement.lines.0,
                other.lines.0,
                "{}",
                statement.cql.as_str()
            );
            assert_eq!(
                statement.lines.1,
                other.lines.1,
                "{}",
                statement.cql.as_str()
            );
        }
    }

    #[test]
    fn test_cql_file_read_statements_incomplete_line() {
        read_statements_test(
            "create table big_business_data (id timeuuid primary key)",
            Vec::new(),
        );
    }

    #[test]
    fn test_cql_file_read_statements_complete_line() {
        read_statements_test(
            "create table big_business_data (id timeuuid primary key);",
            Vec::from([CqlStatement {
                cql: "create table big_business_data (id timeuuid primary key)".to_string(),
                lines: (1, 1),
            }]),
        );
    }

    #[test]
    fn test_cql_file_read_statements_two_lines() {
        read_statements_test(
            "create table big_business_data (id timeuuid primary key);
                      create table more_business_data (id timeuuid primary key);",
            Vec::from([
                CqlStatement {
                    cql: "create table big_business_data (id timeuuid primary key)".to_string(),
                    lines: (1, 1),
                },
                CqlStatement {
                    cql: "create table more_business_data (id timeuuid primary key)".to_string(),
                    lines: (2, 2),
                },
            ]),
        );
    }

    #[test]
    fn test_cql_file_read_statements_block_comment_only() {
        read_statements_test(
            "/*create table big_business_data (id timeuuid primary key);*/",
            Vec::new(),
        );
    }

    #[test]
    fn test_cql_file_read_statements_line_comment_only() {
        read_statements_test(
            "--create table big_business_data (id timeuuid primary key);",
            Vec::new(),
        );
    }

    #[test]
    fn test_cql_file_read_statements_multiline_statement_with_line_comments() {
        read_statements_test(
            "create table big_business_data (
            id timeuuid primary key,
            -- here's some docs
            data text, -- and more docs
            created timestamp
            );",
            vec![CqlStatement {
                cql: "create table big_business_data (id timeuuid primary key, data text, created timestamp)"
                    .to_string(),
                lines: (1, 6),
            }],
        );
    }

    #[test]
    fn test_cql_file_read_statements_block_comment_in_statement() {
        read_statements_test(
            "create table big_business_data (
            /*id timeuuid primary key,*/
            another_id uuid primary key,
            /*data text,*/
            data text
            );",
            vec![CqlStatement {
                cql: "create table big_business_data ( another_id uuid primary key, data text)"
                    .to_string(),
                lines: (1, 6),
            }],
        );
    }

    #[test]
    fn test_cql_file_read_statements_line_comment_out_statement_ending() {
        read_statements_test(
            "create table big_business_data (--id timeuuid primary key);\nanother_id uuid primary key);",
            vec!(
                CqlStatement {
                    cql: "create table big_business_data ( another_id uuid primary key)".to_string(),
                    lines: (1, 2),
                })
        );
    }

    #[test]
    fn test_cql_file_read_statements_block_comment_between_statements() {
        read_statements_test(
            "create table big_business_data (id timeuuid primary key);
            /*create table another_business_data (id timeuuid primary key);*/
            create table more_business_data (id timeuuid primary key);",
            vec![
                CqlStatement {
                    cql: "create table big_business_data (id timeuuid primary key)".to_string(),
                    lines: (1, 1),
                },
                CqlStatement {
                    cql: "create table more_business_data (id timeuuid primary key)".to_string(),
                    lines: (3, 3),
                },
            ],
        );
    }

    #[test]
    fn test_cql_file_read_statements_line_comment_between_statements() {
        read_statements_test(
            "create table big_business_data (id timeuuid primary key);
            --create table another_business_data (id timeuuid primary key);
            create table more_business_data (id timeuuid primary key);",
            vec![
                CqlStatement {
                    cql: "create table big_business_data (id timeuuid primary key)".to_string(),
                    lines: (1, 1),
                },
                CqlStatement {
                    cql: "create table more_business_data (id timeuuid primary key)".to_string(),
                    lines: (3, 3),
                },
            ],
        );
    }

    #[test]
    fn test_cql_file_read_statements_partial_line_comment_between_statements() {
        read_statements_test(
            "create table big_business_data (id timeuuid primary key);
            create table --another_business_data (id timeuuid primary key);
            more_business_data (id timeuuid primary key);
            create table even_more_business_data (id timeuuid primary key);",
            vec![
                CqlStatement {
                    cql: "create table big_business_data (id timeuuid primary key)".to_string(),
                    lines: (1, 1),
                },
                CqlStatement {
                    cql: "create table more_business_data (id timeuuid primary key)".to_string(),
                    lines: (2, 3),
                },
                CqlStatement {
                    cql: "create table even_more_business_data (id timeuuid primary key)"
                        .to_string(),
                    lines: (4, 4),
                },
            ],
        );
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
