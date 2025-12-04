// use std::{
//     collections::HashMap,
//     fs::{read_dir, read_to_string}, path::PathBuf,
// };

// use crate::{ParseResult, parse_cql, ast::CqlStatement};

// #[test]
// fn int_test() {
//     let paths: Vec<PathBuf> = read_dir("out")
//         .unwrap()
//         .map(|dir_entry| dir_entry.unwrap().path())
//         .collect();
//     let mut results: Vec<(String, std::thread::Result<ParseResult<Vec<CqlStatement>>>)> = Vec::new();
//     for p in paths {
//         let cql = read_to_string(&p).unwrap();
//         let file_name = p.file_name().unwrap().to_string_lossy().to_string();
//         println!("{file_name}");
//         let result = std::panic::catch_unwind(|| {
//             parse_cql(cql)
//         });
//         dbg!(&result);
//         results.push((file_name, result));
//     }

//     for (file_name, result) in results {
//         if result.is_err() {
//             dbg!(result);
//             panic!();
//         }
//     }
// }
