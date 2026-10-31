# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [v0.0.10] - 2026-10-31

### Changed

- `MigrateOpts` improved with `ConnectionInfo` variants `Cqlshrc`, `Session`
  & `NewSession`. `Cqlshrc` inits the connection from a user's
  `cqlshrc` connection config, `Session` allows providing a custom
  connection using `SessionBuilder` from the `scylla` crate, and `NewSession`
  creates a connection from a `ConnectionOpts` struct with node address,
  authentication and TLS/mTLS options.
- The `CASSANDRA_NODE` env var is no longer used to override the `127.0.0.1:9042`
  default value.

## [0.0.9] - 2024-04-15

[Unreleased]: https://github.com/eighty4/cquill/compare/v0.0.10...HEAD
[0.0.10]: https://github.com/eighty4/cquill/compare/0.0.9...v0.0.10
[0.0.9]: https://github.com/eighty4/cquill/releases/tag/0.0.9
