# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Changed

- `MigrateOpts` improved with `ConnectionOpts` that allows using a `Session`
  from the `scylla` crate. This allows using the `scyalla` crate's
  `SessionBuilder` to support mTLS and all other supported Amazon Keyspaces,
  Astra DB, Cassandra & ScyllaDB connection schemes. `MigrateOpts.cassandra_opts`
  is now `MigrateOpts.connection_opts`.
- `ConnectionOpts::default()` no longer uses the `CASSANDRA_NODE` env var
  that `CassandraOpts` used to override the `127.0.0.1:9042` default value.
  Checking `CASSANDRA_NODE` is left out of the crate api and is now done by
  the `cquill` binary when resolving a connection config and not providing
  a connected `Session`.

## 0.0.9 - 2024-04-15

[Unreleased]: https://github.com/eighty4/cquill/compare/0.0.9...HEAD
[0.0.9]: https://github.com/eighty4/cquill/releases/tag/0.0.9
