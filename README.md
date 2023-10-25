[![CI](https://img.shields.io/github/actions/workflow/status/eighty4/cquill/verify.yml)](https://github.com/eighty4/cquill/actions/workflows/verify.yml)

# Versioned CQL migrations for Cassandra and ScyllaDB

Create a directory with CQL files. Develop with Cquill locally. Then version, package and release with Cquill to your database deployment.

VC interest is welcome.

## Migrate command

`cquill migrate` performs a migration using cql sources from the `./cql` directory.

CQL files are versioned with a 3 digit version prefix specified `v001` or `V001` and must be sequentially versioned.
`v001-create-api-keyspace.cql` is valid while `v8.cql` is not valid.

On the event of a CQL statement error, Cquill will stop executing statements from the file and report which statement failed.
Remediation at this point is a manual process and guidance is included with the error message from Cassandra.

Migration history is stored in a table named `cquill.migrated_cql` with a md5 hash record for every completed CQL file.
Future migrations will validate previously migrated CQL files against the md5 hashes.
This step ensures correctness and prevents a migration that could cause data integrity problems.

Use `cquill help migrate` for parameters.
The migration history table's keyspace, name and replication can be configured with the migrate command's parameters.

## Getting started

### Install locally with Cargo

Cargo will build the latest published version of Cquill with install:

```bash
cargo install cquill
```

Versions published with Cargo are detailed on [crates.io/crates/cquill](https://crates.io/crates/cquill/versions).

### Run a migration with local CQL sources in a Docker container

Image `84tech/cquill` will migrate CQL sources in its `/cquill/cql` directory (documented in Cquill's [Dockerfile](cquill.install.Dockerfile)).

This approach requires specifying the `CASSANDRA_NODE` env variable to match Cassandra's hostname and the Docker network:

```bash
docker run -it --rm -v $(pwd)/cql:/cquill/cql:ro -e CASSANDRA_NODE=cassandra --network my_network 84tech/cquill migrate
```

### Create a Docker image of versioned CQL sources

In a containerized environment using CI/CD automation, versioning an artifact is ideal for workflow automation.
Copy the release's CQL sources `./cql` relative to the `WORKDIR`:

```dockerfile
FROM 84tech/cquill
WORKDIR /
COPY cql cql
```

Given a `./cql` directory and a `cql.Dockerfile` build manifest, Docker build a versioned image to be deployed in coordination with the API:

```bash
docker build -t my-api-cql:0.0.1 -f cql.Dockerfile .
```

## Contributing

[Rust](https://rustup.rs/) and [Docker](https://www.docker.com/get-started/) are Cquill's only development dependencies.

Use `docker compose up -d --wait` to launch a ScyllaDB instance for running `cargo test`.

CI checks on pull requests are detailed in the [verify.yml](.github/workflows/verify.yml) workflow. This workflow runs:

```bash
cargo fmt
cargo clippy -- -D warnings
cargo test
cargo build --release
```

## Roadmap

This is a list of nice-to-have features that I will hope to add in the future:

- `cquill verify` command validates CQL file names, CQL connection, and the md5 hashes of previously migrated CQL files
- `cquill doctor` command corrects migration history and md5 hashes
- `cquill dev` command using file watches to drop and recreate keyspaces and tables during active development
- Support `v001.dev.cql` or similar dev-annotated filenames to populate development environments with data
- Create an AST for CQL statements, enabling support for several additional features:
  - rewrite keyspace names for a migration to create parallel deploys of a system's keyspaces (useful for isolated testing)
  - validate CQL statement syntax before executing against a live database
  - resolve specific line and column data for CQL statements for command output
  - invert CQL statements, such as creating an `ALTER TABLE` to drop a column from a statement that creates the column, to revert statements executed before an error prevents a CQL file from completing
