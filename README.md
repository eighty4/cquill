[![CI](https://github.com/eighty4/cquill/actions/workflows/ci_verify.yml/badge.svg)](https://github.com/eighty4/cquill/actions/workflows/ci_verify.yml)

# Versioning & migrating CQL schemas for Cassandra & ScyllaDB

> "VC interest is welcome."

## The `what`

`cquill` is a CLI for versioned CQL migrations for Cassandra & ScyllaDB databases.

#### Develop your product with predictable & automated schema migrations.

#### Version, package, and release your schema evolution the same way you version and package your services' code.

#### Tested on Linux, MacOS & Windows systems.

#### Containerized releases ready for DevOps workflows.

## The `why`

Software systems evolve: data schemas changing *is* data.

`cquill` enhances your migrations with conventions *for developer efficiency*, integrity & validation checks *for release safety*, and is built for **modern software delivery**.

`cquill` is ready to add value & improve velocity whether your project is greenfield or legacy.

## The `where`

`cquill` works with Cassandra & ScyllaDB and serverless platforms like Amazon Keyspaces, DataStax Astra DB, Azure Cosmos DB & ScyllaDB Cloud.

## The `who`

Every organization should practice the paradigms `cquill` was built for.

## The `when`

`cquill` helps delivery teams during development & release orchestration.

## The `how`

`cquill` has been built [The Apache Way](https://www.apache.org/#software-for-the-public-good) & on the ingenuity of [the ScyllaDB folks](https://rust-driver.docs.scylladb.com/stable/).

# Using `cquill`

## `cquill migrate`

```bash
cquill migrate -d ./cql

# for all migrate options
cquill migrate -h
```

CQL sources reside in the `./cql` directory by default and can be overriden with `--cql-dir` or `-d`.

CQL files are versioned with a 3 digit version prefix specified `v001` or `V001` and must be sequentially versioned. `v001-create-api-keyspace.cql` is a valid example.

Migration history is stored in a table named `cquill.migrated_cql` with a md5 hash record for every completed CQL file. Future migrations will validate previously migrated CQL files against the md5 hashes. This step ensures correctness and prevents a migration that could cause data integrity problems.

Use `cquill help migrate` for parameters. The database connection options and the migration history table's keyspace, name & replication can be configured with the `migrate` command's parameters.

On the event of a CQL statement error, `cquill` will stop executing statements from the file and report which statement failed. Remediation at this point is a manual process and guidance is included with the failed migration report.

## Getting started

Prebuilt binaries are available in the [latest GitHub release](https://github.com/eighty4/cquill/releases/latest).

`cquill` can also be built with Cargo/Rust, run as a Docker image, or used as a Rust library.

### Install locally with Cargo

Cargo will build the latest published version of `cquill` with install:

```bash
cargo install cquill
```

Versions published with Cargo are detailed on [crates.io/crates/cquill](https://crates.io/crates/cquill/versions).

### Run a migration with local CQL sources in a Docker container

Image `84tech/cquill` will migrate CQL sources in its `/cquill/cql` directory (documented in the `cquill` [Dockerfile](./cquill.install.Dockerfile)).

This approach requires specifying the `CASSANDRA_NODE` env variable to match Cassandra's hostname & the Docker network:

```bash
docker run -it --rm -v $(pwd)/cql:/cquill/cql:ro -e CASSANDRA_NODE=cassandra --network my_network 84tech/cquill migrate
```

### Create a Docker image of versioned CQL sources

Versioning CQL to distribute with a service release is ideal for automation in a containerized environment using CI/CD automation & container orchestrators.

Copy the release's CQL sources `./cql` relative to the `WORKDIR`:

```dockerfile
FROM 84tech/cquill
WORKDIR /
COPY cql cql
```

Given a `./cql` directory and a `cql.Dockerfile` build manifest, `docker build` a versioned image to be deployed in coordination with the API:

```bash
docker build -t my-api-cql:0.0.1 -f cql.Dockerfile .
```

Now this CQL can be scheduled by your orchestrator to migrate to your service environemnt as part of your release pipeline:

```bash
docker run my-api-cql:0.0.1 migrate
```

### Using `cquill` as a library

Rust projects can embed the `cquill` crate into their project to keep an environment's CQL up-to-date as new versions of a project is deployed or run `cquill` from unit tests.

The examples show the [cquill::migrate_cql](./cquill/src/lib.rs) API that can be used directly from your Rust project. Check them out in the [./examples directory](./cquill/examples).

## Contributing

See [docs/CONTRIBUTING.md] for guidelines on contributing to `cquill`.

## Roadmap
  
This is a list of features that I hope to add in the future:

- `cquill verify` command validates CQL file names, CQL connection, and the md5 hashes of previously migrated CQL files
- `cquill doctor` command corrects migration history and md5 hashes
- `cquill dev` command using file watches to drop and recreate keyspaces and tables during active development
- Support `v001.dev.cql` or similar dev-annotated filenames to populate development environments with data
- Create an AST for CQL statements, enabling support for several additional features:
  - rewrite keyspace names for a migration to create parallel deploys of a system's keyspaces (useful for isolated testing)
  - validate CQL statement syntax before executing against a live database
  - resolve specific line and column data for CQL statements for command output
  - invert CQL statements, such as creating an `ALTER TABLE` to drop a column from a statement that creates the column, to revert statements executed before an error prevents a CQL file from completing
