[Rust](https://rustup.rs/) and [Docker](https://www.docker.com/get-started/) are the only development dependencies for `cquill`.

Use `docker compose up scylla_2026 -d --wait` before running `cargo test`.

Document any user-level changes made by your PR that affect CLI usage or the lib API in `CHANGELOG.md` under the `Unreleased` section.

## `docker-compose.yaml`

Services are configured for all supported Cassandra & ScyllaDB versions (3 through 6 & 5 through 2026, respectively).

All of the services are bound to `:9042`. Only one service is meant to run for `cargo test` so you must specify the service name when running `docker compose up`.

## `./ci_verify.sh`

`ci_verify.sh` will perform all checks required by a PR. Running this script locally ensures your changes will pass the [ci_verify.yml](.github/workflows/verify.yml) GitHub workflow.

`ci_verify.sh` has flags to create symlinks to itself in `.git/hooks`:

```bash
./ci_verify.sh --on-git-commit
./ci_verify.sh --on-git-push
```

## `./int_tests.sh`

All connection options are tested with the `cquill` crate's integration tests. Testing authentication and SSL configurations separate from functional tests is a huge time savings!

`int_tests.sh` runs each scenario without parallelism due to hardware constraints (vs running up to 35 database containers).
