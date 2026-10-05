#!/bin/sh
set -e

if ! nc -z -w 1 "127.0.0.1" 9042 > /dev/null 2>&1; then
    echo "\033[31merror:\033[0m running scylladb is required for testing\n"
    echo "  docker compose up scylla_2026 -d --wait\n"
    exit 1
fi

# run through all the checks done for ci

_git_status_output=$(git status --porcelain)

echo '\n*** cargo build ***'
cargo build --workspace

echo '\n*** cargo fmt ***'
cargo fmt --all
if [ -z "$_git_status_output" ]; then
  git diff --exit-code
fi

echo '\n*** cargo clippy -- -D warnings ***'
cargo clippy --workspace -- -D warnings

echo '\n*** cargo clippy --tests -- -D warnings ***'
cargo clippy --workspace --tests -- -D warnings

echo '\n*** cargo test `cquill` crate ***'
cargo test -p cquill_ast

echo '\n*** cargo test `cquill_ast` crate ***'
cargo test -p cquill --bin cquill
cargo test -p cquill --lib
cargo test -p cquill --doc

echo '\n*** cargo run `cquill` crate --example "*" ***'
cargo run -p cquill --example cqlshrc_password_auth
cargo run -p cquill --example password_auth
cargo run -p cquill --example default_connection
cargo run -p cquill --example session_connection

if [ -n "$_git_status_output" ]; then
  echo
  echo "all ci verifications passed"
  echo "however, working directory had uncommited changes before running cargo fmt"
  exit 1
fi
