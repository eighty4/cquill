#!/bin/sh
set -e

# run through all the checks done for ci

_git_status_output=$(git status --porcelain)

echo '\n*** cargo build ***'
cargo build

echo '\n*** cargo fmt -v ***'
cargo fmt -v
if [ -z "$_git_status_output" ]; then
  git diff --exit-code
fi

echo '\n*** cargo test ***'
cargo test

echo '\n*** cargo clippy -- -D warnings ***'
cargo clippy -- -D warnings

echo '\n*** cargo run --example(s) ***'
cargo run --example migrate

if [ -n "$_git_status_output" ]; then
  echo
  echo "all ci verifications passed"
  echo "however, working directory had uncommited changes before running cargo fmt"
  exit 1
fi
