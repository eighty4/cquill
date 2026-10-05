#!/bin/sh
set -e

docker pull scylladb/scylla:2026.1
docker pull scylladb/scylla:2025.1
docker pull scylladb/scylla:6.2
docker pull scylladb/scylla:5.4

docker pull cassandra:6.0
docker pull cassandra:5.0
docker pull cassandra:4.1

run_test() {
    local _engine="$1"
    local _version="$2"
    local _test="$3"
    echo "\n~~\n$_engine $_version $_test\n~~\n"
    CQUILL_TEST_DB="$_engine" CQUILL_TEST_DB_VERSION="$_version" cargo test -p cquill --test integration_tests -- --exact "$_test"
}

run_test "scylla" "2026" "test_migrate_anon_tcp"
run_test "scylla" "2026" "test_migrate_password_auth"
run_test "scylla" "2026" "test_migrate_tls_no_password"
run_test "scylla" "2026" "test_migrate_tls_password_auth"
run_test "scylla" "2026" "test_migrate_mtls"

run_test "scylla" "2025" "test_migrate_anon_tcp"
run_test "scylla" "2025" "test_migrate_password_auth"
run_test "scylla" "2025" "test_migrate_tls_no_password"
run_test "scylla" "2025" "test_migrate_tls_password_auth"
run_test "scylla" "2025" "test_migrate_mtls"

run_test "scylla" "6" "test_migrate_anon_tcp"
run_test "scylla" "6" "test_migrate_password_auth"
run_test "scylla" "6" "test_migrate_tls_no_password"
run_test "scylla" "6" "test_migrate_tls_password_auth"
run_test "scylla" "6" "test_migrate_mtls"

run_test "scylla" "5" "test_migrate_anon_tcp"
run_test "scylla" "5" "test_migrate_password_auth"
run_test "scylla" "5" "test_migrate_tls_no_password"
run_test "scylla" "5" "test_migrate_tls_password_auth"
run_test "scylla" "5" "test_migrate_mtls"

run_test "cassandra" "6" "test_migrate_anon_tcp"
run_test "cassandra" "6" "test_migrate_password_auth"
run_test "cassandra" "6" "test_migrate_tls_no_password"
run_test "cassandra" "6" "test_migrate_tls_password_auth"
run_test "cassandra" "6" "test_migrate_mtls"

run_test "cassandra" "5" "test_migrate_anon_tcp"
run_test "cassandra" "5" "test_migrate_password_auth"
run_test "cassandra" "5" "test_migrate_tls_no_password"
run_test "cassandra" "5" "test_migrate_tls_password_auth"
run_test "cassandra" "5" "test_migrate_mtls"

run_test "cassandra" "4" "test_migrate_anon_tcp"
run_test "cassandra" "4" "test_migrate_password_auth"
run_test "cassandra" "4" "test_migrate_tls_no_password"
run_test "cassandra" "4" "test_migrate_tls_password_auth"
run_test "cassandra" "4" "test_migrate_mtls"
