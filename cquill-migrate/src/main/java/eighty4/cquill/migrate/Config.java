package eighty4.cquill.migrate;

import java.util.Optional;

class Config {

    private static final String ENV_KEY_CASSANDRA_HOST = "CQUILL_CASSANDRA_HOST";

    private static final String ENV_KEY_CASSANDRA_PORT = "CQUILL_CASSANDRA_PORT";

    private static final String ENV_KEY_MIGRATE_CQL_DIR = "CQUILL_MIGRATE_CQL_DIR";

    private static final String ENV_KEY_MIGRATE_KEYSPACE = "CQUILL_MIGRATE_KEYSPACE";

    private static final String ENV_KEY_MIGRATE_REFRESH = "CQUILL_MIGRATE_REFRESH";

    private static final String ENV_KEY_MIGRATE_TABLE = "CQUILL_MIGRATE_TABLE";

    static String cassandraHost() {
        return orDefault(ENV_KEY_CASSANDRA_HOST, "localhost");
    }

    static int cassandraPort() {
        return env(ENV_KEY_CASSANDRA_PORT)
                .map(portStr -> {
                    try {
                        return Integer.parseInt(portStr, 10);
                    } catch (Exception e) {
                        throw new IllegalStateException("unable to parse CQUILL_CASSANDRA_PORT as int");
                    }
                })
                .orElse(9042);
    }

    static String cqlDir() {
        return expect(ENV_KEY_MIGRATE_CQL_DIR);
    }

    static String keyspace() {
        return expect(ENV_KEY_MIGRATE_KEYSPACE);
    }

    static String migrationTable() {
        return orDefault(ENV_KEY_MIGRATE_TABLE, "migrated_cql");
    }

    static boolean refreshKeyspace() {
        return orDefault(ENV_KEY_MIGRATE_REFRESH, "false").equals("true");
    }

    private static Optional<String> env(String key) {
        return Optional.ofNullable(System.getenv(key));
    }

    private static String expect(String key) {
        return env(key).orElseThrow(() -> new IllegalStateException("must set " + key));
    }

    private static String orDefault(String key, String def) {
        return env(key).orElse(def);
    }
}
