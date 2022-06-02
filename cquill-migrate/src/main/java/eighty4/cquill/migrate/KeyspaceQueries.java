package eighty4.cquill.migrate;

public class KeyspaceQueries {

    static String dropKeyspace(String keyspace) {
        return "drop keyspace %s".formatted(keyspace);
    }

    static String createLocalKeyspace(String keyspace) {
        return """
                create keyspace %s with replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
                """.formatted(keyspace);
    }

    static String createMigrationTable(String keyspace, String migrationTable) {
        return """
                create table %s.%s (
                id timeuuid primary key,
                ver int,
                name varchar,
                )
                """.formatted(keyspace, migrationTable);
    }

    static String keyspaceExists(String keyspace) {
        return """
                select table_name from system_schema.tables where keyspace_name='%s'
                """.formatted(keyspace);
    }

    static String migratedCql(String keyspace, String migrationTable) {
        return "select ver, name from %s.%s".formatted(keyspace, migrationTable);
    }
}
