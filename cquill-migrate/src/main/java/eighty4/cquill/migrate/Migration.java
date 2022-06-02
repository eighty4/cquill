package eighty4.cquill.migrate;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;

import java.io.File;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.datastax.oss.driver.api.core.cql.SimpleStatement.newInstance;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static eighty4.cquill.migrate.Config.*;
import static eighty4.cquill.migrate.KeyspaceQueries.*;

public class Migration {

    private static final Logger log = Logger.getLogger(Migration.class.getSimpleName());

    private static final String CQL_PATTERN = "V\\d{3}__.*\\.cql";

    public static void main(String[] args) {
        String keyspace = keyspace();
        String cqlDir = cqlDir();
        String migrationTable = migrationTable();
        boolean refreshKeyspace = refreshKeyspace();

        CqlSession cqlSession;
        try {
            cqlSession = CqlSessionFactory.create();
        } catch (Exception e) {
            log.severe("unable to connect to Cassandra");
            throw e;
        }

        new Migration(
                cqlSession,
                cqlDir,
                keyspace,
                migrationTable,
                refreshKeyspace
        ).execute();
    }

    private final String cqlDir;

    private final String keyspace;

    private final String migrationTable;

    private final boolean refreshKeyspace;

    private final CqlSession cqlSession;

    private Migration(CqlSession cqlSession,
                      String cqlDir,
                      String keyspace,
                      String migrationTable,
                      boolean refreshKeyspace) {
        this.cqlSession = cqlSession;
        this.cqlDir = cqlDir;
        this.keyspace = keyspace;
        this.migrationTable = migrationTable;
        this.refreshKeyspace = refreshKeyspace;
    }

    private void execute() {
        try {
            var cqlFiles = cqlFilesFromCqlDir();
            if (cqlFiles.isEmpty()) {
                return;
            }
            prepareKeyspace();
            filterCqlFilesToMigrate(cqlFiles).forEach(this::executeCqlFile);
        } finally {
            cqlSession.close();
        }
    }

    private void prepareKeyspace() {
        boolean keyspaceExists = doesKeyspaceExist();
        boolean createKeyspace = !keyspaceExists;
        if (keyspaceExists && refreshKeyspace) {
            log.info("dropping existing keyspace " + keyspace);
            executeStatement(dropKeyspace(keyspace));
            createKeyspace = true;
        }
        if (createKeyspace) {
            log.info("creating keyspace " + keyspace);
            executeStatement(createLocalKeyspace(keyspace));
            executeStatement(createMigrationTable(keyspace, migrationTable));
        }
    }

    private boolean doesKeyspaceExist() {
        return executeStatement(keyspaceExists(keyspace)).all().size() > 0;
    }

    private ResultSet executeStatement(String cql) {
        return executeStatement(newInstance(cql));
    }

    private ResultSet executeStatement(SimpleStatement statement) {
        ResultSet result = cqlSession.execute(statement.setTimeout(Duration.of(10, ChronoUnit.SECONDS)));
        if (!result.wasApplied()) {
            throw new IllegalStateException("unable to execute statement " + statement.getQuery());
        }
        return result;
    }

    private List<CqlFile> cqlFilesFromCqlDir() {
        File[] values = new File(cqlDir).listFiles();
        if (values == null || values.length == 0) {
            log.info("no cql files to migrate in dir " + cqlDir);
            return Collections.emptyList();
        }
        return Stream.of(values)
                .filter(file -> file.getName().matches(CQL_PATTERN))
                .map(CqlFile::new)
                .sorted(Comparator.comparing(CqlFile::getName))
                .toList();
    }

    private List<CqlFile> filterCqlFilesToMigrate(List<CqlFile> cqlFiles) {
        int completed = getLastCompletedVersion();
        var cqlFilesToMigrate = cqlFiles.stream()
                .filter(file -> file.getVersion() > completed)
                .toList();
        if (cqlFilesToMigrate.isEmpty()) {
            log.info("no new cql files to migrate");
        } else if (cqlFilesToMigrate.size() == 1) {
            log.info("new cql file to migrate: " + cqlFilesToMigrate.get(0));
        } else {
            log.info("new cql files to migrate: " + cqlFilesToMigrate.stream()
                    .map(Object::toString)
                    .collect(Collectors.joining(", ")));
        }
        return cqlFilesToMigrate;
    }

    private int getLastCompletedVersion() {
        List<Integer> completedVersions = executeStatement(migratedCql(keyspace, migrationTable)).all().stream()
                .map(row -> row.getInt("ver"))
                .sorted()
                .toList();
        return completedVersions.isEmpty() ? 0 : completedVersions.get(completedVersions.size() - 1);
    }

    private void executeCqlFile(CqlFile file) {
        log.info("executing cql file " + file.getName());
        for (String cqlLine : file.cqlLines()) {
            try {
                log.fine("executing cql statement (from %s):\n\n%s\n".formatted(file.getName(), cqlLine));
                executeStatement(cqlLine);
            } catch (Exception e) {
                throw new IllegalStateException("failed executing cql file " + file.getName(), e);
            }
        }
        cqlSession.execute(QueryBuilder.insertInto(keyspace, migrationTable)
                .value("id", literal(Uuids.timeBased()))
                .value("ver", literal(file.getVersion()))
                .value("name", literal(file.getName()))
                .build());
    }
}
