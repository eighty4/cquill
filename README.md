# cquill - a CQL migration helper

Configure the process with env vars:
- `CQUILL_MIGRATE_CQL_DIR` directory with CQL files
- `CQUILL_MIGRATE_KEYSPACE` keyspace to manage with migration process
- `CQUILL_MIGRATE_REFRESH` for development, specify `true` to drop and rebuild keyspace on migration
- `CQUILL_MIGRATE_TABLE` table that tracks migration history

CQL dir should contain cql files versioned with a naming pattern `V001__my-file.cql` and `V002__next-file.cql`

---

Running from Gradle
```
./gradlew run
```

Building a self-contained jar
```
./gradlew clean deployableJar
```

Building a docker image via Gradle
```
./gradlew clean buildDockerImage
```

Building a docker image via Gradle
```
./gradlew clean deployableJar
cd cquill-migrate
docker build -t cquill/migrate .
```

---

Features not (yet) supported:
- Rolling back a migration
- Customizing cql file versioning patterns
- Configuring Cassandra connection authentication
- Match and replace keyspace names for test keyspaces
- Tests, support community or proven CPU cycles via adoption
