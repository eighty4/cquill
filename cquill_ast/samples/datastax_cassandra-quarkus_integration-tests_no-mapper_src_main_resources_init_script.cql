CREATE KEYSPACE IF NOT EXISTS ks1
    WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};

CREATE TABLE IF NOT EXISTS ks1.product
(
    id   uuid PRIMARY KEY,
    name text
);
