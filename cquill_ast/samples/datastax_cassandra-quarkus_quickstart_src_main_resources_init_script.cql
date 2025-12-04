CREATE KEYSPACE IF NOT EXISTS k1 WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};
CREATE TABLE IF NOT EXISTS k1.fruit(name text PRIMARY KEY, description text);