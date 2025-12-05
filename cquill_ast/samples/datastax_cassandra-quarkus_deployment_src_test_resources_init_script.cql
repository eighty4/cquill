CREATE KEYSPACE IF NOT EXISTS test_keyspace
    WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};