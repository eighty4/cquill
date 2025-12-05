CREATE KEYSPACE IF NOT EXISTS accord WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};

CREATE TABLE IF NOT EXISTS accord.cas_registers
(
    id int PRIMARY KEY,
    contents int
);

CREATE TABLE IF NOT EXISTS accord.rw_registers
(
    id int PRIMARY KEY,
    contents bigint
);

CREATE TABLE IF NOT EXISTS accord.list_append
(
    id int PRIMARY KEY,
    contents LIST<bigint>
);