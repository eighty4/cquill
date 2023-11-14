// https://cassandra.apache.org/doc/stable/cassandra/cql/ddl.html#create-keyspace-statement

pub const CREATE_KEYSPACE: &str = "\
create keyspace big_data_keyspace with replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
";

pub const CREATE_KEYSPACE_IF_NOT_EXISTS: &str = "\
create keyspace if not exists big_data_keyspace with replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
";

pub const CREATE_KEYSPACE_WITH_DURABLE_WRITES_OPTION_EXISTS: &str = "\
create keyspace big_data_keyspace with replication = {'class': 'SimpleStrategy', 'replication_factor': 1} and durable_writes = false;
";

// https://cassandra.apache.org/doc/stable/cassandra/cql/ddl.html#use-statement

pub const USE_KEYSPACE: &str = "\
use big_data_keyspace;
";

// https://cassandra.apache.org/doc/stable/cassandra/cql/ddl.html#alter-keyspace-statement

pub const ALTER_KEYSPACE_WITH_DURABLE_WRITES: &str = "\
alter keyspace big_data_keyspace with durable_writes = false;
";

pub const ALTER_KEYSPACE_WITH_REPLICATION: &str = "\
alter keyspace big_data_keyspace with replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
";

pub const ALTER_KEYSPACE_IF_EXISTS: &str = "\
alter keyspace if exists big_data_keyspace with durable_writes = false;
";

// https://cassandra.apache.org/doc/stable/cassandra/cql/ddl.html#drop-keyspace-statement

pub const DROP_KEYSPACE: &str = "\
drop keyspace big_data_keyspace;
";

pub const DROP_KEYSPACE_IF_EXISTS: &str = "\
drop keyspace if exists big_data_keyspace;
";

// https://cassandra.apache.org/doc/stable/cassandra/cql/ddl.html#create-table-statement

pub const CREATE_TABLE_WITH_ALL_DATA_TYPES: &str = "\
create table big_data_table
(
    ascii_column     ascii,
    bigint_column    bigint,
    blob_column      blob,
    boolean_column   boolean,
    counter_column   counter,
    date_column      date,
    decimal_column   decimal,
    double_column    double,
    duration_column  duration,
    float_column     float,
    inet_column      inet,
    int_column       int,
    smallint_column  smallint,
    text_column      text,
    time_column      time,
    timestamp_column timestamp,
    timeuuid_column  timeuuid,
    tinyint_column   tinyint,
    uuid_column      uuid,
    varchar_column   varchar,
    varint_column    varint,
    primary key (text_column)
);
";

pub const CREATE_TABLE_IF_NOT_EXISTS: &str = "\
create table if not exists big_data_table (uuid_column uuid primary key);
";

pub const CREATE_TABLE_WITH_EXPLICIT_KEYSPACE: &str = "\
create table big_data_keyspace.big_data_table (uuid_column uuid primary key);
";

pub const CREATE_TABLE_WITH_COMPOSITE_PRIMARY_KEY: &str = "\
create table big_data_table (
    uuid_column uuid,
    timeuuid_column timeuuid,
    primary key (timeuuid_column, uuid_column)
);
";

pub const CREATE_TABLE_WITH_COMMENT: &str = "\
create table big_data_table (
    uuid_column uuid primary key
) with comment = 'big data!';
";

pub const CREATE_TABLE_WITH_COMPACT_STORAGE: &str = "\
create table big_data_table (
    uuid_column uuid primary key
) with compact storage;
";

pub const CREATE_TABLE_WITH_COMPACTION: &str = "\
create table big_data_table (
    uuid_column uuid primary key
) with compaction = {'class': 'LeveledCompactionStrategy'};
";

pub const CREATE_TABLE_WITH_ASC_CLUSTERING_ORDER: &str = "\
create table big_data_table (
    text_column text,
    uuid_column uuid,
    time_column timeuuid,
    primary key (text_column, time_column)
)  with clustering order by (time_column asc);
";

pub const CREATE_TABLE_WITH_DESC_CLUSTERING_ORDER: &str = "\
create table big_data_table (
    text_column text,
    uuid_column uuid,
    time_column timeuuid,
    primary key (text_column, time_column)
)  with clustering order by (time_column desc);
";

pub const CREATE_TABLE_WITH_IMPLICIT_DESC_CLUSTERING_ORDER: &str = "\
create table big_data_table (
    text_column text,
    uuid_column uuid,
    time_column timeuuid,
    primary key (text_column, time_column)
)  with clustering order by ();
";

// https://cassandra.apache.org/doc/stable/cassandra/cql/ddl.html#alter-table-statement

pub const ALTER_TABLE_IF_EXISTS: &str = "\
alter table if exists big_data_table add text_column text;
";

pub const ALTER_TABLE_ADD_COLUMN: &str = "\
alter table big_data_table add text_column text;
";

pub const ALTER_TABLE_ADD_MULTIPLE_COLUMNS: &str = "\
alter table big_data_table add text_column text, uuid_column uuid;
";

pub const ALTER_TABLE_ADD_COLUMN_IF_NOT_EXISTS: &str = "\
alter table big_data_table add if not exists text_column text;
";

pub const ALTER_TABLE_WITH_COMMENT: &str = "\
alter table big_data_table with comment = 'big data!';
";

pub const ALTER_TABLE_DROP_COLUMN: &str = "\
alter table big_data_table drop text_column;
";

pub const ALTER_TABLE_DROP_COLUMN_IF_EXISTS: &str = "\
alter table big_data_table drop if exists text_column;
";

pub const ALTER_TABLE_DROP_MULTIPLE_COLUMNS: &str = "\
alter table big_data_table drop text_column uuid_column;
";

pub const ALTER_TABLE_RENAME_COLUMN: &str = "\
alter table big_data_table rename text_column to text_col;
";

pub const ALTER_TABLE_RENAME_COLUMN_IF_EXISTS: &str = "\
alter table big_data_table rename if exists text_column to text_col;
";

pub const ALTER_TABLE_RENAME_MULTIPLE_COLUMNS: &str = "\
alter table big_data_table rename text_column to text_col and uuid_column to uuid_col;
";

// https://cassandra.apache.org/doc/stable/cassandra/cql/ddl.html#drop-table-statement

pub const DROP_TABLE: &str = "\
drop table big_data_table;
";

pub const DROP_TABLE_IF_EXISTS: &str = "\
drop table if exists big_data_table;
";

// https://cassandra.apache.org/doc/stable/cassandra/cql/ddl.html#truncate-statement

pub const TRUNCATE: &str = "\
truncate big_data_table;
";

pub const TRUNCATE_TABLE: &str = "\
truncate table big_data_table;
";

// https://cassandra.apache.org/doc/stable/cassandra/cql/dml.html#select-statement

pub const SELECT_EXPLICIT_COLUMNS: &str = "\
select text_column, uuid_column from big_data_table;
";

pub const SELECT_COLUMN_AS: &str = "\
select text_column as text_col from big_data_table;
";

pub const SELECT_COLUMN_CAST: &str = "\
select cast (uuid_column as text) from big_data_table;
";

pub const SELECT_COUNT: &str = "\
select count (*) from big_data_table;
";

pub const SELECT_JSON: &str = "\
select json * from big_data_table;
";

pub const SELECT_DISTINCT: &str = "\
select distinct * from big_data_table;
";

pub const SELECT_WHERE_EQ: &str = "\
select * from big_data_table where text_column = 'big data!';
";

pub const SELECT_WHERE_NEQ: &str = "\
select * from big_data_table where text_column != 'big data!';
";

pub const SELECT_WHERE_LT: &str = "\
select * from big_data_table where int_column < 3;
";

pub const SELECT_WHERE_GT: &str = "\
select * from big_data_table where int_column > 3;
";

pub const SELECT_WHERE_LTE: &str = "\
select * from big_data_table where int_column <= 3;
";

pub const SELECT_WHERE_GTE: &str = "\
select * from big_data_table where int_column >= 3;
";

pub const SELECT_WHERE_IN: &str = "\
select * from big_data_table where partition_col = 'big data!' and clustering_col in ('abc', 'def');
";

pub const SELECT_WHERE_IN_TUPLE: &str = "\
select * from big_data_table where partition_col = 'big data!'
    and (clustering_col1, clustering_col2) in (('abc', 123), ('def', 456));
";

pub const SELECT_WHERE_CONTAINS: &str = "\
select * from big_data_table where list_column contains 'big data!';
";

pub const SELECT_WHERE_CONTAINS_KEY: &str = "\
select * from big_data_table where map_column contains key 'big data!';
";

pub const SELECT_WHERE_AND_WHERE: &str = "\
select * from big_data_table where partition_col = 'big data!' and clustering_col = 'more data!';
";

pub const SELECT_GROUP_BY_COLUMN: &str = "\
select * from big_data_table group by text_column;
";

pub const SELECT_GROUP_BY_MULTIPLE_COLUMNS: &str = "\
select * from big_data_table group by text_column, uuid_column;
";

pub const SELECT_ORDER_BY_COLUMN: &str = "\
select * from big_data_table order by text_column asc;
";

pub const SELECT_ORDER_BY_MULTIPLE_COLUMNS: &str = "\
select * from big_data_table order by text_column asc, uuid_column desc;
";

pub const SELECT_PER_PARTITION_LIMIT: &str = "\
select * from big_data_table per partition limit 1;
";

pub const SELECT_LIMIT: &str = "\
select * from big_data_table limit 5;
";

pub const SELECT_ALLOW_FILTERING: &str = "\
select * from big_data_table allow filtering;
";

// https://cassandra.apache.org/doc/stable/cassandra/cql/dml.html#insert-statement

pub const INSERT_SINGLE_VALUE: &str = "\
insert into big_data_table (text_column) values ('big data!');
";

pub const INSERT_MULTIPLE_VALUES: &str = "\
insert into big_data_table (uuid_column, text_column) values (89b7aa7a-8776-460b-8e1a-60cb4bcd523c, 'big data!');
";

pub const INSERT_IF_NOT_EXISTS: &str = "\
insert into big_data_table (text_column) values ('big data!') if not exists;
";

pub const INSERT_USING_TTL: &str = "\
insert into big_data_table (text_column) values ('big data!') using ttl 86400;
";

pub const INSERT_USING_TIMESTAMP: &str = "\
insert into big_data_table (text_column) values ('big data!') using timestamp '2023-11-14T04:05+0000';
";

pub const INSERT_JSON: &str = "\
insert into big_data_table json '{\"text_column\": \"big data!\"}';
";

pub const INSERT_JSON_DEFAULT_NULL: &str = "\
insert into big_data_table json '{\"text_column\": \"big data!\"}' default null;
";

pub const INSERT_JSON_DEFAULT_UNSET: &str = "\
insert into big_data_table json '{\"text_column\": \"big data!\"}' default unset;
";

// https://cassandra.apache.org/doc/stable/cassandra/cql/dml.html#update-statement

pub const UPDATE_SINGLE_COLUMN: &str = "\
update big_data_table set int_column = 1 where text_column = 'big data!';
";

pub const UPDATE_MULTIPLE_COLUMNS: &str = "\
update big_data_table set int_column = 1, float_column = 1.1 where text_column = 'big data!';
";

pub const UPDATE_IF_EXISTS: &str = "\
update big_data_table set int_column = 1 where text_column = 'big data!' if exists;
";

pub const UPDATE_IF_CONDITION: &str = "\
update big_data_table set int_column = 1 where text_column = 'big data!' if int_column > 6;
";

pub const UPDATE_USING_TTL: &str = "\
update big_data_table using ttl 86400 set int_column = 1 where text_column = 'big data!';
";

pub const UPDATE_USING_TIMESTAMP: &str = "\
update big_data_table using timestamp '2023-11-14T04:05+0000' set int_column = 1 where text_column = 'big data!';
";

// https://cassandra.apache.org/doc/stable/cassandra/cql/dml.html#delete_statement

pub const DELETE_SINGLE_COLUMN: &str = "\
delete uuid_column from big_data_table where text_column = 'big data!';
";

pub const DELETE_MULTIPLE_COLUMNS: &str = "\
delete uuid_column, int_column from big_data_table where text_column = 'big data!';
";

pub const DELETE_IF_EXISTS: &str = "\
delete uuid_column from big_data_table where text_column = 'big data!' if exists;
";

pub const DELETE_IF_CONDITION: &str = "\
delete uuid_column from big_data_table where text_column = 'big data!' if uuid_column != 89b7aa7a-8776-460b-8e1a-60cb4bcd523c;
";

pub const DELETE_USING_TIMESTAMP: &str = "\
delete uuid_column from big_data_table using timestamp '2023-11-14T04:05+0000' where text_column = 'big data!';
";

// https://cassandra.apache.org/doc/stable/cassandra/cql/dml.html#batch_statement

pub const BATCH: &str = "\
begin batch
insert into big_data_table (text_col1) values ('big data!');
insert into big_data_table (text_col2) values ('more data!');
apply batch;
";

pub const BATCH_UNLOGGED: &str = "\
begin batch unlogged
insert into big_data_table (text_col1) values ('big data!');
insert into big_data_table (text_col2) values ('more data!');
apply batch;
";

pub const BATCH_COUNTER: &str = "\
begin batch counter
insert into big_data_table (text_col1) values ('big data!');
insert into big_data_table (text_col2) values ('more data!');
apply batch;
";

pub const BATCH_USING_TIMESTAMP: &str = "\
begin batch using timestamp '2023-11-14T04:05+0000'
insert into big_data_table (text_col1) values ('big data!');
insert into big_data_table (text_col2) values ('more data!');
apply batch;
";
