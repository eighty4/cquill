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
