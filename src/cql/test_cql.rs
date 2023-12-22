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

// https://cassandra.apache.org/doc/stable/cassandra/cql/indexes.html#create-index-statement

pub const CREATE_INDEX: &str = "\
create index big_data_index on big_data_table (text_column);
";

pub const CREATE_INDEX_IF_NOT_EXISTS: &str = "\
create index if not exists big_data_index on big_data_table (text_column);
";

pub const CREATE_INDEX_ON_KEYS: &str = "\
create index big_data_index on big_data_table (keys(map_column));
";

pub const CREATE_INDEX_ON_VALUES: &str = "\
create index big_data_index on big_data_table (values(map_column));
";

pub const CREATE_INDEX_ON_ENTRIES: &str = "\
create index big_data_index on big_data_table (entries(map_column));
";

pub const CREATE_INDEX_ON_FULL: &str = "\
create index big_data_index on big_data_table (full(map_column));
";

pub const CREATE_CUSTOM_INDEX: &str = "\
create custom index big_data_index on big_data_table (text_column) using 'fqpn.IndexClass';
";

pub const CREATE_CUSTOM_INDEX_WITH_OPTIONS: &str = "\
create custom index big_data_index on big_data_table (text_column)
    using 'fqpn.IndexClass'
    with options = {'option':'value'};
";

// https://cassandra.apache.org/doc/stable/cassandra/cql/indexes.html#drop-index-statement

pub const DROP_INDEX: &str = "\
drop index big_data_index;
";

pub const DROP_INDEX_IF_EXISTS: &str = "\
drop index if exists big_data_index;
";

// https://cassandra.apache.org/doc/stable/cassandra/cql/mvs.html#create-materialized-view-statement

pub const CREATE_MATERIALIZED_VIEW: &str = "\
create materialized view big_data_view as
    select * from big_data_table
    where int_column > 4
    primary key (int_column);
";

pub const CREATE_MATERIALIZED_VIEW_IF_NOT_EXISTS: &str = "\
create materialized view if not exists big_data_view as
    select * from big_data_table
    where int_column > 4
    primary key (int_column);
";

pub const CREATE_MATERIALIZED_VIEW_WITH_OPTIONS: &str = "\
create materialized view big_data_view as
    select * from big_data_table
    where int_column > 4
    primary key (int_column)
    with comment = 'comment ca va';
";

// https://cassandra.apache.org/doc/stable/cassandra/cql/mvs.html#alter-materialized-view-statement

pub const ALTER_MATERIALIZED_VIEW: &str = "\
alter materialized view big_data_view with comment = 'quoi de neuf';
";

pub const ALTER_MATERIALIZED_VIEW_IF_EXISTS: &str = "\
alter materialized view if exists big_data_view with comment = 'quoi de neuf';
";

// https://cassandra.apache.org/doc/stable/cassandra/cql/mvs.html#drop-materialized-view-statement
pub const DROP_MATERIALIZED_VIEW: &str = "\
drop materialized view big_data_view;
";

pub const DROP_MATERIALIZED_VIEW_IF_EXISTS: &str = "\
drop materialized view if exists big_data_view;
";

// https://cassandra.apache.org/doc/stable/cassandra/cql/functions.html#create-function-statement

pub const CREATE_FUNCTION_WITH_STRING_LITERAL: &str = "\
create function big_data_fn(fn_arg int)
    called on null input
    returns text
    language java
    as 'return fn_arg.toString();';
";

pub const CREATE_FUNCTION_CALLED_ON_NULL_INPUT: &str = "\
create function big_data_fn(fn_arg int)
    called on null input
    returns text
    language java
    as $$
        return fn_arg.toString();
    $$;
";

pub const CREATE_FUNCTION_RETURNS_NULL_ON_NULL_INPUT: &str = "\
create function big_data_fn (fn_arg int)
    returns null on null input
    returns text
    language java
    as $$
        return fn_arg.toString();
    $$;
";

pub const CREATE_OR_REPLACE_FUNCTION: &str = "\
create or replace function big_data_fn (fn_arg int)
    called on null input
    returns text
    language java
    as $$
        return fn_arg.toString();
    $$;
";

pub const CREATE_FUNCTION_IF_NOT_EXISTS: &str = "\
create function if not exists big_data_fn (fn_arg int)
    called on null input
    returns text
    language java
    as $$
        return fn_arg.toString();
    $$;
";

pub const CREATE_FUNCTION_WITH_MULTIPLE_ARGS: &str = "\
create function big_data_fn (fn_arg1 int, fn_arg2 text)
    called on null input
    returns text
    language java
    as $$
        return fn_arg1.toString();
    $$;
";

pub const CREATE_FUNCTION_WITH_FROZEN_ARG: &str = "\
create function big_data_fn (fn_arg frozen<some_udt>)
    called on null input
    returns text
    language java
    as $$
        return fn_arg.toString();
    $$;
";

pub const CREATE_FUNCTION_RETURNS_USER_DEFINED_TYPE: &str = "\
create function big_data_fn (fn_arg int)
    called on null input
    returns some_udt
    language java
    as $$
        return fn_arg.toString();
    $$;
";

// https://cassandra.apache.org/doc/stable/cassandra/cql/functions.html#drop-function-statement

pub const DROP_FUNCTION_WITHOUT_ARGS: &str = "\
drop function big_data_fn;
";

pub const DROP_FUNCTION_WITH_EXPLICIT_KEYSPACE: &str = "\
drop function big_data_keyspace.big_data_fn;
";

pub const DROP_FUNCTION_WITH_SINGLE_ARG: &str = "\
drop function big_data_fn(int);
";

pub const DROP_FUNCTION_WITH_MULTIPLE_ARGS: &str = "\
drop function big_data_fn(int, text);
";

pub const DROP_FUNCTION_WITH_EXPLICIT_KEYSPACE_AND_MULTIPLE_ARGS: &str = "\
drop function big_data_keyspace.big_data_fn(int, text);
";

pub const DROP_FUNCTION_IF_EXISTS: &str = "\
drop function if exists big_data_fn;
";

// https://cassandra.apache.org/doc/stable/cassandra/cql/functions.html#create-aggregate-statement

pub const CREATE_AGGREGATE_WITH_SINGLE_ARG: &str = "\
create aggregate big_data_agg (int) sfunc fn_name stype list;
";

pub const CREATE_AGGREGATE_WITH_MULTIPLE_ARGS: &str = "\
create aggregate big_data_agg (int, double) sfunc fn_name stype list;
";

pub const CREATE_AGGREGATE_WITH_UDT_STYPE: &str = "\
create aggregate big_data_agg (int) sfunc fn_name stype some_udt;
";

pub const CREATE_OR_REPLACE_AGGREGATE: &str = "\
create or replace aggregate big_data_agg (int) sfunc fn_name stype list;
";

pub const CREATE_AGGREGATE_IF_NOT_EXISTS: &str = "\
create aggregate if not exists big_data_agg (int) sfunc fn_name stype list;
";

pub const CREATE_AGGREGATE_WITH_FINALFUNC: &str = "\
create aggregate big_data_agg (int) sfunc fn_name stype list finalfunc ffn_name;
";

pub const CREATE_AGGREGATE_WITH_INITCOND: &str = "\
create aggregate big_data_agg (int) sfunc fn_name stype list initcond 'state value';
";

pub const CREATE_AGGREGATE_WITH_FINALFUNC_AND_INITCOND: &str = "\
create aggregate big_data_agg (int) sfunc fn_name stype list finalfunc ffn_name initcond 'state value';
";

// https://cassandra.apache.org/doc/stable/cassandra/cql/functions.html#drop-aggregate-statement

pub const DROP_AGGREGATE_WITHOUT_ARGS: &str = "\
drop aggregate big_data_agg;
";

pub const DROP_AGGREGATE_WITH_EXPLICIT_KEYSPACE: &str = "\
drop aggregate big_data_keyspace.big_data_agg;
";

pub const DROP_AGGREGATE_WITH_SINGLE_ARG: &str = "\
drop aggregate big_data_agg(int);
";

pub const DROP_AGGREGATE_WITH_MULTIPLE_ARGS: &str = "\
drop aggregate big_data_agg(int, text);
";

pub const DROP_AGGREGATE_WITH_EXPLICIT_KEYSPACE_AND_MULTIPLE_ARGS: &str = "\
drop aggregate big_data_keyspace.big_data_agg(int, text);
";

pub const DROP_AGGREGATE_IF_EXISTS: &str = "\
drop aggregate if exists big_data_agg;
";

// https://cassandra.apache.org/doc/stable/cassandra/cql/types.html#creating-a-udt

pub const CREATE_UDT_WITH_SINGLE_ATTRIBUTE: &str = "\
create type big_data_udt (int_attribute int);
";

pub const CREATE_UDT_WITH_MULTIPLE_ATTRIBUTES: &str = "\
create type big_data_udt (int_attr int, text_attr text);
";

pub const CREATE_UDT_IF_NOT_EXISTS: &str = "\
create type if not exists big_data_udt (int_attr int);
";

// https://cassandra.apache.org/doc/stable/cassandra/cql/types.html#udt-literals

pub const INSERT_UDT_LITERAL: &str = "\
insert into big_data_table (big_data_udt_column) values ({int_attr: 1, text_attr: 'big data!'});
";

// https://cassandra.apache.org/doc/stable/cassandra/cql/types.html#altering-a-udt

pub const ALTER_UDT_ADD_FIELD: &str = "\
alter type big_data_udt add big_data_text_column text;
";

pub const ALTER_UDT_IF_EXISTS: &str = "\
alter type if exists big_data_udt add big_data_text_column text;
";

pub const ALTER_UDT_ADD_FIELD_IF_NOT_EXISTS: &str = "\
alter type big_data_udt add if not exists big_data_text_column text;
";

pub const ALTER_UDT_RENAME_FIELD: &str = "\
alter type big_data_udt rename big_data_text_column to modest_data_text_column;
";

pub const ALTER_UDT_RENAME_MULTIPLE_FIELDS: &str = "\
alter type big_data_udt rename big_data_text_column to modest_data_text_column and big_data_int_column to gargantuan_int_column;
";

pub const ALTER_UDT_RENAME_FIELD_IF_EXISTS: &str = "\
alter type big_data_udt rename if exists big_data_text_column to modest_data_text_column;
";

// https://cassandra.apache.org/doc/stable/cassandra/cql/types.html#dropping-a-udt

pub const DROP_UDT: &str = "\
drop type big_data_udt;
";

pub const DROP_UDT_IF_EXISTS: &str = "\
drop type if exists big_data_udt;
";

// https://cassandra.apache.org/doc/stable/cassandra/cql/security.html#create-role-statement

pub const CREATE_ROLE_WITH_PASSWORD: &str = "\
create role big_data_role with password = 'asdf';
";

pub const CREATE_ROLE_WITH_HASHED_PASSWORD: &str = "\
create role big_data_role with hashed password = 'aassddff';
";

pub const CREATE_ROLE_WITH_LOGIN_TRUE: &str = "\
create role big_data_role with login = true;
";

pub const CREATE_ROLE_WITH_LOGIN_FALSE: &str = "\
create role big_data_role with login = false;
";

pub const CREATE_ROLE_WITH_SUPERUSER_TRUE: &str = "\
create role big_data_role with superuser = true;
";

pub const CREATE_ROLE_WITH_SUPERUSER_FALSE: &str = "\
create role big_data_role with superuser = false;
";

pub const CREATE_ROLE_WITH_OPTIONS_MAP: &str = "\
create role big_data_role with options = { 'opt1': 'val', 'opt2': 99};
";

pub const CREATE_ROLE_WITH_ACCESS_TO_DATACENTERS_SET: &str = "\
create role big_data_role with access to datacenters {'dc1', 'dc2'};
";

pub const CREATE_ROLE_WITH_ACCESS_TO_ALL_DATACENTERS: &str = "\
create role big_data_role with access to all datacenters;
";

pub const CREATE_ROLE_WITH_MULTIPLE_ROLE_OPTIONS: &str = "\
create role big_data_role with password = 'asdf' and login = true;
";

pub const CREATE_ROLE_IF_NOT_EXISTS: &str = "\
create role if not exists big_data_role with password = 'asdf';
";

// https://cassandra.apache.org/doc/stable/cassandra/cql/security.html#alter-role-statement

pub const ALTER_ROLE_WITH_PASSWORD: &str = "\
alter role big_data_role with password = 'asdf';
";

pub const ALTER_ROLE_WITH_HASHED_PASSWORD: &str = "\
alter role big_data_role with hashed password = 'aassddff';
";

pub const ALTER_ROLE_WITH_LOGIN_TRUE: &str = "\
alter role big_data_role with login = true;
";

pub const ALTER_ROLE_WITH_LOGIN_FALSE: &str = "\
alter role big_data_role with login = false;
";

pub const ALTER_ROLE_WITH_SUPERUSER_TRUE: &str = "\
alter role big_data_role with superuser = true;
";

pub const ALTER_ROLE_WITH_SUPERUSER_FALSE: &str = "\
alter role big_data_role with superuser = false;
";

pub const ALTER_ROLE_WITH_OPTIONS_MAP: &str = "\
alter role big_data_role with options = { 'opt1': 'val', 'opt2': 99};
";

pub const ALTER_ROLE_WITH_ACCESS_TO_DATACENTERS_SET: &str = "\
alter role big_data_role with access to datacenters {'dc1', 'dc2'};
";

pub const ALTER_ROLE_WITH_ACCESS_TO_ALL_DATACENTERS: &str = "\
alter role big_data_role with access to all datacenters;
";

pub const ALTER_ROLE_WITH_MULTIPLE_ROLE_OPTIONS: &str = "\
alter role big_data_role with password = 'asdf' and login = true;
";

pub const ALTER_ROLE_IF_EXISTS: &str = "\
alter role if exists big_data_role with password = 'asdf';
";

// https://cassandra.apache.org/doc/stable/cassandra/cql/security.html#drop-role-statement

pub const DROP_ROLE: &str = "\
drop role big_data_role;
";

pub const DROP_ROLE_IF_EXISTS: &str = "\
drop role if exists big_data_role;
";

// https://cassandra.apache.org/doc/stable/cassandra/cql/security.html#grant-role-statement

pub const GRANT_ROLE: &str = "\
grant role big_data_role to other_big_data_role;
";

// https://cassandra.apache.org/doc/stable/cassandra/cql/security.html#revoke-role-statement

pub const REVOKE_ROLE: &str = "\
revoke role big_data_role from other_big_data_role;
";

// https://cassandra.apache.org/doc/stable/cassandra/cql/security.html#list-roles-statement

pub const LIST_ROLES: &str = "\
list roles;
";

pub const LIST_ROLES_NOT_RECURSIVELY: &str = "\
list roles norecursive;
";

pub const LIST_ROLES_OF_ROLE: &str = "\
list roles of big_data_role;
";

pub const LIST_ROLES_OF_ROLE_NOT_RECURSIVELY: &str = "\
list roles of big_data_role norecursive;
";

// https://cassandra.apache.org/doc/stable/cassandra/cql/security.html#create-user-statement

pub const CREATE_USER: &str = "\
create user big_data_user;
";

pub const CREATE_USER_IF_NOT_EXISTS: &str = "\
create user if not exists big_data_user;
";

pub const CREATE_USER_SUPERUSER: &str = "\
create user big_data_user superuser;
";

pub const CREATE_USER_NOT_SUPERUSER: &str = "\
create user big_data_user nosuperuser;
";

pub const CREATE_USER_WITH_PASSWORD: &str = "\
create user big_data_user with password 'asdf';
";

pub const CREATE_USER_WITH_PASSWORD_SUPERUSER: &str = "\
create user big_data_user with password 'asdf' superuser;
";

pub const CREATE_USER_WITH_PASSWORD_NOT_SUPERUSER: &str = "\
create user big_data_user with password 'asdf' nosuperuser;
";

pub const CREATE_USER_WITH_HASHED_PASSWORD: &str = "\
create user big_data_user with hashed password 'aassddff';
";

pub const CREATE_USER_WITH_HASHED_PASSWORD_SUPERUSER: &str = "\
create user big_data_user with hashed password 'aassddff' superuser;
";

pub const CREATE_USER_WITH_HASHED_PASSWORD_NOT_SUPERUSER: &str = "\
create user big_data_user with hashed password 'aassddff' nosuperuser;
";

// https://cassandra.apache.org/doc/stable/cassandra/cql/security.html#alter-user-statement

pub const ALTER_USER: &str = "\
alter user big_data_user;
";

pub const ALTER_USER_IF_EXISTS: &str = "\
alter user if exists big_data_user;
";

pub const ALTER_USER_SUPERUSER: &str = "\
alter user big_data_user superuser;
";

pub const ALTER_USER_NOT_SUPERUSER: &str = "\
alter user big_data_user nosuperuser;
";

pub const ALTER_USER_WITH_PASSWORD: &str = "\
alter user big_data_user with password 'asdf';
";

pub const ALTER_USER_WITH_PASSWORD_SUPERUSER: &str = "\
alter user big_data_user with password 'asdf' superuser;
";

pub const ALTER_USER_WITH_PASSWORD_NOT_SUPERUSER: &str = "\
alter user big_data_user with password 'asdf' nosuperuser;
";

pub const ALTER_USER_WITH_HASHED_PASSWORD: &str = "\
alter user big_data_user with hashed password 'aassddff';
";

pub const ALTER_USER_WITH_HASHED_PASSWORD_SUPERUSER: &str = "\
alter user big_data_user with hashed password 'aassddff' superuser;
";

pub const ALTER_USER_WITH_HASHED_PASSWORD_NOT_SUPERUSER: &str = "\
alter user big_data_user with hashed password 'aassddff' nosuperuser;
";

// https://cassandra.apache.org/doc/stable/cassandra/cql/security.html#drop-user-statement

pub const DROP_USER: &str = "\
drop user big_data_user;
";

pub const DROP_USER_IF_EXISTS: &str = "\
drop user if exists big_data_user;
";

// https://cassandra.apache.org/doc/stable/cassandra/cql/security.html#list-users-statement

pub const LIST_USERS: &str = "\
list users;
";

// https://cassandra.apache.org/doc/stable/cassandra/cql/security.html#grant-permission-statement

pub const GRANT_ALL_PERMISSIONS_ON_ALL_KEYSPACES: &str = "\
grant all permissions on all keyspaces to big_data_user;
";

pub const GRANT_ALL_ON_ALL_KEYSPACES: &str = "\
grant all on all keyspaces to big_data_user;
";

pub const GRANT_ALL_ON_KEYSPACE: &str = "\
grant all on keyspace big_data_keyspace to big_data_user;
";

pub const GRANT_ALL_ON_TABLE_IMPLICITLY: &str = "\
grant all on big_data_table to big_data_user;
";

pub const GRANT_ALL_ON_TABLE_WITH_EXPLICIT_KEYSPACE_IMPLICITLY: &str = "\
grant all on big_data_keyspace.big_data_table to big_data_user;
";

pub const GRANT_ALL_ON_TABLE_EXPLICITLY: &str = "\
grant all on table big_data_table to big_data_user;
";

pub const GRANT_ALL_ON_TABLE_WITH_EXPLICIT_KEYSPACE_EXPLICITLY: &str = "\
grant all on table big_data_keyspace.big_data_table to big_data_user;
";

pub const GRANT_ALL_ON_ALL_ROLES: &str = "\
grant all on all roles to big_data_user;
";

pub const GRANT_ALL_ON_ROLE: &str = "\
grant all on role big_data_role to big_data_user;
";

pub const GRANT_ALL_ON_ALL_FUNCTIONS: &str = "\
grant all on all functions to big_data_user;
";

pub const GRANT_ALL_ON_FUNCTION: &str = "\
grant all on function big_data_function(int) to big_data_user;
";

pub const GRANT_ALL_ON_FUNCTION_WITH_EXPLICIT_KEYSPACE: &str = "\
grant all on function big_data_keyspace.big_data_function(int) to big_data_user;
";

pub const GRANT_ALL_ON_ALL_MBEANS: &str = "\
grant all on all mbeans to big_data_user;
";

pub const GRANT_ALL_ON_MBEANS: &str = "\
grant all on mbeans big_data_mbean to big_data_user;
";

pub const GRANT_ALL_ON_MBEAN: &str = "\
grant all on mbean big_data_mbean to big_data_user;
";

pub const GRANT_CREATE_PERMISSION_ON_ALL_KEYSPACES: &str = "\
grant create permission on all keyspaces to big_data_user;
";

pub const GRANT_CREATE_ON_ALL_KEYSPACES: &str = "\
grant create on all keyspaces to big_data_user;
";

pub const GRANT_ALTER_ON_ALL_KEYSPACES: &str = "\
grant alter on all keyspaces to big_data_user;
";

pub const GRANT_DROP_ON_ALL_KEYSPACES: &str = "\
grant drop on all keyspaces to big_data_user;
";

pub const GRANT_SELECT_ON_ALL_KEYSPACES: &str = "\
grant select on all keyspaces to big_data_user;
";

pub const GRANT_MODIFY_ON_ALL_KEYSPACES: &str = "\
grant modify on all keyspaces to big_data_user;
";

pub const GRANT_AUTHORIZE_ON_ALL_KEYSPACES: &str = "\
grant authorize on all keyspaces to big_data_user;
";

pub const GRANT_DESCRIBE_ON_ALL_KEYSPACES: &str = "\
grant describe on all keyspaces to big_data_user;
";

pub const GRANT_EXECUTE_ON_ALL_KEYSPACES: &str = "\
grant execute on all keyspaces to big_data_user;
";

// https://cassandra.apache.org/doc/stable/cassandra/cql/security.html#revoke-permission-statement

pub const REVOKE_ALL_PERMISSIONS_ON_ALL_KEYSPACES: &str = "\
revoke all permissions on all keyspaces from big_data_user;
";

pub const REVOKE_ALL_ON_ALL_KEYSPACES: &str = "\
revoke all on all keyspaces from big_data_user;
";

pub const REVOKE_ALL_ON_KEYSPACE: &str = "\
revoke all on keyspace big_data_keyspace from big_data_user;
";

pub const REVOKE_ALL_ON_TABLE_IMPLICITLY: &str = "\
revoke all on big_data_table from big_data_user;
";

pub const REVOKE_ALL_ON_TABLE_WITH_EXPLICIT_KEYSPACE_IMPLICITLY: &str = "\
revoke all on big_data_keyspace.big_data_table from big_data_user;
";

pub const REVOKE_ALL_ON_TABLE_EXPLICITLY: &str = "\
revoke all on table big_data_table from big_data_user;
";

pub const REVOKE_ALL_ON_TABLE_WITH_EXPLICIT_KEYSPACE_EXPLICITLY: &str = "\
revoke all on table big_data_keyspace.big_data_table from big_data_user;
";

pub const REVOKE_ALL_ON_ALL_ROLES: &str = "\
revoke all on all roles from big_data_user;
";

pub const REVOKE_ALL_ON_ROLE: &str = "\
revoke all on role big_data_role from big_data_user;
";

pub const REVOKE_ALL_ON_ALL_FUNCTIONS: &str = "\
revoke all on all functions from big_data_user;
";

pub const REVOKE_ALL_ON_FUNCTION: &str = "\
revoke all on function big_data_function(int) from big_data_user;
";

pub const REVOKE_ALL_ON_FUNCTION_WITH_EXPLICIT_KEYSPACE: &str = "\
revoke all on function big_data_keyspace.big_data_function(int) from big_data_user;
";

pub const REVOKE_ALL_ON_ALL_MBEANS: &str = "\
revoke all on all mbeans from big_data_user;
";

pub const REVOKE_ALL_ON_MBEANS: &str = "\
revoke all on mbeans big_data_mbean from big_data_user;
";

pub const REVOKE_ALL_ON_MBEAN: &str = "\
revoke all on mbean big_data_mbean from big_data_user;
";

pub const REVOKE_CREATE_PERMISSION_ON_ALL_KEYSPACES: &str = "\
revoke create permission on all keyspaces from big_data_user;
";

pub const REVOKE_CREATE_ON_ALL_KEYSPACES: &str = "\
revoke create on all keyspaces from big_data_user;
";

pub const REVOKE_ALTER_ON_ALL_KEYSPACES: &str = "\
revoke alter on all keyspaces from big_data_user;
";

pub const REVOKE_DROP_ON_ALL_KEYSPACES: &str = "\
revoke drop on all keyspaces from big_data_user;
";

pub const REVOKE_SELECT_ON_ALL_KEYSPACES: &str = "\
revoke select on all keyspaces from big_data_user;
";

pub const REVOKE_MODIFY_ON_ALL_KEYSPACES: &str = "\
revoke modify on all keyspaces from big_data_user;
";

pub const REVOKE_AUTHORIZE_ON_ALL_KEYSPACES: &str = "\
revoke authorize on all keyspaces from big_data_user;
";

pub const REVOKE_DESCRIBE_ON_ALL_KEYSPACES: &str = "\
revoke describe on all keyspaces from big_data_user;
";

pub const REVOKE_EXECUTE_ON_ALL_KEYSPACES: &str = "\
revoke execute on all keyspaces from big_data_user;
";

// https://cassandra.apache.org/doc/stable/cassandra/cql/security.html#list-permissions-statement

pub const LIST_ALL: &str = "\
list all;
";

pub const LIST_ALL_PERMISSIONS: &str = "\
list all permissions;
";

pub const LIST_ALL_PERMISSIONS_OF_USER: &str = "\
list all permissions of big_data_user;
";

pub const LIST_ALL_PERMISSIONS_OF_USER_NOT_RECURSIVELY: &str = "\
list all permissions of big_data_user norecursive;
";

pub const LIST_ALL_PERMISSIONS_ON_ALL_KEYSPACES: &str = "\
list all permissions on all keyspaces of big_data_user;
";

pub const LIST_ALL_ON_ALL_KEYSPACES: &str = "\
list all on all keyspaces of big_data_user;
";

pub const LIST_ALL_ON_KEYSPACE: &str = "\
list all on keyspace big_data_keyspace of big_data_user;
";

pub const LIST_ALL_ON_TABLE_IMPLICITLY: &str = "\
list all on big_data_table of big_data_user;
";

pub const LIST_ALL_ON_TABLE_WITH_EXPLICIT_KEYSPACE_IMPLICITLY: &str = "\
list all on big_data_keyspace.big_data_table of big_data_user;
";

pub const LIST_ALL_ON_TABLE_EXPLICITLY: &str = "\
list all on table big_data_table of big_data_user;
";

pub const LIST_ALL_ON_TABLE_WITH_EXPLICIT_KEYSPACE_EXPLICITLY: &str = "\
list all on table big_data_keyspace.big_data_table of big_data_user;
";

pub const LIST_ALL_ON_ALL_ROLES: &str = "\
list all on all roles of big_data_user;
";

pub const LIST_ALL_ON_ROLE: &str = "\
list all on role big_data_role of big_data_user;
";

pub const LIST_ALL_ON_ALL_FUNCTIONS: &str = "\
list all on all functions of big_data_user;
";

pub const LIST_ALL_ON_FUNCTION: &str = "\
list all on function big_data_function(int) of big_data_user;
";

pub const LIST_ALL_ON_FUNCTION_WITH_EXPLICIT_KEYSPACE: &str = "\
list all on function big_data_keyspace.big_data_function(int) of big_data_user;
";

pub const LIST_ALL_ON_ALL_MBEANS: &str = "\
list all on all mbeans of big_data_user;
";

pub const LIST_ALL_ON_MBEANS: &str = "\
list all on mbeans big_data_mbean of big_data_user;
";

pub const LIST_ALL_ON_MBEAN: &str = "\
list all on mbean big_data_mbean of big_data_user;
";

pub const LIST_CREATE_PERMISSION_ON_ALL_KEYSPACES: &str = "\
list create permission on all keyspaces of big_data_user;
";

pub const LIST_CREATE_ON_ALL_KEYSPACES: &str = "\
list create on all keyspaces of big_data_user;
";

pub const LIST_ALTER_ON_ALL_KEYSPACES: &str = "\
list alter on all keyspaces of big_data_user;
";

pub const LIST_DROP_ON_ALL_KEYSPACES: &str = "\
list drop on all keyspaces of big_data_user;
";

pub const LIST_SELECT_ON_ALL_KEYSPACES: &str = "\
list select on all keyspaces of big_data_user;
";

pub const LIST_MODIFY_ON_ALL_KEYSPACES: &str = "\
list modify on all keyspaces of big_data_user;
";

pub const LIST_AUTHORIZE_ON_ALL_KEYSPACES: &str = "\
list authorize on all keyspaces of big_data_user;
";

pub const LIST_DESCRIBE_ON_ALL_KEYSPACES: &str = "\
list describe on all keyspaces of big_data_user;
";

pub const LIST_EXECUTE_ON_ALL_KEYSPACES: &str = "\
list execute on all keyspaces of big_data_user;
";

// https://cassandra.apache.org/doc/stable/cassandra/cql/triggers.html#create-trigger-statement

pub const CREATE_TRIGGER: &str = "\
create trigger big_data_trigger on big_data_table using 'trigger name';
";

pub const CREATE_TRIGGER_IF_NOT_EXISTS: &str = "\
create trigger if not exists big_data_trigger on big_data_table using 'trigger name';
";

// https://cassandra.apache.org/doc/stable/cassandra/cql/triggers.html#drop-trigger-statement

pub const DROP_TRIGGER: &str = "\
drop trigger big_data_trigger on big_data_table;
";

pub const DROP_TRIGGER_IF_EXISTS: &str = "\
drop trigger if exists big_data_trigger on big_data_table;
";
