# CQL reference

Lexer, parser and AST for [CQL](https://cassandra.apache.org/doc/latest/cassandra/developing/cql/changes.html).

## Not supported

- CQLSH commands like CAPTURE, CLEAR, CONSISTENCY, COPY, DESCRIBE, EXIT, LOGIN, PAGING, SERIAL CONSISTENCY, SHOW, SOURCE and TRACING
- [Custom types](https://cassandra.apache.org/doc/stable/cassandra/cql/types.html#custom-types)

## todos

- error reporting
  - miette?
- ast
  - expressions as ast nodes
    - maths
    - function calls
    - literals
      - strings
      - maps
      - booleans
      - lists/sets
- visitor and transform apis
- lex <=> parse gotchas
  - ValuesKeyword and values fn
  - TtlKeyword and ttl fn
- todo
  - https://issues.apache.org/jira/browse/CASSANDRA-18504
  - https://docs.datastax.com/en/cql-oss/3.3/cql/cql_reference/valid_literal_r.html
  - https://cassandra.apache.org/doc/stable/cassandra/cql/types.html#frozen
  - https://cassandra.apache.org/doc/stable/cassandra/cql/types.html#dates
  - https://cassandra.apache.org/doc/latest/cassandra/developing/cql/functions.html
