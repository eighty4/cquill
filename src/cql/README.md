# CQL reference

Lexer, parser and AST for [CQL 3.0](https://cassandra.apache.org/doc/stable/cassandra/cql/index.html).

## Not supported

- CQLSH commands like CAPTURE, CLEAR, CONSISTENCY, COPY, DESCRIBE, EXIT, LOGIN, PAGING, SERIAL CONSISTENCY, SHOW, SOURCE and TRACING
- [Custom types](https://cassandra.apache.org/doc/stable/cassandra/cql/types.html#custom-types)

## todos

- lexer
  - maths
  - types
    - collections
      - table column definitions
      - insert collection constants
      - update set, +, -, etc. operations
    - datetime operations
    - strings
      - double-quoted case-sensitive identifiers
  - lex <=> parse gotchas
    - ValuesKeyword and values fn
    - TtlKeyword and ttl fn
  - error reporting
    - miette?
- parser
- ast
- https://issues.apache.org/jira/browse/CASSANDRA-18504
- https://docs.datastax.com/en/cql-oss/3.3/cql/cql_reference/valid_literal_r.html
