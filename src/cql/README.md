# CQL reference

Lexer, parser and AST for [CQL 3.0](https://cassandra.apache.org/doc/stable/cassandra/cql/index.html).

## Not supported

- CQLSH commands like CAPTURE, CLEAR, CONSISTENCY, COPY, DESCRIBE, EXIT, LOGIN, PAGING, SERIAL CONSISTENCY, SHOW, SOURCE and TRACING
- [Custom types](https://cassandra.apache.org/doc/stable/cassandra/cql/types.html#custom-types)

## todos

- [json](https://cassandra.apache.org/doc/stable/cassandra/cql/json.html)
- [security](https://cassandra.apache.org/doc/stable/cassandra/cql/security.html)
- [triggers](https://cassandra.apache.org/doc/stable/cassandra/cql/triggers.html)
- comments
  - `--`
  - `//`
  - `/*    \n    */`
- types
  - collections
    - table column definitions
    - insert collection constants
    - update set, +, -, etc. operations
  - datetime operations
  - strings
    - double-quoted case-sensitive identifiers
    - using multiple `''` to escape a single quote in a string literal: `'tiffany's breakfast`
    - triple single quote `'''string literals with a ' without escaping'''`
- lex / parse gotchas
  - ValuesKeyword and values fn
  - TtlKeyword and ttl fn
- lexer error reporting
- parser
- ast
