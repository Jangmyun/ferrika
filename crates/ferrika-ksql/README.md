# ferrika-ksql

**Preview.** ksqlDB support of [Ferrika](https://github.com/Jangmyun/ferrika),
behind the `ferrika ksql` command. Ferrika does not embed or ship ksqlDB: it
talks to an existing ksqlDB server over its REST API. Its API and behaviour
may change in any release until it leaves preview.

- `client`: a client for the ksqlDB REST API over HTTP/1.1 (`http://` and
  `https://` with the OS trust store or a private CA, a client certificate for
  mutual TLS, basic authentication): server info, streams, tables, queries
  and topics, `DESCRIBE`, statements (`/ksql`) and pull and push queries
  (`/query-stream`) with a row limit, a time limit and closing the query on
  the server
- `statement`: a small lexer that strips comments, keeps string literals and
  quoted identifiers intact, splits a script into statements and classifies
  each one (read, session, or a write with the company-policy categories it
  needs); unknown statements need every ksqlDB category
- `config`: a ksqlDB connection, built by hand or from a Ferrika cluster
  profile (`ksql_url`, CA and client certificate paths)
- `view`: query rows with PII masking applied per column, for display

Ferrika's company policy and audit log are applied by the caller (the
`ferrika` command line or another front end) through `ferrika-governance`;
see `docs/plans/ksqldb-client.md`.

## Stability

Preview, pre-1.0: public items can change in any version.

## License

Apache-2.0
