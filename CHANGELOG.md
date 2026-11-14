# Changelog

## Unreleased

Sockets registered with `registerWebsocket` are now pumped on the page's main thread. `ws-worker.js`, the
`wsWorkerUrl` option and the `@n6k.io/db/workers` export are removed; drop them from serving and bundling.

Reads, writes, DDL and transactions on an attached database now travel on links, crossing's far transactions, and
the server runs them with crossing's far side. This is protocol version 13: it removes the `READ_PLAN`, `WRITE_PLAN`,
`TXN_*`, `CREATE_TABLE`, `ALTER_TABLE`, `CREATE_VIEW` and `DROP_VIEW` ops and every `txn` field, and adds the `LINK_*`
and `STREAM_*` frames. A client and server of different versions do not connect.

`RETURNING`, `DROP TABLE`, `ALTER COLUMN TYPE`, `ALTER TABLE RENAME TO`, and `CREATE TABLE` with a default, `CHECK` or
generated column now work on an attached database, as does writing to a table with no primary key. Writes in
autocommit run in a server transaction, so a statement that fails after its write leaves nothing behind.

`n6k_serve_*` takes `max_links`: far transactions open at once on a connection, each on its own thread.

The recursive part of a `WITH RECURSIVE` can no longer read an attached table; it fails with an error.

`CREATE SCHEMA` on an attached database now creates the schema on the server. It previously reported success but only
changed that client's local catalog, so a following `CREATE TABLE` in it failed with a permission error. This adds the
`CREATE_SCHEMA` op and the `schema_ddl` capability; against a server without them, `CREATE SCHEMA` fails with
`BinderException`. `CREATE SCHEMA` in a transaction, nested schemas and `CREATE OR REPLACE SCHEMA` also fail with
`BinderException`.

A client function the server lacks now runs on the client instead of failing or running the server's function of
that name. This adds the `HAS_FUNCTION` op and the `has_function` capability; against a server without them, named
function calls run on the client, with the same results but slower.

`CREATE TABLE` on an attached database now keeps `PRIMARY KEY`, `UNIQUE` and `NOT NULL`, and `DESCRIBE` shows them.
`CREATE TABLE`, `CREATE TABLE AS` and `ALTER TABLE` now join the open transaction, so `ROLLBACK` undoes them and other
clients do not see them before `COMMIT`.

Interrupting a query or write on an attached database now stops it on the server, and an interrupted write does not
commit.

## v0.2.0

Persistent `n6k` and `n6k_refresh` secrets created by earlier builds with a `SCOPE` no longer
match: recreate them. Secrets with no `SCOPE`, including the one `n6k_login` stores, still match.
