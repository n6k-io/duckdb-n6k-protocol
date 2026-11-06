# Changelog

## Unreleased

`RETURNING` on an attached database now runs on the server. This adds the `write_returning` capability; against a
server without it, `RETURNING` fails with `BinderException`.

Writes in autocommit now run in a server transaction, so a statement that fails after its write leaves nothing behind.
Each such write costs two more round trips.

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
clients do not see them before `COMMIT`. This adds the `table_ddl` capability; against a server without it, a
`CREATE TABLE` with constraints or table DDL in a transaction fails with `BinderException`.

`CREATE TABLE` with a default, `CHECK`, foreign key or generated column, and `ALTER TABLE ADD COLUMN` with a default,
now fail with `BinderException` on an attached database. These were previously accepted and silently dropped.

Interrupting a query or write on an attached database now stops it on the server, and an interrupted write does not
commit.

## v0.2.0

Persistent `n6k` and `n6k_refresh` secrets created by earlier builds with a `SCOPE` no longer
match: recreate them. Secrets with no `SCOPE`, including the one `n6k_login` stores, still match.
