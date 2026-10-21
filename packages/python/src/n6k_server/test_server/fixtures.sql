-- Canonical n6k test fixture -- the single source of truth for the seeded data every
-- mount serves, and the contract each server implementation is asserted against.
--
-- Assertions in integration_tests/, packages/npm/src/__tests__/ and the framework unit
-- tests hard-code these exact rows, so editing this file changes the conformance
-- contract for every implementation at once. That is the point: it previously lived in
-- two places (seeding.py and the test server's serve prelude) which had already drifted.
--
-- Format: one statement per line, no trailing semicolon, `{catalog}` substituted with
-- the target catalog name -- which the caller must have ATTACHed already. Blank lines
-- and `--` comments are ignored. The one-statement-per-line rule is what lets both a
-- statement-at-a-time executor and a single-string SQL prelude consume this unchanged.

CREATE SCHEMA IF NOT EXISTS "{catalog}".test_schema
CREATE TABLE "{catalog}".main.users (id INTEGER, name VARCHAR, age INTEGER)
INSERT INTO "{catalog}".main.users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)
CREATE TABLE "{catalog}".test_schema.products (id INTEGER, name VARCHAR, price DOUBLE)
INSERT INTO "{catalog}".test_schema.products VALUES (1, 'Widget', 9.99), (2, 'Gadget', 19.99)
