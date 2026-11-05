# Changelog

## Unreleased

A client function the server lacks now runs on the client instead of failing or running the server's function of
that name. This adds the `HAS_FUNCTION` op and the `has_function` capability; against a server without them, named
function calls run on the client, with the same results but slower.

## v0.2.0

Persistent `n6k` and `n6k_refresh` secrets created by earlier builds with a `SCOPE` no longer
match: recreate them. Secrets with no `SCOPE`, including the one `n6k_login` stores, still match.
