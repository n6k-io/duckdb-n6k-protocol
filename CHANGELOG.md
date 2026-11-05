# Changelog

## v0.1.0

Initial release.

Persistent `n6k` and `n6k_refresh` secrets created by earlier builds with a `SCOPE` no longer
match: recreate them. Secrets with no `SCOPE`, including the one `n6k_login` stores, still match.
