# Threat model

This file defines n6k's threat model: who is trusted, who is the attacker, and what n6k must
guarantee. Code and reviews are judged against it, and changes to the model are made here.
It does not cover host and deployment concerns or implementation detail.

n6k gives a client SQL, including raw SQL, on the database the host serves. That database is the
boundary.

## Trusted

- **Host.** Chooses what database each client is served.

## Attacker

- **Client.** Sends any frame and any SQL.
- **Server, from the client's side.** Sends any frame.

## Boundary

- **Bare database.** The client reaches everything in it. Clients sharing one reach each other.
- **Bridged database.** The bridge's grants are the boundary; see `duckdb-virtual-catalog`.
- **Provider catalog.** The host's code decides what is visible.
- **RPC.** Text-frame methods and tools are host code and carry the usual RPC risks.

## Hardening

- Run `vcatbridge_lockdown` on the served instance; see `src/bridge/lockdown.cpp` in
  `duckdb-virtual-catalog` for what it restricts.

## n6k must

- Serve nothing until the host accepts the token, then only what the host served.
- Never widen access beyond the served database.
- Survive hostile frames on both sides.
- Send a client's token only to servers its secret's `SCOPE` matches.
- Route an upstream's frames only to the sessions forwarded to it.

## Accepted leaks

- **SQL error text** reaches the client.
