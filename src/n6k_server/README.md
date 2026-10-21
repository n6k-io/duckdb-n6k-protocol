# n6k_server — native n6k protocol server

A DuckDB-loadable C++ extension that **serves** one or more DuckDB catalogs over
the n6k database protocol, with no Python. It is the server-side counterpart to
the client `n6k` extension (`src/n6k_client`), reusing the same wire protocol
(`src/common/include/n6k_protocol_generated.hpp`), the same SQL builder
(`src/common/n6k_sql_builder.cpp`) and the same permissions collector
(`src/common/n6k_permissions.cpp`) as the client.

Documentation: [`docs/n6k-server.md`](../../docs/n6k-server.md).
