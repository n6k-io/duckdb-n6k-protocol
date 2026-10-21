#pragma once

namespace duckdb {

class ClientContext;

namespace n6k {

void MarkServed(ClientContext &context);
void ThrowIfServed(ClientContext &context, const char *fn_name);

} // namespace n6k
} // namespace duckdb
