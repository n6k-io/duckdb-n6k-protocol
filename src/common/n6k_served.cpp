#include "n6k_served.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_context_state.hpp"

namespace duckdb {
namespace n6k {

namespace {

constexpr const char *SERVED_STATE_KEY = "n6k_served";

class ServedState : public ClientContextState {};

} // namespace

void MarkServed(ClientContext &context) {
	context.registered_state->GetOrCreate<ServedState>(SERVED_STATE_KEY);
}

void ThrowIfServed(ClientContext &context, const char *fn_name) {
	if (context.registered_state->Get<ClientContextState>(SERVED_STATE_KEY)) {
		throw PermissionException("%s: not callable from a served connection", fn_name);
	}
}

} // namespace n6k
} // namespace duckdb
