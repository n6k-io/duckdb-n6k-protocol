#pragma once

namespace duckdb {

class ExtensionLoader;

namespace n6k {

// Table functions the conformance suite calls through RPC, for the two shapes SQL cannot express:
// an unbounded stream (n6k_testing_stream_counter) and a declared TABLE parameter
// (n6k_testing_sum_table), plus n6k_testing_served_catalogs for asserting on the catalog set a
// serve call would resolve.
void RegisterN6kTestingFixtures(ExtensionLoader &loader);

} // namespace n6k
} // namespace duckdb
