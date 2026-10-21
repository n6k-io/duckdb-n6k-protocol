#pragma once

namespace duckdb {

class ExtensionLoader;

// Registers the n6k secret type + config provider (CREATE SECRET (TYPE n6k, TOKEN '…', SCOPE 'host')).
void RegisterN6kSecret(ExtensionLoader &loader);

} // namespace duckdb
