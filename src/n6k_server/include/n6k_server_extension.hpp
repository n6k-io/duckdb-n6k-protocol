#pragma once

#include "duckdb.hpp"

namespace duckdb {

class N6kServerExtension : public Extension {
public:
	void Load(ExtensionLoader &loader) override;
	std::string Name() override;
	std::string Version() const override;
};

void N6kServerLoadInternal(ExtensionLoader &loader);

} // namespace duckdb
