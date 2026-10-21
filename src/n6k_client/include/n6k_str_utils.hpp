#pragma once

#include <string>

namespace duckdb {
namespace n6k {

std::string EscapeSingleQuotedJsLiteral(const std::string &s);

} // namespace n6k
} // namespace duckdb
