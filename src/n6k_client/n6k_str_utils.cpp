#include "n6k_str_utils.hpp"

namespace duckdb {
namespace n6k {

std::string EscapeSingleQuotedJsLiteral(const std::string &s) {
	std::string result;
	result.reserve(s.size());
	for (auto c : s) {
		switch (c) {
		case '\\':
			result += "\\\\";
			break;
		case '\'':
			result += "\\'";
			break;
		case '\n':
			result += "\\n";
			break;
		case '\r':
			result += "\\r";
			break;
		case '\0':
			result += "\\0";
			break;
		default:
			result += c;
		}
	}
	return result;
}

} // namespace n6k
} // namespace duckdb
