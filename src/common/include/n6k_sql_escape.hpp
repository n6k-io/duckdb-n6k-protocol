#pragma once

#include <string>

namespace duckdb {
namespace n6k {

// SQL-standard escaping: an embedded single quote is doubled, never backslash-escaped. Every path
// that interpolates a string into SQL goes through here, so the rule is stated once.
inline std::string EscapeSqlLiteral(const std::string &s) {
	std::string out;
	out.reserve(s.size() + 2);
	for (char c : s) {
		if (c == '\'') {
			out += "''";
		} else {
			out += c;
		}
	}
	return out;
}

// The escaped form wrapped in the surrounding quotes, ready to splice into a statement.
inline std::string QuoteSqlLiteral(const std::string &s) {
	return "'" + EscapeSqlLiteral(s) + "'";
}

// Quote a SQL identifier, doubling any embedded double-quote. DuckDB auto-names unaliased computed
// columns with the source expression, which can itself contain quoted identifiers (e.g.
// max(CASE WHEN ("year"(date) = 2015) ...)); wrapping one in "..." unescaped splits the SQL.
inline std::string QuoteIdent(const std::string &name) {
	std::string out = "\"";
	for (char c : name) {
		if (c == '"') {
			out += "\"\"";
		} else {
			out += c;
		}
	}
	out += "\"";
	return out;
}

} // namespace n6k
} // namespace duckdb
