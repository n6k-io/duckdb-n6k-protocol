#pragma once

#include "crossing.hpp"
#include "crossing_link.hpp"
#include "n6k_catalog_session.hpp"

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/mutex.hpp"

#include <memory>

namespace duckdb {

struct N6kTableInfo {
	vector<string> verbs;
	bool row_ids = true;
	vector<string> primary_keys;
	vector<string> not_null;
	std::vector<std::vector<std::string>> unique_keys;
};

class DatabaseInstance;

class N6kSource : public CrossingSource {
public:
	N6kSource(DatabaseInstance &db, std::shared_ptr<CatalogSession> session, string catalog_name);

	vector<string> Schemas() override;
	vector<string> Tables(const string &schema) override;
	CrossingTable Describe(const string &schema, const string &name) override;
	CrossingSchema DescribeSchema(const string &schema) override;
	void DescribeChanged(const string &schema, CrossingTable &table) override;
	CrossingVerdict AcceptsCall(const Expression &expr) override;
	CrossingVerdict AcceptsType(const LogicalType &type) override;
	CrossingVerdict AcceptsOperator(const LogicalOperator &op) override;
	unique_ptr<CrossingLink> Connect(ClientContext &context) override;
	void Detach(ClientContext &context) override;
	//! The host socket, when the server takes plans naming any catalog open on it.
	const void *Group() const override;

	N6kTableInfo Info(const string &schema, const string &table);

	//! In crossing's operator order.
	vector<string> CrossingOperators();
	//! `action` is set, add or remove.
	vector<string> SetCrossingOperators(const string &action, const vector<string> &names);

	void ForgetServerFunctions();
	void ForgetListing();

private:
	void ListIfNeeded();
	bool ServerHasFunction(const string &name, const vector<string> &args);

	DatabaseInstance &db;
	std::shared_ptr<CatalogSession> session;
	string catalog_name;
	const void *group;
	//! Empty unless grouped: a slice over several catalogs runs on one session.
	string floor_catalog;
	mutex lock;
	bool listed = false;
	case_insensitive_map_t<case_insensitive_map_t<N6kTableInfo>> tables;
	case_insensitive_map_t<vector<string>> schema_verbs;
	mutex crossing_lock;
	case_insensitive_set_t crossing_operators;
	mutex server_functions_lock;
	case_insensitive_map_t<bool> server_functions;
};

} // namespace duckdb
