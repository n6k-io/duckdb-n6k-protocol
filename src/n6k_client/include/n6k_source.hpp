#pragma once

#include "crossing.hpp"
#include "n6k_catalog_session.hpp"

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/mutex.hpp"

#include <memory>

namespace duckdb {

struct N6kTableInfo {
	vector<string> verbs;
	vector<string> primary_keys;
};

class DatabaseInstance;
class N6kSession;

class N6kSource {
public:
	using Session = N6kSession;

	N6kSource(DatabaseInstance &db, std::shared_ptr<CatalogSession> session, string catalog_name);

	vector<string> Schemas();
	vector<string> Tables(const string &schema);
	CrossingTable Describe(const string &schema, const string &name);
	CrossingSchema DescribeSchema(const string &schema);
	//! Listing and describing inside server transaction `txn`; -1 is the committed catalog.
	vector<string> TablesIn(const string &schema, int64_t txn);
	CrossingTable DescribeIn(const string &schema, const string &name, int64_t txn);
	CrossingPlan Plan(const CrossingPlanRequest &request);
	CrossingVerdict AcceptsCall(const Expression &expr);
	CrossingVerdict AcceptsType(const LogicalType &type);
	CrossingVerdict AcceptsOperator(const LogicalOperator &op);
	unique_ptr<N6kSession> Begin(ClientContext &context);
	void Detach(ClientContext &context);
	//! The host socket, when the server takes plans naming any catalog open on it.
	const void *Group();

	N6kTableInfo Info(const string &schema, const string &table);

	//! In crossing's operator order.
	vector<string> CrossingOperators();
	//! `action` is set, add or remove.
	vector<string> SetCrossingOperators(const string &action, const vector<string> &names);

private:
	void ListIfNeeded();

	struct Columns {
		vector<string> names;
		vector<LogicalType> types;
	};

	DatabaseInstance &db;
	std::shared_ptr<CatalogSession> session;
	string catalog_name;
	const void *group;
	//! Empty unless grouped: a fragment over several catalogs runs on one session.
	string floor_catalog;
	mutex lock;
	bool listed = false;
	case_insensitive_map_t<case_insensitive_map_t<N6kTableInfo>> tables;
	case_insensitive_map_t<vector<string>> schema_verbs;
	case_insensitive_map_t<case_insensitive_map_t<Columns>> described;
	mutex crossing_lock;
	case_insensitive_set_t crossing_operators;
};

class N6kSession {
public:
	N6kSession(N6kSource &source, std::shared_ptr<CatalogSession> session, string catalog_name, bool autocommit);
	~N6kSession();

	CrossingScan Read(ClientContext &context, const CrossingQuery &query);
	CrossingWriter Write(ClientContext &context, const CrossingQuery &query);
	void Ddl(ClientContext &context, const CrossingDdl &ddl);
	vector<string> Tables(const string &schema);
	CrossingTable Describe(const string &schema, const string &name);
	void Commit();
	void Rollback();

private:
	int64_t Txn();

	N6kSource &source;
	std::shared_ptr<CatalogSession> session;
	string catalog_name;
	bool autocommit;
	mutex lock;
	int64_t txn = -1;
};

string RenderCrossingPlan(const LogicalOperator &plan);

} // namespace duckdb
