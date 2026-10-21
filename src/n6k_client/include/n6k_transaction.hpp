#pragma once

#include "duckdb.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/common/reference_map.hpp"

namespace duckdb {

class N6kCatalog;

class N6kTransaction : public Transaction {
public:
	N6kTransaction(TransactionManager &manager, ClientContext &context);
	~N6kTransaction() override;
};

class N6kTransactionManager : public TransactionManager {
public:
	N6kTransactionManager(AttachedDatabase &db, N6kCatalog &catalog);

	Transaction &StartTransaction(ClientContext &context) override;
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	void RollbackTransaction(Transaction &transaction) override;
	void Checkpoint(ClientContext &context, bool force = false) override;

private:
	N6kCatalog &catalog;
	mutex transaction_lock;
	reference_map_t<Transaction, unique_ptr<N6kTransaction>> transactions;
};

} // namespace duckdb
