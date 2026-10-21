#include "n6k_transaction.hpp"
#include "n6k_catalog.hpp"

namespace duckdb {

N6kTransaction::N6kTransaction(TransactionManager &manager, ClientContext &context) : Transaction(manager, context) {
}

N6kTransaction::~N6kTransaction() = default;

N6kTransactionManager::N6kTransactionManager(AttachedDatabase &db, N6kCatalog &catalog)
    : TransactionManager(db), catalog(catalog) {
}

Transaction &N6kTransactionManager::StartTransaction(ClientContext &context) {
	auto transaction = make_uniq<N6kTransaction>(*this, context);
	auto &result = *transaction;
	lock_guard<mutex> l(transaction_lock);
	transactions[result] = std::move(transaction);
	return result;
}

ErrorData N6kTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	auto &attach = catalog.Attach();
	auto error = attach.Commit(attach.Release(transaction));
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
	return error;
}

void N6kTransactionManager::RollbackTransaction(Transaction &transaction) {
	auto &attach = catalog.Attach();
	attach.Rollback(attach.Release(transaction));
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
}

void N6kTransactionManager::Checkpoint(ClientContext &context, bool force) {
}

} // namespace duckdb
