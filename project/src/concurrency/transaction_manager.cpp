/**
 * transaction_manager.cpp
 * 
 * NOTE: 
 * TT latestLSN is in txn.cpp not here 
 * - its not a table, just each txn maintains its own latestLSN
 * - prevLSN == latestLSN
 * 
 * 
 *
 */
#include "concurrency/transaction_manager.h"
#include "table/table_heap.h"

#include <cassert>
namespace cmudb {


Transaction *TransactionManager::Begin() {
  Transaction *txn = new Transaction(next_txn_id_++);

  if (ENABLE_LOGGING) {
    // TODO: write log and update transaction's prev_lsn here
    

    // 1. u() START in WAL 
    LogRecord log_entry(txn->GetTransactionId(), txn->GetPrevLSN(), LogRecordType::BEGIN);
    auto lsn = log_manager_->AppendLogRecord(log_entry);

    // 2. init TT, latestLSN/prevLSN == lsn, status == running 
    txn->SetPrevLSN(lsn);

  }

  return txn;
}


//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////



/**
 * @brief 
 * commit == garbage collection for 4 mngrs 
 * - for releasing 2 levels of locks == strict 2PL, 2PL s/xlock
 * - for d() rollback history in case of abort 
 * - for u() WAL in RAM 
 * 
 * 
 * commit == commit + flush 
 * 
 * 
 * 
 * 
 * when Txn Manager hit COMMIT 
 * - write log record COMMIT to Log Manager’s log_buffer (WAL in RAM) 
 * - Log Manager copy log_buffer to flush_buffer + updates flushedLSN 
 * - spawn a thread to flush() flush_buffer to WAL(Disk) 
 * 
 * after flush() WAL to Disk
 * - trim/delete() WAL in RAM up to flushedLSN
 * 
 * u() WAL, TT
 * d() DPT, buffer pool 
 * 
 * 
 * 
 * Q&A 
 * why wait for log mngr to flush WAL to disk ?? why let buffer pool mngr to flush dirty page to disk ??
 * - txn mngr == u() "COMMIT/ABORT" in WAL in RAM
 * - buffer mngr == flush() dirty pages periodically by LRU/FIFO 
 * - log mngr == flush() WAL periodically
 * 
 */
void TransactionManager::Commit(Transaction *txn) {


  // 1. for strict 2PL
  txn->SetState(TransactionState::COMMITTED); 


  // 2. for txn aborts rollback
  // truly delete before commit
  auto write_set = txn->GetWriteSet(); 
  while (!write_set->empty()) {
    auto &item = write_set->back();
    auto table = item.table_;
    if (item.wtype_ == WType::DELETE) {
      // this also release the lock when holding the page latch
      table->ApplyDelete(item.rid_, txn);
    }
    write_set->pop_back();
  }
  write_set->clear();


  // 3. for WAL 
  // u() WAL, TT + d() DPT, buffer pool 
  // if user commits/save/appends WAL in RAM every 10 seconds, 
  // but wait for bg flush thread to flush WAL every 1 min 
  // if crash, then what user committed is lost ?? 
  // if flush WAL everytime user commits == too slow ?? 
  // if flush WAL, why not directly flush page ?? 
  if (ENABLE_LOGGING) {
    // TODO: write log and update transaction's prev_lsn here


    // 3.1 appends "COMMIT" row in WAL in RAM 
    LogRecord log_entry(txn->GetTransactionId(), txn->GetPrevLSN(), 
              LogRecordType::COMMIT);
    // locking implemented within AppendLogRecord()
    auto lsn = log_manager_->AppendLogRecord(log_entry);    
    // txn->SetPrevLSN(lsn); // no u() TT latestLSN


    // 3.2 block till flush WAL(RAM) up to commit to disk
    log_manager_->ForceFlushWAL();

    
    // 3.3 w() <END, txnID> to log     

  }


  // 4. for 2PL 
  // release all the lock
  std::unordered_set<RID> lock_set;
  for (auto item : *txn->GetSharedLockSet())
    lock_set.emplace(item);
  for (auto item : *txn->GetExclusiveLockSet())
    lock_set.emplace(item);
  // release all the lock
  for (auto locked_rid : lock_set) {
    lock_manager_->Unlock(txn, locked_rid);
  }
}


//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////


void TransactionManager::Abort(Transaction *txn) {
  txn->SetState(TransactionState::ABORTED);
  // rollback before releasing lock
  auto write_set = txn->GetWriteSet();
  while (!write_set->empty()) {
    auto &item = write_set->back();
    auto table = item.table_;
    if (item.wtype_ == WType::DELETE) {
      LOG_DEBUG("rollback delete");
      table->RollbackDelete(item.rid_, txn);
    } else if (item.wtype_ == WType::INSERT) {
      LOG_DEBUG("rollback insert");
      table->ApplyDelete(item.rid_, txn);
    } else if (item.wtype_ == WType::UPDATE) {
      LOG_DEBUG("rollback update");
      table->UpdateTuple(item.tuple_, item.rid_, txn);
    }
    write_set->pop_back();
  }
  write_set->clear();

  if (ENABLE_LOGGING) {
    // TODO: write log and update transaction's prev_lsn here


    LogRecord log_entry(txn->GetTransactionId(), txn->GetPrevLSN(), 
              LogRecordType::ABORT);    
    auto lsn = log_manager->AppendLogRecord(log_entry);                
    log_manager_->ForceFlushWAL();


  }

  // release all the lock
  std::unordered_set<RID> lock_set;
  for (auto item : *txn->GetSharedLockSet())
    lock_set.emplace(item);
  for (auto item : *txn->GetExclusiveLockSet())
    lock_set.emplace(item);
  // release all the lock
  for (auto locked_rid : lock_set) {
    lock_manager_->Unlock(txn, locked_rid);
  }
}
} // namespace cmudb
