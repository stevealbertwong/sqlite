/**
 * log_recovery.h
 * 
 * Read log file from disk, redo and undo
 */

#pragma once
#include <algorithm>
#include <mutex>
#include <unordered_map>

#include "buffer/buffer_pool_manager.h"
#include "concurrency/lock_manager.h"
#include "logging/log_record.h"

namespace cmudb {

class LogRecovery {
public:
  LogRecovery(DiskManager *disk_manager,
                    BufferPoolManager *buffer_pool_manager)
      : disk_manager_(disk_manager), buffer_pool_manager_(buffer_pool_manager),
        offset_(0) {
    // global transaction through recovery phase
    log_buffer_ = new char[LOG_BUFFER_SIZE];
  }

  ~LogRecovery() {
    delete[] log_buffer_;
    log_buffer_ = nullptr;
  }

  void Redo();
  void Undo();
  bool DeserializeLogRecord(const char *data, LogRecord &log_record);

private:
  // TODO: you can add whatever member variable here
  // Don't forget to initialize newly added variable in constructor
  
  
  /* global var shared by everyone */
  DiskManager *disk_manager_;
  BufferPoolManager *buffer_pool_manager_;
  LogManager *log_manager_;
  LockManager *lock_manager_;


 
  // during normal run == txn->latestLSN as prevLSN in WAL
  // during log recovery == WAL's txn n LSN -> ATT + latest lsn 
  // i.e. built during analysis, used for undo as the start pt to undo
  std::unordered_map<txn_id_t, lsn_t> active_txn_; // ATT <txn_id, txn's latest_lsn>

  
  
  /* for read log from disk */    
  
  // LOG_BUFFER_SIZE = (BUFFER_POOL_SIZE + 1) * PAGE_SIZE
  // BUFFER_POOL_SIZE = 10
  // PAGE_SIZE = 512 bytes
  char *log_buffer_; // 11 * 512 bytes rows of WAL
  int offset_;
  
  // map of LSN <-> log file offset 
  // == only need to parse log buffer once into WAL
  std::unordered_map<lsn_t, int> lsn_offset_map_;
};

} // namespace cmudb
