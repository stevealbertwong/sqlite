/**
 * log_manager.h
 * log manager maintain a separate thread that is awaken when the log buffer is
 * full or time out(every X second) to write log buffer's content into disk log
 * file.
 * 
 * 
 * 
 * 
 */

#pragma once
#include <algorithm>
#include <condition_variable>
#include <future>
#include <mutex>

#include "disk/disk_manager.h"
#include "logging/log_record.h"

namespace cmudb {

class LogManager {
public:
  LogManager(DiskManager *disk_manager)
      : next_lsn_(0), persistent_lsn_(INVALID_LSN),
        disk_manager_(disk_manager) {
    // TODO: you may intialize your own defined memeber variables here
    log_buffer_ = new char[LOG_BUFFER_SIZE];
    flush_buffer_ = new char[LOG_BUFFER_SIZE];
  }

  ~LogManager() {
    delete[] log_buffer_;
    delete[] flush_buffer_;
    log_buffer_ = nullptr;
    flush_buffer_ = nullptr;
  }


  /* major funcs */
  // spawn a separate thread to wake up periodically to flush
  lsn_t AppendLogRecord(LogRecord &log_record);
  void RunFlushThread();
  void ForceFlushWAL();
  void log_buffer_to_flush_buffer();

  void StopFlushThread();
  
  
  /* getter, setter, helper */
  inline lsn_t GetPersistentLSN() { return persistent_lsn_; }
  inline void SetPersistentLSN(lsn_t lsn) { persistent_lsn_ = lsn; }
  inline char *GetLogBuffer() { return log_buffer_; }
  
  

/**
 * @brief 
 * 
 * 5 LSNs are all here ??
 * - LSN, flushLSN == here
 * - 
 * 
 * 4 tables
 * TT == txn mananger 
 * ATT == log recovery 
 */
private:

  /* WAL */
  // LSN, prevLSN, pageID, payload 
  char *log_buffer_; // WAL in RAM  
  int log_buffer_size_;
  std::atomic<lsn_t> next_lsn_; // LSN
  


  /* flush */
  // log records before & include persistent_lsn_ have been written to disk
  std::atomic<lsn_t> persistent_lsn_; // flushLSN == WAL LSN in disk
  char *flush_buffer_; // buffer since IO is slow, while holds lock
  int flush_buffer_size_;
  std::thread *flush_thread_;
  bool new_log_entries_;
  DiskManager *disk_manager_; // w() log


  /* syncronization */
  // 1+ txns will call 6 funcs, pages can flush/fetch anytime
  std::mutex lock_WAL_; // protect all vars  
  std::condition_variable cv_flush_WAL_; // only 1 bg flush thread 
  std::promise<void> *promise_done_flush_WAL_; // like cv
  
  

};

} // namespace cmudb
