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


  // spawn a separate thread to wake up periodically to flush
  void RunFlushThread();
  void StopFlushThread();

  // append a log record into log buffer
  lsn_t AppendLogRecord(LogRecord &log_record);

  // get/set helper functions
  inline lsn_t GetPersistentLSN() { return persistent_lsn_; }
  inline void SetPersistentLSN(lsn_t lsn) { persistent_lsn_ = lsn; }
  inline char *GetLogBuffer() { return log_buffer_; }





// 4 tables, 5 LSNs are all here ??
private:

  /* WAL */
  // LSN, prevLSN, pageID, payload 
  char *log_buffer_; // WAL in RAM  
  int log_buffer_size_;
  std::atomic<lsn_t> next_lsn_; // LSN
  


  /* flush */
  // log records before & include persistent_lsn_ have been written to disk
  std::atomic<lsn_t> persistent_lsn_; // flushLSN    
  char *flush_buffer_; // buffer since IO is slow, while holds lock
  std::thread *flush_thread_;    


  /* lock */
  // 1+ txns will call 6 funcs, pages can flush/fetch anytime
  std::mutex lock_WAL_; // protect all vars  
  
  // only 1 bg flush thread 
  // notified by timer + WAL full 
  std::condition_variable cv_flush_WAL_; 
  

  
  DiskManager *disk_manager_; // ?? 

};

} // namespace cmudb
