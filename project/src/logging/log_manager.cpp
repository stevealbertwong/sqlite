/**
 * log_manager.cpp
 * - for w() log
 * 
 * TODO:
 * - 4 tables: WAL, TT, DPT, Buffer Page
 * - 6 funcs == start, u(), commit, abort, flush, fetch
 * - bg flush thread 
 * - 6 funcs -> 4 tables (table_page.cpp + txn_manager.cpp)
 * - 3 phases recovery
 * - 
 * - 
 * 
 * Q&A:
 * what function needs logging ?? 
 * - txn manager
 *   - init_txn, commit, abort 
 *   - starts, read, write a page 
 * - table page
 *   - get tuple, insert tuple, update tuple
 *   - mark delete, apply delete, rollaback delete
 * - buffer manager 
 *   - fetch, flush a page <-> disk 
 *   - force log manager to flush ?? 
 *   - waits for logs to be permanently stored before releasing the locks ??
 * 
 * 
 * when force flush ?? how to deal with pageLSN > flushLSN
 * - 
 * - 
 * - 
 * 
 * 
 * cv vs promise vs future 
 * - future == get == cv.wait_for()
 * - promise == set == cv.notify()
 * https://www.youtube.com/watch?v=SZQ6-pf-5Us - chinese dude
 * https://www.youtube.com/watch?v=XDZkyQVsbDY - non american
 * https://www.youtube.com/watch?v=hic5W_UMjqU - u victoria, but 1 hour long
 * 
 */

#include "logging/log_manager.h"

namespace cmudb {



/*
 * background thread to flush WAL in RAM to disk 
 * - u() 4 tables 
 * 
 * set ENABLE_LOGGING = true
 * 
 * The flush can be triggered when the log buffer is full or 
 * 
 * buffer pool manager wants to force flush (it only happens when the flushed page has a
 * larger LSN than persistent LSN) ?? FIFO ?? 
 * shouild i flush 
 * 
 * 
 * flush() WAL(log entries) first, before flush() Page
 * - flush() Page from RAM to Disk only when: flushedLSN >= pageLSN 
 * - flushedLSN == log’s latest LSN in disk 
 * 
 * 
 * even if txn commits 
 * - WAL might not be flushed before it crashes
 * - only flushes periodically
 * - 1 txn commit =/= 1 user's SQL statement
 * - 
 * 
 * 
 * log buffer == WAL 
 * 
 * d() WAL 
 * d() buffer pool 
 * d() DPT 
 * no u() TT 
 */
void LogManager::RunFlushThread() { // flush WAL

  while(true){

    std::unique_lock<std::mutex> unique_lock_WAL(lock_WAL_);
        
    // once flush thread gets lock, no txn can u() WAL until timeout/full WAL
    cv_flush_WAL_.wait_for(unique_lock_WAL, LOG_TIMEOUT){ // timeout + full WAL
      if(new_log_entries_){        
        
        log_buffer_to_flush_buffer(); // non-blocking == no IO
      }
    }

    // can append log_buffer during flush_buffer I/O
    unique_lock_WAL.unlock(); 

    // flush WAL == blocking since IO
    disk_manager_.WriteLog(flush_buffer_, flush_buffer_size_);

    if(promise_done_flush_WAL_){
      promise_done_flush_WAL_.SetValue(); // notify()
    }

  }
}



/**
 * @brief 
 * 
 * - flush WAL before flushing dirty page (force flush)
 * - WAL full (cv notify)
 * - timer (cv notify)
 */
void LogManager::ForceFlushWAL(std::shared_future future){
  
  
  // 1. wake up flsuh thread 
  cv_flush_WAL_.notify_one();

  
  // 2. block until flush thread flushed WAL 
  future.get();


}



/*
 * Stop and join the flush thread, set ENABLE_LOGGING = false
 *
 * 
 * 
 * ?? 
 * 
 */
void LogManager::StopFlushThread() {

  flush_thread_->join();

}


//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////




/*
 * log record (object) -> log buffer (char* not map)
 * 
 * you MUST set the log record's lsn within this method
 * @return: lsn that is assigned to this log record
 *
 *
 * example below
 * // First, serialize the must have fields(20 bytes in total)
 * log_record.lsn_ = next_lsn_++;
 * memcpy(log_buffer_ + offset_, &log_record, 20);
 * int pos = offset_ + 20;
 *
 * if (log_record.log_record_type_ == LogRecordType::INSERT) {
 *    memcpy(log_buffer_ + pos, &log_record.insert_rid_, sizeof(RID));
 *    pos += sizeof(RID);
 *    // we have provided serialize function for tuple class
 *    log_record.insert_tuple_.SerializeTo(log_buffer_ + pos);
 *  }
 *
 * 
 * 
 * tuple == 1 row in table 
 * log_record == tuple + log table cols (lsn, prevlsn, txnid, rid etc.)
 * 
 * Q&A:
 * why WAL == char* not map
 * - you will hv to flush char* at the end anyway
 * 
 * 
 * why pass by reference ??
 * - normal == by value/copy
 * - & == by reference/actual var == affect original var
 * - char* x_ptr = &x_var;
 * 
 * 
 */
lsn_t LogManager::AppendLogRecord(LogRecord &log_record){ 

  
  // 1. acquire WAL lock 
  // lock_guard == no unlock(), lock til func ends, but lightweighter
  std::unique_lock<std::mutex> unique_lock_WAL(lock_WAL_);

  
  // 2. if WAL is full, wake up flush thread
  if(log_buffer_.GetSize() + log_record.GetSize() >= LOG_BUFFER_SIZE){
    
    std::unique_lock<std::mutex> unique_lock_WAL(lock_WAL_);
    log_buffer_to_flush_buffer();
    
    cv_flush_WAL_.notify_one();
  }

  
  // 3. depends on log type, appends log record -> log buffer
  // 20 bytes header for all types of log
  memcpy(log_buffer_ + log_buffer_size_, &log_record, 20);
  log_buffer_size_ += 20;


  switch (log_record.log_record_type_)
  {
  case LogRecordType::INSERT:
    // rid, tuple 
    memcpy(log_buffer_ + log_buffer_size_, &log_record.insert_rid_, sizeof(RID));
    log_buffer_size_ += sizeof(RID);

    log_record.insert_tuple_.SerializeTo(log_buffer_ + log_buffer_size_);
    log_buffer_size_ += log_record.insert_tuple_.GetLength();

    break;
  
  case LogRecordType::MARKDELETE:
       LogRecordType::ROLLBACKDELETE:
       LogRecordType::APPLYDELETE:
    // rid, tuple 
    memcpy(log_buffer_ + log_buffer_size_, &log_record.delete_rid_, sizeof(RID));
    log_buffer_size_ += sizeof(RID);

    log_record.delete_tuple_.SerializeTo(log_buffer_ + log_buffer_size_);
    log_buffer_size_ += log_record.delete_tuple_.GetLength();

    break;

  case LogRecordType::UPDATE:

    // new rid, old tuple, new tuple 
    memcpy(log_buffer_ + log_buffer_size_, &log_record.update_rid, sizeof(RID));
    log_buffer_size_ += sizeof(RID);
    
    log_record.old_tuple.SerializeTo(log_buffer_ + log_buffer_size_);
    log_buffer_size_ += log_record.old_tuple.GetLength();
    
    log_record.new_tuple.SerializeTo(log_buffer_ + log_buffer_size_);
    log_buffer_size_ += log_record.new_tuple.GetLength();
    
    break;
  
  
  case LogRecordType::NEWPAGE:
    
    // previous page id
    memcpy(log_buffer_ + log_buffer_size_, &log_record.prev_page_id, sizeof(page_id_t));
    log_buffer_size_ += sizeof(page_id_t);
    
    
    break;

  default:
    break;
  }


  // 4. u() WAL
  log_record.lsn_ += next_lsn_;
  new_log_entries_ = true;

  return log_record.lsn_;
}


/**
 * @brief 
 * why swap instead of append ??
 */
void log_buffer_to_flush_buffer(){
  
  // 1. apeend at end, no flush 
  memcpy(flush_buffer_ + flush_buffer_size_, log_buffer_, log_buffer_size_);

  // 2. u() log manager struct
  flush_buffer_size_ += log_buffer_size_;
  log_buffer_size_ = 0;
  new_log_entries_ = false;

}

} // namespace cmudb
