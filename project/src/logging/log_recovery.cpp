/**
 * log_recovey.cpp
 */

#include "logging/log_recovery.h"
#include "page/table_page.h"

namespace cmudb {


/*
 * helper of redo() n undo()
 * == raw bytes -> struct 
 * == helper's code all happens in RAM
 * 
 * deserialize a log record from log buffer
 * == raw bytes (log buffer) -> struct (log record)
 * == construct 1 row of WAL
 * 
 * log record == 1 row in WAL
 * 
 * @return: true means deserialize succeed, otherwise can't deserialize cause
 * incomplete log record
 * 
 * 
 * 
 * 
 * 
 * Q&A: 
 * == analyse phase ?? reconstruct ATT, DPT ?? 
 * - ATT == each txn has its own latestLSN
 * - DPT == each page has is_dirty, wt abt earliestLSN ?? minDirtyPageLSN ??
 * 
 * 
 */
bool LogRecovery::DeserializeLogRecord(const char *data,
                                      LogRecord &log_record) {
  
  // 1. raw bytes (data) -> struct (log record)
  
  // 1.1 header, 5 fields, 4 bytes each 
  // | size | LSN | transID | prevLSN | LogType |
  int32_t bytes_of_field = 4;
  log_record.size_ = *reinterpret_cast<const int *>(data);
  log_record.lsn_ = *reinterpret_cast<const lsn_t *>(data + bytes_of_field);
  log_record.txn_id_ = *reinterpret_cast<const txn_id_t *>(data + bytes_of_field*2);
  log_record.prev_lsn_ = *reinterpret_cast<const lsn_t *>(data + bytes_of_field*3);
  log_record.log_record_type_ = *reinterpret_cast<const LogRecordType*>(data + bytes_of_field*4);

  // 1.2
  switch (log_record.log_record_type_){
  
  // | HEADER | tuple_rid | tuple_size | tuple_data(char[] array) |
  // log record does not follow graph bytes
  // use log_record.h private var instead of bytes layout graph
  case LogRecordType::INSERT:
    log_record.insert_rid_ = *reinterpret_cast<const RID *>(data + LogRecord::HEADER_SIZE);
    // NOTE: tuple == class, 4 types of constructor
    log_record.insert_tuple_.DeserializeFrom(data + LogRecord::HEADER_SIZE + sizeof(RID));
    break;
  
  case LogRecordType::MARKDELETE:
  case LogRecordType::APPLYDELETE:
  case LogRecordType::ROLLBACKDELETE:
    log_record.delete_rid_ = *reinterpret_cast<const RID *>(data + LogRecord::HEADER_SIZE);    
    log_record.delete_tuple_.DeserializeFrom(data + LogRecord::HEADER_SIZE + sizeof(RID));
    break;

  case LogRecordType::UPDATE:
    log_record.update_rid_ = *reinterpret_cast<const RID *>(data + LogRecord::HEADER_SIZE);
    log_record.old_tuple_.DeserializeFrom(data + LogRecord::HEADER_SIZE + sizeof(RID));
    log_record.new_tuple_.DeserializeFrom(data + LogRecord::HEADER_SIZE + sizeof(RID) + log_record.old_tuple_.GetLength());
    break;

  case LogRecordType::NEWPAGE: 
    log_record.prev_page_id_ = *reinterpret_cast<const page_id_t *>(data + LogRecord::HEADER_SIZE);


  // skip ?? 
  case LogRecordType::BEGIN:
  case LogRecordType::COMMIT:
  case LogRecordType::ABORT:    
  case LogRecordType::CLR: // ??

  default:
    break;
  }


  // 2. reconstruct ATT, DPT, minDirtyPageLSN ?? 
  // loop from top to bottom 




  
  return true;
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
 * disk page LSN < commit LSN == redo 
 * 
 * redo == analysis + redo 
 * since in test == redo(), then undo()
 * 
 * 
 * redo phase on TABLE PAGE level(table/table_page.h)
 * 
 * read log file from the beginning to end 
 * (you must prefetch log records into log buffer to reduce unnecessary I/O operations)
 * 
 *
 * remember to compare page's LSN with log_record's sequence number, 
 * and also build active_txn_ table & lsn_mapping_ table
 * 
 * 
 * 
 * Q&A 
 * virtual table vs table heap vs table page ?? 
 * read abort() + 3 table classes to help understand
 * 
 * 
 * 
 * ATT 
 * - who are the uncommitted txns 
 * - which LSN to start undo changes by uncommitted txns
 * 
 * 
 */
void LogRecovery::Redo() {

  
  // 1. while loop read WAL from disk from top to bottom == all txns' logs
  // each time read 11 * 512 bytes == (BUFFER_POOL_SIZE + 1) * PAGE_SIZE
  int offset = 0;
  while(disk_manager_->ReadLog(log_buffer_, LOG_BUFFER_SIZE, offset)){
    offset += LOG_BUFFER_SIZE;
    
    // 2. raw bytes -> struct 
    // each time parse log_entry size bytes from 11 * 512 bytes
    LogRecord log_entry;
    int log_file_offset = 0;
    while(DeserializeLogRecord(log_buffer_, log_entry)){
      
      // for later undo 
      lsn_offset_map_[log_entry.GetLSN()] = offset_ + log_file_offset;
      log_file_offset += log_entry.GetSize();
    
      
      
      // 3. re-execute WAL row 
      // make sure disk page's LSN to up to disk WAL's LSN
      switch (log_entry.GetLogRecordType()){
            
      // init ATT
      // ATT == find txns that need to undo 
      case LogRecordType::BEGIN: 
        active_txn_[log_entry.GetTxnID()] = log_entry.GetLSN();
        break;
            
      
      // remove txn from ATT       
      case LogRecordType::COMMIT:
        active_txn_.erase(record.GetTxnId());
        break;

      
      // remove txn from ATT 
      case LogRecordType::ABORT:
        active_txn_.erase(record.GetTxnId());
        break;
      


      // for all following == u() ATT to latest LSN ?? 

      case LogRecordType::NEWPAGE: 
        
        // 0. 
        auto new_page = buffer_pool_manager_->NewPage();
        TablePage* table_page = reinterpret_cast<TablePage*>(new_page);
        table_page->Init(new_page->GetPageId(), PAGE_SIZE, log_entry->GetPrevPageId(), log_manager, nullptr);

        break;
      
      
      case LogRecordType::INSERT:
        
        // 1. fetch page
        // record id == page id + slot no.
        auto page = buffer_pool_manager_->FetchPage(log_entry->GetInsertRID()->GetPageID());
        
        // 2. disk page LSN < commit LSN == redo 
        if(page->GetLSN() < log_entry->GetLSN()){

          // 3. insert tuple into table again 
          TablePage *table_page = reinterpret_cast<TablePage *>(page);
          table_page->InsertTuple(log_entry.insert_tuple_, 
              log_entry.GetInsertRID(), nullptr, lock_manager_, log_manager_);
        }
        
        buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);

        break;
      
      case LogRecordType::MARKDELETE:


        break;

      case LogRecordType::APPLYDELETE:


        break;

      case LogRecordType::ROLLBACKDELETE:

        break;

      case LogRecordType::UPDATE:

        break;

      
        

      // ??
      case LogRecordType::CLR: 

      default:
        break;
      }
    }


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

/*
 * disk page LSN > commit LSN == undo 
 * undo == undo all changes if its not "committed"
 * 
 * undo phase on TABLE PAGE level(table/table_page.h)
 * iterate through active txn map and undo each operation
 * 
 * 
 * 
 * redo() then undo() 
 * - redo() == bring page up to date of what log says it shd be like
 * - undo() == then undo all uncommitted changes to previous commit
 * 
 * 
 * redo() + undo() 
 * - undo what's been redone, wasted repeated steps
 * - since no analysis phase, redo once to learn whether committed at the end
 * 
 * 
 * 
 * ATT 
 * - who are the uncommitted txns 
 * - latestLSN == LSN to start undo changes by uncommitted txns
 * 
 * 
 * 
 * 
 */
void LogRecovery::Undo() {

  // 1. read log entries into heap == loop WAL from bottom to top   
  for (auto ATT_entry = active_txn_.begin(); ATT_entry < active_txn_.end(); ++ATT_entry){
    
    // alternative heap: 
    // txn's latest_lsn + lsn_offset_map + row's prevLSN
    lsn_t txn_latest_lsn = ATT_entry->second();
    int txn_latest_lsn_offset = lsn_offset_map_[txn_latest_lsn];
    
    
    // 2. raw bytes -> struct        
    bool curr_txn_finished_undo = false;
    while(true){

      if(curr_txn_finished_undo){
        break;
      }

      // read log belonging to T1/T2/T3 etc. from its last entry
      // only need 1 row's size so LOG_BUFFER_SIZE is overkill
      disk_manager_->ReadLog(log_buffer_, LOG_BUFFER_SIZE, txn_latest_lsn_offset);                
      LogRecord log_entry;    
      DeserializeLogRecord(log_buffer_, log_entry)
                            
      
      // 3. undo all uncommitted == until "BEGIN"
      // undo == do the opposite 
      switch (log_entry.GetLogRecordType()){
          
      
      case LogRecordType::BEGIN:         
        curr_txn_finished_undo = true; // next txn 
        break;
      
      
      case LogRecordType::COMMIT:
        // uncommitted txn not suppose to have this
        assert(false);
        break;
            
      case LogRecordType::ABORT:
        // uncommitted txn not suppose to have this
        assert(false);
        break;
      
            
      
      // undo INSERT == APPLYDELETE 
      case LogRecordType::INSERT:
        
        // 1. fetch page        
        auto page = buffer_pool_manager_->FetchPage(log_entry->GetInsertRID()->GetPageID());
        
        // 2. disk page LSN < commit LSN == nothing to undo 
        if(page->GetLSN() < log_entry->GetLSN()){
          break;
        }       

        // 3. ApplyDelete
        TablePage *table_page = reinterpret_cast<TablePage *>(page);
        table_page->ApplyDelete(log_entry->rid, nullptr, nullptr);
        buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
        
        break;
      
      
      //undo MARKDELETE == ROLLBACKDELETE
      case LogRecordType::MARKDELETE:
        
        // 1. fetch page        
        auto page = buffer_pool_manager_->FetchPage(log_entry->GetInsertRID()->GetPageID());
        
        // 2. disk page LSN < commit LSN == nothing to undo 
        if(page->GetLSN() < log_entry->GetLSN()){
          break;
        }       

        // 3. ApplyDelete
        TablePage *table_page = reinterpret_cast<TablePage *>(page);
        table_page->RollbackDelete(log_entry->rid, nullptr, nullptr);
        buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
                
        break;

      


      case LogRecordType::APPLYDELETE:

        
        

        // 1. fetch page        
        auto page = buffer_pool_manager_->FetchPage(log_entry->GetInsertRID()->GetPageID());
        
        // 2. disk page LSN < commit LSN == nothing to undo 
        if(page->GetLSN() < log_entry->GetLSN()){
          break;
        }       

        // 3. ApplyDelete
        TablePage *table_page = reinterpret_cast<TablePage *>(page);
        // log contains delete_tuple -> put tuple back to original slot 
        // tuple == delete_tuple
        table_page->InsertTuple(log_entry->delete_tuple_, log_entry->delete_rid_, nullptr, nullptr);
        buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
                
        
        break;

      case LogRecordType::ROLLBACKDELETE:

        // 1. fetch page        
        auto page = buffer_pool_manager_->FetchPage(log_entry->GetInsertRID()->GetPageID());
        
        // 2. disk page LSN < commit LSN == nothing to undo 
        if(page->GetLSN() < log_entry->GetLSN()){
          break;
        }       

        // 3. MarkDelete
        TablePage *table_page = reinterpret_cast<TablePage *>(page);
        table_page->MarkDelete(log_entry->rid, nullptr, nullptr);
        buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
        
        break;

      case LogRecordType::UPDATE:


        // 1. fetch page        
        auto page = buffer_pool_manager_->FetchPage(log_entry->GetInsertRID()->GetPageID());
        
        // 2. disk page LSN < commit LSN == nothing to undo 
        if(page->GetLSN() < log_entry->GetLSN()){
          break;
        }       

        // 3. MarkDelete
        TablePage *table_page = reinterpret_cast<TablePage *>(page);
        table_page->UpdateTuple(log_entry->old_tuple, log_entry->new_tuple, log_entry->rid, nullptr, nullptr, nullptr);
        buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
        
        break;



      // fetch the new page n zero out ??
      case LogRecordType::NEWPAGE: 
        
        auto new_page = buffer_pool_manager_->NewPage();
        TablePage* table_page = reinterpret_cast<TablePage*>(new_page);
        table_page->Init(new_page->GetPageId(), PAGE_SIZE, log_entry->GetPrevPageId(), log_manager, nullptr);

        break;
      

      default:
        break;
                  
      }

    
      txn_latest_lsn = log_entry->GetPrevLSN();
      txn_latest_lsn_offset = lsn_offset_map_[txn_latest_lsn];

    }

  }
  


  


  // 3. 




}



} // namespace cmudb
