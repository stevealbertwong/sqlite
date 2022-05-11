/**
 * table_page.cpp
 * 
 * 
 * Slotted page format:
 *  ---------------------------------------
 * | HEADER | ... FREE SPACES ... | TUPLES |
 *  ---------------------------------------
 *                                 ^
 *                         free space pointer
 *
 *  Header format (size in byte):
 *  --------------------------------------------------------------------------
 * | PageId (4)| LSN (4)| PrevPageId (4)| NextPageId (4)| FreeSpacePointer(4) |
 *  --------------------------------------------------------------------------
 *  --------------------------------------------------------------
 * | TupleCount (4) | Tuple_1 offset (4) | Tuple_1 size (4) | ... |
 *  --------------------------------------------------------------
 * 
 * write operation + its corresponding operation when UNDO/REDO
 * CRUB APIs at table level ??
 * 
 * 
 * 1 table of DB == 1 tab of excel 
 * 
 * 
 * NOTE:
 * "txn u()" for the purpose of u() 4 tables 
 * 
 * 1. 
 * table class hierarchy: 
 * - virtual table -> table heap -> table page
 * - all 3 has InsertTuple(), MarkDelete(), ApplyDelete(), 
 * GetTuple(), UpdateTuple(), RollbackDelete()
 * 
 * - virtual table == 
 * 
 * - table heap == find any disk page have enough space to insert tuple 
 *    *buffer_pool_manager, LockManager *lock_manager, LogManager *log_manager,
 * 
 * - table page == actual memcpy data -> disk page 
 *   CRUD() pageid, pageLSN, free slot ptr, tuple offset, tuple count 
 *   disk page can be directly typecast to table page
 * 
 * 
 * 
 * 
 * 2. 
 * why u() RAM WAL at lowest table page not the higher ups ?
 * - all 3 classes == run only in RAM
 * - u() RAM page n RAM WAL should happen at same time 
 * - table page == to u() RAM page 
 * 
 * 
 * 
 * 3. 
 * page vs tuple 
 * 
 * 
 * 
 */

#include <cassert>

#include "page/table_page.h"



namespace cmudb {


page_id_t TablePage::GetPageId() {
  return *reinterpret_cast<page_id_t *>(GetData());
}

page_id_t TablePage::GetPrevPageId() {
  return *reinterpret_cast<page_id_t *>(GetData() + 8);
}

page_id_t TablePage::GetNextPageId() {
  return *reinterpret_cast<page_id_t *>(GetData() + 12);
}

void TablePage::SetPrevPageId(page_id_t prev_page_id) {
  memcpy(GetData() + 8, &prev_page_id, 4);
}

void TablePage::SetNextPageId(page_id_t next_page_id) {
  memcpy(GetData() + 12, &next_page_id, 4);
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
 * 
 * 
 * prev_page_id == for linked list of pages
 */
void TablePage::Init(page_id_t page_id, size_t page_size,
                     page_id_t prev_page_id, LogManager *log_manager,
                     Transaction *txn) {
  
  // 1st 4 bytes of any page == PageId
  memcpy(GetData(), &page_id, 4); // set PageId
  
  if (ENABLE_LOGGING) {
    // TODO: add your logging logic here

    // 1. u() WAL 
    LogRecord(txn->GetTransactionId(), txn->GetPrevLSN(), LogRecordType::INSERT,
          page_id_t prev_page_id)    
    auto lsn = log_manager->AppendLogRecord(log_entry);

    // 2. u() TT lastest LSN
    txn->SetPrevLSN(lsn);
    
    // 3. u() buffer pool PageLSN
    SetLSN(lsn);

  }

  SetPrevPageId(prev_page_id);
  SetNextPageId(INVALID_PAGE_ID);
  SetFreeSpacePointer(page_size);
  SetTupleCount(0);
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
 * InsertTuple == deep add
 * 
 * rid == tuple's offset in page
 */
bool TablePage::InsertTuple(const Tuple &tuple, RID &rid, Transaction *txn,
                            LockManager *lock_manager,
                            LogManager *log_manager) {
  
  
  // 0.
  assert(tuple.size_ > 0);
  if (GetFreeSpaceSize() < tuple.size_) {
    return false; // not enough space
  }

  // 1. loop header's bitmap to find a free slot 
  int i;  
  for (i = 0; i < GetTupleCount(); ++i) { // loop bitmap in header
    
    rid.Set(GetPageId(), i); // u() tuple's rid (page_id, slot)
    if (GetTupleSize(i) == 0) { // found empty slot
      
      if (ENABLE_LOGGING) { 
        // ?? 
        assert(txn->GetSharedLockSet()->find(rid) ==
                   txn->GetSharedLockSet()->end() &&
               txn->GetExclusiveLockSet()->find(rid) ==
                   txn->GetExclusiveLockSet()->end());
      }
      break;
    }
  }

  // no free slot left
  if (i == GetTupleCount() && GetFreeSpaceSize() < tuple.size_ + 8) {
    return false; // not enough space
  }

  
  // 2. found, tuple -> page, u() page meta in header       
  SetFreeSpacePointer(GetFreeSpacePointer() - tuple.size_); // mv left == occupied
  memcpy(GetData() + GetFreeSpacePointer(), tuple.data_, tuple.size_);
  SetTupleOffset(i, GetFreeSpacePointer()); // bitmap
  SetTupleSize(i, tuple.size_); // bitmap, i == slot
  if (i == GetTupleCount()) {
    rid.Set(GetPageId(), i);
    SetTupleCount(GetTupleCount() + 1);
  }
  // write the log after set rid
  
  
  // 3. u() 4 tables w updated rid 
  if (ENABLE_LOGGING) {
    // TODO: add your logging logic here

    // acquire the exclusive lock
    // ?? 
    assert(lock_manager->LockExclusive(txn, rid.Get()));
    
    // u() WAL 
    LogRecord log_entry(txn->GetTransactionId(), txn->GetPrevLSN(), 
              LogRecordType::INSERT, rid, tuple);
    
    // no flush() to disk == job of bg flush thread (minimum IO)
    auto lsn = log_manager->AppendLogRecord(log_entry);

    // u() TT
    txn->SetPrevLSN(lsn);
    
    // u() buffer pool PageLSN
    SetLSN(lsn);

    // u() DPT if 1st u() ?? 


  }
  // LOG_DEBUG("Tuple inserted");
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
 * MarkDelete == shallow delete
 * - does not truly delete a tuple from table page
 * - only set tuple as 'deleted' w tuple size metadata to -ve
 * - no other txn can reuse this slot
 *
 */
bool TablePage::MarkDelete(const RID &rid, Transaction *txn,
                           LockManager *lock_manager, LogManager *log_manager) {
  
  
  // 1. r() bitmap (deleted tuple's size)
  int slot_num = rid.GetSlotNum();
  if (slot_num >= GetTupleCount()) {
    if (ENABLE_LOGGING) {
      txn->SetState(TransactionState::ABORTED);
    }
    return false;
  }

  int32_t tuple_size = GetTupleSize(slot_num);
  if (tuple_size < 0) {
    if (ENABLE_LOGGING) {
      txn->SetState(TransactionState::ABORTED);
    }
    return false;
  }



  // 2. log 
  if (ENABLE_LOGGING) {
    
    // acquire exclusive lock, if has shared lock
    // why ?? 
    if (txn->GetSharedLockSet()->find(rid) != txn->GetSharedLockSet()->end()) {
      if (!lock_manager->LockUpgrade(txn, rid))
        return false;
    } else if (txn->GetExclusiveLockSet()->find(rid) ==
                   txn->GetExclusiveLockSet()->end() &&
               !lock_manager->LockExclusive(txn, rid)) { // no shared lock
      return false;
    }
    // TODO: add your logging logic here

    // u() WAL 
    Tuple tuple;
    GetTuple(rid, tuple, txn, lock_manager);
    LogRecord log_entry(txn->GetTransactionId(), txn->GetPrevLSN(), 
              LogRecordType::MARKDELETE, rid, tuple);    
    auto lsn = log_manager->AppendLogRecord(log_entry);    
    txn->SetPrevLSN(lsn); // u() TT        
    SetLSN(lsn); // u() buffer pool PageLSN
    
  }

  
  // 3. u() bitmap only (no memmove)
  if (tuple_size > 0)
    SetTupleSize(slot_num, -tuple_size); // tuple size to -ve
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


/**
 * 
 * 
 * old_tuple == copy of old tuple, log in 
 */
bool TablePage::UpdateTuple(const Tuple &new_tuple, Tuple &old_tuple,
                            const RID &rid, Transaction *txn,
                            LockManager *lock_manager,
                            LogManager *log_manager) {
  
   
  // 1. r() bitmap (tuple's size + offset)
  int slot_num = rid.GetSlotNum();
  if (slot_num >= GetTupleCount()) { // rid not valid anymore
    if (ENABLE_LOGGING) {
      txn->SetState(TransactionState::ABORTED);
    }
    return false;
  }
  int32_t tuple_size = GetTupleSize(slot_num); // old tuple size
  if (tuple_size <= 0) { // tuple corrupted / not valid anymore
    if (ENABLE_LOGGING) {
      txn->SetState(TransactionState::ABORTED);
    }
    return false;
  }
  if (GetFreeSpaceSize() < new_tuple.size_ - tuple_size) {
    // should delete/insert because not enough space
    return false;
  }  
  int32_t tuple_offset =
      GetTupleOffset(slot_num); // the tuple offset of the old tuple
  
  


  // 2. replicate old tuple == for undo 
  old_tuple.size_ = tuple_size;
  if (old_tuple.allocated_)
    delete[] old_tuple.data_;
  old_tuple.data_ = new char[old_tuple.size_];
  memcpy(old_tuple.data_, GetData() + tuple_offset, old_tuple.size_);
  old_tuple.rid_ = rid;
  old_tuple.allocated_ = true;

  if (ENABLE_LOGGING) {
    // acquire exclusive lock
    // if has shared lock
    if (txn->GetSharedLockSet()->find(rid) != txn->GetSharedLockSet()->end()) {
      if (!lock_manager->LockUpgrade(txn, rid))
        return false;
    } else if (txn->GetExclusiveLockSet()->find(rid) ==
                   txn->GetExclusiveLockSet()->end() &&
               !lock_manager->LockExclusive(txn, rid)) { // no shared lock
      return false;
    }

    // TODO: add your logging logic here

    // u() 4 tables 
    // rid doesnt change, still same slot
    LogRecord(txn->GetTransactionId(), txn->GetPrevLSN(), LogRecordType::UPDATE,
              rid, old_tuple, new_tuple);
    auto lsn = log_manager->AppendLogRecord(log_entry);    
    txn->SetPrevLSN(lsn); // u() TT    
    SetLSN(lsn); // u() buffer pool PageLSN


  }


  // 3. memcpy + u() meta
  int32_t old_free_space_pointer = GetFreeSpacePointer();
  assert(tuple_offset >= old_free_space_pointer);
  // if new tuple bigger than old tuple == extend old tuple, vice sersa
  memmove(GetData() + old_free_space_pointer + tuple_size - new_tuple.size_,
          GetData() + old_free_space_pointer, tuple_offset - old_free_space_pointer);
  SetFreeSpacePointer(old_free_space_pointer + tuple_size - new_tuple.size_);
  
  // copy new tuple into old tuple's place (extended/shrinked)
  memcpy(GetData() + tuple_offset + tuple_size - new_tuple.size_,
         new_tuple.data_,
         new_tuple.size_);
  SetTupleSize(slot_num, new_tuple.size_); // new tuple size
  for (int i = 0; i < GetTupleCount();
       ++i) { // update tuple offsets (including the updated one)
    int32_t tuple_offset_i = GetTupleOffset(i);
    if (GetTupleSize(i) > 0 && tuple_offset_i < tuple_offset + tuple_size) {
      SetTupleOffset(i, tuple_offset_i + tuple_size - new_tuple.size_);
    }
  }
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
 * ApplyDelete == deep delete
 * - truly delete a tuple from table page
 * - called when a txn commits or 
 * - when you undo insert
 * 
 * NOTE: 
 * txn n log_manager == nullptr during recovery
 * 
 */
void TablePage::ApplyDelete(const RID &rid, Transaction *txn,
                            LogManager *log_manager) {
  
    
  // 1. r() bitmap (deleted tuple's size + offset)
  int slot_num = rid.GetSlotNum();
  assert(slot_num < GetTupleCount());  
  int32_t tuple_offset = GetTupleOffset(slot_num); // of the deleted tuple
  int32_t tuple_size = GetTupleSize(slot_num);
  if (tuple_size < 0) { // commit delete
    tuple_size = -tuple_size;
  } // else: rollback insert op


  // 2. replicate deleted tuple, save in log for undo 
  Tuple delete_tuple;
  delete_tuple.size_ = tuple_size;
  delete_tuple.data_ = new char[delete_tuple.size_];
  memcpy(delete_tuple.data_, GetData() + tuple_offset, delete_tuple.size_);
  delete_tuple.rid_ = rid;
  delete_tuple.allocated_ = true;

  
        
  // 3. log deleted tuple for undo 
  if (ENABLE_LOGGING) {
    // must already grab the exclusive lock
    assert(txn->GetExclusiveLockSet()->find(rid) !=
           txn->GetExclusiveLockSet()->end());
    // TODO: add your logging logic here

    // u() 4 tables 
    LogRecord(txn->GetTransactionId(), txn->GetPrevLSN(), LogRecordType::APPLYDELETE,
              rid, delete_tuple)
    auto lsn = log_manager->AppendLogRecord(log_entry);    
    txn->SetPrevLSN(lsn); // u() TT    
    SetLSN(lsn); // u() buffer pool PageLSN

  }

  
  // 4. deep delete tuple from page 
  // memmove + u() metadata
  int32_t free_space_pointer = GetFreeSpacePointer();
  assert(tuple_offset >= free_space_pointer);
  memmove(GetData() + free_space_pointer + tuple_size,
          GetData() + free_space_pointer, tuple_offset - free_space_pointer);
  SetFreeSpacePointer(free_space_pointer + tuple_size);
  SetTupleSize(slot_num, 0);
  SetTupleOffset(slot_num, 0); // invalid offset
  for (int i = 0; i < GetTupleCount(); ++i) { // loop bitmap
    int32_t tuple_offset_i = GetTupleOffset(i);
    if (GetTupleSize(i) != 0 && tuple_offset_i < tuple_offset) {
      SetTupleOffset(i, tuple_offset_i + tuple_size); // fix messed up offset
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
 * RollbackDelete == shallow add
 * - opposite of MarkDelete 
 * - flip the tuple size from -ve to +ve, visible again
 * - called when abort a transaction
 * 
 */
void TablePage::RollbackDelete(const RID &rid, Transaction *txn,
                               LogManager *log_manager) {
  
  // 1. log
  if (ENABLE_LOGGING) {
    // must have already grab the exclusive lock
    assert(txn->GetExclusiveLockSet()->find(rid) !=
           txn->GetExclusiveLockSet()->end());

    // TODO: add your logging logic here

    LogRecord log_entry(txn->GetTransactionId(), txn->GetPrevLSN(), 
              LogRecordType::ROLLBACKDELETE, rid, tuple);    
    auto lsn = log_manager->AppendLogRecord(log_entry);    
    txn->SetPrevLSN(lsn); // u() TT        
    SetLSN(lsn); // u() buffer pool PageLSN

  }

  // 2. r() bitmap (tuple's size)
  int slot_num = rid.GetSlotNum();
  assert(slot_num < GetTupleCount());
  int32_t tuple_size = GetTupleSize(slot_num);

  // 3. u() bitmap only (no memcopy) 
  if (tuple_size < 0)
    SetTupleSize(slot_num, -tuple_size); // tuple size to +ve
}


//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////



bool TablePage::GetTuple(const RID &rid, Tuple &tuple, Transaction *txn,
                         LockManager *lock_manager) {
  int slot_num = rid.GetSlotNum();
  if (slot_num >= GetTupleCount()) {
    if (ENABLE_LOGGING)
      txn->SetState(TransactionState::ABORTED);
    return false;
  }
  int32_t tuple_size = GetTupleSize(slot_num);
  if (tuple_size <= 0) {
    if (ENABLE_LOGGING)
      txn->SetState(TransactionState::ABORTED);
    return false;
  }

  if (ENABLE_LOGGING) {
    // acquire shared lock
    if (txn->GetExclusiveLockSet()->find(rid) ==
            txn->GetExclusiveLockSet()->end() &&
        txn->GetSharedLockSet()->find(rid) == txn->GetSharedLockSet()->end() &&
        !lock_manager->LockShared(txn, rid)) {
      return false;
    }
  }

  int32_t tuple_offset = GetTupleOffset(slot_num);
  tuple.size_ = tuple_size;
  if (tuple.allocated_)
    delete[] tuple.data_;
  tuple.data_ = new char[tuple.size_];
  memcpy(tuple.data_, GetData() + tuple_offset, tuple.size_);
  tuple.rid_ = rid;
  tuple.allocated_ = true;
  return true;
}


/**
 * Tuple iterator
 */
bool TablePage::GetFirstTupleRid(RID &first_rid) {
  for (int i = 0; i < GetTupleCount(); ++i) {
    if (GetTupleSize(i) > 0) { // valid tuple
      first_rid.Set(GetPageId(), i);
      return true;
    }
  }
  // there is no tuple within current page
  first_rid.Set(INVALID_PAGE_ID, -1);
  return false;
}

bool TablePage::GetNextTupleRid(const RID &cur_rid, RID &next_rid) {
  assert(cur_rid.GetPageId() == GetPageId());
  for (auto i = cur_rid.GetSlotNum() + 1; i < GetTupleCount(); ++i) {
    if (GetTupleSize(i) > 0) { // valid tuple
      next_rid.Set(GetPageId(), i);
      return true;
    }
  }
  return false; // End of last tuple
}

//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////



/* getter n setter, all table page header related */


int32_t TablePage::GetTupleOffset(int slot_num) {
  return *reinterpret_cast<int32_t *>(GetData() + 24 + 8 * slot_num);
}

int32_t TablePage::GetTupleSize(int slot_num) {
  return *reinterpret_cast<int32_t *>(GetData() + 28 + 8 * slot_num);
}

int32_t TablePage::GetTupleCount() { // max no. of tuples in 1 page
  return *reinterpret_cast<int32_t *>(GetData() + 20);
}

int32_t TablePage::GetFreeSpacePointer() {
  return *reinterpret_cast<int32_t *>(GetData() + 16);
}

int32_t TablePage::GetFreeSpaceSize() {
  return GetFreeSpacePointer() - 24 - GetTupleCount() * 8;
}

void TablePage::SetTupleOffset(int slot_num, int32_t offset) {
  memcpy(GetData() + 24 + 8 * slot_num, &offset, 4);
}

void TablePage::SetTupleSize(int slot_num, int32_t offset) {
  memcpy(GetData() + 28 + 8 * slot_num, &offset, 4);
}


void TablePage::SetFreeSpacePointer(int32_t free_space_pointer) {
  memcpy(GetData() + 16, &free_space_pointer, 4);
}


void TablePage::SetTupleCount(int32_t tuple_count) {
  memcpy(GetData() + 20, &tuple_count, 4);
}


} // namespace cmudb
