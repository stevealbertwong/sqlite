/**
 * lock_manager.cpp
 * 
 * - u() txn state (grow, shrink, commit, abort)
 * 
 * 
 * 
 * lock_manager.cc VS b+tree.cc VS thread.cc 


r w lock == var == syscall lock() + cv / signal / wait / notify_all 
r w latch == pages in b+tree == r w lock +  txn_manger’s pages_it_touches + buffer_pool_manger’s pinned_pages 
lock_shared == pages in txn == r w lock +  txn_manger’s state of txn + lock_table

 * 
 * - u() txn state 
 * 
 * when lock req arrives:
 * 1. see if any other txn holding the lock
 * 2. if no, create entry in lock request table, grant the lock
 * 3. 
 * 
 * 
 * NOTE: 
 * assumes lock req arrives in 2PL pattern
 * - 2PL: txn will request n wait for all locks it needs first, before unlocking any
 * - gurantees conflict serialbility
 * 
 * 
 * incorporate 2PL n wait die into lock manager: 
 * 2PL 
 * - wait queue ordered and grouped by txn_id 
 * - T1 gets all locks first, T2 gets only after T1 release
 * 
 * wait die 
 * - old wait for young
 * - young kill itself if wait for old 
 * - txn id == start time == priorities
 * - wait die only kill xlock ?? 
 * 
 * cv
 * - no need to notify other txns u granted lock
 * - only need to notify when unlock()
 * 
 * 1 wait queue == 1 txn can only have 1 entry at most
 * - 2PL == for each record id, 1 txn calls lock() 1 time
 * - for lock_upgrade(), txn must hv slock in grant set already
 * 
 * 
 * TODO: txn manager s/xlock set 
 */

#include "concurrency/lock_manager.h"

namespace cmudb {

/**
 * @brief 
 * 
 * 
 * txn == e.g. txn1, txn2, txn3 
 * rid == e.g. rid1, rid2, rid3
 * 
 * 
 * @return 
 * whether granted lock
 * 
 */
bool LockManager::LockShared(Transaction *txn, const RID &rid) {


  // 0. 2PL checking condition
  // 2PL gurantees txn will call all lock() before unlock()
  assert(txn->GetState() == TransactionState::GROWING); 
  assert(!txn->GetState() == TransactionState::ABORTED);


  // 1. wait die + lock table to decide if: 
  // 1. txn gets lock, 2. wait for lock or 3. kill itself
  // 1.1 if strict 2PL == waits til txn is committed
  auto row = lock_table_[rid];
  txn_id_t tid = txn->GetTransactionId();
  
  Request lock_req(tid, Mode::SHARED);

  // 1.2.1 
  // if empty or grant set only has shared lock, grant 
  // newly arrived, even if younger under wait die, 
  // as long as shared mode + shared req == grant shared lock
  if(row->GrantedSetEmpty() || row->CouldGrantSharedLock()){    
    row.AddGrantedSet(lock_req);
    return true;

  // 1.2.2
  } else { // grant set has exclusive lock == wait or die


    // 1.2.2.1: wait
    /**
    if txn is older than oldest in existing txns == wait
    wait == append “lock request table” 
    wait die == only older than oldest wait == younger than older all die
    multithreads == older might arrive late == possible   
    if txn is younger than oldest in wait queue == die
    shouldnt it be any txn in queue == die since not oldest ??    
     */
    
    // txn older/same as oldest in wait queue
    if(tid <= row->GetOldestWaitQueue() 
        || row->TxnInGrantedSet(tid)
        || row->WaitQueueEmpty()) { 
      row->AddFrontWaitQueue(lock_req);
      
      // block + check condition every time when other txn unlock()
      // cv unblocks to get lock (lock table) until lock (lock manager) granted
      cv_wait_queue_.wait(lock_table_lock_, [&]()){
        // condition: no txn in grant set + 1st of wait queue
        if (row->GrantedSetEmpty() && wait_queue->Begin()->tid_ == tid){
          return true;
        } 
        return false;
      }

      row->AddGrantedSet(lock_req);
      row->DeleteWaitqueue(lock_req);
      return true;
    
    // 1.2.2.2: die 
    } else { // txn not older than oldest in wait queue
      txn->SetState(TransactionState::ABORTED);
      return false;
    }
  }

  return false;
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
 * 
 * 
 * how is it different from lock_shared() ??
 * - lock_s() == granted slock of any txn == grant lock
 * - lock_x() == granted slock of same txn == 1st in wait queue
 * 
 */
bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {


  // 1. wait die to decide if txn gets lock, wait for lock or kill itself

  // 1.1 no txn, must be able to get xlock

  // 1.2 wait die == dealing w deadlock == only oldest txn than waiting queue cna get xlock



  // 2. cv wait til all previous lock requests are granted



  // 3. u() txn granted xlock + notify other threads

  return false;
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
 * upgrade() == xlock() 
 * - but txn needs to have a slock in grant set first
 * - n wait til only u 1 slock left in grant set
 * 
 * 
 * Q&A
 * can be called by both slock in grant set n wait queue ??
 * - see test
 * - called only by slock in grant set 
 * - since LockUpgrade() must after LockShared()
 * - n LockShared() wont return unless gets lock 
 * 
 * why once slock -> xlock in wait queue == wait die ??
 * 
 * why wait die ?
 * - 1+ slock txns in grant set can call upgrade()
 * - deadlock == since there can only be 1 xlock,
 * - and no txns will unlock() before they done upgrade()
 * - wait die == T1, T2 slock grant set, if both upgrade()
 * - then T2 dies, T1 directly upgrades() in grant set
 * - T2 forced to unlock() w/o upgrade() == no deadlock
 * 
 * slock() waitdie vs upgrade() waitdie
 * - upgrade(): T1 cv waits on "only T1 in grant set", in upgrade_wait_queue_
 * - slock(): other txns cv wait on "grant set empty", in wait_queue_
 * 
 */
bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {

  // 0.
  auto row = lock_table_[rid];
  txn_id_t tid = txn->GetTransactionId();
  if(!row->TxnInGrantedSet(tid)){return false;}

  
  // 1. if only 1 txn in grant set, auto upgrade to xlock
  if(IsOnlyTxnInGrantedSet(tid)){
    // T1 slock -> xlock (in grant set)
    row->UpgradeGrantedSet(tid);
    return true; 
  }

  // 2. if 1+ txns == wait die
  
  // 2.1. wait 
  if(row->UpgradeWaitQueueEmpty() || tid <= upgrade_wait_queue_[0] ){
    // cv block til only T1 in grant set
    cv_grant_set_.wait(lock_table_lock_, [&]()){
      return row->IsOnlyTxnInGrantedSet(tid);
    }

    // T1 slock -> xlock (in grant set)
    row->UpgradeGrantedSet(tid);
    return true;

  // 2.2. die 
  } else {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  
}




/**
 * @brief 
 * 
 * txn state: grow -> shrink
 * 
 * Q&A:
 * how to ensure txn got all locks (2PL) ?? 
 * - txn manager once got signal from last lock() call
 * - txn manager then u() to shrinking state
 * 
 * once granted set changes from x to s
 * all slock in wait queue auto granted slock ??
 * - i dont think so ??
 * 
 */
bool LockManager::Unlock(Transaction *txn, const RID &rid) {

  // 0. 
  auto row = lock_table_[rid];
  txn_id_t tid = txn->GetTransactionId();  
  if(!IsTxnInGrantedSet(tid)){return false;}


  // 1. if strict 2pl
  // txn must "commit" or "abort" before it calls unlock()


  // 2. if vanilla 2pl 

  // 2.1 u() txn manager 
  txn->SetState(TransactionState::SHRINKING);
  if(row->GetGrantedSet(tid).mode_ == LockMode::SHARED){
    txn->GetSharedLockSet()->erase(rid);
  }else{
    txn->GetExclusiveLockSet()->erase(rid);
  }

  // 2.2 u() lock table  
  row->DeleteGrantedSet(tid);

  
  // 3. notify txns in wait queue + grant set 
  cv_wait_queue_.notify_all();
  cv_grant_set_.notify_all();



  return false;
}

} // namespace cmudb
