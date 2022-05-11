/**
 * lock_manager.h
 *
 * 
 * Tuple level lock manager, use wait-die to prevent deadlocks
 * modeled after 
 */

#pragma once

#include <condition_variable>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "common/rid.h"
#include "concurrency/transaction.h"

namespace cmudb {


enum class Mode {ERROR = -1, SHARED = 0, EXCLUSIVE = 1};
struct Request {
  explicit Request(txn_id_t tid, Mode mode) : 
  tid_(tid), mode_(mode) {}

  txn_id_t tid_;
  Mode mode_;
};

/**
 * 1 row of a lock rquest table 
 * - 3 columns (granted set, lock mode, wait queue)
 * 
 * 
 * Q&A
 * how to incorporate strict 2PL ??
 */
class Row {

public:

  // init
  Row(Request lock_req){
    wait_queue_.push_back(lock_req);
  }
  
  // wait die: 
  txn_id_t GetOldestWaitQueue(){
    return wait_queue_.begin().tid_;
  };

  
  /**
   * 
   * 2PL == T1 gets all lock first, T2 gets only after T1 release
   * ordered by txn id NOT arrival time 
   * wait die == only oldest is added == always add front
   */
  void AddFrontWaitQueue(Request lock_req){
    wait_queue_.push_front(lock_req);
  };


  bool DeleteWaitQueue(txn_id_t tid){
    for (auto i = wait_queue_.begin(); i !=wait_queue_.end(); ++i){
      if(i->tid_ == tid){
        wait_queue_.erase(i); 
        return true; // successfully deleted 
      }
    }
    return false; // not found
  };

  // granted set related
  bool WaitQueueEmpty(){return wait_queue_.empty()};
  bool GrantedSetEmpty(){return granted_set_.empty()};
  bool UpgradeWaitQueueEmpty(){return upgrade_wait_queue_.empty()};

  
  // TRICKY CASE !!!! 2PL conflicts w wait die
  // if T3 already granted, but T1, T2 arrived late, then T3 lock()
  // wait die: T3 dies, but then deadlock since 2PL
  // T3 wont release lock unless T3 gets all the lock 
  // thic func == for T3 to get 1st in wait queue to get the lock
  // 
  // what if upgrade ?? 
  bool IsTxnInGrantedSet(txn_id_t tid){ 
    for (Request* i = granted_set_.begin(); i !=granted_set_.end(); ++i){
      if(i->tid_ == tid){
        return true; // T3 gets 1st in wait queue since T3 in grant set
      }
    }
    return false; // kill T3  
  };


  // for upgrade()
  bool IsOnlyTxnInGrantedSet(txn_id_t tid){     
    if(IsTxnInGrantedSet(tid) && granted_set_.size()== 1 ){
      return true;
    }
    return false;

  };



  // for upgrade()
  bool IsTxnInGrantedSet(txn_id_t tid){ 
    for (Request* i = granted_set_.begin(); i !=granted_set_.end(); ++i){
      if(i->tid_ == tid && i->mode_ == Mode::SHARED){
        return true; 
      }
    }
    return false; 
  };

  bool UpgradeGrantedSet(txn_id_t tid){ 
    for (Request* i = granted_set_.begin(); i !=granted_set_.end(); ++i){
      if(i->tid_ == tid && i->mode_ == Mode::SHARED){
        i->mode_ = Mode::EXCLUSIVE; 
      }
    }
    return false; 
  };
  
  bool CouldGrantSharedLock(){
    assert(!granted_set_.empty());
    return granted_set_.begin()->mode == Mode::SHARED;
  };


  // for unlock garbage collecting
  Request GetGrantedSet(txn_id_t tid){
    for (Request* i = granted_set_.begin(); i !=granted_set_.end(); ++i){
      if(i->tid_ == tid){
        return *i; 
      }
    }
    return Request(Mode::ERROR, Mode::ERROR);
  };

  void AddGrantedSet(Request lock_req){
    granted_set_.push_back(lock_req);
  };

  bool DeleteGrantedSet(txn_id_t tid){
    for (auto i = granted_set_.begin(); i !=granted_set_.end(); ++i){
      if(i->tid_ == tid){
        granted_set_.erase(i); 
        return true;
      }
    }
    return false; // not found
  };



private:
  // TODO: lock !!!!!!!!

  /* 3 columns of lock request table */
  std::list<Request> granted_set_;  
  std::list<Request> wait_queue_;
  std::list<txn_id_t> upgrade_wait_queue_; // mode must be x

};





/**
 * 
 */
class LockManager {

public:
  LockManager(bool strict_2PL) : strict_2PL_(strict_2PL){};

  /*** below are APIs need to implement ***/
  // lock:
  // return false if transaction is aborted
  // it should be blocked on waiting and should return true when granted
  // note the behavior of trying to lock locked rids by same txn is undefined
  // it is transaction's job to keep track of its current locks
  bool LockShared(Transaction *txn, const RID &rid);
  bool LockExclusive(Transaction *txn, const RID &rid);
  bool LockUpgrade(Transaction *txn, const RID &rid);

  // unlock:
  // release the lock hold by the txn
  bool Unlock(Transaction *txn, const RID &rid);
  /*** END OF APIs ***/

  


private:
  bool strict_2PL_;

  // {rid : {granted_set, s/x, wait_queue[{txn : s/x}]}}
  std::unordered_map<RID, std::unique_ptr<Row>> lock_table_; 
  std::mutex lock_table_lock_;
  std::condition_variable cv_wait_queue_;
  std::condition_variable cv_grant_set_;  





};

} // namespace cmudb

