/**
 * lock_manager_test.cpp
 * 
 * 
 * 1 txn == 1 thread
 * 
 */

#include <thread>

#include "concurrency/transaction_manager.h"
#include "gtest/gtest.h"

namespace cmudb {

// std::thread is movable
class scoped_guard {
  std::thread t;
public:
  explicit scoped_guard(std::thread t_) : t(std::move(t_)) {
    if (!t.joinable()) {
      throw std::logic_error("No thread");
    }
  }
  ~scoped_guard() {
    t.join();
  }
  scoped_guard(const scoped_guard &) = delete;
  scoped_guard &operator=(const scoped_guard &)= delete;
};


//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////


/*
 * This test is only a sanity check. Please do not rely on this test
 * to check the correctness.
 */
TEST(LockManagerTest, BasicTest) {
  LockManager lock_mgr{false};
  TransactionManager txn_mgr{&lock_mgr};
  RID rid{0, 0};

  std::thread t0([&] {
    Transaction txn(0);
    bool res = lock_mgr.LockShared(&txn, rid);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
    txn_mgr.Commit(&txn);
    EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
  });

  std::thread t1([&] {
    Transaction txn(1);
    bool res = lock_mgr.LockShared(&txn, rid);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
    txn_mgr.Commit(&txn);
    EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
  });

  t0.join();
  t1.join();
}


//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////



TEST(LockManagerTest, BasicShareTest) {
  LockManager lock_mgr{false};
  TransactionManager txn_mgr{&lock_mgr};
  RID rid{0, 0};

  std::thread t0([&] {
    Transaction txn(0);
    bool res = lock_mgr.LockShared(&txn, rid);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
    txn_mgr.Commit(&txn);
    EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
  });

  std::thread t1([&] {
    Transaction txn(1);

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    bool res = lock_mgr.LockShared(&txn, rid);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
    txn_mgr.Commit(&txn);
    EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
  });

  std::thread t2([&] {
    Transaction txn(2);
    bool res = lock_mgr.LockShared(&txn, rid);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
    txn_mgr.Commit(&txn);
    EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
  });

  std::thread t3([&] {
    Transaction txn(3);

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    bool res = lock_mgr.LockShared(&txn, rid);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
    txn_mgr.Commit(&txn);
    EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
  });

  std::thread t4([&] {
    Transaction txn(4);
    bool res = lock_mgr.LockShared(&txn, rid);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
    txn_mgr.Commit(&txn);
    EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
  });

  std::thread t5([&] {
    Transaction txn(5);

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    bool res = lock_mgr.LockShared(&txn, rid);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
    txn_mgr.Commit(&txn);
    EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
  });

  std::thread t6([&] {
    Transaction txn(6);
    bool res = lock_mgr.LockShared(&txn, rid);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
    txn_mgr.Commit(&txn);
    EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
  });

  t0.join();
  t1.join();
  t2.join();
  t3.join();
  t4.join();
  t5.join();
  t6.join();
}

//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////



TEST(LockManagerTest, BasicExclusiveTest) {
  LockManager lock_mgr{false};
  TransactionManager txn_mgr{&lock_mgr};
  RID rid{0, 0};

  std::promise<void> go, p0, p1, p2;
  std::shared_future<void> ready(go.get_future());

  std::thread t0([&, ready] {
    Transaction txn(5);
    bool res = lock_mgr.LockExclusive(&txn, rid); // x lock

    p0.set_value();
    ready.wait();

    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    res = lock_mgr.Unlock(&txn, rid);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
  });

  std::thread t1([&, ready] {
    Transaction txn(3);

    p1.set_value();
    ready.wait();

    //std::this_thread::sleep_for(std::chrono::milliseconds(30));
    bool res = lock_mgr.LockShared(&txn, rid); // s lock

    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

    res = lock_mgr.Unlock(&txn, rid);

    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);

  });

  std::thread t2([&, ready] {
    Transaction txn(1);

    p2.set_value();
    ready.wait();

    // wait for t1
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    bool res = lock_mgr.LockShared(&txn, rid);

    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

    res = lock_mgr.Unlock(&txn, rid);

    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);

  });

  p0.get_future().wait();
  p1.get_future().wait();
  p2.get_future().wait();

  go.set_value();

  t0.join();
  t1.join();
  t2.join();
}


//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////


TEST(LockManagerTest, BasicUpdateTest) {
  LockManager lock_mgr{false};
  TransactionManager txn_mgr{&lock_mgr};
  RID rid{0, 0};

  std::promise<void> go, p0, p1, p2, p3;
  std::shared_future<void> ready(go.get_future());

  std::thread t0([&, ready] {
    Transaction txn(0);
    
    // txn must gotten slock in grant set before this unblocks
    // if txn in wait queue, 
    bool res = lock_mgr.LockShared(&txn, rid);

    p0.set_value();
    ready.wait();

    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

    // wont hit here until txn gets slock in grant set
    res = lock_mgr.LockUpgrade(&txn, rid);

    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

    res = lock_mgr.Unlock(&txn, rid);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
  });

  std::thread t1([&, ready] {
    Transaction txn(1);

    bool res = lock_mgr.LockShared(&txn, rid);

    p1.set_value();
    ready.wait();

    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    res = lock_mgr.Unlock(&txn, rid);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
  });

  std::thread t2([&, ready] {
    Transaction txn(2);
    bool res = lock_mgr.LockShared(&txn, rid);

    p2.set_value();
    ready.wait();

    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

    res = lock_mgr.Unlock(&txn, rid);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
  });

  std::thread t3([&, ready] {
    Transaction txn(3);
    bool res = lock_mgr.LockShared(&txn, rid);

    p3.set_value();
    ready.wait();

    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    res = lock_mgr.Unlock(&txn, rid);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
  });

  p0.get_future().wait();
  p1.get_future().wait();
  p2.get_future().wait();
  p3.get_future().wait();

  go.set_value();

  t0.join();
  t1.join();
  t2.join();
  t3.join();
}

//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////

TEST(LockManagerTest, BasicTest1) {
  LockManager lock_mgr{false};
  TransactionManager txn_mgr{&lock_mgr};
  RID rid{0, 0};

  std::promise<void> go, t0, t1, t2;
  std::shared_future<void> ready(go.get_future());

  std::thread thread0([&, ready] {
    Transaction txn(2);

    // will block and can wait
    bool res = lock_mgr.LockShared(&txn, rid);

    t0.set_value();
    ready.wait();

    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // unlock
    res = lock_mgr.Unlock(&txn, rid);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
  });

  std::thread thread1([&, ready] {
    Transaction txn(1);

    // will block and can wait
    bool res = lock_mgr.LockShared(&txn, rid);

    t1.set_value();
    ready.wait();

    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // unlock
    res = lock_mgr.Unlock(&txn, rid);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
  });

  std::thread thread2([&, ready] {
    Transaction txn(0);

    t2.set_value();
    ready.wait();

    // can wait and will block
    bool res = lock_mgr.LockExclusive(&txn, rid);

    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

    res = lock_mgr.Unlock(&txn, rid);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
  });

  t0.get_future().wait();
  t1.get_future().wait();
  t2.get_future().wait();

  // go!
  go.set_value();

  thread0.join();
  thread1.join();
  thread2.join();
}

//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////

TEST(LockManagerTest, BasicTest2) {
  LockManager lock_mgr{false};
  TransactionManager txn_mgr{&lock_mgr};
  RID rid{0, 0};

  std::promise<void> go, t0, t1, t2;
  std::shared_future<void> ready(go.get_future());


  // t1 
  std::thread thread0([&, ready] {
    Transaction txn(0);

    t0.set_value();
    ready.wait();

    // let thread1 try to acquire shared lock first
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // will block and can wait
    bool res = lock_mgr.LockShared(&txn, rid);

    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

    // unlock
    res = lock_mgr.Unlock(&txn, rid);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
  });


  // t2 
  std::thread thread1([&, ready] {
    Transaction txn(1);

    t1.set_value();
    ready.wait();

    // will block and can wait
    bool res = lock_mgr.LockShared(&txn, rid);

    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);


    // unlock
    res = lock_mgr.Unlock(&txn, rid);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
  });


  // t3
  std::thread thread2([&, ready] {
    Transaction txn(2);

    // can wait and will block
    bool res = lock_mgr.LockExclusive(&txn, rid);

    t2.set_value();
    ready.wait();

    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    res = lock_mgr.Unlock(&txn, rid);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
  });

  t0.get_future().wait();
  t1.get_future().wait();
  t2.get_future().wait();

  // go!
  go.set_value();

  thread0.join();
  thread1.join();
  thread2.join();
}


//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////


// basic wait-die test
// why tho ??
TEST(LockManagerTest, DeadlockTest1) {
  LockManager lock_mgr{false};
  TransactionManager txn_mgr{&lock_mgr};
  RID rid{0, 0};

  std::promise<void> go, go2, t1, t2;
  std::shared_future<void> ready(go.get_future());

  std::thread thread0([&, ready] {
    Transaction txn(0);
    bool res = lock_mgr.LockShared(&txn, rid);

    t1.set_value();
    ready.wait();

    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

    // waiting thread2 call LockExclusive before unlock
    go2.get_future().wait();

    // unlock
    res = lock_mgr.Unlock(&txn, rid);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
  });

  std::thread thread1([&, ready] {
    Transaction txn(1);

    // wait thread t0 to get shared lock first
    t2.set_value();
    ready.wait();

    bool res = lock_mgr.LockExclusive(&txn, rid);
    go2.set_value();

    EXPECT_EQ(res, false);
    EXPECT_EQ(txn.GetState(), TransactionState::ABORTED);
  });

  t1.get_future().wait();
  t2.get_future().wait();

  // go!
  go.set_value();

  thread0.join();
  thread1.join();
}



//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////

TEST(LockManagerTest, DeadlockTest2) {
  LockManager lock_mgr{false};
  TransactionManager txn_mgr{&lock_mgr};
  RID rid{0, 0};

  std::promise<void> go, t1, t2;
  std::shared_future<void> ready(go.get_future());

  std::thread thread0([&, ready] {
    Transaction txn(0);

    t1.set_value();
    ready.wait();

    // will block and can wait
    bool res = lock_mgr.LockShared(&txn, rid);

    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

    // unlock
    res = lock_mgr.Unlock(&txn, rid);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
  });

  std::thread thread1([&, ready] {
    Transaction txn(1);

    bool res = lock_mgr.LockExclusive(&txn, rid);

    t2.set_value();
    ready.wait();

    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

    res = lock_mgr.Unlock(&txn, rid);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
  });

  t1.get_future().wait();
  t2.get_future().wait();

  // go!
  go.set_value();

  thread0.join();
  thread1.join();
}


//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////

TEST(LockManagerTest, DeadlockTest3) {
  LockManager lock_mgr{false};
  TransactionManager txn_mgr{&lock_mgr};
  RID rid{0, 0};
  RID rid2{0, 1};

  std::promise<void> go, t1, t2;
  std::shared_future<void> ready(go.get_future());

  std::thread thread0([&, ready] {
    Transaction txn(0);

    // try get exclusive lock on rid2, will succeed
    bool res = lock_mgr.LockExclusive(&txn, rid2);

    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

    t1.set_value();
    ready.wait();

    // will block and can wait
    res = lock_mgr.LockShared(&txn, rid);

    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

    // unlock rid1
    res = lock_mgr.Unlock(&txn, rid);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);

    // unblock rid2
    res = lock_mgr.Unlock(&txn, rid2);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
  });

  std::thread thread1([&, ready] {
    Transaction txn(1);

    // try to get shared lock on rid, will succeed
    bool res = lock_mgr.LockExclusive(&txn, rid);

    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

    t2.set_value();
    ready.wait();

    res = lock_mgr.LockShared(&txn, rid2);

    EXPECT_EQ(res, false);
    EXPECT_EQ(txn.GetState(), TransactionState::ABORTED);

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // unlock rid
    res = lock_mgr.Unlock(&txn, rid);

    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::ABORTED);
  });

  t1.get_future().wait();
  t2.get_future().wait();

  // go!
  go.set_value();

  thread0.join();
  thread1.join();
}


} // namespace cmudb




























































//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////

void CheckGrowing(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::GROWING); }

void CheckShrinking(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::SHRINKING); }

void CheckAborted(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::ABORTED); }

void CheckCommitted(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::COMMITTED); }

void CheckTxnLockSize(Transaction *txn, size_t shared_size, size_t exclusive_size) {
  EXPECT_EQ(txn->GetSharedLockSet()->size(), shared_size);
  EXPECT_EQ(txn->GetExclusiveLockSet()->size(), exclusive_size);
}


//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////

// Basic shared lock test under REPEATABLE_READ
void BasicTest1() {
  LockManager lock_mgr{false};
  TransactionManager txn_mgr{&lock_mgr};

  std::vector<RID> rids; // record id == disk address == page id, offset/slot
  std::vector<Transaction *> txns;


  int num_rids = 10;
  for (int i = 0; i < num_rids; i++) {
    RID rid{i, static_cast<uint32_t>(i)};
    rids.push_back(rid);
    txns.push_back(txn_mgr.Begin());
    EXPECT_EQ(i, txns[i]->GetTransactionId());
  }

  // 10 threads, for each 1 txn/thread, 10 rids == 100 slocks
  // 1. setting out what each thread is going to do 
  // slock() -> unlock() -> commit
  auto task = [&](int txn_id) {
    bool res;
    for (const RID &rid : rids) {
      res = lock_mgr.LockShared(txns[txn_id], rid);
      EXPECT_TRUE(res);
      CheckGrowing(txns[txn_id]);
    }
    CheckTxnLockSize(txns[txn_id], num_rids, 0); // slock size, xlock size

    for (const RID &rid : rids) {
      res = lock_mgr.Unlock(txns[txn_id], rid);
      EXPECT_TRUE(res);
      CheckShrinking(txns[txn_id]);
    }
    CheckTxnLockSize(txns[txn_id], 0, 0);

    txn_mgr.Commit(txns[txn_id]);
    CheckCommitted(txns[txn_id]);
  };


  // 2. spawn 10 threads / txns
  std::vector<std::thread> threads;
  threads.reserve(num_rids);

  for (int i = 0; i < num_rids; i++) { // 10
    threads.emplace_back(std::thread{task, i});
  }

  for (int i = 0; i < num_rids; i++) {
    threads[i].join();
  }

  for (int i = 0; i < num_rids; i++) {
    delete txns[i];
  }
}
TEST(LockManagerTest, BasicTest1) { BasicTest1(); }


//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////

// Basic shared lock test under READ_COMMITTED (IsolationLevel)
void BasicTest2() {
  LockManager lock_mgr{false};
  TransactionManager txn_mgr{&lock_mgr};

  std::vector<RID> rids;
  std::vector<Transaction *> txns;

  int num_rids = 10;
  for (int i = 0; i < num_rids; i++) {
    RID rid{i, static_cast<uint32_t>(i)};
    rids.push_back(rid);
    // ONLY DIFFERENCE: Isolation Level
    txns.push_back(txn_mgr.Begin(nullptr, IsolationLevel::READ_COMMITTED));
    EXPECT_EQ(i, txns[i]->GetTransactionId());
  }

  
  // 10 threads, for each 1 txn/thread, 10 rids == 100 slocks
  // 1. setting out what each thread is going to do 
  // slock() -> unlock() -> commit
  auto task = [&](int txn_id) {
    bool res;
    for (const RID &rid : rids) {
      res = lock_mgr.LockShared(txns[txn_id], rid); // == read lock
      EXPECT_TRUE(res);
      CheckGrowing(txns[txn_id]);
    }
    CheckTxnLockSize(txns[txn_id], num_rids, 0);

    for (const RID &rid : rids) {
      res = lock_mgr.Unlock(txns[txn_id], rid);
      EXPECT_TRUE(res);
      CheckGrowing(txns[txn_id]);
    }
    CheckTxnLockSize(txns[txn_id], 0, 0);

    txn_mgr.Commit(txns[txn_id]);
    CheckCommitted(txns[txn_id]);
  };

  
  // 2. spawn 10 threads / txns
  std::vector<std::thread> threads;
  threads.reserve(num_rids);
  for (int i = 0; i < num_rids; i++) {
    threads.emplace_back(std::thread{task, i});
  }

  for (int i = 0; i < num_rids; i++) {
    threads[i].join();
  }

  for (int i = 0; i < num_rids; i++) {
    delete txns[i];
  }
}
TEST(LockManagerTest, BasicTest2) { BasicTest2(); }



//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////


void TwoPLTest() {
  TransactionManager txn_mgr{&lock_mgr};
  LockManager lock_mgr{false};
  RID rid0{0, 0};
  RID rid1{0, 1};

  auto txn = txn_mgr.Begin();
  EXPECT_EQ(0, txn->GetTransactionId());

  
  // 
  bool res;
  res = lock_mgr.LockShared(txn, rid0); // 
  EXPECT_TRUE(res);
  CheckGrowing(txn);
  CheckTxnLockSize(txn, 1, 0);

  res = lock_mgr.LockExclusive(txn, rid1); // 
  EXPECT_TRUE(res);
  CheckGrowing(txn);
  CheckTxnLockSize(txn, 1, 1); // slock, xlock

  res = lock_mgr.Unlock(txn, rid0); // 
  EXPECT_TRUE(res);
  CheckShrinking(txn);
  CheckTxnLockSize(txn, 0, 1);


  // 2PL check
  // == once unlock, cannot lock again
  try {
    lock_mgr.LockShared(txn, rid0);    
    EXPECT_TRUE(false); // should not execute here
    CheckAborted(txn); // not yet abort ??    
    CheckTxnLockSize(txn, 0, 1); // slock, xlock. Size shouldn't change here
  } catch (TransactionAbortException &e) {
    // std::cout << e.GetInfo() << std::endl;
    CheckAborted(txn);    
    CheckTxnLockSize(txn, 0, 1); // Size shouldn't change here
  }

  // Need to call txn_mgr's abort
  txn_mgr.Abort(txn);
  CheckAborted(txn); // only abort until now ?? 
  CheckTxnLockSize(txn, 0, 0);

  delete txn;
}
TEST(LockManagerTest, TwoPLTest) { TwoPLTest(); }


//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////

void UpgradeTest() {
  LockManager lock_mgr{false};
  TransactionManager txn_mgr{&lock_mgr};
  RID rid{0, 0};
  Transaction txn(0);
  txn_mgr.Begin(&txn);

  bool res = lock_mgr.LockShared(&txn, rid);
  EXPECT_TRUE(res);
  CheckTxnLockSize(&txn, 1, 0);
  CheckGrowing(&txn);

  // upgrade == slock -> xlock
  res = lock_mgr.LockUpgrade(&txn, rid); // only difference
  EXPECT_TRUE(res);
  CheckTxnLockSize(&txn, 0, 1);
  CheckGrowing(&txn);

  res = lock_mgr.Unlock(&txn, rid);
  EXPECT_TRUE(res);
  CheckTxnLockSize(&txn, 0, 0);
  CheckShrinking(&txn);

  txn_mgr.Commit(&txn);
  CheckCommitted(&txn);
}
TEST(LockManagerTest, UpgradeLockTest) { UpgradeTest(); }


//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////


TEST(LockManagerTest, GraphEdgeTest) {
  LockManager lock_mgr{false};
  TransactionManager txn_mgr{&lock_mgr};
  const int num_nodes = 100;
  const int num_edges = num_nodes / 2;
  const int seed = 15445;
  std::srand(seed);

  // Create txn ids and shuffle
  std::vector<txn_id_t> txn_ids;
  txn_ids.reserve(num_nodes);
  for (int i = 0; i < num_nodes; i++) {
    txn_ids.emplace_back(i);
  }
  EXPECT_EQ(num_nodes, txn_ids.size());
  auto rng = std::default_random_engine{};
  std::shuffle(std::begin(txn_ids), std::end(txn_ids), rng);
  EXPECT_EQ(num_nodes, txn_ids.size());

  // Create edges by pairing adjacent txn_ids
  std::vector<std::pair<txn_id_t, txn_id_t>> edges;
  for (int i = 0; i < num_nodes; i += 2) {
    EXPECT_EQ(i / 2, lock_mgr.GetEdgeList().size());
    auto t1 = txn_ids[i];
    auto t2 = txn_ids[i + 1];
    lock_mgr.AddEdge(t1, t2);
    edges.emplace_back(t1, t2);
    EXPECT_EQ((i / 2) + 1, lock_mgr.GetEdgeList().size());
  }

  auto lock_mgr_edges = lock_mgr.GetEdgeList();
  EXPECT_EQ(num_edges, lock_mgr_edges.size());
  EXPECT_EQ(num_edges, edges.size());

  std::sort(lock_mgr_edges.begin(), lock_mgr_edges.end());
  std::sort(edges.begin(), edges.end());

  for (int i = 0; i < num_edges; i++) {
    EXPECT_EQ(edges[i], lock_mgr_edges[i]);
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


TEST(LockManagerTest, BasicCycleTest) {
  /*** Create 0->1->0 cycle ***/
  {
    LockManager lock_mgr{false};
    TransactionManager txn_mgr{&lock_mgr};

    lock_mgr.AddEdge(0, 1);
    lock_mgr.AddEdge(1, 0);
    EXPECT_EQ(2, lock_mgr.GetEdgeList().size());

    txn_id_t txn;
    EXPECT_EQ(true, lock_mgr.HasCycle(&txn));
    EXPECT_EQ(1, txn);

    lock_mgr.RemoveEdge(1, 0);
    EXPECT_EQ(false, lock_mgr.HasCycle(&txn));
  }

  // 0->1->2->0 cycle
  {
    LockManager lock_mgr{false};
    TransactionManager txn_mgr{&lock_mgr};

    lock_mgr.AddEdge(0, 1);
    lock_mgr.AddEdge(1, 2);
    lock_mgr.AddEdge(2, 0);

    EXPECT_EQ(3, lock_mgr.GetEdgeList().size());

    txn_id_t txn;
    EXPECT_EQ(true, lock_mgr.HasCycle(&txn));
    EXPECT_EQ(2, txn);

    lock_mgr.RemoveEdge(1, 2);
    EXPECT_EQ(false, lock_mgr.HasCycle(&txn));
  }

  // 0->1->2->0 cycle, 3->0
  {
    LockManager lock_mgr{false};
    TransactionManager txn_mgr{&lock_mgr};

    lock_mgr.AddEdge(0, 1);
    lock_mgr.AddEdge(1, 2);
    lock_mgr.AddEdge(2, 0);
    lock_mgr.AddEdge(3, 0);

    EXPECT_EQ(4, lock_mgr.GetEdgeList().size());

    txn_id_t txn;
    EXPECT_EQ(true, lock_mgr.HasCycle(&txn));
    EXPECT_EQ(2, txn);

    lock_mgr.RemoveEdge(1, 2);
    EXPECT_EQ(false, lock_mgr.HasCycle(&txn));
  }

  // 0->1->2->0 cycle, 2->3
  {
    LockManager lock_mgr{false};
    TransactionManager txn_mgr{&lock_mgr};

    lock_mgr.AddEdge(0, 1);
    lock_mgr.AddEdge(1, 2);
    lock_mgr.AddEdge(2, 0);
    lock_mgr.AddEdge(2, 3);

    EXPECT_EQ(4, lock_mgr.GetEdgeList().size());

    txn_id_t txn;
    EXPECT_EQ(true, lock_mgr.HasCycle(&txn));
    EXPECT_EQ(2, txn);

    lock_mgr.RemoveEdge(1, 2);
    EXPECT_EQ(false, lock_mgr.HasCycle(&txn));
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


TEST(LockManagerTest, MultipleCycleTest) {
  // 0->1->2->0 cycle, 3->4->5->3 cycle
  {
    LockManager lock_mgr{false};
    TransactionManager txn_mgr{&lock_mgr};

    lock_mgr.AddEdge(0, 1);
    lock_mgr.AddEdge(1, 2);
    lock_mgr.AddEdge(2, 0);
    lock_mgr.AddEdge(3, 4);
    lock_mgr.AddEdge(4, 5);
    lock_mgr.AddEdge(5, 3);

    EXPECT_EQ(6, lock_mgr.GetEdgeList().size());

    txn_id_t txn;
    EXPECT_EQ(true, lock_mgr.HasCycle(&txn));
    EXPECT_EQ(2, txn);

    lock_mgr.RemoveEdge(0, 1);

    EXPECT_EQ(true, lock_mgr.HasCycle(&txn));
    EXPECT_EQ(5, txn);

    lock_mgr.RemoveEdge(4, 5);
    EXPECT_EQ(false, lock_mgr.HasCycle(&txn));
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


TEST(LockManagerTest, OverlappingCyclesTest) {
  // 0->1->0 cycle, 1->2->1 cycle
  {
    LockManager lock_mgr{false};
    TransactionManager txn_mgr{&lock_mgr};

    lock_mgr.AddEdge(0, 1);
    lock_mgr.AddEdge(1, 0);
    lock_mgr.AddEdge(1, 2);
    lock_mgr.AddEdge(2, 1);

    EXPECT_EQ(4, lock_mgr.GetEdgeList().size());

    txn_id_t txn;
    EXPECT_EQ(true, lock_mgr.HasCycle(&txn));
    EXPECT_EQ(1, txn);

    lock_mgr.RemoveEdge(0, 1);

    EXPECT_EQ(true, lock_mgr.HasCycle(&txn));
    EXPECT_EQ(2, txn);

    lock_mgr.RemoveEdge(2, 1);
    EXPECT_EQ(false, lock_mgr.HasCycle(&txn));
  }

  // 0->1->2->0 cycle, 2->3->4->2 cycle
  {
    LockManager lock_mgr{false};
    TransactionManager txn_mgr{&lock_mgr};

    lock_mgr.AddEdge(0, 1);
    lock_mgr.AddEdge(1, 2);
    lock_mgr.AddEdge(2, 0);
    lock_mgr.AddEdge(2, 3);
    lock_mgr.AddEdge(3, 4);
    lock_mgr.AddEdge(4, 2);

    EXPECT_EQ(6, lock_mgr.GetEdgeList().size());

    txn_id_t txn;
    EXPECT_EQ(true, lock_mgr.HasCycle(&txn));
    EXPECT_EQ(2, txn);

    lock_mgr.RemoveEdge(0, 1);

    EXPECT_EQ(true, lock_mgr.HasCycle(&txn));
    EXPECT_EQ(4, txn);

    lock_mgr.RemoveEdge(3, 4);
    EXPECT_EQ(false, lock_mgr.HasCycle(&txn));
  }

  // 0->1->2->3->4->5->0 cycle, 2->6->7->2 cycle
  {
    LockManager lock_mgr{false};
    TransactionManager txn_mgr{&lock_mgr};

    lock_mgr.AddEdge(0, 1);
    lock_mgr.AddEdge(1, 2);
    lock_mgr.AddEdge(2, 3);
    lock_mgr.AddEdge(3, 0);
    // lock_mgr.AddEdge(4, 5);
    // lock_mgr.AddEdge(5, 0);

    lock_mgr.AddEdge(2, 6);
    lock_mgr.AddEdge(6, 7);
    lock_mgr.AddEdge(7, 2);

    EXPECT_EQ(7, lock_mgr.GetEdgeList().size());

    txn_id_t txn;
    EXPECT_EQ(true, lock_mgr.HasCycle(&txn));
    EXPECT_EQ(3, txn);
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


TEST(LockManagerTest, BasicDeadlockDetectionTest) {
  cycle_detection_interval = std::chrono::milliseconds(200);
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  RID rid0{0, 0};
  RID rid1{1, 1};
  auto *txn0 = txn_mgr.Begin();
  auto *txn1 = txn_mgr.Begin();
  EXPECT_EQ(0, txn0->GetTransactionId());
  EXPECT_EQ(1, txn1->GetTransactionId());

  std::thread t0([&] {
    // Lock and sleep
    bool res = lock_mgr.LockExclusive(txn0, rid0);
    EXPECT_EQ(true, res);
    EXPECT_EQ(TransactionState::GROWING, txn0->GetState());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // This will block
    lock_mgr.LockExclusive(txn0, rid1);

    lock_mgr.Unlock(txn0, rid0);
    lock_mgr.Unlock(txn0, rid1);

    txn_mgr.Commit(txn0);
    EXPECT_EQ(TransactionState::COMMITTED, txn0->GetState());
  });

  std::thread t1([&] {
    // Sleep so T0 can take necessary locks
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    bool res = lock_mgr.LockExclusive(txn1, rid1);
    EXPECT_EQ(res, true);
    EXPECT_EQ(TransactionState::GROWING, txn1->GetState());

    // This will block
    try {
      res = lock_mgr.LockExclusive(txn1, rid0);
      EXPECT_TRUE(false);
      EXPECT_FALSE(res);
      EXPECT_EQ(TransactionState::ABORTED, txn1->GetState());
      txn_mgr.Abort(txn1);
    } catch (TransactionAbortException &e) {
      EXPECT_EQ(TransactionState::ABORTED, txn1->GetState());
      txn_mgr.Abort(txn1);
    }
  });

  // Sleep for enough time to break cycle
  std::this_thread::sleep_for(cycle_detection_interval * 2);

  t0.join();
  t1.join();

  lock_mgr.StopCycleDetection();

  std::this_thread::sleep_for(cycle_detection_interval);

  delete txn0;
  delete txn1;

}  // namespace bustub
