//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

// 检查 transaction 的状态和 隔离级别
auto LockManager::CheckTransStateAndLockMode(Transaction *txn, const LockMode &lock_mode) -> bool {
  auto txn_id = txn->GetTransactionId();
  auto isolation_level = txn->GetIsolationLevel();
  // 1. 检查 txn 的状态，如果是 Abort/Commit 状态就是逻辑异常
  auto txn_state = txn->GetState();
  if (txn_state == TransactionState::ABORTED || txn_state == TransactionState::COMMITTED) {
    throw std::runtime_error("the txn shouldn't be abort Or commit while lock table");
    return false;
  }
  // 1.2 如果是 shrinking phase, check the lock_mode if correct
  if (txn_state == TransactionState::SHRINKING) {
    // 可重读读 + shrinking 下不能够进行加锁
    if (isolation_level == IsolationLevel::REPEATABLE_READ) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
      return false;
    }
    if (isolation_level == IsolationLevel::READ_COMMITTED) {
      if (lock_mode != LockMode::INTENTION_SHARED && lock_mode != LockMode::SHARED) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
        return false;
      }
    } else if (isolation_level == IsolationLevel::READ_UNCOMMITTED) {
      txn->SetState(TransactionState::ABORTED);
      if (lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::EXCLUSIVE) {
        throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
        return false;
      }
      throw TransactionAbortException(txn_id, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      return false;
    }
  }

  // 1.3 如果是在 GROUPING phase
  if (txn_state == TransactionState::GROWING) {
    if (isolation_level == IsolationLevel::READ_UNCOMMITTED) {
      if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
          lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn_id, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
        return false;
      }
    }
  }
  return true;
}

auto LockManager::CheckTwoModeCompatible(const LockMode &exist_mode, const LockMode &lock_mode) -> bool {
  switch (exist_mode) {
    case LockMode::INTENTION_SHARED:
      if (lock_mode == LockMode::EXCLUSIVE) {
        return false;
      }
    case LockMode::INTENTION_EXCLUSIVE:
      if (lock_mode != LockMode::INTENTION_SHARED && lock_mode != LockMode::INTENTION_EXCLUSIVE) {
        return false;
      }
      break;
    case LockMode::SHARED:
      if (lock_mode != LockMode::INTENTION_SHARED && lock_mode != LockMode::SHARED) {
        return false;
      }
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      if (lock_mode != LockMode::INTENTION_SHARED) {
        return false;
      }
      break;
    case LockMode::EXCLUSIVE:
      return false;
      break;
  }
  return true;
}

auto LockManager::CheckCompatible(Transaction *txn, const LockMode &lock_mode, LockRequestQueue *request_queue,
                                  std::unordered_set<LockMode> *mode_set) {
  // 检查给定的集合中的 mode 是否是兼容的
  if (mode_set != nullptr) {
    for (auto &exist_mode : *mode_set) {
      if (!CheckTwoModeCompatible(exist_mode, lock_mode)) {
        return false;
      }
    }
    return true;
  }

  // 检查与 queue 中其他的【已经上锁了的】lock mode 是否是兼容的
  auto list = request_queue->request_queue_;
  bool check = std::all_of(list.begin(), list.end(), [&](LockRequest *it) {
    if (it->granted_) {
      LockMode exist_mode = it->lock_mode_;
      if (!CheckTwoModeCompatible(exist_mode, lock_mode)) {
        return false;
      }
    }
    return true;
  });
  return check;
}

auto LockManager::GrantLock(Transaction *txn, const LockMode &lock_mode, LockRequestQueue *request_queue,
                            bool it_will_upgrade_lock) -> bool {
  // 加锁的 mode 兼容性检查
  if (!CheckCompatible(txn, lock_mode, request_queue)) {
    return false;
  }

  // 目前的兼容性是通过了的，需要判断优先级是不是最高的 才能给你加锁
  // 有锁升级的情况
  if (request_queue->upgrading_ != INVALID_TXN_ID) {
    // 本次的请求就是锁升级请求，优先级最高
    if (it_will_upgrade_lock) {
      return true;
    }
    // 别的事务正在升级锁，Return false
    return false;
  }
  // 判断是不是在队列的最前面【第一个 waiting 的 request】
  bool first_waiting{true};
  std::unordered_set<LockMode> mode_set;
  for (auto &it : request_queue->request_queue_) {  // lock_request*
    if (it->granted_) {
      continue;
    }
    // 本事务是最前面的等待的请求
    if (first_waiting && it->txn_id_ == txn->GetTransactionId()) {
      return true;
    }
    // 本事务不是最前面等待的，但是兼容前面的锁
    if (it->txn_id_ == txn->GetTransactionId()) {
      if (CheckCompatible(txn, lock_mode, request_queue, &mode_set)) {
        return true;
      }
      return false;
    }
    // 根本不是本事务，更新一下数据
    first_waiting = false;  // 能够走到这里那么下一个判断的就不是第1个了
    mode_set.insert(it->lock_mode_);
  }
  //
  UNREACHABLE("判断是否能够加锁，一定走不到这里【只要current-txn加入到了队列中】");
  return true;
}

void LockManager::BookKeepForTransRow(Transaction *txn, const LockMode &lock_mode, const table_oid_t &oid,
                                      const RID &rid) {
  if (lock_mode == LockMode::SHARED) {
    (*txn->GetSharedRowLockSet())[oid].insert(rid);
  } else if (lock_mode == LockMode::EXCLUSIVE) {
    (*txn->GetExclusiveRowLockSet())[oid].insert(rid);
  }
}

void LockManager::BookKeepForTransUnRow(Transaction *txn, const LockMode &lock_mode, const table_oid_t &oid,
                                        const RID &rid) {
  if (lock_mode == LockMode::SHARED) {
    (*txn->GetSharedRowLockSet())[oid].erase(rid);
  } else if (lock_mode == LockMode::EXCLUSIVE) {
    (*txn->GetExclusiveRowLockSet())[oid].erase(rid);
  }
}

void LockManager::BookKeepForTransTable(Transaction *txn, const LockMode &lock_mode, const table_oid_t &oid) {
  switch (lock_mode) {
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->insert(oid);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->insert(oid);
      break;
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->insert(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->insert(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
      break;
  };
}

void LockManager::BookKeepForTransUnTable(Transaction *txn, const LockMode &lock_mode, const table_oid_t &oid) {
  switch (lock_mode) {
    case LockMode::SHARED:
      txn->GetSharedTableLockSet()->erase(oid);
      break;
    case LockMode::EXCLUSIVE:
      txn->GetExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::INTENTION_SHARED:
      txn->GetIntentionSharedTableLockSet()->erase(oid);
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      txn->GetIntentionExclusiveTableLockSet()->erase(oid);
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
      break;
  }
}

auto LockManager::CheckIfCanUpgrade(Transaction *txn, const LockMode &exist_mode, const LockMode &lock_mode) -> bool {
  assert(lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
         lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE ||
         lock_mode == LockMode::EXCLUSIVE | lock_mode == LockMode::INTENTION_EXCLUSIVE);
  // 走到这里能够升级的话 exist mode 应该是下面几个
  assert(exist_mode != LockMode::EXCLUSIVE);

  txn_id_t txn_id = txn->GetTransactionId();

  if (exist_mode == LockMode::INTENTION_SHARED) {
    if (lock_mode == LockMode::INTENTION_SHARED) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn_id, AbortReason::INCOMPATIBLE_UPGRADE);
      return false;
    }
  }
  if (exist_mode == LockMode::SHARED) {
    if (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn_id, AbortReason::INCOMPATIBLE_UPGRADE);
      return false;
    }
  }
  if (exist_mode == LockMode::INTENTION_EXCLUSIVE) {
    if (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn_id, AbortReason::INCOMPATIBLE_UPGRADE);
      return false;
    }
  }
  if (exist_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    if (lock_mode != LockMode::EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn_id, AbortReason::INCOMPATIBLE_UPGRADE);
      return false;
    }
  }
  return true;
}

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  auto txn_id = txn->GetTransactionId();
  // 保证了锁请求、事务、隔离级别的兼容
  if (!CheckTransStateAndLockMode(txn, lock_mode)) {
    return false;
  }
  // 获得 table 对应的 lock request queue
  std::shared_ptr<LockRequestQueue> request_queue;

  // =======================================================================================
  // LOCK THE TABLE MAP LATCH, 内部锁住 QUEUE
  // =======================================================================================
  table_lock_map_latch_.lock();
  if (!static_cast<bool>(table_lock_map_.count(oid))) {
    // 创建一个新的 queue
    auto new_request_queue = std::make_shared<LockRequestQueue>();
    //      LockRequest request{txn_id, lock_mode, oid};
    std::list<LockRequest *> request_list{};
    new_request_queue->request_queue_ = request_list;

    table_lock_map_[oid] = new_request_queue;
  } else {
    request_queue = table_lock_map_[oid];
  }
  // TODO(sgx):Lock queue
  request_queue = table_lock_map_[oid];
  std::unique_lock<std::mutex> queue_lock(request_queue->latch_);
  table_lock_map_latch_.unlock();
  // =======================================================================================
  // UNLOCK THE TABLE MAP LATCH
  // =======================================================================================

  bool if_can_upgrade_lock{false};
  LockRequest *exist_request{nullptr};  // 记录本事务之前已经授权的 Request

  assert(request_queue.get() != nullptr);
  // Check if this request is a lock upgrade
  auto queue = request_queue->request_queue_;
  if (!queue.empty()) {
    // 查看是否是有与当前事务相同的请求


    for (auto& it : queue) {
      if (it->txn_id_ == txn_id) {
        assert(it->granted_ == true);
        exist_request = it.get();
        if_can_upgrade_lock = true;
        break;
      }
    }

    
    auto if_has_same_txn_id =
        std::find_if(queue.begin(), queue.end(), [&](auto *request) { return request->txn_id_ = txn_id; });
    if (if_has_same_txn_id != queue.end()) {
      // 当前的事务已经从本资源上面取得了一把锁了, grant 一定为 true
      // 如果没有通过那么就不可能走到这一步 txn 肯定是阻塞的状态
      assert((*if_has_same_txn_id)->granted_ == true);
      exist_request = *if_has_same_txn_id;
      if_can_upgrade_lock = true;
    }
  }
  // 3.尝试对锁进行升级, 判断 mode 是否与之前的一样
  if (if_can_upgrade_lock) {
    assert(exist_request != nullptr);
    // 如果 lock_mode 相同(重复的请求) 直接Return就好了
    if (exist_request->lock_mode_ == lock_mode) {
      request_queue->latch_.unlock();
      return true;
    }
    // 没有其他的事务正在进行尝试升级
    if (request_queue->upgrading_ != INVALID_TXN_ID) {
      request_queue->latch_.unlock();
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn_id, AbortReason::UPGRADE_CONFLICT);
      return false;
    }
    // 判断升级的锁是否是兼容的, 不能反向升级
    // valid: IS -> [S, X, IX, SIX] S -> [X, SIX] IX -> [X, SIX] SIX -> [X]
    auto exist_mode = exist_request->lock_mode_;
    if (!CheckIfCanUpgrade(txn, exist_mode, lock_mode)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn_id, AbortReason::INCOMPATIBLE_UPGRADE);
      return false;
    }

    // 2. 能够对锁进行升级了 释放当前已经持有的锁，在 queue 中标记正在尝试升级
    exist_request->granted_ = false;  // 解除同 txn 之前的锁，进行锁升级
    request_queue->upgrading_ = 1;    // 标记为尝试升级锁

    auto *new_upgrade_request = new LockRequest(txn_id, lock_mode, oid);
    request_queue->request_queue_.emplace_back(new_upgrade_request);
    // TODO(shaoguixin): unlock while upgrade condition
    // 3. 等待直到新锁被授予
    {
      request_queue->cv_.wait(queue_lock, [&]() { return GrantLock(txn, lock_mode, request_queue.get(), true); });
      new_upgrade_request->granted_ = true;
      request_queue->upgrading_ = INVALID_TXN_ID;  // 升级结束
      // book keeping, 将加锁记录更新到 transaction 中
      BookKeepForTransTable(txn, lock_mode, oid);
      delete new_upgrade_request;  // 将申请的数据进行清除掉
      new_upgrade_request = nullptr;
      return true;
    }
  }

  // 4. 平凡的锁申请, 加入到申请队列中
  auto *new_lock_request = new LockRequest(txn_id, lock_mode, oid);
  request_queue->request_queue_.emplace_back(new_lock_request);
  // TODO(shaoguixin): unlock while normal condition
  {
    // 5. 尝试获得锁
    // 条件变量等待别的事务释放掉锁
    request_queue->cv_.wait(queue_lock, [&]() { return GrantLock(txn, lock_mode, request_queue.get(), false); });
    // 等待成功，下面进行普通的锁的授予
    new_lock_request->granted_ = true;
    // book keeping for transaction
    BookKeepForTransTable(txn, lock_mode, oid);
    delete new_lock_request;  // 将申请的数据进行清除掉
    new_lock_request = nullptr;
    return true;
  }
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  txn_id_t txn_id = txn->GetTransactionId();
  // 1. 检查 row lock 是否是全部释放掉了
  auto s_row_s_lock_map = *(txn->GetSharedRowLockSet());
  auto s_row_x_lock_map = *(txn->GetExclusiveRowLockSet());
  if (!s_row_x_lock_map[oid].empty() || !s_row_x_lock_map[oid].empty()) {
    throw TransactionAbortException(txn_id, AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
    return false;
  }
  // 公共步骤：1. 获得对应的 lock_queue
  std::shared_ptr<LockRequestQueue> request_queue;
  table_lock_map_latch_.lock();
  assert(table_lock_map_.count(oid));  // 存在对应的 queue
  request_queue = table_lock_map_[oid];
  {
    std::unique_lock<std::mutex> queue_lock(request_queue->latch_);
    table_lock_map_latch_.unlock();
    // 2. 遍历请求队列，找到 unlock 对应的 granted 请求
    bool found_the_lock{false};
    LockMode lock_mode;
    for (auto &it : request_queue->request_queue_) {  // LockRequest *
      if (it->txn_id_ == txn_id && it->oid_ == oid) {
        if (!it->granted_) {
          throw TransactionAbortException(txn_id, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
          return false;
        }
        found_the_lock = true;
        lock_mode = it->lock_mode_;
        // 找到了对应的 锁了, 根据隔离级别和锁类型修改 txn phase
        UpdateTxnPhaseWhileUnlock(txn, lock_mode);
        break;
      }
    }
    if (!found_the_lock) {
      throw TransactionAbortException(txn_id, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
      return false;
    }
    // 将对应的 request 进行删除
    auto list = request_queue->request_queue_;
    auto index = std::find_if(list.begin(), list.end(),
                              [&](LockRequest *it) { return it->txn_id_ == txn_id && it->oid_ == oid; });
    assert(index != list.end());
    list.remove(*index);

    // BookKeeping
    BookKeepForTransUnTable(txn, lock_mode, oid);
  }

  request_queue->cv_.notify_all();  // notify 不需要获得锁

  return true;
}

void LockManager::UpdateTxnPhaseWhileUnlock(Transaction *txn, const LockMode &lock_mode) {
  auto isolation_level = txn->GetIsolationLevel();
  if (isolation_level == IsolationLevel::REPEATABLE_READ) {
    if (lock_mode == LockMode::SHARED || lock_mode == LockMode::EXCLUSIVE) {
      txn->SetState(TransactionState::SHRINKING);  // 事务进入到 shrinking phase
    }
  } else if (isolation_level == IsolationLevel::READ_COMMITTED) {
    if (lock_mode == LockMode::EXCLUSIVE) {
      txn->SetState(TransactionState::SHRINKING);  // 事务进入到 shrinking phase
    }
  } else if (isolation_level == IsolationLevel::READ_UNCOMMITTED) {
    if (lock_mode == LockMode::EXCLUSIVE) {        // 压根不会出现 s 锁
      txn->SetState(TransactionState::SHRINKING);  // 事务进入到 shrinking phase
    }
  }
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  // 需要先检查是否持有 row 对应的 table_lock

  auto txn_id = txn->GetTransactionId();
  // 保证了锁请求、事务、隔离级别的兼容
  if (!CheckTransStateAndLockMode(txn, lock_mode)) {
    return false;
  }

  // 检查是否是有 row 对应的 table 的 lock
  std::shared_ptr<LockRequestQueue> table_request_queue;
  {
    std::lock_guard<std::mutex> table_map_lock(table_lock_map_latch_);
    if (!static_cast<bool>(table_lock_map_.count(oid))) {
      LOG_INFO("want to lock row, but don't held the table lock");
      return false;
    }
    table_request_queue = table_lock_map_[oid];
    bool has_table_lock{false};
    for (auto &it : table_request_queue->request_queue_) {  // LockRequest *
      if (it->txn_id_ == txn_id && it->oid_ == oid && it->granted_) {
        has_table_lock = true;
        break;
      }
    }
    if (!has_table_lock) {
      LOG_INFO("want to lock row, but don't held the table lock");
      return false;
    }
  }

  // 获得 table 对应的 lock request queue
  std::shared_ptr<LockRequestQueue> request_queue;

  // =======================================================================================
  // LOCK THE TABLE MAP LATCH, 内部锁住 QUEUE
  // =======================================================================================
  row_lock_map_latch_.lock();
  if (!static_cast<bool>(row_lock_map_.count(rid))) {
    // 创建一个新的 queue
    auto new_request_queue = std::make_shared<LockRequestQueue>();
    //      LockRequest request{txn_id, lock_mode, oid};
    std::list<LockRequest *> request_list{};
    new_request_queue->request_queue_ = request_list;

    row_lock_map_[rid] = new_request_queue;
  } else {
    request_queue = row_lock_map_[rid];
  }
  // TODO(sgx):Lock queue
  request_queue = row_lock_map_[rid];
  std::unique_lock<std::mutex> queue_lock(request_queue->latch_);
  row_lock_map_latch_.unlock();
  // =======================================================================================
  // UNLOCK THE TABLE MAP LATCH
  // =======================================================================================

  bool if_can_upgrade_lock{false};
  LockRequest *exist_request{nullptr};  // 记录本事务之前已经授权的 Request

  // Check if this request is a lock upgrade
  auto queue = request_queue->request_queue_;
  // 查看是否是有与当前事务相同的请求
  if (!queue.empty()) {
    auto if_has_same_txn_id =
        std::find_if(queue.begin(), queue.end(), [&](LockRequest *request) { return request->txn_id_ = txn_id; });
    if (if_has_same_txn_id != queue.end()) {
      // 当前的事务已经从本资源上面取得了一把锁了, grant 一定为 true
      // 如果没有通过那么就不可能走到这一步 txn 肯定是阻塞的状态
      assert((*if_has_same_txn_id)->granted_ == true);
      exist_request = *if_has_same_txn_id;
      if_can_upgrade_lock = true;
    }
  }

  // 3.尝试对锁进行升级, 判断 mode 是否与之前的一样
  if (if_can_upgrade_lock) {
    assert(exist_request != nullptr);
    // 如果 lock_mode 相同(重复的请求) 直接Return就好了
    if (exist_request->lock_mode_ == lock_mode) {
      request_queue->latch_.unlock();
      return true;
    }
    // 没有其他的事务正在进行尝试升级
    if (request_queue->upgrading_ != INVALID_TXN_ID) {
      request_queue->latch_.unlock();
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn_id, AbortReason::UPGRADE_CONFLICT);
      return false;
    }
    // 判断升级的锁是否是兼容的, 不能反向升级
    // valid: IS -> [S, X, IX, SIX] S -> [X, SIX] IX -> [X, SIX] SIX -> [X]
    auto exist_mode = exist_request->lock_mode_;
    if (!CheckIfCanUpgrade(txn, exist_mode, lock_mode)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn_id, AbortReason::INCOMPATIBLE_UPGRADE);
      return false;
    }

    // 2. 能够对锁进行升级了 释放当前已经持有的锁，在 queue 中标记正在尝试升级
    exist_request->granted_ = false;  // 解除同 txn 之前的锁，进行锁升级
    request_queue->upgrading_ = 1;    // 标记为尝试升级锁

    auto *new_upgrade_request = new LockRequest(txn_id, lock_mode, oid, rid);
    request_queue->request_queue_.emplace_back(new_upgrade_request);
    // TODO(shaoguixin): unlock while upgrade condition
    // 3. 等待直到新锁被授予
    {
      request_queue->cv_.wait(queue_lock, [&]() { return GrantLock(txn, lock_mode, request_queue.get(), true); });
      new_upgrade_request->granted_ = true;
      request_queue->upgrading_ = INVALID_TXN_ID;  // 升级结束
      // book keeping, 将加锁记录更新到 transaction 中
      BookKeepForTransRow(txn, lock_mode, oid, rid);
      delete new_upgrade_request;  // 将申请的数据进行清除掉
      new_upgrade_request = nullptr;
      return true;
    }
  }

  // 4. 平凡的锁申请, 加入到申请队列中
  auto *new_lock_request = new LockRequest(txn_id, lock_mode, oid, rid);
  request_queue->request_queue_.emplace_back(new_lock_request);
  // TODO(shaoguixin): unlock while normal condition
  {
    // 5. 尝试获得锁
    // 条件变量等待别的事务释放掉锁
    request_queue->cv_.wait(queue_lock, [&]() { return GrantLock(txn, lock_mode, request_queue.get(), false); });
    // 等待成功，下面进行普通的锁的授予
    new_lock_request->granted_ = true;
    // book keeping for transaction
    BookKeepForTransRow(txn, lock_mode, oid, rid);
    delete new_lock_request;  // 将申请的数据进行清除掉
    new_lock_request = nullptr;
    return true;
  }
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  txn_id_t txn_id = txn->GetTransactionId();

  std::shared_ptr<LockRequestQueue> request_queue;
  row_lock_map_latch_.lock();
  assert(row_lock_map_.count(rid));  // 存在对应的 queue
  request_queue = row_lock_map_[rid];
  {
    std::unique_lock<std::mutex> queue_lock(request_queue->latch_);
    row_lock_map_latch_.unlock();
    // 2. 遍历请求队列，找到 unlock 对应的 granted 请求
    bool found_the_lock{false};
    LockMode lock_mode;
    for (auto &it : request_queue->request_queue_) {  // LockRequest *
      if (it->txn_id_ == txn_id && it->oid_ == oid && it->rid_ == rid) {
        if (!it->granted_) {
          throw TransactionAbortException(txn_id, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
          return false;
        }
        found_the_lock = true;
        lock_mode = it->lock_mode_;
        // 找到了对应的 锁了, 根据隔离级别和锁类型修改 txn phase
        UpdateTxnPhaseWhileUnlock(txn, lock_mode);
        break;
      }
    }
    if (!found_the_lock) {
      throw TransactionAbortException(txn_id, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
      return false;
    }
    // 将对应的 request 进行删除
    auto list = request_queue->request_queue_;
    auto index = std::find_if(list.begin(), list.end(), [&](LockRequest *it) {
      return it->txn_id_ == txn_id && it->oid_ == oid && it->rid_ == rid;
    });
    assert(index != list.end());
    list.remove(*index);

    // BookKeeping
    BookKeepForTransUnRow(txn, lock_mode, oid, rid);
  }
  request_queue->cv_.notify_all();  // notify 不需要获得锁
  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { return false; }

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
    }
  }
}

}  // namespace bustub
