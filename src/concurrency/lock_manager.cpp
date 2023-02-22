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
    throw std::logic_error("the txn shouldn't be abort Or commit while lock table");
    return false;
  }
  // 1.2 如果是 shrinking phase, 收缩阶段也能加锁？（写锁一般是不能够授予的）
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
      return true;
    }
    if (isolation_level == IsolationLevel::READ_UNCOMMITTED) {
      if (lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
        return false;
      }
      // 读锁永远不会加在 读未提交的隔离界别上面的
      txn->SetState(TransactionState::ABORTED);
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
      printf("Check if the lock mode compatible with IS\n");
      if (lock_mode == LockMode::EXCLUSIVE) {
        return false;
      }
      break;
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
  auto &list = request_queue->request_queue_;
  for (auto &it : list) {
    if (it->granted_ && it->txn_id_ != txn->GetTransactionId()) {
      LockMode exist_mode = it->lock_mode_;
      if (!CheckTwoModeCompatible(exist_mode, lock_mode)) {
        return false;
      }
    }
  }
  return true;
}

auto LockManager::GrantLock(Transaction *txn, const LockMode &lock_mode, LockRequestQueue *request_queue,
                            bool it_will_upgrade_lock) -> bool {
  // 加锁的 mode 兼容性检查
  if (!CheckCompatible(txn, lock_mode, request_queue)) {
    //    LOG_DEBUG("txn id:%d, check grant lock, 兼容性不行", txn->GetTransactionId());
    return false;
  }

  // 目前的兼容性是通过了的，需要判断优先级是不是最高的 才能给你加锁
  // 有锁升级的情况
  if (request_queue->upgrading_ != INVALID_TXN_ID) {
    // 本次的请求就是锁升级请求，优先级最高
    if (it_will_upgrade_lock) {
      return true;
    }
    return false;
  }
  // 判断是不是在队列的最前面【第一个 waiting 的 request】， 前面的正在阻塞你也是需要等待的
  bool first_waiting{true};
  std::unordered_set<LockMode> mode_set;
  for (auto &it : request_queue->request_queue_) {  // lock_request*
    if (it->granted_) {
      continue;
    }

    if (it->txn_id_ == txn->GetTransactionId()) {
      // 目前的事务是第一个等待的事务，那么就能够直接授予锁
      if (first_waiting) {
        return true;
      }
      /* 如果不是第一个等待的事务，那么检查一下前面waiting- request的是否是能够直接授予锁
       * (想要的好的结果就是目前的同之前的所有waiting的request都同时加锁，否则就直接 Return false：可能是锁不兼容)
       */
      // 1. 目前的锁同之前的 waiting 的锁是兼容的
      if (CheckCompatible(txn, lock_mode, request_queue, &mode_set)) {
        // 2.判断之前的 waiting 的锁是否和 已经上好了的锁兼容(如果是的话能够直接 grant 给目前的及之前的所有的 waiting
        // 的锁)
        for (auto &item : mode_set) {
          if (!CheckCompatible(txn, item, request_queue, nullptr)) {
            return false;
          }
        }
        // 目前就是所有的 waiting 的请求都能够授予锁了
        return true;
      }
      return false;
    }

    //    // 本事务是最前面的等待的请求
    //    if (first_waiting && it->txn_id_ == txn->GetTransactionId()) {
    //      return true;
    //    }
    //    // 本事务不是最前面等待的，但是兼容前面的锁
    //    if (it->txn_id_ == txn->GetTransactionId()) {
    //      if (CheckCompatible(txn, lock_mode, request_queue, &mode_set)) {  // 同前面的所有的 waiting 请求都是兼容的
    //        return true;
    //      }
    //      return false;
    //    }
    first_waiting = false;  // 能够走到这里那么下一个判断的就不是第1个了
    // 根本不是本事务，更新一下数据
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
  } else {
    std::logic_error("bookkeep the rows, the lock mode is not S Or X\n");
  }
}

void LockManager::BookKeepForTransUnRow(Transaction *txn, const LockMode &lock_mode, const table_oid_t &oid,
                                        const RID &rid) {
  if (lock_mode == LockMode::SHARED) {
    (*txn->GetSharedRowLockSet())[oid].erase(rid);
  } else if (lock_mode == LockMode::EXCLUSIVE) {
    (*txn->GetExclusiveRowLockSet())[oid].erase(rid);
  } else {
    std::logic_error("bookkeep the rows, the lock mode is not S Or X\n");
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

auto LockManager::CheckAbort(Transaction *txn) -> bool {
  // condition: 目前已经是能够授予锁了 但是肯能外部的状态已经是 abort 了
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
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
  // LOCK THE TABLE MAP LATCH, 然后在内部锁住 QUEUE
  // =======================================================================================
  table_lock_map_latch_.lock();
  if (!static_cast<bool>(table_lock_map_.count(oid))) {
    // 创建一个新的 queue
    auto new_request_queue = std::make_shared<LockRequestQueue>();
    std::list<std::shared_ptr<LockRequest>> request_list{};
    new_request_queue->request_queue_ = std::move(request_list);

    table_lock_map_[oid] = new_request_queue;
  } else {
    request_queue = table_lock_map_[oid];
  }
  // TODO(sgx):Lock queue
  request_queue = table_lock_map_[oid];
  std::unique_lock<std::mutex> queue_lock(request_queue->latch_);
  //  LOG_DEBUG("txn id:%d lock the table queue:%d", txn_id, oid);
  table_lock_map_latch_.unlock();
  // =======================================================================================
  // UNLOCK THE TABLE MAP LATCH
  // =======================================================================================

  bool if_can_upgrade_lock{false};
  LockRequest *exist_request{nullptr};  // 记录本事务之前已经授权的 Request

  assert(request_queue.get() != nullptr);
  // Check if this request is a lock upgrade
  auto &queue = request_queue->request_queue_;
  if (!queue.empty()) {
    // 查看是否是有与当前事务相同的请求
    for (auto &it : queue) {
      if (it->txn_id_ == txn_id) {
        assert(it->granted_ == true);
        exist_request = it.get();
        if_can_upgrade_lock = true;
        break;
      }
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
    // 有其他的事务正在进行尝试升级
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
    // 直接释放之前的请求
    auto &list = request_queue->request_queue_;
    auto index = std::find_if(list.begin(), list.end(),
                              [&](auto it) { return it->txn_id_ == exist_request->txn_id_ && it->oid_ == oid; });
    assert(index != list.end());
    list.remove(*index);
    request_queue->upgrading_ = 1;  // 标记为尝试升级锁

    auto new_upgrade_request = std::make_shared<LockRequest>(txn_id, lock_mode, oid);
    request_queue->request_queue_.emplace_back(new_upgrade_request);
    // TODO(shaoguixin): unlock while upgrade condition
    // 3. 等待直到新锁被授予
    {
      BookKeepForTransUnTable(txn, exist_mode, oid);  // 因为升级 lock reques， 所以需要将之前的锁给释放掉
      request_queue->cv_.wait(queue_lock, [&]() { return GrantLock(txn, lock_mode, request_queue.get(), true); });
      if (!CheckAbort(txn)) {
        txn->SetState(TransactionState::ABORTED);
        request_queue->request_queue_.remove(new_upgrade_request);  // 需要将锁的申请先释放掉
        return false;
      }
      new_upgrade_request->granted_ = true;
      request_queue->upgrading_ = INVALID_TXN_ID;  // 升级结束
      // book keeping, 将加锁记录更新到 transaction 中
      BookKeepForTransTable(txn, lock_mode, oid);
      return true;
    }
  }

  // 4. 平凡的锁申请, 加入到申请队列中
  auto new_lock_request = std::make_shared<LockRequest>(txn_id, lock_mode, oid);
  //  auto *new_lock_request = new LockRequest(txn_id, lock_mode, oid);
  request_queue->request_queue_.emplace_back(new_lock_request);
  // TODO(shaoguixin): unlock while normal condition
  {
    // 条件变量等待别的事务释放掉锁, 即使这里是不兼容的，也需要放在队列中 waiting
    request_queue->cv_.wait(queue_lock, [&]() { return GrantLock(txn, lock_mode, request_queue.get(), false); });
    // 需要特殊判断一下 等待的过程中是否出现了 Abort 的情况
    if (!CheckAbort(txn)) {
      request_queue->request_queue_.remove(new_lock_request);
      txn->SetState(TransactionState::ABORTED);
      //      throw TransactionAbortException(txn_id, AbortReason::GrantLatchButAborted);
      return false;
    }
    // 等待成功，下面进行普通的锁的授予
    new_lock_request->granted_ = true;
    // book keeping for transaction
    BookKeepForTransTable(txn, lock_mode, oid);
    return true;
  }
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  txn_id_t txn_id = txn->GetTransactionId();
  // 1. 检查 row lock 是否是全部释放掉了
  auto s_row_s_lock_map = *(txn->GetSharedRowLockSet());
  auto s_row_x_lock_map = *(txn->GetExclusiveRowLockSet());
  if (!s_row_x_lock_map[oid].empty() || !s_row_x_lock_map[oid].empty()) {
    txn->SetState(TransactionState::ABORTED);
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
          txn->SetState(TransactionState::ABORTED);
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
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn_id, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
      return false;
    }
    // 将对应的 request 进行删除
    auto &list = request_queue->request_queue_;
    auto index =
        std::find_if(list.begin(), list.end(), [&](auto it) { return it->txn_id_ == txn_id && it->oid_ == oid; });
    assert(index != list.end());
    list.remove(*index);
    // BookKeeping
    BookKeepForTransUnTable(txn, lock_mode, oid);
  }
  request_queue->cv_.notify_all();  // notify 不需要获得锁
  return true;
}

void LockManager::UpdateTxnPhaseWhileUnlock(Transaction *txn, const LockMode &lock_mode) {
  auto cur_trans_state = txn->GetState();
  // 如果说目前是处于 abort 状态来释放锁的话 就不要再更改状态了
  if (cur_trans_state == TransactionState::COMMITTED || cur_trans_state == TransactionState::ABORTED) {
    return;
  }
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
  //  LOG_INFO("lock row, check if has the table lock");
  {
    std::lock_guard<std::mutex> table_map_lock(table_lock_map_latch_);
    if (!static_cast<bool>(table_lock_map_.count(oid))) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn_id, AbortReason::TABLE_LOCK_NOT_PRESENT);
      return false;
    }
    table_request_queue = table_lock_map_[oid];
    bool has_table_lock{false};
    for (auto &it : table_request_queue->request_queue_) {  // LockRequest *
      if (it->txn_id_ == txn_id && it->oid_ == oid && it->granted_) {
        if (lock_mode == LockMode::EXCLUSIVE) {
          if (it->lock_mode_ != LockMode::EXCLUSIVE && it->lock_mode_ != LockMode::INTENTION_EXCLUSIVE &&
              it->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE) {
            txn->SetState(TransactionState::ABORTED);
            throw TransactionAbortException(txn_id, AbortReason::TABLE_LOCK_NOT_PRESENT);
            return false;
          }
        }
        has_table_lock = true;
        break;
      }
    }
    if (!has_table_lock) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn_id, AbortReason::TABLE_LOCK_NOT_PRESENT);
      return false;
    }
  }
  //  LOG_INFO("In row lock, after the check of has the table lock");

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
    std::list<std::shared_ptr<LockRequest>> request_list{};
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

  assert(request_queue.get() != nullptr);
  // Check if this request is a lock upgrade
  auto &queue = request_queue->request_queue_;
  // 查看是否是有与当前事务相同的请求
  if (!queue.empty()) {
    for (auto &it : queue) {
      if (it->txn_id_ == txn_id) {
        assert(it->granted_ == true);
        exist_request = it.get();
        if_can_upgrade_lock = true;
        break;
      }
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
      LOG_DEBUG("upgrade conflict");
      throw TransactionAbortException(txn_id, AbortReason::UPGRADE_CONFLICT);
      return false;
    }
    // 判断升级的锁是否是兼容的, 不能反向升级
    // valid: IS -> [S, X, IX, SIX] S -> [X, SIX] IX -> [X, SIX] SIX -> [X]
    auto exist_mode = exist_request->lock_mode_;
    if (!CheckIfCanUpgrade(txn, exist_mode, lock_mode)) {
      txn->SetState(TransactionState::ABORTED);
      LOG_DEBUG("incompatible_upgrade");
      throw TransactionAbortException(txn_id, AbortReason::INCOMPATIBLE_UPGRADE);
      return false;
    }

    // 2. 能够对锁进行升级了 释放当前已经持有的锁，在 queue 中标记正在尝试升级
    exist_request->granted_ = false;  // 解除同 txn 之前的锁，进行锁升级
    // 直接释放之前的请求
    auto &list = request_queue->request_queue_;
    auto index = std::find_if(list.begin(), list.end(), [&](auto it) {
      return it->txn_id_ == exist_request->txn_id_ && it->oid_ == oid && it->rid_ == exist_request->rid_;
    });
    assert(index != list.end());
    list.remove(*index);
    request_queue->upgrading_ = 1;  // 标记为尝试升级锁

    auto new_upgrade_request = std::make_shared<LockRequest>(txn_id, lock_mode, oid, rid);
    request_queue->request_queue_.emplace_back(new_upgrade_request);
    // TODO(shaoguixin): unlock while upgrade condition
    // 3. 等待直到新锁被授予
    {
      BookKeepForTransUnRow(txn, exist_mode, oid, rid);  // 直接先释放掉之前的 S 锁
      request_queue->cv_.wait(queue_lock, [&]() { return GrantLock(txn, lock_mode, request_queue.get(), true); });
      if (!CheckAbort(txn)) {
        txn->SetState(TransactionState::ABORTED);
        request_queue->request_queue_.remove(new_upgrade_request);
        return false;
      }

      new_upgrade_request->granted_ = true;
      request_queue->upgrading_ = INVALID_TXN_ID;  // 升级结束
      // book keeping, 将加锁记录更新到 transaction 中
      BookKeepForTransRow(txn, lock_mode, oid, rid);
      return true;
    }
  }

  // 4. 平凡的锁申请, 加入到申请队列中
  auto new_lock_request = std::make_shared<LockRequest>(txn_id, lock_mode, oid);
  request_queue->request_queue_.emplace_back(new_lock_request);
  // TODO(shaoguixin): unlock while normal condition
  {
    // 5. 尝试获得锁
    request_queue->cv_.wait(queue_lock, [&]() { return GrantLock(txn, lock_mode, request_queue.get(), false); });
    if (!CheckAbort(txn)) {
      txn->SetState(TransactionState::ABORTED);
      request_queue->request_queue_.remove(new_lock_request);
      //      throw TransactionAbortException(txn_id, AbortReason::GrantLatchButAborted);
      return false;
    }
    // 等待成功，下面进行普通的锁的授予
    if (txn->GetState() == TransactionState::ABORTED) {
      request_queue->request_queue_.remove(new_lock_request);
      queue_lock.unlock();
      throw TransactionAbortException(txn_id, AbortReason::UPGRADE_CONFLICT);
    }
    new_lock_request->granted_ = true;
    // book keeping for transaction
    BookKeepForTransRow(txn, lock_mode, oid, rid);
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
      if (it->txn_id_ == txn_id) {
        if (!it->granted_) {
          LOG_DEBUG("txnid:%d, attemped unlock but no lock held1", txn_id);
          txn->SetState(TransactionState::ABORTED);
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
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn_id, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
      return false;
    }
    // 将对应的 request 进行删除
    auto &list = request_queue->request_queue_;
    auto index = std::find_if(list.begin(), list.end(), [&](auto it) { return it->txn_id_ == txn_id; });
    assert(index != list.end());
    list.remove(*index);

    // BookKeeping
    BookKeepForTransUnRow(txn, lock_mode, oid, rid);
  }
  //  LOG_INFO("Unlock row notify the cv");
  request_queue->cv_.notify_all();  // notify 不需要获得锁
  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) { waits_for_[t1].emplace_back(t2); }

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  auto index = waits_for_[t1].begin();
  while (index != waits_for_[t1].end()) {
    if (*index == t2) {
      break;
    }
    index++;
  }
  waits_for_[t1].erase(index);
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  // Return the minimum txn id(largest txn id)
  auto edges = GetEdgeList();

  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);

  for (auto &it : waits_for_) {
    for (auto &g_txn : it.second) {
      edges.emplace_back(std::make_pair(it.first, g_txn));
    }
  }

  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
      // 构建有向图，如果说形成了环那么就优先释放掉 txn_id 比较大【那么就代表这个执行的时间比较短】的事务的锁
      // 1. 遍历 table_lock_map & row_lock_map 中的所有的请求队列
      for (auto &it : table_lock_map_) {
        auto request_queue = it.second->request_queue_;  // 请求队列
        std::vector<std::shared_ptr<LockRequest>> granted_set;
        std::vector<std::shared_ptr<LockRequest>> waiting_set;
        for (auto &request : request_queue) {
          if (request->granted_) {
            granted_set.emplace_back(request);
          } else {
            granted_set.emplace_back(request);
          }
        }

        for (auto &w_txn : waiting_set) {
          for (auto &g_txn : granted_set) {
            // 如果说锁不兼容 & w_txn 正在等待 g_txn 的锁
            if (!CheckTwoModeCompatible(g_txn->lock_mode_, w_txn->lock_mode_)) {
              AddEdge(w_txn->txn_id_, g_txn->txn_id_);
            }
          }
        }
      }
    }
  }
}

}  // namespace bustub
