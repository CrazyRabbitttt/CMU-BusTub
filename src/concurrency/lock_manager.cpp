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

#include <algorithm>

namespace bustub {

// 检查 transaction 的状态和 隔离级别
auto LockManager::CheckTransStateAndLockMode(Transaction *txn, const LockMode &lock_mode, bool is_table) -> bool {
  auto txn_id = txn->GetTransactionId();
  auto isolation_level = txn->GetIsolationLevel();
  // 1. 检查 txn 的状态，如果是 Abort/Commit 状态就是逻辑异常
  auto txn_state = txn->GetState();
  if (txn_state == TransactionState::ABORTED || txn_state == TransactionState::COMMITTED) {
    throw std::logic_error("the txn shouldn't be abort Or commit while lock table");
    return false;
  }

  if (isolation_level == IsolationLevel::READ_UNCOMMITTED) {
    if (lock_mode != LockMode::INTENTION_EXCLUSIVE && lock_mode != LockMode::EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn_id, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      return false;
    }
  }

  // 1.2 如果是 shrinking phase, 收缩阶段也能加锁？（写锁一般是不能够授予的）
  if (txn_state == TransactionState::SHRINKING) {
    if (!is_table) {
      // X, IX 锁在 row 的 shrinking 阶段也是不允许的
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
      return false;
    }
    // 可重读读 + shrinking 下不能够进行加锁
    if (isolation_level == IsolationLevel::REPEATABLE_READ) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
      return false;
    }
    // 读提交下只有 IS S锁能够通过
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
      return lock_mode != LockMode::EXCLUSIVE;
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      return lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::INTENTION_EXCLUSIVE;
      break;
    case LockMode::SHARED:
      return lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::SHARED;
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      return lock_mode == LockMode::INTENTION_SHARED;
      break;
    case LockMode::EXCLUSIVE:
      return false;
      break;
  }
  UNREACHABLE("判断兼容性，上面已经将所有情况判断完毕了，这里不可达");
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
                            bool it_will_upgrade_lock, const std::shared_ptr<LockRequest> &request) -> bool {
  // 如果在判断等待的条件中就 abort 直接释放掉就好了
  if (!CheckAbort(txn)) {
    return true;
  }

  if (request->granted_) {
    return true;
  }
  // 加锁的 mode 兼容性检查
  if (!CheckCompatible(txn, lock_mode, request_queue)) {
    return false;
  }

  // 目前的兼容性是通过了的，需要判断优先级是不是最高的 才能给你加锁
  if (request_queue->upgrading_ != INVALID_TXN_ID) {
    // 本次的请求就是锁升级请求，优先级最高, 如果不是的话那就是别的事务正在加锁，不能够授予
    return it_will_upgrade_lock;
  }
  // 判断是不是在队列的最前面【第一个 waiting 的 request】， 前面的正在阻塞你也是需要等待的
  bool first_waiting{true};
  std::unordered_set<LockMode> mode_set;
  std::unordered_set<std::shared_ptr<LockRequest>> waiting_set;
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
        for (auto &request_item : waiting_set) {
          request_item->granted_ = true;
        }
        return true;
      }
      return false;
    }
    first_waiting = false;  // 能够走到这里那么下一个判断的就不是第1个了
    // 根本不是本事务，更新一下数据
    mode_set.insert(it->lock_mode_);
    waiting_set.insert(it);  // 将前面的 waiting 的 request 放进来
  }
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
    throw std::logic_error("bookkeep the rows, the lock mode is not S Or X\n");
  }
}

void LockManager::BookKeepForTransUnRow(Transaction *txn, const LockMode &lock_mode, const table_oid_t &oid,
                                        const RID &rid) {
  if (lock_mode == LockMode::SHARED) {
    (*txn->GetSharedRowLockSet())[oid].erase(rid);
  } else if (lock_mode == LockMode::EXCLUSIVE) {
    (*txn->GetExclusiveRowLockSet())[oid].erase(rid);
  } else {
    throw std::logic_error("bookkeep the rows, the lock mode is not S Or X\n");
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
         lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE || lock_mode == LockMode::EXCLUSIVE ||
         lock_mode == LockMode::INTENTION_EXCLUSIVE);

  assert(exist_mode != lock_mode);  // 之间应该检查完毕是否是相等的

  switch (exist_mode) {
    case LockMode::INTENTION_SHARED:
      if (lock_mode == LockMode::INTENTION_SHARED) {
        return false;
      }
      return true;
    case LockMode::SHARED:
      if (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        return true;
      }
      return false;
    case LockMode::INTENTION_EXCLUSIVE:
      if (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        return true;
      }
      return false;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      if (lock_mode == LockMode::EXCLUSIVE) {
        return true;
      }
      return false;
    case LockMode::EXCLUSIVE:
      return false;
  }
  UNREACHABLE("check if can upgrade");
}

auto LockManager::CheckAbort(Transaction *txn) -> bool {
  // condition: 目前已经是能够授予锁了 但是肯能外部的状态已经是 abort 了
  return txn->GetState() != TransactionState::ABORTED;
}

auto LockManager::CheckRowWillUpgrade(Transaction *txn, const LockMode &lock_mode, const table_oid_t &oid,
                                      const RID &rid) -> CheckUpState {
  auto s_row_set = txn->GetSharedRowLockSet();
  auto x_row_set = txn->GetExclusiveRowLockSet();
  LockMode exist_mode;
  if (!(*s_row_set)[oid].count(rid) && !(*x_row_set)[oid].count(rid)) {
    return CheckUpState::NORMAL_REQUEST;
  }
  if ((*s_row_set)[oid].count(rid)) {
    exist_mode = LockMode::SHARED;
  } else {
    exist_mode = LockMode::EXCLUSIVE;
  }
  if (exist_mode == lock_mode) {
    return CheckUpState::REPEAT_REQUEST;
  }
  if (CheckIfCanUpgrade(txn, exist_mode, lock_mode)) {
    return CheckUpState::UPGRADE_REQUEST;
  }
  return CheckUpState::INVALID_REQUEST;
}

auto LockManager::CheckTableWillUpgrade(Transaction *txn, const LockMode &lock_mode, const table_oid_t &oid,
                                        bool is_table) -> CheckUpState {
  auto s_table_set = txn->GetSharedTableLockSet();
  auto x_table_set = txn->GetExclusiveTableLockSet();
  auto is_table_set = txn->GetIntentionSharedTableLockSet();
  auto ix_table_set = txn->GetIntentionExclusiveTableLockSet();
  auto six_table_set = txn->GetSharedIntentionExclusiveTableLockSet();

  LockMode exist_mode;

  // 之前没有在 oid 加过锁 => normal request
  if (!s_table_set->count(oid) && !x_table_set->count(oid) && !is_table_set->count(oid) && !ix_table_set->count(oid) &&
      !six_table_set->count(oid)) {
    return CheckUpState::NORMAL_REQUEST;
  }
  if (s_table_set->count(oid)) {
    exist_mode = LockMode::SHARED;
  } else if (x_table_set->count(oid)) {
    exist_mode = LockMode::EXCLUSIVE;
  } else if (is_table_set->count(oid)) {
    exist_mode = LockMode::INTENTION_SHARED;
  } else if (ix_table_set->count(oid)) {
    exist_mode = LockMode::INTENTION_EXCLUSIVE;
  } else {
    exist_mode = LockMode::SHARED_INTENTION_EXCLUSIVE;
  }
  // 如果是相同的 Lock Request
  if (lock_mode == exist_mode) {
    return CheckUpState::REPEAT_REQUEST;
  }
  // 如果是能够升级的
  if (CheckIfCanUpgrade(txn, exist_mode, lock_mode)) {
    return CheckUpState::UPGRADE_REQUEST;
  }
  // 剩下的就是不合理的加锁请求，记得抛出异常
  return CheckUpState::INVALID_REQUEST;
}

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  LOG_INFO("txn %d %s  LockTable %d  %s ", txn->GetTransactionId(),
           IsoLevelToString.at(txn->GetIsolationLevel()).c_str(), oid, LockModeToString.at((int)lock_mode).c_str());
  auto txn_id = txn->GetTransactionId();
  /** 保证了锁请求、事务、隔离级别的兼容 */
  if (!CheckTransStateAndLockMode(txn, lock_mode)) {
    return false;
  }
  /** 直接在外部检查是否升级 */
  bool upgrade_flag{false};
  auto if_can_upgrade = CheckTableWillUpgrade(txn, lock_mode, oid);
  if (if_can_upgrade == CheckUpState::REPEAT_REQUEST) {
    return true;
  } else if (if_can_upgrade == CheckUpState::INVALID_REQUEST) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn_id, AbortReason::INCOMPATIBLE_UPGRADE);
    return false;
  } else if (if_can_upgrade == CheckUpState::UPGRADE_REQUEST) {
    upgrade_flag = true;
  }

  // 获得 table 对应的 lock request queue
  std::shared_ptr<LockRequestQueue> request_queue;
  // =======================================================================================
  // LOCK THE TABLE MAP LATCH, 然后在内部锁住 QUEUE
  // =======================================================================================
  table_lock_map_latch_.lock();
  if (!static_cast<bool>(table_lock_map_.count(oid))) {
    auto new_request_queue = std::make_shared<LockRequestQueue>();
    std::list<std::shared_ptr<LockRequest>> request_list{};
    new_request_queue->request_queue_ = request_list;
    table_lock_map_[oid] = new_request_queue;

    // 这个是第一条数据，直接将 request 放进队列中就好了
    auto new_lock_request = std::make_shared<LockRequest>(txn_id, lock_mode, oid);
    new_request_queue->request_queue_.emplace_back(new_lock_request);  // 添加锁
    BookKeepForTransTable(txn, lock_mode, oid);                        // 添加记录
    new_lock_request->granted_ = true;                                 // 授予锁
    table_lock_map_latch_.unlock();
    return true;
  }

  request_queue = table_lock_map_[oid];
  std::unique_lock<std::mutex> queue_lock(request_queue->latch_);
  table_lock_map_latch_.unlock();
  // =======================================================================================
  // UNLOCK THE TABLE MAP LATCH
  // =======================================================================================

  if (upgrade_flag) {
    // 其他的事务正在升级呢
    if (request_queue->upgrading_ != INVALID_TXN_ID && request_queue->upgrading_ != txn_id) {
      request_queue->latch_.unlock();
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn_id, AbortReason::UPGRADE_CONFLICT);
      return false;
    }
    request_queue->upgrading_ = txn_id;
    LockRequest *exist_request{nullptr};  // 记录本事务之前已经授权的 Request
    LockMode exist_mode;
    // 首先获得 exist_lock_mode
    for (auto &it : request_queue->request_queue_) {
      if (it->txn_id_ == txn_id) {
        assert(it->granted_ == true);
        exist_request = it.get();
        exist_mode = exist_request->lock_mode_;
        break;
      }
    }
    // 检查是否能够升级
    if (!CheckIfCanUpgrade(txn, exist_request->lock_mode_, lock_mode)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn_id, AbortReason::INCOMPATIBLE_UPGRADE);
      return false;
    }
    // 删除旧的记录 & 事务中的记录
    auto &list = request_queue->request_queue_;
    auto index1 = std::find_if(list.begin(), list.end(),
                               [&](auto it) { return it->txn_id_ == exist_request->txn_id_ && it->oid_ == oid; });
    list.remove(*index1);
    BookKeepForTransUnTable(txn, exist_mode, oid);
    // 将新的request插入到队列的最前面的 waiting 的前面（成为最新的waiting）
    auto index = request_queue->request_queue_.begin();
    auto new_upgrade_request = std::make_shared<LockRequest>(txn_id, lock_mode, oid);
    if (request_queue->request_queue_.empty()) {
      request_queue->request_queue_.push_back(new_upgrade_request);
    } else {
      for (auto &it : request_queue->request_queue_) {
        if (it->granted_) {
          std::advance(index, 1);  // 向后移动一位
          continue;
        }
        break;
      }
      request_queue->request_queue_.insert(index, new_upgrade_request);
    }
    request_queue->cv_.wait(
        queue_lock, [&]() { return GrantLock(txn, lock_mode, request_queue.get(), true, new_upgrade_request); });
    if (!CheckAbort(txn)) {
      request_queue->request_queue_.remove(new_upgrade_request);
      txn->SetState(TransactionState::ABORTED);
      return false;
    }
    new_upgrade_request->granted_ = true;  // 授予锁
    BookKeepForTransTable(txn, lock_mode, oid);
    request_queue->upgrading_ = INVALID_TXN_ID;  // 升级结束，释放掉flag
  } else {                                       // 不用升级，简单的将锁加到后面
    auto new_lock_request = std::make_shared<LockRequest>(txn_id, lock_mode, oid);
    request_queue->request_queue_.emplace_back(new_lock_request);
    request_queue->cv_.wait(queue_lock,
                            [&]() { return GrantLock(txn, lock_mode, request_queue.get(), false, new_lock_request); });
    if (!CheckAbort(txn)) {
      request_queue->request_queue_.remove(new_lock_request);
      txn->SetState(TransactionState::ABORTED);
      return false;
    }
    // 通过了授予锁的检查，授予锁
    new_lock_request->granted_ = true;
    BookKeepForTransTable(txn, lock_mode, oid);
  }
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  LOG_INFO("Unlock table txnid %d, oid %d", txn->GetTransactionId(), oid);
  txn_id_t txn_id = txn->GetTransactionId();
  // 1. 检查 row lock 是否是全部释放掉了
  auto s_row_s_lock_map = *(txn->GetSharedRowLockSet());
  auto s_row_x_lock_map = *(txn->GetExclusiveRowLockSet());
  if (!s_row_x_lock_map[oid].empty() || !s_row_x_lock_map[oid].empty()) {
    txn->SetState(TransactionState::ABORTED);  // 没有提前解锁所有的 row lock
    throw TransactionAbortException(txn_id, AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
    return false;
  }
  // 公共步骤：1. 获得对应的 lock_queue
  std::shared_ptr<LockRequestQueue> request_queue;
  table_lock_map_latch_.lock();
  assert(table_lock_map_.count(oid));  // 存在对应的 queue
  request_queue = table_lock_map_[oid];
  LockMode lock_mode;
  {
    std::unique_lock<std::mutex> queue_lock(request_queue->latch_);
    table_lock_map_latch_.unlock();
    // 2. 遍历请求队列，找到 unlock 对应的 granted 请求
    bool found_the_lock{false};
    for (auto &it : request_queue->request_queue_) {  // LockRequest *
      if (it->txn_id_ == txn_id && it->oid_ == oid) {
        if (!it->granted_) {
          txn->SetState(TransactionState::ABORTED);
          throw TransactionAbortException(txn_id, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
          return false;
        }
        found_the_lock = true;
        lock_mode = it->lock_mode_;
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
    request_queue->cv_.notify_all();  // notify all of the waiting request to get the lock
  }
  UpdateTxnPhaseWhileUnlock(txn, lock_mode);
  BookKeepForTransUnTable(txn, lock_mode, oid);
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
    return;
  } else if (isolation_level == IsolationLevel::READ_UNCOMMITTED) {
    if (lock_mode == LockMode::EXCLUSIVE) {        // 压根不会出现 s 锁
      txn->SetState(TransactionState::SHRINKING);  // 事务进入到 shrinking phase
    }
  }
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  // 需要先检查是否持有 row 对应的 table_lock
  auto txn_id = txn->GetTransactionId();
  if (lock_mode != LockMode::SHARED && lock_mode != LockMode::EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn_id, AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
    return false;
  }

  // 保证了锁请求、事务、隔离级别的兼容
  if (!CheckTransStateAndLockMode(txn, lock_mode, false)) {
    return false;
  }
  // 检查是否是有 row 对应的 table 的 lock
  std::shared_ptr<LockRequestQueue> table_request_queue;
  {
    std::lock_guard<std::mutex> table_map_lock(table_lock_map_latch_);
    if (!static_cast<bool>(table_lock_map_.count(oid))) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn_id, AbortReason::TABLE_LOCK_NOT_PRESENT);
      return false;
    }
    table_request_queue = table_lock_map_[oid];
    std::unique_lock<std::mutex> table_queue_lock(table_request_queue->latch_);
    bool has_table_lock{false};
    for (auto &it : table_request_queue->request_queue_) {  // LockRequest *
      if (it->txn_id_ == txn_id && it->oid_ == oid && it->granted_) {
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

  /** 保证了锁请求、事务、隔离级别的兼容 */
  if (!CheckTransStateAndLockMode(txn, lock_mode)) {
    return false;
  }
  /** 直接在外部检查是否升级 */
  bool upgrade_flag{false};
  auto if_can_upgrade = CheckRowWillUpgrade(txn, lock_mode, oid, rid);
  if (if_can_upgrade == CheckUpState::REPEAT_REQUEST) {
    return true;
  } else if (if_can_upgrade == CheckUpState::INVALID_REQUEST) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn_id, AbortReason::INCOMPATIBLE_UPGRADE);
    return false;
  } else if (if_can_upgrade == CheckUpState::UPGRADE_REQUEST) {
    upgrade_flag = true;
  }

  // 获得 table 对应的 lock request queue
  std::shared_ptr<LockRequestQueue> request_queue;
  // =======================================================================================
  // LOCK THE TABLE MAP LATCH, 然后在内部锁住 QUEUE
  // =======================================================================================
  row_lock_map_latch_.lock();
  if (!static_cast<bool>(row_lock_map_.count(rid))) {
    auto new_request_queue = std::make_shared<LockRequestQueue>();
    std::list<std::shared_ptr<LockRequest>> request_list{};
    new_request_queue->request_queue_ = request_list;
    row_lock_map_[rid] = new_request_queue;

    // 这个是第一条数据，直接将 request 放进队列中就好了
    auto new_lock_request = std::make_shared<LockRequest>(txn_id, lock_mode, oid, rid);
    new_request_queue->request_queue_.emplace_back(new_lock_request);  // 添加锁
    BookKeepForTransRow(txn, lock_mode, oid, rid);                     // 添加记录
    new_lock_request->granted_ = true;                                 // 授予锁
    row_lock_map_latch_.unlock();
    return true;
  }

  request_queue = row_lock_map_[rid];
  std::unique_lock<std::mutex> queue_lock(request_queue->latch_);
  row_lock_map_latch_.unlock();
  // =======================================================================================
  // UNLOCK THE TABLE MAP LATCH
  // =======================================================================================

  if (upgrade_flag) {
    // 其他的事务正在升级呢
    if (request_queue->upgrading_ != INVALID && request_queue->upgrading_ != txn_id) {
      request_queue->latch_.unlock();
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn_id, AbortReason::UPGRADE_CONFLICT);
      return false;
    }
    request_queue->upgrading_ = txn_id;
    LockRequest *exist_request{nullptr};  // 记录本事务之前已经授权的 Request
    LockMode exist_mode;
    // 首先获得 exist_lock_mode
    for (auto &it : request_queue->request_queue_) {
      if (it->txn_id_ == txn_id) {
        assert(it->granted_ == true);
        exist_request = it.get();
        exist_mode = exist_request->lock_mode_;
        break;
      }
    }
    // 检查是否能够升级
    if (!CheckIfCanUpgrade(txn, exist_request->lock_mode_, lock_mode)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn_id, AbortReason::INCOMPATIBLE_UPGRADE);
      return false;
    }
    // 删除旧的记录 & 事务中的记录
    auto &list = request_queue->request_queue_;
    auto index1 = std::find_if(list.begin(), list.end(), [&](auto it) {
      return it->txn_id_ == exist_request->txn_id_ && it->oid_ == oid && it->rid_ == rid;
    });
    list.remove(*index1);
    BookKeepForTransUnRow(txn, exist_mode, oid, rid);
    // 将新的request插入到队列的最前面的 waiting 的前面（成为最新的waiting）
    auto index = request_queue->request_queue_.begin();
    auto new_upgrade_request = std::make_shared<LockRequest>(txn_id, lock_mode, oid, rid);
    for (auto &it : request_queue->request_queue_) {
      if (it->granted_) {
        std::advance(index, 1);  // 向后移动一位
        continue;
      }
      break;
    }
    request_queue->request_queue_.insert(index, new_upgrade_request);
    request_queue->cv_.wait(
        queue_lock, [&]() { return GrantLock(txn, lock_mode, request_queue.get(), true, new_upgrade_request); });
    if (!CheckAbort(txn)) {
      request_queue->request_queue_.remove(new_upgrade_request);
      txn->SetState(TransactionState::ABORTED);
      return false;
    }
    new_upgrade_request->granted_ = true;  // 授予锁
    BookKeepForTransRow(txn, lock_mode, oid, rid);
    request_queue->upgrading_ = INVALID_TXN_ID;  // 升级结束，释放掉flag
  } else {                                       // 不用升级，简单的将锁加到后面
    auto new_lock_request = std::make_shared<LockRequest>(txn_id, lock_mode, oid, rid);
    request_queue->request_queue_.emplace_back(new_lock_request);
    request_queue->cv_.wait(queue_lock,
                            [&]() { return GrantLock(txn, lock_mode, request_queue.get(), false, new_lock_request); });
    if (!CheckAbort(txn)) {
      request_queue->request_queue_.remove(new_lock_request);
      txn->SetState(TransactionState::ABORTED);
      return false;
    }
    // 通过了授予锁的检查，授予锁
    new_lock_request->granted_ = true;
    BookKeepForTransRow(txn, lock_mode, oid, rid);
  }
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  txn_id_t txn_id = txn->GetTransactionId();

  std::shared_ptr<LockRequestQueue> request_queue;
  row_lock_map_latch_.lock();
  assert(row_lock_map_.count(rid));  // 存在对应的 queue
  request_queue = row_lock_map_[rid];
  LockMode lock_mode;
  {
    std::unique_lock<std::mutex> queue_lock(request_queue->latch_);
    row_lock_map_latch_.unlock();
    // 2. 遍历请求队列，找到 unlock 对应的 granted 请求
    bool found_the_lock{false};
    for (auto &it : request_queue->request_queue_) {  // LockRequest *
      if (it->txn_id_ == txn_id && it->rid_ == rid) {
        if (!it->granted_) {
          txn->SetState(TransactionState::ABORTED);
          throw TransactionAbortException(txn_id, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
          return false;
        }
        found_the_lock = true;
        lock_mode = it->lock_mode_;
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
        std::find_if(list.begin(), list.end(), [&](auto it) { return it->txn_id_ == txn_id && rid == it->rid_; });
    assert(index != list.end());
    list.remove(*index);

    request_queue->cv_.notify_all();  // notify 不需要获得锁
  }
  UpdateTxnPhaseWhileUnlock(txn, lock_mode);
  // BookKeeping
  BookKeepForTransUnRow(txn, lock_mode, oid, rid);
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

auto LockManager::dfs(int node, std::map<int, std::vector<int>> &graph, std::vector<bool> &visited,
                      std::vector<bool> &recStack, int &max_node) -> bool {
  visited[node] = true;
  recStack[node] = true;
  bool has_cycle = false;
  for (int neighbor : graph[node]) {
    if (!visited[neighbor]) {
      has_cycle = dfs(neighbor, graph, visited, recStack, max_node) || has_cycle;
    } else if (recStack[neighbor]) {
      has_cycle = true;
      if (neighbor > max_node) {
        max_node = neighbor;
      }
    }
  }
  recStack[node] = false;
  return has_cycle;
}

auto LockManager::detect_cycle(std::map<txn_id_t, std::vector<txn_id_t>> &graph) -> std::pair<bool, txn_id_t> {
  std::vector<bool> visited(graph.size(), false);
  std::vector<bool> recStack(graph.size(), false);
  txn_id_t max_node = -1;
  bool has_cycle = false;
  for (const auto &[node, neighbors] : graph) {
    if (!visited[node]) {
      has_cycle = dfs(node, graph, visited, recStack, max_node) || has_cycle;
    }
  }
  return {has_cycle, max_node};
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  // Return the minimum txn id(largest txn id)
  auto edges = GetEdgeList();

  for (auto &it : waits_for_) {
    std::sort(it.second.begin(), it.second.end());
  }
  std::pair<bool, txn_id_t> res;
  res = detect_cycle(waits_for_);
  if (res.first) {
    *txn_id = res.second;
  }
  return res.first;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges;
  for (auto &it : waits_for_) {
    for (auto &g_txn : it.second) {
      edges.emplace_back(std::make_pair(it.first, g_txn));
    }
  }
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);  // 每 million second 进行一次 detection
    {                                                       // TODO(students): detect deadlock
       // 构建有向图，如果说形成了环那么就优先释放掉 txn_id 比较大【那么就代表这个执行的时间比较短】的事务的锁
       // 1. 遍历 table_lock_map & row_lock_map 中的所有的请求队列, 构建图
      for (auto &it : table_lock_map_) {
        auto &request_list = it.second->request_queue_;
        for (auto &x : request_list) {
          for (auto &y : request_list) {
            if (x->txn_id_ == y->txn_id_) {
              continue;
            }
            if (x->granted_ && !y->granted_ && !CheckTwoModeCompatible(x->lock_mode_, y->lock_mode_)) {
              AddEdge(y->txn_id_, x->txn_id_);
            }
          }
        }
      }

      for (auto &it : row_lock_map_) {
        auto &request_list = it.second->request_queue_;
        for (auto &x : request_list) {
          for (auto &y : request_list) {
            if (x->txn_id_ == y->txn_id_) {
              continue;
            }
            if (x->granted_ && !y->granted_ && !CheckTwoModeCompatible(x->lock_mode_, y->lock_mode_)) {
              AddEdge(y->txn_id_, x->txn_id_);
            }
          }
        }
      }

      /** 2.检测是否是有环路的等待 */
      txn_id_t youngest_txn_id;
      bool has_cycle = HasCycle(&youngest_txn_id);
      if (has_cycle) {
        auto txn = TransactionManager::GetTransaction(youngest_txn_id);
        txn->SetState(TransactionState::ABORTED);
        // 删除 table request 中的 txn
        for (auto &it : table_lock_map_) {
          auto index = it.second->request_queue_.begin();
          for (auto &item : it.second->request_queue_) {
            if (item->txn_id_ != youngest_txn_id) {  // 找到了 request_list 了
              std::advance(index, 1);
              break;
            }
          }
          (*index)->granted_ = false;
          it.second->request_queue_.remove(*index);
          it.second->cv_.notify_all();
        }

        // 删除 row request list中的 txn
        for (auto &it : row_lock_map_) {
          auto index = it.second->request_queue_.begin();
          for (auto &item : it.second->request_queue_) {
            if (item->txn_id_ != youngest_txn_id) {  // 找到了 request_list 了
              std::advance(index, 1);
              break;
            }
          }
          (*index)->granted_ = false;
          it.second->request_queue_.remove(*index);
          it.second->cv_.notify_all();
        }

        // 移除 graph 中所有与其相关的边
        for (auto &ii : waits_for_) {
          if (ii.first == youngest_txn_id) {
            for (auto &item : ii.second) {
              RemoveEdge(ii.first, item);
            }
          }
          auto cur_index = ii.second.begin();
          for (auto &it : ii.second) {
            if (it != youngest_txn_id) {
              std::advance(cur_index, 1);
            }
          }
          ii.second.erase(
              std::remove_if(ii.second.begin(), ii.second.end(), [&](txn_id_t x) { return x == youngest_txn_id; }));
        }
      }
    }
  }
}

}  // namespace bustub
