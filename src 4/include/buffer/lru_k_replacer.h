//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.h
//
// Identification: src/include/buffer/lru_k_replacer.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <iostream>
#include <limits>
#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/logger.h"
#include "common/macros.h"

#define THROWERROR(message) throw std::logic_error(message)

namespace bustub {

/**
 * LRUKRe
 * placer implements the LRU-k replacement policy.
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time between
 * current timestamp and the timestamp of kth previous access.
 *
 * A frame with less than k historical references is given
 * +inf as its backward k-distance. When multiple frames have +inf backward k-distance,
 * classical LRU algorithm is used to choose victim.
 */
class LRUKReplacer {
 public:
  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief a new LRUKReplacer.
   * @param num_frames the maximum number of frames the LRUReplacer will be required to store
   */
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Destroys the LRUReplacer.
   */
  ~LRUKReplacer() = default;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
   * that are marked as 'evictable' are candidates for eviction.
   *
   * A frame with less than k historical references is given +inf as its backward k-distance.
   * If multiple frames have inf backward k-distance, then evict the frame with the earliest
   * timestamp overall.
   *
   * Successful eviction of a frame should decrement the size of replacer and remove the frame's
   * access history.
   *
   * @param[out] frame_id id of frame that is evicted.
   * @return true if a frame is evicted successfully, false if no frames can be evicted.
   */
  auto Evict(frame_id_t *frame_id) -> bool;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Record the event that the given frame id is accessed at current timestamp.
   * Create a new entry for access history if frame id has not been seen before.                // 1.
   * 没有访问过就创建一个新的
   *
   * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
   * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
   *
   * @param frame_id id of frame that received a new access.
   */
  void RecordAccess(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Toggle whether a frame is evictable or non-evictable. This function also
   * controls replacer's size. Note that size is equal to number of evictable entries.
   *
   * If a frame was previously evictable and is to be set to non-evictable, then size should
   * decrement. If a frame was previously non-evictable and is to be set to evictable,
   * then size should increment.
   *
   * If frame id is invalid, throw an exception or abort the process.     // $$$$$$$$$$$$$$$$ if frame id is invalid,
   * throw an exception Or abort the process $$$$$$$$$
   *
   * For other scenarios, this function should terminate without modifying anything.
   *
   * @param frame_id id of frame whose 'evictable' status will be modified
   * @param set_evictable whether the given frame is evictable or not
   */
  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Remove an evictable frame from replacer, along with its access history.
   * This function should also decrement replacer's size if removal is successful.
   *
   * Note that this is different from evicting a frame, which always remove the frame
   * with largest backward k-distance. This function removes specified frame id,
   * no matter what its backward k-distance is.
   *
   * If Remove is called on a non-evictable frame, throw an exception or abort the
   * process.
   *
   * If specified frame is not found, directly return from this function.
   *
   * @param frame_id id of frame to be removed
   */
  void Remove(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Return replacer's size, which tracks the number of evictable frames.
   *
   * @return size_t
   */
  // 返回 【能够驱逐的数据大小】
  auto Size() -> size_t;

  void InsertIntoMap(int a) { counter_[a]++; }

  void PrintMapSize() {
    std::cout << "The size of map ";
    std::cout << counter_.max_size();
  }

  void ShowCounters() {
    for (auto &it : counter_) {
      std::cout << it.first << " " << it.second << "\n";
    }
  }

 private:
  // TODO(student): implement me! You can replace these member variables as you like.
  // Remove maybe_unused if you start using them.  开了眼了

  void IncrementEvictaleSize();
  void DecrementEvictableSize();

  using entry = std::pair<int, bool>;  // the entry is [key & flag]
  using iterr = std::list<entry>::iterator;

  [[maybe_unused]] size_t current_timestamp_{0};
  [[maybe_unused]] size_t curr_size_{0};
  std::unordered_map<int, iterr> cache_mp_;
  std::unordered_map<int, iterr> history_map_;  // key ====> iterator of list
  std::unordered_map<int, int> counter_;        // key ====> accessed count
  std::list<entry> history_linked_list_;
  std::list<entry> cache_list_;

  // if access the key and the flag bit is not false, then evitable size + 1
  size_t evictable_size_{0};  // 可驱逐的数目
  size_t replacer_size_;      // what is replacer size ?
  size_t k_;
  std::mutex latch_;
};

}  // namespace bustub
