//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  // Return true if a frame is evicted successfully, False if no frame can be evicted
  // 1. 首先从 history 中查询是不是有存在的
  auto index = std::find_if(history_linked_list_.begin(), history_linked_list_.end(),
                            [&](const entry &it) { return it.second; });
  if (index == history_linked_list_.end()) {
    auto index2 = std::find_if(cache_list_.begin(), cache_list_.end(), [&](const entry &it) { return it.second; });
    if (index2 == cache_list_.end()) {
      return false;
    }  // 根本没有找到
    // Run here means : find an entry to be evicted in cached list
    DecrementEvictableSize();
    *frame_id = index2->first;
    cache_list_.erase(index2);
    cache_mp_.erase(*frame_id);
    counter_.erase(*frame_id);
    return true;
  }
  // Run here means : find an entry to be evicted in history list
  DecrementEvictableSize();
  *frame_id = index->first;  // 将需要 evict 的 frameId 传回去
  int tmp_id = *frame_id;
  history_linked_list_.erase(index);
  history_map_.erase(tmp_id);
  counter_.erase(tmp_id);
  return true;
}

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

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);
  // 1. 首先判断数据是否是访问过，没有那么就创建一个新的, 如果说是在 history 中，不需要位置的更新
  if (counter_.count(frame_id) == 0) {
    entry it = std::make_pair(frame_id, true);
    if (history_linked_list_.size() == replacer_size_) {
      throw std::exception();
    }
    counter_[frame_id]++;
    history_linked_list_.push_back(it);
    auto iter = history_linked_list_.end();
    iter--;
    history_map_[frame_id] = iter;
    IncrementEvictaleSize();
  } else {
    counter_[frame_id]++;
    if (counter_[frame_id] == static_cast<int>(k_)) {  // from history list
      /** 从 history_list 中删除 */
      if (history_map_.count(frame_id) != 0) {
        auto it = history_map_[frame_id];
        cache_list_.push_back(*it);
        history_linked_list_.erase(it);
        history_map_.erase(frame_id);
        auto iter = cache_list_.end();
        iter--;
        cache_mp_[frame_id] = iter;
      } else {
        cache_list_.emplace_back(std::make_pair(frame_id, true));
        auto iter = cache_list_.end();
        iter--;
        cache_mp_[frame_id] = iter;
      }
    } else if (counter_[frame_id] > static_cast<int>(k_)) {
      /** 更新在 cache_list 的位置*/
      entry tmp = std::make_pair(frame_id, true);
      if (cache_mp_.count(frame_id) != 0) {
        auto iter = cache_mp_[frame_id];
        tmp = *iter;
        cache_list_.erase(iter);
      }
      cache_list_.push_back(std::move(tmp));
      auto it = cache_list_.end();
      it--;
      cache_mp_[frame_id] = it;
    }
  }
}

void LRUKReplacer::IncrementEvictaleSize() { evictable_size_++; }
void LRUKReplacer::DecrementEvictableSize() {
  if (evictable_size_ == 0) {
    return;
  }
  evictable_size_--;
}

//        * If a frame was previously evictable and is to be set to non-evictable, then size should
//            * decrement. If a frame was previously non-evictable and is to be set to evictable,
//    * then size should increment.
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> guard(latch_);
  //   1. if the frame_id is invalid, throw the exception and abort process
  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::exception();
  }
  if (history_map_.count(frame_id) == 0 && cache_mp_.count(frame_id) == 0) {
    return;
  }
  std::list<entry>::iterator index;
  if (history_map_.count(frame_id) > 0) {  // 如果说 frame id 存在于 history list 中
    index = history_map_[frame_id];
  } else {
    index = cache_mp_[frame_id];
  }
  if (index->second && !set_evictable) {
    DecrementEvictableSize();
    index->second = false;
  } else if (!index->second && set_evictable) {
    IncrementEvictaleSize();
    index->second = true;
  }
}

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

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);
  // Remove node, 不管是在 history 中还是 cache 中
  if (history_map_.count(frame_id) == 0 && cache_mp_.count(frame_id) == 0) {
    return;
  }
  if (history_map_.count(frame_id) > 0) {  // 在 history list 中
    if (!history_map_[frame_id]->second) {
      throw std::exception();
    }  // 如果说不存在就抛出异常
    DecrementEvictableSize();
    history_linked_list_.erase(history_map_[frame_id]);
    history_map_.erase(frame_id);
    counter_.erase(frame_id);
  } else {
    if (!cache_mp_[frame_id]->second) {
      throw std::exception();
    }
    DecrementEvictableSize();
    cache_list_.erase(cache_mp_[frame_id]);
    cache_mp_.erase(frame_id);
    counter_.erase(frame_id);
  }
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> guard(latch_);
  // 获得 【可驱逐的数据大小】
  return evictable_size_;
}

}  // namespace bustub