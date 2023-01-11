//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "common/logger.h"
#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

// 对应的 哈希函数 的实现
template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.push_back(std::make_shared<Bucket>(bucket_size_));  // dir_ 中插入一个 bucket
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {  // 能够通过
  int mask = (1 << global_depth_) - 1;                             // 获得 global 位全是 1 的掩码
  return std::hash<K>()(key) & mask;                               // 获得 (hash value) 的 底 global 位数据
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IncrementGlobalDepth() -> void {
  std::scoped_lock<std::mutex> lock(latch_);
  IncrementGlobalDepthinternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IncrementGlobalDepthinternal() -> void {
  global_depth_++;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  // 1. 通过 IndexOfKey() 获得 哈希桶的位置
  size_t index = IndexOf(key);
  std::shared_ptr<Bucket> bucket = dir_[index];
  // 2. 从哈希桶中查找数据
  return bucket->Find(key, value);
}

//* @brief Given the key, remove the corresponding key-value pair in the hash table.
//* Shrink & Combination is not required for this project
//* @param key The key to be deleted.
//* @return True if the key exists, false otherwise.
//*/

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  size_t index = IndexOf(key);
  std::shared_ptr<Bucket> bucket = dir_[index];
  return bucket->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> guard(latch_); /** 这里只是加了一把大锁*/
  InsertInstance(key, value);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::InsertInstance(const K &key, const V &value) {
  // 1. If the key exists, the value should be updated.
  size_t index = IndexOf(key);
  std::shared_ptr<Bucket> bucket = dir_[index];
  int local_depth = bucket->GetDepth();
  //  int global_depth = GetGlobalDepth();
  int global_depth = GetGlobalDepthInternal();
  if (bucket->IfExistsThenUpdate(key, value)) {
    return;
  }
  // 2. If the bucket is not full, then insert to the bucket. Return false 就代表着【满了】放不进去
  if (bucket->Insert(key, value)) {
    return;
  }
  // 3. Run here means the bucket is [full]
  num_buckets_++;
  if (local_depth < global_depth) {  // 只能是说 桶进行分裂， 但是目录不会进行一个扩张的操作
    bucket->IncrementDepth();
    // 直接将对应的 index ====> bucket pointer 进行一个重新的分配
    // local_depth 的最高位进行一个 bucket 的重新分配
    auto pairs = bucket->GetItems();
    int n = dir_.size();
    pairs.push_back(std::make_pair(key, value));
    bucket->ClearTheBucket();
    int mask = (1 << local_depth) - 1;
    int target = index & mask;
    auto old_bucket = dir_[index];
    auto new_bucket = std::make_shared<Bucket>(bucket_size_, local_depth + 1);
    for (int i = 0; i < n; i++) {
      if ((i & mask) == target) {
        if ((i & (1 << local_depth)) != 0) {
          dir_[i] = old_bucket;
        } else {
          dir_[i] = new_bucket;
        }
      }
    }
    RedistributeBucket1(pairs);
  } else {
    IncrementGlobalDepthinternal();
    bucket->IncrementDepth();
    int n = dir_.size();
    dir_.resize(n * 2);
    // 下面的操作同上面是一样的， 只不过是说 最高 bit 位为1的时候进行一个映射
    for (int i = 0; i < n; i++) {
      dir_[i + (1 << local_depth)] = dir_[i];
    }  // 相同的映射， 只有 bucket 进行分裂的时候才创建新的 shared_ptr
    auto pairs = bucket->GetItems();
    pairs.push_back(std::make_pair(key, value));
    bucket->ClearTheBucket();
    dir_[index + (1 << local_depth)] = std::make_shared<Bucket>(bucket_size_, local_depth + 1);  // 创建一个新的 bucket
    RedistributeBucket1(pairs);
  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket1(std::list<std::pair<K, V>> &list) -> void {
  // 获得了 list, 直接进行 hash 操作插入就行了， 不会出现 split 的现象的
  for (auto &it : list) {
    InsertInstance(it.first, it.second);
  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket) -> void {
  // 重新的分配，通过 global depth 直接进行哈希映射就行了
  auto pairs = bucket->GetItems();
  //  bucket->ClearTheBucket();
  for (auto &it : pairs) {
    Insert(it.first, it.second);  // 直接插入，因为肯定不满的
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(const ExtendibleHashTable<K, V>::Bucket &bucket) {
  // 进行拷贝构造
  this->depth_ = bucket.depth_;
  this->list_ = bucket.list_;
  this->size_ = bucket.size_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  // find the key int pairs
  auto index = std::find_if(list_.begin(), list_.end(), [&](const std::pair<K, V> v) { return v.first == key; });
  if (index == list_.end()) {
    return false;
  }
  value = (*index).second;
  return true;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  // True if key exists, return false otherwise
  auto index = std::find_if(list_.begin(), list_.end(), [&](const std::pair<K, V> v) { return v.first == key; });
  if (index == list_.end()) {
    return false;
  }
  list_.erase(index);
  return true;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  auto index = std::find_if(list_.begin(), list_.end(), [&](const std::pair<K, V> v) { return v.first == key; });
  if (index != list_.end()) {
    (*index).second = value;
    return true;
  }
  if (IsFull()) {  // 2.如果说 bucket is full, do nothing & return false
    return false;
  }
  std::pair<K, V> entry = std::make_pair(key, value);
  list_.push_back(entry);
  return true;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::IfExistsThenUpdate(const K &key, const V &value) -> bool {
  // 判断在本 bucket 中是否是存在 key
  auto index = std::find_if(list_.begin(), list_.end(), [&](const std::pair<K, V> v) { return v.first == key; });
  if (index != list_.end()) {
    (*index).second = value;
    return true;
  }
  return false;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
