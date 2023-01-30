//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/b_plus_tree.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <atomic>
#include <queue>
#include <string>
#include <utility>
#include <vector>

#include "concurrency/transaction.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

/**
 * Main class providing the API for the Interactive B+ Tree.
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

  enum OpType { Read, Write, InSert, Delete };

 public:
  explicit BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                     int leaf_max_size = LEAF_PAGE_SIZE, int internal_max_size = INTERNAL_PAGE_SIZE);

  void PrintMaxSize();

  // Returns true if this B+ tree has no keys and values.
  auto IsEmpty() const -> bool;

  // Insert a key-value pair into this B+ tree.
  auto Insert(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr) -> bool;

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key, Transaction *transaction = nullptr);

  // 删除的时候 如果删除的是根结点的数据 进行更改
  auto AdjustRoot(BPlusTreePage *old_root_node, Transaction *transaction = nullptr) -> bool;

  template <typename N>
  auto MergeOrRedistribute(N *node, Transaction *transaction = nullptr) -> bool;

  template <typename N>
  auto IfCanMerge(N *node, N *neighbor_node) -> bool;

  template <typename N>
  auto FindSibling(N *node, N *&sibling, Transaction *transaction = nullptr) -> bool;

  template <typename N>
  void Redistribute(N *neighbor_node, N *node, int index);

  template <typename N>
  auto Merge(N *&neighbor_node, N *&node, BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *&parent, int index,
             Transaction *transaction = nullptr) -> bool;

  // return the value associated with a given key
  auto GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction = nullptr) -> bool;

  void UnlatchAndUnpin(enum OpType op, Transaction *transaction = nullptr);

  void FreePagesInTrans(bool exclusive, Transaction *transaction, page_id_t cur = -1);

  // Find the leaf page which contains the key
  auto FindLeafPage(const KeyType &key, const KeyComparator &comparator) -> Page *;

  auto FetchPage(page_id_t page_id) -> BPlusTreePage *;

  // fetch page & release father pages if safe
  auto CrabbingFetchPage(page_id_t page_id, OpType op, page_id_t previous, Transaction *transaction) -> BPlusTreePage *;

  auto FindLeafPageRW(const KeyType &key, bool left_most = false, enum OpType op = OpType::Read,
                      Transaction *transaction = nullptr) -> B_PLUS_TREE_LEAF_PAGE_TYPE *;

  template <typename N>
  auto IsSafe(N *node, enum OpType op) -> bool;

  auto FindLeftMostLeafPage() -> Page *;

  // Insert into leaf page
  auto InsertIntoLeaf(const KeyType &key, const ValueType &value, const KeyComparator &comparator,
                      Transaction *transaction = nullptr) -> bool;

  template <typename N>
  auto Split(N *node, Transaction *transaction) -> N *;

  void InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                        Transaction *transaction = nullptr);

  // Start a new node
  void StartNewNode(const KeyType &key, const ValueType &value);

  // return the page id of the root node
  auto GetRootPageId() -> page_id_t;

  inline void Lock(bool exclusive, Page *page) {
    if (exclusive) {
      page->WLatch();
    } else {
      page->RLatch();
    }
  }

  inline void Unlock(bool exclusive, Page *page) {
    if (exclusive) {
      page->WUnlatch();
    } else {
      page->RUnlatch();
    }
  }

  // if we just have pageid & want to unpin it
  inline void Unlock(bool exclusive, page_id_t pageId) {
    Page *page = buffer_pool_manager_->FetchPage(pageId);
    Unlock(exclusive, page);
    buffer_pool_manager_->UnpinPage(pageId, exclusive);  // if the page is dirty
  }

  // index iterator
  auto Begin() -> INDEXITERATOR_TYPE;
  auto Begin(const KeyType &key) -> INDEXITERATOR_TYPE;
  auto End() -> INDEXITERATOR_TYPE;

  // print the B+ tree
  void Print(BufferPoolManager *bpm);

  // draw the B+ tree
  void Draw(BufferPoolManager *bpm, const std::string &outf);

  // read data from file and insert one by one
  void InsertFromFile(const std::string &file_name, Transaction *transaction = nullptr);

  // read data from file and remove one by one
  void RemoveFromFile(const std::string &file_name, Transaction *transaction = nullptr);

  // DEBUG
  auto IsPageCorr(page_id_t pid, std::pair<KeyType, KeyType> &out) -> bool;

  auto IsBalanced(page_id_t pid) -> int;

  auto Check() -> bool;

  void LockRootPageId(bool exclusive) {
    if (exclusive) {
      mutex_.WLock();
    } else {
      mutex_.RLock();
    }
    root_locked_cnt++;
  }

  void TryUnlockRootPageId(bool exclusive) {
    // 如果已经是提前解开了(在 findleafpage 中)，就不用走这一步了
    if (root_locked_cnt > 0) {
      if (exclusive) {
        mutex_.WUnlock();
      } else {
        mutex_.RUnlock();
      }
      root_locked_cnt--;
    }
  }

 private:
  void UpdateRootPageId(int insert_record = 0);

  /* Debug Routines for FREE!! */
  void ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const;

  void ToString(BPlusTreePage *page, BufferPoolManager *bpm) const;

  // member variable
  std::string index_name_;
  page_id_t root_page_id_;
  BufferPoolManager *buffer_pool_manager_;
  KeyComparator comparator_;
  int leaf_max_size_;
  int internal_max_size_;
  ReaderWriterLatch mutex_;
  // thread local variable to store the count of root locked
  inline static thread_local int root_locked_cnt;
};

}  // namespace bustub
