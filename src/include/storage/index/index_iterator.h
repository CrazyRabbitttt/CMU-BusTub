//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page, BufferPoolManager *buffer_pool_manager, int index = 0);
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    return cur_page_ == itr.cur_page_ && cur_index_ == itr.cur_index_;
  }

  auto operator!=(const IndexIterator &itr) const -> bool { return !(itr == *this); }

 private:
  // add your own private member variables here
  void UnlockAndUnpin() {
    buffer_pool_manager_->FetchPage(cur_page_->GetPageId())->RUnlatch();
    buffer_pool_manager_->UnpinPage(cur_page_->GetPageId(), false);
    buffer_pool_manager_->UnpinPage(cur_page_->GetPageId(), false);
  }

  B_PLUS_TREE_LEAF_PAGE_TYPE *cur_page_;
  BufferPoolManager *buffer_pool_manager_;
  int cur_index_;
};

}  // namespace bustub
