/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page,
                                  BufferPoolManager *buffer_pool_manager, int index)
    : cur_page_(leaf_page), buffer_pool_manager_(buffer_pool_manager), cur_index_(index) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  return cur_page_ == nullptr;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return cur_page_->GetItem(cur_index_); }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  cur_index_++;
  // 还是需要最终判断一下是否到达了 End() -> leaf_nod, bpm, GetSize()
  if (cur_index_ >= cur_page_->GetSize()) {
    page_id_t next_page_id = cur_page_->GetNextPageId();
    buffer_pool_manager_->UnpinPage(cur_page_->GetPageId(), false);
    int max_size = cur_page_->GetMaxSize();
    if (next_page_id == INVALID_PAGE_ID) {
      cur_page_ = nullptr;    // in the end of the b plus tree
      cur_index_ = max_size;
    } else {
      Page *page = buffer_pool_manager_->FetchPage(next_page_id);
      cur_page_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(page->GetData());
      cur_index_ = 0;
    }
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
