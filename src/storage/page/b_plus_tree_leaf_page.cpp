//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetPageId(page_id);
  SetSize(0);
  SetParentPageId(parent_id);
  SetNextPageId(INVALID_PAGE_ID);
  SetMaxSize(max_size);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const -> int {
  assert(GetSize() >= 0);

  page_id_t start = 0;
  page_id_t end = GetSize() - 1;
  while (start <= end) {
    int mid = (end - start) / 2 + start;
    if (comparator(array_[mid].first, key) >= 0) {
      end = mid - 1;
    } else {
      start = mid + 1;
    }
  }
  return end + 1;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::LookUp(const KeyType &key, ValueType &value, const KeyComparator &comparator) const
    -> bool {
  /** ?????? key, ??? leafpage ?????????????????? Value */
  int index = KeyIndex(key, comparator);
  KeyType target_key = array_[index].first;
  // fix bug: ????????? index < GetSize, ????????????????????????????????????????????? Size ????????????????????????????????????????????????
  if (index < GetSize() && comparator(target_key, key) == 0) {
    value = array_[index].second;
    return true;
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) -> int {
  int index = KeyIndex(key, comparator);
  if (index < GetSize() && comparator(array_[index].first, key) == 0) {
    //    memmove(array_ + index, array_ + index + 1,
    //            static_cast<size_t>((GetSize() - index - 1)*sizeof (MappingType)));
    /** ?????????????????????????????? -> ?????? index ???????????????*/
    for (int i = index + 1; i < GetSize(); i++) {
      array_[i - 1] = array_[i];
    }
    IncreaseSize(-1);
  }
  return GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> int {
  /** find the position of the leaf page*/
  int index = KeyIndex(key, comparator);
  assert(index >= 0);
  IncreaseSize(1);
  int cur_size = GetSize();
  /** make room for the new pair */
  for (int i = cur_size - 1; i > index; i--) {
    array_[i] = array_[i - 1];
  }
  array_[index] = MappingType{key, value};
  return cur_size;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient) {
  assert(recipient != nullptr);
  // recipient node is before node
  int index = recipient->GetSize();
  for (int i = 0; i < GetSize(); i++) {
    recipient->array_[index + i].first = array_[i].first;
    recipient->array_[index + i].second = array_[i].second;
  }
  // Set pointer
  recipient->SetNextPageId(GetNextPageId());
  // Set size
  recipient->IncreaseSize(GetSize());
  SetSize(0);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(B_PLUS_TREE_LEAF_PAGE_TYPE *recipient) {
  assert(recipient != nullptr);
  int total = GetMaxSize() + 1;
  assert(GetSize() == total);
  int copy_idx = total / 2;
  /** 7: 0 1 2 3 4 5 6 |  8: 0 1 2 3 4 5 6 7 */
  /**          ^       |             ^       */
  for (int i = copy_idx; i < total; i++) {
    recipient->array_[i - copy_idx].first = array_[i].first;
    recipient->array_[i - copy_idx].second = array_[i].second;
  }
  /** Set pointer*/
  recipient->SetNextPageId(GetNextPageId());
  SetNextPageId(recipient->GetPageId());
  SetSize(copy_idx);
  recipient->SetSize(total - copy_idx);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) -> const MappingType & {
  assert(index >= 0 && index < GetSize());
  return array_[index];
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient, int index_in_parent,
                                                  BufferPoolManager *buffer_pool_manager) {
  MappingType pair = GetItem(0);
  IncreaseSize(-1);
  //    memmove(array_, array_ + 1, static_cast<size_t>(GetSize()*sizeof(MappingType)));
  /** ?????????????????????, memmove ?????????????????????????????? */
  for (int i = 1; i <= GetSize(); i++) {
    array_[i - 1] = array_[i];
  }
  //  memmove(array_, array_ + 1, static_cast<size_t>(GetSize() * sizeof(MappingType)));
  /** ??? pair ?????????????????? recipient ????????? */
  recipient->CopyLastFrom(pair);
  /** ???????????? parent_page ????????? */
  Page *page = buffer_pool_manager->FetchPage(GetParentPageId());
  auto *parent_node = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(page->GetData());
  parent_node->SetKeyAt(parent_node->ValueIndex(GetPageId()), array_[0].first);

  buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient, int parentIndex,
                                                   BufferPoolManager *buffer_pool_manager) {
  MappingType pair = GetItem(GetSize() - 1);
  IncreaseSize(-1);
  recipient->CopyFirstFrom(pair, parentIndex, buffer_pool_manager);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const std::pair<KeyType, ValueType> &item, int parentIndex,
                                               BufferPoolManager *buffer_pool_manager) {
  assert(GetSize() + 1 < GetMaxSize());
  for (int i = GetSize(); i > 0; i--) {
    array_[i] = array_[i - 1];
  }
  //  memmove(array_ + 1, array_, GetSize() * sizeof(MappingType));
  IncreaseSize(1);
  array_[0] = item;

  /** first entry ?????????????????????????????? relevant page*/
  Page *page = buffer_pool_manager->FetchPage(GetParentPageId());
  auto *parent_node = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(page->GetData());
  parent_node->SetKeyAt(parentIndex, array_[0].first);
  buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(MappingType &pair) {
  assert(GetSize() + 1 <= GetMaxSize());
  array_[GetSize()] = pair;
  IncreaseSize(1);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
