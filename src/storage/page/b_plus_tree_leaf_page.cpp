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
  int left = 0;
  int right = GetSize();
  if (left >= right) {
    return GetSize();
  }
  /** 返回的是 第一个大于等于 key 的编号 */
  while (left < right) {
    int mid = (left + right) >> 1;
    if (comparator(array_[mid].first, key) < 0) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }
  return left;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::LookUp(const KeyType &key, ValueType &value, const KeyComparator &comparator) const
    -> bool {
  /** 传入 key, 从 leafpage 中寻找对应的 Value */
  int index = KeyIndex(key, comparator);
  assert(index >= 0 && index <= GetSize());
  KeyType target_key = array_[index].first;
  // fix bug: 必须要 index < GetSize, 因为数据实际没有删除而是限制了 Size 的大小，强制访问还是能够访问到的
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
    /** 后面所有的数据都前移 -> 删除 index 位置的数据*/
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
  assert(GetSize() < GetMaxSize());
  if (GetSize() >= GetMaxSize()) {
    throw "The leaf page is full, can not insert into this page";
  }
  /** find the position of the leaf page*/
  int index = KeyIndex(key, comparator);
  /** make room for the new pair */
  for (int i = GetSize() - 1; i >= index; i--) {
    array_[i + 1] = array_[i];
  }
  array_[index] = MappingType{key, value};
  IncreaseSize(1);
  return GetSize();
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
  int total = GetMaxSize();
  assert(GetSize() == total);
  int copy_idx = total / 2;
  /** 7: 0 1 2 3 4 5 6 |  8: 0 1 2 3 4 5 6 7 */
  /**          ^       |             ^       */
  for (int i = copy_idx; i < total; i++) {
    recipient->array_[i - copy_idx].first = array_[i].first;
    recipient->array_[i - copy_idx].second = array_[i].second;
  }
  /** Set pointer*/

  SetSize(copy_idx);
  recipient->SetSize(total - copy_idx);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) -> const MappingType &{
  assert(index >= 0 && index < GetSize());
  return array_[index];
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient, int index_in_parent,
                                                  BufferPoolManager *buffer_pool_manager) {
  MappingType pair = GetItem(0);
  IncreaseSize(-1);
  /** 相较于拷贝数据, memmove 直接拷贝内存数据更快 */
  memmove(array_, array_ + 1, static_cast<size_t>(GetSize() * sizeof(MappingType)));
  /** 将 pair 的数据插入到 recipient 的末尾 */
  recipient->CopyLastFrom(pair);
  /** 更新一下 parent_page 的内容 */
  Page *page = buffer_pool_manager->FetchPage(GetParentPageId());
  auto *parent_node = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(page->GetData());
  parent_node->SetKeyAt(index_in_parent, array_[0].first);

  buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient, int parentIndex,
                                                   BufferPoolManager *buffer_pool_manager) {
  MappingType pair = array_[GetSize() - 1];
  IncreaseSize(-1);
  recipient->CopyFirstFrom(pair, parentIndex, buffer_pool_manager);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const std::pair<KeyType, ValueType> &item, int parentIndex,
                                               BufferPoolManager *buffer_pool_manager) {
  assert(GetSize() + 1 <= GetMaxSize());
  memmove(array_ + 1, array_, GetSize() * sizeof(MappingType));
  IncreaseSize(1);
  array_[0] = item;

  /** first entry 更新过后需要同时更新 relevant page*/
  Page *page = buffer_pool_manager->FetchPage(GetParentPageId());
  B_PLUS_TREE_INTERNAL_PAGE *parent_node = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(page->GetData());
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
