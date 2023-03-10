//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetPageId(page_id);
  SetSize(0);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  //  assert(index >= 0 && index < GetSize());
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  assert(index >= 0 && index < GetSize());
  array_[index].first = key;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

/*
 * Helper method to find the index which value is equal to the input value
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const -> int {
  for (int i = 0; i < GetSize(); i++) {
    if (value != ValueAt(i)) {
      continue;
    }
    return i;
  }
  return -1;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::LookUp(const KeyType &key, const KeyComparator &comparator) -> ValueType {
  // ??? index == 1 ?????????????????????????????????????????????????????????
  for (int i = 1; i < GetSize(); i++) {
    KeyType cur_key = array_[i].first;
    if (comparator(key, cur_key) < 0) {
      return array_[i - 1].second;
    }
  }
  return array_[GetSize() - 1].second;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  /** ???????????? internal-node ??????????????????key */
  array_[0].second = old_value;
  for (int i = 1; i <= 1; i++) {
    array_[i].first = new_key;
    array_[i].second = new_value;
  }
  SetSize(2);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) -> int {
  int index = ValueIndex(old_value) + 1;
  assert(index > 0);
  IncreaseSize(1);
  int cur_size = GetSize();
  for (int i = cur_size - 1; i > index; i--) {
    array_[i].first = array_[i - 1].first;
    array_[i].second = array_[i - 1].second;
  }
  array_[index].first = new_key;
  array_[index].second = new_value;
  return static_cast<int>(cur_size);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage<KeyType, ValueType, KeyComparator> *recipient,
                                               int index_in_parent, BufferPoolManager *buffer_pool_manager) {
  int start = recipient->GetSize();
  page_id_t recip_page_id = recipient->GetPageId();
  // 1. find the parent page
  Page *page = buffer_pool_manager->FetchPage(GetParentPageId());
  assert(page != nullptr);
  auto *parent_node = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());

  SetKeyAt(0, parent_node->KeyAt(index_in_parent));
  buffer_pool_manager->UnpinPage(parent_node->GetPageId(), false);
  for (int i = 0; i < GetSize(); i++) {
    recipient->array_[start + i].first = array_[i].first;
    recipient->array_[start + i].second = array_[i].second;
    /** update children's parent page*/
    Page *child_page = buffer_pool_manager->FetchPage(array_[i].second);
    auto *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    child_node->SetParentPageId(recip_page_id);
    buffer_pool_manager->UnpinPage(array_[i].second, true);
  }
  // update relevant key & value in its parent page
  recipient->SetSize(start + GetSize());
  assert(recipient->GetSize() <= GetMaxSize());
  SetSize(0);
  //  buffer_pool_manager->UnpinPage(GetPageId(), true);
  //  buffer_pool_manager->UnpinPage(recipient->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::UpdateAllNodesParent(BufferPoolManager *bmp) {
  for (int i = 0; i < GetSize(); i++) {
    ValueType value = array_[i].second;
    Page *child_page = bmp->FetchPage(value);
    auto *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    child_node->SetParentPageId(GetPageId());
    bmp->UnpinPage(value, true);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  assert(index >= 0 && index < GetSize());
  for (int i = index + 1; i < GetSize(); i++) {
    array_[i - 1] = array_[i];
  }
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, int index_in_parent,
                                                      BufferPoolManager *buffer_pool_manager) {
  MappingType pair{KeyAt(0), ValueAt(0)};
  IncreaseSize(-1);

  // not memmove, just directly copy nodes
  //    memmove(array_, array_ + 1, static_cast<size_t>(GetSize() * sizeof(MappingType)));
  for (int i = 1; i < GetSize(); i++) {
    array_[i - 1] = array_[i];
  }

  recipient->CopyLastFrom(pair, buffer_pool_manager);
  // update child node's parent page id
  page_id_t child_page_id = pair.second;
  Page *page = buffer_pool_manager->FetchPage(child_page_id);
  assert(page != nullptr);
  auto *child_node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  child_node->SetParentPageId(recipient->GetPageId());
  assert(child_node->GetParentPageId() == recipient->GetPageId());
  buffer_pool_manager->UnpinPage(child_page_id, true);
  // ????????????????????? Key??????????????????????????????
  page = buffer_pool_manager->FetchPage(GetParentPageId());
  auto *parent_node = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());
  parent_node->SetKeyAt(parent_node->ValueIndex(GetPageId()), array_[0].first);
  buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  assert(GetSize() + 1 <= GetMaxSize());
  array_[GetSize()] = pair;
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, int parent_index,
                                                       BufferPoolManager *buffer_pool_manager) {
  MappingType pair{KeyAt(GetSize() - 1), ValueAt(GetSize() - 1)};
  IncreaseSize(-1);
  recipient->CopyFirstFrom(pair, parent_index, buffer_pool_manager);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, int index_in_parent,
                                                   BufferPoolManager *buffer_pool_manager) {
  // ??? first entry ????????? pair
  assert(GetSize() + 1 < GetMaxSize());

  for (int i = GetSize(); i >= 1; i--) {
    array_[i] = array_[i - 1];
  }

  IncreaseSize(1);
  array_[0] = pair;

  // update new child_node's parent page
  page_id_t child_page_id = pair.second;
  Page *page = buffer_pool_manager->FetchPage(child_page_id);
  assert(page != nullptr);
  auto *child_node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  child_node->SetParentPageId(GetPageId());
  assert(child_node->GetParentPageId() == GetPageId());
  buffer_pool_manager->UnpinPage(child_node->GetPageId(), true);

  // update parent_node ????????? Key(?????? first entry ?????????)?????????????????????
  page = buffer_pool_manager->FetchPage(GetParentPageId());
  auto *parent_node = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());
  parent_node->SetKeyAt(index_in_parent, array_[0].first);
  buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::AdjustRootForInternal() -> ValueType {
  /** ?????????????????? root node, ???????????? Value???0???*/
  assert(GetSize() == 1);
  ValueType res = ValueAt(0);
  IncreaseSize(-1);
  return res;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(B_PLUS_TREE_INTERNAL_PAGE_TYPE *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  assert(GetSize() == GetMaxSize() + 1);
  assert(recipient != nullptr);
  int total = GetSize();
  /** ?????? internal page ??????????????????????????? ???????????????????????? & ??????????????? node */
  int copy_idx = total / 2;
  page_id_t recip_page_id = recipient->GetPageId();
  for (int i = copy_idx; i < total; i++) {
    recipient->array_[i - copy_idx].first = array_[i].first;
    recipient->array_[i - copy_idx].second = array_[i].second;
    // update children's parent page
    Page *child_raw_page = buffer_pool_manager->FetchPage(array_[i].second);
    auto child_node = reinterpret_cast<BPlusTreePage *>(child_raw_page->GetData());
    child_node->SetParentPageId(recip_page_id);
    buffer_pool_manager->UnpinPage(array_[i].second, true);
  }
  /** Set the size of pages */
  SetSize(copy_idx);
  recipient->SetSize(total - copy_idx);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub