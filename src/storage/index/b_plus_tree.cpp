#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintMaxSize() {
  std::cout << "internal page size : " << internal_max_size_ << " leaf page size " << leaf_max_size_ << std::endl;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  // 1. Find the leaf page
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_node = FindLeafPageRW(key, 0, OpType::Read, transaction);
  if (leaf_node == nullptr) {
    return false;
  }
  // 2. Find the value in the leaf page, use local function
  result->resize(1);
  ValueType tmp_value;
  bool exists = leaf_node->LookUp(key, tmp_value, comparator_);
  (*result)[0] = tmp_value;
  // 3. Unlock and unpin pages
  FreePagesInTrans(false, transaction, leaf_node->GetPageId());
  return exists;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::FreePagesInTrans(bool exclusive, Transaction *transaction, page_id_t cur) {
  TryUnlockRootPageId(exclusive);
  // compatible with single thread version
  if (transaction == nullptr) {
    assert(!exclusive && cur >= 0);
    Unlock(false, cur);
    buffer_pool_manager_->UnpinPage(cur, false);
    return;
  }

  for (Page *page : *transaction->GetPageSet()) {
    page_id_t page_id = page->GetPageId();
    Unlock(exclusive, page);
    buffer_pool_manager_->UnpinPage(page_id, exclusive);
    // 能够被删除掉的肯定是出现在 locked pages 中的
    if (transaction->GetDeletedPageSet()->find(page_id) != transaction->GetDeletedPageSet()->end()) {
      buffer_pool_manager_->DeletePage(page_id);
      transaction->GetDeletedPageSet()->erase(page_id);
    }
  }
  assert(transaction->GetDeletedPageSet()->empty());
  transaction->GetPageSet()->clear();
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnlatchAndUnpin(enum OpType op, Transaction *transaction) {
  // unlock first, cause if unpin first that will be a condition : page is not in the pool but the latch is still locked
  if (transaction == nullptr) {
    return;
  }
  // the pages that were latched during index operator
  auto pages = transaction->GetPageSet();
  for (auto page : *pages) {
    page_id_t page_id = page->GetPageId();
    if (op == OpType::Read) {
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page_id, false);
    } else {
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page_id, true);
    }
  }
  transaction->GetPageSet()->clear();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeftMostLeafPage() -> Page * {
  if (IsEmpty()) {
    return nullptr;
  }
  // find the root page
  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  if (page == nullptr) {
    throw "no page can find";
  }
  auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  while (!node->IsLeafPage()) {
    auto *internal_node = reinterpret_cast<InternalPage *>(node);
    page_id_t next_page_id = internal_node->ValueAt(0);
    /** Get the next page */
    Page *next_page = buffer_pool_manager_->FetchPage(next_page_id);
    auto *next_node = reinterpret_cast<BPlusTreePage *>(next_page);

    buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
    page = next_page;
    node = next_node;
  }
  return page;
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::IsSafe(N *node, OpType op) -> bool {
  int size = node->GetSize();
  if (op == OpType::InSert) {
    return node->GetSize() < node->GetMaxSize();
  }

  int min_size = node->GetMinSize() + 1;
  if (op == OpType::Delete) {
    return (node->IsLeafPage()) ? size >= min_size : size > min_size;
  }
  assert(false);  // invalid area
}

// who use this function should first get the latch of the root_page_id
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPageRW(const KeyType &key, bool left_most, OpType op, Transaction *transaction)
    -> B_PLUS_TREE_LEAF_PAGE_TYPE * {
  bool exclusive = (op != OpType::Read);
  LockRootPageId(exclusive);
  if (IsEmpty()) {
    TryUnlockRootPageId(exclusive);
    return nullptr;
  }

  auto node = CrabbingFetchPage(root_page_id_, op, -1, transaction);
  page_id_t next_page_id;
  page_id_t cur_page_id = root_page_id_;
  while (!node->IsLeafPage()) {
    auto internal_node = reinterpret_cast<InternalPage *>(node);
    next_page_id = left_most ? internal_node->ValueAt(0) : internal_node->LookUp(key, comparator_);
    // 更新 cur_page & cur_node
    node = CrabbingFetchPage(next_page_id, op, cur_page_id, transaction);
    cur_page_id = next_page_id;
  }
  return static_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(node);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CrabbingFetchPage(page_id_t page_id, OpType op, page_id_t previous, Transaction *transaction)
    -> BPlusTreePage * {
  // fetch page
  bool exclusive = (op != OpType::Read);
  Page *page = buffer_pool_manager_->FetchPage(page_id);
  Lock(exclusive, page);  // lock the page
  auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  // Release the old node if current node's op == Read Or safe & current node is [not root node]
  if (previous > 0 && (!exclusive || IsSafe(node, op))) {
    FreePagesInTrans(exclusive, transaction, previous);
  }
  // 释放完祖先节点之后才将当前的 page 加入到 page set中去
  if (transaction != nullptr) {
    transaction->AddIntoPageSet(page);
  }
  return node;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, const KeyComparator &comparator) -> Page * {
  /** 1. if the tree is empty, return nullptr */
  if (IsEmpty()) {
    return nullptr;
  }

  /** 2. search from root page */
  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  if (page == nullptr) {
    throw "no page can find";
  }
  auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  while (!node->IsLeafPage()) {
    auto internal_node = reinterpret_cast<InternalPage *>(node);
    page_id_t next_page_id = internal_node->LookUp(key, comparator);
    /** Get the next page */
    Page *next_page = buffer_pool_manager_->FetchPage(next_page_id);
    assert(next_page != nullptr);
    auto next_node = reinterpret_cast<BPlusTreePage *>(next_page);

    /** 只是说获得 page，并没有使用，dirty = false */
    buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
    page = next_page;
    node = next_node;
  }
  // 最后返回的这个 page 并没有 unpin 掉
  return page;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewNode(const KeyType &key, const ValueType &value) {
  Page *root_page = buffer_pool_manager_->NewPage(&root_page_id_);
  if (root_page == nullptr) {
    throw "out of memory";
  }
  auto *new_leaf_node = reinterpret_cast<LeafPage *>(root_page->GetData());
  new_leaf_node->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
  UpdateRootPageId(1);
  new_leaf_node->Insert(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  /** 1.如果说目前是一个空的 Tree，就需要新创建一个新的节点 */
  LockRootPageId(true);
  if (IsEmpty()) {
    StartNewNode(key, value);
    TryUnlockRootPageId(true);
    return true;
  }
  TryUnlockRootPageId(true);

  bool res = InsertIntoLeaf(key, value, comparator_, transaction);
  //  assert(Check());
  return res;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, const KeyComparator &comparator,
                                    Transaction *transaction) -> bool {
  auto *leaf_node = FindLeafPageRW(key, false, OpType::InSert, transaction);
  ValueType v;
  bool exist = leaf_node->LookUp(key, v, comparator);
  if (exist) {
    FreePagesInTrans(true, transaction);
    return false;
  }
  leaf_node->Insert(key, value, comparator);
  /** leaf page 满了，进行 Split 的操作*/
  if (leaf_node->GetSize() >= leaf_node->GetMaxSize()) {
    LeafPage *new_leaf_node = Split(leaf_node, transaction);
    // unpin the new leaf node in the below function
    InsertIntoParent(leaf_node, new_leaf_node->KeyAt(0), new_leaf_node, transaction);
  }
  // 最后直接释放掉所有的锁 [leaf node unpin here in early version]
  FreePagesInTrans(true, transaction);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::Split(N *cur_node, Transaction *transaction) -> N * {
  // 创建一个新的 page， 用于存放新的 page, 将一半的数据转移到新的 node
  page_id_t new_page_id;
  Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);
  assert(new_page != nullptr);
  // TODO(shaoguixin): here not need to lock new page(但是老版本需要)
  new_page->WLatch();
  transaction->AddIntoPageSet(new_page);
  N *new_node;  // for return
  if (cur_node->IsLeafPage()) {
    auto leaf_node = reinterpret_cast<LeafPage *>(cur_node);
    auto new_leaf_node = reinterpret_cast<LeafPage *>(new_page);
    new_leaf_node->Init(new_page_id, leaf_node->GetParentPageId(), leaf_max_size_);  // set new page's parent node
    leaf_node->MoveHalfTo(new_leaf_node);
    /** 叶子节点更新双向链表*/
    new_leaf_node->SetNextPageId(leaf_node->GetNextPageId());
    leaf_node->SetNextPageId(new_leaf_node->GetPageId());
    new_node = reinterpret_cast<N *>(new_leaf_node);
  } else {
    auto internal_node = reinterpret_cast<InternalPage *>(cur_node);
    auto new_internal_node = reinterpret_cast<InternalPage *>(new_page);
    new_internal_node->Init(new_page_id, internal_node->GetParentPageId(), internal_max_size_);
    internal_node->MoveHalfTo(new_internal_node, buffer_pool_manager_);
    new_node = reinterpret_cast<N *>(new_internal_node);
  }
  /** 对于 new page 还没有 UnPin，需要在外面进行 unpin 的操作*/
  return new_node;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  /** 如果说目前就是一个 root node，那么只能是创建一个新的 root page*/
  if (old_node->IsRootPage()) {
    page_id_t new_root_id;
    Page *page = buffer_pool_manager_->NewPage(&new_root_id);
    assert(page != nullptr);
    assert(page->GetPinCount() == 1);
    auto new_root_node = reinterpret_cast<InternalPage *>(page->GetData());

    // New Page init
    new_root_node->Init(new_root_id, INVALID_PAGE_ID, internal_max_size_);
    root_page_id_ = new_root_id;
    UpdateRootPageId(0);  // TODO(sgx): new root here, just unpin rather than unlock

    // Set Parent page
    // 将数据给拆分开了 internal page 的首位key不存东西
    new_root_node->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    old_node->SetParentPageId(new_root_id);
    new_node->SetParentPageId(new_root_id);
    // this two node is new node which means no need to Unlatch[cause it not even locked]
    buffer_pool_manager_->UnpinPage(new_root_node->GetPageId(), true);
    return;
  }
  /** 如果说目前不是 root page，那么需要找到 parent page */
  page_id_t parent_page_id = old_node->GetParentPageId();
  Page *parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
  assert(parent_page != nullptr);
  auto parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());

  //  buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
  // insert node to parent node after the old node
  parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());

  /** 如果 internal page 仍然是满了的，继续分裂 */
  if (parent_node->GetSize() > parent_node->GetMaxSize()) {
    /** recursive insertion */
    InternalPage *new_internal_node = Split(parent_node, transaction);
    InsertIntoParent(parent_node, new_internal_node->KeyAt(0), new_internal_node, transaction);
  }
  buffer_pool_manager_->UnpinPage(parent_page_id, true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) {
    return;
  }
  /** 1. find the leaf page*/
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_node = FindLeafPageRW(key, false, OpType::Delete, transaction);
  int cur_size = leaf_node->RemoveAndDeleteRecord(key, comparator_);
  /** 如果说 leaf node 需要进行合并 Or 重分配 */
  if (cur_size < leaf_node->GetMinSize()) {
    MergeOrRedistribute(leaf_node, transaction);
  }
  FreePagesInTrans(true, transaction);
  //  assert(Check());
}

/*
 * @return: true means target leaf page should be deleted, false otherwise
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::MergeOrRedistribute(N *node, Transaction *transaction) -> bool {
  if (node->IsRootPage()) {
    bool remove_old_root = AdjustRoot(node, transaction);
    if (remove_old_root) {
      transaction->AddIntoDeletedPageSet(node->GetPageId());
    }
    return remove_old_root;
  }
  /** Find the sibling node */
  N *node2;
  bool is_next_sibling = FindSibling(node, node2, transaction);  // locked sibling node temporary
  Page *parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  auto *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  /** 能够进行合并的操作, leaf node's merge condition is sum < max_size */
  if (node->GetSize() + node2->GetSize() <= node->GetMaxSize()) {
    // assume node is after node2
    if (is_next_sibling) {
      std::swap(node, node2);
    }
    int remove_idx = parent_node->ValueIndex(node->GetPageId());
    // 进行了 merge 操作[may be recursion call the function]， 能够解锁所有的父节点祖先节点的锁
    Merge(node2, node, parent_node, remove_idx, transaction);

    buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
    return true;
  }
  /** borrow entry from node2 */
  int node_in_parent_index = parent_node->ValueIndex(node->GetPageId());
  Redistribute(node2, node, node_in_parent_index);  // unpin node, node2
  // unlock the sibling node after some operators (actually no need, after return it will unlock)
  buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), false);
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::IfCanMerge(N *node, N *neighbor_node) -> bool {
  if (node->IsLeafPage()) {
    return node->GetSize() + neighbor_node->GetSize() < node->GetMaxSize();  // 不能够等于 maxsize 的
  }
  return node->GetSize() + neighbor_node->GetSize() <= node->GetMaxSize();
}

/*
 * 进行 node 之间的 merge 操作，将所有的 KV 移动到 sibling node 中去，然后
 * buffer pool manager 删除掉对应的 page
 * @parm  neighbor_node  sibling page of input "node"
 * @parm  node           input from method
 *
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::Merge(N *&neighbor_node, N *&node,
                           BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *&parent, int index,
                           Transaction *transaction) -> bool {
  // assume neighbor node is before node
  assert(node->GetSize() + neighbor_node->GetSize() <= node->GetMaxSize());
  if (node->IsLeafPage()) {
    auto *leaf_node = reinterpret_cast<LeafPage *>(node);
    auto *leaf_neighbor = reinterpret_cast<LeafPage *>(neighbor_node);

    leaf_node->MoveAllTo(leaf_neighbor);
  } else {
    auto *internal_node = reinterpret_cast<InternalPage *>(node);
    auto *internal_neighbor = reinterpret_cast<InternalPage *>(neighbor_node);

    internal_node->MoveAllTo(internal_neighbor, index, buffer_pool_manager_);
  }
  //  buffer_pool_manager_->UnpinPage(pid, true);
  //  buffer_pool_manager_->DeletePage(pid);
  transaction->AddIntoDeletedPageSet(node->GetPageId());
  //  buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
  parent->Remove(index);
  /** parent node less than min_size or equal to min_size */
  if (parent->GetSize() <= parent->GetMinSize()) {
    return MergeOrRedistribute(parent, transaction);
  }
  return false;
}

/*
 * Redistribute key & value pairs from one page to another.
 * If index == 0, move sibling page's first entry pair into end of input "node",
 * node <-> neighbor_node
 * otherwise : neighbor_node <-> node
 */

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  /** index is "node"'s index in parent node */
  if (index == 0) {
    // 需要更改的是 neighbor node 的对应的在 parent node 中的索引数据
    neighbor_node->MoveFirstToEndOf(node, index + 1, buffer_pool_manager_);
  } else {
    // 需要更改的是 node 对应的在 parent node 中的索引数据
    neighbor_node->MoveLastToFrontOf(node, index, buffer_pool_manager_);
  }
  //  buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
}

/*
 * Find the sibling of the node
 * @return: true if the sibling is next sibling, false means prev sibling
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::FindSibling(N *node, N *&sibling, Transaction *transaction) -> bool {
  Page *parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  auto *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  int index = parent_node->ValueIndex(node->GetPageId());
  int sibling_index = index - 1;
  if (index == 0) {  // 没有 prev sibling, 选择 next sibling
    sibling_index = index + 1;
  }

  // sibling node 同样需要进行 Lock， 传入 -1 防止将祖先 lock 全部解锁
  auto *sibling_page = CrabbingFetchPage(parent_node->ValueAt(sibling_index), OpType::Delete, -1, transaction);
  sibling = reinterpret_cast<N *>(sibling_page);

  buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), false);
  return index == 0;  // 如果是 next sibling 就是返回 true
}

/*
 * @return: true means the root page should be deleted, false otherwise
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node, Transaction *transaction) -> bool {
  /** 2. 如果说是 Leaf node 并且已经是空了的, 删除对应的 page */
  if (old_root_node->IsLeafPage()) {
    assert(old_root_node->GetSize() == 0);
    assert(old_root_node->GetParentPageId() == INVALID_PAGE_ID);
    // 稍后从返回值中就能够标记为删除的状态，最后进行删除
    //    buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), false);
    //  buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(0);
    return true;
  }
  /** 如果说 root node 是internal节点 & has only one size*/
  if (!old_root_node->IsLeafPage() && old_root_node->GetSize() == 1) {
    // 需要重新设置一下 child node 为新的 root node
    auto *root_node = reinterpret_cast<InternalPage *>(old_root_node);
    page_id_t new_root_page_id = root_node->AdjustRootForInternal();
    root_page_id_ = new_root_page_id;
    UpdateRootPageId(0);

    // 重复获取了 child node[now is root node]
    Page *new_root_page = buffer_pool_manager_->FetchPage(root_page_id_);
    assert(new_root_page != nullptr);
    auto *new_root_node = reinterpret_cast<BPlusTreePage *>(new_root_page->GetData());
    new_root_node->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(new_root_page_id, true);
    //    buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), false);
    //    buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
    return true;
  }
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  KeyType tmp_key;
  auto *begin_node = FindLeafPageRW(tmp_key, true);
  TryUnlockRootPageId(false);
  return INDEXITERATOR_TYPE(begin_node, buffer_pool_manager_, 0);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  // 寻找以 Leaf node 中以 key 为开头的 index iterator ?
  auto *begin_node = FindLeafPageRW(key);
  TryUnlockRootPageId(false);
  if (begin_node == nullptr) {
    return INDEXITERATOR_TYPE(nullptr, buffer_pool_manager_, 0);
  }
  int index = begin_node->KeyIndex(key, comparator_);
  assert(comparator_(begin_node->KeyAt(index), key) == 0);
  // not unpin in current condition
  return INDEXITERATOR_TYPE(begin_node, buffer_pool_manager_, index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  return INDEXITERATOR_TYPE(nullptr, buffer_pool_manager_, leaf_max_size_);
  //  // return the end of the key & value pair in the leaf node: last leaf node's last entry
  //  Page *page = FindLeftMostLeafPage();
  //  auto leaf_node = reinterpret_cast<LeafPage *>(page->GetData());
  //  while (leaf_node->GetNextPageId() != INVALID_PAGE_ID) {
  //    page_id_t next_page_id = leaf_node->GetNextPageId();
  //    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);
  //
  //    Page *next_page = buffer_pool_manager_->FetchPage(next_page_id);
  //    leaf_node = reinterpret_cast<LeafPage *>(next_page->GetData());
  //  }
  //  // now we have found the last leaf node of the b plus tree
  //  // the next step is to find the last entry of the leaf node(not legal entry)
  //  return INDEXITERATOR_TYPE(leaf_node, buffer_pool_manager_, leaf_node->GetSize());
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FetchPage(page_id_t page_id) -> BPlusTreePage * {
  Page *page = buffer_pool_manager_->FetchPage(page_id);
  return reinterpret_cast<BPlusTreePage *>(page->GetData());
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsPageCorr(page_id_t pid, std::pair<KeyType, KeyType> &out) -> bool {
  if (IsEmpty()) {
    return true;
  }
  auto node = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(pid));
  if (node == nullptr) {
    throw "all page are pinned while isPageCorr";
  }
  bool res = true;
  if (node->IsLeafPage()) {
    auto page = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(node);
    int size = page->GetSize();
    res = res && (size >= node->GetMinSize() && size <= node->GetMaxSize());
    /** if not legal, the output assigned with the key to */
    for (int i = 1; i < size; i++) {
      if (comparator_(page->KeyAt(i - 1), page->KeyAt(i)) > 0) {
        res = false;
        break;
      }
    }
    out = std::pair<KeyType, KeyType>{page->KeyAt(0), page->KeyAt(size - 1)};
  } else {
    auto page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(node);
    int size = page->GetSize();
    res = res && (size >= node->GetMinSize() && size <= node->GetMaxSize());
    std::pair<KeyType, KeyType> left;
    std::pair<KeyType, KeyType> right;
    for (int i = 1; i < size; i++) {
      if (i == 1) {
        res = res && IsPageCorr(page->ValueAt(0), left);
      }
      res = res && IsPageCorr(page->ValueAt(i), right);
      res = res && (comparator_(page->KeyAt(i), left.second) > 0 && comparator_(page->KeyAt(i), right.first) <= 0);
      res = res && (i == 1 || comparator_(page->KeyAt(i - 1), page->KeyAt(i)) < 0);
      if (!res) {
        break;
      }
      left = right;
    }
    out = std::pair<KeyType, KeyType>{page->KeyAt(0), page->KeyAt(size - 1)};
  }
  buffer_pool_manager_->UnpinPage(pid, false);
  return res;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsBalanced(page_id_t pid) -> int {
  if (IsEmpty()) {
    return 1;
  }
  auto node = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(pid));
  if (node == nullptr) {
    throw "all page are pinned while is_balanced";
  }
  int ret = 0;
  if (!node->IsLeafPage()) {
    auto page = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(node);
    int last = -2;
    for (int i = 0; i < page->GetSize(); i++) {
      int cur = IsBalanced(page->ValueAt(i));
      if (cur >= 0 && last == -2) {
        last = cur;
        ret = last + 1;
      } else if (last != cur) {
        ret = -1;
        break;
      }
    }
  }
  buffer_pool_manager_->UnpinPage(pid, false);
  return ret;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Check() -> bool {
  std::pair<KeyType, KeyType> in;
  bool is_page_in_order_and_size_corr = IsPageCorr(root_page_id_, in);
  bool is_bal = (IsBalanced(root_page_id_) >= 0);
  //  bool is_all_unpin = buffer_pool_manager_->Check();
  if (!is_page_in_order_and_size_corr) {
    std::cout << "problem in page order or page size" << std::endl;
    std::cout << "first: " << in.first << " second: " << in.second << std::endl;
  }
  if (!is_bal) {
    std::cout << "problem in balance" << std::endl;
  }
  return is_page_in_order_and_size_corr && is_bal;
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
