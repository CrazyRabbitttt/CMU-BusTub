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
  result->resize(1);
  /** 1. Find the corresponding leaf page of the key*/
  Page *leaf_page = FindLeafPage(key, comparator_);
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  /** 2. Find the value in the leaf page, use local function */
  ValueType tmp_value;
  bool exists = leaf_node->LookUp(key, tmp_value, comparator_);
  (*result)[0] = tmp_value;
  return exists;
}

INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, const KeyComparator &comparator) {
  /** 1. if the tree is empty, return nullptr */
  if (IsEmpty()) {
    return nullptr;
  }
  /** 2. search from root page */
  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  if (page == nullptr) {
    throw "no page can find";
  }
  BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  while (!node->IsLeafPage()) {
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(node);
    page_id_t next_page_id = internal_node->LookUp(key, comparator);
    /** Get the next page */
    Page *next_page = buffer_pool_manager_->FetchPage(next_page_id);
    BPlusTreePage *next_node = reinterpret_cast<BPlusTreePage *>(next_page);

    /** 只是说获得 page，并没有使用，dirty = false */
    buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
    page = next_page;
    node = next_node;
  }
  buffer_pool_manager_->UnpinPage(root_page_id_, false);
  return page;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewNode(const KeyType &key, const ValueType &value) {
  Page *root_page = buffer_pool_manager_->NewPage(&root_page_id_);
  if (root_page == nullptr) {
    throw "out of memory";
  }
  LeafPage *new_leaf_node = reinterpret_cast<LeafPage *>(root_page->GetData());
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
  /** 如果说目前是一个空的 Tree，就需要新创建一个新的节点 */
  if (IsEmpty()) {
    StartNewNode(key, value);
    return true;
  }
  return InsertIntoLeaf(key, value, comparator_);
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
  Page *leaf_page = FindLeafPage(key, comparator);
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  ValueType v;
  bool exist = leaf_node->LookUp(key, v, comparator);
  if (exist) {
    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);
    return false;
  }
  leaf_node->Insert(key, value, comparator);
  /** leaf page 满了，进行 Split 的操作*/
  if (leaf_node->GetSize() >= leaf_node->GetMaxSize()) {
    LeafPage *new_leaf_node = Split(leaf_node);
    LOG_DEBUG("leaf 满了，The leafnode id :%d, the new_leaf id :%d", leaf_node->GetPageId(),
              new_leaf_node->GetPageId());
    InsertIntoParent(leaf_node, new_leaf_node->KeyAt(0), new_leaf_node);
  }
  buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *cur_node) {
  // 创建一个新的 page， 用于存放新的 page, 将一半的数据转移到新的 node
  page_id_t new_page_id;
  Page *newPage = buffer_pool_manager_->NewPage(&new_page_id);
  if (newPage == nullptr) {
    throw std::string("out of memory");
  }
  N *new_node;  // for return
  if (cur_node->IsLeafPage()) {
    LeafPage *leaf_node = reinterpret_cast<LeafPage *>(cur_node);
    LeafPage *new_leaf_node = reinterpret_cast<LeafPage *>(newPage);
    new_leaf_node->Init(new_page_id, leaf_node->GetParentPageId(), leaf_max_size_);
    leaf_node->MoveHalfTo(new_leaf_node);
    /** 叶子节点更新双向链表*/
    new_leaf_node->SetNextPageId(leaf_node->GetNextPageId());
    leaf_node->SetNextPageId(new_leaf_node->GetPageId());
    new_node = reinterpret_cast<N *>(new_leaf_node);
  } else {
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(cur_node);
    InternalPage *new_internal_node = reinterpret_cast<InternalPage *>(newPage);
    new_internal_node->Init(new_page_id, internal_node->GetParentPageId(), internal_max_size_);
    internal_node->MoveHalfTo(new_internal_node);
    new_node = reinterpret_cast<N *>(new_internal_node);
  }
  /** 对于 new page 还没有 UnPin，需要在外面进行 unpin 的操作*/
  return new_node;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node) {
  /** 如果说目前就是一个 root node，那么只能是创建一个新的 root page*/
  if (old_node->IsRootPage()) {
    page_id_t new_root_id;
    Page *page = buffer_pool_manager_->NewPage(&new_root_id);
    assert(page != nullptr);
    InternalPage *new_root_node = reinterpret_cast<InternalPage *>(page->GetData());

    // New Page init
    new_root_node->Init(new_root_id, INVALID_PAGE_ID, internal_max_size_);
    root_page_id_ = new_root_id;
    UpdateRootPageId(0);

    // Set Parent page
    // 将数据给拆分开了 internal page 的首位key不存东西
    new_root_node->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    old_node->SetParentPageId(new_root_id);
    new_node->SetParentPageId(new_root_id);
    buffer_pool_manager_->UnpinPage(new_root_id, true);
    return;
  }
  /** 如果说目前不是 root page，那么需要找到 parent page */
  page_id_t parent_page_id = old_node->GetParentPageId();
  Page *parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
  InternalPage *parent_node = reinterpret_cast<InternalPage *>(parent_page);
  LOG_DEBUG("The parent node's id : %d", parent_page_id);
  parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());

  /** ！！！！！！！不需要设置一下新的 page 的 parent，Split初始化时候就弄好了*/
  //  new_node->SetParentPageId(parent_node->GetPageId());

  /** 如果 internal page 仍然是满了的，继续分裂 */
  if (parent_node->GetSize() > parent_node->GetMaxSize()) {
    /** recursive insertion */
    InternalPage *new_internal_node = Split(parent_node);
    InsertIntoParent(parent_node, new_internal_node->KeyAt(0), new_internal_node);
    /** 本次是 internal page 的分裂，需要设置所有的新的子节点的 parent node */
    new_internal_node->UpdateAllNodesParent(buffer_pool_manager_);
    buffer_pool_manager_->UnpinPage(new_internal_node->GetPageId(), true);
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
  if (IsEmpty()) return;
  /** 1. find the leaf page*/
  Page *page = FindLeafPage(key, comparator_);
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(page->GetData());
  int curSize = leaf_node->RemoveAndDeleteRecord(key, comparator_);
  /** 如果说 leaf node 需要进行合并 Or 重分配 */
  if (curSize < leaf_node->GetMinSize()) {
    LOG_DEBUG("leaf page node's size < min_size, merge or redistribute.");
    MergeOrRedistribute(leaf_node);
  }
  buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);
}

/*
 * @return: true means target leaf page should be deleted, false otherwise
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::MergeOrRedistribute(N *node) {
  if (node->IsRootPage()) {
    return AdjustRoot(node);
  }
  /** Find the sibling node */
  N *node2;
  bool isNextSibling = FindSibling(node, node2);
  Page *parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  InternalPage *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  /** 能够进行合并的操作, leaf node's merge condition is sum < max_size */
  if (IfCanMerge(node, node2)) {
    // assume node is after node2
    if (isNextSibling) {
      std::swap(node, node2);
    }
    int removeIdx = parent_node->ValueIndex(node->GetPageId());
    Merge(node2, node, parent_node, removeIdx);
    buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
    return true;
  }
  /** borrow entry from node2 */
  int nodeInParentIndex = parent_node->ValueIndex(node->GetPageId());
  Redistribute(node2, node, nodeInParentIndex);
  buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), false);
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::IfCanMerge(N *node, N *neighbor_node) {
  if (node->IsLeafPage()) {
    return node->GetSize() + neighbor_node->GetSize() < node->GetMaxSize();
  } else {
    return node->GetSize() + neighbor_node->GetSize() <= node->GetMaxSize();
  }
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
bool BPLUSTREE_TYPE::Merge(N *&neighbor_node, N *&node,
                           BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *&parent, int index) {
  // assume neighbor node is before node
  assert(node->GetSize() + neighbor_node->GetSize() <= node->GetMaxSize());
  if (node->IsLeafPage()) {
    LeafPage *leaf_node = reinterpret_cast<LeafPage *>(node);
    LeafPage *leaf_neighbor = reinterpret_cast<LeafPage *>(neighbor_node);

    leaf_node->MoveAllTo(leaf_neighbor);
  } else {
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(node);
    InternalPage *internal_neighbor = reinterpret_cast<InternalPage *>(neighbor_node);

    internal_node->MoveAllTo(internal_neighbor, index, buffer_pool_manager_);
  }
  page_id_t pid = node->GetPageId();
  buffer_pool_manager_->UnpinPage(pid, true);
  buffer_pool_manager_->DeletePage(pid);
  buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
  parent->Remove(index);
  /** parent node less than min_size of equal to min_size */
  if (parent->GetSize() < parent->GetMinSize()) {
    return MergeOrRedistribute(parent);
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
  buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
}

/*
 * Find the sibling of the node
 * @return: true if the sibling is next sibling, false means prev sibling
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::FindSibling(N *node, N *&sibling) {
  Page *parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  InternalPage *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  int index = parent_node->ValueIndex(node->GetPageId());
  int siblingIndex = index - 1;
  if (index == 0) {  // 没有 prev sibling, 选择 next sibling
    siblingIndex = index + 1;
  }
  sibling = reinterpret_cast<N *>(buffer_pool_manager_->FetchPage(parent_node->ValueAt(siblingIndex))->GetData());
  buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), false);
  return index == 0;  // 如果是 next sibling 就是返回 true
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  /** 如果说 root node 是internal节点 & has only one size*/
  if (!old_root_node->IsLeafPage() && old_root_node->GetSize() == 1) {
    // 需要重新设置一下 child node 为新的 root node
    InternalPage *root_node = reinterpret_cast<InternalPage *>(old_root_node);
    page_id_t new_root_page_id = root_node->AdjustRootForInternal();
    root_page_id_ = new_root_page_id;
    UpdateRootPageId(0);

    Page *new_root_page = buffer_pool_manager_->FetchPage(root_page_id_);
    BPlusTreePage *new_root_node = reinterpret_cast<BPlusTreePage *>(new_root_page->GetData());
    new_root_node->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(new_root_page_id, true);
    buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), false);
    buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
    return true;
  }

  /** 2. 如果说是 Leaf node 并且已经是空了的, 删除对应的 page */
  if (old_root_node->IsLeafPage() && old_root_node->GetSize() == 0) {
    assert(old_root_node->GetParentPageId() == INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), false);
    buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(0);
    return true;
  }
  return false;
}

/*****************************************************************************
 * SELF CHECK
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Check(bool force) { return true; }

/*
 * check if the page is correct(incremental order)
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::isPageCorrect(page_id_t pid, std::pair<KeyType, ValueType> &out) {
  return false;
  if (IsEmpty()) return true;
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
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

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
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
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
