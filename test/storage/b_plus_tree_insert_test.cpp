////===----------------------------------------------------------------------===//
////
////                         BusTub
////
//// b_plus_tree_insert_test.cpp
////
//// Identification: test/storage/b_plus_tree_insert_test.cpp
////
//// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
////
////===----------------------------------------------------------------------===//
//
//#include <algorithm>
//#include <cstdio>
//
//#include "buffer/buffer_pool_manager_instance.h"
//#include "gtest/gtest.h"
//#include "storage/index/b_plus_tree.h"
//#include "test_util.h"  // NOLINT
//
//namespace bustub {
//
//TEST(BPlusTreeTests, InsertTest1) {
//  // create KeyComparator and index schema
//  auto key_schema = ParseCreateStatement("a bigint");
//  GenericComparator<8> comparator(key_schema.get());
//
//  auto *disk_manager = new DiskManager("test.db");
//  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
//  // create b+ tree
//  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 2, 3);
//  GenericKey<8> index_key;
//  RID rid;
//  // create transaction
//  auto *transaction = new Transaction(0);
//
//  // create and fetch header_page
//  page_id_t page_id;
//  auto header_page = bpm->NewPage(&page_id);
//  ASSERT_EQ(page_id, HEADER_PAGE_ID);
//  (void)header_page;
//
//  int64_t key = 42;
//  int64_t value = key & 0xFFFFFFFF;
//  rid.Set(static_cast<int32_t>(key), value);
//  index_key.SetFromInteger(key);
//  tree.Insert(index_key, rid, transaction);
//
//  auto root_page_id = tree.GetRootPageId();
//  auto root_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id)->GetData());
//  ASSERT_NE(root_page, nullptr);
//  ASSERT_TRUE(root_page->IsLeafPage());
//
//  auto root_as_leaf = reinterpret_cast<BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>> *>(root_page);
//  ASSERT_EQ(root_as_leaf->GetSize(), 1);
//  ASSERT_EQ(comparator(root_as_leaf->KeyAt(0), index_key), 0);
//
//  bpm->UnpinPage(root_page_id, false);
//  bpm->UnpinPage(HEADER_PAGE_ID, true);
//  delete transaction;
//  delete disk_manager;
//  delete bpm;
//  remove("test.db");
//  remove("test.log");
//}
//
//TEST(BPlusTreeTests, InsertTest2) {
//  // create KeyComparator and index schema
//  auto key_schema = ParseCreateStatement("a bigint");
//  GenericComparator<8> comparator(key_schema.get());
//
//  auto *disk_manager = new DiskManager("test.db");
//  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
//  // create b+ tree
//  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 2, 3);
//  GenericKey<8> index_key;
//  RID rid;
//  // create transaction
//  auto *transaction = new Transaction(0);
//
//  // create and fetch header_page
//  page_id_t page_id;
//  auto header_page = bpm->NewPage(&page_id);
//  (void)header_page;
//
//  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
//  for (auto key : keys) {
//    int64_t value = key & 0xFFFFFFFF;
//    rid.Set(static_cast<int32_t>(key >> 32), value);
//    index_key.SetFromInteger(key);
//    tree.Insert(index_key, rid, transaction);
//  }
//
//  std::vector<RID> rids;
//  for (auto key : keys) {
//    rids.clear();
//    index_key.SetFromInteger(key);
//    tree.GetValue(index_key, &rids);
//    EXPECT_EQ(rids.size(), 1);
//
//    int64_t value = key & 0xFFFFFFFF;
//    EXPECT_EQ(rids[0].GetSlotNum(), value);
//  }
//
//  int64_t size = 0;
//  bool is_present;
//
//  for (auto key : keys) {
//    rids.clear();
//    index_key.SetFromInteger(key);
//    is_present = tree.GetValue(index_key, &rids);
//
//    EXPECT_EQ(is_present, true);
//    EXPECT_EQ(rids.size(), 1);
//    EXPECT_EQ(rids[0].GetPageId(), 0);
//    EXPECT_EQ(rids[0].GetSlotNum(), key);
//    size = size + 1;
//  }
//
//  EXPECT_EQ(size, keys.size());
//
//  bpm->UnpinPage(HEADER_PAGE_ID, true);
//  delete transaction;
//  delete disk_manager;
//  delete bpm;
//  remove("test.db");
//  remove("test.log");
//}
//
//TEST(BPlusTreeTests, InsertTest3) {
//  // create KeyComparator and index schema
//  auto key_schema = ParseCreateStatement("a bigint");
//  GenericComparator<8> comparator(key_schema.get());
//
//  auto *disk_manager = new DiskManager("test.db");
//  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
//  // create b+ tree
//  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
//  GenericKey<8> index_key;
//  RID rid;
//  // create transaction
//  auto *transaction = new Transaction(0);
//
//  // create and fetch header_page
//  page_id_t page_id;
//  auto header_page = bpm->NewPage(&page_id);
//  ASSERT_EQ(page_id, HEADER_PAGE_ID);
//  (void)header_page;
//
//  std::vector<int64_t> keys = {5, 4, 3, 2, 1};
//  for (auto key : keys) {
//    int64_t value = key & 0xFFFFFFFF;
//    rid.Set(static_cast<int32_t>(key >> 32), value);
//    index_key.SetFromInteger(key);
//    tree.Insert(index_key, rid, transaction);
//  }
//
//  std::vector<RID> rids;
//  for (auto key : keys) {
//    rids.clear();
//    index_key.SetFromInteger(key);
//    tree.GetValue(index_key, &rids);
//    EXPECT_EQ(rids.size(), 1);
//
//    int64_t value = key & 0xFFFFFFFF;
//    EXPECT_EQ(rids[0].GetSlotNum(), value);
//  }
//
//  int64_t start_key = 1;
//  int64_t current_key = start_key;
//  index_key.SetFromInteger(start_key);
//  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
//    auto location = (*iterator).second;
//    EXPECT_EQ(location.GetPageId(), 0);
//    EXPECT_EQ(location.GetSlotNum(), current_key);
//    current_key = current_key + 1;
//  }
//
//  EXPECT_EQ(current_key, keys.size() + 1);
//
//  start_key = 3;
//  current_key = start_key;
//  index_key.SetFromInteger(start_key);
//  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
//    auto location = (*iterator).second;
//    EXPECT_EQ(location.GetPageId(), 0);
//    EXPECT_EQ(location.GetSlotNum(), current_key);
//    current_key = current_key + 1;
//  }
//
//  bpm->UnpinPage(HEADER_PAGE_ID, true);
//  delete transaction;
//  delete disk_manager;
//  delete bpm;
//  remove("test.db");
//  remove("test.log");
//}
//}  // namespace bustub

//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_insert_test.cpp
//
// Identification: test/storage/b_plus_tree_insert_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cstdio>
#include <numeric>
#include <random>
#include <string>

#include "buffer/buffer_pool_manager_instance.h"
#include "concurrency/transaction.h"
#include "gtest/gtest.h"
#include "storage/index/b_plus_tree.h"
#include "test_util.h"  // NOLINT
namespace bustub {

// 此文件放在对应test文件夹下，重新构建即可
TEST(BPlusTreeTests, InsertTest1) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 3, 3);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  int size = 800;

  std::vector<int64_t> keys(size);

  std::iota(keys.begin(), keys.end(), 1);

  std::random_device rd;
  std::mt19937 g(rd());
  std::shuffle(keys.begin(), keys.end(), g);
  std::cout << "---------" << std::endl;
  int i = 0;
  (void)i;

  for (auto &key : keys) {
    i++;
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }
  std::vector<RID> rids;
//  std::cout << "running here....1\n";
  std::shuffle(keys.begin(), keys.end(), g);

  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }
//  std::cout << "running here....2\n";
  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);

  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }
  std::cout << "Insert & GetValue passed..\n";
//  EXPECT_EQ(current_key, keys.size() + 1);

  std::shuffle(keys.begin(), keys.end(), g);
  for (auto key : keys) {
    i++;
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
  }
  EXPECT_EQ(true, tree.IsEmpty());

  bpm->UnpinPage(HEADER_PAGE_ID, true);

  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeTests, InsertTest2) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 4, 4);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  int size = 1000;

  std::vector<int64_t> keys(size);
  // assign value to [begin, end) with the increasing value
  std::iota(keys.begin(), keys.end(), 1);

  std::random_device rd;
  std::mt19937 g(rd());
  std::shuffle(keys.begin(), keys.end(), g);  // shuffle the vector
  std::cout << "---------" << std::endl;
  int i = 0;
  (void)i;
  for (auto key : keys) {
    i++;
    int64_t value = key & 0xFFFFFFFF; // low 32 bits
    rid.Set(static_cast<int32_t>(key >> 32), value);  // set with page_id & slot_num
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }
  std::vector<RID> rids;

  std::shuffle(keys.begin(), keys.end(), g);

  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);

  // yep, because the high 32 bits is zero
  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);
  std::cout << "Test2 Insert & Getvalue passed...\n";
  std::shuffle(keys.begin(), keys.end(), g);
  for (auto key : keys) {
    i++;
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
  }
  EXPECT_EQ(true, tree.IsEmpty());
  std::cout << "Test2 Remove passed..\n";
  bpm->UnpinPage(HEADER_PAGE_ID, true);

  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeTests, InsertTest3) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  int size = 50000;

  std::vector<int64_t> keys(size);

  std::iota(keys.begin(), keys.end(), 1);

  std::random_device rd;
  std::mt19937 g(rd());
  std::shuffle(keys.begin(), keys.end(), g);
  std::cout << "---------" << std::endl;
  int i = 0;
  (void)i;
  for (auto key : keys) {
    i++;
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);

  }
  std::vector<RID> rids;

  std::shuffle(keys.begin(), keys.end(), g);

  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);

  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  std::shuffle(keys.begin(), keys.end(), g);
  for (auto key : keys) {
    i++;
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
  }

  EXPECT_EQ(true, tree.IsEmpty());

  bpm->UnpinPage(HEADER_PAGE_ID, true);

  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

}  // namespace bustub

