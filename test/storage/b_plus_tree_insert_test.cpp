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

  int size = 10000;

  std::vector<int64_t> keys(size);

  std::iota(keys.begin(), keys.end(), 1);

  std::random_device rd;
  std::mt19937 g(rd());
  std::shuffle(keys.begin(), keys.end(), g);
  std::cout << "---------" << std::endl;
  int i = 0;
  (void)i;

  std::vector<int64_t> arr{0,634,7529,9154,783,4330,7993,3895,7931,6702,6793,8909,2356,1140,766,6925,4703,147,3866,636,3817,8827,2562,8562,358,375,4874,9004,1014,6176,9229,1654,2767,474,6763,122,1673,1544,
                       2557,9178,4070,4652,8712,1167,4633,1145,8532,3193,5158,80,2617,8110,2981,1834,9656,3405,3322,2418,6150,
                       3543,4392,108,3528,4942,8584,6858,6098,472,6233,7349,9050};

  for (int j = 1; j <= 65; j++) {
    int64_t key = arr[j];
    i++;
    std::cout << "counter : " << i << std::endl;
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }
//  tree.Draw(bpm, "tmp_tree.dot");
  std::cout << "the mid line ======== \n";
  return;



  for (auto &key : keys) {
    i++;
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
    std::cout << "counter : " << i << std::endl;
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

  int size = 10000;

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

}

