//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_concurrent_test.cpp
//
// Identification: test/storage/b_plus_tree_concurrent_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <chrono>  // NOLINT
#include <cstdio>
#include <functional>
#include <thread>  // NOLINT

#include "buffer/buffer_pool_manager_instance.h"
#include "gtest/gtest.h"
#include "storage/index/b_plus_tree.h"
#include "test_util.h"  // NOLINT

namespace bustub {
// helper function to launch multiple threads
template <typename... Args>
void LaunchParallelTest(uint64_t num_threads, Args &&...args) {
  std::vector<std::thread> thread_group;

  // Launch a group of threads
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group.push_back(std::thread(args..., thread_itr));
  }

  // Join the threads with the main thread
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group[thread_itr].join();
  }
}

// helper function to insert
void InsertHelper(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &keys,
                  __attribute__((unused)) uint64_t thread_itr = 0) {
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree->Insert(index_key, rid, transaction);
  }
  delete transaction;
}

// helper function to seperate insert
void InsertHelperSplit(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &keys,
                       int total_threads, __attribute__((unused)) uint64_t thread_itr) {
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);
  for (auto key : keys) {
    if (static_cast<uint64_t>(key) % total_threads == thread_itr) {
      int64_t value = key & 0xFFFFFFFF;
      rid.Set(static_cast<int32_t>(key >> 32), value);
      index_key.SetFromInteger(key);
      tree->Insert(index_key, rid, transaction);
    }
  }
  delete transaction;
}

// helper function to delete
void DeleteHelper(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &remove_keys,
                  __attribute__((unused)) uint64_t thread_itr = 0) {
  GenericKey<8> index_key;
  // create transaction
  auto *transaction = new Transaction(0);
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree->Remove(index_key, transaction);
  }
  delete transaction;
}

// helper function to seperate delete
void DeleteHelperSplit(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree,
                       const std::vector<int64_t> &remove_keys, int total_threads,
                       __attribute__((unused)) uint64_t thread_itr) {
  GenericKey<8> index_key;
  // create transaction
  auto *transaction = new Transaction(0);
  for (auto key : remove_keys) {
    if (static_cast<uint64_t>(key) % total_threads == thread_itr) {
      index_key.SetFromInteger(key);
      tree->Remove(index_key, transaction);
    }
  }
  delete transaction;
}

TEST(BPlusTreeConcurrentTest, DISABLED_InsertPressureTest) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // keys to Insert
  std::vector<int64_t> keys;
  int64_t scale_factor = 1000;
  uint64_t num_threads = 32;
  for (int64_t key = 1; key < scale_factor; key++) {
    keys.push_back(key);
  }
  // ----
  // add random numbers in this range, and permutation randomly. now the keys may not be unique.
  std::random_device rd;
  std::mt19937 gen0(rd());
  std::mt19937 gen1(rd());
  std::uniform_int_distribution<int64_t> val_dist(1, scale_factor - 1);
  for (int64_t index = 0; index < scale_factor / 10; index++) {
    int64_t tmp_val = val_dist(gen0);
    keys.push_back(tmp_val);
  }
  std::shuffle(keys.begin(), keys.end(), gen1);

  std::vector<int64_t> uniq_keys = keys;
  std::sort(uniq_keys.begin(), uniq_keys.end());
  auto new_end = std::unique(uniq_keys.begin(), uniq_keys.end());
  uniq_keys.erase(new_end, uniq_keys.end());
  // ----

  // after prepare keys, insert in parallel.
  // LaunchParallelTest(num_threads, InsertHelperSplit, &tree, keys, num_threads);
  LaunchParallelTest(num_threads, InsertHelper, &tree, keys);

  std::vector<RID> rids;
  GenericKey<8> index_key;
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

  EXPECT_EQ(current_key, uniq_keys.size() + 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, DISABLED_InsertTest1) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // keys to Insert
  std::vector<int64_t> keys;
  int64_t scale_factor = 100;
  for (int64_t key = 1; key < scale_factor; key++) {
    keys.push_back(key);
  }
  LaunchParallelTest(2, InsertHelper, &tree, keys);

  std::vector<RID> rids;
  GenericKey<8> index_key;
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

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, DISABLED_InsertTest2) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());
  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // keys to Insert
  std::vector<int64_t> keys;
  int64_t scale_factor = 100;
  for (int64_t key = 1; key < scale_factor; key++) {
    keys.push_back(key);
  }
  LaunchParallelTest(2, InsertHelperSplit, &tree, keys, 2);

  std::vector<RID> rids;
  GenericKey<8> index_key;
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

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, DISABLED_DeleteThenTravelTest1) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 2, 3);
  GenericKey<8> index_key;
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // sequential insert
  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  InsertHelper(&tree, keys);

  std::vector<RID> rids;
  for (const auto &key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  // LaunchParallelTest(2, DeleteHelper, &tree, remove_keys);

  // tree.PrintGraphUtil();
  int64_t start_key = 1;
  int64_t current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, 5);

  std::vector<int64_t> remove_keys = {4, 5, 3};
  for (const auto &key : remove_keys) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key);
  }
  tree.PrintGraphUtil();

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, RandomDeleteTest) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 2, 3);
  GenericKey<8> index_key;
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  int64_t scale_factor = 512;
  std::random_device rd;
  std::mt19937 gen1(rd());
  // std::mt19937 gen2(rd());

  // sequential insert
  std::vector<int64_t> keys;
  for (int64_t i = 1; i < scale_factor; i++) {
    keys.push_back(i);
  }
  std::shuffle(keys.begin(), keys.end(), gen1);
  // keys = {12, 16, 4, 1, 28, 9, 23, 13, 11, 20, 7, 21, 27, 5};
  // keys = {12, 16, 4, 1, 28, 9, 23, 13, 11};
  // keys = {12, 16, 4, 1};
  std::string filename = "RandomDeleteTestData.out";
  std::ofstream file(filename, std::ios::trunc);
  if (!file.is_open()) {
    std::cerr << "Failed to open file: " << filename << std::endl;
    return;
  }
  /*
    toggle the following write patterns (bin vs text).
  */
  // file.write(reinterpret_cast<const char *>(keys.data()), keys.size() * sizeof(int64_t));

  file << "[";
  for (size_t i = 0; i < keys.size(); ++i) {
    file << keys[i];
    if (i != keys.size() - 1) {
      file << ", ";
    }
  }
  file << "]";

  file.close();

  // InsertHelper(&tree, keys);
  LaunchParallelTest(8, InsertHelper, &tree, keys);

  tree.PrintGraphUtil();

  int64_t rem_end_offset = scale_factor >> 2;
  std::vector<int64_t> remove_keys = std::vector<int64_t>(keys.begin(), keys.begin() + rem_end_offset);
  std::vector<int64_t> remain_keys = std::vector<int64_t>(keys.begin() + rem_end_offset, keys.end());
  std::sort(remain_keys.begin(), remain_keys.end());

  LOG_INFO("insert phase end");
  LaunchParallelTest(1, DeleteHelper, &tree, remove_keys);

  tree.PrintGraphUtil();

  LOG_INFO("remove phase end");
  int64_t start_key = remain_keys[0];
  int64_t current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);

  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    LOG_INFO("FUCKKKKK");
    current_key = remain_keys.at(size);
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    LOG_INFO("fuck val: %d", location.GetSlotNum());
    size = size + 1;
  }

  EXPECT_EQ(size, remain_keys.size());

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, DISABLED_CaseStudy1DeleteTest) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 2, 3);
  GenericKey<8> index_key;
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  // sequential insert
  // awk '{gsub(/\./, "", $4); print $4}' bbbb.log | tr '\n' ',' | sed 's/^/{/; s/,$/};/'
  // what about insert a new minimal key?
  std::vector<int64_t> keys = {60, 3,  62, 51, 48, 58, 15, 23, 11, 55, 24, 10, 44, 47, 38, 41, 21, 12, 26, 5,  39,
                               45, 19, 63, 7,  28, 50, 16, 13, 59, 9,  52, 22, 37, 54, 46, 30, 1,  34, 17, 49, 43,
                               36, 4,  33, 29, 18, 2,  14, 20, 53, 27, 40, 6,  31, 42, 32, 56, 25, 35, 57, 8,  61};

  InsertHelper(&tree, keys);

  std::vector<int64_t> remove_keys = {60, 3, 62, 51, 48, 58, 15, 23, 11, 55, 24, 10, 44, 47, 38, 41};
  std::vector<int64_t> remain_keys = std::vector<int64_t>(keys.begin() + remove_keys.size(), keys.end());
  std::sort(remain_keys.begin(), remain_keys.end());

  LaunchParallelTest(1, DeleteHelper, &tree, remove_keys);

  int64_t start_key = remain_keys[0];
  int64_t current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);

  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    current_key = remain_keys.at(size);
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    LOG_INFO("fuck val: %d", location.GetSlotNum());
    size = size + 1;
  }

  EXPECT_EQ(size, remain_keys.size());

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, DISABLED_CaseStudy2DeleteTest) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 2, 3);
  GenericKey<8> index_key;
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  // sequential insert
  // awk '{gsub(/\./, "", $4); print $4}' bbbb.log | tr '\n' ',' | sed 's/^/{/; s/,$/};/'
  // find bugs of unlock a already unlocked latch.
  std::vector<int64_t> keys = {31, 12, 13, 10, 1,  58, 49, 34, 20, 61, 7,  6,  40, 62, 37, 50, 32, 11, 35, 36, 5,
                               3,  57, 46, 63, 41, 16, 59, 4,  21, 45, 55, 56, 26, 48, 8,  25, 27, 15, 38, 54, 24,
                               43, 39, 29, 47, 52, 9,  23, 19, 42, 53, 44, 60, 30, 51, 18, 17, 33, 14, 22, 28, 2};

  InsertHelper(&tree, keys);

  std::vector<int64_t> remove_keys = {31, 12, 13, 10, 1, 58, 49, 34, 20, 61, 7, 6, 40, 62, 37, 50};
  std::vector<int64_t> remain_keys = std::vector<int64_t>(keys.begin() + remove_keys.size(), keys.end());
  std::sort(remain_keys.begin(), remain_keys.end());

  LaunchParallelTest(1, DeleteHelper, &tree, remove_keys);

  int64_t start_key = remain_keys[0];
  int64_t current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);

  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    current_key = remain_keys.at(size);
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    LOG_INFO("fuck val: %d", location.GetSlotNum());
    size = size + 1;
  }

  EXPECT_EQ(size, remain_keys.size());

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, DISABLED_CaseStudy3DeleteTest) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 2, 3);
  GenericKey<8> index_key;
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  // sequential insert
  // awk '{gsub(/\./, "", $4); print $4}' bbbb.log | tr '\n' ',' | sed 's/^/{/; s/,$/};/'
  std::vector<int64_t> keys = {59, 9,  52, 35, 58, 28, 42, 56, 23, 53, 7,  25, 60, 12, 37, 27, 54, 18, 8,  51, 24,
                               20, 34, 1,  30, 10, 48, 6,  21, 2,  31, 3,  14, 11, 45, 15, 16, 22, 44, 19, 50, 55,
                               33, 32, 13, 38, 5,  17, 26, 43, 62, 39, 29, 41, 36, 47, 40, 49, 61, 46, 57, 4,  63};

  InsertHelper(&tree, keys);

  std::vector<int64_t> remove_keys = {59, 9, 52, 35, 58, 28, 42, 56, 23, 53, 7, 25, 60, 12, 37, 27};
  std::set<int64_t> remain_keys_set(keys.begin(), keys.end());
  for (const auto &val : remove_keys) {
    remain_keys_set.erase(val);
  }
  std::vector<int64_t> remain_keys = std::vector<int64_t>(remain_keys_set.begin(), remain_keys_set.end());
  std::sort(remain_keys.begin(), remain_keys.end());

  LaunchParallelTest(1, DeleteHelper, &tree, remove_keys);

  int64_t start_key = remain_keys[0];
  int64_t current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);

  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    current_key = remain_keys.at(size);
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    LOG_INFO("fuck val: %d", location.GetSlotNum());
    size = size + 1;
  }

  EXPECT_EQ(size, remain_keys.size());

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, DISABLED_CaseStudy4DeleteTest) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 2, 3);
  GenericKey<8> index_key;
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  // sequential insert
  // awk '{gsub(/\./, "", $4); print $4}' bbbb.log | tr '\n' ',' | sed 's/^/{/; s/,$/};/'
  std::vector<int64_t> keys = {47, 26, 2,  37, 61, 30, 15, 63, 39, 25, 52, 10, 20, 12, 9,  62, 57, 14, 6,  8,  21,
                               60, 50, 34, 48, 24, 18, 3,  29, 42, 16, 35, 41, 55, 27, 11, 17, 54, 36, 19, 38, 56,
                               5,  44, 53, 23, 45, 4,  49, 32, 33, 40, 59, 51, 22, 28, 58, 31, 46, 43, 1,  13, 7};

  InsertHelper(&tree, keys);

  std::vector<int64_t> remove_keys = {47, 26, 2, 37, 61, 30, 15, 63, 39, 25, 52, 10, 20, 12, 9, 62};
  std::vector<int64_t> remain_keys = std::vector<int64_t>(keys.begin() + remove_keys.size(), keys.end());
  std::sort(remain_keys.begin(), remain_keys.end());

  LaunchParallelTest(1, DeleteHelper, &tree, remove_keys);

  int64_t start_key = remain_keys[0];
  int64_t current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);

  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    current_key = remain_keys.at(size);
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    LOG_INFO("fuck val: %d", location.GetSlotNum());
    size = size + 1;
  }

  EXPECT_EQ(size, remain_keys.size());

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, DISABLED_CaseStudy5DeleteTest) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 2, 3);
  GenericKey<8> index_key;
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  // sequential insert
  // awk '{gsub(/\./, "", $4); print $4}' bbbb.log | tr '\n' ',' | sed 's/^/{/; s/,$/};/'
  std::vector<int64_t> keys = {63, 4,  58, 49, 42, 36, 11, 38, 16, 35, 41, 30, 37, 39, 21, 62, 32, 57, 23, 52, 44,
                               17, 24, 28, 19, 14, 20, 61, 53, 47, 5,  27, 46, 18, 22, 40, 13, 1,  51, 26, 50, 3,
                               29, 7,  54, 45, 48, 43, 60, 59, 6,  9,  12, 31, 10, 25, 33, 2,  55, 15, 8,  56, 34};

  InsertHelper(&tree, keys);

  // std::vector<int64_t> remove_keys = {63, 4, 58, 49, 42, 36, 11, 38, 16, 35, 41, 30, 37, 39, 21, 62};
  std::vector<int64_t> remove_keys = {63, 4, 58, 49, 42, 36, 11, 38, 16, 35, 41, 30, 37};
  std::vector<int64_t> remain_keys = std::vector<int64_t>(keys.begin() + remove_keys.size(), keys.end());
  std::sort(remain_keys.begin(), remain_keys.end());

  LaunchParallelTest(1, DeleteHelper, &tree, remove_keys);

  int64_t start_key = remain_keys[0];
  int64_t current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);

  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    current_key = remain_keys.at(size);
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    LOG_INFO("fuck val: %d", location.GetSlotNum());
    size = size + 1;
  }

  EXPECT_EQ(size, remain_keys.size());

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, DISABLED_CaseStudy6DeleteTest) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 2, 3);
  GenericKey<8> index_key;
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  // sequential insert
  // awk '{gsub(/\./, "", $4); print $4}' bbbb.log | tr '\n' ',' | sed 's/^/{/; s/,$/};/'
  // std::vector<int64_t> keys = {27, 63, 61, 60, 20, 34, 62, 22, 51, 18, 24, 19, 45, 26, 5,  44, 7,  17, 4,  33, 48,
  //                              21, 59, 1,  23, 42, 14, 47, 25, 15, 12, 2,  37, 6,  41, 28, 43, 40, 57, 50, 13, 31,
  //                              30, 29, 35, 11, 32, 46, 55, 58, 10, 16, 36, 49, 38, 8,  39, 9,  52, 53, 54, 56, 3};
  std::vector<int64_t> keys = {31, 61, 26, 14, 62, 22, 63, 23, 24, 34, 46, 25, 19, 39, 11, 56, 37, 2,  32, 15, 38,
                               6,  28, 43, 52, 1,  60, 41, 3,  9,  27, 35, 20, 10, 16, 40, 47, 55, 54, 45, 44, 58,
                               29, 8,  13, 42, 33, 18, 50, 53, 57, 4,  59, 21, 36, 48, 12, 49, 30, 17, 5,  51, 7};

  InsertHelper(&tree, keys);

  // std::vector<int64_t> remove_keys = {27, 63, 61, 60, 20, 34, 62, 22, 51, 18, 24, 19, 45, 26, 5, 44};
  std::vector<int64_t> remove_keys = {31, 61, 26, 14, 62, 22, 63, 23, 24, 34, 46, 25, 19, 39, 11, 56};
  std::set<int64_t> remain_keys_set(keys.begin(), keys.end());
  for (const auto &val : remove_keys) {
    remain_keys_set.erase(val);
  }
  std::vector<int64_t> remain_keys = std::vector<int64_t>(remain_keys_set.begin(), remain_keys_set.end());
  std::sort(remain_keys.begin(), remain_keys.end());

  LaunchParallelTest(1, DeleteHelper, &tree, remove_keys);

  int64_t start_key = remain_keys[0];
  int64_t current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);

  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    current_key = remain_keys.at(size);
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    LOG_INFO("fuck val: %d", location.GetSlotNum());
    size = size + 1;
  }

  EXPECT_EQ(size, remain_keys.size());

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, DISABLED_CP2DeleteTest3) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 2, 3);
  GenericKey<8> index_key;
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  int64_t scale_factor = 64;
  std::random_device rd;
  std::mt19937 gen1(rd());

  // sequential insert
  std::vector<int64_t> keys;
  for (int64_t i = 1; i < scale_factor; i++) {
    keys.push_back(i);
  }
  std::shuffle(keys.begin(), keys.end(), gen1);

  InsertHelper(&tree, keys);

  int64_t rem_end_offset = scale_factor >> 2;
  std::vector<int64_t> remove_keys = std::vector<int64_t>(keys.begin(), keys.begin() + rem_end_offset);
  std::vector<int64_t> remain_keys = std::vector<int64_t>(keys.begin() + rem_end_offset, keys.end());
  std::sort(remain_keys.begin(), remain_keys.end());

  LaunchParallelTest(1, DeleteHelper, &tree, remove_keys);

  int64_t start_key = remain_keys[0];
  int64_t current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  LOG_INFO("end remove phase. first key: %ld", start_key);
  // tree.PrintGraphUtil();

  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    LOG_INFO("FUCKKKKK");
    current_key = remain_keys.at(size);
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    LOG_INFO("fuck val: %d", location.GetSlotNum());
    size = size + 1;
  }

  EXPECT_EQ(size, remain_keys.size());

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, DISABLED_DeleteTest1) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // sequential insert
  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  InsertHelper(&tree, keys);

  std::vector<int64_t> remove_keys = {1, 5, 3, 4};
  LaunchParallelTest(2, DeleteHelper, &tree, remove_keys);

  int64_t start_key = 2;
  int64_t current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, DISABLED_DeleteTest2) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  // sequential insert
  std::vector<int64_t> keys = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  InsertHelper(&tree, keys);

  std::vector<int64_t> remove_keys = {1, 4, 3, 2, 5, 6};
  LaunchParallelTest(2, DeleteHelperSplit, &tree, remove_keys, 2);

  int64_t start_key = 7;
  int64_t current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, 4);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, DISABLED_MixTest) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // first, populate index
  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  InsertHelper(&tree, keys);

  // concurrent insert
  keys.clear();
  for (int i = 6; i <= 10; i++) {
    keys.push_back(i);
  }
  LaunchParallelTest(1, InsertHelper, &tree, keys);
  // concurrent delete
  std::vector<int64_t> remove_keys = {1, 4, 3, 5, 6};
  LaunchParallelTest(1, DeleteHelper, &tree, remove_keys);

  int64_t start_key = 2;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    size = size + 1;
  }

  EXPECT_EQ(size, 5);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

}  // namespace bustub
