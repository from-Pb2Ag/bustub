//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/b_plus_tree.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <iomanip>
#include <queue>
#include <random>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "concurrency/transaction.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

/**
 * Main class providing the API for the Interactive B+ Tree.
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  explicit BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                     int leaf_max_size = LEAF_PAGE_SIZE, int internal_max_size = INTERNAL_PAGE_SIZE);

  // Returns true if this B+ tree has no keys and values.
  auto IsEmpty() const -> bool;

  // Insert a key-value pair into this B+ tree.
  auto Insert(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr) -> bool;

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key, Transaction *transaction = nullptr);

  // return the value associated with a given key
  auto GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction = nullptr) -> bool;

  // return the page id of the root node
  auto GetRootPageId() -> page_id_t;

  // index iterator
  auto Begin() -> INDEXITERATOR_TYPE;
  auto Begin(const KeyType &key) -> INDEXITERATOR_TYPE;
  auto End() -> INDEXITERATOR_TYPE;

  // print the B+ tree
  void Print(BufferPoolManager *bpm);

  // draw the B+ tree
  void Draw(BufferPoolManager *bpm, const std::string &outf);

  // read data from file and insert one by one
  void InsertFromFile(const std::string &file_name, Transaction *transaction = nullptr);

  // read data from file and remove one by one
  void RemoveFromFile(const std::string &file_name, Transaction *transaction = nullptr);

  void PrintGraphUtil();

 private:
  void UpdateRootPageId(int insert_record = 0);

  /*
    K[0] V[0] | K[1] V[1] | ... | K[n] V[n].
    K[1] ~ K[n] are legal; V[0] ~ V[n] are legal.
    find the index of K. K[t] <= key < K[t].
    if key < K[1].              return {0, true}.
    if K[t] == key < K[t + 1].  return {t, false}.
    if K[t] <  key < K[t + 1].  return {t, true}, 1 <= t <= n.
  */
  auto FindFirstInfIndex(const KeyType &key, BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *page_ptr)
      -> std::pair<int, bool>;
  /*
    K[0] V[0] | K[1] V[1] | ... | K[n] V[n].
    for leaf page, if K[t] == key,  return {t, false}.
    if K[0] > key,                  return {0, true}.
    if K[t] < key exactly,          return {t + 1, true}.
  */
  auto FindFirstInfIndex(const KeyType &key, BPlusTreeLeafPage<KeyType, RID, KeyComparator> *page_ptr)
      -> std::pair<int, bool>;

  void InsertInternalCanSplit(const std::vector<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *> &st,
                              const KeyType &key, const page_id_t &value,
                              std::unordered_map<page_id_t, bool> *unpin_is_dirty,
                              std::unordered_map<page_id_t, size_t> *fuck,
                              std::unordered_map<page_id_t, Page *> *cached_ptr,
                              std::unordered_map<page_id_t, Page *> *unpin_coll, const std::string &signature);

  void InnerPageMerge(
      const std::vector<std::pair<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *, int>> &st);

  /*
    call by a function, un-pin the pages in a thread scope.
  */
  void UnpinPages(const std::unordered_map<page_id_t, Page *> &unpin_coll, const std::string &sig);

  // find the first index >= key.
  auto BinarySearch(const KeyType &key, BPlusTreeLeafPage<KeyType, RID, KeyComparator> *page_ptr) -> int;
  /*
    if a page (no matter leaf or internal) updates its key in slot #0,
    it will populate through its ancestors (and stop conditionally).
  */
  void FirstKeyPopulateUp(
      const KeyType &new_key,
      std::vector<std::pair<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *, int>> *st_1);

  void MergedToLeftSibling(BPlusTreePage *prev_page, BPlusTreePage *this_page);

  void BorrowFromLeftSibling(BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *prev_page,
                             BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *this_page);

  void BorrowFromRightSibling(BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *this_page,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *next_page);

  void PopulateUpV2(BPlusTreePage *this_page, const KeyType &old_first_key, const KeyType &new_first_key);

  void MergedWithRightSibling(BPlusTreePage *this_page, BPlusTreePage *next_page);

  /*
    -----------------------------------------
    | Type: R|I | cur_sz: xxx | max_sz: xxx |
    -----------------------------------------
    | PPid: xxx | Pid: xxx    | Nid: xxx    |
    -----------------------------------------
  */
  void LogBPlusTreePageHeader(BPlusTreePage *page);
  /* Debug Routines for FREE!! */
  void ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const;

  void ToString(BPlusTreePage *page, BufferPoolManager *bpm) const;

  void UnlatchRootPage();

  /*
    find `pid`'s corresponding `Page*` ptr in `cached_ptr`. If can not find update `cached_ptr`,
    as well as `unpin_coll` which is responsible for un-pin pages in the end of the outer function call.
    And W-latch it as soon as we can.
  */
  auto FastFetchWithWLatch(page_id_t pid, const std::string &signature,
                           std::unordered_map<bustub::page_id_t, bustub::Page *> *cached_ptr,
                           std::unordered_map<bustub::page_id_t, bustub::Page *> *unpin_coll) -> Page *;

  /*
    only called in `remove` function. since remove CAN make root page only has one child page,
    then the root page is stale. we save these root(s) in `stale_root_coll`.
  */
  void TreeHeightTrim(std::set<Page *> *stale_root_coll);

  // member variable
  std::string index_name_;
  page_id_t root_page_id_;
  BufferPoolManager *buffer_pool_manager_;
  KeyComparator comparator_;
  int leaf_max_size_;
  int internal_max_size_;
  int op_id_;
  std::atomic<bool> is_empty_;
  // double-check guard the is_empty_.
  std::mutex mux_;
  std::atomic<size_t> root_locked_;
  std::condition_variable c_v_;
  // how many remain buffer pool page frames?
  std::atomic<size_t> rem_cnt_;
  // current B+ tree height.
  std::atomic<size_t> cur_height_;
  std::condition_variable buffer_pool_page_quota_;
  std::mutex quota_mux_;
  /*
    guard the page delete option. when should we del a page? 1: merged due to `remove` op; 2: stale root page.
    Since `delete` a page, first we need to `un-pin` and `un-latch` it.
  */
  std::mutex del_page_mux_;

  enum class RootLockType : size_t { UN_LOCKED = 0, READ_LOCKED = 1, WRITE_LOCKED = 1 << 1 };
  class FinalAction {
   public:
    explicit FinalAction(BPlusTree<KeyType, ValueType, KeyComparator> *tree) : tree_(tree), active_(true) {}

    ~FinalAction() {
      if (active_) {
        Perform();
      }
    }

    void Perform() {
      Page *ptr = tree_->buffer_pool_manager_->FetchPage(tree_->GetRootPageId());
      auto *tmp_new_root = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(ptr);

      std::string full = base_ + std::to_string(tree_->op_id_) + ".dot";
      LOG_INFO("save: %s", full.c_str());
      std::ofstream ofs(full.c_str(), std::ios_base::app);
      std::ofstream head(full.c_str(), std::ios_base::trunc);

      head << "digraph G {" << std::endl;
      head.close();

      tree_->ToGraph(tmp_new_root, tree_->buffer_pool_manager_, ofs);
      ofs << "}" << std::endl;
      ofs.close();
    }

    void Deactivate() { active_ = false; }

   private:
    BPlusTree<KeyType, ValueType, KeyComparator> *tree_;
    bool active_;
    std::string base_ = "/home/yhqian/archives/LABS/CMU_445/bustub/build/fuck";
  };
};

}  // namespace bustub
