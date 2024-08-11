//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include <atomic>
#include "common/logger.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator();
  explicit IndexIterator(BufferPoolManager *mng, BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *ptr,
                         size_t offset = 0);
  ~IndexIterator();  // NOLINT

  auto IsEnd() const -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool;

  auto operator!=(const IndexIterator &itr) const -> bool;

 private:
  // add your own private member variables here
  BPlusTreeLeafPage<KeyType, RID, KeyComparator> *cur_leaf_page_ptr_;
  std::atomic<size_t> cur_offset_;
  std::atomic<bool> end_flag_;
  // we have to have access to this page manager.
  BufferPoolManager *buffer_pool_manager_;
};

}  // namespace bustub
