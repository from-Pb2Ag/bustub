//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "common/logger.h"
#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.reserve(num_buckets_);
  for (int i = 0; i < GetNumBuckets(); i++) {
    dir_.emplace_back(std::make_shared<Bucket>(bucket_size_));
  }
  insert_op_ = 0;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  // LOG_INFO("%d. mask: %d", global_depth_, mask);
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  int index = IndexOf(key);
  return dir_[index]->Find(key, value);
  // UNREACHABLE("not implemented");
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  int index = IndexOf(key);
  return dir_[index]->Remove(key);
  // UNREACHABLE("not implemented");
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);

  DoInsert(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::DoInsert(const K &key, const V &value) -> bool {
  // insert with success.
  if (dir_.at(IndexOf(key))->Insert(key, value)) {
    return true;
  }

  // entry corresponding bucket is full and depth is large.
  if (dir_.at(IndexOf(key))->GetDepth() == global_depth_) {
    int primary_dir_len = dir_.size();

    global_depth_++;

    for (int i = 0; i < primary_dir_len; i++) {
      dir_.emplace_back(std::shared_ptr<Bucket>(dir_.at(i)));
    }
  }
  // `2. Increment the local depth of the bucket.`
  dir_.at(IndexOf(key))->IncrementDepth();

  int local_mask = (1 << dir_.at(IndexOf(key))->GetDepth()) - 1;
  /*
    suppose takes 3 LSB, local_mask => 0b111.
    (~local_mask >> 1) => 0b...100.
    thus `divide_bucket` flips `origin_bucket`'s MSB `1` bit.
  */
  size_t origin_index = IndexOf(key) & local_mask;
  size_t divide_index = (origin_index ^ (~local_mask >> 1)) & local_mask;
  std::shared_ptr<Bucket> origin_bucket = dir_.at(IndexOf(key));
  std::shared_ptr<Bucket> divide_bucket = std::make_shared<Bucket>(bucket_size_, dir_.at(IndexOf(key))->GetDepth());
  num_buckets_++;

  auto origin_items = origin_bucket->GetItems();
  for (const auto &[k, v] : origin_items) {
    if ((IndexOf(k) & local_mask) != origin_index) {
      origin_bucket->Remove(k);
      divide_bucket->Insert(k, v);
    }
  }

  for (size_t dir_index = 0; dir_index < dir_.size(); dir_index++) {
    if ((dir_index & local_mask) == origin_index) {
      dir_.at(dir_index) = origin_bucket;
    } else if ((dir_index & local_mask) == divide_index) {
      dir_.at(dir_index) = divide_bucket;
    }
  }

  if (!dir_.at(IndexOf(key))->IsFull()) {
    return dir_.at(IndexOf(key))->Insert(key, value);
  }
  return DoInsert(key, value);
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {
  // LOG_INFO("Bucket constructed with list_ address: %p, arr size: %ld.", &list_, size_);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (const auto &kv : list_) {
    if (key == kv.first) {
      value = kv.second;
      return true;
    }
  }

  return false;
  // UNREACHABLE("not implemented");
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for (auto it = list_.begin(); it != list_.end(); ++it) {
    if (key == it->first) {
      list_.erase(it);
      return true;
    }
  }

  return false;
  // UNREACHABLE("not implemented");
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  // mod in-place.
  for (auto &kv : list_) {
    if (key == kv.first) {
      kv.second = value;
      return true;
    }
  }

  if (IsFull()) {
    return false;
  }
  // list_.push_back(std::make_pair(key, value));
  list_.emplace_back(key, value);
  return true;
  // UNREACHABLE("not implemented");
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
