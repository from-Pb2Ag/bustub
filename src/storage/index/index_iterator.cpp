/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
// INDEXITERATOR_TYPE::IndexIterator() = default;
INDEXITERATOR_TYPE::IndexIterator() : cur_leaf_page_ptr_(nullptr), cur_offset_(0), end_flag_(true), ret_to_(nullptr) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *mng, BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *ptr,
                                  size_t offset, size_t this_quota, std::atomic<size_t> *ret_to)
    : cur_leaf_page_ptr_(ptr), cur_offset_(offset), end_flag_(false), buffer_pool_manager_(mng), ret_to_(ret_to) {
  this_quota_.store(this_quota);
}

INDEX_TEMPLATE_ARGUMENTS
// INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT
INDEXITERATOR_TYPE::~IndexIterator() {
  if (ret_to_ != nullptr) {
    LOG_INFO("return %ld pages to buffer pool.", this_quota_.load());
    ret_to_->fetch_add(this_quota_.load());
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() const -> bool {
  //   size_t offset_value = cur_offset_.load(std::memory_order_seq_cst);
  //   return offset_value >= static_cast<size_t>(cur_leaf_page_ptr_->GetSize());
  bool is_end = end_flag_.load(std::memory_order_seq_cst);
  return is_end;
  //   throw std::runtime_error("unimplemented");
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  size_t offset_value = cur_offset_.load(std::memory_order_seq_cst);
  return cur_leaf_page_ptr_->KVAt(offset_value);
  //   return {cur_leaf_page_ptr_->KeyAt(offset_value), cur_leaf_page_ptr_->ValueAt(offset_value)};
  //   throw std::runtime_error("unimplemented");
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  cur_offset_.fetch_add(1, std::memory_order_seq_cst);
  size_t offset_value = cur_offset_.load(std::memory_order_seq_cst);
  if (offset_value >= static_cast<size_t>(cur_leaf_page_ptr_->GetSize())) {
    page_id_t next_page_id = cur_leaf_page_ptr_->GetNextPageId();
    reinterpret_cast<Page *>(cur_leaf_page_ptr_)->RUnlatch();
    buffer_pool_manager_->UnpinPage(cur_leaf_page_ptr_->GetPageId(), false);

    if (next_page_id == INVALID_PAGE_ID) {
      end_flag_.store(true, std::memory_order_seq_cst);
    } else {
      // a new leaf page?
      cur_leaf_page_ptr_ = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(
          buffer_pool_manager_->FetchPage(next_page_id));
      reinterpret_cast<Page *>(cur_leaf_page_ptr_)->RLatch();
      cur_offset_.store(0, std::memory_order_seq_cst);
    }
  }

  return *this;
  //   throw std::runtime_error("unimplemented");
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const -> bool {
  //   LOG_INFO("what fuck? `==`");
  if (itr.IsEnd()) {
    return IsEnd();
  }
  return cur_leaf_page_ptr_ == itr.cur_leaf_page_ptr_ && cur_offset_ == itr.cur_offset_;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const -> bool {
  //   LOG_INFO("what fuck? `!=`");
  if (itr.IsEnd()) {
    // LOG_INFO("this end? %d. who to comp? %d", IsEnd(), itr.IsEnd());
    return !IsEnd();
  }
  return cur_leaf_page_ptr_ != itr.cur_leaf_page_ptr_ || cur_offset_ != itr.cur_offset_;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
