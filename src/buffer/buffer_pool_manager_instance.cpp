//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  // std::scoped_lock<std::mutex> lock(latch_);
  latch_.lock();
  frame_id_t new_frame_id = -1;
  // frame id. find from the free list first.
  if (!free_list_.empty()) {
    new_frame_id = free_list_.back();
    free_list_.pop_back();
  } else {
    if (replacer_->Evict(&new_frame_id)) {
      page_id_t stale_page_id = pages_[new_frame_id].GetPageId();
      if (pages_[new_frame_id].IsDirty()) {
        FlushPgImpInner(stale_page_id);
      }
      page_table_->Remove(stale_page_id);
    } else {
      // if now free_list and can evict nothing.
      latch_.unlock();
      return nullptr;
    }
  }
  /*
    reset the memory and metadata for the new page.
  */
  latch_.unlock();
  *page_id = AllocatePage();

  // LOG_INFO("new a page with `NewPgImp`. new page#%d. frame#%d", *page_id, new_frame_id);
  pages_[new_frame_id].ResetMemory();
  pages_[new_frame_id].page_id_ = *page_id;
  pages_[new_frame_id].pin_count_ = 1;
  pages_[new_frame_id].is_dirty_ = false;

  replacer_->SetEvictable(new_frame_id, false);
  replacer_->RecordAccess(new_frame_id);

  page_table_->Insert(*page_id, new_frame_id);
  return &pages_[new_frame_id];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  // std::scoped_lock<std::mutex> lock(latch_);
  latch_.lock();
  // page_id can present invalid page.
  if (page_id == INVALID_PAGE_ID) {
    latch_.unlock();
    return nullptr;
  }
  frame_id_t corresponding_f_id;
  // fetch in buffer pool in success.
  if (page_table_->Find(page_id, corresponding_f_id)) {
    // is it??
    pages_[corresponding_f_id].pin_count_++;
    replacer_->SetEvictable(corresponding_f_id, false);
    replacer_->RecordAccess(corresponding_f_id);
    // LOG_INFO("####0 get from page table. page#%d, frame#%d, pin cnt: %d.", page_id, corresponding_f_id,
    //          pages_[corresponding_f_id].pin_count_);
    latch_.unlock();
    return &pages_[corresponding_f_id];
    // other wise find a appropriate frame first.
  }

  if (!free_list_.empty()) {
    corresponding_f_id = free_list_.back();
    free_list_.pop_back();
  } else {
    if (replacer_->Evict(&corresponding_f_id)) {
      page_id_t stale_page_id = pages_[corresponding_f_id].GetPageId();
      if (pages_[corresponding_f_id].IsDirty()) {
        FlushPgImpInner(stale_page_id);
      }
      page_table_->Remove(stale_page_id);
    } else {
      latch_.unlock();
      return nullptr;
      // return NULL;
    }
  }
  // LOG_INFO("fetch an existing page with `FetchPgImp`. page#%d. frame#%d", page_id, corresponding_f_id);
  // read the page from disk.
  pages_[corresponding_f_id].ResetMemory();
  pages_[corresponding_f_id].page_id_ = page_id;
  pages_[corresponding_f_id].pin_count_ = 1;
  pages_[corresponding_f_id].is_dirty_ = false;

  disk_manager_->ReadPage(page_id, pages_[corresponding_f_id].data_);

  replacer_->SetEvictable(corresponding_f_id, false);
  replacer_->RecordAccess(corresponding_f_id);
  page_table_->Insert(page_id, corresponding_f_id);

  latch_.unlock();
  return &pages_[corresponding_f_id];
  // return nullptr;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t corresponding_f_id;
  /*
    If page_id is not in the buffer pool or
    its pin count is already 0, return false.
  */
  if (!page_table_->Find(page_id, corresponding_f_id)) {
    return false;
  }
  if (pages_[corresponding_f_id].GetPinCount() <= 0) {
    return false;
  }

  if (--pages_[corresponding_f_id].pin_count_ == 0) {
    // set evitable.

    replacer_->SetEvictable(corresponding_f_id, true);
  }
  if (is_dirty) {
    pages_[corresponding_f_id].is_dirty_ = true;
  }
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  // std::scoped_lock<std::mutex> lock(latch_);
  return FlushPgImpInner(page_id);
}

auto BufferPoolManagerInstance::FlushPgImpInner(page_id_t page_id) -> bool {
  frame_id_t corresponding_f_id;
  // if the page id is invalid or not in the buffer pool.
  if (page_id == INVALID_PAGE_ID || !page_table_->Find(page_id, corresponding_f_id)) {
    return false;
  }

  // flush into disk, then unset dirty flag.
  pages_[corresponding_f_id].RLatch();
  disk_manager_->WritePage(page_id, pages_[corresponding_f_id].GetData());
  pages_[corresponding_f_id].RUnlatch();

  pages_[corresponding_f_id].is_dirty_ = false;

  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // std::scoped_lock<std::mutex> lock(latch_);
  latch_.lock();
  size_t pool_size = GetPoolSize();
  latch_.unlock();

  for (size_t i = 0; i < pool_size; i++) {
    pages_[i].RLatch();
    disk_manager_->WritePage(pages_[i].GetPageId(), pages_[i].GetData());
    pages_[i].RUnlatch();

    pages_[i].is_dirty_ = false;
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t corresponding_f_id;
  /*
    If page_id is not in the buffer pool,
    if the page is pinned.
  */
  if (!page_table_->Find(page_id, corresponding_f_id)) {
    return true;
  }
  if (pages_[corresponding_f_id].GetPinCount() > 0) {
    return false;
  }

  /*
    stop tracking the frame in the replacer,
    add the frame back to the free list,
    reset the page's memory and metadata.
    DeallocatePage() freeing the page on the disk.
  */
  // LOG_INFO("attempts delete page#%d, frame#%d", page_id, corresponding_f_id);
  free_list_.push_back(corresponding_f_id);
  page_table_->Remove(page_id);
  replacer_->Remove(corresponding_f_id);

  // pages_[corresponding_f_id].rwlatch_.WLock();
  pages_[corresponding_f_id].ResetMemory();
  pages_[corresponding_f_id].page_id_ = INVALID_PAGE_ID;
  // pages_[corresponding_f_id].rwlatch_.WUnlock();

  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
