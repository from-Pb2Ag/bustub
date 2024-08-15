#include <string>
#include <vector>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
auto GenerateNRandomString(int n) -> std::vector<std::string>;

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
  op_id_ = 0;
  is_empty_.store(true);
  root_locked_.store(static_cast<size_t>(RootLockType::UN_LOCKED));
  rem_cnt_.store(buffer_pool_manager->GetPoolSize());
  cur_height_.store(0);
  LOG_INFO("create a new B+ tree. leaf_max_size: %d, internal_max_size: %d. pool size: %ld.", leaf_max_size_,
           internal_max_size_, buffer_pool_manager_->GetPoolSize());
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
// auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return is_empty_.load(); }
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
  auto signatures = GenerateNRandomString(1);
  std::string signature = signatures[0];
  LOG_INFO("[%s]: attempts get value of key %ld. rem_quota: %ld.", signature.c_str(), key.ToString(), rem_cnt_.load());
  Page *ptr = nullptr;
  Page *tmp = nullptr;
  std::atomic<size_t> prime_quota = 2;

  do {
    std::unique_lock<std::mutex> lock(mux_);
    while (rem_cnt_.load() < prime_quota) {
      buffer_pool_page_quota_.wait(lock);
    }

    rem_cnt_.fetch_sub(prime_quota);
    LOG_INFO("[%s]: reserved frames. now rem_quota: %ld.", signature.c_str(), rem_cnt_.load());
    break;
  } while (true);

  ptr = buffer_pool_manager_->FetchPage(root_page_id_);
  ptr->RLatch();
  LOG_INFO("[%s]: page#%d acquire a R-latch. pin cnt: %d.", signature.c_str(), ptr->GetPageId(), ptr->GetPinCount());

  do {
    auto *tmp_ptr = reinterpret_cast<BPlusTreePage *>(ptr);

    if (!tmp_ptr->IsLeafPage()) {
      auto ptr_2_internal = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(ptr);
      auto ret = FindFirstInfIndex(key, ptr_2_internal);

      tmp = ptr;
      ptr = buffer_pool_manager_->FetchPage(ptr_2_internal->ValueAt(ret.first));
      // ----
      // tmp is parent.
      ptr->RLatch();
      LOG_INFO("[%s]: page#%d acquire a R-latch. pin cnt: %d.", signature.c_str(), ptr->GetPageId(),
               ptr->GetPinCount());
      tmp->RUnlatch();
      buffer_pool_manager_->UnpinPage(tmp->GetPageId(), false);
      LOG_INFO("[%s]: page#%d release a R-latch. pin cnt: %d.", signature.c_str(), tmp->GetPageId(),
               tmp->GetPinCount());
      // ----
      buffer_pool_manager_->UnpinPage(tmp->GetPageId(), false);
      if (ptr_2_internal->ValueAt(ret.first) == INVALID_PAGE_ID) {
        ptr->RUnlatch();
        buffer_pool_manager_->UnpinPage(ptr->GetPageId(), false);
        LOG_INFO("[%s]: page#%d release a R-latch. pin cnt: %d.", signature.c_str(), ptr->GetPageId(),
                 ptr->GetPinCount());
        rem_cnt_.fetch_add(prime_quota);
        buffer_pool_page_quota_.notify_one();
        return false;
      }
    } else {
      // only get value in leaf page.
      auto ptr_2_leaf = reinterpret_cast<BPlusTreeLeafPage<KeyType, RID, KeyComparator> *>(ptr);

      // int dst_index = BinarySearch(key, ptr_2_leaf);
      // if (comparator_(key, ptr_2_leaf->KeyAt(dst_index)) == 0) {
      //   result->push_back(ptr_2_leaf->ValueAt(dst_index));
      //   return true;
      // }
      for (int i = 0; i < ptr_2_leaf->GetSize(); i++) {
        KeyType this_key = ptr_2_leaf->KeyAt(i);
        ValueType this_value = ptr_2_leaf->ValueAt(i);
        int comp = comparator_(key, this_key);

        if (comp == 0) {
          result->push_back(this_value);
          ptr->RUnlatch();
          buffer_pool_manager_->UnpinPage(ptr_2_leaf->GetPageId(), false);
          LOG_INFO("[%s]: page#%d release a R-latch. pin cnt: %d.", signature.c_str(), ptr->GetPageId(),
                   ptr->GetPinCount());
          rem_cnt_.fetch_add(prime_quota);
          buffer_pool_page_quota_.notify_one();
          return true;
        }
        if (comp < 0) {
          ptr->RUnlatch();
          buffer_pool_manager_->UnpinPage(ptr_2_leaf->GetPageId(), false);
          LOG_INFO("[%s]: page#%d release a R-latch. pin cnt: %d.", signature.c_str(), ptr->GetPageId(),
                   ptr->GetPinCount());
          rem_cnt_.fetch_add(prime_quota);
          buffer_pool_page_quota_.notify_one();
          return false;
        }
      }
      ptr->RUnlatch();
      buffer_pool_manager_->UnpinPage(ptr_2_leaf->GetPageId(), false);
      LOG_INFO("[%s]: page#%d release a R-latch. pin cnt: %d.", signature.c_str(), ptr->GetPageId(),
               ptr->GetPinCount());
      rem_cnt_.fetch_add(prime_quota);
      buffer_pool_page_quota_.notify_one();
      return false;
    }
  } while (true);

  ptr->RUnlatch();
  buffer_pool_manager_->UnpinPage(ptr->GetPageId(), false);
  LOG_INFO("[%s]: page#%d release a R-latch. pin cnt: %d.", signature.c_str(), ptr->GetPageId(), ptr->GetPinCount());
  rem_cnt_.fetch_add(prime_quota);
  buffer_pool_page_quota_.notify_one();
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::BinarySearch(const KeyType &key, BPlusTreeLeafPage<KeyType, RID, KeyComparator> *page_ptr) -> int {
  int left = 0;
  int right = page_ptr->GetSize() - 1;

  while (left <= right) {
    int mid = (left + right) / 2;
    int comp = comparator_(key, page_ptr->KeyAt(mid));

    if (comp == 0) {
      return mid;
    }
    if (comp < 0) {
      right = mid - 1;
    } else {
      left = mid + 1;
    }
  }

  return left;
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
  auto signatures = GenerateNRandomString(1);
  std::string signature = signatures[0];
  LOG_INFO("[%s]: attempts insert kv: %ld. cur depth: %ld", signature.c_str(), key.ToString(), cur_height_.load());

  if (IsEmpty()) {
    std::lock_guard<std::mutex> lock(mux_);
    if (IsEmpty()) {
      auto *ptr_2_leaf = reinterpret_cast<BPlusTreeLeafPage<KeyType, RID, KeyComparator> *>(
          buffer_pool_manager_->NewPage(&root_page_id_));
      Page *ptr = reinterpret_cast<Page *>(ptr_2_leaf);
      ptr->WLatch();
      // LOG_INFO("page#%d latch.", ptr->GetPageId());
      root_locked_.store(static_cast<size_t>(RootLockType::WRITE_LOCKED));
      UpdateRootPageId();

      // LOG_INFO("1update root page id: %d", root_page_id_);
      ptr_2_leaf->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);

      ptr_2_leaf->SetKeyAt(0, key);
      ptr_2_leaf->SetValueAt(0, value);
      ptr_2_leaf->IncreaseSize(1);

      ptr_2_leaf->SetNextPageId(INVALID_PAGE_ID);

      // LogLeafPage(ptr_2_leaf);
      root_locked_.store(static_cast<size_t>(RootLockType::UN_LOCKED));
      cur_height_.fetch_add(1);
      // LOG_INFO("page#%d un-latch.", ptr->GetPageId());
      ptr->WUnlatch();
      buffer_pool_manager_->UnpinPage(root_page_id_, true);
      is_empty_.store(false);
      LOG_INFO("[%s]: attempts insert kv: %ld. success path 1.", signature.c_str(), key.ToString());
      return true;
    }
  }

  std::unordered_map<page_id_t, bool> unpin_is_dirty;
  std::unordered_map<page_id_t, size_t> fuck;
  std::unordered_map<page_id_t, Page *> cached_ptr;
  // for some pages (in ancestor), we can un-pin them as early as we can. we un-pin them and remove from coll.
  std::unordered_map<page_id_t, Page *> unpin_coll;
  Page *ptr = nullptr;

  // 2 * (cur_height_.load() + 1) 至多消耗的frame数?
  // 当前剩余可用frame数 rem_cnt_.load()
  // while (rem_cnt_.load() < 2 * (cur_height_.load() + 1)) {

  // }
  std::atomic<size_t> prime_quota = 2 * (cur_height_.load() + 1);
  do {
    std::unique_lock<std::mutex> lock(mux_);
    while (rem_cnt_.load() < prime_quota) {
      LOG_INFO("[%s]: buffer pool is stained, sleep.", signature.c_str());
      buffer_pool_page_quota_.wait(lock);
    }

    rem_cnt_.fetch_sub(prime_quota);
    LOG_INFO("[%s]: buffer get buffer pool reserve.", signature.c_str());
    break;
  } while (true);

  do {
    std::unique_lock<std::mutex> lock(mux_);
    while (root_locked_.load() != static_cast<size_t>(RootLockType::UN_LOCKED)) {
      LOG_INFO("[%s]: root page is locked, sleep.", signature.c_str());
      c_v_.wait(lock);
    }

    root_locked_.store(static_cast<size_t>(RootLockType::WRITE_LOCKED));

    if (auto it = cached_ptr.find(root_page_id_); it != cached_ptr.end()) {
      ptr = it->second;
    } else {
      ptr = buffer_pool_manager_->FetchPage(root_page_id_);
      cached_ptr.insert({root_page_id_, ptr});
      unpin_coll.insert({root_page_id_, ptr});
    }

    ptr->WLatch();
    op_id_++;
    LOG_INFO("[%s]: acquire root page W-latch.", signature.c_str());
    break;
  } while (true);

  if (unpin_is_dirty.find(root_page_id_) == unpin_is_dirty.end()) {
    unpin_is_dirty.insert({root_page_id_, false});
    // LOG_INFO("pin page #%d", root_page_id_);
  }

  std::vector<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *> st;
  size_t next_unlock_idx = 0;

  BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *ptr_2_internal = nullptr;
  BPlusTreeLeafPage<KeyType, RID, KeyComparator> *ptr_2_leaf = nullptr;
  BPlusTreePage *cur_bpluspage_ptr = nullptr;

  do {
    cur_bpluspage_ptr = reinterpret_cast<BPlusTreePage *>(ptr);
    if (!cur_bpluspage_ptr->IsLeafPage()) {
      ptr_2_internal = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(cur_bpluspage_ptr);
      st.push_back(ptr_2_internal);

      auto ret = FindFirstInfIndex(key, ptr_2_internal);
      // already existing (in internal page, fast path).
      if (!ret.second) {
        for (size_t i = next_unlock_idx; i < st.size(); i++) {
          Page *damn = reinterpret_cast<Page *>(st[i]);
          if (i == 0) {
            // mux_.lock();
            // root_locked_.store(false);
            // c_v_.notify_one();
            // mux_.unlock();
            UnlatchRootPage();
          }
          damn->WUnlatch();
        }
        UnpinPages(unpin_coll, signature);

        rem_cnt_.fetch_add(prime_quota);
        buffer_pool_page_quota_.notify_one();

        LOG_INFO("[%s]: attempts insert kv: %ld. fail path 1.", signature.c_str(), key.ToString());
        return false;
      }

      page_id_t nxt_level_page = ptr_2_internal->ValueAt(ret.first);
      // LOG_INFO("[%s]: op #%d attempts insert kv: %ld. ####. fetch page?%d", signature.c_str(), op_id_,
      // key.ToString(),
      //          nxt_level_page);
      // bool damn_it = false;
      if (auto it = cached_ptr.find(nxt_level_page); it != cached_ptr.end()) {
        ptr = it->second;
      } else {
        ptr = buffer_pool_manager_->FetchPage(nxt_level_page);
        cached_ptr.insert({nxt_level_page, ptr});
        unpin_coll.insert({nxt_level_page, ptr});
        // damn_it = true;
      }

      ptr->WLatch();

      // if `ptr` will not split. unlock its ancestors' W latch.
      // then un-pin them (not dirty) with fast path.
      auto peek_ptr = reinterpret_cast<BPlusTreePage *>(ptr);
      if (peek_ptr->GetSize() < peek_ptr->GetMaxSize()) {
        // std::atomic<size_t> un_pin_cnt = 0;
        for (size_t i = next_unlock_idx; i < st.size(); i++) {
          Page *damn = reinterpret_cast<Page *>(st[i]);
          // page_id_t damn_pid = damn->GetPageId();
          if (i == 0) {
            UnlatchRootPage();
          }

          damn->WUnlatch();
          // buffer_pool_manager_->UnpinPage(damn_pid, false);
          // unpin_coll.erase(damn_pid);
          // un_pin_cnt.fetch_add(1);
        }
        // LOG_INFO("fast path un-pin %ld pages.", un_pin_cnt.load());
        // prime_quota.fetch_sub(un_pin_cnt.load());
        // rem_cnt_.fetch_add(un_pin_cnt.load());
        // buffer_pool_page_quota_.notify_one();

        next_unlock_idx = st.size();
      }

      if (unpin_is_dirty.find(ptr->GetPageId()) == unpin_is_dirty.end()) {
        unpin_is_dirty.insert({ptr->GetPageId(), false});
        // LOG_INFO("pin page #%d", ptr->GetPageId());
      }
      if (ptr->GetPageId() == 0) {
        break;
      }
      continue;
    }

    ptr_2_leaf = reinterpret_cast<BPlusTreeLeafPage<KeyType, RID, KeyComparator> *>(cur_bpluspage_ptr);
    auto ret = FindFirstInfIndex(key, ptr_2_leaf);

    // already existing (in leaf page, slow path).
    if (!ret.second && ret.first < ptr_2_leaf->GetSize() + 1) {
      for (size_t i = next_unlock_idx; i < st.size(); i++) {
        Page *damn = reinterpret_cast<Page *>(st[i]);
        if (i == 0) {
          UnlatchRootPage();
        }
        damn->WUnlatch();
      }
      /*
        un-latch leaf page individually.
        what happens if the leaf page is ALSO the root?
      */
      if (st.empty()) {
        UnlatchRootPage();
      }
      ptr->WUnlatch();
      UnpinPages(unpin_coll, signature);

      rem_cnt_.fetch_add(prime_quota);
      buffer_pool_page_quota_.notify_one();

      LOG_INFO("[%s]: attempts insert kv: %ld. fail path 2.", signature.c_str(), key.ToString());
      return false;
    }

    // can insert in safe.
    unpin_is_dirty.insert_or_assign(ptr_2_leaf->GetPageId(), true);
    if (ptr_2_leaf->GetSize() < ptr_2_leaf->GetMaxSize()) {
      std::atomic<size_t> un_pin_cnt = 0;
      // devil lives here.
      for (size_t i = next_unlock_idx; i < st.size(); i++) {
        Page *damn = reinterpret_cast<Page *>(st[i]);
        if (i == 0) {
          UnlatchRootPage();
        }

        damn->WUnlatch();
        // buffer_pool_manager_->UnpinPage(damn_pid, false);
        // unpin_coll.erase(damn_pid);
        un_pin_cnt.fetch_add(1);
      }

      if ((un_pin_cnt.load() >> 1) >= cur_height_.load()) {
        for (size_t i = next_unlock_idx; i < st.size(); i++) {
          Page *damn = reinterpret_cast<Page *>(st[i]);
          page_id_t damn_pid = damn->GetPageId();

          buffer_pool_manager_->UnpinPage(damn_pid, false);
          unpin_coll.erase(damn_pid);
        }
        prime_quota.fetch_sub(un_pin_cnt.load());
        rem_cnt_.fetch_add(un_pin_cnt.load());
        buffer_pool_page_quota_.notify_one();
      }

      next_unlock_idx = st.size();
      // ----

      ptr_2_leaf->MoveForward(ret.first);
      ptr_2_leaf->SetKeyAt(ret.first, key);
      ptr_2_leaf->SetValueAt(ret.first, value);
      ptr_2_leaf->IncreaseSize(1);
      if (ptr_2_leaf->GetParentPageId() == INVALID_PAGE_ID) {
        UnlatchRootPage();
      }
      // LOG_INFO("[%s]: page#%d un-latch.", signature.c_str(), ptr->GetPageId());
      ptr->WUnlatch();

      UnpinPages(unpin_coll, signature);
      // LOG_INFO("[%s]: attempts insert kv: %ld. ok2.", signature.c_str(), key.ToString());
      rem_cnt_.fetch_add(prime_quota);
      buffer_pool_page_quota_.notify_one();

      LOG_INFO("[%s]: attempts insert kv: %ld. success path 2.", signature.c_str(), key.ToString());
      return true;
    }
    // A = ptr_2_leaf->GetMaxSize() + 1. => [0, A / 2), [A / 2, A).
    /*
      |----------|      |-------------|
      |ptr_2_leaf| ---> |next_leaf_ptr|
      |----------|      |-------------|

      |----------|      |---|      |-------------|
      |ptr_2_leaf| ---> |new| ---> |next_leaf_ptr|
      |----------|      |---|      |-------------|
      `<····` are only conditional.
    */
    page_id_t next_page = ptr_2_leaf->GetNextPageId();
    page_id_t neww_page = INVALID_PAGE_ID;

    Page *neww_ptr = buffer_pool_manager_->NewPage(&neww_page);
    cached_ptr.insert({neww_page, neww_ptr});
    unpin_coll.insert({neww_page, neww_ptr});
    // Page *next_ptr = buffer_pool_manager_->FetchPage(next_page);
    // neww_ptr->WLatch();

    // fuck[neww_page]++;
    // fuck[next_page]++;

    unpin_is_dirty.insert_or_assign(neww_page, true);
    if (unpin_is_dirty.find(next_page) == unpin_is_dirty.end()) {
      unpin_is_dirty.insert({next_page, false});
    }

    auto *neww_page_ptr = reinterpret_cast<BPlusTreeLeafPage<KeyType, RID, KeyComparator> *>(neww_ptr);

    if (neww_page_ptr == nullptr) {
      LOG_INFO("neww_page id: %d", neww_page);
      throw std::runtime_error("Pointer neww_page_ptr is null.");
    }

    neww_page_ptr->Init(neww_page, ptr_2_leaf->GetParentPageId(), leaf_max_size_);
    neww_page_ptr->SetNextPageId(next_page);
    ptr_2_leaf->SetNextPageId(neww_page);

    // ret = FindFirstInfIndex(key, ptr_2_leaf);
    int total = ptr_2_leaf->GetMaxSize() + 1;
    int rem_to_mov = total - (total / 2);
    int save = rem_to_mov;
    int cur = ptr_2_leaf->GetSize();
    int cnt = 0;
    bool flag = false;

    if (ret.first == cur) {
      // ptr_2_leaf ----> neww_page_ptr. the additional one is also moved to neww_page_ptr.
      neww_page_ptr->SetKeyAt(rem_to_mov - 1, key);
      neww_page_ptr->SetValueAt(rem_to_mov - 1, value);
      rem_to_mov--;
      flag = true;
    }
    while (rem_to_mov > 0) {
      cnt++;
      neww_page_ptr->SetKeyAt(rem_to_mov - 1, ptr_2_leaf->KeyAt(cur - 1));
      neww_page_ptr->SetValueAt(rem_to_mov - 1, ptr_2_leaf->ValueAt(cur - 1));
      rem_to_mov--;
      cur--;
      if (!flag && ret.first == cur && rem_to_mov > 0) {
        // ptr_2_leaf ----> neww_page_ptr. the additional one is also moved to neww_page_ptr.
        neww_page_ptr->SetKeyAt(rem_to_mov - 1, key);
        neww_page_ptr->SetValueAt(rem_to_mov - 1, value);
        rem_to_mov--;
        flag = true;
      }
    }

    ptr_2_leaf->IncreaseSize(-cnt);
    neww_page_ptr->IncreaseSize(save);
    // neww_ptr->WUnlatch();
    if (!flag) {
      ptr_2_leaf->MoveForward(ret.first);
      ptr_2_leaf->SetKeyAt(ret.first, key);
      ptr_2_leaf->SetValueAt(ret.first, value);
      ptr_2_leaf->IncreaseSize(1);
    }

    // LOG_INFO("[%s]: op #%d WTF????1", signature.c_str(), op_id_);
    // new root page is yield.
    if (ptr_2_leaf->GetParentPageId() == INVALID_PAGE_ID) {
      Page *tmp = buffer_pool_manager_->NewPage(&root_page_id_);
      cached_ptr.insert({root_page_id_, tmp});
      unpin_coll.insert({root_page_id_, tmp});
      // LOG_INFO("[%s]: 2update root page id: %d", signature.c_str(), root_page_id_);
      UpdateRootPageId();
      // fuck[root_page_id_]++;

      auto *new_root = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(tmp);
      // fuck[root_page_id_]++;
      unpin_is_dirty.insert_or_assign(root_page_id_, true);
      new_root->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);

      ptr_2_leaf->SetParentPageId(root_page_id_);
      neww_page_ptr->SetParentPageId(root_page_id_);

      new_root->SetKeyAt(0, ptr_2_leaf->KeyAt(0));
      new_root->SetValueAt(0, ptr_2_leaf->GetPageId());
      new_root->SetKeyAt(1, neww_page_ptr->KeyAt(0));
      new_root->SetValueAt(1, neww_page);
      new_root->SetSize(2);
      cur_height_.fetch_add(1);

      // LogInternalPage(new_root);
      for (size_t i = next_unlock_idx; i < st.size(); i++) {
        Page *damn = reinterpret_cast<Page *>(st[i]);
        if (i == 0) {
          UnlatchRootPage();
        }
        // LOG_INFO("[%s]: page#%d un-latch.", signature.c_str(), damn->GetPageId());
        damn->WUnlatch();
      }

      if (st.empty()) {
        UnlatchRootPage();
      }
      // LOG_INFO("[%s]: page#%d un-latch.", signature.c_str(), ptr->GetPageId());
      ptr->WUnlatch();
      // LOG_INFO("[%s]: UnpinPages case4", signature.c_str());
      UnpinPages(unpin_coll, signature);
      // LOG_INFO("[%s]: attempts insert kv: %ld. ok3.", signature.c_str(), key.ToString());
      rem_cnt_.fetch_add(prime_quota);
      buffer_pool_page_quota_.notify_one();

      LOG_INFO("[%s]: attempts insert kv: %ld. success path 3.", signature.c_str(), key.ToString());
      return true;
    }
    // !!!!
    // LOG_INFO("[%s]: op #%d WTF????2", signature.c_str(), op_id_);
    InsertInternalCanSplit(st, neww_page_ptr->KeyAt(0), neww_page_ptr->GetPageId(), &unpin_is_dirty, &fuck, &cached_ptr,
                           &unpin_coll, signature);
    for (size_t i = next_unlock_idx; i < st.size(); i++) {
      Page *damn = reinterpret_cast<Page *>(st[i]);
      if (i == 0) {
        UnlatchRootPage();
      }
      // LOG_INFO("[%s]: page#%d un-latch.", signature.c_str(), damn->GetPageId());
      damn->WUnlatch();
    }
    // LOG_INFO("[%s]: page#%d un-latch.", signature.c_str(), ptr->GetPageId());
    ptr->WUnlatch();
    // LOG_INFO("[%s]: UnpinPages case5", signature.c_str());

    UnpinPages(unpin_coll, signature);
    // LOG_INFO("[%s]: attempts insert kv: %ld. ok4.", signature.c_str(), key.ToString());
    rem_cnt_.fetch_add(prime_quota);
    buffer_pool_page_quota_.notify_one();

    LOG_INFO("[%s]: attempts insert kv: %ld. success path 4.", signature.c_str(), key.ToString());
    return true;
  } while (true);

  for (size_t i = next_unlock_idx; i < st.size(); i++) {
    Page *damn = reinterpret_cast<Page *>(st[i]);
    if (i == 0) {
      UnlatchRootPage();
    }
    // LOG_INFO("[%s]: page#%d un-latch.", signature.c_str(), damn->GetPageId());
    damn->WUnlatch();
  }
  // LOG_INFO("[%s]: page#%d un-latch.", signature.c_str(), ptr->GetPageId());
  ptr->WUnlatch();
  // LOG_INFO("[%s]: UnpinPages case6", signature.c_str());
  UnpinPages(unpin_coll, signature);
  // LOG_INFO("[%s]: attempts insert kv: %ld. return path 2", signature.c_str(), key.ToString());
  rem_cnt_.fetch_add(prime_quota);
  buffer_pool_page_quota_.notify_one();

  LOG_INFO("[%s]: attempts insert kv: %ld. fail path 3.", signature.c_str(), key.ToString());
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::LogBPlusTreePageHeader(BPlusTreePage *page) {
  std::string type_content;
  if (page->IsRootPage()) {
    type_content += "R|";
  }
  if (page->IsLeafPage()) {
    type_content += "L|";
  } else {
    type_content += "I|";
  }
  if (type_content.size() >= 2) {
    type_content.pop_back();
  }

  std::string cur_size_content = std::to_string(page->GetSize());
  std::string max_size_content = std::to_string(page->GetMaxSize());
  std::string ppid_content = std::to_string(page->GetParentPageId());
  std::string pid_content = std::to_string(page->GetPageId());
  std::string nxt_pid_content;
  if (page->IsLeafPage()) {
    nxt_pid_content +=
        std::to_string(reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(page)->GetNextPageId());
  }

  std::ostringstream oss;

  std::string e11 = "Type: " + type_content;
  std::string e13 = "cur_sz: " + cur_size_content;
  std::string e14 = "max_sz: " + max_size_content;
  std::string e21 = "PPid: " + ppid_content;
  std::string e22 = "Pid: " + pid_content;
  std::string e23;
  if (!nxt_pid_content.empty()) {
    e23 += "Nid: " + nxt_pid_content;
  }

  size_t max_len = std::max({e11.size(), e13.size(), e14.size(), e21.size(), e22.size(), e23.size()});
  max_len = std::max(max_len, static_cast<size_t>(3));
  size_t ele_in_row = 3;
  size_t dash_line_len = (max_len + 3) * ele_in_row + 1;

  oss << "\n";

  oss << std::string(dash_line_len, '-') << std::endl;
  oss << "| " << std::setw(max_len) << std::left << e11 << " | " << std::setw(max_len) << std::left << e13 << " | "
      << std::setw(max_len) << std::left << e14 << " |\n";
  oss << std::string(dash_line_len, '-') << std::endl;
  oss << "| " << std::setw(max_len) << std::left << e21 << " | " << std::setw(max_len) << std::left << e22 << " | "
      << std::setw(max_len) << std::left << e23 << " |\n";
  oss << std::string(dash_line_len, '-') << std::endl;

  std::string format_str = oss.str();
  LOG_INFO("%s", format_str.c_str());
}

INDEX_TEMPLATE_ARGUMENTS
auto LogLeafPage(bustub::BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *ptr_2_leaf) -> void {
  LOG_INFO("****start log leaf page #%d****", ptr_2_leaf->GetPageId());
  for (int i = 0; i < ptr_2_leaf->GetSize(); i++) {
    LOG_INFO("[%ld:%s]", ptr_2_leaf->KeyAt(i).ToString(), ptr_2_leaf->ValueAt(i).ToString().c_str());
  }
  LOG_INFO("****end   log leaf page #%d****", ptr_2_leaf->GetPageId());
}

INDEX_TEMPLATE_ARGUMENTS
auto LogInternalPage(bustub::BPlusTreeInternalPage<KeyType, ValueType, KeyComparator> *ptr_2_internal) -> void {
  LOG_INFO("****start log inte page #%d****", ptr_2_internal->GetPageId());
  LOG_INFO("its parent id: #%d", ptr_2_internal->GetParentPageId());
  for (int i = 0; i < ptr_2_internal->GetSize(); i++) {
    LOG_INFO("[%ld:%d]", ptr_2_internal->KeyAt(i).ToString(), ptr_2_internal->ValueAt(i));
  }
  LOG_INFO("****end   log inte page #%d****", ptr_2_internal->GetPageId());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnpinPages(const std::unordered_map<page_id_t, Page *> &unpin_coll, const std::string &sig) {
  for (const auto &[pid, ptr] : unpin_coll) {
    buffer_pool_manager_->UnpinPage(pid, true);
    // LOG_INFO("[%s]: fuck, un-pin page #%d. now pin cnt: %d", sig.c_str(), pid, ptr->GetPinCount());
  }
  LOG_INFO("total un-pin cnt: %ld.", unpin_coll.size());
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
  auto signatures = GenerateNRandomString(1);
  std::string signature = signatures[0];
  LOG_INFO("[%s]: attempts remove k:  %ld. rem quota: %ld", signature.c_str(), key.ToString(), rem_cnt_.load());

  if (IsEmpty()) {
    LOG_INFO("empty remove path 1.");
    return;
  }

  std::unordered_map<page_id_t, Page *> cached_ptr;
  std::unordered_map<page_id_t, Page *> unpin_coll;
  std::set<Page *> stale_root_coll;
  Page *ptr = nullptr;

  std::atomic<size_t> prime_quota = 2 * (cur_height_.load() + 1);
  do {
    std::unique_lock<std::mutex> lock(mux_);
    while (rem_cnt_.load() < prime_quota) {
      LOG_INFO("[%s]: buffer pool is strained, sleep.", signature.c_str());
      buffer_pool_page_quota_.wait(lock);
    }

    rem_cnt_.fetch_sub(prime_quota);
    LOG_INFO("[%s]: buffer get buffer pool reserve.", signature.c_str());
    break;
  } while (true);

  do {
    std::unique_lock<std::mutex> lock(mux_);
    while (root_locked_.load() != static_cast<size_t>(RootLockType::UN_LOCKED)) {
      LOG_INFO("[%s]: root page is locked, sleep.", signature.c_str());
      c_v_.wait(lock);
    }

    root_locked_.store(static_cast<size_t>(RootLockType::WRITE_LOCKED));

    if (auto it = cached_ptr.find(root_page_id_); it != cached_ptr.end()) {
      ptr = it->second;
    } else {
      ptr = buffer_pool_manager_->FetchPage(root_page_id_);
      cached_ptr.insert({root_page_id_, ptr});
      unpin_coll.insert({root_page_id_, ptr});
    }

    ptr->WLatch();
    LOG_INFO("[%s]: page#%d acquire a W-latch.", signature.c_str(), ptr->GetPageId());
    break;
  } while (true);

  std::vector<std::pair<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *, int>> st_1;
  size_t next_unlock_idx = 0;

  BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *ptr_2_internal = nullptr;
  BPlusTreePage *tmp_ptr = nullptr;

  do {
    tmp_ptr = reinterpret_cast<BPlusTreePage *>(ptr);

    // LOG_INFO("page id: %d", ptr->GetPageId());
    if (!tmp_ptr->IsLeafPage()) {
      ptr_2_internal = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(ptr);
      auto ret = FindFirstInfIndex(key, ptr_2_internal);
      st_1.push_back({ptr_2_internal, ret.first});
      LOG_INFO("page#%d en-stack.", ptr_2_internal->GetPageId());

      page_id_t nxt_level_page = ptr_2_internal->ValueAt(ret.first);

      if (auto it = cached_ptr.find(nxt_level_page); it != cached_ptr.end()) {
        ptr = it->second;
      } else {
        ptr = buffer_pool_manager_->FetchPage(nxt_level_page);
        cached_ptr.insert({nxt_level_page, ptr});
        unpin_coll.insert({nxt_level_page, ptr});
      }

      ptr->WLatch();
      tmp_ptr = reinterpret_cast<BPlusTreePage *>(ptr);
      LOG_INFO("[%s]: page#%d acquire a W-latch.", signature.c_str(), ptr->GetPageId());

      auto peek_ptr = reinterpret_cast<BPlusTreePage *>(ptr);
      if (peek_ptr->GetSize() > peek_ptr->GetMinSize()) {
        for (size_t i = next_unlock_idx; i < st_1.size(); i++) {
          Page *damn = reinterpret_cast<Page *>(st_1[i].first);
          if (i == 0) {
            UnlatchRootPage();
          }

          damn->WUnlatch();
        }
        next_unlock_idx = st_1.size();
      }
    } else {
      LOG_INFO("[%s]: lands page#%d.", signature.c_str(), ptr->GetPageId());
      auto ptr_2_leaf = reinterpret_cast<BPlusTreeLeafPage<KeyType, RID, KeyComparator> *>(ptr);

      size_t index_for_key = 0;
      bool find = false;
      int dst_index = BinarySearch(key, ptr_2_leaf);

      if (comparator_(key, ptr_2_leaf->KeyAt(dst_index)) == 0) {
        index_for_key = dst_index;
        find = true;
      }

      if (!find) {
        for (size_t i = next_unlock_idx; i < st_1.size(); i++) {
          Page *damn = reinterpret_cast<Page *>(st_1[i].first);
          if (i == 0) {
            UnlatchRootPage();
          }
          damn->WUnlatch();
        }
        if (st_1.empty()) {
          UnlatchRootPage();
        }
        ptr->WUnlatch();
        UnpinPages(unpin_coll, signature);

        rem_cnt_.fetch_add(prime_quota);
        buffer_pool_page_quota_.notify_one();

        LOG_INFO("not found remove path 1.");
        return;
      }

      // move `ptr_2_leaf` first.
      ptr_2_leaf->MoveBackward(index_for_key);
      ptr_2_leaf->IncreaseSize(-1);
      /*
        find case1: the leaf will not be merged (Simple Deletes), after delete, size still >= min size.
        if the FIRST kv in leaf page is deleted, will look-up.
      */
      if (ptr_2_leaf->GetSize() >= ptr_2_leaf->GetMinSize() || ptr_2_leaf->IsRootPage()) {
        // ptr_2_leaf->MoveBackward(index_for_key);
        // ptr_2_leaf->IncreaseSize(-1);

        size_t populate_index = index_for_key;
        KeyType new_leaf_first_key = ptr_2_leaf->KeyAt(0);
        for (int i = st_1.size() - 1; i >= 0 && populate_index == 0; i--) {
          st_1[i].first->SetKeyAt(st_1[i].second, new_leaf_first_key);
          populate_index = st_1[i].second;
        }
        for (size_t i = next_unlock_idx; i < st_1.size(); i++) {
          Page *damn = reinterpret_cast<Page *>(st_1[i].first);
          if (i == 0) {
            UnlatchRootPage();
          }
          damn->WUnlatch();
        }
        if (st_1.empty()) {
          UnlatchRootPage();
        }
        ptr->WUnlatch();

        UnpinPages(unpin_coll, signature);

        rem_cnt_.fetch_add(prime_quota);
        buffer_pool_page_quota_.notify_one();

        LOG_INFO("find remove path 1.");
        return;
      }

      if (st_1.back().second > 0) {
        page_id_t prev_page_id = st_1.back().first->ValueAt(st_1.back().second - 1);
        Page *prev_ptr = nullptr;
        BPlusTreeLeafPage<KeyType, RID, KeyComparator> *prev_leaf_ptr = nullptr;

        if (auto it = cached_ptr.find(prev_page_id); it != cached_ptr.end()) {
          prev_ptr = it->second;
        } else {
          prev_ptr = buffer_pool_manager_->FetchPage(prev_page_id);
          prev_leaf_ptr = reinterpret_cast<BPlusTreeLeafPage<KeyType, RID, KeyComparator> *>(prev_ptr);
          cached_ptr.insert({prev_page_id, prev_ptr});
          unpin_coll.insert({prev_page_id, prev_ptr});
        }

        prev_ptr->WLatch();

        // prev can "provide" the last one.
        // ptr_2_leaf page using it as the new first key (and look up.).
        assert(prev_leaf_ptr != nullptr);
        if (prev_leaf_ptr->GetSize() > ptr_2_leaf->GetMinSize()) {
          KeyType mov_key = prev_leaf_ptr->KeyAt(prev_leaf_ptr->GetSize() - 1);
          ValueType mov_value = prev_leaf_ptr->ValueAt(prev_leaf_ptr->GetSize() - 1);

          ptr_2_leaf->MoveForward(0);
          ptr_2_leaf->SetKeyAt(0, mov_key);
          ptr_2_leaf->SetValueAt(0, mov_value);
          ptr_2_leaf->IncreaseSize(1);

          prev_leaf_ptr->IncreaseSize(-1);

          KeyType new_leaf_first_key = ptr_2_leaf->KeyAt(0);
          FirstKeyPopulateUp(new_leaf_first_key, &st_1);

          for (size_t i = next_unlock_idx; i < st_1.size(); i++) {
            Page *damn = reinterpret_cast<Page *>(st_1[i].first);
            if (i == 0) {
              UnlatchRootPage();
            }
            damn->WUnlatch();
          }

          if (st_1.empty()) {
            UnlatchRootPage();
          }
          prev_ptr->WUnlatch();
          ptr->WUnlatch();
          UnpinPages(unpin_coll, signature);

          rem_cnt_.fetch_add(prime_quota);
          buffer_pool_page_quota_.notify_one();

          LOG_INFO("find remove path 2.");
          return;
        }

        // merged to prev page (merge leaf pages)?
        // and set flag as TRUE. if merged with next page toggle to FALSE.
        MergedToLeftSibling(prev_leaf_ptr, ptr_2_leaf);
        bool flag = true;
        // this line replace the following ones.
        // ----
        // int before_size = prev_leaf_ptr->GetSize();
        // for (int i = 0; i < ptr_2_leaf->GetSize(); i++) {
        //   prev_leaf_ptr->SetKeyAt(before_size + i, ptr_2_leaf->KeyAt(i));
        //   prev_leaf_ptr->SetValueAt(before_size + i, ptr_2_leaf->ValueAt(i));
        // }
        // prev_leaf_ptr->SetNextPageId(ptr_2_leaf->GetNextPageId());
        // prev_leaf_ptr->IncreaseSize(ptr_2_leaf->GetSize());

        // buffer_pool_manager_->DeletePage(ptr_2_leaf->GetPageId());
        // ----

        // st_1.back().first->MoveBackward(st_1.back().second);
        // st_1.back().first->IncreaseSize(-1);

        for (int i = st_1.size() - 1; i >= 0; i--) {
          KeyType old_first_key = st_1[i].first->KeyAt(0);
          if (flag) {
            st_1[i].first->MoveBackward(st_1[i].second);
          } else {
            st_1[i].first->MoveBackward(st_1[i].second + 1);
          }
          st_1[i].first->IncreaseSize(-1);
          if (st_1[i].first->GetSize()) {
            KeyType new_first_key = st_1[i].first->KeyAt(0);
            LOG_INFO("page#%d. old/new key: %ld, %ld", st_1[i].first->GetPageId(), old_first_key.ToString(),
                     new_first_key.ToString());
            PopulateUpV2(st_1[i].first, old_first_key, new_first_key);
          }
          if (st_1[i].first->GetSize() >= st_1[i].first->GetMinSize()) {
            break;
          }
          if (i > 0) {
            // internal borrow/merge with left sibling.
            if (st_1[i - 1].second > 0) {
              // the other one is  st_1[i].first.
              page_id_t prev_page_id = st_1[i - 1].first->ValueAt(st_1[i - 1].second - 1);
              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *prev_ptr = nullptr;

              if (auto it = cached_ptr.find(prev_page_id); it != cached_ptr.end()) {
                prev_ptr = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(it->second);
              } else {
                prev_ptr = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(
                    buffer_pool_manager_->FetchPage(prev_page_id));
                cached_ptr.insert({prev_page_id, reinterpret_cast<Page *>(prev_ptr)});
                unpin_coll.insert({prev_page_id, reinterpret_cast<Page *>(prev_ptr)});
              }

              if (prev_ptr->GetSize() > prev_ptr->GetMinSize()) {
                BorrowFromLeftSibling(prev_ptr, st_1[i].first);
                break;
              }
              MergedToLeftSibling(prev_ptr, st_1[i].first);
              flag = true;
              // internal borrow/merge with right sibling.
            } else {
              page_id_t next_page_id = st_1[i - 1].first->ValueAt(st_1[i - 1].second + 1);
              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *next_ptr = nullptr;

              if (auto it = cached_ptr.find(next_page_id); it != cached_ptr.end()) {
                next_ptr = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(it->second);
              } else {
                next_ptr = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(
                    buffer_pool_manager_->FetchPage(next_page_id));
                cached_ptr.insert({next_page_id, reinterpret_cast<Page *>(next_ptr)});
                unpin_coll.insert({next_page_id, reinterpret_cast<Page *>(next_ptr)});
              }

              if (next_ptr->GetSize() > next_ptr->GetMinSize()) {
                BorrowFromRightSibling(st_1[i].first, next_ptr);
                break;
              }
              MergedWithRightSibling(st_1[i].first, next_ptr);
              // LogInternalPage(st_1[i].first);
              flag = false;
            }
          }
        }

        TreeHeightTrim(&stale_root_coll);
        for (const auto &ptr : stale_root_coll) {
          while (ptr->GetPinCount() > 0) {
            buffer_pool_manager_->UnpinPage(ptr->GetPageId(), false);
          }
          unpin_coll.erase(ptr->GetPageId());

          LOG_INFO("page#%d is deleted.", ptr->GetPageId());
          ptr->WUnlatch();
          buffer_pool_manager_->DeletePage(ptr->GetPageId());
        }

        for (size_t i = next_unlock_idx; i < st_1.size(); i++) {
          Page *damn = reinterpret_cast<Page *>(st_1[i].first);
          if (i == 0) {
            UnlatchRootPage();
          }
          damn->WUnlatch();
        }
        if (st_1.empty()) {
          UnlatchRootPage();
        }

        prev_ptr->WUnlatch();
        ptr->WUnlatch();

        UnpinPages(unpin_coll, signature);
        rem_cnt_.fetch_add(prime_quota);
        buffer_pool_page_quota_.notify_one();

        LOG_INFO("find remove path 3.");
        return;
      }

      if (st_1.back().second < st_1.back().first->GetSize() - 1) {
        // it should in line with `st_1.back().first->ValueAt(st_1.back().second + 1);`.
        page_id_t next_page_id = ptr_2_leaf->GetNextPageId();
        Page *next_ptr = nullptr;
        BPlusTreeLeafPage<KeyType, RID, KeyComparator> *next_leaf_ptr = nullptr;

        if (auto it = cached_ptr.find(next_page_id); it != cached_ptr.end()) {
          next_ptr = it->second;
        } else {
          next_ptr = buffer_pool_manager_->FetchPage(next_page_id);
          next_leaf_ptr = reinterpret_cast<BPlusTreeLeafPage<KeyType, RID, KeyComparator> *>(next_ptr);
          cached_ptr.insert({next_page_id, next_ptr});
          unpin_coll.insert({next_page_id, next_ptr});
        }

        // if (index_for_key == 0 && ptr_2_leaf->GetSize() > 0) {
        //   // !!!
        //   KeyType new_leaf_first_key = ptr_2_leaf->KeyAt(0);
        //   FirstKeyPopulateUp(new_leaf_first_key, &st_1);
        //   LOG_INFO("[%s]: new first key: %ld.", signature.c_str(), new_leaf_first_key.ToString());
        // }

        // next can "provide" the first one.
        assert(next_leaf_ptr != nullptr);
        if (next_leaf_ptr->GetSize() > next_leaf_ptr->GetMinSize()) {
          KeyType mov_key = next_leaf_ptr->KeyAt(0);
          ValueType mov_value = next_leaf_ptr->ValueAt(0);

          ptr_2_leaf->SetKeyAt(ptr_2_leaf->GetSize(), mov_key);
          ptr_2_leaf->SetValueAt(ptr_2_leaf->GetSize(), mov_value);
          ptr_2_leaf->IncreaseSize(1);

          next_leaf_ptr->MoveBackward(0);
          next_leaf_ptr->IncreaseSize(-1);
          KeyType new_key = next_leaf_ptr->KeyAt(0);

          PopulateUpV2(ptr_2_leaf, key, ptr_2_leaf->KeyAt(0));
          PopulateUpV2(next_leaf_ptr, mov_key, new_key);

          for (size_t i = next_unlock_idx; i < st_1.size(); i++) {
            Page *damn = reinterpret_cast<Page *>(st_1[i].first);
            if (i == 0) {
              UnlatchRootPage();
            }
            damn->WUnlatch();
          }

          if (st_1.empty()) {
            UnlatchRootPage();
          }
          next_ptr->WUnlatch();
          ptr->WUnlatch();
          UnpinPages(unpin_coll, signature);

          rem_cnt_.fetch_add(prime_quota);
          buffer_pool_page_quota_.notify_one();

          LOG_INFO("find remove path 4.");
          return;
        }
        // merged with next page (next page is merged)?
        // and set flag as FALSE. if merged with next page toggle to FALSE.
        if (ptr_2_leaf->GetSize() == 0) {
          PopulateUpV2(ptr_2_leaf, key, next_leaf_ptr->KeyAt(0));
        }
        MergedWithRightSibling(ptr_2_leaf, next_leaf_ptr);
        bool flag = false;

        for (int i = st_1.size() - 1; i >= 0; i--) {
          KeyType old_first_key = st_1[i].first->KeyAt(0);
          if (flag) {
            st_1[i].first->MoveBackward(st_1[i].second);
          } else {
            st_1[i].first->MoveBackward(st_1[i].second + 1);
          }
          st_1[i].first->IncreaseSize(-1);
          KeyType new_first_key = st_1[i].first->KeyAt(0);
          LOG_INFO("[%s]: old/new first key: %ld, %ld.", signature.c_str(), old_first_key.ToString(),
                   new_first_key.ToString());
          PopulateUpV2(st_1[i].first, old_first_key, new_first_key);

          if (st_1[i].first->GetSize() >= st_1[i].first->GetMinSize()) {
            break;
          }
          if (i > 0) {
            // internal borrow/merge with left sibling.
            if (st_1[i - 1].second > 0) {
              // the other one is  st_1[i].first.
              page_id_t prev_page_id = st_1[i - 1].first->ValueAt(st_1[i - 1].second - 1);
              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *prev_ptr = nullptr;

              if (auto it = cached_ptr.find(prev_page_id); it != cached_ptr.end()) {
                prev_ptr = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(it->second);
              } else {
                prev_ptr = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(
                    buffer_pool_manager_->FetchPage(prev_page_id));
                cached_ptr.insert({prev_page_id, reinterpret_cast<Page *>(prev_ptr)});
                unpin_coll.insert({prev_page_id, reinterpret_cast<Page *>(prev_ptr)});
              }

              if (prev_ptr->GetSize() > prev_ptr->GetMinSize()) {
                BorrowFromLeftSibling(prev_ptr, st_1[i].first);
                break;
              }
              MergedToLeftSibling(prev_ptr, st_1[i].first);
              flag = true;
            } else {
              page_id_t next_page_id = st_1[i - 1].first->ValueAt(st_1[i - 1].second + 1);
              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *next_ptr = nullptr;

              if (auto it = cached_ptr.find(next_page_id); it != cached_ptr.end()) {
                next_ptr = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(it->second);
              } else {
                next_ptr = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(
                    buffer_pool_manager_->FetchPage(next_page_id));
                cached_ptr.insert({next_page_id, reinterpret_cast<Page *>(next_ptr)});
                unpin_coll.insert({next_page_id, reinterpret_cast<Page *>(next_ptr)});
              }

              if (next_ptr->GetSize() > next_ptr->GetMinSize()) {
                BorrowFromRightSibling(st_1[i].first, next_ptr);
                break;
              }
              MergedWithRightSibling(st_1[i].first, next_ptr);
              // LogInternalPage(st_1[i].first);
              flag = false;
            }
          }
        }

        TreeHeightTrim(&stale_root_coll);
        for (const auto &ptr : stale_root_coll) {
          while (ptr->GetPinCount() > 0) {
            buffer_pool_manager_->UnpinPage(ptr->GetPageId(), false);
          }
          unpin_coll.erase(ptr->GetPageId());

          LOG_INFO("page#%d is deleted.", ptr->GetPageId());
          ptr->WUnlatch();
          buffer_pool_manager_->DeletePage(ptr->GetPageId());
        }

        for (size_t i = next_unlock_idx; i < st_1.size(); i++) {
          Page *damn = reinterpret_cast<Page *>(st_1[i].first);
          if (i == 0) {
            UnlatchRootPage();
          }
          damn->WUnlatch();
        }
        if (st_1.empty()) {
          UnlatchRootPage();
        }

        next_ptr->WUnlatch();
        ptr->WUnlatch();

        UnpinPages(unpin_coll, signature);
        rem_cnt_.fetch_add(prime_quota);
        buffer_pool_page_quota_.notify_one();

        LOG_INFO("find remove path 5.");
        return;
      }
      // !!!!
      // when inner page has rank = 2, 3?
      // has no sibling.
      if (st_1.back().first->GetSize() == 1) {
        // next exist (but is not a sibling).
        if (ptr_2_leaf->GetNextPageId() != INVALID_PAGE_ID) {
          page_id_t next_page_id = ptr_2_leaf->GetNextPageId();
          Page *next_ptr = nullptr;
          BPlusTreeLeafPage<KeyType, RID, KeyComparator> *next_leaf_ptr = nullptr;

          if (auto it = cached_ptr.find(next_page_id); it != cached_ptr.end()) {
            next_ptr = it->second;
          } else {
            next_ptr = buffer_pool_manager_->FetchPage(next_page_id);
            next_ptr->WLatch();
            next_leaf_ptr = reinterpret_cast<BPlusTreeLeafPage<KeyType, RID, KeyComparator> *>(next_ptr);
            cached_ptr.insert({next_page_id, next_ptr});
            unpin_coll.insert({next_page_id, next_ptr});
          }

          assert(next_leaf_ptr != nullptr);
          if (next_leaf_ptr->GetSize() > next_leaf_ptr->GetMinSize()) {
            KeyType mov_key = next_leaf_ptr->KeyAt(0);
            ValueType mov_value = next_leaf_ptr->ValueAt(0);

            ptr_2_leaf->SetKeyAt(ptr_2_leaf->GetSize(), mov_key);
            ptr_2_leaf->SetValueAt(ptr_2_leaf->GetSize(), mov_value);
            ptr_2_leaf->IncreaseSize(1);

            next_leaf_ptr->MoveBackward(0);
            next_leaf_ptr->IncreaseSize(-1);
            KeyType new_key = next_leaf_ptr->KeyAt(0);

            PopulateUpV2(ptr_2_leaf, key, ptr_2_leaf->KeyAt(0));
            PopulateUpV2(next_leaf_ptr, mov_key, new_key);

            for (size_t i = next_unlock_idx; i < st_1.size(); i++) {
              Page *damn = reinterpret_cast<Page *>(st_1[i].first);
              if (i == 0) {
                UnlatchRootPage();
              }
              damn->WUnlatch();
            }

            if (st_1.empty()) {
              UnlatchRootPage();
            }
            next_ptr->WUnlatch();
            ptr->WUnlatch();
            UnpinPages(unpin_coll, signature);

            rem_cnt_.fetch_add(prime_quota);
            buffer_pool_page_quota_.notify_one();

            LOG_INFO("find remove path 4-1.");
            return;
          }

          page_id_t next_pr_pid = next_leaf_ptr->GetParentPageId();
          MergedWithRightSibling(ptr_2_leaf, next_leaf_ptr);
          PopulateUpV2(ptr_2_leaf, key, ptr_2_leaf->KeyAt(0));

          Page *next_pr_ptr = buffer_pool_manager_->FetchPage(next_pr_pid);
          BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *next_pr_int_ptr = nullptr;
          next_pr_int_ptr = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(next_pr_ptr);

          next_pr_int_ptr->MoveBackward(0);
          next_pr_int_ptr->IncreaseSize(-1);

          for (int i = st_1.size() - 1; i >= 0; i--) {
            if (next_pr_int_ptr->GetPageId() == st_1[i].first->GetPageId()) {
              break;
            }
            if (next_pr_int_ptr->GetSize() < next_pr_int_ptr->GetMinSize()) {
              next_pr_pid = next_pr_int_ptr->GetParentPageId();
              MergedWithRightSibling(st_1[i].first, next_pr_int_ptr);

              if (next_pr_pid == INVALID_PAGE_ID) {
                break;
              }

              next_pr_int_ptr = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(
                  buffer_pool_manager_->FetchPage(next_pr_pid));
              next_pr_int_ptr->MoveBackward(1);
              next_pr_int_ptr->IncreaseSize(-1);
            } else {
              break;
            }
          }

          TreeHeightTrim(&stale_root_coll);
          for (const auto &ptr : stale_root_coll) {
            while (ptr->GetPinCount() > 0) {
              buffer_pool_manager_->UnpinPage(ptr->GetPageId(), false);
            }
            unpin_coll.erase(ptr->GetPageId());

            LOG_INFO("page#%d is deleted.", ptr->GetPageId());
            ptr->WUnlatch();
            buffer_pool_manager_->DeletePage(ptr->GetPageId());
          }

          for (size_t i = next_unlock_idx; i < st_1.size(); i++) {
            Page *damn = reinterpret_cast<Page *>(st_1[i].first);
            if (i == 0) {
              UnlatchRootPage();
            }
            damn->WUnlatch();
          }

          if (st_1.empty()) {
            UnlatchRootPage();
          }
          next_ptr->WUnlatch();
          ptr->WUnlatch();
          UnpinPages(unpin_coll, signature);

          rem_cnt_.fetch_add(prime_quota);
          buffer_pool_page_quota_.notify_one();

          return;
        }
        // prev exist.
        int check_point_int = -1;
        for (int i = st_1.size() - 1; i >= 0; i--) {
          if (st_1[i].second > 0) {
            check_point_int = i;
            break;
          }
        }

        // st_1[check_point].first is the parent ptr (st_1[check_point].first is the most recent shared parent).
        // A <=> st_1[check_point].second - 1 is the idx.
        // KeyAt(A).
        assert(check_point_int != -1);
        size_t check_point = check_point_int;

        auto st_0 = st_1;
        st_0[check_point].second = st_1[check_point].second - 1;
        for (size_t i = check_point + 1; i < st_0.size(); i++) {
          if (i > check_point + 1) {
            st_0[i - 1].second = st_0[i - 1].first->GetSize() - 1;
          }
          page_id_t next_page_id = st_0[i - 1].first->ValueAt(st_0[i - 1].second);

          if (auto it = cached_ptr.find(next_page_id); it != cached_ptr.end()) {
            ptr = it->second;
          } else {
            ptr = buffer_pool_manager_->FetchPage(next_page_id);
            cached_ptr.insert({next_page_id, ptr});
            unpin_coll.insert({next_page_id, ptr});
          }

          ptr->WLatch();
          st_0[i].first = reinterpret_cast<BPlusTreeInternalPage<KeyType, bustub::page_id_t, KeyComparator> *>(ptr);
        }
        st_0[st_0.size() - 1].second = st_0[st_0.size() - 1].first->GetSize() - 1;

        page_id_t prev_page_id = st_0.back().first->ValueAt(st_0.back().second);
        Page *prev_ptr = nullptr;
        BPlusTreeLeafPage<KeyType, RID, KeyComparator> *prev_leaf_ptr = nullptr;

        if (auto it = cached_ptr.find(prev_page_id); it != cached_ptr.end()) {
          prev_ptr = it->second;
        } else {
          prev_ptr = buffer_pool_manager_->FetchPage(prev_page_id);
          prev_ptr->WLatch();
          prev_leaf_ptr = reinterpret_cast<BPlusTreeLeafPage<KeyType, RID, KeyComparator> *>(prev_ptr);
          cached_ptr.insert({prev_page_id, prev_ptr});
          unpin_coll.insert({prev_page_id, prev_ptr});
        }

        assert(prev_leaf_ptr != nullptr);
        if (prev_leaf_ptr->GetSize() > prev_leaf_ptr->GetMinSize()) {
          int last_idx = prev_leaf_ptr->GetSize() - 1;
          KeyType mov_key = prev_leaf_ptr->KeyAt(last_idx);
          ValueType mov_value = prev_leaf_ptr->ValueAt(last_idx);

          ptr_2_leaf->MoveForward(0);
          ptr_2_leaf->SetKeyAt(0, mov_key);
          ptr_2_leaf->SetValueAt(0, mov_value);
          ptr_2_leaf->IncreaseSize(1);

          prev_leaf_ptr->IncreaseSize(-1);

          PopulateUpV2(ptr_2_leaf, key, ptr_2_leaf->KeyAt(0));

          for (size_t i = next_unlock_idx; i < st_1.size(); i++) {
            Page *damn0 = reinterpret_cast<Page *>(st_0[i].first);
            Page *damn1 = reinterpret_cast<Page *>(st_1[i].first);
            if (i == 0) {
              UnlatchRootPage();
            }
            damn0->WUnlatch();
            if (damn0 != damn1) {
              damn1->WUnlatch();
            }
          }

          if (st_1.empty()) {
            UnlatchRootPage();
          }
          prev_ptr->WUnlatch();
          ptr->WUnlatch();
          UnpinPages(unpin_coll, signature);

          rem_cnt_.fetch_add(prime_quota);
          buffer_pool_page_quota_.notify_one();

          LOG_INFO("find remove path 4-2.");
          return;
        }

        page_id_t this_pr_pid = ptr_2_leaf->GetParentPageId();
        MergedToLeftSibling(prev_leaf_ptr, ptr_2_leaf);

        st_1.back().first->MoveBackward(0);
        st_1.back().first->IncreaseSize(-1);

        for (int i = st_1.size() - 1; i >= 0; i--) {
          if (st_0[i].first->GetPageId() == st_1[i].first->GetPageId()) {
            break;
          }
          if (st_1[i].first->GetSize() < st_1[i].first->GetMinSize()) {
            this_pr_pid = st_1[i].first->GetParentPageId();
            MergedToLeftSibling(st_0[i].first, st_1[i].first);

            if (this_pr_pid == INVALID_PAGE_ID) {
              break;
            }

            st_1[i].first->MoveBackward(1);
            st_1[i].first->IncreaseSize(-1);
          } else {
            break;
          }
        }

        TreeHeightTrim(&stale_root_coll);
        for (const auto &ptr : stale_root_coll) {
          while (ptr->GetPinCount() > 0) {
            buffer_pool_manager_->UnpinPage(ptr->GetPageId(), false);
          }
          unpin_coll.erase(ptr->GetPageId());

          LOG_INFO("page#%d is deleted.", ptr->GetPageId());
          ptr->WUnlatch();
          buffer_pool_manager_->DeletePage(ptr->GetPageId());
        }

        for (size_t i = next_unlock_idx; i < st_1.size(); i++) {
          Page *damn0 = reinterpret_cast<Page *>(st_0[i].first);
          Page *damn1 = reinterpret_cast<Page *>(st_1[i].first);
          if (i == 0) {
            UnlatchRootPage();
          }
          damn0->WUnlatch();
          if (damn0 != damn1) {
            damn1->WUnlatch();
          }
        }

        if (st_1.empty()) {
          UnlatchRootPage();
        }
        prev_ptr->WUnlatch();
        ptr->WUnlatch();
        UnpinPages(unpin_coll, signature);

        rem_cnt_.fetch_add(prime_quota);
        buffer_pool_page_quota_.notify_one();

        LOG_INFO("find remove path 4-3.");

        return;
      }
    }
  } while (true);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::BorrowFromLeftSibling(BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *prev_page,
                                           BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *this_page) {
  KeyType mov_key = prev_page->KeyAt(prev_page->GetSize() - 1);
  page_id_t mov_value = prev_page->ValueAt(prev_page->GetSize() - 1);

  KeyType old_first_key = this_page->KeyAt(0);

  this_page->MoveForward(0);
  this_page->SetKeyAt(0, mov_key);
  this_page->SetValueAt(0, mov_value);
  this_page->IncreaseSize(1);

  auto tmp = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(mov_value));
  tmp->SetParentPageId(this_page->GetPageId());

  prev_page->IncreaseSize(-1);
  KeyType new_first_key = this_page->KeyAt(0);

  PopulateUpV2(this_page, old_first_key, new_first_key);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::BorrowFromRightSibling(BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *this_page,
                                            BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *next_page) {
  KeyType mov_key = next_page->KeyAt(0);
  page_id_t mov_value = next_page->ValueAt(0);

  this_page->SetKeyAt(this_page->GetSize(), mov_key);
  this_page->SetValueAt(this_page->GetSize(), mov_value);
  this_page->IncreaseSize(1);

  auto tmp = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(mov_value));
  tmp->SetParentPageId(this_page->GetPageId());

  next_page->MoveBackward(0);
  next_page->IncreaseSize(-1);
  KeyType new_first_key = next_page->KeyAt(0);

  PopulateUpV2(next_page, mov_key, new_first_key);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PopulateUpV2(BPlusTreePage *this_page, const KeyType &old_first_key,
                                  const KeyType &new_first_key) {
  page_id_t ppid = this_page->GetParentPageId();
  auto fuck = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(
      buffer_pool_manager_->FetchPage(ppid));

  if (ppid != INVALID_PAGE_ID) {
    int left = 0;
    int right = fuck->GetSize() - 1;
    int dst = -1;
    bool ok = false;

    while (left <= right) {
      int mid = (left + right) / 2;
      int comp = comparator_(fuck->KeyAt(mid), old_first_key);
      if (comp == 0) {
        dst = mid;
        ok = true;
        break;
      }
      if (comp < 0) {
        right = mid - 1;
      } else {
        left = mid + 1;
      }
    }

    if (ok) {
      fuck->SetKeyAt(dst, new_first_key);
      ppid = fuck->GetParentPageId();
    }

    // for (int i = 0; i < fuck->GetSize(); i++) {
    //   if (comparator_(fuck->KeyAt(i), old_first_key) == 0) {
    //     fuck->SetKeyAt(i, new_first_key);
    //     ppid = fuck->GetParentPageId();
    //     break;
    //   }
    // }
  }

  while (ppid != INVALID_PAGE_ID) {
    buffer_pool_manager_->UnpinPage(fuck->GetPageId(), true);
    fuck = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(
        buffer_pool_manager_->FetchPage(ppid));
    auto ret = FindFirstInfIndex(old_first_key, fuck);

    if (!ret.second && comparator_(fuck->KeyAt(ret.first), old_first_key) == 0) {
      fuck->SetKeyAt(ret.first, new_first_key);
      // not necessarily in index [0].
      // fuck->SetKeyAt(0, new_first_key);
      ppid = fuck->GetParentPageId();
    } else {
      break;
    }
  }
  if (ppid != INVALID_PAGE_ID) {
    buffer_pool_manager_->UnpinPage(fuck->GetPageId(), true);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::MergedToLeftSibling(BPlusTreePage *prev_page, BPlusTreePage *this_page) {
  /*
    prev_page ---> this_page ···> next.
    => prev_page ---> next.
  */
  int before_size = prev_page->GetSize();
  if (prev_page->IsLeafPage()) {
    auto prev_leaf_ptr = reinterpret_cast<BPlusTreeLeafPage<KeyType, RID, KeyComparator> *>(prev_page);
    auto this_leaf_ptr = reinterpret_cast<BPlusTreeLeafPage<KeyType, RID, KeyComparator> *>(this_page);

    for (int i = 0; i < this_leaf_ptr->GetSize(); i++) {
      prev_leaf_ptr->SetKeyAt(before_size + i, this_leaf_ptr->KeyAt(i));
      prev_leaf_ptr->SetValueAt(before_size + i, this_leaf_ptr->ValueAt(i));
    }

    prev_leaf_ptr->SetNextPageId(this_leaf_ptr->GetNextPageId());
  } else {
    // merge a internal page pair?
    auto prev_int_ptr = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(prev_page);
    auto this_int_ptr = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(this_page);
    page_id_t prev_page_id = prev_page->GetPageId();
    // for internal page, we need additional work, for its children to "re-point" to the new one.
    for (int i = 0; i < this_int_ptr->GetSize(); i++) {
      prev_int_ptr->SetKeyAt(before_size + i, this_int_ptr->KeyAt(i));
      prev_int_ptr->SetValueAt(before_size + i, this_int_ptr->ValueAt(i));

      auto *tmp = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(this_int_ptr->ValueAt(i)));
      // Page *tmp1 = reinterpret_cast<Page *>(tmp);
      // tmp1->WLatch();
      tmp->SetParentPageId(prev_page_id);
      // tmp1->WUnlatch();
      buffer_pool_manager_->UnpinPage(tmp->GetPageId(), true);
    }

    page_id_t ppid = prev_page->GetParentPageId();
    auto *p_ptr = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(ppid));
    Page *p_page_ptr = reinterpret_cast<Page *>(p_ptr);
    if (GetRootPageId() == ppid && p_ptr->GetSize() == 2) {
      root_page_id_ = prev_page->GetPageId();
      prev_page->SetParentPageId(INVALID_PAGE_ID);
      UpdateRootPageId();

      while (p_page_ptr->GetPinCount() > 0) {
        buffer_pool_manager_->UnpinPage(ppid, false);
      }
      LOG_INFO("page#%d is deleted.", ppid);
      p_page_ptr->WUnlatch();

      buffer_pool_manager_->DeletePage(ppid);
    } else {
      buffer_pool_manager_->UnpinPage(ppid, false);
    }
  }

  prev_page->IncreaseSize(this_page->GetSize());
  Page *this_page_ptr = reinterpret_cast<Page *>(this_page);
  while (this_page_ptr->GetPinCount() > 0) {
    buffer_pool_manager_->UnpinPage(this_page->GetPageId(), false);
  }

  del_page_mux_.lock();
  LOG_INFO("page#%d is deleted.", this_page->GetPageId());
  this_page_ptr->WUnlatch();
  buffer_pool_manager_->DeletePage(this_page->GetPageId());
  del_page_mux_.unlock();
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::MergedWithRightSibling(BPlusTreePage *this_page, BPlusTreePage *next_page) {
  /*
  this_page ---> next_page ···> nnext.
  => this_page ---> nnext.
*/
  int before_size = this_page->GetSize();
  if (this_page->IsLeafPage()) {
    auto this_leaf_ptr = reinterpret_cast<BPlusTreeLeafPage<KeyType, RID, KeyComparator> *>(this_page);
    auto next_leaf_ptr = reinterpret_cast<BPlusTreeLeafPage<KeyType, RID, KeyComparator> *>(next_page);

    for (int i = 0; i < next_leaf_ptr->GetSize(); i++) {
      this_leaf_ptr->SetKeyAt(before_size + i, next_leaf_ptr->KeyAt(i));
      this_leaf_ptr->SetValueAt(before_size + i, next_leaf_ptr->ValueAt(i));
    }

    this_leaf_ptr->SetNextPageId(next_leaf_ptr->GetNextPageId());
  } else {
    // merge a internal page pair?
    auto this_int_ptr = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(this_page);
    auto next_int_ptr = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(next_page);
    page_id_t this_page_id = this_page->GetPageId();
    // for internal page, we need additional work, for its children to "re-point" to the new one.
    for (int i = 0; i < next_int_ptr->GetSize(); i++) {
      this_int_ptr->SetKeyAt(before_size + i, next_int_ptr->KeyAt(i));
      this_int_ptr->SetValueAt(before_size + i, next_int_ptr->ValueAt(i));

      auto *tmp = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(next_int_ptr->ValueAt(i)));
      // Page *tmp1 = reinterpret_cast<Page *>(tmp);
      // tmp1->WLatch();
      tmp->SetParentPageId(this_page_id);
      // tmp1->WUnlatch();
      buffer_pool_manager_->UnpinPage(tmp->GetPageId(), true);
    }

    page_id_t ppid = this_page->GetParentPageId();
    auto *p_ptr = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(ppid));
    Page *p_page_ptr = reinterpret_cast<Page *>(p_ptr);
    if (GetRootPageId() == ppid && p_ptr->GetSize() == 2) {
      root_page_id_ = this_page->GetPageId();
      this_page->SetParentPageId(INVALID_PAGE_ID);
      UpdateRootPageId();

      while (p_page_ptr->GetPinCount() > 0) {
        buffer_pool_manager_->UnpinPage(ppid, false);
      }
      LOG_INFO("page#%d is deleted.", ppid);
      p_page_ptr->WUnlatch();

      buffer_pool_manager_->DeletePage(ppid);
    } else {
      buffer_pool_manager_->UnpinPage(ppid, false);
    }
  }

  this_page->IncreaseSize(next_page->GetSize());
  Page *next_page_ptr = reinterpret_cast<Page *>(next_page);
  while (next_page_ptr->GetPinCount() > 0) {
    buffer_pool_manager_->UnpinPage(next_page->GetPageId(), false);
  }

  del_page_mux_.lock();
  LOG_INFO("page#%d is deleted.", next_page->GetPageId());
  next_page_ptr->WUnlatch();
  buffer_pool_manager_->DeletePage(next_page->GetPageId());
  del_page_mux_.unlock();
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::FirstKeyPopulateUp(
    const KeyType &new_key,
    std::vector<std::pair<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *, int>> *st_1) {
  size_t populate_index = 0;
  for (int i = st_1->size() - 1; i >= 0 && populate_index == 0; i--) {
    (*st_1)[i].first->SetKeyAt((*st_1)[i].second, new_key);
    populate_index = (*st_1)[i].second;
  }
}
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InnerPageMerge(
    const std::vector<std::pair<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *, int>> &st) {
  for (int i = st.size() - 1; i >= 0; i--) {
    if (st[i].first->GetSize() > st[i].first->GetMinSize()) {
      st[i].first->MoveBackward(st[i].second);
      st[i].first->IncreaseSize(-1);
      return;
    }
    // st[i] has its right sibling?
    if (i > 0 && st[i - 1].first->GetSize() > st[i - 1].second + 1) {
      page_id_t sibling_page_id = st[i - 1].first->ValueAt(st[i - 1].second + 1);
      if (sibling_page_id != INVALID_PAGE_ID) {
        auto sibling_page_ptr = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(
            buffer_pool_manager_->FetchPage(sibling_page_id));
        if (sibling_page_ptr->GetSize() > sibling_page_ptr->GetMinSize()) {
          st[i].first->IncreaseSize(-1);
          st[i].first->SetKeyAt(st[i].first->GetSize(), sibling_page_ptr->KeyAt(0));
          st[i].first->SetValueAt(st[i].first->GetSize(), sibling_page_ptr->ValueAt(0));
          st[i].first->IncreaseSize(1);
        }
      }
    }
  }
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
  auto signatures = GenerateNRandomString(1);
  std::string signature = signatures[0];
  Page *ptr = nullptr;
  std::atomic<size_t> prime_quota = 2;

  do {
    std::unique_lock<std::mutex> lock(mux_);
    while (rem_cnt_.load() < prime_quota) {
      buffer_pool_page_quota_.wait(lock);
    }

    rem_cnt_.fetch_sub(prime_quota);
    break;
  } while (true);

  ptr = buffer_pool_manager_->FetchPage(root_page_id_);
  ptr->RLatch();
  LOG_INFO("[%s]: page#%d acquire a R-latch. pin cnt: %d.", signature.c_str(), ptr->GetPageId(), ptr->GetPinCount());

  auto tmp = reinterpret_cast<BPlusTreePage *>(ptr);
  while (!tmp->IsLeafPage()) {
    auto fuck = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(tmp);
    tmp = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(fuck->ValueAt(0)));
    LOG_INFO("[%s]: page#%d acquire a R-latch. pin cnt: %d.", signature.c_str(), tmp->GetPageId(),
             reinterpret_cast<Page *>(tmp)->GetPinCount());
    reinterpret_cast<Page *>(tmp)->RLatch();
    LOG_INFO("[%s]: page#%d release a R-latch. pin cnt: %d.", signature.c_str(), fuck->GetPageId(),
             reinterpret_cast<Page *>(fuck)->GetPinCount());
    reinterpret_cast<Page *>(fuck)->RUnlatch();

    buffer_pool_manager_->UnpinPage(fuck->GetPageId(), false);
  }

  auto fuck = reinterpret_cast<BPlusTreeLeafPage<KeyType, RID, KeyComparator> *>(tmp);
  LOG_INFO("[%s]: page#%d release a R-latch. pin cnt: %d.", signature.c_str(), tmp->GetPageId(),
           reinterpret_cast<Page *>(tmp)->GetPinCount());
  reinterpret_cast<Page *>(tmp)->RUnlatch();
  buffer_pool_manager_->UnpinPage(tmp->GetPageId(), false);
  // rem_cnt_.fetch_add(prime_quota);
  // BPlusTreeLeafPage<KeyType, RID, KeyComparator> *
  return INDEXITERATOR_TYPE(buffer_pool_manager_, fuck, 0, prime_quota.load(), &rem_cnt_);
  // return INDEXITERATOR_TYPE();
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  Page *ptr = nullptr;
  std::atomic<size_t> prime_quota = 2;

  do {
    std::unique_lock<std::mutex> lock(mux_);
    while (rem_cnt_.load() < prime_quota) {
      buffer_pool_page_quota_.wait(lock);
    }

    rem_cnt_.fetch_sub(prime_quota);
    break;
  } while (true);

  ptr = buffer_pool_manager_->FetchPage(root_page_id_);
  ptr->RLatch();

  auto tmp = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_));
  while (!tmp->IsLeafPage()) {
    auto fuck = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(tmp);
    auto ret = FindFirstInfIndex(key, fuck);
    tmp = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(fuck->ValueAt(ret.first)));
    reinterpret_cast<Page *>(tmp)->RLatch();
    reinterpret_cast<Page *>(fuck)->RUnlatch();

    buffer_pool_manager_->UnpinPage(fuck->GetPageId(), false);
  }

  auto fuck = reinterpret_cast<BPlusTreeLeafPage<KeyType, RID, KeyComparator> *>(tmp);
  int index = BinarySearch(key, fuck);
  if (comparator_(key, fuck->KeyAt(index)) == 0) {
    LOG_INFO("case 2 now first page. key: %ld. index: %d", key.ToString(), index);
  } else {
    LOG_INFO("WTF??? key: %ld", key.ToString());
  }

  reinterpret_cast<Page *>(tmp)->RUnlatch();
  buffer_pool_manager_->UnpinPage(tmp->GetPageId(), false);
  // rem_cnt_.fetch_add(prime_quota);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, fuck, index, prime_quota.load(), &rem_cnt_);
  // return INDEXITERATOR_TYPE();
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  LOG_INFO("WTF33333");
  return INDEXITERATOR_TYPE();
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
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  // LOG_INFO("page #%d", HEADER_PAGE_ID);
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindFirstInfIndex(const KeyType &key,
                                       BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *page_ptr)
    -> std::pair<int, bool> {
  if (comparator_(key, page_ptr->KeyAt(1)) < 0) {
    return {0, true};
  }

  // int index = page_ptr->GetSize();
  // for (int i = 1; i < page_ptr->GetSize(); i++) {
  //   int comp = comparator_(key, page_ptr->KeyAt(i));
  //   if (comp == 0) {
  //     return {i, false};
  //   }
  //   if (comp < 0) {
  //     return {i - 1, true};
  //   }
  // }
  // return {index - 1, true};

  int left = 1;
  int right = page_ptr->GetSize() - 1;
  while (left <= right) {
    int mid = (left + right) / 2;
    int comp = comparator_(key, page_ptr->KeyAt(mid));

    if (comp == 0) {
      return {mid, false};
    }
    if (comp < 0) {
      right = mid - 1;
    } else {
      left = mid + 1;
    }
  }

  return {left - 1, true};
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindFirstInfIndex(const KeyType &key, BPlusTreeLeafPage<KeyType, RID, KeyComparator> *page_ptr)
    -> std::pair<int, bool> {
  if (page_ptr->GetSize() == 0) {
    return {0, true};
  }

  // LogLeafPage(page_ptr);

  // int index = page_ptr->GetSize();
  // for (int i = 0; i < page_ptr->GetSize(); i++) {
  //   int comp = comparator_(key, page_ptr->KeyAt(i));
  //   if (comp == 0) {
  //     return {i, false};
  //   }
  //   if (comp < 0) {
  //     return {i, true};
  //   }
  // }
  // return {index, true};

  int left = 0;
  int right = page_ptr->GetSize() - 1;

  while (left <= right) {
    int mid = (left + right) / 2;
    int comp = comparator_(key, page_ptr->KeyAt(mid));

    if (comp == 0) {
      return {mid, false};
    }
    if (comp < 0) {
      right = mid - 1;
    } else {
      left = mid + 1;
    }
  }

  return {left, true};
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInternalCanSplit(
    const std::vector<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *> &st, const KeyType &key,
    const page_id_t &value, std::unordered_map<page_id_t, bool> *unpin_is_dirty,
    std::unordered_map<page_id_t, size_t> *fuck, std::unordered_map<page_id_t, Page *> *cached_ptr,
    std::unordered_map<page_id_t, Page *> *unpin_coll, const std::string &signature) {
  KeyType tmp_key = key;
  page_id_t tmp_value = value;

  for (int i = st.size() - 1; i >= 0; i--) {
    auto ret = FindFirstInfIndex(tmp_key, st[i]);
    unpin_is_dirty->insert_or_assign(st[i]->GetPageId(), true);
    if (st[i]->GetSize() < st[i]->GetMaxSize()) {
      // LogInternalPage(st[i]);
      st[i]->MoveForward(ret.first + 1);
      st[i]->SetKeyAt(ret.first + 1, tmp_key);
      st[i]->SetValueAt(ret.first + 1, tmp_value);
      st[i]->IncreaseSize(1);

      // LogInternalPage(st[i]);
      break;
    }
    int total = st[i]->GetMaxSize() + 1;
    int rem_to_mov = total - (total / 2);
    int save = rem_to_mov;
    int cur = st[i]->GetSize() - 1;
    int cnt = 0;
    bool flag = false;

    page_id_t neww_page;
    Page *neww_ptr = buffer_pool_manager_->NewPage(&neww_page);
    cached_ptr->insert({neww_page, neww_ptr});
    unpin_coll->insert({neww_page, neww_ptr});
    auto *neww_page_ptr = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(neww_ptr);
    // (*fuck)[neww_page]++;
    unpin_is_dirty->insert_or_assign(neww_page, true);

    neww_page_ptr->Init(neww_page, st[i]->GetParentPageId(), internal_max_size_);
    // LOG_INFO("[%s]: fyck ret: [%d, %d]", signature.c_str(), ret.first, ret.second);
    // if tmp_key is strictly larger than anyone in the page. move it first.
    if (ret.first == cur && ret.second) {
      neww_page_ptr->SetKeyAt(rem_to_mov - 1, tmp_key);
      neww_page_ptr->SetValueAt(rem_to_mov - 1, tmp_value);
      // LOG_INFO("[%s]: fuckkk: move key: %ld", signature.c_str(), tmp_key.ToString());

      if (auto it = cached_ptr->find(tmp_value); it != cached_ptr->end()) {
        reinterpret_cast<BPlusTreePage *>(it->second)->SetParentPageId(neww_page);
      } else {
        Page *tmp_page = buffer_pool_manager_->FetchPage(tmp_value);
        cached_ptr->insert({tmp_value, tmp_page});
        unpin_coll->insert({tmp_value, tmp_page});
        reinterpret_cast<BPlusTreePage *>(tmp_page)->SetParentPageId(neww_page);
      }

      // (*fuck)[tmp_value]++;
      unpin_is_dirty->insert_or_assign(tmp_value, true);

      rem_to_mov--;
      flag = true;
    }

    while (rem_to_mov > 0) {
      // first peek tmp_key.
      if (!flag && ret.first == cur) {
        neww_page_ptr->SetKeyAt(rem_to_mov - 1, tmp_key);
        neww_page_ptr->SetValueAt(rem_to_mov - 1, tmp_value);
        // LOG_INFO("[%s]: fuckkk: move key: %ld", signature.c_str(), tmp_key.ToString());

        if (auto it = cached_ptr->find(tmp_value); it != cached_ptr->end()) {
          reinterpret_cast<BPlusTreePage *>(it->second)->SetParentPageId(neww_page);
        } else {
          Page *tmp_page = buffer_pool_manager_->FetchPage(tmp_value);
          cached_ptr->insert({tmp_value, tmp_page});
          unpin_coll->insert({tmp_value, tmp_page});
          reinterpret_cast<BPlusTreePage *>(tmp_page)->SetParentPageId(neww_page);
        }

        // (*fuck)[tmp_value]++;
        unpin_is_dirty->insert_or_assign(tmp_value, true);

        rem_to_mov--;
        flag = true;
      }
      if (rem_to_mov == 0) {
        break;
      }

      cnt++;
      neww_page_ptr->SetKeyAt(rem_to_mov - 1, st[i]->KeyAt(cur));
      neww_page_ptr->SetValueAt(rem_to_mov - 1, st[i]->ValueAt(cur));
      // LOG_INFO("[%s]: fuckkk: move key: %ld", signature.c_str(), st[i]->KeyAt(cur).ToString());

      BPlusTreePage *tmp = nullptr;
      bool is_fetched = false;
      if (auto it = cached_ptr->find(st[i]->ValueAt(cur)); it != cached_ptr->end()) {
        tmp = reinterpret_cast<BPlusTreePage *>(it->second);
      } else {
        tmp = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(st[i]->ValueAt(cur)));
        // ---- !!!!
        // cached_ptr->insert({st[i]->ValueAt(cur), reinterpret_cast<Page *>(tmp)});
        is_fetched = true;
      }

      // (*fuck)[st[i]->ValueAt(cur)]++;
      if (tmp != nullptr) {
        tmp->SetParentPageId(neww_page);
        // internal node split can be very frame consuming!
        if (is_fetched) {
          // LOG_INFO("[%s]: page #%d", signature.c_str(), tmp->GetPageId());
          buffer_pool_manager_->UnpinPage(tmp->GetPageId(), true);
        }
      }
      unpin_is_dirty->insert_or_assign(st[i]->ValueAt(cur), true);

      rem_to_mov--;
      cur--;
    }
    st[i]->IncreaseSize(-cnt);
    neww_page_ptr->IncreaseSize(save);
    if (!flag) {
      // LOG_INFO("[%s]: fuckkknimabi: move key: %ld", signature.c_str(), key.ToString());
      st[i]->MoveForward(ret.first + 1);
      st[i]->SetKeyAt(ret.first + 1, key);
      st[i]->SetValueAt(ret.first + 1, value);
      st[i]->IncreaseSize(1);
    }
    // LOG_INFO("[%s]: !!!!!!!!!!!!!!!!!!", signature.c_str());
    // LogInternalPage(st[i]);
    // LogInternalPage(neww_page_ptr);
    // LOG_INFO("[%s]: !!!!!!!!!!!!!!!!!!", signature.c_str());

    tmp_key = neww_page_ptr->KeyAt(0);
    tmp_value = neww_page_ptr->GetPageId();
    if (i == 0) {
      Page *new_root_ptr = buffer_pool_manager_->NewPage(&root_page_id_);
      cached_ptr->insert({root_page_id_, new_root_ptr});
      unpin_coll->insert({root_page_id_, new_root_ptr});
      UpdateRootPageId();
      // (*fuck)[root_page_id_]++;
      auto *new_root_page_ptr =
          reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(new_root_ptr);
      new_root_page_ptr->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);

      st[i]->SetParentPageId(root_page_id_);
      neww_page_ptr->SetParentPageId(root_page_id_);

      new_root_page_ptr->SetKeyAt(0, st[i]->KeyAt(0));
      new_root_page_ptr->SetValueAt(0, st[i]->GetPageId());
      new_root_page_ptr->SetKeyAt(1, neww_page_ptr->KeyAt(0));
      new_root_page_ptr->SetValueAt(1, neww_page_ptr->GetPageId());
      new_root_page_ptr->SetSize(2);
      cur_height_.fetch_add(1);

      unpin_is_dirty->insert_or_assign(root_page_id_, true);
    }
  }
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

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintGraphUtil() { FinalAction final_action(this); }

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

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnlatchRootPage() {
  mux_.lock();
  root_locked_.store(static_cast<size_t>(RootLockType::UN_LOCKED));
  c_v_.notify_one();
  mux_.unlock();
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::TreeHeightTrim(std::set<Page *> *stale_root_coll) {
  Page *tmp_root = buffer_pool_manager_->FetchPage(GetRootPageId());
  auto tmp_bp_root = reinterpret_cast<BPlusTreePage *>(tmp_root);
  /*
    see this case: internal max_size = 3, leaf max_size = 2.
          _________
          |  2|4  |
          ---------
          /       \             remove [4] or [2] will yield only one leaf page as the root.
      ______      ______
      |    |      |    |        so it can be cascade and we need the `while`.
      ------      ------
        |            |
      ______      ______
      | 2  |      |  4 |
      ------      ------
  */

  while (!tmp_bp_root->IsLeafPage() && tmp_bp_root->GetSize() == 1) {
    auto old_root_ptr = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(tmp_bp_root);
    cur_height_.fetch_sub(1);
    root_page_id_ = old_root_ptr->ValueAt(0);

    auto new_root_ptr = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_));
    new_root_ptr->SetParentPageId(INVALID_PAGE_ID);
    UpdateRootPageId();
    /*
      for stale root page(s), we save their `Page*` ptrs.
    */
    while (reinterpret_cast<Page *>(tmp_bp_root)->GetPinCount() > 0) {
      buffer_pool_manager_->UnpinPage(tmp_bp_root->GetPageId(), false);
    }

    // del_page_mux_.lock();
    // reinterpret_cast<Page *>(tmp_bp_root)->WUnlatch();
    // buffer_pool_manager_->DeletePage(tmp_bp_root->GetPageId());
    // del_page_mux_.unlock();
    stale_root_coll->insert(reinterpret_cast<Page *>(tmp_bp_root));

    tmp_bp_root = new_root_ptr;
  }

  buffer_pool_manager_->UnpinPage(tmp_bp_root->GetPageId(), true);
  LOG_INFO("stale root page cnt: %ld. cur root: %d", stale_root_coll->size(), root_page_id_);
}

// Generate n random strings
auto GenerateNRandomString(int n) -> std::vector<std::string> {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<char> char_dist('A', 'z');
  std::uniform_int_distribution<int> len_dist(8, 8);

  std::vector<std::string> rand_strs(n);

  for (auto &rand_str : rand_strs) {
    int str_len = len_dist(gen);
    for (int i = 0; i < str_len; ++i) {
      rand_str.push_back(char_dist(gen));
    }
  }

  return rand_strs;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
