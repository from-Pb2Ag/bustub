#include <string>
#include <vector>

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
      internal_max_size_(internal_max_size) {
  op_id_ = 0;
  LOG_INFO("create a new B+ tree. leaf_max_size: %d, internal_max_size: %d. pool size: %ld.", leaf_max_size_,
           internal_max_size_, buffer_pool_manager_->GetPoolSize());
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
  Page *ptr = buffer_pool_manager_->FetchPage(root_page_id_);
  Page *tmp = nullptr;
  // op_id_++;
  // LOG_INFO("op: %d. attempts get value of key %ld", op_id_, key.ToString());

  do {
    // BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *ptr_2_internal =
    //     reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(ptr);
    // BPlusTreeLeafPage<KeyType, RID, KeyComparator> *ptr_2_leaf =
    //     reinterpret_cast<BPlusTreeLeafPage<KeyType, RID, KeyComparator> *>(ptr);
    auto *tmp_ptr = reinterpret_cast<BPlusTreePage *>(ptr);

    if (!tmp_ptr->IsLeafPage()) {
      auto ptr_2_internal = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(ptr);
      auto ret = FindFirstInfIndex(key, ptr_2_internal);
      // if (op_id_ == 8971) {
      //   LOG_INFO("key: %ld. ret: [%d, %d]", key.ToString(), ret.first, ret.second);
      //   LogInternalPage(ptr_2_internal);
      // }
      tmp = ptr;
      ptr = buffer_pool_manager_->FetchPage(ptr_2_internal->ValueAt(ret.first));
      buffer_pool_manager_->UnpinPage(tmp->GetPageId(), false);
      // LOG_INFO("next page id %d, ptr %p", ptr_2_internal->ValueAt(ret.first), ptr);
      if (ptr_2_internal->ValueAt(ret.first) == INVALID_PAGE_ID) {
        return false;
      }
      if (ptr_2_internal->ValueAt(ret.first) == 0) {
        return false;
      }
    } else {
      // only get value in leaf page.
      auto ptr_2_leaf = reinterpret_cast<BPlusTreeLeafPage<KeyType, RID, KeyComparator> *>(ptr);
      for (int i = 0; i < ptr_2_leaf->GetSize(); i++) {
        KeyType this_key = ptr_2_leaf->KeyAt(i);
        ValueType this_value = ptr_2_leaf->ValueAt(i);
        int comp = comparator_(key, this_key);
        buffer_pool_manager_->UnpinPage(ptr_2_leaf->GetPageId(), false);

        if (comp == 0) {
          result->push_back(this_value);
          return true;
        }
        if (comp < 0) {
          return false;
        }
      }
      return false;
    }
  } while (true);

  return false;
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
  // LOG_INFO("attempts insert kv: %ld. op_id: %d", key.ToString(), op_id_);
  op_id_++;

  // if ((op_id_ >= 400) && (op_id_ <= 600)) {
  //   KeyType index_key;
  //   index_key.SetFromInteger(2933);
  //   std::vector<ValueType> tmp;
  //   bool flag = GetValue(index_key, &tmp, transaction);
  //   LOG_INFO("op id: %d, attempts insert k: %ld. can find key 3453? %d", op_id_, key.ToString(), flag);
  // }
  // FinalAction final_action(this);
  // final_action.Deactivate();

  if (IsEmpty()) {
    // insert a empty tree.
    // buffer_pool_manager_->NewPage(&root_page_id_);
    auto *ptr_2_leaf = reinterpret_cast<BPlusTreeLeafPage<KeyType, RID, KeyComparator> *>(
        buffer_pool_manager_->NewPage(&root_page_id_));

    LOG_INFO("update root page id: %d", root_page_id_);
    ptr_2_leaf->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
    ptr_2_leaf->SetKeyAt(0, key);

    ptr_2_leaf->SetValueAt(0, value);
    ptr_2_leaf->IncreaseSize(1);

    ptr_2_leaf->SetNextPageId(INVALID_PAGE_ID);
    // ptr_2_leaf->SetPrevPageId(INVALID_PAGE_ID);

    // LogLeafPage(ptr_2_leaf);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    return true;
  }

  std::unordered_map<page_id_t, bool> unpin_is_dirty;
  std::unordered_map<page_id_t, size_t> fuck;
  Page *ptr = buffer_pool_manager_->FetchPage(root_page_id_);
  fuck[root_page_id_]++;
  if (unpin_is_dirty.find(root_page_id_) == unpin_is_dirty.end()) {
    unpin_is_dirty.insert({root_page_id_, false});
    // LOG_INFO("pin page #%d", root_page_id_);
  }

  std::vector<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *> st;

  BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *ptr_2_internal = nullptr;
  BPlusTreeLeafPage<KeyType, RID, KeyComparator> *ptr_2_leaf = nullptr;
  BPlusTreePage *cur_bpluspage_ptr = nullptr;
  if (key.ToString() == 3322) {
    LOG_INFO("phase 1");
  }

  do {
    cur_bpluspage_ptr = reinterpret_cast<BPlusTreePage *>(ptr);
    if (!cur_bpluspage_ptr->IsLeafPage()) {
      if (key.ToString() == 3322) {
        LOG_INFO("phase 2");
      }
      ptr_2_internal = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(cur_bpluspage_ptr);
      st.push_back(ptr_2_internal);
      auto ret = FindFirstInfIndex(key, ptr_2_internal);
      // LOG_INFO("ret: %d, %d", ret.first, ret.second);
      // already existing?
      if (!ret.second) {
        UnpinPages(unpin_is_dirty, fuck);
        if (key.ToString() == 3322) {
          LOG_INFO("phase 3");
        }
        return false;
      }

      ptr = buffer_pool_manager_->FetchPage(ptr_2_internal->ValueAt(ret.first));
      fuck[ptr_2_internal->ValueAt(ret.first)]++;
      if (unpin_is_dirty.find(ptr->GetPageId()) == unpin_is_dirty.end()) {
        unpin_is_dirty.insert({ptr->GetPageId(), false});
        // LOG_INFO("pin page #%d", ptr->GetPageId());
      }
      if (ptr->GetPageId() == 0) {
        if (key.ToString() == 3322) {
          LOG_INFO("phase 4");
        }
        break;
      }
      continue;
    }

    // LOG_INFO("++++stack trace begin++++");
    // for (const auto &a : st) {
    //   LOG_INFO("page id: %d", a->GetPageId());
    //   for (int i = 0; i < a->GetSize(); i++) {
    //     LOG_INFO("fuck: nested id: %ld, %d", a->KeyAt(i).ToString(), a->ValueAt(i));
    //   }
    // }
    // LOG_INFO("++++stack trace end++++++");
    ptr_2_leaf = reinterpret_cast<BPlusTreeLeafPage<KeyType, RID, KeyComparator> *>(cur_bpluspage_ptr);
    auto ret = FindFirstInfIndex(key, ptr_2_leaf);
    // 草泥马的2933
    if (key.ToString() == 3322) {
      LOG_INFO("attempts insert key %ld. ret: [%d, %d]. leaf page id: %d. size:[%d, %d]", key.ToString(), ret.first,
               ret.second, ptr_2_leaf->GetPageId(), ptr_2_leaf->GetSize(), ptr_2_leaf->GetMaxSize());

      // LogLeafPage(ptr_2_leaf);
    }
    if (!ret.second && ret.first < ptr_2_leaf->GetSize() + 1) {
      UnpinPages(unpin_is_dirty, fuck);
      if (key.ToString() == 3322) {
        LOG_INFO("phase 5");
      }
      return false;
    }
    // can insert in safe.
    unpin_is_dirty.insert_or_assign(ptr_2_leaf->GetPageId(), true);
    // LOG_INFO("pin page #%d", ptr_2_leaf->GetPageId());
    // LOG_INFO("page id:%d, size: %d, %d", ptr_2_leaf->GetPageId(), ptr_2_leaf->GetSize(), ptr_2_leaf->GetMaxSize());
    if (ptr_2_leaf->GetSize() < ptr_2_leaf->GetMaxSize()) {
      ptr_2_leaf->MoveForward(ret.first);
      ptr_2_leaf->SetKeyAt(ret.first, key);
      ptr_2_leaf->SetValueAt(ret.first, value);
      ptr_2_leaf->IncreaseSize(1);

      // LogLeafPage(ptr_2_leaf);
      UnpinPages(unpin_is_dirty, fuck);
      return true;
    }
    // A = ptr_2_leaf->GetMaxSize() + 1. => [0, A / 2), [A / 2, A).
    /*
      |----------|      |-------------|
      |ptr_2_leaf| ---> |next_leaf_ptr|
      |----------|<···· |-------------|

      |----------|      |---|      |-------------|
      |ptr_2_leaf| ---> |new| ---> |next_leaf_ptr|
      |----------|<---- |---|<···· |-------------|
      `<····` are only conditional.
    */
    page_id_t next_page = ptr_2_leaf->GetNextPageId();
    page_id_t neww_page = INVALID_PAGE_ID;

    Page *neww_ptr = buffer_pool_manager_->NewPage(&neww_page);
    Page *next_ptr = buffer_pool_manager_->FetchPage(next_page);

    fuck[neww_page]++;
    fuck[next_page]++;
    // LOG_INFO("pin page #%d", neww_page);
    // LOG_INFO("pin page #%d", next_page);
    unpin_is_dirty.insert_or_assign(neww_page, true);
    if (unpin_is_dirty.find(next_page) == unpin_is_dirty.end()) {
      unpin_is_dirty.insert({next_page, false});
    }

    auto *neww_page_ptr = reinterpret_cast<BPlusTreeLeafPage<KeyType, RID, KeyComparator> *>(neww_ptr);
    auto *next_page_ptr = reinterpret_cast<BPlusTreeLeafPage<KeyType, RID, KeyComparator> *>(next_ptr);

    if (neww_page_ptr == nullptr) {
      LOG_INFO("neww_page id: %d", neww_page);
      throw std::runtime_error("Pointer neww_page_ptr is null.");
    }
    // if (next_page_ptr == nullptr) {
    //   LOG_INFO("neww_page id: %d", neww_page);
    //   throw std::runtime_error("Pointer next_page_ptr is null.");
    // }

    neww_page_ptr->Init(neww_page, ptr_2_leaf->GetParentPageId(), leaf_max_size_);
    neww_page_ptr->SetNextPageId(next_page);
    // neww_page_ptr->SetPrevPageId(ptr_2_leaf->GetPageId());
    ptr_2_leaf->SetNextPageId(neww_page);
    if (next_page_ptr != nullptr) {
      // next_page_ptr->SetPrevPageId(neww_page);
      unpin_is_dirty.insert_or_assign(next_page, true);
    }
    if (key.ToString() == 3322) {
      LOG_INFO("1new page#%d. get from ptr: %d", neww_page, neww_page_ptr->GetPageId());
    }

    // ret = FindFirstInfIndex(key, ptr_2_leaf);
    int total = ptr_2_leaf->GetMaxSize() + 1;
    int rem_to_mov = total - (total / 2);
    int save = rem_to_mov;
    int cur = ptr_2_leaf->GetSize();
    int cnt = 0;
    bool flag = false;
    // LOG_INFO("prev size: [%d =>%d] + 1", ptr_2_leaf->GetPageId(), ptr_2_leaf->GetSize());
    if (key.ToString() == 3322) {
      LOG_INFO("++++++++++++++++");
      LOG_INFO("total to remove: %d", rem_to_mov);
      LOG_INFO("2new page#%d. get from ptr: %d", neww_page, neww_page_ptr->GetPageId());
      LOG_INFO("ParentPageId: %d.", neww_page_ptr->GetParentPageId());
      LOG_INFO("PageId: %d.", neww_page_ptr->GetPageId());
      LOG_INFO("NextPageId: %d.", neww_page_ptr->GetNextPageId());
      LOG_INFO("++++++++++++++++");
    }
    if (ret.first == cur) {
      // ptr_2_leaf ----> neww_page_ptr. the additional one is also moved to neww_page_ptr.
      // rem_to_mov++;
      // save++;
      if (key.ToString() == 3322) {
        LOG_INFO("[%ld:%s] >> case1 `new_page`", key.ToString(), value.ToString().c_str());
      }
      neww_page_ptr->SetKeyAt(rem_to_mov - 1, key);
      neww_page_ptr->SetValueAt(rem_to_mov - 1, value);
      rem_to_mov--;
      flag = true;
    }
    while (rem_to_mov > 0) {
      cnt++;
      if (key.ToString() == 3322) {
        LOG_INFO("id: %d [%ld:%s] >> case2 `new_page`", ptr_2_leaf->GetPageId(), ptr_2_leaf->KeyAt(cur - 1).ToString(),
                 ptr_2_leaf->ValueAt(cur - 1).ToString().c_str());
        if (rem_to_mov < 10) {
          LOG_INFO("++++++++++++++++");
          LOG_INFO("count down: %d", rem_to_mov);
          LOG_INFO("4new page#%d. get from ptr: %d", neww_page, neww_page_ptr->GetPageId());
          LOG_INFO("ParentPageId: %d.", neww_page_ptr->GetParentPageId());
          LOG_INFO("PageId: %d.", neww_page_ptr->GetPageId());
          LOG_INFO("NextPageId: %d.", neww_page_ptr->GetNextPageId());
          LOG_INFO("++++++++++++++++");
        }
      }
      neww_page_ptr->SetKeyAt(rem_to_mov - 1, ptr_2_leaf->KeyAt(cur - 1));
      neww_page_ptr->SetValueAt(rem_to_mov - 1, ptr_2_leaf->ValueAt(cur - 1));
      rem_to_mov--;
      cur--;
      if (!flag && ret.first == cur && rem_to_mov > 0) {
        // ptr_2_leaf ----> neww_page_ptr. the additional one is also moved to neww_page_ptr.
        if (key.ToString() == 3322) {
          LOG_INFO("[%ld:%s] >> case3 `new_page`", key.ToString(), value.ToString().c_str());
        }
        // rem_to_mov++;
        // save++;
        neww_page_ptr->SetKeyAt(rem_to_mov - 1, key);
        neww_page_ptr->SetValueAt(rem_to_mov - 1, value);
        rem_to_mov--;
        flag = true;
        if (key.ToString() == 3322) {
          if (rem_to_mov < 10) {
            LOG_INFO("++++++++++++++++");
            LOG_INFO("count down: %d", rem_to_mov);
            LOG_INFO("5new page#%d. get from ptr: %d", neww_page, neww_page_ptr->GetPageId());
            LOG_INFO("ParentPageId: %d.", neww_page_ptr->GetParentPageId());
            LOG_INFO("PageId: %d.", neww_page_ptr->GetPageId());
            LOG_INFO("NextPageId: %d.", neww_page_ptr->GetNextPageId());
            LOG_INFO("++++++++++++++++");
          }
        }
      }
    }

    if (key.ToString() == 3322) {
      LOG_INFO("++++++++++++++++");
      LOG_INFO("3new page#%d. get from ptr: %d", neww_page, neww_page_ptr->GetPageId());
      LOG_INFO("ParentPageId: %d.", neww_page_ptr->GetParentPageId());
      LOG_INFO("PageId: %d.", neww_page_ptr->GetPageId());
      LOG_INFO("NextPageId: %d.", neww_page_ptr->GetNextPageId());
      LOG_INFO("++++++++++++++++");
    }
    ptr_2_leaf->IncreaseSize(-cnt);
    neww_page_ptr->IncreaseSize(save);
    if (!flag) {
      ptr_2_leaf->MoveForward(ret.first);
      ptr_2_leaf->SetKeyAt(ret.first, key);
      ptr_2_leaf->SetValueAt(ret.first, value);
      ptr_2_leaf->IncreaseSize(1);
    }

    if (key.ToString() == 3322) {
      LOG_INFO("#####");
      LOG_INFO("new page#%d. get from ptr: %d", neww_page, neww_page_ptr->GetPageId());
      LogLeafPage(ptr_2_leaf);
      LogLeafPage(neww_page_ptr);
    }
    if (ptr_2_leaf->GetParentPageId() == INVALID_PAGE_ID) {
      // LOG_INFO("!!!!!");
      buffer_pool_manager_->NewPage(&root_page_id_);
      LOG_INFO("update root page id: %d", root_page_id_);
      // LOG_INFO("?????");
      fuck[root_page_id_]++;
      // LOG_INFO("pin page #%d", root_page_id_);

      auto *new_root = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(
          buffer_pool_manager_->FetchPage(root_page_id_));
      // LOG_INFO("pin page #%d", root_page_id_);
      fuck[root_page_id_]++;
      unpin_is_dirty.insert_or_assign(root_page_id_, true);
      new_root->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);

      ptr_2_leaf->SetParentPageId(root_page_id_);
      neww_page_ptr->SetParentPageId(root_page_id_);

      new_root->SetValueAt(0, ptr_2_leaf->GetPageId());
      new_root->SetKeyAt(1, neww_page_ptr->KeyAt(0));
      new_root->SetValueAt(1, neww_page);
      new_root->SetSize(2);

      // LogInternalPage(new_root);

      UnpinPages(unpin_is_dirty, fuck);
      return true;
    }
    InsertInternalCanSplit(st, neww_page_ptr->KeyAt(0), neww_page_ptr->GetPageId(), &unpin_is_dirty, &fuck);
    UnpinPages(unpin_is_dirty, fuck);
    return true;
  } while (true);

  UnpinPages(unpin_is_dirty, fuck);
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

  // -----------------------------------------
  // | Type: R|I | cur_sz: xxx | max_sz: xxx |
  // -----------------------------------------
  // | PPid: xxx | Pid: xxx    | Nid: xxx    |
  // -----------------------------------------
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
void BPLUSTREE_TYPE::UnpinPages(const std::unordered_map<page_id_t, bool> &unpin_is_dirty,
                                const std::unordered_map<page_id_t, size_t> &fuck) {
  // LOG_INFO("how many pages to unpin? %ld, %ld", unpin_is_dirty.size(), fuck.size());
  // for (const auto &[pid, is_dirty] : unpin_is_dirty) {
  //   if (pid != INVALID_PAGE_ID) {
  //     buffer_pool_manager_->UnpinPage(pid, is_dirty);
  //     LOG_INFO("page id#%d, %d", pid, is_dirty);
  //   }
  // }
  for (const auto &[pid, cnt] : fuck) {
    if (pid != INVALID_PAGE_ID) {
      // LOG_INFO("unpin page %d", pid);
      for (size_t i = 0; i < cnt + 10; i++) {
        buffer_pool_manager_->UnpinPage(pid, true);
      }
    }
  }
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
  LOG_INFO("attempts remove k:  %ld", key.ToString());
  op_id_++;
  FinalAction final_action(this);
  final_action.Deactivate();

  if (IsEmpty()) {
    return;
  }
  Page *ptr = buffer_pool_manager_->FetchPage(root_page_id_);
  std::vector<std::pair<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *, int>> st_1;
  do {
    auto *tmp_ptr = reinterpret_cast<BPlusTreePage *>(ptr);

    // LOG_INFO("page id: %d", ptr->GetPageId());
    if (!tmp_ptr->IsLeafPage()) {
      auto ptr_2_internal = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(ptr);
      auto ret = FindFirstInfIndex(key, ptr_2_internal);
      ptr = buffer_pool_manager_->FetchPage(ptr_2_internal->ValueAt(ret.first));
      st_1.push_back({ptr_2_internal, ret.first});
    } else {
      auto ptr_2_leaf = reinterpret_cast<BPlusTreeLeafPage<KeyType, RID, KeyComparator> *>(ptr);
      // LogLeafPage(ptr_2_leaf);

      // LOG_INFO("++++stack trace begin++++");
      // for (const auto &[a, b] : st_1) {
      //   LOG_INFO("page id: %d, index: %d", a->GetPageId(), b);
      // }
      // LOG_INFO("++++stack trace end++++++");

      for (int i = 0; i < ptr_2_leaf->GetSize(); i++) {
        int comp = comparator_(key, ptr_2_leaf->KeyAt(i));
        // LOG_INFO("comp: %d, key: %ld, %ld", comp, key.ToString(), ptr_2_leaf->KeyAt(i).ToString());
        if (comp == 0) {
          // the leaf is large enough. it will not be merged.
          // the worst case is the previous first key is removed (<<), and update key in its ancestors.
          if (ptr_2_leaf->GetSize() > ptr_2_leaf->GetMinSize() || ptr_2_leaf->IsRootPage()) {
            // LOG_INFO("remove in safe, branch 1");
            ptr_2_leaf->MoveBackward(i);
            ptr_2_leaf->IncreaseSize(-1);
            KeyType new_leaf_first_key = ptr_2_leaf->KeyAt(0);

            while (!st_1.empty()) {
              KeyType tmp_key = st_1.back().first->KeyAt(st_1.back().second);
              if (comparator_(tmp_key, key) == 0) {
                st_1.back().first->SetKeyAt(st_1.back().second, new_leaf_first_key);
                st_1.pop_back();
              } else {
                break;
              }
            }
            return;
          }
          // if ptr_2_leaf achieve it's min size bound.
          if (ptr_2_leaf->GetNextPageId() != INVALID_PAGE_ID) {
            // LOG_INFO("find next page, branch 2");
            auto nxt_page_ptr = reinterpret_cast<BPlusTreeLeafPage<KeyType, RID, KeyComparator> *>(
                buffer_pool_manager_->FetchPage(ptr_2_leaf->GetNextPageId()));

            // auto left_ptr = reinterpret_cast<BPlusTreePage *>(ptr_2_leaf);
            // auto right_ptr = reinterpret_cast<BPlusTreePage *>(nxt_page_ptr);

            if (nxt_page_ptr->GetSize() > nxt_page_ptr->GetMinSize()) {
              // LOG_INFO("next page can borrow");
              // LogLeafPage(ptr_2_leaf);
              // this is important.
              ptr_2_leaf->MoveBackward(i);
              ptr_2_leaf->IncreaseSize(-1);
              ptr_2_leaf->SetKeyAt(ptr_2_leaf->GetSize(), nxt_page_ptr->KeyAt(0));
              ptr_2_leaf->SetValueAt(ptr_2_leaf->GetSize(), nxt_page_ptr->ValueAt(0));
              ptr_2_leaf->IncreaseSize(1);

              // LogLeafPage(ptr_2_leaf);

              Remove(nxt_page_ptr->KeyAt(0), transaction);
              return;
            }
            // can not borrow, then we merge.
            // A = ptr_2_leaf->GetMaxSize() + 1. => [0, A / 2), [A / 2, A).
            /*
              |----------|      |---|      |-------------|
              |ptr_2_leaf| ---> |mer| ---> |next_leaf_ptr|
              |----------|<---- |---|<···· |-------------|


              |----------|      |-------------|
              |ptr_2_leaf| ---> |next_leaf_ptr|
              |----------|<···· |-------------|
              `<····` are only conditional.
            */

            ptr_2_leaf->MoveBackward(i);
            ptr_2_leaf->IncreaseSize(-1);
            for (int i = 0; i < nxt_page_ptr->GetSize(); i++) {
              ptr_2_leaf->SetKeyAt(ptr_2_leaf->GetSize() + i, nxt_page_ptr->KeyAt(i));
              ptr_2_leaf->SetValueAt(ptr_2_leaf->GetSize() + i, nxt_page_ptr->ValueAt(i));
            }
            ptr_2_leaf->IncreaseSize(nxt_page_ptr->GetSize());
            // set two ptrs.
            ptr_2_leaf->SetNextPageId(nxt_page_ptr->GetNextPageId());
            // if (nxt_page_ptr->GetNextPageId() != INVALID_PAGE_ID) {
            //   auto nnxt_page_ptr = reinterpret_cast<BPlusTreeLeafPage<KeyType, RID, KeyComparator> *>(
            //       buffer_pool_manager_->FetchPage(ptr_2_leaf->GetNextPageId()));
            //   nnxt_page_ptr->SetPrevPageId(ptr_2_leaf->GetPageId());
            // }

            // LogLeafPage(ptr_2_leaf);
            int a = nxt_page_ptr->GetParentPageId();
            // LOG_INFO("parents for they: %d, %d", ptr_2_leaf->GetParentPageId(), a);
            if (ptr_2_leaf->GetParentPageId() != a) {
              auto tmp = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(
                  buffer_pool_manager_->FetchPage(a));
              tmp->MoveBackward(0);
              tmp->IncreaseSize(-1);
            }

            buffer_pool_manager_->DeletePage(nxt_page_ptr->GetPageId());

            st_1.back().first->MoveBackward(st_1.back().second + 1);
            st_1.back().first->IncreaseSize(-1);
            if (st_1.back().first->GetSize() >= st_1.back().first->GetMinSize()) {
              return;
            }
          }

          // if (ptr_2_leaf->GetPrevPageId() != INVALID_PAGE_ID) {
          //   // LOG_INFO("find prev page, branch 3");
          //   auto pev_page_ptr = reinterpret_cast<BPlusTreeLeafPage<KeyType, RID, KeyComparator> *>(
          //       buffer_pool_manager_->FetchPage(ptr_2_leaf->GetPrevPageId()));
          //   if (pev_page_ptr->GetSize() > pev_page_ptr->GetMinSize()) {
          //     // LOG_INFO("prev page can borrow");
          //     ptr_2_leaf->MoveBackward(i);
          //     ptr_2_leaf->IncreaseSize(-1);
          //     ptr_2_leaf->MoveForward(0);
          //     ptr_2_leaf->SetKeyAt(0, pev_page_ptr->KeyAt(pev_page_ptr->GetSize() - 1));
          //     ptr_2_leaf->SetValueAt(0, pev_page_ptr->ValueAt(pev_page_ptr->GetSize() - 1));
          //     ptr_2_leaf->IncreaseSize(1);
          //     // LOG_INFO("fuck!!!!, parent id: %d", st_1.back().first->GetPageId());
          //     // LOG_INFO("fuck!!!!, key %ld => %ld", st_1.back().first->KeyAt(st_1.back().second).ToString(),
          //     //          ptr_2_leaf->KeyAt(0).ToString());

          //     Remove(pev_page_ptr->KeyAt(pev_page_ptr->GetSize() - 1), transaction);
          //     // when borrow from prev
          //     st_1.back().first->SetKeyAt(st_1.back().second, ptr_2_leaf->KeyAt(0));
          //     return;
          //   }

          //   ptr_2_leaf->MoveBackward(i);
          //   ptr_2_leaf->IncreaseSize(-1);
          //   int prev_size = pev_page_ptr->GetSize();
          //   for (int i = 0; i < ptr_2_leaf->GetSize(); i++) {
          //     pev_page_ptr->SetKeyAt(prev_size + i, ptr_2_leaf->KeyAt(i));
          //     pev_page_ptr->SetValueAt(prev_size + i, ptr_2_leaf->ValueAt(i));
          //   }
          //   pev_page_ptr->IncreaseSize(ptr_2_leaf->GetSize());
          //   // set 2 ptrs.
          //   pev_page_ptr->SetNextPageId(ptr_2_leaf->GetNextPageId());
          //   if (ptr_2_leaf->GetNextPageId() != INVALID_PAGE_ID) {
          //     auto nxt_page_ptr = reinterpret_cast<BPlusTreeLeafPage<KeyType, RID, KeyComparator> *>(
          //         buffer_pool_manager_->FetchPage(ptr_2_leaf->GetNextPageId()));
          //     nxt_page_ptr->SetPrevPageId(pev_page_ptr->GetPageId());
          //   }
          //   st_1.back().first->MoveBackward(st_1.back().second);
          //   st_1.back().first->IncreaseSize(-1);
          //   return;
          // }
        }
      }
    }
  } while (true);
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

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindFirstInfIndex(const KeyType &key,
                                       BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *page_ptr)
    -> std::pair<int, bool> {
  if (comparator_(key, page_ptr->KeyAt(1)) < 0) {
    return {0, true};
  }

  int index = page_ptr->GetSize();
  for (int i = 1; i < page_ptr->GetSize(); i++) {
    int comp = comparator_(key, page_ptr->KeyAt(i));
    if (comp == 0) {
      return {i, false};
    }
    if (comp < 0) {
      return {i - 1, true};
    }
  }
  return {index - 1, true};
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindFirstInfIndex(const KeyType &key, BPlusTreeLeafPage<KeyType, RID, KeyComparator> *page_ptr)
    -> std::pair<int, bool> {
  if (page_ptr->GetSize() == 0) {
    return {0, true};
  }

  // LogLeafPage(page_ptr);

  int index = page_ptr->GetSize();
  for (int i = 0; i < page_ptr->GetSize(); i++) {
    int comp = comparator_(key, page_ptr->KeyAt(i));
    if (comp == 0) {
      return {i, false};
    }
    if (comp < 0) {
      return {i, true};
    }
  }
  return {index, true};
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInternalCanSplit(
    const std::vector<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *> &st, const KeyType &key,
    const page_id_t &value, std::unordered_map<page_id_t, bool> *unpin_is_dirty,
    std::unordered_map<page_id_t, size_t> *fuck) {
  KeyType tmp_key = key;
  page_id_t tmp_value = value;

  for (int i = st.size() - 1; i >= 0; i--) {
    auto ret = FindFirstInfIndex(tmp_key, st[i]);
    unpin_is_dirty->insert_or_assign(st[i]->GetPageId(), true);
    if (st[i]->GetSize() < st[i]->GetMaxSize()) {
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
    // LOG_INFO("rem: %d. tmp_key: %ld, ret: %d, %d. cur: %d", rem_to_mov, tmp_key.ToString(), ret.first, ret.second,
    // cur);

    page_id_t neww_page;
    Page *neww_ptr = buffer_pool_manager_->NewPage(&neww_page);
    auto *neww_page_ptr = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(neww_ptr);
    (*fuck)[neww_page]++;
    // LOG_INFO("pin page #%d", neww_page);
    unpin_is_dirty->insert_or_assign(neww_page, true);

    neww_page_ptr->Init(neww_page, st[i]->GetParentPageId(), internal_max_size_);
    // if tmp_key is strictly larger than anyone in the page. move it first.
    if (ret.first == cur && ret.second) {
      neww_page_ptr->SetKeyAt(rem_to_mov - 1, tmp_key);
      neww_page_ptr->SetValueAt(rem_to_mov - 1, tmp_value);
      reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(tmp_value))->SetParentPageId(neww_page);
      (*fuck)[tmp_value]++;
      // LOG_INFO("pin page #%d", tmp_value);
      unpin_is_dirty->insert_or_assign(tmp_value, true);

      rem_to_mov--;
      flag = true;
    }

    while (rem_to_mov > 0) {
      cnt++;
      neww_page_ptr->SetKeyAt(rem_to_mov - 1, st[i]->KeyAt(cur));
      neww_page_ptr->SetValueAt(rem_to_mov - 1, st[i]->ValueAt(cur));
      auto tmp = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(st[i]->ValueAt(cur)));
      (*fuck)[st[i]->ValueAt(cur)]++;
      // LOG_INFO("pin page #%d", st[i]->ValueAt(cur));
      if (tmp != nullptr) {
        tmp->SetParentPageId(neww_page);
        // internal node split can be very frame consuming!
        buffer_pool_manager_->UnpinPage(tmp->GetPageId(), true);
      }
      unpin_is_dirty->insert_or_assign(st[i]->ValueAt(cur), true);

      rem_to_mov--;
      // then peek tmp_key.
      if (!flag && ret.first == cur) {
        neww_page_ptr->SetKeyAt(rem_to_mov - 1, tmp_key);
        neww_page_ptr->SetValueAt(rem_to_mov - 1, tmp_value);
        reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(tmp_value))->SetParentPageId(neww_page);
        (*fuck)[tmp_value]++;
        // LOG_INFO("pin page #%d", tmp_value);
        unpin_is_dirty->insert_or_assign(tmp_value, true);

        // tmp = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(
        //     buffer_pool_manager_->FetchPage(tmp_value));
        // tmp->SetParentPageId(neww_page_ptr->GetPageId());
        rem_to_mov--;
        flag = true;
      }
      cur--;
    }
    st[i]->IncreaseSize(-cnt);
    neww_page_ptr->IncreaseSize(save);
    if (!flag) {
      st[i]->MoveForward(ret.first);
      st[i]->SetKeyAt(ret.first, key);
      st[i]->SetValueAt(ret.first, value);
      st[i]->IncreaseSize(1);
    }

    // LogInternalPage(st[i]);
    // LogInternalPage(neww_page_ptr);

    tmp_key = neww_page_ptr->KeyAt(0);
    tmp_value = neww_page_ptr->GetPageId();
    if (i == 0) {
      Page *new_root_ptr = buffer_pool_manager_->NewPage(&root_page_id_);
      LOG_INFO("update root page id: %d", root_page_id_);
      (*fuck)[root_page_id_]++;
      auto *new_root_page_ptr =
          reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(new_root_ptr);
      new_root_page_ptr->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);

      st[i]->SetParentPageId(root_page_id_);
      neww_page_ptr->SetParentPageId(root_page_id_);

      new_root_page_ptr->SetValueAt(0, st[i]->GetPageId());
      new_root_page_ptr->SetKeyAt(1, neww_page_ptr->KeyAt(0));
      new_root_page_ptr->SetValueAt(1, neww_page_ptr->GetPageId());
      new_root_page_ptr->SetSize(2);

      unpin_is_dirty->insert_or_assign(root_page_id_, true);
      // LogInternalPage(new_root_page_ptr);
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
