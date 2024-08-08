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

  do {
    // BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *ptr_2_internal =
    //     reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(ptr);
    // BPlusTreeLeafPage<KeyType, RID, KeyComparator> *ptr_2_leaf =
    //     reinterpret_cast<BPlusTreeLeafPage<KeyType, RID, KeyComparator> *>(ptr);
    auto *tmp_ptr = reinterpret_cast<BPlusTreePage *>(ptr);

    if (!tmp_ptr->IsLeafPage()) {
      auto ptr_2_internal = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(ptr);
      auto ret = FindFirstInfIndex(key, ptr_2_internal);
      ptr = buffer_pool_manager_->FetchPage(ptr_2_internal->ValueAt(ret.first));
    } else {
      // only get value in leaf page.
      auto ptr_2_leaf = reinterpret_cast<BPlusTreeLeafPage<KeyType, RID, KeyComparator> *>(ptr);
      for (int i = 0; i < ptr_2_leaf->GetSize(); i++) {
        int comp = comparator_(key, ptr_2_leaf->KeyAt(i));

        if (comp == 0) {
          result->push_back(ptr_2_leaf->ValueAt(i));
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
  LOG_INFO("attempts insert kv: %ld, %s", key.ToString(), value.ToString().c_str());
  op_id_++;

  FinalAction final_action(this);
  final_action.Deactivate();

  if (IsEmpty()) {
    // insert a empty tree.
    buffer_pool_manager_->NewPage(&root_page_id_);
    auto *ptr_2_leaf = reinterpret_cast<BPlusTreeLeafPage<KeyType, RID, KeyComparator> *>(
        buffer_pool_manager_->FetchPage(root_page_id_));

    ptr_2_leaf->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
    ptr_2_leaf->SetKeyAt(0, key);

    ptr_2_leaf->SetValueAt(0, value);
    ptr_2_leaf->IncreaseSize(1);

    ptr_2_leaf->SetNextPageId(INVALID_PAGE_ID);
    ptr_2_leaf->SetPrevPageId(INVALID_PAGE_ID);

    // LogLeafPage(ptr_2_leaf);

    return true;
  }

  Page *ptr = buffer_pool_manager_->FetchPage(root_page_id_);
  std::vector<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *> st;

  BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *ptr_2_internal = nullptr;
  BPlusTreeLeafPage<KeyType, RID, KeyComparator> *ptr_2_leaf = nullptr;
  BPlusTreePage *cur_bpluspage_ptr = nullptr;

  do {
    cur_bpluspage_ptr = reinterpret_cast<BPlusTreePage *>(ptr);
    if (!cur_bpluspage_ptr->IsLeafPage()) {
      ptr_2_internal = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(cur_bpluspage_ptr);
      st.push_back(ptr_2_internal);
      auto ret = FindFirstInfIndex(key, ptr_2_internal);
      // already existing?
      if (!ret.second) {
        return false;
      }

      ptr = buffer_pool_manager_->FetchPage(ptr_2_internal->ValueAt(ret.first));
      if (ptr->GetPageId() == 0) {
        break;
      }
      continue;
    }

    ptr_2_leaf = reinterpret_cast<BPlusTreeLeafPage<KeyType, RID, KeyComparator> *>(cur_bpluspage_ptr);
    auto ret = FindFirstInfIndex(key, ptr_2_leaf);
    if (!ret.second && ret.first < ptr_2_leaf->GetSize() + 1) {
      return false;
    }
    // can insert in safe.
    if (ptr_2_leaf->GetSize() < ptr_2_leaf->GetMaxSize()) {
      ptr_2_leaf->MoveForward(ret.first);
      ptr_2_leaf->SetKeyAt(ret.first, key);
      ptr_2_leaf->SetValueAt(ret.first, value);
      ptr_2_leaf->IncreaseSize(1);

      // LogLeafPage(ptr_2_leaf);
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
    page_id_t neww_page;

    Page *neww_ptr = buffer_pool_manager_->NewPage(&neww_page);
    Page *next_ptr = buffer_pool_manager_->FetchPage(next_page);

    auto *neww_page_ptr = reinterpret_cast<BPlusTreeLeafPage<KeyType, RID, KeyComparator> *>(neww_ptr);
    auto *next_page_ptr = reinterpret_cast<BPlusTreeLeafPage<KeyType, RID, KeyComparator> *>(next_ptr);

    neww_page_ptr->Init(neww_page, ptr_2_leaf->GetParentPageId(), leaf_max_size_);
    neww_page_ptr->SetNextPageId(next_page);
    neww_page_ptr->SetPrevPageId(ptr_2_leaf->GetPageId());
    ptr_2_leaf->SetNextPageId(neww_page);
    if (next_page_ptr != nullptr) {
      next_page_ptr->SetPrevPageId(neww_page);
    }

    ret = FindFirstInfIndex(key, ptr_2_leaf);
    int total = ptr_2_leaf->GetMaxSize() + 1;
    int rem_to_mov = total - (total / 2);
    int save = rem_to_mov;
    int cur = ptr_2_leaf->GetSize();
    int cnt = 0;
    bool flag = false;
    // LOG_INFO("prev size: [%d =>%d] + 1", ptr_2_leaf->GetPageId(), ptr_2_leaf->GetSize());
    if (ret.first == cur) {
      // LOG_INFO("[%ld:%s] >> case1 `new_page`", key.ToString(), value.ToString().c_str());
      neww_page_ptr->SetKeyAt(rem_to_mov - 1, key);
      neww_page_ptr->SetValueAt(rem_to_mov - 1, value);
      rem_to_mov--;
      flag = true;
    }
    while (rem_to_mov > 0) {
      cnt++;
      // LOG_INFO("id: %d [%ld:%s] >> case2 `new_page`", ptr_2_leaf->GetPageId(), ptr_2_leaf->KeyAt(cur -
      // 1).ToString(),
      //          ptr_2_leaf->ValueAt(cur - 1).ToString().c_str());

      neww_page_ptr->SetKeyAt(rem_to_mov - 1, ptr_2_leaf->KeyAt(cur - 1));
      neww_page_ptr->SetValueAt(rem_to_mov - 1, ptr_2_leaf->ValueAt(cur - 1));
      rem_to_mov--;
      if (!flag && ret.first == cur) {
        // LOG_INFO("[%ld:%s] >> case3 `new_page`", key.ToString(), value.ToString().c_str());
        neww_page_ptr->SetKeyAt(rem_to_mov - 1, key);
        neww_page_ptr->SetValueAt(rem_to_mov - 1, value);
        rem_to_mov--;
        flag = true;
      }
      cur--;
    }

    ptr_2_leaf->IncreaseSize(-cnt);
    neww_page_ptr->IncreaseSize(save);
    if (!flag) {
      ptr_2_leaf->MoveForward(ret.first);
      ptr_2_leaf->SetKeyAt(ret.first, key);
      ptr_2_leaf->SetValueAt(ret.first, value);
      ptr_2_leaf->IncreaseSize(1);
    }

    // LogLeafPage(ptr_2_leaf);
    // LogLeafPage(neww_page_ptr);

    if (ptr_2_leaf->GetParentPageId() == INVALID_PAGE_ID) {
      buffer_pool_manager_->NewPage(&root_page_id_);

      auto *new_root = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(
          buffer_pool_manager_->FetchPage(root_page_id_));
      new_root->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);

      ptr_2_leaf->SetParentPageId(root_page_id_);
      neww_page_ptr->SetParentPageId(root_page_id_);

      new_root->SetValueAt(0, ptr_2_leaf->GetPageId());
      new_root->SetKeyAt(1, neww_page_ptr->KeyAt(0));
      new_root->SetValueAt(1, neww_page);
      new_root->SetSize(2);

      // LogInternalPage(new_root);

      return true;
    }
    InsertInternalCanSplit(st, neww_page_ptr->KeyAt(0), neww_page_ptr->GetPageId());
    return true;
  } while (true);

  return false;
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
  for (int i = 0; i < ptr_2_internal->GetSize(); i++) {
    LOG_INFO("[%ld:%d]", ptr_2_internal->KeyAt(i).ToString(), ptr_2_internal->ValueAt(i));
  }
  LOG_INFO("****end   log inte page #%d****", ptr_2_internal->GetPageId());
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
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {}

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
    const page_id_t &value) {
  KeyType tmp_key = key;
  page_id_t tmp_value = value;

  for (int i = st.size() - 1; i >= 0; i--) {
    auto ret = FindFirstInfIndex(tmp_key, st[i]);
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

    page_id_t neww_page;
    Page *neww_ptr = buffer_pool_manager_->NewPage(&neww_page);
    auto *neww_page_ptr = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(neww_ptr);

    neww_page_ptr->Init(neww_page, st[i]->GetParentPageId(), internal_max_size_);
    // if tmp_key is strictly larger than anyone in the page. move it first.
    if (ret.first == cur && ret.second) {
      neww_page_ptr->SetKeyAt(rem_to_mov - 1, tmp_key);
      neww_page_ptr->SetValueAt(rem_to_mov - 1, tmp_value);
      reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(tmp_value))->SetParentPageId(neww_page);

      rem_to_mov--;
      flag = true;
    }

    while (rem_to_mov > 0) {
      cnt++;
      neww_page_ptr->SetKeyAt(rem_to_mov - 1, st[i]->KeyAt(cur));
      neww_page_ptr->SetValueAt(rem_to_mov - 1, st[i]->ValueAt(cur));
      reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(st[i]->ValueAt(cur)))
          ->SetParentPageId(neww_page);

      rem_to_mov--;
      // then peek tmp_key.
      if (!flag && ret.first == cur) {
        neww_page_ptr->SetKeyAt(rem_to_mov - 1, tmp_key);
        neww_page_ptr->SetValueAt(rem_to_mov - 1, tmp_value);
        reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(tmp_value))->SetParentPageId(neww_page);

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
    tmp_value = neww_page_ptr->ValueAt(0);
    if (i == 0) {
      Page *new_root_ptr = buffer_pool_manager_->NewPage(&root_page_id_);
      auto *new_root_page_ptr =
          reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(new_root_ptr);
      new_root_page_ptr->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);

      st[i]->SetParentPageId(root_page_id_);
      neww_page_ptr->SetParentPageId(root_page_id_);

      new_root_page_ptr->SetValueAt(0, st[i]->GetPageId());
      new_root_page_ptr->SetKeyAt(1, neww_page_ptr->KeyAt(0));
      new_root_page_ptr->SetValueAt(1, neww_page_ptr->GetPageId());
      new_root_page_ptr->SetSize(2);

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
