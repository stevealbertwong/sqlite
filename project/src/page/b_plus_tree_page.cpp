#include "page/b_plus_tree_page.h"

namespace cmudb {

// PAGE TYPE
bool BPlusTreePage::IsLeafPage() const { return page_type_ == IndexPageType::LEAF_PAGE; }
bool BPlusTreePage::IsRootPage() const { return parent_page_id_ == INVALID_PAGE_ID; }
void BPlusTreePage::SetPageType(IndexPageType page_type) {page_type_ = page_type;}


// PAGE SIZE
// num of occupied kv slots
int BPlusTreePage::GetSize() const { return size_; }
void BPlusTreePage::SetSize(int size) {size_ = size;}
void BPlusTreePage::IncreaseSize(int amount) {size_ += amount;}
void BPlusTreePage::DecreaseSize(int amount) {size_ -= amount;}
// page's capacity
int BPlusTreePage::GetMaxSize() const { return max_size_; }
void BPlusTreePage::SetMaxSize(int size) {max_size_ = size;}


// PAGE ID
// parent == btree == 1 page only 1 parent, shared by all kv
// child == internal / leaf == each kv has 1 child
page_id_t BPlusTreePage::GetParentPageId() const { return parent_page_id_; }
void BPlusTreePage::SetParentPageId(page_id_t parent_page_id) {parent_page_id_ = parent_page_id;}
page_id_t BPlusTreePage::GetPageId() const { return page_id_; }
void BPlusTreePage::SetPageId(page_id_t page_id) {page_id_ = page_id;}


// PAGE LSN
void BPlusTreePage::SetLSN(lsn_t lsn) { lsn_ = lsn; }

} // namespace cmudb
