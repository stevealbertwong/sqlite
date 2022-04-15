/**
 * b_plus_tree_leaf_page.cpp
 * 
 * heavy lifting == all real changes in leaf node
 * 
 * 
 * TODO: 
 */

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "page/b_plus_tree_leaf_page.h"

namespace cmudb {

//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////

/*****************************************************************************
 * getters n setters
 * 
 * index == array offset
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id) {

  
}

INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const {
  return next_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
}



/**
 * find pos in leaf node to insert key
 * i.e. index i so that array[i].first >= key
 * 
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::GetIndexByKey(
    const KeyType &key, const KeyComparator &comparator) const {
  
  // CASE 1: found pos key should be inserted in leaf node
  for (size_t i = 0; i < GetSize(); i++){
    if(comparator(key, array[i].first) == 0){
      return i;
    }
  }

  // CASE 2: key is bigger than every item in leaf node
  return GetSize(); 
}



GetIndexById(){

}


INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::GetKeyByIndex(int index) const {
  
  KeyType key = array[index].first;
  return key;
}



INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetKVpair(int index) {
  return array[index];
}


//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////


/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * insert + sorted logic
 * (caller func deals w r() page n recursion)
 * 
 * @return  page size after insertion
 * 
 * 
 * NOTE: 
 * called after split == guaranteed still space left 
 * 
 * example of a comparator:
 * comparator == many if statements, return 0,1,1+ as comparison results
 * https://www.youtube.com/watch?v=4cBEUF3zjUY&ab_channel=BrianDyck 
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key,
                                       const ValueType &value,
                                       const KeyComparator &comparator) {
  
  // CASE 1. empty array
  if(GetSize() == 0){
    array[0] = {key, value};
  
  // CASE 2. if new key > last element, insert at back 
  } else if(comparator(key, array[GetSize()-1]) > 0){
    array[GetSize()] = {key, value};
    
  // CASE 3. if key < 1st, insert at front 
  } else if (comparator(key, array[GetSize()-1]) < 0)) {
    size_t total_bytes_to_move = static_cast<size_t>(GetSize() * sizeof(MappingType));
    memmove(array + 1, array, total_bytes_to_move);
    array[0] = {key, value};
  
  // CASE 4. 98% of cases, sort everything
  } else {
    // TODO:

  }

  


  return 0;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * just mv 1/2 of ourselves to new page
 * (dun worry about new key, caller func will take care)
 * 
 * NOTE: 
 * recipient == new page for split
 * array == original full page 
 * array is already sorted 
 * 
 * Q: 
 * split == must mean full array ?? 
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(
    BPlusTreeLeafPage *recipient,
    __attribute__((unused)) BufferPoolManager *buffer_pool_manager) {
  
  // 1. mv 1/2 to new page
  int start_pos_to_new_page = GetSize()/2; // 5 / 2 = 3 
  for (size_t i = 0; i < GetSize()/2; i++){
    recipient->array[i] = *array[start_pos_to_new_page];
    array[start_pos_to_new_page] = 0;
  }
  
  // 2. u() meta of both
  recipient->IncreaseSize();
  DecreaseSize();
}


// INDEX_TEMPLATE_ARGUMENTS
// void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyHalfFrom(MappingType *items, int size) {}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType &value,
                                        const KeyComparator &comparator) const {
  return false;
}

//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immdiately.
 * NOTE: store key&value pair continuously after deletion
 * 
 * 
 * @return   page size == whether needs recurse + whether successfully delete key
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::DeleteKVFromLeaf(
    const KeyType &key, const KeyComparator &comparator) {
  
  int key_pos = GetIndexByKey(key, comparator_);
  int page_size = GetSize();

  // 1. if key does not exit 
  if(page_size == 0 || comparator_(key, GetKeyByIndex(0)) < 0) || comparator_(key, GetKeyByIndex(total_size-1)) > 0) { 
    return page_size;    
  }

  // 2. if key exists
  if(comparator_(key, GetKeyByIndex(key_pos)) == 0){ 
    // delete kv from array
    memmove(array + i, array + i + 1, sizeof(MappingType));
    DecreaseSize(1);  
  }
  
  return page_size;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * move all remaining kv from deleted node to untouched node + u() meta
 * 
 * recipient == untouched node
 * this == deleted kv node 
 * 
 * 
 * Q&A:
 * merge == deleted node must b empty ??
 * 
 * no need to sort kv by key ?? already sorted when insert ??
 * 
 * no need to u() parent ?? or u() in coalesce() ??
 * - parent is always already deleted (either left or right leaf child)
 * 
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient,
                                           int, BufferPoolManager *) {
  
  int kv_size = static_cast<size_t>(GetSize()*sizeof(MappingType)); 
  
  // 1. this's array[] -> recipient's array[] 
  int deleted_node_size = GetSize();
  int recipient_node_start_pos = recipient->GetSize();   
  memmove(array[0], recipient->array[recipient_node_start_pos], deleted_node_size * kv_size);
  

  // 2. u() recipient's meta
  deleted_node_next_node_id = GetNextPageId();
  recipient->SetNextPageId(deleted_node_next_node_id);
  
  recipient->IncreaseSize(deleted_node_size);

}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * CASE leftmost: 
 * mv 1st kv of right node to left node's end + u() child n parent meta
 * 
 * leftmost node == recipient == deleted node 
 * right node == this == to mv its 1st key to left node's end
 * 
 * key == number on node
 * value == disk addr 
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(
    BPlusTreeLeafPage *recipient,
    BufferPoolManager *buffer_pool_manager) {

    // 1. mv 1st key of right node to left node's end 
    int kv_size = static_cast<size_t>(GetSize()*sizeof(MappingType)); 
    int recipient_node_start_pos = recipient->GetSize();
    MappingType right_node_first_kv = GetKVByIndex(0);

    memmove(array, array + 1, kv_size); // dest, src, size
    memmove(array[0], recipient->array[recipient_node_start_pos], kv_size);

    
    // 2. u() child + parent meta
    // 2.1 right node 1st key becomes new parent 
    // NOTE: parent still pointing at the same children, n vice versa
    // just child's array is u()
    auto *page = buffer_pool_manager->FetchPage(GetParentPageId());
    auto parent_page =
        reinterpret_cast<BPlusTreeInternalPage<KeyType, decltype(GetPageId()),
                                            KeyComparator> *>(page->GetData());
    int right_node_id = GetPageId();
    int right_child_node_pos_at_parent = parent_page->GetIndexById(right_node_id);
    parent_page->SetKeyByIndex(right_child_node_pos_at_parent, right_node_first_kv.first);
    buffer_pool_manager->UnpinPage(GetParentPageId(), true);

    // 2.2
    DecreaseSize(1);      
}


/*
 * CASE non-leftmost:
 * mv last kv of left node to right node's first + u() child n parent meta
 * 
 * left node == this == to mv left node last key to right node's 1st
 * right node == recipient == deleted node  
 * 
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(
    BPlusTreeLeafPage *recipient, int parentIndex,
    BufferPoolManager *buffer_pool_manager) {

    // 1. mv last kv of left node to right node's first
    int kv_size = static_cast<size_t>(GetSize()*sizeof(MappingType)); 
    MappingType left_node_last_kv = GetItem(GetSize()-1);    
    int left_node_last_pos = GetSize();
    
    // right node clear out 1st pos for left node last item
    memmove(recipient->array + 1, recipient->array, recipient->GetSize()*sizeof(MappingType));
    recipient->array[0] = left_node_last_kv;
    array->[GetSize()-1)] = nullptr;

    
    // 2. u() child + parent meta
    // 2.1 left node last key becomes new parent 
    // NOTE: parent still pointing at the same children, n vice versa
    // just child's array is u()
    auto *page = buffer_pool_manager->FetchPage(GetParentPageId());
    auto parent_page =
        reinterpret_cast<BPlusTreeInternalPage<KeyType, decltype(GetPageId()),
                                            KeyComparator> *>(page->GetData());
    
    int left_node_id = GetPageId();
    int left_child_node_pos_at_parent = parent->GetIndexById(left_node_id);
    parent->SetKeyByIndex(left_child_node_pos_at_parent, left_node_last_kv.first);
    buffer_pool_manager->UnpinPage(GetParentPageId(), true);

    // 2.2
    DecreaseSize(1);
    recipient->IncreaseSize(1);

}


//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////

/*****************************************************************************
 * DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
std::string B_PLUS_TREE_LEAF_PAGE_TYPE::ToString(bool verbose) const {
  if (GetSize() == 0) {
    return "";
  }
  std::ostringstream stream;
  if (verbose) {
    stream << "[pageId: " << GetPageId() << " parentId: " << GetParentPageId()
           << "]<" << GetSize() << "> ";
  }
  int entry = 0;
  int end = GetSize();
  bool first = true;

  while (entry < end) {
    if (first) {
      first = false;
    } else {
      stream << " ";
    }
    stream << std::dec << array[entry].first;
    if (verbose) {
      stream << "(" << array[entry].second << ")";
    }
    ++entry;
  }
  return stream.str();
}

template class BPlusTreeLeafPage<GenericKey<4>, RID,
                                       GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID,
                                       GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID,
                                       GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID,
                                       GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID,
                                       GenericComparator<64>>;
} // namespace cmudb
