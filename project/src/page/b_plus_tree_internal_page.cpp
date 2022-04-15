/**
 * b_plus_tree_internal_page.cpp
 * 
 * 
 * key == no.
 * value == child page id
 * parent -> child == many children page id
 * child -> parent == 1 parent page id
 */
#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "page/b_plus_tree_internal_page.h"

namespace cmudb {

//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////


/*
 * init: 
 *
 * set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id,
                                          page_id_t parent_id) {


  // 1. parent class page


  // 2. child class internal page


}

/*****************************************************************************
 * edit node's kv and edges of b+tree
 * 
 * GetValueByKey() == map[k] -> v 
 * 
 * NOTE: internal's 
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetKeyByIndex(int index) const {
  // replace with your own code
  KeyType key = {};
  return key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyByIndex(int index, const KeyType &key) {
  
}


INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetIndexById(const ValueType &value) const {
  return 0;
}


INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetValueByIndex(int index) const { 
  return 0; 
}

/*
 * map[k] -> v (but since here is an array)
 * search within internal page which child internal/leaf page 
 * 
 * start w second key(the first key should always be invalid)
 * return the child pointer(page_id)
 * 
 * key == no. you use to compare traversing down the tree
 * 
 * == LookUp()
 * 
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType
B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetValueByKey(const KeyType &key,
                                       const KeyComparator &comparator) const {
  
  // 1. takes 1st and last 
  
  // smaller than smallest
  if (comparator(key, array[1].first) < 0) {
    return array[0].second;
  }
    
  
  // 2. then for loop 1 by 1 

  
  return INVALID_PAGE_ID;
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
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * 
 * 
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(
    const ValueType &old_value, const KeyType &new_key,
    const ValueType &new_value) {}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(
    const ValueType &old_value, const KeyType &new_key,
    const ValueType &new_value) {
  return 0;
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
 * SPLIT
 *****************************************************************************/
/*
 when insert into full leaf/internal page, tranfer half of old -> new page 
 
 recipient == new page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(
    BPlusTreeInternalPage *recipient,
    BufferPoolManager *buffer_pool_manager) {
  

  // 1. move later half to new page 
  old_page_size = GetSize();
  old_page_half_index = (old_page_size + 1) / 2;
  for (int i = 0; i < old_page_half_index; ++i) {
    // recipient->array[i] = array[old_page_half_index + i];
    recipient->SetArray(i, array[old_page_half_index + i]);
  }

  // 2. u() old, new page meta
  DecreaseSize(old_page_half_index);
  recipient->IncreaseSize(old_page_half_index);


  // 3. u() moved children page's parent == to point to new page
  for (int i = old_page_half_index; i < old_page_size; ++i) {
    new_page_child_page_id = GetValueFromIndex(i);
    auto *new_page_child_page = buffer_pool_manager->FetchPage(new_page_child_page_id);
    auto page_bytes = new_page_child_page->GetData();
    auto child_tree_page = reinterpret_cast<BPlusTreePage *>(page_bytes);
    child_tree_page->SetParentPageId(recipient->GetPageId());
  }

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
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  
  // 



}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  return INVALID_PAGE_ID;
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
 * MERGE
 *****************************************************************************/
/*
 * move all kv from deleted node to untouched node  
 * 
 * parent goes to deleted node, could be either right or left
 * then rhs merges into left (code seems easier)
 * 
 * 
 * this == deleted kv node 
 * recipient == untouched node
 * 
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(
    BPlusTreeInternalPage *recipient, int deleted_node_parent_pos,
    BufferPoolManager *buffer_pool_manager) {
  
  // 1. get parent 
  auto *page = buffer_pool_manager->FetchPage(GetParentPageId());
  auto parent_page =
    reinterpret_cast<BPlusTreeInternalPage<KeyType, decltype(GetPageId()),
                                        KeyComparator> *>(page->GetData());
  
  
  // 2. parent move to deleted child 
  MappingType parent_first_kv = parent_page->GetKVByIndex(0);
  int kv_size = static_cast<size_t>(GetSize()*sizeof(MappingType)); 
  memmove(array[0], parent_first_kv, kv_size);
  buffer_pool_manager->UnpinPage(GetParentPageId(), true);


  // 3. untouched child move all to deleted child (merge)
  // recipient's array[] -> this's array[] 
  int recipient_node_size = recipient->GetSize();   
  int deleted_node_start_pos = GetSize();
  memmove(recipient->array[0], array[deleted_node_start_pos], recipient_node_size * kv_size);

  // 4. u() meta of u and children
  int num_recipient_children = recipient->GetSize();
  for (size_t i = 0; i < num_recipient_children; i++){
    child_page_id = recipient->GetValueByIndex(i);
  
    auto *page = buffer_pool_manager->FetchPage(child_page_id);
    auto child_page =
      reinterpret_cast<BPlusTreeInternalPage<KeyType, decltype(GetPageId()),
                                          KeyComparator> *>(page->GetData());
    child_page->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(GetParentPageId(), true);
  }
  
  // each child set to same parent is enough == can use child id to get parent index later


}





/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * CASE 1: deleted node on lhs
 * 
 * move 1st kv from rhs sibling to lhs deleted node + u() meta
 * 
 * hint: graph from https://www.youtube.com/watch?v=YZECPU-3iHs&ab_channel=QandA  
 * 
 * this == untouched node
 * recipient == deleted node 
 * pair.first == key
 * pair.second == page id pointing to
 * 
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(
    BPlusTreeInternalPage *recipient,
    BufferPoolManager *buffer_pool_manager) {

  // 1. get untouched right node's 1st kv n its leftmost child 
  // MappingType untouched_first_kv = GetKVByIndex(0);
  leftmost_key = GetKeyByIndex(0);
  leftmost_child_id = GetValueByIndex(0);

  // delete right node empty space
  memmove(key_array[0], key_array[1] , GetKeySize() * key_size); // dest, src
  memmove(value_array[0], value_array[1] , GetValueSize() * value_size); 
  DecreaseSize(1);


  // 2. triangle rotate !!!! (NOT lateral move)
  // rhs first -> lhs last 

  // get parent 
  auto *page = buffer_pool_manager->FetchPage(GetParentPageId());
  auto parent_page =
  reinterpret_cast<BPlusTreeInternalPage<KeyType, decltype(GetPageId()),
                                      KeyComparator> *>(page->GetData());

  // insert parent's key behind lhs delete node's last key
  // NOTE: child page id still the same 
  MappingType parent_key = parent_page->GetKeyByValue(leftmost_child_id);  
  memmove(recipient->key_array[recipient->GetKeySize()], parent_key, key_size);
  recipient->IncreaseSize(1);
  
  // parent's 1st key is replaced by right node's 1st key
  memmove(parent_page->key_array[GetIndexByKey(parent_key)], leftmost_key, key_size);

  // parent (now at left node) points at what right node's 1st child
  recipient->value_array.insert(recipient->value_array.end(), leftmost_child_id);
  buffer_pool_manager->UnpinPage(GetParentPageId(), true);


  // 3. u() child's meta 
  auto *page = buffer_pool_manager->FetchPage(leftmost_child_id);
  auto child_page =
    reinterpret_cast<BPlusTreeInternalPage<KeyType, decltype(GetPageId()),
                                        KeyComparator> *>(page->GetData());
  child_page->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(leftmost_child_id, true);
}



/*
 * CASE 2: delete node on rhs
 * 
 * move last kv from left node to rhs deleted node + u() meta
 * hint: graph from https://www.youtube.com/watch?v=YZECPU-3iHs&ab_channel=QandA  
 * 
 * 
 * this == untouched node == lhs
 * recipient == deleted node == rhs
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(
    BPlusTreeInternalPage *recipient, int parent_index,
    BufferPoolManager *buffer_pool_manager) {

  // 1. get untouched lhs node's last kv n its rightmost child 
  rightmost_key = GetKeyByIndex(GetKeySize());
  rightmost_child_id = GetValueByIndex(GetValueSize());

  // delete left node empty space at the end
  key_array[GetKeySize()] = nullptr;
  value_array[GetValueSize()] = nullptr;
  DecreaseSize(1);


  // 2. triangle rotate !!!! (NOT lateral move)
  // rhs last -> lhs first


  // get parent 
  auto *page = buffer_pool_manager->FetchPage(GetParentPageId());
  auto parent_page =
  reinterpret_cast<BPlusTreeInternalPage<KeyType, decltype(GetPageId()),
                                      KeyComparator> *>(page->GetData());
                                      
  
  // insert parent's key in front of rhs's 1st key
  MappingType parent_key = parent_page->GetKeyByValue(rightmost_child_id);  
  memmove(recipient->key_array[0], parent_key, key_size);
  recipient->IncreaseSize(1);


  // parent's key is replaced by lhs sibling's key
  memmove(parent_page->key_array[GetIndexByKey(parent_key)], rightmost_key, key_size);

  // parent points at what rhs sibling originally points at 

  // parent (now at right node) points at what left node's last child
  recipient->value_array.insert(recipient->value_array.begin(), rightmost_child_id);
  buffer_pool_manager->UnpinPage(GetParentPageId(), true);


  // 3. u() child's meta 
  auto *page = buffer_pool_manager->FetchPage(leftmost_child_id);
  auto child_page =
    reinterpret_cast<BPlusTreeInternalPage<KeyType, decltype(GetPageId()),
                                        KeyComparator> *>(page->GetData());
  child_page->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(leftmost_child_id, true);
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
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::QueueUpChildren(
    std::queue<BPlusTreePage *> *queue,
    BufferPoolManager *buffer_pool_manager) {
  for (int i = 0; i < GetSize(); i++) {
    auto *page = buffer_pool_manager->FetchPage(array[i].second);
    if (page == nullptr)
      throw Exception(EXCEPTION_TYPE_INDEX,
                      "all page are pinned while printing");
    BPlusTreePage *node =
        reinterpret_cast<BPlusTreePage *>(page->GetData());
    queue->push(node);
  }
}

INDEX_TEMPLATE_ARGUMENTS
std::string B_PLUS_TREE_INTERNAL_PAGE_TYPE::ToString(bool verbose) const {
  if (GetSize() == 0) {
    return "";
  }
  std::ostringstream os;
  if (verbose) {
    os << "[pageId: " << GetPageId() << " parentId: " << GetParentPageId()
       << "]<" << GetSize() << "> ";
  }

  int entry = verbose ? 0 : 1;
  int end = GetSize();
  bool first = true;
  while (entry < end) {
    if (first) {
      first = false;
    } else {
      os << " ";
    }
    os << std::dec << array[entry].first.ToString();
    if (verbose) {
      os << "(" << array[entry].second << ")";
    }
    ++entry;
  }
  return os.str();
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t,
                                           GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t,
                                           GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t,
                                           GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t,
                                           GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t,
                                           GenericComparator<64>>;
} // namespace cmudb
