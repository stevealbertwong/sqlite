/**
 * class representing internal page node 
 * all funcs specific to internal page
 * 
 * b_plus_tree_internal_page.cpp
 * 
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

/*****************************************************************************
 * getter, setter of kv of internal page 
 *****************************************************************************/
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



INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetKeyByIndex(int index) const {
  // replace with your own code
  KeyType key = {};
  return key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyByIndex(int index, const KeyType &key) {}


INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetIndexByValue(const ValueType &value) const {
  return 0;
}


INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetValueByIndex(int index) const { return 0; }


//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * search within internal page which child internal/leaf page 
 * 
 * start w second key(the first key should always be invalid)
 * return the child pointer(page_id)
 * 
 * key == no. you use to compare traversing down the tree
 * 
 * 
 * 
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType
B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key,
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


// useless 
// INDEX_TEMPLATE_ARGUMENTS
// void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyHalfFrom(
//     MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {}

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
 * recipient == untouched node
 * this == deleted kv node 
 * 

 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(
    BPlusTreeInternalPage *recipient, int index_in_parent,
    BufferPoolManager *buffer_pool_manager) {
  
  // 1. get parent 
  // 2. parent move to deleted child 
  // 3. deleted child move all to untouched child (merge)
  // 4. u() meta of u and children

}




// INDEX_TEMPLATE_ARGUMENTS
// void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyAllFrom(
//     MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {}


//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////


/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * move 1st kv from rhs sibling to lhs deleted node + u() meta
 * 
 * hint: graph from https://www.youtube.com/watch?v=YZECPU-3iHs&ab_channel=QandA  
 * 
 * recipient == kv deleted node 
 * pair.first == key
 * pair.second == page id pointing to
 * 
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(
    BPlusTreeInternalPage *recipient,
    BufferPoolManager *buffer_pool_manager) {

  // 1. get rhs sibling's 1st kv n children it points to



  // 2. rhs first -> lhs last == triangle rotate 

  // get parent 
  
  // insert parent's key behind lhs delete node's last key
  
  // parent's key is replaced by rhs sibling's 1st key

  // parent points at what lhs sibling originally points at 


  // 3. u() child's meta 


}


// useless
// INDEX_TEMPLATE_ARGUMENTS
// void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(
//     const MappingType &pair, BufferPoolManager *buffer_pool_manager) {}



/*
 * move last kv from lhs sibling to rhs deleted node + u() meta
 * hint: graph from https://www.youtube.com/watch?v=YZECPU-3iHs&ab_channel=QandA  
 * 
 * 
 * Remove the last key & value pair from this page to head of "recipient"
 * page, then update relavent key & value pair in its parent page.
 * 
 * 
 * 
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(
    BPlusTreeInternalPage *recipient, int parent_index,
    BufferPoolManager *buffer_pool_manager) {

  // 1. get lhs sibling's last kv n childrent it points to



  // 2. lhs last -> rhs first == triangle rotate 

  // get parent 
  
  // insert parent's key in front of rhs's 1st key
  
  // parent's key is replaced by lhs sibling's key

  // parent points at what rhs sibling originally points at 



  // 3. u() child's meta 
  
}


// useless
// INDEX_TEMPLATE_ARGUMENTS
// void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(
//     const MappingType &pair, int parent_index,
//     BufferPoolManager *buffer_pool_manager) {}





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
