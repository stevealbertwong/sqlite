/**
 * b_plus_tree.cpp
 * 
 * reference: 
 * https://www.programiz.com/dsa/insertion-on-a-b-plus-tree 
 * https://www.programiz.com/dsa/deletion-from-a-b-plus-tree
 * 
 * 
 * 3 APIs:
 * search()
 * insert()
 * remove()
 * 
 * 
 * 
 * 
 * 

B+ tree of degree m:

A node can have a maximum of m children. (i.e. 3)
A node can contain a maximum of m - 1 keys. (i.e. 2) == if less, then underflow
A node should have a minimum of ⌈m/2⌉ children. (i.e. 2) 
A node (except root node) should contain a minimum of ⌈m/2⌉ - 1 keys. (i.e. 1)
 * 
 */
#include <iostream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "index/b_plus_tree.h"
#include "page/header_page.h"

namespace cmudb {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(const std::string &name,
                                BufferPoolManager *buffer_pool_manager,
                                const KeyComparator &comparator,
                                page_id_t root_page_id)
    : index_name_(name), root_page_id_(root_page_id),
      buffer_pool_manager_(buffer_pool_manager), comparator_(comparator) {}


/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return true; }


//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////


/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key,
                              std::vector<ValueType> &result,
                              Transaction *transaction) {


  // 1. get "data page" of certain range of keys from disk


  // 2. use key to search for value within that data page 
      
  
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
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value,
                            Transaction *transaction) {
  
  StartNewTree()

  FindLeafPage()

  auto leaf, leaf2 = InsertIntoLeaf();


   
  // split key == MUST BE 1st element of 2nd leaf
  InsertIntoParent()
  
  
  return false;
}




/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {










}


/*
 *
 *
 * 
*/
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value,
                                    Transaction *transaction) {

  // 1. find the leaf page 
  auto *leaf_page = FindLeafPage();

  // 2. check if key already exists 





  // 3. insert {k,v} into leaf page


  // 3.1 if still space left 
  if (leaf->GetSize() < leaf->GetMaxSize()) { 
    leaf->Insert();



  // 3.2 if leaf page already full
  } else {

    // 3.2.1 
    auto *leaf, *leaf2 = Split(LEAF_SPLIT)
    if { // if key smaller than leaf2 first k/v
      leaf->Insert();
    } else {
      leaf2->Insert();
    }
    
    // 3.2.2 chain neigbour leaf pages tgt 
    if { // leaf1 k/v smaller than leaf2 first k/v
      leaf2->SetNextPageId(leaf->GetNextPageId()); // insert leaf2 in between leaf n leaf1
      leaf->SetNextPageId(leaf2->GetPageId());
    } else {
      leaf2->SetNextPageId(leaf->GetPageId());
    }
  }

  return leaf, leaf2 
  // return false;
}


/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N> N *BPLUSTREE_TYPE::Split(N *node) { 
  
  // 1. init new internal page



  // 2. tranfer half of old -> new page 
  MoveHalfTo()


  return nullptr; 


}



/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 * 
 * 
 * insert the split key into parent
 * 
 * key == split key == 1st element of 2nd split page == become parent 
 * old_node == original lhs leaf node, exists before split 
 * new_node == newly split rhs leaf node 
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_child_node,
                                      const KeyType &key,
                                      BPlusTreePage *new_child_node,
                                      Transaction *transaction) {

  // base condition
  // CASE 1 : root node level split
  if (old_child_node->IsRootPage()){

    
    // 1. init new page for root
    auto *root_page = buffer_pool_manager_->NewPage(ROOT_PAGE_ID);
    auto root_internal_page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t,
                                            KeyComparator> *>(page->root_page());

    // 2. new root page register() old, new split leaf page as children
    // 1 to many (1 parent, many kv/children)
    root_internal_page->Init();
    root_internal_page->PopulateNewRoot(old_child_node, key, new_child_node);

    
    // 3. old, new split leaf page register() new root page as parent
    // 1 to 1 (1 child, 1 parent)
    old_child_node->SetParentPageId(ROOT_PAGE_ID);
    new_child_node->SetParentPageId(ROOT_PAGE_ID);


  // CASE 2: NON root node level split
  } else {
      
    // 1. get 1st page's parent + u() new node's parent
    auto *old_node_parent_page = buffer_pool_manager_->FetchPage(old_child_node->GetParentPageId());
    auto parent_page_bytes = old_node_parent_page->GetData();
    auto old_node_parent_internal_page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(parent_page_bytes);
    new_child_node.SetParentPageId(old_node_parent_internal_page->GetPageId());


    // 2. 
    // 2.1 if not full, just vanilla insert  
    if (old_node_parent_internal_page->GetSize() < old_node_parent_internal_page->GetMaxSize()){

      // insert 1st key of 2nd page (orphan) into 1st page's existing parent
      old_node_parent_internal_page.InsertNodeAfter(key);
      


    // 2.2 if full, internal page split 
    } else {

      // 1. init new page
      auto *new_internal_parent_page = buffer_pool_manager_->NewPage();


      // 2. split == move later half to + u() child + insert key (5) in 1st internal page
      Split(old_node_parent_internal_page, new_internal_parent_page), INTERNAL_SPLIT);
      if(){ // if center key between children < 1st element of 2nd rhs parent split internal page
      // rhs new child node belongs to lhs parent 
        new_child_node->SetSetParentPageId(old_node_parent_internal_page); 
      } else{
        new_child_node->SetSetParentPageId(new_internal_parent_page); 
      }

      // 3. recurse to parent w 1st element of 2nd page (17)
      // only MoveHalfTo() is u() from leaf to internal version ??
      InsertIntoParent(new_internal_page->GetKeyByIndex(0))
    }
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
 * 
 * 
 * 
 * 

Start from root node, perform exact match for key as ‘key’ till a leaf node. 
search path == x1, x2, … , xh.  
x1 == first node == root
xh == leaf node
xi == parent of xi+1

delete the object where key is ‘key’ from xh.
if h = 1, then return, as there is only one node which is root.
i := h
while xi underflows, do
   
   if immediate sibling node s of xi, has at least m/2 + 1 elements, then
      redistribute entries evenly between s and xi. corresponding to re distribution, 
      a key k in the parent node xi-1, will be changed. 
      (parent u() to point to newly redistributed entries.)
      
      if xi is non-leaf node, then
         key k is dragged down to xi. and a key from sibling node s is 
         pushed up to fill the place of original k
      
      else // leaf node
         k is simply replaced by a key in s
      return
   


   // cant redistribute as sibling only has enough for itself
   else 
      merge xi with the sibling node s. 
      delete the corresponding child pointer in xi-1.

      if xi is an internal node, then
         drag the key in xi-1. which previously divides xi and s. 
         into the new node xi and s, into the new node xi.
      
      else // leaf node
         delete that key in xi-1.
      i := i – 1
   end if
done

get all the nodes n on the path from the root to the leaf containing K
    
    If n is root, remove K
      a. if root has more than one key, done
      b. if root has only K
        i) if any of its child nodes can lend a node
            Borrow key from the child and adjust child links
        ii) Otherwise merge the children nodes. It will be a new root
    

    If n is an internal node, remove K
      i) If n has at least ceil(m/2) keys, done!
      ii) If n has less than ceil(m/2) keys,
          If a sibling can lend a key,
          Borrow key from the sibling and adjust keys in n and the parent node
              Adjust child links
          Else
              Merge n with its sibling
              Adjust child links
    

    If n is a leaf node, remove K
      i) If n has at least ceil(M/2) elements, done!
          In case the smallest key is deleted, push up the next key
      ii) If n has less than ceil(m/2) elements
      If the sibling can lend a key
          Borrow key from a sibling and adjust keys in n and its parent node
      Else
          Merge n and its sibling
          Adjust keys in the parent node





Step 1: Take the input in a key-value and search for the leaf node containing the key value.

Step 2: If the key is found, remove that entry from the leaf

• If the leaf meets “Half Full criteria” then it is done
• otherwise, the leaf has some data entries.

Step 3: 

• If the leaf’s right sibling can have an entry. 
move the very biggest entry to that right sibling of the leaf.

• Otherwise, if the leaf’s left sibling can take an entry, 
move the smallest node to that left sibling of the leaf.

• If it doesn’t meet the above two criteria, merge both leaf and a sibling.


Step 4: 

While merging, recursively deletes the entry which is pointing to the leaf or sibling from the parent.


Step 5: Merging could make a change in the height of the tree.





1. Descend to the leaf where the key exists.
 * 
 * 2. Remove the required key and associated reference from the node.
 * 
 * 3. If the node still has enough keys and references 
 * to satisfy the invariants, stop.
 * 
 * 
 * 
 * 4. redistribute 
 * 4.1 If the node has too few keys to satisfy the invariants, 
 * but its next oldest or next youngest sibling at the same level has more than necessary 
 * 
 * then distribute the keys between this node and the neighbor. 
 * 
 * 4.2 Repair the keys in the level above to represent that these nodes now have 
 * a different “split point” between them; this involves simply changing a 
 * key in the levels above, without deletion or insertion.
 * 
 * 
 * 
 * 5. coalise / merge 
 * 5.1 If the node has too few keys to satisfy the invariant, 
 * and the next oldest or next youngest sibling is at the minimum for the invariant, 
 * 
 * then merge the node with its sibling; 
 * 
 * 5.2 if the node is a non-leaf, we will 
 * need to incorporate the “split key” from the parent into our merging.
 * 
 * 
 * 
 * 6. In either case, we will need to repeat the removal algorithm on the 
 * parent node to remove the “split key” that previously separated these 
 * merged nodes — unless the parent is the root and we are removing the 
 * final key from the root, in which case the merged node becomes the 
 * new root (and the tree has become one level shorter than before).
 *****************************************************************************/



/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 * 
 * 
 * 
 * total cases (12 cases):
 * 
 * - root (2 cases)
    * - enough keys vs only 1 key left
 * 
 * - leaf node (5 cases)
    * - enough keys vs coalesce vs redistribute     
    * - leftmost rhs sibling vs lhs sibling
 * 
 * - non leaf node (5 cases)
    * - enough children vs coalesce vs redistribute 
    * - leftmost rhs sibling vs lhs sibling
 *  
 * 
 * NOTE:
 * - there are other ways of implementing B+ tree
 * - but the above one is the easiest to implement 
 * - e.g. steal from children etc. 
 * 
 * FOUND
 * leaf, coalesce, non-leftmost 
 * 
 * 
 * 
 * 
 * 
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {

  // 1. traverse tree, delete leaf key first
  FindLeafPage()
  RemoveAndDeleteRecord()


  // 2. starting from leaf, recursively delete + 14 cases
  CoalesceOrRedistribute()
  


}

/*
 * decides 8 cases, then call its 8 sub functions to heavy lift
 * merge and rebalance w sibling and parent (triangle), then recurse the same up 
 * 
 * 
 * User needs to first find the sibling of input page. 
 * If sibling's size + input page's size > page's max size, then redistribute. 
 * Otherwise, merge.
 * 
 * Using template N to represent either internal page or leaf page.
 * 
 * 
 * covers 8 cases:
 * - you being leftmost child vs not
 * - leaf vs non leaf 
 * - coalesce vs redistribute
 * 
 * 
 * 
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 * 
 * 
 
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  
  // 0. base condition: reach root page



  // 1. fetch parent n sibling page 
  // use parent to get sibling 
  

  // 2. 14 cases
  // 2.1 enough keys -> 4 cases 


  
  
  
  // 2.2 not enough keys -> 8 cases 
  
  // 2.2.1 if sibling > 50% keys 
  // 4 cases: leaf, non leaf, lhs, rhs 
  // leaf vs non leaf auto covered by page.cc 
  Redistribute()


  // 2.2.2 if sibling < 50% keys 
  // 4 cases
  Coalesces() 





  // 2.3 deal w root
  


  // 3. decides whether redistribute or coalesce




  
  
  return false;
}

/*
 * deals w 2 cases + recurse 
 *
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(
    N *&neighbor_node, N *&node,
    BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *&parent,
    int index, Transaction *transaction) {

  // 1. merge neighbours  
  MoveAllTo()

  


  // 2. recurse 
  // u() parent to delete key before recurse
  Remove()

  CoalesceOrRedistribute()

  return false;
}



/*
 * deal w 4 cases (interanl, leaf, lhs, rhs)
 * 
 * redistribute / rebalance == move kv from original page -> sibling page
 * If index == 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input "node".
 * 
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * 
 * 
 * NOTE:
 * no need to recurse == since no key deleted in parent 
 * hardest part == namespace 
 * u() parent, parent's parent n children's pointer when swapping
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {


  // 1. if sibling is successor (in your right)
  // rhs -> lhs
  MoveFirstToEndOf()

  // 2. if sibling is predecessor
  // lhs -> rhs
  MoveLastToFrontOf()
  

}




/*
 * deal w 2 cases of root 
 * 
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 * 
 * de
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
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
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  return INDEXITERATOR_TYPE();
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
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * simple traverse b+tree using key
 * 
 * 
 * leftMost == find the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
B_PLUS_TREE_LEAF_PAGE_TYPE *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key,
                                                         bool leftMost) {
    
  // 1. set up
  
  // 1.1 get b+ root page from disk 




  // 2. depends if root / internal / leaf, left to entry is smaller, right bigger
  // recusively hop til found the key in leaf node entry


  // 


  
  
  return nullptr;
}




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
  HeaderPage *header_page = static_cast<HeaderPage *>(
      buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record)
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  else
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for debug only
 * print out whole b+tree sturcture, rank by rank
 */
INDEX_TEMPLATE_ARGUMENTS
std::string BPLUSTREE_TYPE::ToString(bool verbose) { return "Empty tree"; }

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name,
                                    Transaction *transaction) {
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
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name,
                                    Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace cmudb
