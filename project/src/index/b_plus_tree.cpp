/**
 * b_plus_tree.cpp
 * 
 * 1. if conditions for 14 cases 
 * 2. abstract name for each case's combined leaf/internal nodes changes
 * (actual leaf/internal nodes changes are in b_plus_tree_leaf_page.cpp)
 * 
 * reference: 
 * https://www.cs.usfca.edu/~galles/visualization/BPlusTree.html 
 * https://www.programiz.com/dsa/insertion-on-a-b-plus-tree 
 * https://www.programiz.com/dsa/deletion-from-a-b-plus-tree
 * 
 * 
 * implementation of 3 APIs:
 * GetValue() / search() 
 * insert()
 * remove()
 * 
 * internal pages == direct search 
 * leaf pages == contain actual data
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 


B+ tree of degree m:

A node can have a maximum of m children. (i.e. 3)
A node can contain a maximum of m - 1 keys. (i.e. 2) == if less, then underflow
A node should have a minimum of ⌈m/2⌉ children. (i.e. 2) 
A node (except root node) should contain a minimum of ⌈m/2⌉ - 1 keys. (i.e. 1)


latching grabbing:
safe == no split, merge or redistribute 
search n insert n delete have their own version of latch grapping
delete == search version up to down + delete version down to up
delete == search version up to down + delete version down to up


Q&A:
why leaf / internal node is big enough to get a page ??
rationale for existence of a class n func ??


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

/*
 * traverse b+tree using key
 * 
 * latch grabbing ORDERING: 
 * - fetch() -> latch() -> r() or u() -> unlatch() -> unpin()
 * - child latch() -> parent unlatch()
 *
 * ORDERING: 
 * - fetch() -> latch() -> r() or u() -> unlatch() -> unpin()
 * - child latch() -> parent unlatch()
 * 
 * 
 * NOTE: 
 * leftMost == find the left most leaf page == for iterator 
 * op == 3 types latch coupling for search, insert/delete, iterate
 * txn == for concurrent btree r() w() since parent of all btrees objects
 * safe == no split, merge, rebalance == current size vs max Size
 * unpin == slowly flush to disk
 * unlatch == txn can r() or u() page
 * typecast to BPlusTreePage == 
 * 
 * while loop but not recursion ?? 
 * why index iterator == FindLeafPage ?? 
 * why would txn be nullptr ?? 
 */



INDEX_TEMPLATE_ARGUMENTS
B_PLUS_TREE_LEAF_PAGE_TYPE *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key,
                                                         bool leftMost,
                                                         Operation op, 
                                                         Transaction *transaction) {
  // assert(!EmptyTree());  

  // 1. get b+ root page from disk 
  auto *root_page = buffer_pool_manager_->FetchPage(root_page_id_); 
  LatchPage(root_page, op, txn);


  // 2. depends if root / internal / leaf,   
  // recusively hop til found the key in leaf node entry
  BPlusTreePage* curr_page = reinterpret_cast<BPlusTreePage *>(root_page->GetData());  
  
  while(!curr_page->Isleaf()){
        
    auto *curr_internal_page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t,
                                               KeyComparator> *>(curr_page);

    if(leftMost){ // traverse to leftmost child
      page_id_t child_page_id = curr_internal_page->GetValueByIndex(0);
    }else{
      page_id_t child_page_id = curr_internal_page->GetValueByKey(key, comparator_);
    }
    
    child_page = buffer_pool_manager_->FetchPage(child_page_id); 
    LatchPage(child_page, op, txn);
    
    curr_page = reinterpret_cast<BPlusTreePage *>(child_page->GetData());

    // KEY !!!!!!
    // if "safe", unlatch all grand parent pages this txn has latched
    // == isolate all potential layers that could be affected by split/merge/rebalance
    // if any child/grandchild still merge/rebalance/split, 
    // all grandparents will still be latched 
    if(op==Operation::SEARCH || 
      (op==Operation::INSERT && curr_page->GetSize() < curr_page->GetMaxSize())|| 
      (op==Operation::DELETE && curr_page->GetSize() > curr_page->GetMinSize())){
          
          // UnLatch(curr_internal_page);
          // buffer_pool_manager_->UnpinPage(curr_internal_page->GetPageId()); 

          UnLatchUnpinAllParents(child_page, txn, op);
      }
  }

  // found leaf page
  return curr_page;
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
 * 
 * recursion split logic
// split key == MUST BE 1st element of 2nd leaf
 * 
 * 
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value,
                            Transaction *transaction) {
  
  // CASE 1: 1st time insert 
  if(IsBTreeEmpty()){
    StartNewBPlusTree(key, value);
    return true;
    
  // CASE 2: every other insert 
  } else {
    bool success = InsertIntoTree();
    return success;              
  }
}

// 1st time insert 
// no need to latch since new page, diff txn gets diff new page
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewBPlusTree(const KeyType &key, const ValueType &value) {
  
  // 1. init root page
  auto *root_page = buffer_pool_manager_->NewPage(root_page_id_);
  if (root_page == nullptr) {
    throw Exception(EXCEPTION_TYPE_INDEX,
                    "all page are pinned while StartNewTree");
  }
  LatchPage(root_page, txn, Operation::INSERT);

  // 2. insert 
  auto root_leaf_page =
      reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType,
                                         KeyComparator> *>(root_page->GetData());
  root_leaf_page->Insert(key, value, comparator_);

  // 3. 
  UnLatchUnpinPage(root_page, txn, Operation::INSERT);
}


/*
 * shouldnt FindLeafPage() already all potentially affected page ?? 
 *
 * each helper == tricky part when insert btree
 * 1. FindLeafPage()
 * 2. Split()
 * 3. InsertIntoParent()
 * 4. UnLatchUnpinAllParents()
 * 5. UnLatchUnpinPage()
 * 
*/
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoBTree(const KeyType &key, const ValueType &value,
                                    Transaction *txn) {

  // 1. find leaf page + latch all "non safe" parents
  auto *page = FindLeafPage(key, NOT_LEFTMOST, Operation::INSERT, txn);
  auto leaf_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());

  // 2. if key already exists, exit 
  if (leaf_page->GetValueByKey(key, value, comparator_)){
    UnLatchUnpinAllParents(leaf_page);
    UnLatchUnpinPage(leaf_page);
    return false; // unsuccess 
  }

  // 3. insert {k,v} into leaf page

  // 3.1 if no split (not full)
  if (leaf_page->GetSize() < leaf_page->GetMaxSize()) { 
    leaf_page->Insert(key, value, comparator_);

    return true;


  // 3.2 if split (full)
  } else {
    
    auto *leaf_page2 = Split(leaf_page);
    auto mid_key = leaf_page2->GetValueByIndex(0); // (MUST BE)
    
    // 3.2.2 insert new page into existing pages chain
    if (comparator_(leaf_page->GetValueByIndex(0), leaf_page2->GetValueByIndex(0)) < 0) { // leaf1 1st k/v < leaf2 1st k/v
      // insert leaf2 in between leaf1 n leaf3
      leaf_page2->SetNextPageId(leaf_page->GetNextPageId()); 
      leaf_page->SetNextPageId(leaf_page2->GetPageId());
    } else { // will leaf 2 < leaf ever ??
      leaf_page2->SetNextPageId(leaf_page->GetPageId());
    }
  }

  // copy (not push) 1st key of 2nd half to parent  
  InsertIntoParent(leaf_page, mid_key, leaf_page2, txn);
  
  // decide which page to insert key into     
  if(comparator_(key, mid_key) < 0){ // if key < leaf2 1st k/v
    leaf_page->Insert(key, value, comparator_); // auto sort 
  } else {
    leaf_page2->Insert(key, value, comparator_);
  }
  
  // wait till all splits done by InsertIntoParent() 
  // no need to unlatch page by page from down to up after split
  // no txn can reach lower pages when upper are latched anyway
  UnLatchUnpinAllParents(leaf_page);
  UnLatchUnpinPage(leaf_page);
}

/*
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
    LatchPage(root_page, txn, Operation::INSERT);    
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
    // no need to latch == already latched in find leaf
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
      // auto *new_internal_parent_page = buffer_pool_manager_->NewPage();


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
    
    // must unpin after fetch
    buffer_pool_manager_->UnpinPage(old_node_parent_page->GetPageId(), true);
  }  
}


/*
mv 50% of node to new_node

typename N == could be internal or leaf 
*/
INDEX_TEMPLATE_ARGUMENTS
template <typename N> N *BPLUSTREE_TYPE::Split(N *old_page) {
    
    // 3.2.1 init new page
    page_id_t new_page_id;
    auto new_page = buffer_pool_manager_->NewPage(new_page_id);
    LatchPage(new_page, txn, Operation::INSERT);    
    N* new_btree_page = reinterpret_cast<N *>(new_page->GetData());
    new_btree_page->Init(new_page_id, INVALID_PAGE_ID);
    
    
    // 3.2.2 tranfer half of old -> new page 
    old_page->MoveHalfTo(new_btree_page, buffer_pool_manager_);
    split_count_ ++; // why ?? 
    
    return new_btree_page;
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
 * delete 1 kv from btree
 * (does not affect the same page in disk)
 * 
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
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
 * 1 case == 1 way nodes and edges are u()
 * 
 * Q&A 
 * why only coalesce gets to delete key in parent ?? 
 * - leaf level redistribute would have replaced key in parent 
 * - so no need to delete 
 * 
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {

  // 1. traverse btree, delete leaf key first
  auto *page = FindLeafPage(key, NOT_LEFTMOST, Operation::DELETE, txn);
  auto leaf_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
  int size_before_deletion = leaf_page->GetSize();
  int size_after_deletion = leaf_page->DeleteKeyFromPage(key, comparator_);
  
  // 2. 
  // key doesn't exist == do nothing
  if(size_after_deletion == size_before_deletion){
    
    
  // no need to recurse size == do nothing 
  } else if (size_after_deletion > leaf_page->GetMinSize()) {


  // need to recurse size 
  } else {
    
    
    
    // 14 cases
    // starting from leaf, recursively delete 
    DeleteKeyFromAllParents(leaf_page);

  }

  // 3. unlatch all parents n leaf (all 3 cases)
  UnLatchUnpinAllParents(leaf_page);
  UnLatchUnpinPage(leaf_page);

  // 4. garbage collect all empty merged pages
  for (auto it = txn->GetDeletedPageSet().begin(); 
      it < txn->GetDeletedPageSet().end(); ++it){
    buffer_pool_manager_->DeletePage(*it);
  }  
}

/*
 * KEY FUNCTION
 * 
 * recursive
 * start from leaf page at bottom, recurse up to delete every key
 * - decides 8 cases, then call its 8 sub functions to heavy lift
 * - merge and rebalance w sibling and parent (triangle), then recurse the same up 
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
 * @return: true == target leaf page deleted
 * false == no deletion happens
 * 
 * 
 * 
 * page == deleted node == you / this 
 * page == start as leaf, then replaced by parent internal 
 * 
 * 
 * TODO: 
 * adjust roots
 * 4 cases if enough space
 * 
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::DeleteKeyFromAllParents(N *deleted_page, Transaction *transaction) {
  
  // 0. 2 cases: base condition == reach root page
  if(deleted_page->IsRoot()){
    AdjustRoot(deleted_page);
    return true;
  }

  // 1. fetch parent n sibling page (use parent to get sibling)
  auto *parent = buffer_pool_manager->FetchPage(deleted_page->GetParentPageId());
  auto parent_page =
  reinterpret_cast<BPlusTreeInternalPage<KeyType, decltype(GetPageId()),
                                      KeyComparator> *>(parent->GetData());
                                      
  
  // Q&A: neigbour == right / left sibling ? always left, except if deleted node (page) is leftmost
  // why ? no reason, just a b+tree protocol everyone follows
  deleted_node_parent_pos = parent_page->GetIndexByValue(deleted_page->GetPageId());
  if(deleted_node_parent_pos = 0){ // leftmost
    sibling_pos = 1; // right sibling
  } else {
    sibling_pos = deleted_node_parent_pos - 1; // left sibling
  }
  sibling_page_id = parent_page->GetValueByIndex(sibling_pos);

  auto *sibling = buffer_pool_manager->FetchPage(sibling_page_id);
  auto sibling_page =
  reinterpret_cast<BPlusTreeInternalPage<KeyType, decltype(GetPageId()),
                                      KeyComparator> *>(sibling->GetData());
                                        

  // 2. 10 cases
  // 2.1 2 cases: page has more than half keys
  if((deleted_page->GetSize() * 2) > deleted_page->GetMaxSize()){ 
    
    // TODO:
    // delete key in leaf + parent + u() parent w rhs immediate child if key exists in parent
    // only non leftmost child 1st key == need to recurse to parent
    // leftmost child or not 1st key == no need to recurse  
    
    // never only internal delete == internal's key always exists in leaves (design of this implementation)
    // resurse to parent of internal ?? 
    // looks like the official version does not do this == key just left in parent


    return true;
  
  // 2.2 8 cases: page has less than half keys
  } else {
  
    // 2.2.1 if sibling > 50% keys 
    // 4 cases: leaf, non leaf, lhs, rhs 
    if((deleted_page->GetSize() + sibling_page->GetSize()) > deleted_page->GetMaxSize()){ 
      Redistribute<N>(deleted_page, sibling_page, deleted_node_parent_pos);
      return true;

    // 2.2.2 if sibling < 50% keys 
    // 4 cases: leaf, non leaf, lhs, rhs 
    // Q&A: why leftmost ? deleted merges into untouched ? or rhs to lhs ?
    // parent goes to deleted node, could be either right or left
    // then rhs merges into left (code seems easier)
    } else {
      Coalesce<N>(deleted_page, sibling_page, parent_page, deleted_node_parent_pos, transaction) 
      if(deleted_node_parent_pos == 0){ // leftmost == no more recurse
        return true;
      }
    }
  }

  // 4. 
  // if key exists, delete key + process
  // if key doesnt exist, stops recursion
  if(parent_page->KeyExists()){
    DeleteKeyFromAllParents(parent_page, transaction);
  } else {
    return true;
  }  
  return false; 
}







/*
 * deals w 2 cases + recurse 
 *
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(
    N *&deleted_node, N *&untouched_node,
    BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *&parent,
    int deleted_node_parent_pos, Transaction *transaction) {

  deleted_node->MoveAllTo(untouched_node, deleted_node_parent_pos, buffer_pool_manager_);

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
 * 
 * deleted page == recipent 
 * untouched node == sender
 * 
 * NOTE:
 * no need to recurse == since no key deleted in parent 
 * hardest part == namespace 
 * u() parent, parent's parent n children's pointer when swapping
 * not passing in parent page == optimization == unpin parent page as soon as finish
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *recipient_node, N *sender_node, int recipient_node_parent_pos, int sibling_pos) {

  // 1. if deleted / recipient node on left == lhs <- rhs
  if(recipient_node_parent_pos == 0){ // recipient on leftmost
    recipient_node->MoveFirstToEndOf(sender_node, buffer_pool_manager_);

  } else {

    // 2. if deleted / recipient node on right, lhs -> rhs
    // node == deleted page == recipent 
    recipient_node->MoveLastToFrontOf(sender_node, recipient_node_parent_pos, buffer_pool_manager)
  }
}




/*
 * deal w 2 cases of root 
 * if root still has > 1 key == CASE: internal/leaf, enough keys
 * 
 * true == root page deleted
 * false == no deletion
 * 
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  
  
  // CASE 1: delete the last element in whole b+ tree
  if (old_root_node->IsLeafPage()) {
    if(old_root_node->GetSize() == 0){ 
      root_page_id_ = INVALID_PAGE_ID;
      UpdateRootPageId(false);
      return true;
    }    
    return false; // root still has at least 1 key (no child)
  
  // ?? 
  // CASE 2: top layer all keys deleted == leftmost child becomes new root
  // delete the last element in root page, but root still has one last child
  } else {
    // why 1 not 0 ? root has key to be deleted in this recursion
    // NOTE: recursion only happens if parent has key
    if(old_root_node->GetSize() == 1){ 

      // get leftmost child of root
      auto old_root_page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(old_root_node);
      root_page_id_ = old_root_page->GetValueByIndex(0); // global var

      // u() leftmost child to be new root 
      auto root_leftmost_child = buffer_pool_manager_->FetchPage(root_page_id_);
      auto root_leftmost_child_page = reinterpret_cast<BPlusTreePage *>(root_leftmost_child->GetData());
      root_leftmost_child_page->SetParentPageId(INVALID_PAGE_ID);
      UpdateRootPageId(false); // false == replace instead of insert, u() root page id in "header page"

      buffer_pool_manager_->UnpinPage(root_leftmost_child_id, true);

      assert(root_leftmost_child_id == root_leftmost_child_page->GetPageId());
      
      return true;
    }
    
    return false; // root still has at least 2 keys (ye child)
  }


  
  
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
 * 
 * 4th API to iterate thru index n leaf page 1 by 1 from left to right
 * (once reach last index of 1 page, jump on the right neigbour start from 1st index)
 * (used by testing code)
 * 
 * caller to index_iterator class
 * 
 * iterator == pointer at vector's item 
 * 
 * e.g.
 * vector::iterator<int> iter = vector<int> vec_int.Begin();
 * for(iter = vec_int.Begin(); iter < vec_int.End(); iter ++)
 * cout << *iter
 * advance(iter, 4) next(iter, 10) prev(iter, 20)
 *****************************************************************************/
/*
 * find the leaftmost leaf page first, then construct
 * 
 * @return : index iterator
 * 
 * REFERENCE: 
 * TableIterator TableHeap::begin()
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() { 

  // index iterator class constructor 

  FindLeafPage(); // leftmost leaf page 

  return INDEXITERATOR_TYPE(); 
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  return INDEXITERATOR_TYPE();
}





/*****************************************************************************
 * HELPERS
 *****************************************************************************/

// TODO:
UnlockUnpinPages();




INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::LatchPage(Page* page, Transaction* txn, Operation op){
    if(op == Operation::SEARCH){
        page->RLatch();
    }else{
        page->WLatch();
    }
    if(txn != nullptr) // testing on just btree == no txn 
        txn->GetPageSet()->push_back(page);
}


/**
 * how to check if page is latched ?? 
 * why unpin ?? 
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnLatchUnpinPage(Page* page, Transaction* txn, Operation op){
    if(op == Operation::SEARCH){
        page->RUnLatch();
        buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    }else{
        page->WUnLatch();
        buffer_pool_manager_->UnpinPage(page->GetPageId(), true);       
    }
    if(txn != nullptr)
        txn->GetPageSet()->pop_front(page);
}


INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnlockPage(Page* page, Transaction* txn, Operation op){
    if(page->GetPageId() == root_page_id_){
        UnlockRoot();
    }
    if(op == Operation::SEARCH){
        page->RUnlatch();
        buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    }else{
        page->WUnlatch();
        buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    }
    if(txn != nullptr)
        txn->GetPageSet()->pop_front();
}




/*
 Used while removing element and the target leaf node is deleted 
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnlockAllPage(Transaction* txn, Operation op){
    if(txn == nullptr) return;

    while(!txn->GetPageSet()->empty()){

        auto front = txn->GetPageSet()->front();
        if(front->GetPageId() != INVALID_PAGE_ID){
            if(op == Operation::SEARCH){
                front->RUnlatch();
                buffer_pool_manager_->UnpinPage(front->GetPageId(), false);
            }else{
                if(front->GetPageId() == root_page_id_){
                    UnlockRoot();
                }
                front->WUnlatch(); 
                buffer_pool_manager_->UnpinPage(front->GetPageId(), true);
            }
        }
        txn->GetPageSet()->pop_front();
    }
}

/*
 If current node is safe, all the lock held by parent can be released
 Safe condition:
    Current size < Max Size
  
  why so diff to unlatch parent page ?? 
  parent might not be your immediate parent but great grand parent
  == since this txn might insert/delete that affects several layers
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnlockParentPage(Page* page, Transaction* txn, Operation op){
    
    // turn into assert() ?? 
    if(txn == nullptr) return;
    if(txn->GetPageSet()->empty()) return;


    if(page->GetPageId() == INVALID_PAGE_ID){
        UnlockAllPage(txn, op);
    
    }else{
        
        // release 1 by 1, up to down, i.e. greatgrand -> grand -> dad
        // pageset == all pages a txn has latched, when doing 1 insert/delete
        while(!txn->GetPageSet()->empty() && 
              txn->GetPageSet()->front()->GetPageId() 
              != page->GetPageId()){
            
            auto front = txn->GetPageSet()->front();


            if(front->GetPageId() != INVALID_PAGE_ID){
                if(op == Operation::SEARCH){
                    front->RUnlatch();
                    buffer_pool_manager_->UnpinPage(front->GetPageId(), false);
                }else{
                    if(front->GetPageId() == root_page_id_){
                        UnlockRoot();
                    }
                    front->WUnlatch(); 
                    buffer_pool_manager_->UnpinPage(front->GetPageId(), true);
                }
            }


            txn->GetPageSet()->pop_front();
        }
    }
}


// 
/**
 * TODO: corner cases 
 * 
 * release 1 by 1, up to down, i.e. greatgrand -> grand -> dad
 * pageset == all pages a txn has latched, when doing 1 insert/delete
 */
UnLatchUnpinAllParents(Page* child_page, Transaction* txn, Operation op){

  while(!txn->GetPageSet()->IfEmpty() &&
          txn->GetPageSet()->front()->GetPageId()
          != child_page->GetPageId()){
      
      auto *FIFO_page = txn->GetPageSet()->front();
      UnLatch(FIFO_page);
      buffer_pool_manager_->UnpinPage(FIFO_page->GetPageId()); 
      
      txn->GetPageSet()->pop_front();
  }



}


/*
 * called whenever root_page_id_ is u()
 * 
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * 
 * @parameter: insert_record      defualt value is false. When set to true,
 * 
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 * 
 * insert_record == true/false == replace / insert 
 * 
 * TODO: 
 * index_name_ == ?? 
 * root_page_id_ == ?? 
 * when were they u() ??
 * what is header page for ??
 * 
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  
  if (insert_record) // insert new 
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  
  else // replace old 
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




INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnlockPage(Page* page, Transaction* txn, Operation op){
    if(page->GetPageId() == root_page_id_){
        UnlockRoot();
    }
    if(op == Operation::SEARCH){
        page->RUnlatch();
        buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    }else{
        page->WUnlatch();
        buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    }
    if(txn != nullptr)
        txn->GetPageSet()->pop_front();
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
 * FOR DEBUGGING
`1 *****************************************************************************/

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const {
    return root_page_id_ == INVALID_PAGE_ID;
}