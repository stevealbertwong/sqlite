/**
 * iterate tree pages (NOT kv within 1 page)
 * 
 * 
 * used only in b+tree testing code 
 * 
 */
#include <cassert>

#include "index/index_iterator.h"

namespace cmudb {


// constructor 
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() {
    self.buffer_pool_manager_ = buffer_pool_manager_;
    self.curr_leaf_index_ = curr_leaf_index_;
    self.curr_leaf_ = curr_leaf_;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {

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
 * operator overloading 
 *****************************************************************************/

// deference *iter
const MappingType &operator*() {}



/*
++ == loop thru index, if reach end, jump to the right leaf

REFERENCE: 
TableIterator &TableIterator::operator++()

NOTE:
latch grabbing == prevents page to be split/merged/rebalanced
but still could deadlock when multi txns compete same page latch
*/
IndexIterator<KeyType, ValueType, KeyComparator>
IndexIterator &operator++() {
    
    // 1
    ++curr_leaf_index_;
    
    // 2. if end of index, jump to the right leaf
    if(curr_leaf_index_  == curr_leaf_->GetSize()){
        
        // 2.1 finish == if end of all leaf pages 
        if(curr_leaf_->GetNextPageId() == INVALID_PAGE_ID){
            return 
        }
                        
        // 2.2 get next page
        Page *right_neigbour_page = buffer_pool_manager_->FetchPage(curr_leaf_->GetNextPageId());
        auto *right_neigbour_leaf_page =  reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType,
                                           KeyComparator> *>(right_neigbour_page->GetData());
        
        // 2.3 latch next page
        right_neigbour_page.RLatch();


        // 2.4 unlatch curr page
        // why fetch first ?? must already been pinned ?? 
        curr_leaf_->RUnlatch(); 


        // 2.5 unpin curr pages
        // why unpin twice ?? 
        buffer_pool_manager_->UnpinPage(curr_leaf_->GetPageId(), false);
        

        // 2. u() curr to next
        curr_leaf_ = right_neigbour_leaf_page;
        curr_leaf_index_ = 0;        
    }

    return *this; // a newly updated index iterator class 
}


bool isEnd(){

}



template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;
template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;
template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;
template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace cmudb
