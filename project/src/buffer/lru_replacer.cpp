/**
 * FIFO queue linked list 
 * 
 * buffer pool manager maintain a LRU linked list to index
 * all the pages that are unpinned and ready to be swapped.
 * 
 * dequeue or enqueue pages when unpinned -> pinned, or vice-versa.
 * 
 */
#include "buffer/lru_replacer.h"
#include "page/page.h"

namespace cmudb {

template <typename T> LRUReplacer<T>::LRUReplacer() {}
template <typename T> LRUReplacer<T>::~LRUReplacer() {}


// NOTE: value == page's ptr NOT page_id !!!!
// page's ptr is direct, no need to query page table again !!!!
template <typename T> void LRUReplacer<T>::Insert(const T &value) {

  // 1. page's ptr -> linked list pos
  auto linked_list_iterator = map_of_pageptr_iter.find(value);

  // 2. if page does not exist in RAM yet, append at front + u() unordered map
  if (linked_list_iterator == map_of_pageptr_iter.end()) {
    linked_list_of_pageptr.insert(linked_list_of_pageptr.begin(), value);
    map_of_pageptr_iter[value] = linked_list_of_pageptr.begin();


  // 3. if page already exists, move yourself to front + u() unordered map
  } else {
    
    linked_list_of_pageptr.erase(linked_list_iterator);

    linked_list_of_pageptr.insert(linked_list_of_pageptr.begin(), value);
    map_of_pageptr_iter[value] = linked_list_of_pageptr.begin();
  }
}





// pop 1st from linked list 
template <typename T> bool LRUReplacer<T>::Victim(T &value) {
    
  // 1. if no page cached in RAM at all, return false
  if (linked_list_of_pageptr.empty()){
    return false;
  }
  
  // 2. If LRU is non-empty, pop the front + return true 
  value = linked_list_of_pageptr.rbegin();
  linked_list_of_pageptr.popback();
  map_of_pageptr_iter.erase(value);
  return true;
}





// delete any node from linked list
template <typename T> bool LRUReplacer<T>::Erase(const T &value) {

  // 1. page's ptr -> addr of linked list node that stores page's ptr
  auto linked_list_iterator = map_of_pageptr_iter.find(value);

  // 2. if does not exist, just return false
  if (linked_list_iterator == map_of_pageptr_iter.end()) {
    return false;
  }
  
  // 3. if exist, delete from linked list + unordered map + return true 
  linked_list_of_pageptr.erase(linked_list_iterator);
  map_of_pageptr_iter.erase(value);
  
  return true;
}





template <typename T> size_t LRUReplacer<T>::Size() { 

  return map_of_pageptr_iter.size(); 
}


//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////




template class LRUReplacer<Page *>;
// test only
template class LRUReplacer<int>;

} // namespace cmudb
