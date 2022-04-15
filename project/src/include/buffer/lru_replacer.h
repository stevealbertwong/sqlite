#pragma once

#include "buffer/replacer.h"
#include "hash/extendible_hash.h"

namespace cmudb {

template <typename T> class LRUReplacer : public Replacer<T> {
public:
  // do not change public interface
  LRUReplacer();

  ~LRUReplacer();

  void Insert(const T &value);

  bool Victim(T &value);

  bool Erase(const T &value);

  size_t Size();




private:  

  /* 
  unordered_map == for quick lookup of linked list
  
  k/T: page's ptr, v: iterator of linked list
  
  unordered_map == hash map 
  iterator == ptr to page's ptr == addr of linked list node that stores page's ptr
  */
  std::list<T> linked_list_of_pageptr;
  std::unordered_map<T, std::list<T>::iterator> map_of_pageptr_iter; 

  
};

} // namespace cmudb
