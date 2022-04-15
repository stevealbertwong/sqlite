/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "page/b_plus_tree_leaf_page.h"

namespace cmudb {

#define INDEXITERATOR_TYPE                                                     \
  IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
public:
  // you may define your own constructor based on your member variables
  IndexIterator();
  ~IndexIterator();

  bool isEnd();

  const MappingType &operator*();

  IndexIterator &operator++();



private:
  
  BufferPoolManager *buffer_pool_manager_; // one and only. here since needs its func
  BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *curr_leaf_; // leaf page iterated this round
  int curr_leaf_index_; // index of leaf page iterated this round

};

} // namespace cmudb
