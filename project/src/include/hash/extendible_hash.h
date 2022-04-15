#pragma once

#include <cstdlib>
#include <vector>
#include <string>

#include "hash/hash_table.h"

namespace cmudb {

template <typename K, typename V>
class ExtendibleHash : public HashTable<K, V> {

struct Bucket {
  
  // Bucket() = default;
  // explicit Bucket(size_t i, int d) : id(i), depth(d) {}
  
  Bucket(int depth) : local_depth(depth) {}  
  int id;
  int local_depth;
  std::map<K, V> map_of_kv;
}




public:
  // constructor
  ExtendibleHash(size_t size);
  // helper function to generate hash addressing
  size_t HashKey(const K &key);
  // helper function to get global & local depth
  int GetGlobalDepth() const;
  int GetLocalDepth(int bucket_id) const;
  int GetNumBuckets() const;
  // lookup and modifier
  bool Find(const K &key, V &value) override;
  bool Remove(const K &key) override;
  void Insert(const K &key, const V &value) override;

private:

  int global_depth;
  const size_t MAX_BUCKET_SIZE;
  // size_t bucket_size;
  std::vector<std::shared_ptr<Bucket>> global_directory; // vertically split 0,1s 


};
} // namespace cmudb
