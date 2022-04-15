/*
 * reference:
 * https://www.youtube.com/watch?v=cmneVtDrhAA - uw data struct class
 * 
 */
#include <list>

#include "hash/extendible_hash.h"
#include "page/page.h"

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
 * constructor
 * 
 * size == fixed array size for each bucket
 * 
 */
template <typename K, typename V> // kv can be any type
ExtendibleHash<K, V>::ExtendibleHash(size_t size) {
  
  // 1. 


  // 2. 

  
}


// hash func 
template <typename K, typename V>
size_t ExtendibleHash<K, V>::HashKey(const K &key) {

  return std::hash<K>()(key);
}

template <typename K, typename V>
int ExtendibleHash<K, V>::GetGlobalDepth() const {

  return global_depth;
}


template <typename K, typename V>
int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const {
  return 0;
}

/*
 * helper function to return current number of bucket in hash table
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetNumBuckets() const {
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



template <typename K, typename V>
bool ExtendibleHash<K, V>::Find(const K &key, V &value) {
  
  // 1. find ptr to bucket from global dir w binary key  
  size_t hashed_key = HashKey(key);
  int global_depth = GetGlobalDepth();
  int global_dir_mask = ((1 << global_depth) - 1); // e.g. 1 << 4 - 1 == 10000 - 1 == 1111
  int global_dir_index = (global_dir_mask & hashed_key);  
  std::shared_ptr<Bucket> dest_bucket = global_directory[global_dir_index];]


  // 2. return whether success + value
  // NOTE: C++ does not have an official way to return multiple values from a function
  if (dest_bucket == NULL){
    // if (bucket == nullptr || bucket->contents.find(key) == bucket->contents.end()) { ??
    return false;
  }
  value = dest_bucket->map_of_kv[key];
  return true;
}

/*
 * delete <key,value> entry in hash table
 *
 * NOTE: Shrink & Combination is not required for this project
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Remove(const K &key) {

  // 1. find ptr to bucket from global dir w binary key  
  size_t hashed_key = HashKey(key);
  int global_depth = GetGlobalDepth();
  int global_dir_mask = ((1 << global_depth) - 1); // e.g. 1 << 4 - 1 == 10000 - 1 == 1111
  int global_dir_index = (global_dir_mask & hashed_key);  
  std::shared_ptr<Bucket> dest_bucket = global_directory[global_dir_index];]


  // 2. return whether success + value
  // NOTE: C++ does not have an official way to return multiple values from a function
  if (dest_bucket == NULL){
    return false;
  }
  dest_bucket->map_of_kv.erase(key);
  return true;
}

/*
 * MOST DIFF !!!!
 * 
 * corner cases:
 * - bucket still full after split, keep splitting till not full
 */
template <typename K, typename V>
void ExtendibleHash<K, V>::Insert(const K &key, const V &value) {

  // 1. find ptr to bucket from global dir w binary key  
  size_t hashed_key = HashKey(key);
  int global_depth = GetGlobalDepth();
  int global_dir_mask = ((1 << global_depth) - 1); // e.g. 1 << 4 - 1 == 10000 - 1 == 1111
  int global_dir_index = (global_dir_mask & hashed_key);  
  std::shared_ptr<Bucket> dest_bucket = global_directory[global_dir_index];
  

  // 2. if not full, just append at end
  // 3. if designated bucket is full
  while( dest_bucket->local_depth == MAX_BUCKET_SIZE ){

    // 3.1 if local depth == global depth -> split dir + bucket
    if(global_depth == dest_bucket->local_depth){

      // ++global depth and double num of dir entries
      // u() each new dir entry to double point to existing buckets      
      global_depth++;      
      for (size_t i = 0; i < global_directory.size(); i++){
        global_directory.push_back(global_directory[i]);
      }

      // then 3.2 split + 4. append      
    }
    
    // 3.2 if global depth > local depth  -> only split bucket
    
    // 3.2.1 init new bucket
    std::shared_ptr<typename ExtendibleHash<K, V>::Bucket> new_bucket = std::make_shared<Bucket>(MAX_BUCKET_SIZE);
    new_bucket->id = dest_bucket->id * 2; // 0010 -> 1010 
    dest_bucket->local_depth ++;
    new_bucket->local_depth = dest_bucket->local_depth;

    int local_dir_mask = (1 << dest_bucket->local_depth); // e.g. 1 << 4 == 10000
    


    // 3.2.2 loop thru local bucket,  mv matches to new bucket
    for kv : dest_bucket->map_of_kv {
      // int split_bucket_id = local_dir_mask & key;    
      // if split_bucket_id == new_bucket->id{
      //   bool move_new_bucket = true;}
      
      bool new_bucket_bit_on = local_dir_mask & HashKey(kv.first()); // 0xxx vs 1xxx after split

      if(new_bucket_bit_on){
        new_bucket->map_of_kv.insert(kv);
        dest_bucket->map_of_kv.delete(kv);
      }
    }
    
    // 3.2.3 u() dir to point to new bucket
    for (size_t i = 0; i < global_directory.size(); i++){
      // NOTE: global dir entries always >= num local dirs
      // e.g. 1101 (global depth 4) == 0101, 1101 (local depth 3)
      // 101 ->  0101, 1101 changes to 1101 -> 1101
      int global_match_to_local_mask = ((1 << new_bucket->local_depth) - 1); 
      if( i && global_match_to_local_mask == new_bucket->id ){  // 1101 = 1101 
        global_directory[i] = new_bucket; // old bucket -> new bucket 
      }
    }

  }

  // 4. all 3 cases append kv at end 
  new_bucket->map_of_kv.insert(key) = value;

}



template class ExtendibleHash<page_id_t, Page *>;
template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
// test purpose
template class ExtendibleHash<int, std::string>;
template class ExtendibleHash<int, std::list<int>::iterator>;
template class ExtendibleHash<int, int>;
} // namespace cmudb
