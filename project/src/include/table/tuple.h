/**
 * tuple.h
 *
 * Tuple format:
 *  ------------------------------------------------------------------
 * | FIXED-SIZE or VARIED-SIZED OFFSET | PAYLOAD OF VARIED-SIZED FIELD|
 *  ------------------------------------------------------------------
 */

#pragma once

#include "catalog/schema.h"
#include "common/rid.h"
#include "type/value.h"

namespace cmudb {

class Tuple {
  friend class TablePage;

  friend class TableHeap;

  friend class TableIterator;

public:

  /* 5 constructors */
  // Default constructor (to create a dummy tuple)
  inline Tuple() : allocated_(false), rid_(RID()), size_(0), data_(nullptr) {}

  // constructor for table heap tuple
  Tuple(RID rid) : allocated_(false), rid_(rid) {}

  // constructor for creating a new tuple based on input value
  Tuple(std::vector<Value> values, Schema *schema);

  // construct w char* / string
  void DeserializeFrom(const char *storage);

  // copy constructor, deep copy
  Tuple(const Tuple &other);

  // assign operator, deep copy
  Tuple &operator=(const Tuple &other);

  ~Tuple() {
    if (allocated_)
      delete[] data_;
    allocated_ = false;
    data_ = nullptr;
  }



  /* getter, setter */

  inline bool IsAllocated() { return allocated_; }
  inline RID GetRid() const { return rid_; }  
  inline int32_t GetLength() const { return size_; }

  
  // Is the column value null ?
  inline bool IsNull(Schema *schema, const int column_id) const {
    Value value = GetValue(schema, column_id);
    return value.IsNull();
  }
  // col value
  Value GetValue(Schema *schema, const int column_id) const;
  // entire row == all columns 
  inline char *GetData() const { return data_; }  
  






  void SerializeTo(char *storage) const;

  std::string ToString(Schema *schema) const;




private:
  // return RAM addr of col value == ptr + offset 
  const char *GetDataPtr(Schema *schema, const int column_id) const;

  bool allocated_;
  RID rid_;        // if pointing to the table heap, the rid is valid
  int32_t size_;
  char *data_;
};

} // namespace cmudb
