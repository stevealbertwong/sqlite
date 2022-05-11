/**
 * tuple.cpp
 * 
 * 
 * 
 * tuple 
 * == 1 row of table
 * == schema + vector of values 
 * == columns of table (data type) specified by user
 * 
 * record id 
 * == disk addr of tuple 
 * == page_id + offset/slot
 * 
 */

#include <cassert>
#include <cstdlib>
#include <sstream>

#include "common/logger.h"
#include "table/tuple.h"

namespace cmudb {

//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////

/* 4 types of contructors */


// construct w vector n schema 
Tuple::Tuple(std::vector<Value> values, Schema *schema) : allocated_(true) {
  assert((int)values.size() == schema->GetColumnCount());

  // step1: calculate size of the tuple
  int32_t tuple_size = schema->GetLength();
  for (auto &i : schema->GetUnlinedColumns())
    tuple_size += (values[i].GetLength() + sizeof(uint32_t));
  // allocate memory using new, allocated_ flag set as true
  size_ = tuple_size;
  data_ = new char[size_];

  // step2: Serialize each column(attribute) based on input value
  int column_count = schema->GetColumnCount();
  int32_t offset = schema->GetLength();
  for (int i = 0; i < column_count; i++) {
    if (!schema->IsInlined(i)) {
      // Serialize relative offset, where the actual varchar data is stored
      *reinterpret_cast<int32_t *>(data_ + schema->GetOffset(i)) = offset;
      // Serialize varchar value, in place(size+data)
      values[i].SerializeTo(data_ + offset);
      offset += (values[i].GetLength() + sizeof(uint32_t));
    } else {
      values[i].SerializeTo(data_ + schema->GetOffset(i));
    }
  }
}



// construct w char*
void Tuple::DeserializeFrom(const char *storage) {
  
  // first 32 bits/8 bytes == size ?? 
  uint32_t size = *reinterpret_cast<const int32_t *>(storage);
  
  // char* -> tuple object
  // this == tuple object you parse char* into
  this->size_ = size;
  if (this->allocated_)
    delete[] this->data_;
  this->data_ = new char[this->size_];
  memcpy(this->data_, storage + sizeof(int32_t), this->size_);
  this->allocated_ = true;
}



// Copy constructor
Tuple::Tuple(const Tuple &other)
    : allocated_(other.allocated_), rid_(other.rid_), size_(other.size_) {
  
  
  // deep copy == 2 copies in RAM
  if (allocated_ == true) {
    // LOG_DEBUG("tuple deep copy");
    data_ = new char[size_];
    memcpy(data_, other.data_, size_);
  
  // shallow copy == 2 share same copy in RAM
  } else {
    // LOG_DEBUG("tuple shallow copy");
    data_ = other.data_;
  }
}



// Copy constructor v2 == assignment constructor
Tuple &Tuple::operator=(const Tuple &other) {
  allocated_ = other.allocated_;
  rid_ = other.rid_;
  size_ = other.size_;


  // deep copy
  if (allocated_ == true) {
    // LOG_DEBUG("tuple deep copy");
    data_ = new char[size_];
    memcpy(data_, other.data_, size_);
  
  // shallow copy 
  } else {
    // LOG_DEBUG("tuple shallow copy");
    data_ = other.data_;
  }

  return *this;
}


//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////

// Get the value of a specified column (const)
Value Tuple::GetValue(Schema *schema, const int column_id) const {
  assert(schema);
  assert(data_);
  const TypeId column_type = schema->GetType(column_id);
  const char *data_ptr = GetDataPtr(schema, column_id);


  // the third parameter "is_inlined" is unused
  // not the above DeserializeFrom() 
  return Value::DeserializeFrom(data_ptr, column_type);
}






// return RAM addr of col value == ptr + offset 
const char *Tuple::GetDataPtr(Schema *schema, const int column_id) const {
  assert(schema);
  assert(data_);

  
  // for inline type, data are stored where they are
  bool is_inlined = schema->IsInlined(column_id);
  if (is_inlined)
    return (data_ + schema->GetOffset(column_id));
  
  else {
    // step1: read relative offset from tuple data
    int32_t offset =
        *reinterpret_cast<int32_t *>(data_ + schema->GetOffset(column_id));
    // step 2: return beginning address of the real data for VARCHAR type
    return (data_ + offset);
  }
}



// tuple -> char* (debug only)
std::string Tuple::ToString(Schema *schema) const {
  std::stringstream os;

  int column_count = schema->GetColumnCount();
  bool first = true;
  os << "(";
  for (int column_itr = 0; column_itr < column_count; column_itr++) {
    if (first) {
      first = false;
    } else {
      os << ", ";
    }
    if (IsNull(schema, column_itr)) {
      os << "<NULL>";
    } else {
      Value val = (GetValue(schema, column_itr));
      os << val.ToString();
    }
  }
  os << ")";
  os << " Tuple size is " << size_;

  return os.str();
}




// tuple -> char*
void Tuple::SerializeTo(char *storage) const {
  memcpy(storage, &size_, sizeof(int32_t)); // 1st 4 bytes == tuple size
  memcpy(storage + sizeof(int32_t), data_, size_); // all remaining bytes == data/payload
}





} // namespace cmudb
