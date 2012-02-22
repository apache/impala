// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_RUNTIME_TUPLE_H
#define IMPALA_RUNTIME_TUPLE_H

#include <cstring>
#include <glog/logging.h>
#include "runtime/descriptors.h"
#include "runtime/mem-pool.h"

namespace impala {

struct StringValue;
class TupleDescriptor;

// A tuple is stored as a contiguous sequence of bytes containing a fixed number
// of fixed-size slots. The slots are arranged in order of increasing byte length;
// the tuple might contain padding between slots in order to align them according
// to their type.
// 
// The contents of a tuple:
// 1) a number of bytes holding a bitvector of null indicators
// 2) bool slots
// 3) tinyint slots
// 4) smallint slots
// 5) int slots
// 6) float slots
// 7) bigint slots
// 8) double slots
// 9) string slots
class Tuple {
 public:
  // initialize individual tuple with data residing in mem pool
  static Tuple* Create(int size, MemPool* pool) {
    // assert(size > 0);
    Tuple* result = reinterpret_cast<Tuple*>(pool->Allocate(size));
    result->Init(size);
    return result;
  }

  void Init(int size) {
    bzero(this, size);
  }

  // Create a copy of 'this', including all of its referenced string data,
  // using pool to allocate memory. Returns the copy.
  // If 'convert_ptrs' is true, converts pointers that are part of the tuple
  // into offsets in 'pool'.
  Tuple* DeepCopy(const TupleDescriptor& desc, MemPool* pool, bool convert_ptrs = false);

  // Create a copy of 'this', including all its referenced string data.  This
  // version does not allocate a tuple, instead copying 'dst'.  dst must already 
  // be allocated to the correct size (desc.byte_size())
  // If 'convert_ptrs' is true, converts pointers that are part of the tuple
  // into offsets in 'pool'.
  void DeepCopy(Tuple* dst, const TupleDescriptor& desc, MemPool* pool,
                bool convert_ptrs = false);

  // Turn null indicator bit on.
  void SetNull(const NullIndicatorOffset& offset) {
    DCHECK(offset.bit_mask != 0);
    char* null_indicator_byte = reinterpret_cast<char*>(this) + offset.byte_offset;
    *null_indicator_byte |= offset.bit_mask;
  }

  // Turn null indicator bit off.
  void SetNotNull(const NullIndicatorOffset& offset) {
    char* null_indicator_byte = reinterpret_cast<char*>(this) + offset.byte_offset;
    *null_indicator_byte &= ~offset.bit_mask;
  }

  bool IsNull(const NullIndicatorOffset& offset) const {
    const char* null_indicator_byte = reinterpret_cast<const char*>(this) + offset.byte_offset;
    return (*null_indicator_byte & offset.bit_mask) != 0;
  }

  void* GetSlot(int offset) {
    DCHECK(offset != -1); // -1 offset indicates non-materialized slot
    return reinterpret_cast<char*>(this) + offset;
  }

  const void* GetSlot(int offset) const {
    DCHECK(offset != -1);  // -1 offset indicates non-materialized slot
    return reinterpret_cast<const char*>(this) + offset;
  }

  StringValue* GetStringSlot(int offset) {
    DCHECK(offset != -1);  // -1 offset indicates non-materialized slot
    return reinterpret_cast<StringValue*>(reinterpret_cast<char*>(this) + offset);
  }

 private:
  void* data_;
};

}

#endif
