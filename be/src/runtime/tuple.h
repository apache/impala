// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


#ifndef IMPALA_RUNTIME_TUPLE_H
#define IMPALA_RUNTIME_TUPLE_H

#include <cstring>
#include "common/logging.h"
#include "gutil/macros.h"
#include "runtime/descriptors.h"
#include "runtime/mem-pool.h"

namespace impala {

struct StringValue;
class TupleDescriptor;
class TupleRow;

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
//
// A tuple with 0 materialised slots is represented as NULL.
class Tuple {
 public:
  // initialize individual tuple with data residing in mem pool
  static Tuple* Create(int size, MemPool* pool) {
    if (size == 0) return NULL;
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

  // Create a copy of 'this', including all referenced string data, into
  // data. The tuple is written first, followed by any strings. data and offset
  // will be incremented by the total number of bytes written. data must already
  // be allocated to the correct size.
  // If 'convert_ptrs' is true, converts pointers that are part of the tuple
  // into offsets in data, based on the provided offset. Otherwise they will be
  // pointers directly into data.
  void DeepCopy(const TupleDescriptor& desc, char** data, int* offset,
                bool convert_ptrs = false);

  // Materialize this by evaluating the expressions in materialize_exprs
  // over the specified 'row'. 'pool' is used to allocate var-length data.
  // (Memory for this tuple itself must already be allocated.)
  // If collect_string_vals is true, the materialized non-NULL string value
  // slots and the total length of the string slots are returned in var_values
  // and total_var_len.
  template <bool collect_string_vals>
  void MaterializeExprs(
      TupleRow* row, const TupleDescriptor& desc,
      const std::vector<ExprContext*>& materialize_expr_ctxs, MemPool* pool,
      std::vector<StringValue*>* non_null_var_len_values = NULL,
      int* total_var_len = NULL);

  // Turn null indicator bit on. For non-nullable slots, the mask will be 0 and
  // this is a no-op (but we don't have to branch to check is slots are nulalble).
  void SetNull(const NullIndicatorOffset& offset) {
    char* null_indicator_byte = reinterpret_cast<char*>(this) + offset.byte_offset;
    *null_indicator_byte |= offset.bit_mask;
  }

  // Turn null indicator bit off.
  void SetNotNull(const NullIndicatorOffset& offset) {
    char* null_indicator_byte = reinterpret_cast<char*>(this) + offset.byte_offset;
    *null_indicator_byte &= ~offset.bit_mask;
  }

  bool IsNull(const NullIndicatorOffset& offset) const {
    const char* null_indicator_byte =
        reinterpret_cast<const char*>(this) + offset.byte_offset;
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

  // For C++/IR interop, we need to be able to look up types by name.
  static const char* LLVM_CLASS_NAME;

 private:
  DISALLOW_COPY_AND_ASSIGN(Tuple);
};

}

#endif
