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

struct CollectionValue;
struct StringValue;
class TupleDescriptor;
class TupleRow;

/// A tuple is stored as a contiguous sequence of bytes containing a fixed number
/// of fixed-size slots. The slots are arranged in order of increasing byte length;
/// the tuple might contain padding between slots in order to align them according
/// to their type.
//
/// The contents of a tuple:
/// 1) a number of bytes holding a bitvector of null indicators
/// 2) bool slots
/// 3) tinyint slots
/// 4) smallint slots
/// 5) int slots
/// 6) float slots
/// 7) bigint slots
/// 8) double slots
/// 9) string slots
//
/// A tuple with 0 materialised slots is represented as NULL.
///
/// TODO: Our projection of collection-typed slots breaks/augments the conventional
/// semantics of the null bits, because we rely on producers of array values to also
/// set the slot value in addition to the null bit. We should address this issue with
/// a proper projection that restores the intended (original) null bit semantics.
/// See also UnnestNode for details on the projection.
class Tuple {
 public:
  /// initialize individual tuple with data residing in mem pool
  static Tuple* Create(int size, MemPool* pool) {
    if (size == 0) return NULL;
    Tuple* result = reinterpret_cast<Tuple*>(pool->Allocate(size));
    result->Init(size);
    return result;
  }

  void Init(int size) {
    bzero(this, size);
  }

  /// The total size of all data represented in this tuple (tuple data and referenced
  /// string and collection data).
  int64_t TotalByteSize(const TupleDescriptor& desc) const;

  /// The size of all referenced string and collection data.
  int64_t VarlenByteSize(const TupleDescriptor& desc) const;

  /// Create a copy of 'this', including all of its referenced variable-length data
  /// (i.e. strings and collections), using pool to allocate memory. Returns the copy.
  Tuple* DeepCopy(const TupleDescriptor& desc, MemPool* pool);

  /// Create a copy of 'this', including all its referenced variable-length data
  /// (i.e. strings and collections), using pool to allocate memory. This version does
  /// not allocate a tuple, instead copying to 'dst'. 'dst' must already be allocated to
  /// the correct size (i.e. TotalByteSize()).
  void DeepCopy(Tuple* dst, const TupleDescriptor& desc, MemPool* pool);

  /// Create a copy of 'this', including all referenced variable-length data (i.e. strings
  /// and collections), into 'data'. The tuple is written first, followed by any
  /// variable-length data. 'data' and 'offset' will be incremented by the total number of
  /// bytes written. 'data' must already be allocated to the correct size
  /// (i.e. TotalByteSize()).
  /// If 'convert_ptrs' is true, rewrites pointers that are part of the tuple as offsets
  /// into 'data'. Otherwise they will remain pointers directly into data. The offsets are
  /// determined by 'offset', where '*offset' corresponds to address '*data'.
  void DeepCopy(const TupleDescriptor& desc, char** data, int* offset,
                bool convert_ptrs = false);

  /// This function should only be called on tuples created by DeepCopy() with
  /// 'convert_ptrs' = true. It takes all pointers contained in this tuple (i.e. in
  /// StringValues and CollectionValues, including those contained within other
  /// CollectionValues), and converts the offset values into pointers into
  /// 'tuple_data'. 'tuple_data' should be the serialized tuple buffer created by
  /// DeepCopy(). Note that 'tuple_data' should always be the beginning of this buffer,
  /// regardless of this tuple's offset in 'tuple_data'.
  void ConvertOffsetsToPointers(const TupleDescriptor& desc, uint8_t* tuple_data);

  /// Materialize this by evaluating the expressions in materialize_exprs
  /// over the specified 'row'. 'pool' is used to allocate var-length data.
  /// (Memory for this tuple itself must already be allocated.)
  /// If collect_string_vals is true, the materialized non-NULL string value
  /// slots and the total length of the string slots are returned in var_values
  /// and total_string.
  /// TODO: this function does not collect other var-len types such as collections.
  template <bool collect_string_vals>
  void MaterializeExprs(
      TupleRow* row, const TupleDescriptor& desc,
      const std::vector<ExprContext*>& materialize_expr_ctxs, MemPool* pool,
      std::vector<StringValue*>* non_null_string_values = NULL,
      int* total_string = NULL);

  /// Turn null indicator bit on. For non-nullable slots, the mask will be 0 and
  /// this is a no-op (but we don't have to branch to check is slots are nulalble).
  void SetNull(const NullIndicatorOffset& offset) {
    char* null_indicator_byte = reinterpret_cast<char*>(this) + offset.byte_offset;
    *null_indicator_byte |= offset.bit_mask;
  }

  /// Turn null indicator bit off.
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

  const StringValue* GetStringSlot(int offset) const {
    DCHECK(offset != -1);  // -1 offset indicates non-materialized slot
    return reinterpret_cast<const StringValue*>(
        reinterpret_cast<const char*>(this) + offset);
  }

  CollectionValue* GetCollectionSlot(int offset) {
    DCHECK(offset != -1);  // -1 offset indicates non-materialized slot
    return reinterpret_cast<CollectionValue*>(reinterpret_cast<char*>(this) + offset);
  }

  const CollectionValue* GetCollectionSlot(int offset) const {
    DCHECK(offset != -1);  // -1 offset indicates non-materialized slot
    return reinterpret_cast<const CollectionValue*>(
        reinterpret_cast<const char*>(this) + offset);
  }

  /// For C++/IR interop, we need to be able to look up types by name.
  static const char* LLVM_CLASS_NAME;

 private:
  DISALLOW_COPY_AND_ASSIGN(Tuple);

  /// Copy all referenced string and collection data by allocating memory from pool,
  /// copying data, then updating pointers in tuple to reference copied data.
  void DeepCopyVarlenData(const TupleDescriptor& desc, MemPool* pool);

  /// Copies all referenced string and collection data into 'data'. Increments 'data' and
  /// 'offset' by the number of bytes written. Recursively writes collection tuple data
  /// and referenced collection and string data.
  void DeepCopyVarlenData(const TupleDescriptor& desc, char** data, int* offset,
      bool convert_ptrs);
};

}

#endif
