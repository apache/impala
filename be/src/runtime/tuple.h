// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.


#ifndef IMPALA_RUNTIME_TUPLE_H
#define IMPALA_RUNTIME_TUPLE_H

#include "codegen/impala-ir.h"
#include "common/logging.h"
#include "gutil/macros.h"
#include "runtime/descriptors.h"
#include "runtime/mem-pool.h"

namespace llvm {
class Function;
class Constant;
}

namespace impala {

struct CollectionValue;
struct StringValue;
class TupleDescriptor;
class TupleRow;

/// Minimal struct to hold slot offset information from a SlotDescriptor. Designed
/// to simplify constant substitution in CodegenCopyStrings() and allow more efficient
/// interpretation in CopyStrings().
struct SlotOffsets {
  NullIndicatorOffset null_indicator_offset;
  int tuple_offset;

  /// Generate an LLVM Constant containing the offset values of this SlotOffsets instance.
  /// Needs to be updated if the layout of this struct changes.
  llvm::Constant* ToIR(LlvmCodeGen* codegen) const;

  static const char* LLVM_CLASS_NAME;
};

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

  /// Pointer that marks an invalid Tuple address. Rather than leaving Tuple
  /// pointers uninitialized, they should point to the value of POISON.
  static Tuple* const POISON;

  void Init(int size) { memset(this, 0, size); }

  void ClearNullBits(const TupleDescriptor& tuple_desc) {
    ClearNullBits(tuple_desc.null_bytes_offset(), tuple_desc.num_null_bytes());
  }

  void ClearNullBits(int null_bytes_offset, int num_null_bytes) {
    memset(reinterpret_cast<uint8_t*>(this) + null_bytes_offset, 0, num_null_bytes);
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

  /// Materialize 'this' by evaluating the expressions in 'materialize_exprs_ctxs' over
  /// the specified 'row'.
  ///
  /// If non-NULL, 'pool' is used to allocate var-length data, otherwise var-length data
  /// isn't copied. (Memory for this tuple itself must already be allocated.) 'NULL_POOL'
  /// must be true if 'pool' is NULL and false otherwise. The template parameter serves
  /// only to differentiate the NULL vs. non-NULL pool cases when we replace the function
  /// calls during codegen; the parameter means there are two different function symbols.
  /// Callers of CodegenMaterializeExprs must set 'use_mem_pool' to true to generate the
  /// IR function for the case 'pool' is non-NULL and false for the NULL pool case.
  ///
  /// If 'COLLECT_STRING_VALS' is true, the materialized non-NULL string value slots and
  /// the total length of the string slots are returned in 'non_null_string_values' and
  /// 'total_string_lengths'. 'non_null_string_values' and 'total_string_lengths' must be
  /// non-NULL in this case. 'non_null_string_values' does not need to be empty; its
  /// original contents will be overwritten.
  /// TODO: this function does not collect other var-len types such as collections.
  template <bool COLLECT_STRING_VALS, bool NULL_POOL>
  inline void IR_ALWAYS_INLINE MaterializeExprs(TupleRow* row,
      const TupleDescriptor& desc, const std::vector<ScalarExprEvaluator*>& evals,
      MemPool* pool, std::vector<StringValue*>* non_null_string_values = NULL,
      int* total_string_lengths = NULL) {
    DCHECK_EQ(NULL_POOL, pool == NULL);
    DCHECK_EQ(evals.size(), desc.slots().size());
    StringValue** non_null_string_values_array = NULL;
    int num_non_null_string_values = 0;
    if (COLLECT_STRING_VALS) {
      DCHECK(non_null_string_values != NULL);
      DCHECK(total_string_lengths != NULL);
      // string::resize() will zero-initialize any new values, so we resize to the largest
      // possible size here, then truncate the vector below once we know the actual size
      // (which preserves already-written values).
      non_null_string_values->resize(desc.string_slots().size());
      non_null_string_values_array = non_null_string_values->data();
      *total_string_lengths = 0;
    }
    MaterializeExprs<COLLECT_STRING_VALS, NULL_POOL>(row, desc,
        evals.data(), pool, non_null_string_values_array,
        total_string_lengths, &num_non_null_string_values);
    if (COLLECT_STRING_VALS) non_null_string_values->resize(num_non_null_string_values);
  }

  /// Copy the var-len string data in this tuple into the provided memory pool and update
  /// the string slots to point at the copied strings. 'string_slot_offsets' contains the
  /// required offsets in a contiguous array to allow efficient iteration and easy
  /// substitution of the array with constants for codegen. 'err_ctx' is a string that is
  /// included in error messages to provide context. Returns true on success, otherwise on
  /// failure sets 'status' to an error. This odd return convention is used to avoid
  /// emitting unnecessary code for ~Status() in perf-critical code.
  bool CopyStrings(const char* err_ctx, RuntimeState* state,
      const SlotOffsets* string_slot_offsets, int num_string_slots, MemPool* pool,
      Status* status) noexcept;

  /// Symbols (or substrings of the symbols) of MaterializeExprs(). These can be passed to
  /// LlvmCodeGen::ReplaceCallSites().
  static const char* MATERIALIZE_EXPRS_SYMBOL;
  static const char* MATERIALIZE_EXPRS_NULL_POOL_SYMBOL;

  /// Generates an IR version of MaterializeExprs(), returned in 'fn'. Currently only
  /// 'collect_string_vals' = false is implemented and some arguments passed to the IR
  /// function are unused.
  ///
  /// If 'use_mem_pool' is true, any varlen data will be copied into the MemPool specified
  /// in the 'pool' argument of the generated function. Otherwise, the varlen data won't
  /// be copied. There are two different MaterializeExprs symbols to differentiate between
  /// these cases when we replace the function calls during codegen. Please see comment
  /// of MaterializeExprs() for details.
  static Status CodegenMaterializeExprs(LlvmCodeGen* codegen, bool collect_string_vals,
      const TupleDescriptor& desc, const vector<ScalarExpr*>& slot_materialize_exprs,
      bool use_mem_pool, llvm::Function** fn);

  /// Generates an IR version of CopyStrings(). The offsets of string slots are replaced
  /// with constants. This can allow the LLVM optimiser to unroll the loop and generate
  /// efficient code without interpretation overhead. The LLVM optimiser at -O2 generally
  /// will only unroll the loop for small numbers of iterations: 8 or less in some
  /// experiements. On success, 'fn' is set to point to the generated function.
  static Status CodegenCopyStrings(LlvmCodeGen* codegen,
      const TupleDescriptor& desc, llvm::Function** materialize_strings_fn);

  /// Turn null indicator bit on. For non-nullable slots, the mask will be 0 and
  /// this is a no-op (but we don't have to branch to check is slots are nullable).
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

  /// Implementation of MaterializedExprs(). This function is replaced during
  /// codegen. 'num_non_null_string_values' must be initialized by the caller.
  template <bool COLLECT_STRING_VALS, bool NULL_POOL>
  void IR_NO_INLINE MaterializeExprs(TupleRow* row, const TupleDescriptor& desc,
      ScalarExprEvaluator* const* evals, MemPool* pool,
      StringValue** non_null_string_values, int* total_string_lengths,
      int* num_non_null_string_values);

  /// Helper for CopyStrings() to allocate 'bytes' of memory. Returns a pointer to the
  /// allocated buffer on success. Otherwise an error was encountered, in which case NULL
  /// is returned and 'status' is set to an error. This odd return convention is used to
  /// avoid emitting unnecessary code for ~Status() in perf-critical code.
  char* AllocateStrings(const char* err_ctx, RuntimeState* state, int64_t bytes,
      MemPool* pool, Status* status) noexcept;

};

}

#endif
