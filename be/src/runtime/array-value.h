// Copyright 2015 Cloudera Inc.
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

#ifndef IMPALA_RUNTIME_ARRAY_VALUE_H
#define IMPALA_RUNTIME_ARRAY_VALUE_H

#include "runtime/mem-pool.h"
#include "runtime/descriptors.h"

namespace impala {

/// The in-memory representation of a collection-type slot. Note that both arrays and maps
/// are represented in memory as arrays of tuples. After being read from the on-disk data,
/// arrays and maps are effectively indistinguishable; a map can be thought of as an array
/// of key/value structs (and neither of these fields are necessarily materialized in the
/// item tuples).
///
/// Possible future implementations:
/// - ArrayValues are backed by a BufferedTupleStream, with one stream per tuple desc and
///   per thread (i.e., a single stream backs many ArrayValues). The ArrayValue itself
///   would look something like: struct ArrayValue { int num_tuples; int64_t row_idx; }
///   This struct could possibly be shrunk to a single int64 field containing both values.
///
/// - A columnar in-memory layout that replaces RowBatches (i.e., each contiguous
///   in-memory column would contain multiple values or arrays). For example, the schema:
///     c1 int, c2 array<float>
///   would yield an in-memory layout like:
///     struct RowBatch {
///       bool* c1_nulls;
///       int* c1_values;
///       bool* c2_nulls;
///       int* c2_sizes; // the length of each non-null c2 array
///       bool* c2_item_nulls; // the number of null bits = sum(c2_sizes)
///       float* c2_item_values;
///     }
/// TODO for 2.3: rename to CollectionValue
struct ArrayValue {
  /// Pointer to buffer containing item tuples.
  uint8_t* ptr;

  /// The number of item tuples.
  int num_tuples;

  ArrayValue() : ptr(NULL), num_tuples(0) {}

  /// Returns the size of this array in bytes, i.e. the number of bytes written to ptr.
  inline int64_t ByteSize(const TupleDescriptor& item_tuple_desc) const {
    return num_tuples * item_tuple_desc.byte_size();
  }
};

}

#endif
