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

#ifndef IMPALA_RUNTIME_COLLECTION_VALUE_H
#define IMPALA_RUNTIME_COLLECTION_VALUE_H

#include "runtime/descriptors.h"
#include "udf/udf-internal.h"

namespace impala {

/// The in-memory representation of a collection-type slot. Note that both arrays and maps
/// are represented in memory as arrays of tuples. After being read from the on-disk data,
/// arrays and maps are effectively indistinguishable; a map can be thought of as an array
/// of key/value structs (and neither of these fields are necessarily materialized in the
/// item tuples).
struct __attribute__((__packed__)) CollectionValue {
  /// Pointer to buffer containing item tuples.
  uint8_t* ptr;

  /// The number of item tuples.
  int num_tuples;

  CollectionValue() : ptr(NULL), num_tuples(0) {}
  CollectionValue(const impala_udf::CollectionVal& val) :
      ptr(val.is_null ? nullptr : val.ptr),
      num_tuples(val.num_tuples)
  {}

  uint8_t* Ptr() { return ptr; }
  const uint8_t* Ptr() const { return ptr; }

  void SetPtr(uint8_t* p) { ptr = p; }

  /// Returns the size of this collection in bytes, i.e. the number of bytes written to
  /// ptr.
  inline int64_t ByteSize(const TupleDescriptor& item_tuple_desc) const {
    return static_cast<int64_t>(num_tuples) * item_tuple_desc.byte_size();
  }

  /// For C++/IR interop, we need to be able to look up types by name.
  static const char* LLVM_CLASS_NAME;
};

}

#endif
