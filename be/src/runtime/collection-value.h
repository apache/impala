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

  /// Returns the size of this collection in bytes, i.e. the number of bytes written to
  /// ptr.
  inline int64_t ByteSize(const TupleDescriptor& item_tuple_desc) const {
    return static_cast<int64_t>(num_tuples) * item_tuple_desc.byte_size();
  }

  /// For C++/IR interop, we need to be able to look up types by name.
  static const char* LLVM_CLASS_NAME;
};

// A struct that contains a pointer to a CollectionValue and its byte size. Used instead
// of std::pair because of codegen, because
//   - the std::pair type is difficult to name in codegen and
//   - we are not in control of the layout of std::pair.
struct CollValueAndSize {
  CollectionValue* coll_value;
  // In most (maybe all) cases a 32 bit int should be enough but
  // 'CollectionValue::ByteSize()' returns int64_t so we use that.
  int64_t byte_size;

  CollValueAndSize(): CollValueAndSize(nullptr, 0) {}
  CollValueAndSize(CollectionValue* cv, int64_t size)
    : coll_value(cv), byte_size(size) {}

  /// For C++/IR interop, we need to be able to look up types by name.
  static const char* LLVM_CLASS_NAME;
};

}

#endif
