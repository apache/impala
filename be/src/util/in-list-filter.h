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

#pragma once

#include "gen-cpp/ImpalaInternalService_types.h"
#include "impala-ir/impala-ir-functions.h"
#include "runtime/date-value.h"
#include "runtime/decimal-value.h"
#include "runtime/string-buffer.h"
#include "runtime/string-value.h"
#include "runtime/timestamp-value.h"
#include "runtime/types.h"

namespace impala {

class InListFilter {
 public:
  /// Upper bound of total length for the string set to avoid it explodes.
  /// TODO: find a better value based on the implementation of ORC lib, or make this
  /// configurable.
  const static uint32_t STRING_SET_MAX_TOTAL_LENGTH = 4 * 1024 * 1024;

  InListFilter(ColumnType type, uint32_t entry_limit, bool contains_null = false);
  ~InListFilter() {}
  void Close() {}

  /// Add a new value to the list.
  void Insert(const void* val);

  std::string DebugString() const noexcept;

  bool ContainsNull() { return contains_null_; }
  bool AlwaysTrue() { return always_true_; }
  bool AlwaysFalse();
  static bool AlwaysFalse(const InListFilterPB& filter);

  /// Makes this filter always return true.
  void SetAlwaysTrue() { always_true_ = true; }

  bool Find(void* val, const ColumnType& col_type) const noexcept;
  int NumItems() const noexcept;

  /// Returns a new InListFilter with the given type, allocated from 'mem_tracker'.
  static InListFilter* Create(ColumnType type, uint32_t entry_limit, ObjectPool* pool);

  /// Returns a new InListFilter created from the protobuf representation, allocated from
  /// 'mem_tracker'.
  static InListFilter* Create(const InListFilterPB& protobuf, ColumnType type,
      uint32_t entry_limit, ObjectPool* pool);

  /// Converts 'filter' to its corresponding Protobuf representation.
  /// If the first argument is NULL, it is interpreted as a complete filter which
  /// contains all elements, i.e. always true.
  static void ToProtobuf(const InListFilter* filter, InListFilterPB* protobuf);

  /// Returns the LLVM_CLASS_NAME for this base class 'InListFilter'.
  static const char* LLVM_CLASS_NAME;

  /// Return a debug string for 'filter'
  static std::string DebugString(const InListFilterPB& filter);
  /// Return a debug string for the list of the 'filter'
  static std::string DebugStringOfList(const InListFilterPB& filter);

 private:
  friend class HdfsOrcScanner;
  void ToProtobuf(InListFilterPB* protobuf) const;

  bool always_true_;
  bool contains_null_;
  PrimitiveType type_;
  // Type len for CHAR type.
  int type_len_;
  /// Value set for all numeric types. Use int64_t for simplicity.
  /// TODO(IMPALA-11141): use the exact type to save memory space.
  std::unordered_set<int64_t> values_;
  /// Value set for all string types.
  std::unordered_set<std::string> str_values_;
  uint32_t str_total_size_ = 0;
  uint32_t entry_limit_;
};
}

