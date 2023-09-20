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

#include <boost/unordered_set.hpp>
#include <orc/sargs/Literal.hh>

#include "gen-cpp/ImpalaInternalService_types.h"
#include "impala-ir/impala-ir-functions.h"
#include "runtime/date-value.h"
#include "runtime/decimal-value.h"
#include "runtime/string-buffer.h"
#include "runtime/string-value.h"
#include "runtime/string-value.inline.h"
#include "runtime/timestamp-value.h"
#include "runtime/types.h"

namespace impala {

typedef google::protobuf::RepeatedPtrField<ColumnValuePB> ColumnValueBatchPB;

class InListFilter {
 public:
  /// Upper bound of total length for the string set to avoid it explodes.
  /// TODO: find a better value based on the implementation of ORC lib, or make this
  /// configurable.
  const static uint32_t STRING_SET_MAX_TOTAL_LENGTH = 4 * 1024 * 1024;

  InListFilter(uint32_t entry_limit, bool contains_null = false);
  virtual ~InListFilter() {}
  virtual void Close() {}

  /// Add a new value to the list.
  virtual void Insert(const void* val) = 0;

  /// Materialize filter values by copying any values stored by filters into memory owned
  /// by the filter. Filters may assume that the memory for Insert()-ed values stays valid
  /// until this is called. Invoked after inserting a batch.
  virtual void MaterializeValues() {}

  std::string DebugString() const noexcept;

  bool ContainsNull() { return contains_null_; }
  bool AlwaysTrue() { return always_true_; }
  bool AlwaysFalse() {
    return !always_true_ && !contains_null_ && total_entries_ == 0;
  }
  static bool AlwaysFalse(const InListFilterPB& filter);

  /// Makes this filter always return true.
  void SetAlwaysTrue() { always_true_ = true; }

  virtual bool Find(const void* val, const ColumnType& col_type) const noexcept = 0;
  int NumItems() const noexcept {
    return total_entries_ + (contains_null_ ? 1 : 0);
  }

  /// Fills the orc::Literal vector with set values (excluding NULL).
  virtual void ToOrcLiteralList(std::vector<orc::Literal>* in_list) = 0;

  /// Returns a new InListFilter with the given type, allocated from 'mem_tracker'.
  static InListFilter* Create(ColumnType type, uint32_t entry_limit, ObjectPool* pool,
      MemTracker* mem_tracker, bool contains_null = false);

  /// Returns a new InListFilter created from the protobuf representation, allocated from
  /// 'mem_tracker'.
  static InListFilter* Create(const InListFilterPB& protobuf, ColumnType type,
      uint32_t entry_limit, ObjectPool* pool, MemTracker* mem_tracker);

  /// Converts 'filter' to its corresponding Protobuf representation.
  /// If the first argument is NULL, it is interpreted as a complete filter which
  /// contains all elements, i.e. always true.
  static void ToProtobuf(const InListFilter* filter, InListFilterPB* protobuf);

  /// Return a debug string for 'filter'
  static std::string DebugString(const InListFilterPB& filter);
  /// Return a debug string for the list of the 'filter'
  static std::string DebugStringOfList(const InListFilterPB& filter);

 protected:
  friend class HdfsOrcScanner;

  virtual void ToProtobuf(InListFilterPB* protobuf) const = 0;

  /// Insert a batch of protobuf values.
  virtual void InsertBatch(const ColumnValueBatchPB& batch) = 0;

  uint32_t entry_limit_;
  uint32_t total_entries_ = 0;
  bool always_true_;
  bool contains_null_;
};

template<typename T, PrimitiveType SLOT_TYPE>
class InListFilterImpl : public InListFilter {
 public:
  InListFilterImpl(uint32_t entry_limit, bool contains_null = false):
      InListFilter(entry_limit, contains_null) {}
  ~InListFilterImpl() {}

  void Insert(const void* val) override;
  void InsertBatch(const ColumnValueBatchPB& batch) override;
  bool Find(const void* val, const ColumnType& col_type) const noexcept override;

  void ToProtobuf(InListFilterPB* protobuf) const override;
  void ToOrcLiteralList(std::vector<orc::Literal>* in_list) override {
    for (auto v : values_) in_list->emplace_back(static_cast<int64_t>(v));
  }

  inline void Reset() {
    always_true_ = true;
    contains_null_ = false;
    values_.clear();
    total_entries_ = 0;
  }

  inline static T GetValue(const void* val) {
    return *reinterpret_cast<const T*>(val);
  }
 private:
  std::unordered_set<T> values_;
};

/// String set that wraps a boost::unordered_set<StringValue> and tracks the total length
/// of strings in the set. Exposes the same methods of boost::unordered_set that are used
/// in InListFilters.
struct StringSetWithTotalLen {
  boost::unordered_set<StringValue> values;
  uint32_t total_len = 0;

  typedef typename boost::unordered_set<StringValue>::iterator iterator;

  /// Inserts a new StringValue. Returns a pair consisting of an iterator to the element
  /// in the set, and a bool denoting whether the insertion took place (true if insertion
  /// happened, false if it did not, i.e. already exists).
  inline pair<iterator, bool> insert(StringValue v) {
    const auto& res = values.emplace(v);
    total_len += (res.second && !v.IsSmall() ? v.Len() : 0);
    return res;
  }

  /// Same as the above one but inserts a value of std::string
  inline pair<iterator, bool> insert(const string& s) {
    const auto& res = values.emplace(s);
    total_len += (res.second ? s.length() : 0);
    return res;
  }

  inline bool find(StringValue v) const {
    return values.find(v) != values.end();
  }

  inline void clear() {
    values.clear();
    total_len = 0;
  }

  inline size_t size() const {
    return values.size();
  }
};

template<PrimitiveType SLOT_TYPE>
class InListFilterImpl<StringValue, SLOT_TYPE> : public InListFilter {
 public:
  InListFilterImpl(ColumnType type, uint32_t entry_limit, MemTracker* mem_tracker,
      bool contains_null = false):
      InListFilter(entry_limit, contains_null), mem_pool_(mem_tracker) {
    if (SLOT_TYPE == TYPE_CHAR) type_len_ = type.len;
  }
  ~InListFilterImpl() {}
  void Close() override { mem_pool_.FreeAll(); }

  void Insert(const void* val) override;
  void InsertBatch(const ColumnValueBatchPB& batch) override;
  void MaterializeValues() override;
  bool Find(const void* val, const ColumnType& col_type) const noexcept override;

  void ToProtobuf(InListFilterPB* protobuf) const override;
  void ToOrcLiteralList(std::vector<orc::Literal>* in_list) override;

  inline void Reset() {
    always_true_ = true;
    contains_null_ = false;
    values_.clear();
    newly_inserted_values_.clear();
    total_entries_ = 0;
  }

  inline static StringValue GetValue(const void* val, int char_type_len) {
    return *reinterpret_cast<const StringValue*>(val);
  }
 private:
  MemPool mem_pool_;
  StringSetWithTotalLen values_;
  /// Temp set used to insert new values. They will be transferred to values_ in
  /// MaterializeValues(). Values should always be inserted into this set first.
  StringSetWithTotalLen newly_inserted_values_;
  /// Type len for CHAR type.
  int type_len_;
};
}

