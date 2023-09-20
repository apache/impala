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

#include "util/in-list-filter.h"

#include "common/object-pool.h"
#include "runtime/string-value.inline.h"

namespace impala {

InListFilter::InListFilter(uint32_t entry_limit, bool contains_null):
  entry_limit_(entry_limit), always_true_(false), contains_null_(contains_null) {
}

bool InListFilter::AlwaysFalse(const InListFilterPB& filter) {
  return !filter.always_true() && !filter.contains_null() && filter.value_size() == 0;
}

InListFilter* InListFilter::Create(ColumnType type, uint32_t entry_limit,
    ObjectPool* pool, MemTracker* mem_tracker, bool contains_null) {
  InListFilter* res;
  switch (type.type) {
    case TYPE_TINYINT:
      res = new InListFilterImpl<int8_t, TYPE_TINYINT>(entry_limit, contains_null);
      break;
    case TYPE_SMALLINT:
      res = new InListFilterImpl<int16_t, TYPE_SMALLINT>(entry_limit, contains_null);
      break;
    case TYPE_INT:
      res = new InListFilterImpl<int32_t, TYPE_INT>(entry_limit, contains_null);
      break;
    case TYPE_BIGINT:
      res = new InListFilterImpl<int64_t, TYPE_BIGINT>(entry_limit, contains_null);
      break;
    case TYPE_DATE:
      // We use int32_t for DATE type as well
      res = new InListFilterImpl<int32_t, TYPE_DATE>(entry_limit, contains_null);
      break;
    case TYPE_STRING:
      res = new InListFilterImpl<StringValue, TYPE_STRING>(type, entry_limit,
          mem_tracker, contains_null);
      break;
    case TYPE_VARCHAR:
      res = new InListFilterImpl<StringValue, TYPE_VARCHAR>(type, entry_limit,
          mem_tracker, contains_null);
      break;
    case TYPE_CHAR:
      res = new InListFilterImpl<StringValue, TYPE_CHAR>(type, entry_limit,
          mem_tracker, contains_null);
      break;
    default:
      DCHECK(false) << "Not support IN-list filter type: " << TypeToString(type.type);
      return nullptr;
  }
  return pool->Add(res);
}

InListFilter* InListFilter::Create(const InListFilterPB& protobuf, ColumnType type,
    uint32_t entry_limit, ObjectPool* pool, MemTracker* mem_tracker) {
  InListFilter* filter = InListFilter::Create(type, entry_limit, pool, mem_tracker,
      protobuf.contains_null());
  filter->always_true_ = protobuf.always_true();
  filter->InsertBatch(protobuf.value());
  filter->MaterializeValues();
  return filter;
}

void InListFilter::ToProtobuf(const InListFilter* filter, InListFilterPB* protobuf) {
  DCHECK(protobuf != nullptr);
  if (filter == nullptr) {
    protobuf->set_always_true(true);
    return;
  }
  filter->ToProtobuf(protobuf);
}

string InListFilter::DebugString() const noexcept {
  std::stringstream ss;
  ss << "IN-list filter of " << total_entries_ << " items";
  if (contains_null_) ss << " with NULL";
  return ss.str();
}

string InListFilter::DebugString(const InListFilterPB& filter) {
  std::stringstream ss;
  ss << "IN-list filter: " << DebugStringOfList(filter);
  return ss.str();
}

string InListFilter::DebugStringOfList(const InListFilterPB& filter) {
  std::stringstream ss;
  ss << "[";
  bool first_value = true;
  for (const ColumnValuePB& v : filter.value()) {
    if (first_value) {
      first_value = false;
    } else {
      ss << ',';
    }
    ss << v.ShortDebugString();
  }
  ss << ']';
  return ss.str();
}

template<PrimitiveType SLOT_TYPE>
void InListFilterImpl<StringValue, SLOT_TYPE>::MaterializeValues() {
  if (newly_inserted_values_.total_len == 0) {
    if (!newly_inserted_values_.values.empty()) {
      // Newly inserted values are all empty strings. Don't need to allocate memory.
      values_.values.insert(
          newly_inserted_values_.values.begin(), newly_inserted_values_.values.end());
    }
    return;
  }
  uint8_t* buffer = mem_pool_.Allocate(newly_inserted_values_.total_len);
  if (buffer == nullptr) {
    VLOG_QUERY << "Not enough memory in materializing string IN-list filters. "
        << "Fallback to always true. New string batch size: "
        << newly_inserted_values_.total_len << "\n" << mem_pool_.DebugString();
    Reset();
    return;
  }
  // Transfer values to the final set. Don't need to update total_entries_ since it's
  // already done in Insert().
  for (const StringValue& s : newly_inserted_values_.values) {
    if (s.IsSmall()) {
      values_.insert(s);
    } else {
      Ubsan::MemCpy(buffer, s.Ptr(), s.Len());
      values_.insert(StringValue(reinterpret_cast<char*>(buffer), s.Len()));
      buffer += s.Len();
    }
  }
  newly_inserted_values_.clear();
}

#define IN_LIST_FILTER_INSERT_BATCH(TYPE, SLOT_TYPE, PB_VAL_METHOD, SET_VAR)             \
  template<>                                                                             \
  void InListFilterImpl<TYPE, SLOT_TYPE>::InsertBatch(const ColumnValueBatchPB& batch) { \
    for (const ColumnValuePB& v : batch) {                                               \
      DCHECK(v.has_##PB_VAL_METHOD()) << v.ShortDebugString();                           \
      const auto& res = SET_VAR.insert(v.PB_VAL_METHOD());                               \
      if (res.second) {                                                                  \
        ++total_entries_;                                                                \
        if (UNLIKELY(total_entries_ > entry_limit_)) {                                   \
          Reset();                                                                       \
          break;                                                                         \
        }                                                                                \
      }                                                                                  \
    }                                                                                    \
    DCHECK_EQ(total_entries_, SET_VAR.size());                                           \
  }

IN_LIST_FILTER_INSERT_BATCH(int8_t, TYPE_TINYINT, byte_val, values_)
IN_LIST_FILTER_INSERT_BATCH(int16_t, TYPE_SMALLINT, short_val, values_)
IN_LIST_FILTER_INSERT_BATCH(int32_t, TYPE_INT, int_val, values_)
IN_LIST_FILTER_INSERT_BATCH(int64_t, TYPE_BIGINT, long_val, values_)
IN_LIST_FILTER_INSERT_BATCH(int32_t, TYPE_DATE, int_val, values_)
IN_LIST_FILTER_INSERT_BATCH(StringValue, TYPE_STRING, string_val, newly_inserted_values_)
IN_LIST_FILTER_INSERT_BATCH(StringValue, TYPE_VARCHAR, string_val, newly_inserted_values_)
IN_LIST_FILTER_INSERT_BATCH(StringValue, TYPE_CHAR, string_val, newly_inserted_values_)

#define NUMERIC_IN_LIST_FILTER_TO_PROTOBUF(TYPE, SLOT_TYPE, PB_VAL_METHOD)             \
  template<>                                                                           \
  void InListFilterImpl<TYPE, SLOT_TYPE>::ToProtobuf(InListFilterPB* protobuf) const { \
    protobuf->set_always_true(always_true_);                                           \
    if (always_true_) return;                                                          \
    protobuf->set_contains_null(contains_null_);                                       \
    for (TYPE v : values_) {                                                           \
      ColumnValuePB* proto = protobuf->add_value();                                    \
      proto->set_##PB_VAL_METHOD(v);                                                   \
    }                                                                                  \
  }

NUMERIC_IN_LIST_FILTER_TO_PROTOBUF(int8_t, TYPE_TINYINT, byte_val)
NUMERIC_IN_LIST_FILTER_TO_PROTOBUF(int16_t, TYPE_SMALLINT, short_val)
NUMERIC_IN_LIST_FILTER_TO_PROTOBUF(int32_t, TYPE_INT, int_val)
NUMERIC_IN_LIST_FILTER_TO_PROTOBUF(int64_t, TYPE_BIGINT, long_val)
NUMERIC_IN_LIST_FILTER_TO_PROTOBUF(int32_t, TYPE_DATE, int_val)

#define STRING_IN_LIST_FILTER_TO_PROTOBUF(SLOT_TYPE)                                   \
  template<>                                                                           \
  void InListFilterImpl<StringValue, SLOT_TYPE>::ToProtobuf(InListFilterPB* protobuf)  \
      const {                                                                          \
    protobuf->set_always_true(always_true_);                                           \
    if (always_true_) return;                                                          \
    protobuf->set_contains_null(contains_null_);                                       \
    for (const StringValue& v : values_.values) {                                      \
      ColumnValuePB* proto = protobuf->add_value();                                    \
      proto->set_string_val(v.Ptr(), v.Len());                                         \
    }                                                                                  \
  }

STRING_IN_LIST_FILTER_TO_PROTOBUF(TYPE_STRING)
STRING_IN_LIST_FILTER_TO_PROTOBUF(TYPE_VARCHAR)
STRING_IN_LIST_FILTER_TO_PROTOBUF(TYPE_CHAR)

template<>
void InListFilterImpl<int32_t, TYPE_DATE>::ToOrcLiteralList(
    vector<orc::Literal>* in_list) {
  for (int32_t v : values_) {
    in_list->emplace_back(orc::PredicateDataType::DATE, v);
  }
}

#define STRING_IN_LIST_FILTER_TO_ORC_LITERAL_LIST(SLOT_TYPE)            \
  template<>                                                            \
  void InListFilterImpl<StringValue, SLOT_TYPE>::ToOrcLiteralList(      \
      vector<orc::Literal>* in_list) {                                  \
    for (const StringValue& s : values_.values) {                       \
      in_list->emplace_back(s.Ptr(), s.Len());                              \
    }                                                                   \
  }

STRING_IN_LIST_FILTER_TO_ORC_LITERAL_LIST(TYPE_STRING)
STRING_IN_LIST_FILTER_TO_ORC_LITERAL_LIST(TYPE_VARCHAR)
STRING_IN_LIST_FILTER_TO_ORC_LITERAL_LIST(TYPE_CHAR)

} // namespace impala
