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

namespace impala {

bool InListFilter::AlwaysFalse() {
  return !always_true_ && !contains_null_ && values_.empty() && str_values_.empty();
}

bool InListFilter::AlwaysFalse(const InListFilterPB& filter) {
  return !filter.always_true() && !filter.contains_null() && filter.value_size() == 0;
}

bool InListFilter::Find(void* val, const ColumnType& col_type) const noexcept {
  if (always_true_) return true;
  if (val == nullptr) return contains_null_;
  DCHECK_EQ(type_, col_type.type);
  int64_t v;
  const StringValue* s;
  switch (col_type.type) {
    case TYPE_TINYINT:
      v = *reinterpret_cast<const int8_t*>(val);
      break;
    case TYPE_SMALLINT:
      v = *reinterpret_cast<const int16_t*>(val);
      break;
    case TYPE_INT:
      v = *reinterpret_cast<const int32_t*>(val);
      break;
    case TYPE_BIGINT:
      v = *reinterpret_cast<const int64_t*>(val);
      break;
    case TYPE_DATE:
      v = reinterpret_cast<const DateValue*>(val)->Value();
      break;
    case TYPE_STRING:
    case TYPE_VARCHAR:
      s = reinterpret_cast<const StringValue*>(val);
      return str_values_.find(string(s->ptr, s->len)) != str_values_.end();
    case TYPE_CHAR:
      return str_values_.find(string(reinterpret_cast<const char*>(val), col_type.len))
          != str_values_.end();
    default:
      DCHECK(false) << "Not support IN-list filter type: " << TypeToString(type_);
      return false;
  }
  return values_.find(v) != values_.end();
}

InListFilter::InListFilter(ColumnType type, uint32_t entry_limit, bool contains_null):
  always_true_(false), contains_null_(contains_null), type_(type.type),
  entry_limit_(entry_limit) {
  if (type.type == TYPE_CHAR) type_len_ = type.len;
}

InListFilter* InListFilter::Create(ColumnType type, uint32_t entry_limit,
    ObjectPool* pool) {
  return pool->Add(new InListFilter(type, entry_limit));
}

InListFilter* InListFilter::Create(const InListFilterPB& protobuf, ColumnType type,
    uint32_t entry_limit, ObjectPool* pool) {
  InListFilter* filter = pool->Add(
      new InListFilter(type, entry_limit, protobuf.contains_null()));
  filter->always_true_ = protobuf.always_true();
  for (const ColumnValuePB& v : protobuf.value()) {
    switch (type.type) {
      case TYPE_TINYINT:
      case TYPE_SMALLINT:
      case TYPE_INT:
      case TYPE_BIGINT:
      case TYPE_DATE:
        DCHECK(v.has_long_val());
        filter->values_.insert(v.long_val());
        break;
      case TYPE_STRING:
      case TYPE_CHAR:
      case TYPE_VARCHAR:
        DCHECK(v.has_string_val());
        // TODO(IMPALA-11143): use mem_tracker
        filter->str_values_.insert(v.string_val());
        break;
      default:
        DCHECK(false) << "Not support IN-list filter type: " << TypeToString(type.type);
        return nullptr;
    }
  }
  if (type.IsStringType()) {
    DCHECK(filter->values_.empty());
  } else {
    DCHECK(filter->str_values_.empty());
  }
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

void InListFilter::ToProtobuf(InListFilterPB* protobuf) const {
  protobuf->set_always_true(always_true_);
  if (always_true_) return;
  protobuf->set_contains_null(contains_null_);
  if (type_ == TYPE_STRING || type_ == TYPE_VARCHAR || type_ == TYPE_CHAR) {
    for (const string& s : str_values_) {
      ColumnValuePB* proto = protobuf->add_value();
      proto->set_string_val(s);
    }
  } else {
    for (int64_t v : values_) {
      ColumnValuePB* proto = protobuf->add_value();
      proto->set_long_val(v);
    }
  }
}

int InListFilter::NumItems() const noexcept {
  int res = contains_null_ ? 1 : 0;
  if (type_ == TYPE_STRING || type_ == TYPE_VARCHAR || type_ == TYPE_CHAR) {
    return res + str_values_.size();
  }
  return res + values_.size();
}

string InListFilter::DebugString() const noexcept {
  std::stringstream ss;
  bool first_value = true;
  ss << "IN-list filter: [";
  if (type_ == TYPE_STRING) {
    for (const string &s : str_values_) {
      if (first_value) {
        first_value = false;
      } else {
        ss << ',';
      }
      ss << "\"" << s << "\"";
    }
  } else {
    for (int64_t v : values_) {
      if (first_value) {
        first_value = false;
      } else {
        ss << ',';
      }
      ss << v;
    }
  }
  if (contains_null_) {
    if (!first_value) ss << ',';
    ss << "NULL";
  }
  ss << ']';
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
    if (v.has_byte_val()) {
      ss << v.byte_val();
    } else if (v.has_short_val()) {
      ss << v.short_val();
    } else if (v.has_int_val()) {
      ss << v.int_val();
    } else if (v.has_long_val()) {
      ss << v.long_val();
    } else if (v.has_date_val()) {
      ss << v.date_val();
    } else if (v.has_string_val()) {
      ss << "\"" << v.string_val() << "\"";
    }
  }
  ss << ']';
  return ss.str();
}

} // namespace impala
