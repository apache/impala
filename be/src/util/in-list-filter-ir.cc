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

#include "common/object-pool.h"
#include "util/in-list-filter.h"

namespace impala {

void InListFilter::Insert(const void* val) {
  if (always_true_) return;
  if (UNLIKELY(val == nullptr)) {
    contains_null_ = true;
    return;
  }
  if (UNLIKELY(values_.size() >= entry_limit_ || str_values_.size() >= entry_limit_)) {
    always_true_ = true;
    values_.clear();
    str_values_.clear();
    return;
  }
  switch (type_) {
    case TYPE_TINYINT:
      values_.insert(*reinterpret_cast<const int8_t*>(val));
      break;
    case TYPE_SMALLINT:
      values_.insert(*reinterpret_cast<const int16_t*>(val));
      break;
    case TYPE_INT:
      values_.insert(*reinterpret_cast<const int32_t*>(val));
      break;
    case TYPE_BIGINT:
      values_.insert(*reinterpret_cast<const int64_t*>(val));
      break;
    case TYPE_DATE:
      values_.insert(reinterpret_cast<const DateValue*>(val)->Value());
      break;
    case TYPE_STRING:
    case TYPE_VARCHAR: {
      const StringValue* s = reinterpret_cast<const StringValue*>(val);
      if (UNLIKELY(s->ptr == nullptr)) {
        contains_null_ = true;
      } else {
        str_total_size_ += s->len;
        if (str_total_size_ >= STRING_SET_MAX_TOTAL_LENGTH) {
          always_true_ = true;
          str_values_.clear();
          return;
        }
        str_values_.insert(string(s->ptr, s->len));
      }
      break;
    }
    case TYPE_CHAR:
      str_values_.insert(string(reinterpret_cast<const char*>(val), type_len_));
      break;
    default:
      DCHECK(false) << "Not supported IN-list filter type: " << TypeToString(type_);
      break;
  }
}
} // namespace impala
