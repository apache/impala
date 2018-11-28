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

#include "util/min-max-filter.h"

#include "runtime/string-value.inline.h"

using std::string;

namespace impala {

#define NUMERIC_MIN_MAX_FILTER_INSERT(NAME, TYPE) \
  void NAME##MinMaxFilter::Insert(void* val) {    \
    if (val == nullptr) return;                   \
    TYPE* value = reinterpret_cast<TYPE*>(val);   \
    if (*value < min_) min_ = *value;             \
    if (*value > max_) max_ = *value;             \
  }

NUMERIC_MIN_MAX_FILTER_INSERT(Bool, bool);
NUMERIC_MIN_MAX_FILTER_INSERT(TinyInt, int8_t);
NUMERIC_MIN_MAX_FILTER_INSERT(SmallInt, int16_t);
NUMERIC_MIN_MAX_FILTER_INSERT(Int, int32_t);
NUMERIC_MIN_MAX_FILTER_INSERT(BigInt, int64_t);
NUMERIC_MIN_MAX_FILTER_INSERT(Float, float);
NUMERIC_MIN_MAX_FILTER_INSERT(Double, double);

void StringMinMaxFilter::Insert(void* val) {
  if (val == nullptr || always_true_) return;
  const StringValue* value = reinterpret_cast<const StringValue*>(val);
  if (always_false_) {
    min_ = *value;
    max_ = *value;
    always_false_ = false;
  } else {
    if (*value < min_) {
      min_ = *value;
      min_buffer_.Clear();
    } else if (*value > max_) {
      max_ = *value;
      max_buffer_.Clear();
    }
  }
}

void TimestampMinMaxFilter::Insert(void* val) {
  if (val == nullptr) return;
  const TimestampValue* value = reinterpret_cast<const TimestampValue*>(val);
  if (always_false_) {
    min_ = *value;
    max_ = *value;
    always_false_ = false;
  } else {
    if (*value < min_) {
      min_ = *value;
    } else if (*value > max_) {
      max_ = *value;
    }
  }
}

#define INSERT_DECIMAL_MINMAX(SIZE)                         \
  do {                                                      \
    if (val == nullptr) return;                             \
    const Decimal##SIZE##Value* value##SIZE##_ =            \
        reinterpret_cast<const Decimal##SIZE##Value*>(val); \
    if (always_false_) {                                    \
      min##SIZE##_ = *value##SIZE##_;                       \
      max##SIZE##_ = *value##SIZE##_;                       \
      always_false_ = false;                                \
    } else {                                                \
      if (*value##SIZE##_ < min##SIZE##_) {                 \
        min##SIZE##_ = *value##SIZE##_;                     \
      } else if (*value##SIZE##_ > max##SIZE##_) {          \
        max##SIZE##_ = *value##SIZE##_;                     \
      }                                                     \
    }                                                       \
  } while (false)

void DecimalMinMaxFilter::Insert4(void* val) {
  INSERT_DECIMAL_MINMAX(4);
}

void DecimalMinMaxFilter::Insert8(void* val) {
  INSERT_DECIMAL_MINMAX(8);
}

void DecimalMinMaxFilter::Insert16(void* val) {
  INSERT_DECIMAL_MINMAX(16);
}

} // namespace impala
