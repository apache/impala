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

#define NUMERIC_MIN_MAX_FILTER_FUNCS(NAME, TYPE)              \
  void NAME##MinMaxFilter::Insert(const void* val) {          \
    if (LIKELY(val)) {                                        \
      const TYPE* value = reinterpret_cast<const TYPE*>(val); \
      if (UNLIKELY(*value < min_)) min_ = *value;             \
      if (UNLIKELY(*value > max_)) max_ = *value;             \
    }                                                         \
  }                                                           \
  bool NAME##MinMaxFilter::AlwaysTrue() const { return always_true_; }

NUMERIC_MIN_MAX_FILTER_FUNCS(Bool, bool);
NUMERIC_MIN_MAX_FILTER_FUNCS(TinyInt, int8_t);
NUMERIC_MIN_MAX_FILTER_FUNCS(SmallInt, int16_t);
NUMERIC_MIN_MAX_FILTER_FUNCS(Int, int32_t);
NUMERIC_MIN_MAX_FILTER_FUNCS(BigInt, int64_t);
NUMERIC_MIN_MAX_FILTER_FUNCS(Float, float);
NUMERIC_MIN_MAX_FILTER_FUNCS(Double, double);

void StringMinMaxFilter::Insert(const void* val) {
  if (LIKELY(val)) {
    const StringValue* value = reinterpret_cast<const StringValue*>(val);
    if (UNLIKELY(always_false_)) {
      min_ = *value;
      max_ = *value;
      always_false_ = false;
    } else {
      if (UNLIKELY(*value < min_)) {
        min_ = *value;
        min_buffer_.Clear();
      } else if (UNLIKELY(*value > max_)) {
        max_ = *value;
        max_buffer_.Clear();
      }
    }
  }
}

bool StringMinMaxFilter::AlwaysTrue() const { return always_true_; }

#define DATE_TIME_MIN_MAX_FILTER_FUNCS(NAME, TYPE)            \
  void NAME##MinMaxFilter::Insert(const void* val) {          \
    if (LIKELY(val)) {                                        \
      const TYPE* value = reinterpret_cast<const TYPE*>(val); \
      if (UNLIKELY(always_false_)) {                          \
        min_ = *value;                                        \
        max_ = *value;                                        \
        always_false_ = false;                                \
      } else {                                                \
        if (UNLIKELY(*value < min_)) {                        \
          min_ = *value;                                      \
        } else if (UNLIKELY(*value > max_)) {                 \
          max_ = *value;                                      \
        }                                                     \
      }                                                       \
    }                                                         \
  }                                                           \
  bool NAME##MinMaxFilter::AlwaysTrue() const { return always_true_; }

DATE_TIME_MIN_MAX_FILTER_FUNCS(Timestamp, TimestampValue);
DATE_TIME_MIN_MAX_FILTER_FUNCS(Date, DateValue);

#define INSERT_DECIMAL_MINMAX(SIZE)                            \
  do {                                                         \
    if (LIKELY(val)) {                                         \
      const Decimal##SIZE##Value* value##SIZE##_ =             \
          reinterpret_cast<const Decimal##SIZE##Value*>(val);  \
      if (UNLIKELY(always_false_)) {                           \
        min##SIZE##_ = *value##SIZE##_;                        \
        max##SIZE##_ = *value##SIZE##_;                        \
        always_false_ = false;                                 \
      } else {                                                 \
        if (UNLIKELY(*value##SIZE##_ < min##SIZE##_)) {        \
          min##SIZE##_ = *value##SIZE##_;                      \
        } else if (UNLIKELY(*value##SIZE##_ > max##SIZE##_)) { \
          max##SIZE##_ = *value##SIZE##_;                      \
        }                                                      \
      }                                                        \
    }                                                          \
  } while (false)

void DecimalMinMaxFilter::Insert4(const void* val) {
  INSERT_DECIMAL_MINMAX(4);
}

void DecimalMinMaxFilter::Insert8(const void* val) {
  INSERT_DECIMAL_MINMAX(8);
}

// Branch prediction for Decimal16 does not work very well.
void DecimalMinMaxFilter::Insert16(const void* val) {
  if (val == nullptr) return;
  const Decimal16Value* value16 = reinterpret_cast<const Decimal16Value*>(val);
  if (always_false_) {
    min16_ = *value16;
    max16_ = *value16;
    always_false_ = false;
  } else {
    if (*value16 < min16_) {
      min16_ = *value16;
    } else if (*value16 > max16_) {
      max16_ = *value16;
    }
  }
}

bool DecimalMinMaxFilter::AlwaysTrue() const { return always_true_; }

} // namespace impala
