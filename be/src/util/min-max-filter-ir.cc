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

#include "runtime/string-value.inline.h"
#include "util/min-max-filter.h"

using std::string;

namespace impala {

template <typename T>
void AddOne(T& v) {
  v++;
}

template <>
void AddOne(bool& v) {
  v = true;
}

template <typename T>
void SubtractOne(T& v) {
  v--;
}

template <>
void SubtractOne(bool& v) {
  v = false;
}

#define NUMERIC_MIN_MAX_FILTER_FUNCS(NAME, TYPE)                   \
  void NAME##MinMaxFilter::Insert(const void* val) {               \
    if (LIKELY(val)) {                                             \
      const TYPE* value = reinterpret_cast<const TYPE*>(val);      \
      if (UNLIKELY(*value < min_)) min_ = *value;                  \
      if (UNLIKELY(*value > max_)) max_ = *value;                  \
    }                                                              \
  }                                                                \
  void NAME##MinMaxFilter::InsertForLE(const void* val) {          \
    if (LIKELY(val)) {                                             \
      const TYPE* value = reinterpret_cast<const TYPE*>(val);      \
      min_ = std::numeric_limits<TYPE>::lowest();                  \
      if (UNLIKELY(*value > max_)) max_ = *value;                  \
    }                                                              \
  }                                                                \
  void NAME##MinMaxFilter::InsertForLT(const void* val) {          \
    if (LIKELY(val)) {                                             \
      TYPE value = *reinterpret_cast<const TYPE*>(val);            \
      min_ = std::numeric_limits<TYPE>::lowest();                  \
      if (value > std::numeric_limits<TYPE>::lowest()) {           \
        SubtractOne(value);                                        \
      }                                                            \
      if (UNLIKELY(value > max_)) max_ = value;                    \
    }                                                              \
  }                                                                \
  void NAME##MinMaxFilter::InsertForGE(const void* val) {          \
    if (LIKELY(val)) {                                             \
      max_ = std::numeric_limits<TYPE>::max();                     \
      const TYPE* value = reinterpret_cast<const TYPE*>(val);      \
      if (UNLIKELY(*value < min_)) min_ = *value;                  \
    }                                                              \
  }                                                                \
  void NAME##MinMaxFilter::InsertForGT(const void* val) {          \
    if (LIKELY(val)) {                                             \
      max_ = std::numeric_limits<TYPE>::max();                     \
      TYPE value = *reinterpret_cast<const TYPE*>(val);            \
      if (value < std::numeric_limits<TYPE>::max()) AddOne(value); \
      if (UNLIKELY(value < min_)) min_ = value;                    \
    }                                                              \
  }                                                                \
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

void StringMinMaxFilter::UpdateMax(const StringValue& value) {
  if (UNLIKELY(always_false_)) {
    min_ = MIN_BOUND_STRING;
    max_ = value;
    always_false_ = false;
  } else {
    if (UNLIKELY(max_ < value)) {
      max_ = value;
    }
  }
  // Transfer the ownership of memory used in value to max_buffer. Truncation is
  // possible if max_ is longer than MAX_BOUND_LENGTH bytes.
  MaterializeMaxValue();
}

void StringMinMaxFilter::InsertForLE(const void* val) {
  if (LIKELY(val)) {
    const StringValue* value = reinterpret_cast<const StringValue*>(val);
    UpdateMax(*value);
  }
}

void StringMinMaxFilter::InsertForLT(const void* val) {
  if (LIKELY(val)) {
    std::string result =
        reinterpret_cast<const StringValue*>(val)->LargestSmallerString();
    if (result.size() > 0) {
      UpdateMax(StringValue(result));
    } else {
      always_true_ = true;
    }
  }
}

void StringMinMaxFilter::UpdateMin(const StringValue& value) {
  if (UNLIKELY(always_false_)) {
    min_ = value;
    max_ = MAX_BOUND_STRING;
    always_false_ = false;
  } else {
    if (UNLIKELY(value < min_)) {
      min_ = value;
    }
  }
  // Transfer the ownership of memory used in value to min_buffer. Truncation is
  // possible if min_ is longer than MAX_BOUND_LENGTH bytes.
  MaterializeMinValue();
}

void StringMinMaxFilter::InsertForGE(const void* val) {
  if (LIKELY(val)) {
    const StringValue* value = reinterpret_cast<const StringValue*>(val);
    UpdateMin(*value);
  }
}

void StringMinMaxFilter::InsertForGT(const void* val) {
  if (LIKELY(val)) {
    std::string result = reinterpret_cast<const StringValue*>(val)->LeastLargerString();
    if (result.size() <= MAX_BOUND_LENGTH) {
      UpdateMin(StringValue(result));
    } else {
      always_true_ = true;
    }
  }
}

bool StringMinMaxFilter::AlwaysTrue() const {
  return always_true_;
}

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

void DateMinMaxFilter::UpdateMax(const DateValue& value) {
  if (UNLIKELY(always_false_)) {
    min_ = DateValue::MIN_DATE;
    max_ = value;
    always_false_ = false;
  } else if (UNLIKELY(value > max_)) {
    max_ = value;
  }
}

void DateMinMaxFilter::InsertForLE(const void* val) {
  if (LIKELY(val)) {
    const DateValue* value = reinterpret_cast<const DateValue*>(val);
    UpdateMax(*value);
  }
}

void DateMinMaxFilter::InsertForLT(const void* val) {
  if (LIKELY(val)) {
    DateValue value = *reinterpret_cast<const DateValue*>(val);
    if (value > DateValue::MIN_DATE) value = value.SubtractDays(1);
    UpdateMax(value);
  }
}

void DateMinMaxFilter::UpdateMin(const DateValue& value) {
  if (UNLIKELY(always_false_)) {
    min_ = value;
    max_ = DateValue::MAX_DATE;
    always_false_ = false;
  } else if (UNLIKELY(value < min_)) {
    min_ = value;
  }
}

void DateMinMaxFilter::InsertForGE(const void* val) {
  if (LIKELY(val)) {
    const DateValue* value = reinterpret_cast<const DateValue*>(val);
    UpdateMin(*value);
  }
}

void DateMinMaxFilter::InsertForGT(const void* val) {
  if (LIKELY(val)) {
    DateValue value = *reinterpret_cast<const DateValue*>(val);
    if (value < DateValue::MAX_DATE) value = value.AddDays(1);
    UpdateMin(value);
  }
}

void TimestampMinMaxFilter::UpdateMax(const TimestampValue& value) {
  if (UNLIKELY(always_false_)) {
    min_ = TimestampValue::GetMinValue();
    max_ = value;
    always_false_ = false;
  } else if (UNLIKELY(value > max_)) {
    max_ = value;
  }
}

void TimestampMinMaxFilter::InsertForLE(const void* val) {
  if (LIKELY(val)) {
    const TimestampValue* value = reinterpret_cast<const TimestampValue*>(val);
    UpdateMax(*value);
  }
}

void TimestampMinMaxFilter::InsertForLT(const void* val) {
  if (LIKELY(val)) {
    TimestampValue value = *reinterpret_cast<const TimestampValue*>(val);
    if (TimestampValue::GetMinValue() < value) {
      // subtract one nanosecond.
      value = value.Subtract(boost::posix_time::time_duration(0, 0, 0, 1));
    }
    UpdateMax(value);
  }
}

void TimestampMinMaxFilter::UpdateMin(const TimestampValue& value) {
  if (UNLIKELY(always_false_)) {
    min_ = value;
    max_ = TimestampValue::GetMaxValue();
    always_false_ = false;
  } else if (UNLIKELY(value < min_)) {
    min_ = value;
  }
}

void TimestampMinMaxFilter::InsertForGE(const void* val) {
  if (LIKELY(val)) {
    const TimestampValue* value = reinterpret_cast<const TimestampValue*>(val);
    UpdateMin(*value);
  }
}

void TimestampMinMaxFilter::InsertForGT(const void* val) {
  if (LIKELY(val)) {
    TimestampValue value = *reinterpret_cast<const TimestampValue*>(val);
    if (value < TimestampValue::GetMaxValue()) {
      // Add one nanosecond.
      value = value.Add(boost::posix_time::time_duration(0, 0, 0, 1));
    }
    UpdateMin(value);
  }
}

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

#define UPDATE_MAX_DECIMAL_MINMAX(TYPE, SIZE)                              \
  void DecimalMinMaxFilter::UpdateMax(const Decimal##SIZE##Value& value) { \
    if (UNLIKELY(always_false_)) {                                         \
      min##SIZE##_.set_value(std::numeric_limits<TYPE>::min());            \
      max##SIZE##_ = value;                                                \
      always_false_ = false;                                               \
    } else {                                                               \
      if (UNLIKELY(value > max##SIZE##_)) {                                \
        max##SIZE##_ = value;                                              \
      }                                                                    \
    }                                                                      \
  }

UPDATE_MAX_DECIMAL_MINMAX(int32_t, 4);
UPDATE_MAX_DECIMAL_MINMAX(int64_t, 8);
UPDATE_MAX_DECIMAL_MINMAX(__int128_t, 16);

#define INSERT_DECIMAL_MINMAX_FOR_LE(TYPE, SIZE)               \
  do {                                                         \
    if (LIKELY(val)) {                                         \
      const Decimal##SIZE##Value* value =                      \
          reinterpret_cast<const Decimal##SIZE##Value*>(val);  \
      UpdateMax(*value);                                       \
    }                                                          \
  } while (false)

#define INSERT_DECIMAL_MINMAX_FOR_LT(TYPE, SIZE)                                \
  do {                                                                          \
    if (LIKELY(val)) {                                                          \
      TYPE value = reinterpret_cast<const Decimal##SIZE##Value*>(val)->value(); \
      if (value > std::numeric_limits<TYPE>::min()) value--;                    \
      UpdateMax(Decimal##SIZE##Value(value));                                   \
    }                                                                           \
  } while (false)

void DecimalMinMaxFilter::Insert4ForLE(const void* val) {
  INSERT_DECIMAL_MINMAX_FOR_LE(int32_t, 4);
}

void DecimalMinMaxFilter::Insert8ForLE(const void* val) {
  INSERT_DECIMAL_MINMAX_FOR_LE(int64_t, 8);
}

void DecimalMinMaxFilter::Insert16ForLE(const void* val) {
  INSERT_DECIMAL_MINMAX_FOR_LE(__int128_t, 16);
}

void DecimalMinMaxFilter::Insert4ForLT(const void* val) {
  INSERT_DECIMAL_MINMAX_FOR_LT(int32_t, 4);
}

void DecimalMinMaxFilter::Insert8ForLT(const void* val) {
  INSERT_DECIMAL_MINMAX_FOR_LT(int64_t, 8);
}

void DecimalMinMaxFilter::Insert16ForLT(const void* val) {
  INSERT_DECIMAL_MINMAX_FOR_LT(__int128_t, 16);
}

#define UPDATE_MIN_DECIMAL_MINMAX(TYPE, SIZE)                              \
  void DecimalMinMaxFilter::UpdateMin(const Decimal##SIZE##Value& value) { \
    if (UNLIKELY(always_false_)) {                                         \
      min##SIZE##_ = value;                                                \
      max##SIZE##_.set_value(std::numeric_limits<TYPE>::max());            \
      always_false_ = false;                                               \
    } else {                                                               \
      if (UNLIKELY(value < min##SIZE##_)) {                                \
        min##SIZE##_ = value;                                              \
      }                                                                    \
    }                                                                      \
  }

UPDATE_MIN_DECIMAL_MINMAX(int32_t, 4);
UPDATE_MIN_DECIMAL_MINMAX(int64_t, 8);
UPDATE_MIN_DECIMAL_MINMAX(__int128_t, 16);

#define INSERT_DECIMAL_MINMAX_FOR_GE(TYPE, SIZE)               \
  do {                                                         \
    if (LIKELY(val)) {                                         \
      const Decimal##SIZE##Value* value =                      \
          reinterpret_cast<const Decimal##SIZE##Value*>(val); \
      UpdateMin(*value);                                       \
    }                                                          \
  } while (false)

#define INSERT_DECIMAL_MINMAX_FOR_GT(TYPE, SIZE)                                \
  do {                                                                          \
    if (LIKELY(val)) {                                                          \
      TYPE value = reinterpret_cast<const Decimal##SIZE##Value*>(val)->value(); \
      if (value < std::numeric_limits<TYPE>::max()) value++;                    \
      UpdateMin(Decimal##SIZE##Value(value));                                   \
    }                                                                           \
  } while (false)

void DecimalMinMaxFilter::Insert4ForGE(const void* val) {
  INSERT_DECIMAL_MINMAX_FOR_GE(int32_t, 4);
}

void DecimalMinMaxFilter::Insert8ForGE(const void* val) {
  INSERT_DECIMAL_MINMAX_FOR_GE(int64_t, 8);
}

void DecimalMinMaxFilter::Insert16ForGE(const void* val) {
  INSERT_DECIMAL_MINMAX_FOR_GE(__int128_t, 16);
}

void DecimalMinMaxFilter::Insert4ForGT(const void* val) {
  INSERT_DECIMAL_MINMAX_FOR_GT(int32_t, 4);
}

void DecimalMinMaxFilter::Insert8ForGT(const void* val) {
  INSERT_DECIMAL_MINMAX_FOR_GT(int64_t, 8);
}

void DecimalMinMaxFilter::Insert16ForGT(const void* val) {
  INSERT_DECIMAL_MINMAX_FOR_GT(__int128_t, 16);
}

bool DecimalMinMaxFilter::AlwaysTrue() const {
  return always_true_;
}

} // namespace impala
