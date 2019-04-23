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

#ifndef IMPALA_EXEC_PARQUET_COLUMN_STATS_INLINE_H
#define IMPALA_EXEC_PARQUET_COLUMN_STATS_INLINE_H

#include "exec/parquet/parquet-common.h"
#include "gen-cpp/parquet_types.h"
#include "parquet-column-stats.h"
#include "runtime/string-value.inline.h"

namespace impala {

inline void ColumnStatsBase::Reset() {
  has_min_max_values_ = false;
  null_count_ = 0;
  ascending_boundary_order_ = true;
  descending_boundary_order_ = true;
}

template <typename T>
inline void ColumnStats<T>::Update(const T& min_value, const T& max_value) {
  if (!has_min_max_values_) {
    has_min_max_values_ = true;
    min_value_ = min_value;
    max_value_ = max_value;
  } else {
    min_value_ = MinMaxTrait<T>::MinValue(min_value_, min_value);
    max_value_ = MinMaxTrait<T>::MaxValue(max_value_, max_value);
  }
}

template <typename T>
inline void ColumnStats<T>::Merge(const ColumnStatsBase& other) {
  DCHECK(dynamic_cast<const ColumnStats<T>*>(&other));
  const ColumnStats<T>* cs = static_cast<const ColumnStats<T>*>(&other);
  if (cs->has_min_max_values_) {
    if (has_min_max_values_) {
      if (ascending_boundary_order_) {
        if (MinMaxTrait<T>::Compare(prev_page_max_value_, cs->max_value_) > 0 ||
            MinMaxTrait<T>::Compare(prev_page_min_value_, cs->min_value_) > 0) {
          ascending_boundary_order_ = false;
        }
      }
      if (descending_boundary_order_) {
        if (MinMaxTrait<T>::Compare(prev_page_max_value_, cs->max_value_) < 0 ||
            MinMaxTrait<T>::Compare(prev_page_min_value_, cs->min_value_) < 0) {
          descending_boundary_order_ = false;
        }
      }
    }
    Update(cs->min_value_, cs->max_value_);
    prev_page_min_value_ = cs->min_value_;
    prev_page_max_value_ = cs->max_value_;
  }
  IncrementNullCount(cs->null_count_);
}

template <typename T>
inline int64_t ColumnStats<T>::BytesNeeded() const {
  return BytesNeeded(min_value_) + BytesNeeded(max_value_)
      + ParquetPlainEncoder::ByteSize(null_count_);
}

template <typename T>
inline void ColumnStats<T>::EncodeToThrift(parquet::Statistics* out) const {
  if (has_min_max_values_) {
    std::string min_str;
    EncodePlainValue(min_value_, BytesNeeded(min_value_), &min_str);
    out->__set_min_value(move(min_str));
    std::string max_str;
    EncodePlainValue(max_value_, BytesNeeded(max_value_), &max_str);
    out->__set_max_value(move(max_str));
  }
  out->__set_null_count(null_count_);
}

template <typename T>
inline void ColumnStats<T>::EncodePlainValue(
    const T& v, int64_t bytes_needed, std::string* out) {
  DCHECK_GT(bytes_needed, 0);
  out->resize(bytes_needed);
  const int64_t bytes_written = ParquetPlainEncoder::Encode(
      v, bytes_needed, reinterpret_cast<uint8_t*>(&(*out)[0]));
  DCHECK_EQ(bytes_needed, bytes_written);
}

template <typename T>
inline bool ColumnStats<T>::DecodePlainValue(const std::string& buffer, void* slot,
    parquet::Type::type parquet_type) {
  T* result = reinterpret_cast<T*>(slot);
  int size = buffer.size();
  const uint8_t* data = reinterpret_cast<const uint8_t*>(buffer.data());
  if (ParquetPlainEncoder::DecodeByParquetType<T>(data, data + size, size, result,
      parquet_type) == -1) {
    return false;
  }
  return true;
}

template <typename T>
inline int64_t ColumnStats<T>::BytesNeeded(const T& v) const {
  return plain_encoded_value_size_ < 0 ? ParquetPlainEncoder::ByteSize<T>(v) :
      plain_encoded_value_size_;
}

/// Plain encoding for Boolean values is not handled by the ParquetPlainEncoder and thus
/// needs special handling here.
template <>
inline void ColumnStats<bool>::EncodePlainValue(
    const bool& v, int64_t bytes_needed, std::string* out) {
  char c = v;
  out->assign(1, c);
}

template <>
inline bool ColumnStats<bool>::DecodePlainValue(const std::string& buffer, void* slot,
    parquet::Type::type parquet_type) {
  bool* result = reinterpret_cast<bool*>(slot);
  DCHECK(buffer.size() == 1);
  *result = (buffer[0] != 0);
  return true;
}

template <>
inline int64_t ColumnStats<bool>::BytesNeeded(const bool& v) const {
  return 1;
}

namespace {
/// Decodes the plain encoded stats value from 'buffer' and writes the result into
/// 'result'. Returns true if decoding was successful, false otherwise. Doesn't perform
/// any validation.
/// Used as a helper function for decoding values that need additional custom validation.
template <typename T, parquet::Type::type ParquetType>
inline bool DecodePlainValueNoValidation(const string& buffer, T* result) {
  int size = buffer.size();
  const uint8_t* data = reinterpret_cast<const uint8_t*>(buffer.data());
  return ParquetPlainEncoder::Decode<T, ParquetType>(
      data, data + size, size, result) != -1;
}

}

/// Timestamp values need validation.
template <>
inline bool ColumnStats<TimestampValue>::DecodePlainValue(
    const std::string& buffer, void* slot, parquet::Type::type parquet_type) {
  if (UNLIKELY(parquet_type != parquet::Type::INT96)) {
    DCHECK(false);
    return false;
  }

  TimestampValue* result = reinterpret_cast<TimestampValue*>(slot);
  if (!DecodePlainValueNoValidation<TimestampValue, parquet::Type::INT96>(buffer,
      result)) {
    return false;
  }

  // We don't need to convert the value here, because it is done by the caller.
  // If this function were not static, then it would be possible to store the information
  // needed for timezone conversion in the object and do the conversion here.
  return TimestampValue::IsValidDate(result->date());
}

/// Date values need validation.
template <>
inline bool ColumnStats<DateValue>::DecodePlainValue(
    const std::string& buffer, void* slot, parquet::Type::type parquet_type) {
  DateValue* result = reinterpret_cast<DateValue*>(slot);
  if (!DecodePlainValueNoValidation<DateValue, parquet::Type::INT32>(buffer, result)) {
    return false;
  }

  return result->IsValid();
}

/// parquet::Statistics stores string values directly and does not use plain encoding.
template <>
inline void ColumnStats<StringValue>::EncodePlainValue(
    const StringValue& v, int64_t bytes_needed, string* out) {
  out->assign(v.ptr, v.len);
}

template <>
inline bool ColumnStats<StringValue>::DecodePlainValue(
    const std::string& buffer, void* slot, parquet::Type::type parquet_type) {
  StringValue* result = reinterpret_cast<StringValue*>(slot);
  result->ptr = const_cast<char*>(buffer.data());
  result->len = buffer.size();
  return true;
}

template <>
inline void ColumnStats<StringValue>::Update(
    const StringValue& min_value, const StringValue& max_value) {
  if (!has_min_max_values_) {
    has_min_max_values_ = true;
    min_value_ = min_value;
    min_buffer_.Clear();
    max_value_ = max_value;
    max_buffer_.Clear();
  } else {
    if (min_value < min_value_) {
      min_value_ = min_value;
      min_buffer_.Clear();
    }
    if (max_value > max_value_) {
      max_value_ = max_value;
      max_buffer_.Clear();
    }
  }
}

template <>
inline void ColumnStats<StringValue>::Merge(const ColumnStatsBase& other) {
  DCHECK(dynamic_cast<const ColumnStats<StringValue>*>(&other));
  const ColumnStats<StringValue>* cs = static_cast<
      const ColumnStats<StringValue>*>(&other);
  if (cs->has_min_max_values_) {
    if (has_min_max_values_) {
      // Make sure that we copied the previous page's min/max values
      // to their own buffer.
      if (prev_page_min_value_.ptr != nullptr) {
        DCHECK_NE(static_cast<void*>(prev_page_min_value_.ptr),
                  static_cast<void*>(cs->min_value_.ptr));
      }
      if (prev_page_max_value_.ptr != nullptr) {
        DCHECK_NE(static_cast<void*>(prev_page_max_value_.ptr),
                  static_cast<void*>(cs->max_value_.ptr));
      }
      if (ascending_boundary_order_) {
        if (prev_page_max_value_ > cs->max_value_ ||
            prev_page_min_value_ > cs->min_value_) {
          ascending_boundary_order_ = false;
        }
      }
      if (descending_boundary_order_) {
        if (prev_page_max_value_ < cs->max_value_ ||
            prev_page_min_value_ < cs->min_value_) {
          descending_boundary_order_ = false;
        }
      }
    }
    Update(cs->min_value_, cs->max_value_);
    prev_page_min_value_ = cs->min_value_;
    prev_page_max_value_ = cs->max_value_;
    prev_page_min_buffer_.Clear();
    prev_page_max_buffer_.Clear();
  }
  IncrementNullCount(cs->null_count_);
}

// StringValues need to be copied at the end of processing a row batch, since the batch
// memory will be released.
template <>
inline Status ColumnStats<StringValue>::MaterializeStringValuesToInternalBuffers() {
  if (min_buffer_.IsEmpty()) RETURN_IF_ERROR(CopyToBuffer(&min_buffer_, &min_value_));
  if (max_buffer_.IsEmpty()) RETURN_IF_ERROR(CopyToBuffer(&max_buffer_, &max_value_));
  if (prev_page_min_buffer_.IsEmpty()) {
    RETURN_IF_ERROR(CopyToBuffer(&prev_page_min_buffer_, &prev_page_min_value_));
  }
  if (prev_page_max_buffer_.IsEmpty()) {
    RETURN_IF_ERROR(CopyToBuffer(&prev_page_max_buffer_, &prev_page_max_value_));
  }
  return Status::OK();
}

} // end ns impala
#endif
