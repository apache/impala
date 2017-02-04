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

#include "parquet-column-stats.h"

namespace impala {

template <typename T>
inline bool ColumnStats<T>::ReadFromThrift(const parquet::Statistics& thrift_stats,
    const StatsField& stats_field, void* slot) {
  T* out = reinterpret_cast<T*>(slot);
  switch (stats_field) {
    case StatsField::MIN:
      return DecodeValueFromThrift(thrift_stats.min, out);
    case StatsField::MAX:
      return DecodeValueFromThrift(thrift_stats.max, out);
    default:
      DCHECK(false) << "Unsupported statistics field requested";
      return false;
  }
}

template <typename T>
inline void ColumnStats<T>::Update(const T& v) {
  if (!has_values_) {
    has_values_ = true;
    min_value_ = v;
    max_value_ = v;
  } else {
    min_value_ = std::min(min_value_, v);
    max_value_ = std::max(max_value_, v);
  }
}

template <typename T>
inline void ColumnStats<T>::Merge(const ColumnStatsBase& other) {
  DCHECK(dynamic_cast<const ColumnStats<T>*>(&other));
  const ColumnStats<T>* cs = static_cast<const ColumnStats<T>*>(&other);
  if (!cs->has_values_) return;
  if (!has_values_) {
    has_values_ = true;
    min_value_ = cs->min_value_;
    max_value_ = cs->max_value_;
  } else {
    min_value_ = std::min(min_value_, cs->min_value_);
    max_value_ = std::max(max_value_, cs->max_value_);
  }
}

template <typename T>
inline int64_t ColumnStats<T>::BytesNeeded() const {
  return BytesNeededInternal(min_value_) + BytesNeededInternal(max_value_);
}

template <typename T>
inline void ColumnStats<T>::EncodeToThrift(parquet::Statistics* out) const {
  DCHECK(has_values_);
  std::string min_str;
  EncodeValueToString(min_value_, &min_str);
  out->__set_min(move(min_str));
  std::string max_str;
  EncodeValueToString(max_value_, &max_str);
  out->__set_max(move(max_str));
}

template <typename T>
inline void ColumnStats<T>::EncodeValueToString(const T& v, std::string* out) const {
  int64_t bytes_needed = BytesNeededInternal(v);
  out->resize(bytes_needed);
  int64_t bytes_written = ParquetPlainEncoder::Encode(
      reinterpret_cast<uint8_t*>(&(*out)[0]), bytes_needed, v);
  DCHECK_EQ(bytes_needed, bytes_written);
}

template <typename T>
inline bool ColumnStats<T>::DecodeValueFromThrift(const std::string& buffer, T* result) {
  int size = buffer.size();
  // The ParquetPlainEncoder interface expects mutable pointers.
  uint8_t* data = const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(&buffer[0]));
  if (ParquetPlainEncoder::Decode(data, data + size, -1, result) == -1) return false;
  return true;
}

template <typename T>
inline int64_t ColumnStats<T>::BytesNeededInternal(const T& v) const {
  return plain_encoded_value_size_ < 0 ? ParquetPlainEncoder::ByteSize<T>(v) :
      plain_encoded_value_size_;
}

/// Plain encoding for Boolean values is not handled by the ParquetPlainEncoder and thus
/// needs special handling here.
template <>
inline void ColumnStats<bool>::EncodeValueToString(const bool& v, std::string* out) const
{
  char c = v;
  out->assign(1, c);
}

template <>
inline bool ColumnStats<bool>::DecodeValueFromThrift(const std::string& buffer,
    bool* result) {
  DCHECK(buffer.size() == 1);
  *result = (buffer[0] != 0);
  return true;
}

template <>
inline int64_t ColumnStats<bool>::BytesNeededInternal(const bool& v) const {
  return 1;
}

/// parquet-mr and subsequently Hive currently do not handle the following types
/// correctly (PARQUET-251, PARQUET-686), so we disable support for them.
/// The relevant Impala Jiras are for
/// - StringValue    IMPALA-4817
/// - TimestampValue IMPALA-4819
/// - DecimalValue   IMPALA-4815
template <>
inline void ColumnStats<StringValue>::Update(const StringValue& v) {}

template <>
inline void ColumnStats<TimestampValue>::Update(const TimestampValue& v) {}

template <>
inline void ColumnStats<Decimal4Value>::Update(const Decimal4Value& v) {}

template <>
inline void ColumnStats<Decimal8Value>::Update(const Decimal8Value& v) {}

template <>
inline void ColumnStats<Decimal16Value>::Update(const Decimal16Value& v) {}


} // end ns impala
#endif
