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

#ifndef IMPALA_EXEC_PARQUET_COLUMN_STATS_H
#define IMPALA_EXEC_PARQUET_COLUMN_STATS_H

#include <type_traits>

namespace impala {

/// This class, together with its derivatives, is used to track column statistics when
/// writing parquet files. It provides an interface to populate a parquet::Statistics
/// object and attach it to an object supplied by the caller.
///
/// We currently support tracking 'min' and 'max' values for statistics. The other two
/// statistical values in parquet.thrift, 'null_count' and 'distinct_count' are not
/// tracked or populated.
///
/// Regarding the ordering of values, we follow the parquet-mr reference implementation.
///
/// Numeric values (BOOLEAN, INT, FLOAT, DOUBLE) are ordered by their numeric
/// value (as opposed to their binary representation).
///
/// We currently don't write statistics for DECIMAL values and character array values
/// (CHAR, VARCHAR, STRING) due to several issues with parquet-mr and subsequently, Hive
/// (PARQUET-251, PARQUET-686). For those types, the Update() method is empty, so that the
/// stats are not tracked.
///
/// NULL values are not considered for min/max statistics, and if a column consists only
/// of NULL values, then no min/max statistics are written.
///
/// Updating the statistics is handled in derived classes to alleviate the need for
/// virtual function calls.
///
/// TODO: Populate null_count and distinct_count.
class ColumnStatsBase {
 public:
  ColumnStatsBase() : has_values_(false) {}
  virtual ~ColumnStatsBase() {}

  /// Merges this statistics object with values from 'other'. If other has not been
  /// initialized, then this object will not be changed.
  virtual void Merge(const ColumnStatsBase& other) = 0;

  /// Returns the number of bytes needed to encode the current statistics into a
  /// parquet::Statistics object.
  virtual int64_t BytesNeeded() const = 0;

  /// Encodes the current values into a Statistics thrift message.
  virtual void EncodeToThrift(parquet::Statistics* out) const = 0;

  /// Resets the state of this object.
  void Reset() { has_values_ = false; }

  bool has_values() const { return has_values_; }

 protected:
  /// Stores whether the current object has been initialized with a set of values.
  bool has_values_;
};

/// This class contains the type-specific behavior to track statistics per column.
template <typename T>
class ColumnStats : public ColumnStatsBase {
  // We explicitly require types to be listed here in order to support column statistics.
  // When adding a type here, users of this class need to ensure that the statistics
  // follow the ordering semantics of parquet's min/max statistics for the new type.
  // Details on how the values should be ordered can be found in the 'parquet-format'
  // project in 'parquet.thrift' and 'LogicalTypes.md'.
  using value_type = typename std::enable_if<
      std::is_arithmetic<T>::value
        || std::is_same<bool, T>::value
        || std::is_same<StringValue, T>::value
        || std::is_same<TimestampValue, T>::value
        || std::is_same<Decimal4Value, T>::value
        || std::is_same<Decimal8Value, T>::value
        || std::is_same<Decimal16Value, T>::value,
      T>::type;

 public:
  ColumnStats(int plain_encoded_value_size)
    : ColumnStatsBase(), plain_encoded_value_size_(plain_encoded_value_size) {}

  /// Updates the statistics based on the value 'v'. If necessary, initializes the
  /// statistics.
  void Update(const T& v) {
    if (!has_values_) {
      has_values_ = true;
      min_value_ = v;
      max_value_ = v;
    } else {
      min_value_ = min(min_value_, v);
      max_value_ = max(max_value_, v);
    }
  }

  virtual void Merge(const ColumnStatsBase& other) override {
    DCHECK(dynamic_cast<const ColumnStats<T>*>(&other));
    const ColumnStats<T>* cs = static_cast<const ColumnStats<T>*>(&other);
    if (!cs->has_values_) return;
    if (!has_values_) {
      has_values_ = true;
      min_value_ = cs->min_value_;
      max_value_ = cs->max_value_;
    } else {
      min_value_ = min(min_value_, cs->min_value_);
      max_value_ = max(max_value_, cs->max_value_);
    }
  }

  virtual int64_t BytesNeeded() const override {
    return BytesNeededInternal(min_value_) + BytesNeededInternal(max_value_);
  }

  virtual void EncodeToThrift(parquet::Statistics* out) const override {
    DCHECK(has_values_);
    string min_str;
    EncodeValueToString(min_value_, &min_str);
    out->__set_min(move(min_str));
    string max_str;
    EncodeValueToString(max_value_, &max_str);
    out->__set_max(move(max_str));
  }

 protected:
  /// Encodes a single value using parquet's PLAIN encoding and stores it into the
  /// binary string 'out'.
  void EncodeValueToString(const T& v, string* out) const {
    int64_t bytes_needed = BytesNeededInternal(v);
    out->resize(bytes_needed);
    int64_t bytes_written = ParquetPlainEncoder::Encode(
        reinterpret_cast<uint8_t*>(&(*out)[0]), bytes_needed, v);
    DCHECK_EQ(bytes_needed, bytes_written);
  }

  /// Returns the number of bytes needed to encode value 'v'.
  int64_t BytesNeededInternal(const T& v) const {
    return plain_encoded_value_size_ < 0 ? ParquetPlainEncoder::ByteSize<T>(v) :
        plain_encoded_value_size_;
  }

  // Size of each encoded value in plain encoding, -1 if the type is variable-length.
  int plain_encoded_value_size_;

  // Minimum value since the last call to Reset().
  T min_value_;

  // Maximum value since the last call to Reset().
  T max_value_;
};

/// Plain encoding for Boolean values is not handled by the ParquetPlainEncoder and thus
/// needs special handling here.
template <>
void ColumnStats<bool>::EncodeValueToString(const bool& v, string* out) const {
  char c = v;
  out->assign(1, c);
}

template <>
int64_t ColumnStats<bool>::BytesNeededInternal(const bool& v) const {
  return 1;
}

/// parquet-mr and subsequently Hive currently do not handle the following types
/// correctly (PARQUET-251, PARQUET-686), so we disable support for them.
/// The relevant Impala Jiras are for
/// - StringValue    IMPALA-4817
/// - TimestampValue IMPALA-4819
/// - DecimalValue   IMPALA-4815
template <>
void ColumnStats<StringValue>::Update(const StringValue& v) {}

template <>
void ColumnStats<TimestampValue>::Update(const TimestampValue& v) {}

template <>
void ColumnStats<Decimal4Value>::Update(const Decimal4Value& v) {}

template <>
void ColumnStats<Decimal8Value>::Update(const Decimal8Value& v) {}

template <>
void ColumnStats<Decimal16Value>::Update(const Decimal16Value& v) {}

} // end ns impala
#endif
