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

#include <string>
#include <type_traits>

#include "runtime/timestamp-value.h"
#include "runtime/types.h"
#include "exec/parquet-common.h"

namespace impala {

/// This class, together with its derivatives, is used to track column statistics when
/// writing parquet files. It provides an interface to populate a parquet::Statistics
/// object and attach it to an object supplied by the caller. It can also be used to
/// decode parquet::Statistics into slots.
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
  /// Enum to select statistics value when reading from parquet::Statistics structs.
  enum class StatsField { MAX, MIN, NULL_COUNT, DISTINCT_COUNT };

  ColumnStatsBase() : has_values_(false) {}
  virtual ~ColumnStatsBase() {}

  /// Decodes the parquet::Statistics from 'row_group' and writes the value selected by
  /// 'stats_field' into the buffer pointed to by 'slot', based on 'col_type'. Returns
  /// 'true' if reading statistics for columns of type 'col_type' is supported and
  /// decoding was successful, 'false' otherwise.
  static bool ReadFromThrift(const parquet::Statistics& thrift_stats,
      const ColumnType& col_type, const StatsField& stats_field, void* slot);

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

  /// Decodes the parquet::Statistics from 'row_group' and writes the value selected by
  /// 'stats_field' into the buffer pointed to by 'slot'. Returns 'true' if reading
  /// statistics for columns of type 'col_type' is supported and decoding was successful,
  /// 'false' otherwise.
  static bool ReadFromThrift(const parquet::Statistics& thrift_stats,
      const StatsField& stats_field, void* slot);

  /// Updates the statistics based on the value 'v'. If necessary, initializes the
  /// statistics.
  void Update(const T& v);

  virtual void Merge(const ColumnStatsBase& other) override;
  virtual int64_t BytesNeeded() const override;
  virtual void EncodeToThrift(parquet::Statistics* out) const override;

 protected:
  /// Encodes a single value using parquet's PLAIN encoding and stores it into the
  /// binary string 'out'.
  void EncodeValueToString(const T& v, std::string* out) const;

  /// Decodes a statistics values from 'buffer' into 'result'.
  static bool DecodeValueFromThrift(const std::string& buffer, T* result);

  /// Returns the number of bytes needed to encode value 'v'.
  int64_t BytesNeededInternal(const T& v) const;

  // Size of each encoded value in plain encoding, -1 if the type is variable-length.
  int plain_encoded_value_size_;

  // Minimum value since the last call to Reset().
  T min_value_;

  // Maximum value since the last call to Reset().
  T max_value_;
};

} // end ns impala
#endif
