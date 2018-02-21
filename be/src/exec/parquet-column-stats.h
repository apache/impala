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

#include "runtime/decimal-value.h"
#include "runtime/string-buffer.h"
#include "runtime/timestamp-value.h"
#include "runtime/types.h"

namespace impala {

/// This class, together with its derivatives, is used to update column statistics when
/// writing parquet files. It provides an interface to populate a parquet::Statistics
/// object and attach it to an object supplied by the caller. It can also be used to
/// decode parquet::Statistics into slots.
///
/// We currently support writing the 'min_value' and 'max_value' fields in
/// parquet::Statistics. The other two statistical values - 'null_count' and
/// 'distinct_count' - are not tracked or populated. We do not populate the deprecated
/// 'min' and 'max' fields.
///
/// Regarding the ordering of values, we follow the parquet-format specification for
/// logical types (LogicalTypes.md in parquet-format):
///
/// - Numeric values (BOOLEAN, INT, FLOAT, DOUBLE, DECIMAL) are ordered by their numeric
///   value (as opposed to their binary representation).
///
/// - Strings are ordered using bytewise, unsigned comparison.
///
/// - Timestamps are compared by numerically comparing the points in time they represent.
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
  /// Enum to select whether to read minimum or maximum statistics. Values do not
  /// correspond to fields in parquet::Statistics, but instead select between retrieving
  /// the minimum or maximum value.
  enum class StatsField { MIN, MAX };

  /// min and max functions for types that are not floating point numbers
  template <typename T, typename Enable = void>
  struct MinMaxTrait {
    static decltype(auto) MinValue(const T& a, const T& b) { return std::min(a, b); }
    static decltype(auto) MaxValue(const T& a, const T& b) { return std::max(a, b); }
  };

  /// min and max functions for floating point types
  template <typename T>
  struct MinMaxTrait<T, std::enable_if_t<std::is_floating_point<T>::value>> {
    static decltype(auto) MinValue(const T& a, const T& b) { return std::fmin(a, b); }
    static decltype(auto) MaxValue(const T& a, const T& b) { return std::fmax(a, b); }
  };

  ColumnStatsBase() : has_min_max_values_(false), null_count_(0) {}
  virtual ~ColumnStatsBase() {}

  /// Decodes the parquet::Statistics from 'col_chunk' and writes the value selected by
  /// 'stats_field' into the buffer pointed to by 'slot', based on 'col_type'. Returns
  /// true if reading statistics for columns of type 'col_type' is supported and decoding
  /// was successful, false otherwise.
  static bool ReadFromThrift(const parquet::ColumnChunk& col_chunk,
      const ColumnType& col_type, const parquet::ColumnOrder* col_order,
      StatsField stats_field, void* slot);

  // Gets the null_count statistics from the given column chunk's metadata and returns
  // it via an output parameter.
  // Returns true if the null_count stats were read successfully, false otherwise.
  static bool ReadNullCountStat(const parquet::ColumnChunk& col_chunk,
      int64_t* null_count);

  /// Merges this statistics object with values from 'other'. If other has not been
  /// initialized, then this object will not be changed.
  virtual void Merge(const ColumnStatsBase& other) = 0;

  /// Copies the contents of this object's statistics values to internal buffers. Some
  /// data types (e.g. StringValue) need to be copied at the end of processing a row
  /// batch, since the batch memory will be released. Overwrite this method in derived
  /// classes to provide the functionality.
  virtual Status MaterializeStringValuesToInternalBuffers() WARN_UNUSED_RESULT {
    return Status::OK();
  }

  /// Returns the number of bytes needed to encode the current statistics into a
  /// parquet::Statistics object.
  virtual int64_t BytesNeeded() const = 0;

  /// Encodes the current values into a Statistics thrift message.
  virtual void EncodeToThrift(parquet::Statistics* out) const = 0;

  /// Resets the state of this object.
  void Reset();

  /// Update the statistics by incrementing the null_count. It is called each time a null
  /// value is appended to the column or the statistics are merged.
  void IncrementNullCount(int64_t count) { null_count_ += count; }

 protected:
  // Copies the memory of 'value' into 'buffer' and make 'value' point to 'buffer'.
  // 'buffer' is reset before making the copy.
  static Status CopyToBuffer(StringBuffer* buffer, StringValue* value) WARN_UNUSED_RESULT;

  /// Stores whether the min and max values of the current object have been initialized.
  bool has_min_max_values_;

  // Number of null values since the last call to Reset().
  int64_t null_count_;

 private:
  /// Returns true if we support reading statistics stored in the fields 'min_value' and
  /// 'max_value' in parquet::Statistics for the type 'col_type' and the column order
  /// 'col_order'. Otherwise, returns false. If 'col_order' is nullptr, only primitive
  /// numeric types are supported.
  static bool CanUseStats(
      const ColumnType& col_type, const parquet::ColumnOrder* col_order);

  /// Returns true if we consider statistics stored in the deprecated fields 'min' and
  /// 'max' in parquet::Statistics to be correct for the type 'col_type' and the column
  /// order 'col_order'. Otherwise, returns false.
  static bool CanUseDeprecatedStats(
      const ColumnType& col_type, const parquet::ColumnOrder* col_order);
};

/// This class contains behavior specific to our in-memory formats for different types.
template <typename T>
class ColumnStats : public ColumnStatsBase {
  friend class ColumnStatsBase;
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
  /// 'mem_pool' is used to materialize string values so that the user of this class can
  /// free the memory of the original values.
  /// 'plain_encoded_value_size' specifies the size of each encoded value in plain
  /// encoding, -1 if the type is variable-length.
  ColumnStats(MemPool* mem_pool, int plain_encoded_value_size)
    : ColumnStatsBase(),
      plain_encoded_value_size_(plain_encoded_value_size),
      mem_pool_(mem_pool),
      min_buffer_(mem_pool),
      max_buffer_(mem_pool) {}

  /// Updates the statistics based on the values min_value and max_value. If necessary,
  /// initializes the statistics. It may keep a reference to either value until
  /// MaterializeStringValuesToInternalBuffers() gets called.
  void Update(const T& min_value, const T& max_value);

  /// Wrapper to call the Update function which takes in the min_value and max_value.
  void Update(const T& v) { Update(v, v); }

  virtual void Merge(const ColumnStatsBase& other) override;
  virtual Status MaterializeStringValuesToInternalBuffers() override {
    return Status::OK();
  }

  virtual int64_t BytesNeeded() const override;
  virtual void EncodeToThrift(parquet::Statistics* out) const override;

 protected:
  /// Encodes a single value using parquet's plain encoding and stores it into the binary
  /// string 'out'. String values are stored without additional encoding. 'bytes_needed'
  /// must be positive.
  static void EncodePlainValue(const T& v, int64_t bytes_needed, std::string* out);

  /// Decodes the plain encoded stats value from 'buffer' and writes the result into the
  /// buffer pointed to by 'slot'. Returns true if decoding was successful, false
  /// otherwise. For timestamps, an additional validation will be performed.
  static bool DecodePlainValue(const std::string& buffer, void* slot,
      parquet::Type::type parquet_type);

  /// Returns the number of bytes needed to encode value 'v'.
  int64_t BytesNeeded(const T& v) const;

  // Size of each encoded value in plain encoding, -1 if the type is variable-length.
  int plain_encoded_value_size_;

  // Minimum value since the last call to Reset().
  T min_value_;

  // Maximum value since the last call to Reset().
  T max_value_;

  // Memory pool to allocate from when making copies of the statistics data.
  MemPool* mem_pool_;

  // Local buffers to copy statistics data into.
  StringBuffer min_buffer_;
  StringBuffer max_buffer_;
};

} // end ns impala
#endif
