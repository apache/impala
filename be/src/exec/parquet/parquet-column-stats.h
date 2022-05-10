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

#include "exec/hdfs-table-writer.h"
#include "exec/parquet/parquet-common.h"
#include "runtime/date-value.h"
#include "runtime/decimal-value.h"
#include "runtime/string-buffer.h"
#include "runtime/timestamp-value.h"
#include "runtime/types.h"

#include "gen-cpp/parquet_types.h"

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
/// - Dates are compared by numerically comparing the days since epoch values.
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
  /// min and max functions for types that are not floating point numbers
  template <typename T, typename Enable = void>
  struct MinMaxTrait {
    static decltype(auto) MinValue(const T& a, const T& b) { return std::min(a, b); }
    static decltype(auto) MaxValue(const T& a, const T& b) { return std::max(a, b); }
    static int Compare(const T& a, const T& b) {
      if (a < b) return -1;
      if (a > b) return 1;
      return 0;
    }
  };

  /// min and max functions for floating point types
  template <typename T>
  struct MinMaxTrait<T, std::enable_if_t<std::is_floating_point<T>::value>> {
    static decltype(auto) MinValue(const T& a, const T& b) { return std::fmin(a, b); }
    static decltype(auto) MaxValue(const T& a, const T& b) { return std::fmax(a, b); }
    static int Compare(const T& a, const T& b) {
      //TODO: Should be aligned with PARQUET-1222, once resolved
      if (a == b) return 0;
      if (std::isnan(a) && std::isnan(b)) return 0;
      if (MaxValue(a, b) == a) return 1;
      return -1;
    }
  };

  ColumnStatsBase() : has_min_max_values_(false), null_count_(0) {}
  virtual ~ColumnStatsBase() {}

  /// Merges this statistics object with values from 'other'. If other has not been
  /// initialized, then this object will not be changed. It maintains internal state that
  /// tracks whether the min/max values are ordered.
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

  /// Writes the current values into IcebergColumnStats struct.
  /// Min and max stats are Single-value serialized.
  virtual void GetIcebergStats(int64_t column_size, int64_t value_count,
                               IcebergColumnStats* out) const = 0;

  /// Resets the state of this object.
  void Reset();

  /// Update the statistics by incrementing the null_count. It is called each time a null
  /// value is appended to the column or the statistics are merged.
  void IncrementNullCount(int64_t count) { null_count_ += count; }

  /// Returns the boundary order of the pages. That is, whether the lists of min/max
  /// elements inside the ColumnIndex are ordered and if so, in which direction.
  /// If both 'ascending_boundary_order_' and 'descending_boundary_order_' is true,
  /// it means all elements are equal, we choose ascending order in this case.
  /// If only one flag is true, or both of them is false, then we return the identified
  /// ordering, or unordered.
  parquet::BoundaryOrder::type GetBoundaryOrder() const {
    if (ascending_boundary_order_) return parquet::BoundaryOrder::ASCENDING;
    if (descending_boundary_order_) return parquet::BoundaryOrder::DESCENDING;
    return parquet::BoundaryOrder::UNORDERED;
  }

 protected:
  // Copies the memory of 'value' into 'buffer' and make 'value' point to 'buffer'.
  // 'buffer' is reset before making the copy.
  static Status CopyToBuffer(StringBuffer* buffer, StringValue* value) WARN_UNUSED_RESULT;

  /// Stores whether the min and max values of the current object have been initialized.
  bool has_min_max_values_;

  // Number of null values since the last call to Reset().
  int64_t null_count_;

  // If true, min/max values are ascending.
  // We assume the values are ascending, so start with true and only make it false when
  // we find a descending value. If not all values are equal, then at least one of
  // 'ascending_boundary_order_' and 'descending_boundary_order_' will be false.
  bool ascending_boundary_order_ = true;

  // If true, min/max values are descending.
  // See description of 'ascending_boundary_order_'.
  bool descending_boundary_order_ = true;
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
        || std::is_same<Decimal16Value, T>::value
        || std::is_same<DateValue, T>::value,
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
      max_buffer_(mem_pool),
      prev_page_min_buffer_(mem_pool),
      prev_page_max_buffer_(mem_pool) {}

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

  virtual void GetIcebergStats(int64_t column_size, int64_t value_count,
      IcebergColumnStats* out) const override;

  /// Decodes the plain encoded stats value from 'buffer' and writes the result into the
  /// buffer pointed to by 'slot'. Returns true if decoding was successful, false
  /// otherwise. For timestamps and dates an additional validation will be performed.
  static bool DecodePlainValue(const std::string& buffer, void* slot,
      parquet::Type::type parquet_type);

 protected:
  /// For BE tests.
  FRIEND_TEST(SerializeSingleValueTest, Decimal);

  /// Encodes a single value using parquet's plain encoding and stores it into the binary
  /// string 'out'. String values are stored without additional encoding. 'bytes_needed'
  /// must be positive.
  static void EncodePlainValue(const T& v, int64_t bytes_needed, std::string* out);

  /// Single-value serialize value 'v'.
  /// https://iceberg.apache.org/spec/#appendix-d-single-value-serialization
  static void SerializeIcebergSingleValue(const T& v, std::string* out);

  /// Returns the number of bytes needed to encode value 'v'.
  int64_t BytesNeeded(const T& v) const;

  // Size of each encoded value in plain encoding, -1 if the type is variable-length.
  int plain_encoded_value_size_;

  // Minimum value since the last call to Reset().
  T min_value_;

  // Maximum value since the last call to Reset().
  T max_value_;

  // Minimum value of the previous page. Need to store that to calculate boundary order.
  T prev_page_min_value_;

  // Maximum value of the previous page. Need to store that to calculate boundary order.
  T prev_page_max_value_;

  // Memory pool to allocate from when making copies of the statistics data.
  MemPool* mem_pool_;

  // Local buffers to copy statistics data into.
  StringBuffer min_buffer_;
  StringBuffer max_buffer_;
  StringBuffer prev_page_min_buffer_;
  StringBuffer prev_page_max_buffer_;
};

/// Class that handles the decoding of Parquet stats (min/max/null_count) for a given
/// column chunk.
class ColumnStatsReader {
public:
  /// Enum to select whether to read minimum or maximum statistics. Values do not
  /// correspond to fields in parquet::Statistics, but instead select between retrieving
  /// the minimum or maximum value.
  enum class StatsField { MIN, MAX };

  ColumnStatsReader(const parquet::ColumnChunk& col_chunk,
      const ColumnType& col_type, const parquet::ColumnOrder* col_order,
      const parquet::SchemaElement& element)
  : col_chunk_(col_chunk),
    col_type_(col_type),
    col_order_(col_order),
    element_(element) {}

  /// Sets extra information that is only needed for decoding TIMESTAMP stats.
  void SetTimestampDecoder(ParquetTimestampDecoder timestamp_decoder) {
    timestamp_decoder_ = timestamp_decoder;
  }

  /// Decodes the parquet::Statistics from 'col_chunk_' and writes the value selected by
  /// 'stats_field' into the buffer pointed to by 'slot', based on 'col_type_'. Returns
  /// true if reading statistics for columns of type 'col_type_' is supported and decoding
  /// was successful, false otherwise.
  bool ReadFromThrift(StatsField stats_field, void* slot) const;

  /// Call the above ReadFromThrift() for both the MIN and MAX stats. Return true if
  /// both stats are read successfully, false otherwise.
  bool ReadMinMaxFromThrift(void* min_slot, void* max_slot) const;

  /// Read plain encoded value from a string 'encoded_value' into 'slot'.
  /// Set paired_stats_value as nullptr if there is no corresponding paired stats,
  /// or paired stats value is not set.
  bool ReadFromString(StatsField stats_field, const std::string& encoded_value,
      const std::string* paired_stats_value,
      void* slot) const;

  /// Batch read encoded values from a range ['start_idx', 'end_idx'] in
  /// vector<string> 'encoded_value' into 'slots'.
  ///
  /// Return 'true' when all values in the range are decoded successfully, 'false'
  /// otherwise.
  bool ReadFromStringsBatch(StatsField stats_field,
      const vector<std::string>& encoded_values, int64_t start_index,
      int64_t end_idx, int fixed_len_size, void* slots) const;

  /// Batch decode routine that reads from a vector of std::string 'source' and decodes
  /// each value in the vector into a value of Impala type 'InternalType'.
  ///
  /// Return the number of decoded values from 'source' or -1 if there was an error
  /// decoding (e.g. invalid data) or invalid specification of the range.
  ///
  /// This routine breaks 'source' into batches of 8 elements and unrolls each batch.
  ///
  /// For each string value, this routine calls
  /// ColumnStats<InternalType>::DecodePlainValue() which in turn calls
  /// ParquetPlainEncoder::DecodeByParquetType() that switches on Parquet type.
  template <typename InternalType>
  int64_t DecodeBatchOneBoundsCheck(const vector<std::string>& source, int64_t start_idx,
      int64_t end_idx, int fixed_len_size, InternalType* v,
      parquet::Type::type PARQUET_TYPE) const;

  /// A fast track version of the above batch decode routine DecodeBatchOneBoundsCheck().
  ///
  /// For each string value, this routine calls ParquetPlainEncoder::DecodeNoBoundsCheck()
  /// that simply memcpy out the value. This version is used when the Impala internal type
  /// and the Parquet type are identical.
  template <typename InternalType>
  int64_t DecodeBatchOneBoundsCheckFastTrack(const vector<std::string>& source,
      int64_t start_idx, int64_t end_idx, int fixed_len_size, InternalType* v) const;

  // Gets the null_count statistics from the column chunk's metadata and returns
  // it via an output parameter.
  // Returns true if the null_count stats were read successfully, false otherwise.
  bool ReadNullCountStat(int64_t* null_count) const;

  /// Gets the null_count and num_values statistics from the column chunk's metadata
  /// and decide whether all values are nulls.
  /// Returns true if the stats were read successfully and 'all_nulls' is set to
  /// null_count being equal to num_values. Return false if stats can not be read. In
  /// the latter case, 'all_nulls' is not altered.
  bool AllNulls(bool* all_nulls) const;

  /// Returns the required stats field for the given function. 'fn_name' can be 'le',
  /// 'lt', 'ge', and 'gt' (i.e. binary operators <=, <, >=, >). If we want to check that
  /// whether a column contains a value less than a constant, we need the minimum value of
  /// the column to answer that question. And, to answer the opposite question we need the
  /// maximum value. The required stats field (min/max) will be stored in 'stats_field'.
  /// The function returns true on success, false otherwise.
  static bool GetRequiredStatsField(const std::string& fn_name, StatsField* stats_field);

private:
  /// Returns true if we support reading statistics stored in the fields 'min_value' and
  /// 'max_value' in parquet::Statistics for the type 'col_type_' and the column order
  /// 'col_order_'. Otherwise, returns false. If 'col_order_' is nullptr, only primitive
  /// numeric types are supported.
  bool CanUseStats() const;

  /// Returns true if we consider statistics stored in the deprecated fields 'min' and
  /// 'max' in parquet::Statistics to be correct for the type 'col_type_' and the column
  /// order 'col_order_'. Otherwise, returns false.
  bool CanUseDeprecatedStats() const;

  /// Decodes decimal value into slot. Does conversion if needed.
  template <typename DecimalType>
  bool DecodeDecimal(const std::string& stat_value, DecimalType* slot) const;

  /// Decodes 'stat_value' and does INT64->TimestampValue and timezone conversions if
  /// necessary. Returns true if the decoding and conversions were successful.
  bool DecodeTimestamp(const std::string& stat_value,
      ColumnStatsReader::StatsField stats_field,
      TimestampValue* slot) const;

  const parquet::ColumnChunk& col_chunk_;
  const ColumnType& col_type_;
  const parquet::ColumnOrder* col_order_;
  const parquet::SchemaElement& element_;
  ParquetTimestampDecoder timestamp_decoder_;
};
} // end ns impala
#endif
