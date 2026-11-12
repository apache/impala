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

#include <limits>
#include <string>
#include <vector>

#include "exec/parquet/parquet-column-stats.inline.h"
#include "gen-cpp/parquet_types.h"
#include "gutil/strings/substitute.h"
#include "runtime/decimal-value.inline.h"
#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"
#include "runtime/types.h"
#include "testutil/gtest-util.h"

#include "common/names.h"

namespace impala {

namespace {

/// Returns the plain encoding of 'v', as it is stored in parquet::Statistics and in
/// parquet::ColumnIndex.
template <typename T>
string EncodeStat(T v) {
  return string(reinterpret_cast<const char*>(&v), sizeof(v));
}

template <typename T>
vector<string> EncodeStats(const vector<T>& values) {
  vector<string> result;
  result.reserve(values.size());
  for (T v : values) result.push_back(EncodeStat(v));
  return result;
}

/// Returns the decimals with the unscaled values 'unscaled' * 'multiplier'.
template <typename DecimalType, typename T>
vector<DecimalType> ToDecimals(const vector<T>& unscaled, int64_t multiplier = 1) {
  using StorageType = typename DecimalType::StorageType;
  vector<DecimalType> result;
  result.reserve(unscaled.size());
  for (T v : unscaled) {
    result.push_back(DecimalType(static_cast<StorageType>(v) * multiplier));
  }
  return result;
}

parquet::SchemaElement PlainElement(parquet::Type::type type) {
  parquet::SchemaElement element;
  element.__set_name("col");
  element.__set_type(type);
  return element;
}

/// Returns a DECIMAL schema element with both the converted type and the logical type
/// set.
parquet::SchemaElement DecimalElement(parquet::Type::type type, int precision,
    int scale) {
  parquet::SchemaElement element = PlainElement(type);
  element.__set_converted_type(parquet::ConvertedType::DECIMAL);
  element.__set_precision(precision);
  element.__set_scale(scale);
  parquet::DecimalType decimal_type;
  decimal_type.__set_precision(precision);
  decimal_type.__set_scale(scale);
  parquet::LogicalType logical_type;
  logical_type.__set_DECIMAL(decimal_type);
  element.__set_logicalType(logical_type);
  return element;
}

/// Returns a signed INTEGER schema element, like the ones Impala writes for INT and
/// BIGINT columns.
parquet::SchemaElement IntElement(parquet::Type::type type) {
  DCHECK(type == parquet::Type::INT32 || type == parquet::Type::INT64);
  parquet::SchemaElement element = PlainElement(type);
  parquet::IntType int_type;
  int_type.__set_bitWidth(type == parquet::Type::INT32 ? 32 : 64);
  int_type.__set_isSigned(true);
  parquet::LogicalType logical_type;
  logical_type.__set_INTEGER(int_type);
  element.__set_logicalType(logical_type);
  return element;
}

/// Reads the plain-encoded statistics 'encoded' (in ascending order) of a Parquet column
/// described by 'element' into slots of type 'col_type' the two ways the scanner reads
/// them: as row group statistics and as page index values. Checks that the slot values
/// are 'expected'.
template <typename SlotType>
void TestReadStats(const parquet::SchemaElement& element, const ColumnType& col_type,
    const vector<string>& encoded, const vector<SlotType>& expected) {
  SCOPED_TRACE(Substitute("Parquet type: $0, slot type: $1",
      to_string(element.type), col_type.DebugString()));
  ASSERT_EQ(expected.size(), encoded.size());
  ASSERT_FALSE(encoded.empty());

  parquet::ColumnOrder col_order;
  col_order.__set_TYPE_ORDER(parquet::TypeDefinedOrder());
  parquet::Statistics stats;
  stats.__set_min_value(encoded.front());
  stats.__set_max_value(encoded.back());
  parquet::ColumnMetaData meta_data;
  meta_data.__set_type(element.type);
  meta_data.__set_statistics(stats);
  parquet::ColumnChunk col_chunk;
  col_chunk.__set_meta_data(meta_data);
  ColumnStatsReader reader(col_chunk, col_type, &col_order, element);

  SlotType min_value;
  SlotType max_value;
  ASSERT_TRUE(reader.ReadMinMaxFromThrift(&min_value, &max_value));
  EXPECT_EQ(expected.front(), min_value);
  EXPECT_EQ(expected.back(), max_value);

  for (int i = 0; i < encoded.size(); ++i) {
    SlotType value;
    ASSERT_TRUE(reader.ReadFromString(ColumnStatsReader::StatsField::MIN, encoded[i],
        nullptr, &value));
    EXPECT_EQ(expected[i], value) << "index " << i;
  }
}

} // anonymous namespace

/// IMPALA-15443: DECIMAL values stored as INT32, read into the same and into wider slots,
/// e.g. after ALTER TABLE CHANGE COLUMN widened the precision (IMPALA-7087).
TEST(ParquetColumnStatsReaderTest, DecimalStoredAsInt32) {
  const vector<int32_t> values = {-999999999, -123456, -500, -1, 0, 1, 400, 65536,
      7777777, 999999999};
  const vector<string> encoded = EncodeStats(values);
  const parquet::SchemaElement element = DecimalElement(parquet::Type::INT32, 9, 2);
  TestReadStats(element, ColumnType::CreateDecimalType(9, 2), encoded,
      ToDecimals<Decimal4Value>(values));
  TestReadStats(element, ColumnType::CreateDecimalType(18, 2), encoded,
      ToDecimals<Decimal8Value>(values));
  TestReadStats(element, ColumnType::CreateDecimalType(20, 2), encoded,
      ToDecimals<Decimal16Value>(values));
  TestReadStats(element, ColumnType::CreateDecimalType(12, 4), encoded,
      ToDecimals<Decimal8Value>(values, 100));
  TestReadStats(element, ColumnType::CreateDecimalType(25, 5), encoded,
      ToDecimals<Decimal16Value>(values, 1000));
}

/// IMPALA-15443: DECIMAL values stored as INT64, read into the same, wider and narrower
/// slots.
TEST(ParquetColumnStatsReaderTest, DecimalStoredAsInt64) {
  const vector<int64_t> values = {-9999999999, -4294967296, -500, -1, 0, 1, 400,
      4294967296, 9999999999};
  const vector<string> encoded = EncodeStats(values);
  const parquet::SchemaElement element = DecimalElement(parquet::Type::INT64, 10, 2);
  TestReadStats(element, ColumnType::CreateDecimalType(10, 2), encoded,
      ToDecimals<Decimal8Value>(values));
  TestReadStats(element, ColumnType::CreateDecimalType(20, 2), encoded,
      ToDecimals<Decimal16Value>(values));
  TestReadStats(element, ColumnType::CreateDecimalType(22, 4), encoded,
      ToDecimals<Decimal16Value>(values, 100));

  // Parquet allows INT64 for any precision up to 18. A precision of at most 9 is read
  // into a 4-byte slot.
  const vector<int64_t> small_values = {-99999, -4096, -500, -1, 0, 1, 500, 4096,
      99999};
  TestReadStats(DecimalElement(parquet::Type::INT64, 5, 2),
      ColumnType::CreateDecimalType(5, 2), EncodeStats(small_values),
      ToDecimals<Decimal4Value>(small_values));
}

/// IMPALA-15443: INT32 and INT64 columns read as DECIMAL (IMPALA-13625).
TEST(ParquetColumnStatsReaderTest, IntegerReadAsDecimal) {
  const vector<int32_t> int_values = {std::numeric_limits<int32_t>::min(), -65536, -5,
      -1, 0, 1, 4, 65536, std::numeric_limits<int32_t>::max()};
  const vector<string> int_encoded = EncodeStats(int_values);
  const parquet::SchemaElement int_element = IntElement(parquet::Type::INT32);
  TestReadStats(int_element, ColumnType::CreateDecimalType(10, 0), int_encoded,
      ToDecimals<Decimal8Value>(int_values));
  TestReadStats(int_element, ColumnType::CreateDecimalType(20, 4), int_encoded,
      ToDecimals<Decimal16Value>(int_values, 10000));

  const vector<int64_t> bigint_values = {std::numeric_limits<int64_t>::min(),
      -4294967296, -5, -1, 0, 1, 4, 4294967296, std::numeric_limits<int64_t>::max()};
  TestReadStats(IntElement(parquet::Type::INT64), ColumnType::CreateDecimalType(19, 0),
      EncodeStats(bigint_values), ToDecimals<Decimal16Value>(bigint_values));
}

/// IMPALA-15443: INT32 values read as BIGINT or DOUBLE and FLOAT values read as DOUBLE
/// (IMPALA-6373), e.g. after an ALTER TABLE CHANGE COLUMN or an Iceberg type promotion.
TEST(ParquetColumnStatsReaderTest, WidenedIntAndFloat) {
  const vector<int32_t> int_values = {std::numeric_limits<int32_t>::min(), -65536, -5,
      -1, 0, 1, 4, 65536, std::numeric_limits<int32_t>::max()};
  const vector<string> int_encoded = EncodeStats(int_values);
  const parquet::SchemaElement int_element = PlainElement(parquet::Type::INT32);
  TestReadStats(int_element, ColumnType(TYPE_BIGINT), int_encoded,
      vector<int64_t>(int_values.begin(), int_values.end()));
  TestReadStats(int_element, ColumnType(TYPE_DOUBLE), int_encoded,
      vector<double>(int_values.begin(), int_values.end()));

  const vector<float> float_values = {-3.0e38f, -5.5f, -1.0f, -0.25f, 0.0f, 0.25f, 1.5f,
      1024.125f, 3.0e38f};
  const vector<double> float_values_as_double(float_values.begin(), float_values.end());
  TestReadStats(PlainElement(parquet::Type::FLOAT), ColumnType(TYPE_DOUBLE),
      EncodeStats(float_values), float_values_as_double);

  // The types that need no conversion.
  TestReadStats(int_element, ColumnType(TYPE_INT), int_encoded, int_values);
  const vector<int64_t> bigint_values = {std::numeric_limits<int64_t>::min(),
      -4294967296, -5, -1, 0, 1, 4, 4294967296, std::numeric_limits<int64_t>::max()};
  TestReadStats(PlainElement(parquet::Type::INT64), ColumnType(TYPE_BIGINT),
      EncodeStats(bigint_values), bigint_values);
  TestReadStats(PlainElement(parquet::Type::FLOAT), ColumnType(TYPE_FLOAT),
      EncodeStats(float_values), float_values);
  const vector<double> double_values = {-1.0e300, -5.5, -1.0, -0.25, 0.0, 0.25, 1.5,
      1024.125, 1.0e300};
  TestReadStats(PlainElement(parquet::Type::DOUBLE), ColumnType(TYPE_DOUBLE),
      EncodeStats(double_values), double_values);
}

/// Decodes 'binary', which is the plain encoding or the Iceberg single-value
/// serialization of a value of type T. They are the same for the tested types.
template <typename T>
T DecodeValue(const string& binary) {
  T value{};
  EXPECT_EQ(sizeof(T), binary.size());
  if (binary.size() == sizeof(T)) memcpy(&value, binary.data(), sizeof(T));
  return value;
}

/// Returns the Iceberg stats of 'values', updating page stats with each value and merging
/// them into the row group stats after every 'page_size' values, like the Parquet writer.
/// Also returns the Parquet statistics in 'parquet_stats' if it is not null, and checks
/// that they have the same NaN count and the same bounds, apart from the sign of zeros.
template <typename T>
IcebergColumnStats ComputeIcebergStats(const vector<T>& values, int page_size,
    parquet::Statistics* parquet_stats = nullptr) {
  MemTracker tracker;
  MemPool pool(&tracker);
  ColumnStats<T> page_stats(&pool, sizeof(T));
  ColumnStats<T> row_group_stats(&pool, sizeof(T));
  for (size_t i = 0; i < values.size(); ++i) {
    page_stats.Update(values[i]);
    if ((i + 1) % page_size == 0 || i + 1 == values.size()) {
      row_group_stats.Merge(page_stats);
      page_stats.Reset();
    }
  }
  IcebergColumnStats result;
  row_group_stats.GetIcebergStats(0, values.size(), &result);
  parquet::Statistics local_parquet_stats;
  if (parquet_stats == nullptr) parquet_stats = &local_parquet_stats;
  *parquet_stats = parquet::Statistics();
  row_group_stats.EncodeToThrift(parquet_stats);
  EXPECT_EQ(result.has_min_max_values, parquet_stats->__isset.min_value);
  EXPECT_EQ(result.has_min_max_values, parquet_stats->__isset.max_value);
  if (result.has_min_max_values) {
    EXPECT_EQ(
        DecodeValue<T>(result.min_binary), DecodeValue<T>(parquet_stats->min_value));
    EXPECT_EQ(
        DecodeValue<T>(result.max_binary), DecodeValue<T>(parquet_stats->max_value));
  }
  EXPECT_EQ(std::is_floating_point<T>::value, parquet_stats->__isset.nan_count);
  if (parquet_stats->__isset.nan_count) {
    EXPECT_EQ(result.nan_count, parquet_stats->nan_count);
  }
  pool.FreeAll();
  return result;
}

/// Checks that 'min_binary' and 'max_binary' are 'lower' and 'upper', including the sign
/// of zeros.
template <typename T>
void ExpectBounds(const string& min_binary, const string& max_binary, T lower, T upper) {
  T actual_lower = DecodeValue<T>(min_binary);
  T actual_upper = DecodeValue<T>(max_binary);
  EXPECT_EQ(lower, actual_lower);
  EXPECT_EQ(std::signbit(lower), std::signbit(actual_lower));
  EXPECT_EQ(upper, actual_upper);
  EXPECT_EQ(std::signbit(upper), std::signbit(actual_upper));
}

/// Checks the Iceberg bounds in 'stats'.
template <typename T>
void ExpectBounds(const IcebergColumnStats& stats, T lower, T upper) {
  ASSERT_TRUE(stats.has_min_max_values);
  ExpectBounds<T>(stats.min_binary, stats.max_binary, lower, upper);
}

/// Checks the min and max values of the Parquet statistics 'stats'.
template <typename T>
void ExpectBounds(const parquet::Statistics& stats, T lower, T upper) {
  ASSERT_TRUE(stats.__isset.min_value);
  ASSERT_TRUE(stats.__isset.max_value);
  ExpectBounds<T>(stats.min_value, stats.max_value, lower, upper);
}

/// IMPALA-15794: Iceberg requires NaN counts, no NaNs in the bounds, and bounds that
/// order -0.0 before 0.0. Parquet statistics also have NaN counts and no NaN bounds, and
/// they write any zero minimum as -0.0 and any zero maximum as +0.0.
template <typename T>
void TestIcebergStatsOfFloatingPoint() {
  const T nan = std::numeric_limits<T>::quiet_NaN();
  for (int page_size : {1, 2, 100}) {
    SCOPED_TRACE(Substitute("page_size: $0", page_size));
    parquet::Statistics parquet_stats;
    IcebergColumnStats stats = ComputeIcebergStats<T>({1, nan}, page_size);
    EXPECT_EQ(1, stats.nan_count);
    ExpectBounds<T>(stats, 1, 1);

    stats = ComputeIcebergStats<T>({nan, 5, nan, -2, nan}, page_size);
    EXPECT_EQ(3, stats.nan_count);
    ExpectBounds<T>(stats, -2, 5);

    stats = ComputeIcebergStats<T>({-3, 0, -0.0}, page_size, &parquet_stats);
    EXPECT_EQ(0, stats.nan_count);
    ExpectBounds<T>(stats, -3, 0);
    ExpectBounds<T>(parquet_stats, -3, 0);

    stats = ComputeIcebergStats<T>({3, -0.0, 0}, page_size, &parquet_stats);
    ExpectBounds<T>(stats, -0.0, 3);
    ExpectBounds<T>(parquet_stats, -0.0, 3);

    stats = ComputeIcebergStats<T>({0, -0.0}, page_size, &parquet_stats);
    ExpectBounds<T>(stats, -0.0, 0);
    ExpectBounds<T>(parquet_stats, -0.0, 0);

    stats = ComputeIcebergStats<T>({-0.0, 0}, page_size, &parquet_stats);
    ExpectBounds<T>(stats, -0.0, 0);
    ExpectBounds<T>(parquet_stats, -0.0, 0);

    // The Iceberg bounds keep the sign of zeros, the Parquet statistics don't.
    stats = ComputeIcebergStats<T>({-1, -0.0}, page_size, &parquet_stats);
    ExpectBounds<T>(stats, -1, -0.0);
    ExpectBounds<T>(parquet_stats, -1, 0);

    stats = ComputeIcebergStats<T>({2, 0}, page_size, &parquet_stats);
    ExpectBounds<T>(stats, 0, 2);
    ExpectBounds<T>(parquet_stats, -0.0, 2);

    stats = ComputeIcebergStats<T>({-0.0, -0.0}, page_size, &parquet_stats);
    ExpectBounds<T>(stats, -0.0, -0.0);
    ExpectBounds<T>(parquet_stats, -0.0, 0);

    stats = ComputeIcebergStats<T>({0, 0}, page_size, &parquet_stats);
    ExpectBounds<T>(stats, 0, 0);
    ExpectBounds<T>(parquet_stats, -0.0, 0);

    // No bounds if all values are NaN, neither in Iceberg nor in Parquet.
    stats = ComputeIcebergStats<T>({nan, nan}, page_size);
    EXPECT_EQ(2, stats.nan_count);
    EXPECT_FALSE(stats.has_min_max_values);
  }
}

TEST(ParquetColumnStatsTest, IcebergStatsOfFloatingPoint) {
  TestIcebergStatsOfFloatingPoint<float>();
  TestIcebergStatsOfFloatingPoint<double>();
}

/// NaNs are only counted for floating point types.
TEST(ParquetColumnStatsTest, IcebergStatsOfNonFloatingPoint) {
  IcebergColumnStats stats = ComputeIcebergStats<int32_t>({3, -1}, 1);
  EXPECT_EQ(-1, stats.nan_count);
  ExpectBounds<int32_t>(stats, -1, 3);
}

} // namespace impala
