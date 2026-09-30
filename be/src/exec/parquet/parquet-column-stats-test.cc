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

} // namespace impala
