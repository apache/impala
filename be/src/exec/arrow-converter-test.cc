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

#include <functional>
#include <limits>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <boost/scoped_ptr.hpp>

#include "exec/arrow-converter.h"
#include "gutil/strings/substitute.h"
#include "runtime/date-value.h"
#include "runtime/decimal-value.inline.h"
#include "runtime/descriptors.h"
#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"
#include "runtime/multi-precision.h"
#include "runtime/smallable-string.h"
#include "runtime/string-value.h"
#include "runtime/test-env.h"
#include "runtime/timestamp-value.h"
#include "runtime/tuple.h"
#include "testutil/desc-tbl-builder.h"
#include "testutil/gtest-util.h"

#include "common/names.h"

using strings::Substitute;

#define EXPECT_ARROW_OK(expr)                                    \
  do {                                                           \
    arrow::Status _arrow_status = (expr);                        \
    EXPECT_TRUE(_arrow_status.ok()) << _arrow_status.ToString(); \
  } while (false)

namespace impala {

namespace {

constexpr int NUM_ROWS = 1024;
// In arrays with nulls, every NULL_PERIOD'th row is null.
constexpr int NULL_PERIOD = 7;
constexpr uint32_t PRIME = 479001599;
constexpr int64_t NANOS_PER_SECOND = 1000000000L;

// Valid DATE range: 0001-01-01 and 9999-12-31 in days since epoch.
constexpr int32_t MIN_DATE_DAYS = -719162;
constexpr int32_t MAX_DATE_DAYS = 2932896;
// Valid TIMESTAMP range: 1400-01-01 00:00:00 and 9999-12-31 23:59:59 in seconds since
// epoch.
constexpr int64_t MIN_TIMESTAMP_SECONDS = -17987443200L;
constexpr int64_t MAX_TIMESTAMP_SECONDS = 253402300799L;

// The value generators below return boundary values for the first few rows, and
// varied values derived from 'row_idx' otherwise. Row 0 is null in arrays with nulls.

bool GenBool(int row_idx) {
  return ((row_idx * PRIME) >> 7) & 1;
}

template <typename T>
T GenInt(int row_idx) {
  switch (row_idx) {
    case 1: return std::numeric_limits<T>::min();
    case 2: return std::numeric_limits<T>::max();
    case 3: return 0;
    default: return static_cast<T>(row_idx * PRIME);
  }
}

template <typename T>
T GenFloat(int row_idx) {
  switch (row_idx) {
    case 1: return std::numeric_limits<T>::lowest();
    case 2: return std::numeric_limits<T>::max();
    case 3: return std::numeric_limits<T>::infinity();
    case 4: return -std::numeric_limits<T>::infinity();
    case 5: return std::numeric_limits<T>::denorm_min();
    default: return static_cast<T>(row_idx - NUM_ROWS / 2) * static_cast<T>(1.37);
  }
}

int32_t GenDateDays(int row_idx) {
  switch (row_idx) {
    case 1: return MIN_DATE_DAYS;
    case 2: return MAX_DATE_DAYS;
    default: {
      uint32_t num_days = MAX_DATE_DAYS - MIN_DATE_DAYS + 1;
      return MIN_DATE_DAYS + static_cast<int32_t>((row_idx * PRIME) % num_days);
    }
  }
}

struct TimestampParts {
  int64_t seconds;
  int64_t nanos;
};

// Nanosecond timestamps only cover years 1677-2262, so the boundary values of the
// valid range are only generated if 'include_boundaries' is true.
TimestampParts GenTimestamp(int row_idx, bool include_boundaries) {
  if (include_boundaries && row_idx == 1) return {MIN_TIMESTAMP_SECONDS, 0};
  if (include_boundaries && row_idx == 2) {
    return {MAX_TIMESTAMP_SECONDS, NANOS_PER_SECOND - 1};
  }
  // Spans about +-256 years around the epoch.
  return {(row_idx - NUM_ROWS / 2) * (182L * 24 * 3600 + 12345),
      static_cast<int64_t>((row_idx * PRIME) % NANOS_PER_SECOND)};
}

// Builds the expected TimestampValue without Impala's unix time conversion functions.
TimestampValue ExpectedTimestamp(int64_t seconds, int64_t nanos) {
  int64_t days = seconds / (24 * 3600);
  int64_t seconds_of_day = seconds % (24 * 3600);
  if (seconds_of_day < 0) {
    seconds_of_day += 24 * 3600;
    --days;
  }
  return TimestampValue(
      boost::gregorian::date(1970, 1, 1) + boost::gregorian::date_duration(days),
      boost::posix_time::seconds(seconds_of_day) + boost::posix_time::nanoseconds(nanos));
}

// Unscaled decimal value with at most 'precision' digits.
int128_t GenDecimal(int row_idx, int precision) {
  int128_t limit = 1;
  for (int i = 0; i < precision; ++i) limit *= 10;
  int128_t max_value = limit - 1;
  switch (row_idx) {
    case 1: return max_value;
    case 2: return -max_value;
    case 3: return 0;
    case 4: return -1;
    case 5: return 128;
    case 6: return -129;
    default: {
      int128_t value = static_cast<int128_t>(row_idx * PRIME) * PRIME * PRIME % limit;
      return row_idx % 2 == 0 ? value : -value;
    }
  }
}

// Big-endian two's complement encoding of 'value' in 'len' bytes. If 'len' is 0, the
// shortest encoding is used, like Java's BigInteger.toByteArray().
std::string EncodeDecimal(int128_t value, int len) {
  constexpr int NUM_BYTES = sizeof(int128_t);
  uint8_t bytes[NUM_BYTES];
  for (int i = 0; i < NUM_BYTES; ++i) {
    bytes[NUM_BYTES - 1 - i] = static_cast<uint8_t>(value >> (8 * i));
  }
  int start = NUM_BYTES - len;
  if (len == 0) {
    start = 0;
    while (start < NUM_BYTES - 1
        && ((bytes[start] == 0x00 && (bytes[start + 1] & 0x80) == 0)
            || (bytes[start] == 0xff && (bytes[start + 1] & 0x80) != 0))) {
      ++start;
    }
  }
  return std::string(reinterpret_cast<const char*>(bytes + start), NUM_BYTES - start);
}

// Covers empty strings and both sides of the small string limit. Binary strings also
// contain zero and non-ASCII bytes.
std::string GenString(int row_idx, bool binary) {
  int len;
  switch (row_idx) {
    case 1: len = 0; break;
    case 2: len = SmallableString::SMALL_LIMIT; break;
    case 3: len = SmallableString::SMALL_LIMIT + 1; break;
    default: len = (row_idx * PRIME) % 40;
  }
  std::string result(len, ' ');
  for (int i = 0; i < len; ++i) {
    result[i] = binary ? static_cast<char>(row_idx * 31 + i)
                       : static_cast<char>('a' + (row_idx + i) % 26);
  }
  return result;
}

// An arrow array, the Impala type it is converted to and a function that checks the
// converted value of a non-null row.
struct ColumnCase {
  std::string name;
  ColumnType type;
  std::shared_ptr<arrow::Array> array;
  std::function<void(int row_idx, const void* value)> check_value;
  bool nullable = true;
};

template <typename BuilderT, typename AppendFn>
std::shared_ptr<arrow::Array> BuildArray(
    BuilderT* builder, bool with_nulls, const AppendFn& append) {
  for (int row_idx = 0; row_idx < NUM_ROWS; ++row_idx) {
    if (with_nulls && row_idx % NULL_PERIOD == 0) {
      EXPECT_ARROW_OK(builder->AppendNull());
    } else {
      EXPECT_ARROW_OK(append(row_idx));
    }
  }
  std::shared_ptr<arrow::Array> array;
  EXPECT_ARROW_OK(builder->Finish(&array));
  return array;
}

ColumnCase MakeBoolCase(bool with_nulls) {
  arrow::BooleanBuilder builder;
  ColumnCase c{"boolean", ColumnType(TYPE_BOOLEAN),
      BuildArray(&builder, with_nulls, [&](int i) { return builder.Append(GenBool(i)); }),
      nullptr};
  // Arrow stores booleans as bits, Impala as bytes that must be exactly 0 or 1.
  c.check_value = [](int i, const void* value) {
    EXPECT_EQ(GenBool(i) ? 1 : 0, *static_cast<const uint8_t*>(value));
  };
  return c;
}

template <typename T, typename BuilderT>
ColumnCase MakeNumericCase(
    const std::string& name, PrimitiveType type, T (*gen)(int), bool with_nulls) {
  BuilderT builder;
  ColumnCase c{name, ColumnType(type),
      BuildArray(&builder, with_nulls, [&](int i) { return builder.Append(gen(i)); }),
      nullptr};
  c.check_value = [gen](int i, const void* value) {
    EXPECT_EQ(gen(i), *static_cast<const T*>(value));
  };
  return c;
}

ColumnCase MakeDateCase(bool with_nulls) {
  arrow::Date32Builder builder;
  ColumnCase c{"date", ColumnType(TYPE_DATE),
      BuildArray(&builder, with_nulls,
          [&](int i) { return builder.Append(GenDateDays(i)); }),
      nullptr};
  c.check_value = [](int i, const void* value) {
    const DateValue* date = static_cast<const DateValue*>(value);
    EXPECT_TRUE(date->IsValid());
    EXPECT_EQ(GenDateDays(i), date->Value());
  };
  return c;
}

ColumnCase MakeTimestampCase(
    arrow::TimeUnit::type unit, const std::string& timezone, bool with_nulls) {
  int64_t ticks_per_second;
  switch (unit) {
    case arrow::TimeUnit::SECOND: ticks_per_second = 1; break;
    case arrow::TimeUnit::MILLI: ticks_per_second = 1000; break;
    case arrow::TimeUnit::MICRO: ticks_per_second = 1000000; break;
    default: ticks_per_second = NANOS_PER_SECOND;
  }
  int64_t nanos_per_tick = NANOS_PER_SECOND / ticks_per_second;
  bool include_boundaries = unit != arrow::TimeUnit::NANO;

  std::shared_ptr<arrow::DataType> arrow_type = arrow::timestamp(unit, timezone);
  arrow::TimestampBuilder builder(arrow_type, arrow::default_memory_pool());
  ColumnCase c{arrow_type->ToString(), ColumnType(TYPE_TIMESTAMP),
      BuildArray(&builder, with_nulls,
          [&](int i) {
            TimestampParts ts = GenTimestamp(i, include_boundaries);
            return builder.Append(
                ts.seconds * ticks_per_second + ts.nanos / nanos_per_tick);
          }),
      nullptr};
  // The test RuntimeState uses UTC as local time zone, so arrays with a time zone are
  // expected to convert to the same values.
  c.check_value = [=](int i, const void* value) {
    TimestampParts ts = GenTimestamp(i, include_boundaries);
    TimestampValue expected =
        ExpectedTimestamp(ts.seconds, ts.nanos / nanos_per_tick * nanos_per_tick);
    EXPECT_EQ(expected, *static_cast<const TimestampValue*>(value));
  };
  return c;
}

// Decimals are sent as big-endian binary values. Even rows use the shortest encoding,
// odd rows the full width of the slot.
template <typename DecimalT>
ColumnCase MakeDecimalCase(int precision, int scale, bool with_nulls) {
  constexpr int SLOT_SIZE = sizeof(typename DecimalT::StorageType);
  arrow::BinaryBuilder builder;
  ColumnCase c{Substitute("decimal($0,$1)", precision, scale),
      ColumnType::CreateDecimalType(precision, scale),
      BuildArray(&builder, with_nulls,
          [&](int i) {
            return builder.Append(
                EncodeDecimal(GenDecimal(i, precision), i % 2 == 0 ? 0 : SLOT_SIZE));
          }),
      nullptr};
  c.check_value = [=](int i, const void* value) {
    DecimalT expected(GenDecimal(i, precision));
    EXPECT_EQ(expected.ToString(precision, scale),
        static_cast<const DecimalT*>(value)->ToString(precision, scale));
  };
  return c;
}

template <typename BuilderT>
ColumnCase MakeStringCase(
    const std::string& name, const ColumnType& type, bool binary, bool with_nulls) {
  BuilderT builder;
  ColumnCase c{name, type,
      BuildArray(&builder, with_nulls,
          [&](int i) { return builder.Append(GenString(i, binary)); }),
      nullptr};
  std::shared_ptr<arrow::Buffer> arrow_data =
      static_cast<const arrow::BinaryArray*>(c.array.get())->value_data();
  c.check_value = [=](int i, const void* value) {
    const StringValue* string_value = static_cast<const StringValue*>(value);
    EXPECT_EQ(GenString(i, binary),
        std::string(string_value->Ptr(), string_value->Len()));
    // The arrow buffers are released after conversion, so data must be copied.
    const uint8_t* ptr = reinterpret_cast<const uint8_t*>(string_value->Ptr());
    EXPECT_FALSE(string_value->Len() > 0 && ptr >= arrow_data->data()
        && ptr < arrow_data->data() + arrow_data->size());
  };
  return c;
}

// The generated strings are both shorter and longer than 'len'.
ColumnCase MakeCharCase(int len, bool with_nulls) {
  arrow::StringBuilder builder;
  ColumnCase c{Substitute("char($0)", len), ColumnType::CreateCharType(len),
      BuildArray(&builder, with_nulls,
          [&](int i) { return builder.Append(GenString(i, false)); }),
      nullptr};
  c.check_value = [=](int i, const void* value) {
    std::string expected = GenString(i, false);
    expected.resize(len, ' ');
    EXPECT_EQ(expected, std::string(static_cast<const char*>(value), len));
  };
  return c;
}

std::vector<ColumnCase> MakeAllTypeCases(bool with_nulls) {
  return {
      MakeBoolCase(with_nulls),
      MakeNumericCase<int8_t, arrow::Int8Builder>(
          "tinyint", TYPE_TINYINT, GenInt<int8_t>, with_nulls),
      MakeNumericCase<int16_t, arrow::Int16Builder>(
          "smallint", TYPE_SMALLINT, GenInt<int16_t>, with_nulls),
      MakeNumericCase<int32_t, arrow::Int32Builder>(
          "int", TYPE_INT, GenInt<int32_t>, with_nulls),
      MakeNumericCase<int64_t, arrow::Int64Builder>(
          "bigint", TYPE_BIGINT, GenInt<int64_t>, with_nulls),
      MakeNumericCase<float, arrow::FloatBuilder>(
          "float", TYPE_FLOAT, GenFloat<float>, with_nulls),
      MakeNumericCase<double, arrow::DoubleBuilder>(
          "double", TYPE_DOUBLE, GenFloat<double>, with_nulls),
      MakeDateCase(with_nulls),
      MakeTimestampCase(arrow::TimeUnit::SECOND, "", with_nulls),
      MakeTimestampCase(arrow::TimeUnit::MILLI, "", with_nulls),
      MakeTimestampCase(arrow::TimeUnit::MICRO, "", with_nulls),
      MakeTimestampCase(arrow::TimeUnit::NANO, "", with_nulls),
      MakeTimestampCase(arrow::TimeUnit::MICRO, "UTC", with_nulls),
      MakeDecimalCase<Decimal4Value>(9, 2, with_nulls),
      MakeDecimalCase<Decimal8Value>(18, 4, with_nulls),
      MakeDecimalCase<Decimal16Value>(38, 10, with_nulls),
      MakeStringCase<arrow::StringBuilder>(
          "string", ColumnType(TYPE_STRING), false, with_nulls),
      MakeStringCase<arrow::StringBuilder>(
          "varchar(50)", ColumnType::CreateVarcharType(50), false, with_nulls),
      MakeStringCase<arrow::BinaryBuilder>(
          "binary", ColumnType::CreateBinaryType(), true, with_nulls),
      MakeCharCase(10, with_nulls),
  };
}

// Returns a case for rows [offset, offset + length) of 'c'.
ColumnCase SliceCase(const ColumnCase& c, int offset, int length) {
  ColumnCase slice = c;
  slice.array = c.array->Slice(offset, length);
  slice.check_value = [check_value = c.check_value, offset](int i, const void* value) {
    check_value(i + offset, value);
  };
  return slice;
}

std::shared_ptr<arrow::RecordBatch> MakeRecordBatch(
    const std::vector<std::shared_ptr<arrow::Array>>& arrays) {
  std::vector<std::shared_ptr<arrow::Field>> fields;
  fields.reserve(arrays.size());
  for (int i = 0; i < arrays.size(); ++i) {
    fields.push_back(arrow::field(Substitute("f$0", i), arrays[i]->type()));
  }
  return arrow::RecordBatch::Make(arrow::schema(fields), arrays[0]->length(), arrays);
}

} // namespace

class ArrowConverterTest : public testing::Test {
 protected:
  void SetUp() override {
    test_env_.reset(new TestEnv());
    ASSERT_OK(test_env_->Init());
    ASSERT_OK(test_env_->CreateQueryState(0, nullptr, &runtime_state_));
    mem_pool_.reset(new MemPool(&tracker_));
  }

  void TearDown() override {
    runtime_state_ = nullptr;
    test_env_.reset();
    pool_.Clear();
    mem_pool_->FreeAll();
  }

  TupleDescriptor* BuildTupleDesc(
      const vector<ColumnType>& slot_types, const vector<bool>& nullable = {}) {
    DescriptorTblBuilder tbl_builder(test_env_->exec_env()->frontend(), &pool_);
    TupleDescBuilder& tuple_builder = tbl_builder.DeclareTuple();
    for (int i = 0; i < slot_types.size(); ++i) {
      tuple_builder.AddSlot(slot_types[i], nullable.empty() || nullable[i]);
    }
    RowDescriptor* row_desc = pool_.Add(new RowDescriptor(*tbl_builder.Build(),
        vector<TTupleId>(1, 0), vector<bool>(1, false)));
    return row_desc->tuple_descriptors()[0];
  }

  // Builds a tuple descriptor with a slot for each of 'cases'.
  TupleDescriptor* BuildTupleDesc(const vector<ColumnCase>& cases) {
    vector<ColumnType> types;
    vector<bool> nullable;
    types.reserve(cases.size());
    nullable.reserve(cases.size());
    for (const ColumnCase& c : cases) {
      types.push_back(c.type);
      nullable.push_back(c.nullable);
    }
    return BuildTupleDesc(types, nullable);
  }

  // Converts rows [start_row_idx, end_row_idx) of a record batch with a column for each
  // of 'cases' with a new converter, and checks every slot.
  void ConvertAndCheckTuples(
      const vector<ColumnCase>& cases, int start_row_idx, int end_row_idx) {
    TupleDescriptor* tuple_desc = BuildTupleDesc(cases);
    ArrowToTupleConverter converter(tuple_desc);
    ASSERT_OK(converter.Init(runtime_state_));
    ConvertAndCheckTuples(&converter, tuple_desc, cases, start_row_idx, end_row_idx);
  }

  // Same as above, but with 'converter' of 'tuple_desc'.
  void ConvertAndCheckTuples(ArrowToTupleConverter* converter,
      const TupleDescriptor* tuple_desc, const vector<ColumnCase>& cases,
      int start_row_idx, int end_row_idx) {
    vector<std::shared_ptr<arrow::Array>> arrays;
    arrays.reserve(cases.size());
    for (const ColumnCase& c : cases) arrays.push_back(c.array);
    std::shared_ptr<arrow::RecordBatch> record_batch = MakeRecordBatch(arrays);

    int tuple_size = tuple_desc->byte_size();
    int num_rows = end_row_idx - start_row_idx;
    uint8_t* tuple_mem = mem_pool_->Allocate(num_rows * tuple_size);
    // The conversion must initialize the null indicators.
    memset(tuple_mem, 0xff, num_rows * tuple_size);
    ASSERT_OK(converter->Convert(
        *record_batch, start_row_idx, end_row_idx, tuple_mem, mem_pool_.get()));

    for (int col = 0; col < cases.size(); ++col) {
      SCOPED_TRACE(cases[col].name);
      const SlotDescriptor* slot_desc = tuple_desc->slots()[col];
      for (int row_idx = start_row_idx; row_idx < end_row_idx; ++row_idx) {
        SCOPED_TRACE(Substitute("row $0", row_idx));
        const Tuple* tuple = reinterpret_cast<const Tuple*>(
            tuple_mem + (row_idx - start_row_idx) * tuple_size);
        bool is_null = cases[col].array->IsNull(row_idx);
        EXPECT_EQ(is_null, tuple->IsNull(slot_desc->null_indicator_offset()));
        if (!is_null) {
          cases[col].check_value(row_idx, tuple->GetSlot(slot_desc->tuple_offset()));
        }
      }
    }
  }

  // Converts the array of 'c' into raw values, and checks every value.
  void ConvertAndCheckRawValues(const ColumnCase& c) {
    int num_rows = c.array->length();
    vector<void*> raw_values(num_rows);
    vector<uint8_t> value_buffer(num_rows * c.type.GetSlotSize());
    ASSERT_OK(ArrowConverter::ConvertArrowArrayToRawValues(c.array.get(),
        raw_values.data(), value_buffer.data(), c.type, mem_pool_.get(),
        runtime_state_));

    for (int row_idx = 0; row_idx < num_rows; ++row_idx) {
      SCOPED_TRACE(Substitute("row $0", row_idx));
      bool is_null = c.array->IsNull(row_idx);
      EXPECT_EQ(is_null, raw_values[row_idx] == nullptr);
      if (!is_null) c.check_value(row_idx, raw_values[row_idx]);
    }
  }

  // Converts 'array' into tuples with a single slot of 'type'. If not null,
  // 'slot_desc' and 'tuple_mem' are set to the slot and the converted tuples.
  Status ConvertToTuples(const std::shared_ptr<arrow::Array>& array,
      const ColumnType& type, bool nullable = true, RuntimeState* state = nullptr,
      const SlotDescriptor** slot_desc = nullptr, uint8_t** tuple_mem = nullptr) {
    TupleDescriptor* tuple_desc = BuildTupleDesc({type}, {nullable});
    std::shared_ptr<arrow::RecordBatch> record_batch = MakeRecordBatch({array});
    uint8_t* mem = mem_pool_->Allocate(array->length() * tuple_desc->byte_size());
    if (slot_desc != nullptr) *slot_desc = tuple_desc->slots()[0];
    if (tuple_mem != nullptr) *tuple_mem = mem;
    ArrowToTupleConverter converter(tuple_desc);
    RETURN_IF_ERROR(converter.Init(state != nullptr ? state : runtime_state_));
    return converter.Convert(*record_batch, mem, mem_pool_.get());
  }

  MemTracker tracker_;
  boost::scoped_ptr<TestEnv> test_env_;
  ObjectPool pool_;
  boost::scoped_ptr<MemPool> mem_pool_;
  RuntimeState* runtime_state_ = nullptr;
};

// Converts every supported type into tuples and raw values, with and without nulls.
// Arrays without nulls are also converted into non-nullable slots.
// Slices start at a non-byte-aligned offset to exercise arrow's bitmap offsets.
TEST_F(ArrowConverterTest, AllTypes) {
  constexpr int SLICE_OFFSET = 3;
  constexpr int SLICE_LENGTH = NUM_ROWS - 8;
  for (bool with_nulls : {true, false}) {
    SCOPED_TRACE(with_nulls ? "with nulls" : "without nulls");
    for (const ColumnCase& c : MakeAllTypeCases(with_nulls)) {
      SCOPED_TRACE(c.name);
      ConvertAndCheckTuples({c}, 0, NUM_ROWS);
      ConvertAndCheckRawValues(c);
      ColumnCase slice = SliceCase(c, SLICE_OFFSET, SLICE_LENGTH);
      ConvertAndCheckTuples({slice}, 0, SLICE_LENGTH);
      ConvertAndCheckRawValues(slice);
      if (!with_nulls) {
        ColumnCase non_nullable = c;
        non_nullable.nullable = false;
        ConvertAndCheckTuples({non_nullable}, 0, NUM_ROWS);
      }
    }
  }
}

TEST_F(ArrowConverterTest, NullsInNonNullableSlot) {
  for (const ColumnCase& c : MakeAllTypeCases(true)) {
    SCOPED_TRACE(c.name);
    Status status = ConvertToTuples(c.array, c.type, false);
    ASSERT_FALSE(status.ok());
    EXPECT_STR_CONTAINS(status.GetDetail(), "null values for a non-nullable slot");
  }
}

// Non-nullable test slots must have the same layout as in real queries: no null bits.
TEST_F(ArrowConverterTest, NonNullableSlotLayout) {
  TupleDescriptor* non_nullable = BuildTupleDesc(
      {ColumnType(TYPE_INT), ColumnType(TYPE_BIGINT)}, {false, false});
  EXPECT_EQ(0, non_nullable->num_null_bytes());
  EXPECT_EQ(12, non_nullable->byte_size());
  for (const SlotDescriptor* slot : non_nullable->slots()) {
    EXPECT_FALSE(slot->is_nullable());
  }

  vector<ColumnType> types(9, ColumnType(TYPE_INT));
  vector<bool> nullable(9, true);
  nullable[0] = false;
  TupleDescriptor* mixed = BuildTupleDesc(types, nullable);
  EXPECT_EQ(1, mixed->num_null_bytes());
  EXPECT_FALSE(mixed->slots()[0]->is_nullable());
  EXPECT_TRUE(mixed->slots()[1]->is_nullable());
}

// Converts all types into a single tuple, alternating columns with and without nulls,
// so null indicators of different slots share bytes.
TEST_F(ArrowConverterTest, AllTypesInOneTuple) {
  vector<ColumnCase> with_nulls = MakeAllTypeCases(true);
  vector<ColumnCase> without_nulls = MakeAllTypeCases(false);
  vector<ColumnCase> cases;
  cases.reserve(with_nulls.size());
  for (int i = 0; i < with_nulls.size(); ++i) {
    cases.push_back(i % 2 == 0 ? with_nulls[i] : without_nulls[i]);
  }
  ConvertAndCheckTuples(cases, 0, NUM_ROWS);
}

// Converting a range of rows writes the first row of the range to the beginning of the
// tuple memory.
TEST_F(ArrowConverterTest, RowRanges) {
  vector<ColumnCase> cases = MakeAllTypeCases(true);
  ConvertAndCheckTuples(cases, 100, 300);
  ConvertAndCheckTuples(cases, NUM_ROWS - 1, NUM_ROWS);
  ConvertAndCheckTuples(cases, 0, 0);
}

// One converter is reused for multiple record batches, like in a scan node.
TEST_F(ArrowConverterTest, ReuseConverter) {
  constexpr int SLICE_OFFSET = 3;
  constexpr int SLICE_LENGTH = NUM_ROWS - 8;
  vector<ColumnCase> with_nulls = MakeAllTypeCases(true);
  vector<ColumnCase> without_nulls = MakeAllTypeCases(false);
  vector<ColumnCase> slices;
  for (const ColumnCase& c : with_nulls) {
    slices.push_back(SliceCase(c, SLICE_OFFSET, SLICE_LENGTH));
  }

  TupleDescriptor* tuple_desc = BuildTupleDesc(with_nulls);
  ArrowToTupleConverter converter(tuple_desc);
  ASSERT_OK(converter.Init(runtime_state_));
  ConvertAndCheckTuples(&converter, tuple_desc, with_nulls, 0, NUM_ROWS);
  ConvertAndCheckTuples(&converter, tuple_desc, without_nulls, 100, 300);
  ConvertAndCheckTuples(&converter, tuple_desc, slices, 0, SLICE_LENGTH);
  ConvertAndCheckTuples(&converter, tuple_desc, with_nulls, NUM_ROWS - 1, NUM_ROWS);
}

// The time unit and time zone of timestamps are read from each record batch.
TEST_F(ArrowConverterTest, ReuseConverterWithDifferentTimestampTypes) {
  vector<ColumnCase> cases = {
      MakeTimestampCase(arrow::TimeUnit::MILLI, "", true),
      MakeTimestampCase(arrow::TimeUnit::NANO, "", true),
      MakeTimestampCase(arrow::TimeUnit::SECOND, "UTC", true),
      MakeTimestampCase(arrow::TimeUnit::MICRO, "", true),
  };
  TupleDescriptor* tuple_desc = BuildTupleDesc({cases[0]});
  ArrowToTupleConverter converter(tuple_desc);
  ASSERT_OK(converter.Init(runtime_state_));
  for (const ColumnCase& c : cases) {
    SCOPED_TRACE(c.name);
    ConvertAndCheckTuples(&converter, tuple_desc, {c}, 0, NUM_ROWS);
  }
}

// A failed conversion does not affect the conversion of the next record batch.
TEST_F(ArrowConverterTest, ReuseConverterAfterError) {
  vector<ColumnCase> cases = MakeAllTypeCases(true);
  ASSERT_EQ(TYPE_BOOLEAN, cases[0].type.type);
  TupleDescriptor* tuple_desc = BuildTupleDesc(cases);
  ArrowToTupleConverter converter(tuple_desc);
  ASSERT_OK(converter.Init(runtime_state_));

  // Only the first column matches, so the conversion fails at the second one.
  vector<std::shared_ptr<arrow::Array>> bool_arrays(cases.size(), cases[0].array);
  std::shared_ptr<arrow::RecordBatch> wrong_batch = MakeRecordBatch(bool_arrays);
  uint8_t* tuple_mem = mem_pool_->Allocate(NUM_ROWS * tuple_desc->byte_size());
  Status status = converter.Convert(*wrong_batch, tuple_mem, mem_pool_.get());
  ASSERT_FALSE(status.ok());
  EXPECT_STR_CONTAINS(status.GetDetail(), "Mismatching types");

  ConvertAndCheckTuples(&converter, tuple_desc, cases, 0, NUM_ROWS);
}

TEST_F(ArrowConverterTest, TypeMismatch) {
  arrow::BooleanBuilder bool_builder;
  std::shared_ptr<arrow::Array> bool_array = BuildArray(&bool_builder, true,
      [&](int i) { return bool_builder.Append(GenBool(i)); });
  arrow::Int8Builder int8_builder;
  std::shared_ptr<arrow::Array> int8_array = BuildArray(&int8_builder, true,
      [&](int i) { return int8_builder.Append(GenInt<int8_t>(i)); });

  for (const ColumnCase& c : MakeAllTypeCases(true)) {
    SCOPED_TRACE(c.name);
    const std::shared_ptr<arrow::Array>& wrong_array =
        c.type.type == TYPE_BOOLEAN ? int8_array : bool_array;
    Status status = ConvertToTuples(wrong_array, c.type);
    ASSERT_FALSE(status.ok());
    EXPECT_STR_CONTAINS(status.GetDetail(), "Mismatching types");
  }
}

TEST_F(ArrowConverterTest, ColumnCountMismatch) {
  vector<ColumnCase> cases = MakeAllTypeCases(true);
  std::shared_ptr<arrow::RecordBatch> record_batch =
      MakeRecordBatch({cases[0].array, cases[1].array});
  TupleDescriptor* tuple_desc = BuildTupleDesc({cases[0].type});
  uint8_t* tuple_mem = mem_pool_->Allocate(NUM_ROWS * tuple_desc->byte_size());
  ArrowToTupleConverter converter(tuple_desc);
  ASSERT_OK(converter.Init(runtime_state_));
  Status status = converter.Convert(*record_batch, tuple_mem, mem_pool_.get());
  ASSERT_FALSE(status.ok());
  EXPECT_STR_CONTAINS(status.GetDetail(), "has 2 columns, expected 1");
}

TEST_F(ArrowConverterTest, ComplexTypesNotSupported) {
  ColumnType array_type;
  array_type.type = TYPE_ARRAY;
  array_type.children.push_back(ColumnType(TYPE_INT));
  ColumnCase c = MakeAllTypeCases(true)[0];
  vector<void*> raw_values(NUM_ROWS);
  vector<uint8_t> value_buffer(NUM_ROWS * array_type.GetSlotSize());
  Status status = ArrowConverter::ConvertArrowArrayToRawValues(c.array.get(),
      raw_values.data(), value_buffer.data(), array_type, mem_pool_.get(),
      runtime_state_);
  ASSERT_FALSE(status.ok());
  EXPECT_STR_CONTAINS(status.GetDetail(), "not supported for complex types");
}

// Arrays whose rows 1 and 3 are valid in Arrow, but out of Impala's range.
struct InvalidValueCase {
  ColumnType type;
  std::shared_ptr<arrow::Array> array;
  std::string message;
};

std::vector<InvalidValueCase> MakeInvalidValueCases() {
  arrow::Date32Builder date_builder;
  for (int32_t days : {0, MIN_DATE_DAYS - 1, 0, MAX_DATE_DAYS + 1}) {
    EXPECT_ARROW_OK(date_builder.Append(days));
  }
  std::shared_ptr<arrow::Array> date_array;
  EXPECT_ARROW_OK(date_builder.Finish(&date_array));

  arrow::TimestampBuilder timestamp_builder(
      arrow::timestamp(arrow::TimeUnit::SECOND), arrow::default_memory_pool());
  for (int64_t seconds :
      {0L, MIN_TIMESTAMP_SECONDS - 1, 0L, MAX_TIMESTAMP_SECONDS + 1}) {
    EXPECT_ARROW_OK(timestamp_builder.Append(seconds));
  }
  std::shared_ptr<arrow::Array> timestamp_array;
  EXPECT_ARROW_OK(timestamp_builder.Finish(&timestamp_array));

  return {
      {ColumnType(TYPE_DATE), date_array, "out of range date"},
      {ColumnType(TYPE_TIMESTAMP), timestamp_array, "out of range timestamp"},
  };
}

// Like in the Parquet scanner, invalid values are converted to NULL with a warning.
TEST_F(ArrowConverterTest, InvalidValuesToNull) {
  for (const InvalidValueCase& c : MakeInvalidValueCases()) {
    SCOPED_TRACE(c.type.DebugString());
    const SlotDescriptor* slot_desc;
    uint8_t* tuple_mem;
    ASSERT_OK(ConvertToTuples(c.array, c.type, true, nullptr, &slot_desc, &tuple_mem));
    for (int row_idx = 0; row_idx < c.array->length(); ++row_idx) {
      const Tuple* tuple = reinterpret_cast<const Tuple*>(
          tuple_mem + row_idx * slot_desc->parent()->byte_size());
      EXPECT_EQ(row_idx % 2 == 1, tuple->IsNull(slot_desc->null_indicator_offset()))
          << "row " << row_idx;
    }

    vector<void*> raw_values(c.array->length());
    vector<uint8_t> value_buffer(c.array->length() * c.type.GetSlotSize());
    ASSERT_OK(ArrowConverter::ConvertArrowArrayToRawValues(c.array.get(),
        raw_values.data(), value_buffer.data(), c.type, mem_pool_.get(),
        runtime_state_));
    for (int row_idx = 0; row_idx < c.array->length(); ++row_idx) {
      EXPECT_EQ(row_idx % 2 == 1, raw_values[row_idx] == nullptr) << "row " << row_idx;
    }

    std::string error_log = runtime_state_->ErrorLog();
    EXPECT_STR_CONTAINS(error_log, Substitute("$0 in row 1.", c.message));
    EXPECT_STR_CONTAINS(error_log, Substitute("$0 in row 3.", c.message));
  }
}

TEST_F(ArrowConverterTest, InvalidValuesWithAbortOnError) {
  TQueryOptions query_options;
  query_options.__set_abort_on_error(true);
  RuntimeState* abort_on_error_state;
  ASSERT_OK(test_env_->CreateQueryState(1, &query_options, &abort_on_error_state));
  for (const InvalidValueCase& c : MakeInvalidValueCases()) {
    SCOPED_TRACE(c.type.DebugString());
    Status status = ConvertToTuples(c.array, c.type, true, abort_on_error_state);
    ASSERT_FALSE(status.ok());
    EXPECT_STR_CONTAINS(status.GetDetail(), Substitute("$0 in row 1.", c.message));
  }
}

// Unlike the Parquet scanner, which keeps invalid values in non-nullable slots, the
// conversion fails.
TEST_F(ArrowConverterTest, InvalidValuesInNonNullableSlot) {
  for (const InvalidValueCase& c : MakeInvalidValueCases()) {
    SCOPED_TRACE(c.type.DebugString());
    Status status = ConvertToTuples(c.array, c.type, false);
    ASSERT_FALSE(status.ok());
    EXPECT_STR_CONTAINS(status.GetDetail(), Substitute("$0 in row 1.", c.message));
    EXPECT_STR_CONTAINS(status.GetDetail(), "the slot is not nullable");
  }
}

TEST_F(ArrowConverterTest, InvalidDecimal) {
  // Empty, and longer than the 4 byte slot.
  for (const std::string& bytes : {std::string(), std::string(5, '\x01')}) {
    SCOPED_TRACE(bytes.size());
    arrow::BinaryBuilder builder;
    EXPECT_ARROW_OK(builder.Append(bytes));
    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(builder.Finish(&array).ok());
    Status status = ConvertToTuples(array, ColumnType::CreateDecimalType(9, 2));
    ASSERT_FALSE(status.ok());
    EXPECT_STR_CONTAINS(status.GetDetail(), "Invalid Decimal Format");
  }
}

TEST_F(ArrowConverterTest, StringMemLimitExceeded) {
  arrow::StringBuilder builder;
  for (int i = 0; i < 10; ++i) {
    EXPECT_ARROW_OK(builder.Append(std::string(100, 'x')));
  }
  std::shared_ptr<arrow::Array> array;
  ASSERT_TRUE(builder.Finish(&array).ok());

  MemTracker limited_tracker(100);
  MemPool limited_pool(&limited_tracker);
  ColumnType type(TYPE_STRING);
  vector<void*> raw_values(array->length());
  vector<uint8_t> value_buffer(array->length() * type.GetSlotSize());
  Status status = ArrowConverter::ConvertArrowArrayToRawValues(array.get(),
      raw_values.data(), value_buffer.data(), type, &limited_pool, runtime_state_);
  EXPECT_TRUE(status.IsMemLimitExceeded()) << status.GetDetail();
  limited_pool.FreeAll();
}

} // namespace impala
