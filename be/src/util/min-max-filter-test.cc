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

#include "testutil/gtest-util.h"
#include "util/min-max-filter.h"

#include "runtime/decimal-value.h"
#include "runtime/decimal-value.inline.h"
#include "runtime/string-value.inline.h"
#include "runtime/test-env.h"
#include "service/fe-support.h"
#include "util/test-info.h"

DECLARE_bool(enable_webserver);

using namespace impala;

// Tests that a BoolMinMaxFilter returns the expected min/max after having values
// inserted into it, and that MinMaxFilter::Or works for bools.
TEST(MinMaxFilterTest, TestBoolMinMaxFilter) {
  MemTracker mem_tracker;
  ObjectPool obj_pool;

  MinMaxFilter* filter = MinMaxFilter::Create(
      ColumnType(PrimitiveType::TYPE_BOOLEAN), &obj_pool, &mem_tracker);
  EXPECT_TRUE(filter->AlwaysFalse());
  bool b1 = true;
  filter->Insert(&b1);
  EXPECT_EQ(*reinterpret_cast<bool*>(filter->GetMin()), b1);
  EXPECT_EQ(*reinterpret_cast<bool*>(filter->GetMax()), b1);
  EXPECT_FALSE(filter->AlwaysFalse());

  bool b2 = false;
  filter->Insert(&b2);
  EXPECT_EQ(*reinterpret_cast<bool*>(filter->GetMin()), b2);
  EXPECT_EQ(*reinterpret_cast<bool*>(filter->GetMax()), b1);

  // Check the behavior of Or.
  TMinMaxFilter tFilter1;
  tFilter1.min.__set_bool_val(false);
  tFilter1.max.__set_bool_val(true);
  TMinMaxFilter tFilter2;
  tFilter2.min.__set_bool_val(false);
  tFilter2.max.__set_bool_val(false);
  MinMaxFilter::Or(tFilter1, &tFilter2, ColumnType(PrimitiveType::TYPE_BOOLEAN));
  EXPECT_FALSE(tFilter2.min.bool_val);
  EXPECT_TRUE(tFilter2.max.bool_val);

  filter->Close();
}

void CheckIntVals(MinMaxFilter* filter, int32_t min, int32_t max) {
  EXPECT_EQ(*reinterpret_cast<int32_t*>(filter->GetMin()), min);
  EXPECT_EQ(*reinterpret_cast<int32_t*>(filter->GetMax()), max);
  EXPECT_FALSE(filter->AlwaysFalse());
  EXPECT_FALSE(filter->AlwaysTrue());
}

// Tests that a IntMinMaxFilter returns the expected min/max after having values
// inserted into it, and that MinMaxFilter::Or works for ints.
// This also provides coverage for the other numeric MinMaxFilter types as they're
// generated with maxcros and the logic is identical.
TEST(MinMaxFilterTest, TestNumericMinMaxFilter) {
  MemTracker mem_tracker;
  ObjectPool obj_pool;

  ColumnType int_type(PrimitiveType::TYPE_INT);
  MinMaxFilter* int_filter = MinMaxFilter::Create(int_type, &obj_pool, &mem_tracker);

  // Test the behavior of an empty filter.
  EXPECT_TRUE(int_filter->AlwaysFalse());
  EXPECT_FALSE(int_filter->AlwaysTrue());
  TMinMaxFilter tFilter;
  int_filter->ToThrift(&tFilter);
  EXPECT_TRUE(tFilter.always_false);
  EXPECT_FALSE(tFilter.always_true);
  EXPECT_FALSE(tFilter.min.__isset.int_val);
  EXPECT_FALSE(tFilter.max.__isset.int_val);
  MinMaxFilter* empty_filter =
      MinMaxFilter::Create(tFilter, int_type, &obj_pool, &mem_tracker);
  EXPECT_TRUE(empty_filter->AlwaysFalse());
  EXPECT_FALSE(empty_filter->AlwaysTrue());

  // Now insert some stuff.
  int32_t i1 = 10;
  int_filter->Insert(&i1);
  CheckIntVals(int_filter, i1, i1);
  int32_t i2 = 15;
  int_filter->Insert(&i2);
  CheckIntVals(int_filter, i1, i2);
  int32_t i3 = 12;
  int_filter->Insert(&i3);
  CheckIntVals(int_filter, i1, i2);
  int32_t i4 = 8;
  int_filter->Insert(&i4);
  CheckIntVals(int_filter, i4, i2);

  int_filter->ToThrift(&tFilter);
  EXPECT_FALSE(tFilter.always_false);
  EXPECT_FALSE(tFilter.always_true);
  EXPECT_EQ(tFilter.min.int_val, i4);
  EXPECT_EQ(tFilter.max.int_val, i2);
  MinMaxFilter* int_filter2 =
      MinMaxFilter::Create(tFilter, int_type, &obj_pool, &mem_tracker);
  CheckIntVals(int_filter2, i4, i2);

  // Check the behavior of Or.
  TMinMaxFilter tFilter1;
  tFilter1.min.__set_int_val(4);
  tFilter1.max.__set_int_val(8);
  TMinMaxFilter tFilter2;
  tFilter2.min.__set_int_val(2);
  tFilter2.max.__set_int_val(7);
  MinMaxFilter::Or(tFilter1, &tFilter2, int_type);
  EXPECT_EQ(tFilter2.min.int_val, 2);
  EXPECT_EQ(tFilter2.max.int_val, 8);

  int_filter->Close();
  empty_filter->Close();
  int_filter2->Close();
}

void CheckStringVals(MinMaxFilter* filter, const string& min, const string& max) {
  StringValue actual_min = *reinterpret_cast<StringValue*>(filter->GetMin());
  StringValue actual_max = *reinterpret_cast<StringValue*>(filter->GetMax());
  StringValue expected_min(min);
  StringValue expected_max(max);
  EXPECT_EQ(actual_min, expected_min);
  EXPECT_EQ(actual_max, expected_max);
  EXPECT_FALSE(filter->AlwaysTrue());
  EXPECT_FALSE(filter->AlwaysFalse());
}

// Tests that a StringMinMaxFilter returns the expected min/max after having values
// inserted into it, and that MinMaxFilter::Or works for strings.
// Also tests truncation behavior when inserted strings are larger than MAX_BOUND_LENTH
// and that the filter is disabled if there's not enough mem to store the min/max.
TEST(MinMaxFilterTest, TestStringMinMaxFilter) {
  ObjectPool obj_pool;
  MemTracker mem_tracker;

  ColumnType string_type(PrimitiveType::TYPE_STRING);
  MinMaxFilter* filter = MinMaxFilter::Create(string_type, &obj_pool, &mem_tracker);

  // Test the behavior of an empty filter.
  EXPECT_TRUE(filter->AlwaysFalse());
  EXPECT_FALSE(filter->AlwaysTrue());
  filter->MaterializeValues();
  EXPECT_TRUE(filter->AlwaysFalse());
  EXPECT_FALSE(filter->AlwaysTrue());
  TMinMaxFilter tFilter;
  filter->ToThrift(&tFilter);
  EXPECT_TRUE(tFilter.always_false);
  EXPECT_FALSE(tFilter.always_true);

  MinMaxFilter* empty_filter =
      MinMaxFilter::Create(tFilter, string_type, &obj_pool, &mem_tracker);
  EXPECT_TRUE(empty_filter->AlwaysFalse());
  EXPECT_FALSE(empty_filter->AlwaysTrue());

  // Now insert some stuff.
  string c = "c";
  StringValue cVal(c);
  filter->Insert(&cVal);
  filter->MaterializeValues();
  CheckStringVals(filter, c, c);

  string d = "d";
  StringValue dVal(d);
  filter->Insert(&dVal);
  filter->MaterializeValues();
  CheckStringVals(filter, c, d);

  string cc = "cc";
  StringValue ccVal(cc);
  filter->Insert(&ccVal);
  filter->MaterializeValues();
  CheckStringVals(filter, c, d);

  filter->ToThrift(&tFilter);
  EXPECT_FALSE(tFilter.always_false);
  EXPECT_FALSE(tFilter.always_true);
  EXPECT_EQ(tFilter.min.string_val, c);
  EXPECT_EQ(tFilter.max.string_val, d);

  // Test that strings longer than 1024 are truncated.
  string b1030(1030, 'b');
  StringValue b1030Val(b1030);
  filter->Insert(&b1030Val);
  filter->MaterializeValues();
  string b1024(1024, 'b');
  CheckStringVals(filter, b1024, d);

  string e1030(1030, 'e');
  StringValue e1030Val(e1030);
  filter->Insert(&e1030Val);
  filter->MaterializeValues();
  string e1024(1024, 'e');
  // For max, after truncating the final char is increased by one.
  e1024[1023] = 'f';
  CheckStringVals(filter, b1024, e1024);

  string trailMaxChar(1030, 'f');
  int trailIndex = 1020;
  for (int i = trailIndex; i < 1030; ++i) trailMaxChar[i] = -1;
  StringValue trailMaxCharVal(trailMaxChar);
  filter->Insert(&trailMaxCharVal);
  filter->MaterializeValues();
  // Check that when adding one for max, if the final char is the max char it overflows
  // and carries.
  string truncTrailMaxChar(1024, 'f');
  truncTrailMaxChar[trailIndex - 1] = 'g';
  for (int i = trailIndex; i < 1024; ++i) truncTrailMaxChar[i] = 0;
  CheckStringVals(filter, b1024, truncTrailMaxChar);

  filter->ToThrift(&tFilter);
  EXPECT_FALSE(tFilter.always_false);
  EXPECT_FALSE(tFilter.always_true);
  EXPECT_EQ(tFilter.min.string_val, b1024);
  EXPECT_EQ(tFilter.max.string_val, truncTrailMaxChar);

  MinMaxFilter* filter2 =
      MinMaxFilter::Create(tFilter, string_type, &obj_pool, &mem_tracker);
  CheckStringVals(filter2, b1024, truncTrailMaxChar);

  // Check that if the entire string is the max char and therefore after truncating for
  // max we can't add one, the filter is disabled.
  string allMaxChar(1030, -1);
  StringValue allMaxCharVal(allMaxChar);
  filter->Insert(&allMaxCharVal);
  filter->MaterializeValues();
  EXPECT_TRUE(filter->AlwaysTrue());

  // We should still be able to insert into a disabled filter.
  filter->Insert(&cVal);
  EXPECT_TRUE(filter->AlwaysTrue());

  filter->ToThrift(&tFilter);
  EXPECT_FALSE(tFilter.always_false);
  EXPECT_TRUE(tFilter.always_true);

  MinMaxFilter* always_true_filter =
      MinMaxFilter::Create(tFilter, string_type, &obj_pool, &mem_tracker);
  EXPECT_FALSE(always_true_filter->AlwaysFalse());
  EXPECT_TRUE(always_true_filter->AlwaysTrue());

  // Check that a filter that hits the mem limit is disabled.
  MemTracker limit_mem_tracker(1);
  // We do not want to start the webserver.
  FLAGS_enable_webserver = false;
  std::unique_ptr<TestEnv> env;
  env.reset(new TestEnv());
  ASSERT_OK(env->Init());

  MinMaxFilter* limit_filter =
      MinMaxFilter::Create(string_type, &obj_pool, &limit_mem_tracker);
  EXPECT_FALSE(limit_filter->AlwaysTrue());
  limit_filter->Insert(&cVal);
  limit_filter->MaterializeValues();
  EXPECT_TRUE(limit_filter->AlwaysTrue());
  limit_filter->Insert(&dVal);
  limit_filter->MaterializeValues();
  EXPECT_TRUE(limit_filter->AlwaysTrue());

  limit_filter->ToThrift(&tFilter);
  EXPECT_FALSE(tFilter.always_false);
  EXPECT_TRUE(tFilter.always_true);

  // Check the behavior of Or.
  TMinMaxFilter tFilter1;
  tFilter1.min.__set_string_val("a");
  tFilter1.max.__set_string_val("d");
  TMinMaxFilter tFilter2;
  tFilter2.min.__set_string_val("b");
  tFilter2.max.__set_string_val("e");
  MinMaxFilter::Or(tFilter1, &tFilter2, string_type);
  EXPECT_EQ(tFilter2.min.string_val, "a");
  EXPECT_EQ(tFilter2.max.string_val, "e");

  filter->Close();
  empty_filter->Close();
  filter2->Close();
  limit_filter->Close();
  always_true_filter->Close();
}

void CheckTimestampVals(
    MinMaxFilter* filter, const TimestampValue& min, const TimestampValue& max) {
  EXPECT_EQ(*reinterpret_cast<TimestampValue*>(filter->GetMin()), min);
  EXPECT_EQ(*reinterpret_cast<TimestampValue*>(filter->GetMax()), max);
  EXPECT_FALSE(filter->AlwaysFalse());
  EXPECT_FALSE(filter->AlwaysTrue());
}

// Tests that a TimestampMinMaxFilter returns the expected min/max after having values
// inserted into it, and that MinMaxFilter::Or works for timestamps.
TEST(MinMaxFilterTest, TestTimestampMinMaxFilter) {
  ObjectPool obj_pool;
  MemTracker mem_tracker;
  ColumnType timestamp_type(PrimitiveType::TYPE_TIMESTAMP);
  MinMaxFilter* filter = MinMaxFilter::Create(timestamp_type, &obj_pool, &mem_tracker);

  // Test the behavior of an empty filter.
  EXPECT_TRUE(filter->AlwaysFalse());
  EXPECT_FALSE(filter->AlwaysTrue());
  TMinMaxFilter tFilter;
  filter->ToThrift(&tFilter);
  EXPECT_TRUE(tFilter.always_false);
  EXPECT_FALSE(tFilter.always_true);
  EXPECT_FALSE(tFilter.min.__isset.timestamp_val);
  EXPECT_FALSE(tFilter.max.__isset.timestamp_val);
  MinMaxFilter* empty_filter =
      MinMaxFilter::Create(tFilter, timestamp_type, &obj_pool, &mem_tracker);
  EXPECT_TRUE(empty_filter->AlwaysFalse());
  EXPECT_FALSE(empty_filter->AlwaysTrue());

  // Now insert some stuff.
  TimestampValue t1 = TimestampValue::Parse("2000-01-01 00:00:00");
  filter->Insert(&t1);
  CheckTimestampVals(filter, t1, t1);
  TimestampValue t2 = TimestampValue::Parse("1990-01-01 12:30:00");
  filter->Insert(&t2);
  CheckTimestampVals(filter, t2, t1);
  TimestampValue t3 = TimestampValue::Parse("2001-04-30 05:00:00");
  filter->Insert(&t3);
  CheckTimestampVals(filter, t2, t3);
  TimestampValue t4 = TimestampValue::Parse("2001-04-30 01:00:00");
  filter->Insert(&t4);
  CheckTimestampVals(filter, t2, t3);

  filter->ToThrift(&tFilter);
  EXPECT_FALSE(tFilter.always_false);
  EXPECT_FALSE(tFilter.always_true);
  EXPECT_EQ(TimestampValue::FromTColumnValue(tFilter.min), t2);
  EXPECT_EQ(TimestampValue::FromTColumnValue(tFilter.max), t3);
  MinMaxFilter* filter2 =
      MinMaxFilter::Create(tFilter, timestamp_type, &obj_pool, &mem_tracker);
  CheckTimestampVals(filter2, t2, t3);

  // Check the behavior of Or.
  TMinMaxFilter tFilter1;
  t2.ToTColumnValue(&tFilter1.min);
  t4.ToTColumnValue(&tFilter1.max);
  TMinMaxFilter tFilter2;
  t1.ToTColumnValue(&tFilter2.min);
  t3.ToTColumnValue(&tFilter2.max);
  MinMaxFilter::Or(tFilter1, &tFilter2, timestamp_type);
  EXPECT_EQ(TimestampValue::FromTColumnValue(tFilter2.min), t2);
  EXPECT_EQ(TimestampValue::FromTColumnValue(tFilter2.max), t3);

  filter->Close();
  empty_filter->Close();
  filter2->Close();
}

#define DECIMAL_CHECK(SIZE)                                                     \
  do {                                                                          \
    EXPECT_EQ(*reinterpret_cast<Decimal##SIZE##Value*>(filter->GetMin()), min); \
    EXPECT_EQ(*reinterpret_cast<Decimal##SIZE##Value*>(filter->GetMax()), max); \
    EXPECT_FALSE(filter->AlwaysFalse());                                        \
    EXPECT_FALSE(filter->AlwaysTrue());                                         \
  } while (false)

void CheckDecimalVals(
    MinMaxFilter* filter, const Decimal4Value& min, const Decimal4Value& max) {
  DECIMAL_CHECK(4);
}

void CheckDecimalVals(
    MinMaxFilter* filter, const Decimal8Value& min, const Decimal8Value& max) {
  DECIMAL_CHECK(8);
}

void CheckDecimalVals(
    MinMaxFilter* filter, const Decimal16Value& min, const Decimal16Value& max) {
  DECIMAL_CHECK(16);
}

void CheckDecimalEmptyFilter(MinMaxFilter* filter, const ColumnType& column_type,
    TMinMaxFilter* tFilter, ObjectPool* obj_pool, MemTracker* mem_tracker) {
  EXPECT_TRUE(filter->AlwaysFalse());
  EXPECT_FALSE(filter->AlwaysTrue());
  filter->ToThrift(tFilter);
  EXPECT_TRUE(tFilter->always_false);
  EXPECT_FALSE(tFilter->always_true);
  EXPECT_FALSE(tFilter->min.__isset.decimal_val);
  EXPECT_FALSE(tFilter->max.__isset.decimal_val);
  MinMaxFilter* empty_filter =
      MinMaxFilter::Create(*tFilter, column_type, obj_pool, mem_tracker);
  EXPECT_TRUE(empty_filter->AlwaysFalse());
  EXPECT_FALSE(empty_filter->AlwaysTrue());
  empty_filter->Close();
}

// values are such that VALUE3 < VALUE1 < VALUE2
// The insert order is VALUE1, VALUE2, VALUE3
// 1. After VALUE1 insert: both min and max are VALUE1
// 2. After VALUE2 insert: min=VALUE1; max=VALUE2
// 3. After VALUE3 insert: min=VALUE3; max=VALUE2
#define DECIMAL_INSERT_AND_CHECK(SIZE, PRECISION, SCALE, VALUE1, VALUE2, VALUE3)     \
  do {                                                                               \
    d1##SIZE =                                                                       \
        Decimal##SIZE##Value::FromDouble(PRECISION, SCALE, VALUE1, true, &overflow); \
    filter##SIZE->Insert(&d1##SIZE);                                                 \
    CheckDecimalVals(filter##SIZE, d1##SIZE, d1##SIZE);                              \
    d2##SIZE =                                                                       \
        Decimal##SIZE##Value::FromDouble(PRECISION, SCALE, VALUE2, true, &overflow); \
    filter##SIZE->Insert(&d2##SIZE);                                                 \
    CheckDecimalVals(filter##SIZE, d1##SIZE, d2##SIZE);                              \
    d3##SIZE =                                                                       \
        Decimal##SIZE##Value::FromDouble(PRECISION, SCALE, VALUE3, true, &overflow); \
    filter##SIZE->Insert(&d3##SIZE);                                                 \
    CheckDecimalVals(filter##SIZE, d3##SIZE, d2##SIZE);                              \
  } while (false)

#define DECIMAL_CHECK_THRIFT(SIZE)                                                  \
  do {                                                                              \
    filter##SIZE->ToThrift(&tFilter##SIZE);                                         \
    EXPECT_FALSE(tFilter##SIZE.always_false);                                       \
    EXPECT_FALSE(tFilter##SIZE.always_true);                                        \
    EXPECT_EQ(Decimal##SIZE##Value::FromTColumnValue(tFilter##SIZE.min), d3##SIZE); \
    EXPECT_EQ(Decimal##SIZE##Value::FromTColumnValue(tFilter##SIZE.max), d2##SIZE); \
    MinMaxFilter* filter##SIZE##2 = MinMaxFilter::Create(                           \
        tFilter##SIZE, decimal##SIZE##_type, &obj_pool, &mem_tracker);              \
    CheckDecimalVals(filter##SIZE##2, d3##SIZE, d2##SIZE);                          \
    filter##SIZE##2->Close();                                                       \
  } while (false)

#define DECIMAL_CHECK_OR(SIZE)                                                       \
  do {                                                                               \
    TMinMaxFilter tFilter1##SIZE;                                                    \
    d3##SIZE.ToTColumnValue(&tFilter1##SIZE.min);                                    \
    d2##SIZE.ToTColumnValue(&tFilter1##SIZE.max);                                    \
    TMinMaxFilter tFilter2##SIZE;                                                    \
    d1##SIZE.ToTColumnValue(&tFilter2##SIZE.min);                                    \
    d1##SIZE.ToTColumnValue(&tFilter2##SIZE.max);                                    \
    MinMaxFilter::Or(tFilter1##SIZE, &tFilter2##SIZE, decimal##SIZE##_type);         \
    EXPECT_EQ(Decimal##SIZE##Value::FromTColumnValue(tFilter2##SIZE.min), d3##SIZE); \
    EXPECT_EQ(Decimal##SIZE##Value::FromTColumnValue(tFilter2##SIZE.max), d2##SIZE); \
  } while (false)

// Tests that a DecimalMinMaxFilter returns the expected min/max after having values
// inserted into it, and that MinMaxFilter::Or works for decimal values.
TEST(MinMaxFilterTest, TestDecimalMinMaxFilter) {
  ObjectPool obj_pool;
  MemTracker mem_tracker;
  bool overflow = false;

  // Create types
  ColumnType decimal4_type = ColumnType::CreateDecimalType(9, 5);
  ColumnType decimal8_type = ColumnType::CreateDecimalType(18, 9);
  ColumnType decimal16_type = ColumnType::CreateDecimalType(38, 19);

  // Decimal Values
  Decimal4Value d14, d24, d34;
  Decimal8Value d18, d28, d38;
  Decimal16Value d116, d216, d316;

  // Create filters
  MinMaxFilter* filter4 = MinMaxFilter::Create(decimal4_type, &obj_pool, &mem_tracker);
  MinMaxFilter* filter8 = MinMaxFilter::Create(decimal8_type, &obj_pool, &mem_tracker);
  MinMaxFilter* filter16 = MinMaxFilter::Create(decimal16_type, &obj_pool, &mem_tracker);

  // Create thrift minmax filters
  TMinMaxFilter tFilter4, tFilter8, tFilter16;

  // Test the behavior of an empty filter.
  CheckDecimalEmptyFilter(filter4, decimal4_type, &tFilter4, &obj_pool, &mem_tracker);
  CheckDecimalEmptyFilter(filter8, decimal8_type, &tFilter8, &obj_pool, &mem_tracker);
  CheckDecimalEmptyFilter(filter16, decimal16_type, &tFilter16, &obj_pool, &mem_tracker);

  // Insert and check
  DECIMAL_INSERT_AND_CHECK(4, 9, 5, 2345.67891, 3456.78912, 1234.56789);
  DECIMAL_INSERT_AND_CHECK(
      8, 18, 9, 234567891.234567891, 345678912.345678912, 123456789.123456789);
  DECIMAL_INSERT_AND_CHECK(16, 38, 19, 2345678912345678912.2345678912345678912,
      3456789123456789123.3456789123456789123, 1234567891234567891.1234567891234567891);

  // Thrift check
  DECIMAL_CHECK_THRIFT(4);
  DECIMAL_CHECK_THRIFT(8);
  DECIMAL_CHECK_THRIFT(16);

  // Check the behavior of Or.
  DECIMAL_CHECK_OR(4);
  DECIMAL_CHECK_OR(8);
  DECIMAL_CHECK_OR(16);

  // Close all filters
  filter4->Close();
  filter8->Close();
  filter16->Close();
}
