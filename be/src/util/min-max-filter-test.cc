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

#include "gen-cpp/data_stream_service.pb.h"
#include "runtime/date-value.h"
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

  ColumnType bool_column_type(PrimitiveType::TYPE_BOOLEAN);
  MinMaxFilter* filter = MinMaxFilter::Create(bool_column_type, &obj_pool, &mem_tracker);
  EXPECT_TRUE(filter->AlwaysFalse());
  bool b1 = true;
  filter->Insert(&b1);
  EXPECT_EQ(*reinterpret_cast<const bool*>(filter->GetMin()), b1);
  EXPECT_EQ(*reinterpret_cast<const bool*>(filter->GetMax()), b1);
  EXPECT_FALSE(filter->AlwaysFalse());

  bool b2 = false;

  // The range is currently [true, true] in filter.
  // Check overlapping with [false, false], which should be false.
  EXPECT_EQ(filter->EvalOverlap(bool_column_type, &b2, &b2), false);
  // Check overlapping with [true, true], which should be true.
  EXPECT_EQ(filter->EvalOverlap(bool_column_type, &b1, &b1), true);
  // Check overlapping with [false, true], which should be true.
  EXPECT_EQ(filter->EvalOverlap(bool_column_type, &b2, &b1), true);

  // The range is currently [false, true] in filter.
  filter->Insert(&b2);
  EXPECT_EQ(*reinterpret_cast<const bool*>(filter->GetMin()), b2);
  EXPECT_EQ(*reinterpret_cast<const bool*>(filter->GetMax()), b1);

  // Check overlapping with [false, false], which should be true.
  EXPECT_EQ(filter->EvalOverlap(bool_column_type, &b2, &b2), true);
  // Check overlapping with [true, true], which should be true.
  EXPECT_EQ(filter->EvalOverlap(bool_column_type, &b1, &b1), true);
  // Check overlapping with [false, true], which should be true.
  EXPECT_EQ(filter->EvalOverlap(bool_column_type, &b2, &b1), true);

  // Check the behavior of Or.
  MinMaxFilterPB pFilter1;
  pFilter1.mutable_min()->set_bool_val(false);
  pFilter1.mutable_max()->set_bool_val(true);
  MinMaxFilterPB pFilter2;
  pFilter2.mutable_min()->set_bool_val(false);
  pFilter2.mutable_max()->set_bool_val(false);
  MinMaxFilter::Or(pFilter1, &pFilter2, ColumnType(PrimitiveType::TYPE_BOOLEAN));
  EXPECT_FALSE(pFilter2.min().bool_val());
  EXPECT_TRUE(pFilter2.max().bool_val());

  MinMaxFilter* f1 = MinMaxFilter::Create(
      pFilter1, ColumnType(PrimitiveType::TYPE_BOOLEAN), &obj_pool, &mem_tracker);
  MinMaxFilter* f2 = MinMaxFilter::Create(
      pFilter2, ColumnType(PrimitiveType::TYPE_BOOLEAN), &obj_pool, &mem_tracker);
  f1->Or(*f2);
  EXPECT_FALSE(*reinterpret_cast<const bool*>(f1->GetMin()));
  EXPECT_TRUE(*reinterpret_cast<const bool*>(f1->GetMax()));
  EXPECT_FALSE(f1->AlwaysTrue());
  EXPECT_FALSE(f1->AlwaysFalse());

  filter->Close();
  f1->Close();
  f2->Close();
}

void CheckIntVals(MinMaxFilter* filter, int32_t min, int32_t max) {
  EXPECT_EQ(*reinterpret_cast<const int32_t*>(filter->GetMin()), min);
  EXPECT_EQ(*reinterpret_cast<const int32_t*>(filter->GetMax()), max);
  EXPECT_FALSE(filter->AlwaysFalse());
  EXPECT_FALSE(filter->AlwaysTrue());

  // Check overlaps. The range is [min, max] in filter.
  ColumnType int_type(PrimitiveType::TYPE_INT);

  // Check overlapping with [min-10, min-1], which should be false.
  int min_minus_10 = min - 10;
  int min_minus_1 = min - 1;
  EXPECT_EQ(filter->EvalOverlap(int_type, &min_minus_10, &min_minus_1), false);

  // Check overlapping with [max+1, max+20], which should be false.
  int max_plus_1 = max + 1;
  int max_plus_20 = max + 20;
  EXPECT_EQ(filter->EvalOverlap(int_type, &max_plus_1, &max_plus_20), false);

  // Check overlapping with [min-1, min+(max-min)/2], which should be true.
  int middle = min + (max-min)/2;
  EXPECT_EQ(filter->EvalOverlap(int_type, &min_minus_1, &middle), true);
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
  MinMaxFilterPB pFilter;
  int_filter->ToProtobuf(&pFilter);
  EXPECT_TRUE(pFilter.always_false());
  EXPECT_FALSE(pFilter.always_true());
  EXPECT_FALSE(pFilter.min().has_int_val());
  EXPECT_FALSE(pFilter.max().has_int_val());
  MinMaxFilter* empty_filter =
      MinMaxFilter::Create(pFilter, int_type, &obj_pool, &mem_tracker);
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

  int_filter->ToProtobuf(&pFilter);
  EXPECT_FALSE(pFilter.always_false());
  EXPECT_FALSE(pFilter.always_true());
  EXPECT_EQ(pFilter.min().int_val(), i4);
  EXPECT_EQ(pFilter.max().int_val(), i2);
  MinMaxFilter* int_filter2 =
      MinMaxFilter::Create(pFilter, int_type, &obj_pool, &mem_tracker);
  CheckIntVals(int_filter2, i4, i2);

  // Check the behavior of Or.
  MinMaxFilterPB pFilter1;
  pFilter1.mutable_min()->set_int_val(4);
  pFilter1.mutable_max()->set_int_val(8);
  MinMaxFilterPB pFilter2;
  pFilter2.mutable_min()->set_int_val(2);
  pFilter2.mutable_max()->set_int_val(7);
  MinMaxFilter::Or(pFilter1, &pFilter2, int_type);
  EXPECT_EQ(pFilter2.min().int_val(), 2);
  EXPECT_EQ(pFilter2.max().int_val(), 8);

  MinMaxFilter* f1 = MinMaxFilter::Create(pFilter1, int_type, &obj_pool, &mem_tracker);
  MinMaxFilter* f2 = MinMaxFilter::Create(pFilter2, int_type, &obj_pool, &mem_tracker);
  f1->Or(*f2);
  EXPECT_EQ(2, *reinterpret_cast<const int32_t*>(f1->GetMin()));
  EXPECT_EQ(8, *reinterpret_cast<const int32_t*>(f1->GetMax()));
  EXPECT_FALSE(f1->AlwaysTrue());
  EXPECT_FALSE(f1->AlwaysFalse());

  int_filter->Close();
  empty_filter->Close();
  int_filter2->Close();
  f1->Close();
  f2->Close();
}

// Make a string that is compared less than 'str'
string make_less_than(const string& str) {
  if (str.length() == 1) {
    unsigned char c = str.c_str()[0];
    if (c == 0) {
      return string("");
    }
    c--;
    return string(1, c);
  }
  string result(str);
  result.pop_back();
  return result;
}

// Make a string that is compared greater than 'str'
string make_greater_than(const string& str) {
  string result(str);
  result.push_back('a');
  return result;
}

void CheckStringVals(MinMaxFilter* filter, const string& min, const string& max) {
  StringValue actual_min = *reinterpret_cast<const StringValue*>(filter->GetMin());
  StringValue actual_max = *reinterpret_cast<const StringValue*>(filter->GetMax());
  StringValue expected_min(min);
  StringValue expected_max(max);
  EXPECT_EQ(actual_min, expected_min);
  EXPECT_EQ(actual_max, expected_max);
  EXPECT_FALSE(filter->AlwaysTrue());
  EXPECT_FALSE(filter->AlwaysFalse());

  // Check overlaps. The range is [min, max] in filter.
  ColumnType string_type(PrimitiveType::TYPE_STRING);

  // Check overlapping with [less_than(min), less_than(min)], which should be false.
  string less_than_str = make_less_than(min);
  StringValue less_than(less_than_str);
  EXPECT_EQ(filter->EvalOverlap(string_type, &less_than, &less_than), false);

  // Check overlapping with [greater_than(max), greater_than(max)], which should be false.
  string greater_than_str = make_greater_than(max);
  StringValue greater_than(greater_than_str);
  EXPECT_EQ(filter->EvalOverlap(string_type, &greater_than, &greater_than), false);

  // Check overlapping with [less_than(min), max], which should be true.
  EXPECT_EQ(filter->EvalOverlap(string_type, &less_than, &actual_max), true);

  // Check overlapping with [min, greater_than(max)], which should be true.
  EXPECT_EQ(filter->EvalOverlap(string_type, &actual_min, &greater_than), true);

  // Test that uint64_t converted values are useful to compute overlap ratio.
  // Note that Strings sharing a common prefix of 8 bytes long can be converted to the
  // same value (e.g. Uint64("12345678") == Uint64("123456789")), the relationship of
  // "<=" is tested.
  uint64_t less_than_as_uint64 = less_than.ToUInt64();
  uint64_t actual_min_as_uint64 = actual_min.ToUInt64();
  uint64_t actual_max_as_uint64 = actual_max.ToUInt64();
  uint64_t greater_than_as_uint64 = greater_than.ToUInt64();
  EXPECT_EQ(less_than_as_uint64 <= actual_min_as_uint64, true);
  EXPECT_EQ(actual_min_as_uint64 <= actual_max_as_uint64, true);
  EXPECT_EQ(actual_max_as_uint64 <= greater_than_as_uint64, true);
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
  MinMaxFilterPB pFilter;
  filter->ToProtobuf(&pFilter);
  EXPECT_TRUE(pFilter.always_false());
  EXPECT_FALSE(pFilter.always_true());

  MinMaxFilter* empty_filter =
      MinMaxFilter::Create(pFilter, string_type, &obj_pool, &mem_tracker);
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

  filter->ToProtobuf(&pFilter);
  EXPECT_FALSE(pFilter.always_false());
  EXPECT_FALSE(pFilter.always_true());
  EXPECT_EQ(pFilter.min().string_val(), c);
  EXPECT_EQ(pFilter.max().string_val(), d);

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

  filter->ToProtobuf(&pFilter);
  EXPECT_FALSE(pFilter.always_false());
  EXPECT_FALSE(pFilter.always_true());
  EXPECT_EQ(pFilter.min().string_val(), b1024);
  EXPECT_EQ(pFilter.max().string_val(), truncTrailMaxChar);

  MinMaxFilter* filter2 =
      MinMaxFilter::Create(pFilter, string_type, &obj_pool, &mem_tracker);
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

  filter->ToProtobuf(&pFilter);
  EXPECT_FALSE(pFilter.always_false());
  EXPECT_TRUE(pFilter.always_true());

  MinMaxFilter* always_true_filter =
      MinMaxFilter::Create(pFilter, string_type, &obj_pool, &mem_tracker);
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

  limit_filter->ToProtobuf(&pFilter);
  EXPECT_FALSE(pFilter.always_false());
  EXPECT_TRUE(pFilter.always_true());

  // Check the behavior of Or.
  MinMaxFilterPB pFilter1;
  pFilter1.mutable_min()->set_string_val("a");
  pFilter1.mutable_max()->set_string_val("d");
  MinMaxFilterPB pFilter2;
  pFilter2.mutable_min()->set_string_val("b");
  pFilter2.mutable_max()->set_string_val("e");
  MinMaxFilter::Or(pFilter1, &pFilter2, string_type);
  EXPECT_EQ(pFilter2.min().string_val(), "a");
  EXPECT_EQ(pFilter2.max().string_val(), "e");

  MinMaxFilter* f1 = MinMaxFilter::Create(pFilter1, string_type, &obj_pool, &mem_tracker);
  MinMaxFilter* f2 = MinMaxFilter::Create(pFilter2, string_type, &obj_pool, &mem_tracker);
  f1->Or(*f2);
  EXPECT_EQ("a", reinterpret_cast<const StringValue*>(f1->GetMin())->DebugString());
  EXPECT_EQ("e", reinterpret_cast<const StringValue*>(f1->GetMax())->DebugString());
  EXPECT_FALSE(f1->AlwaysTrue());
  EXPECT_FALSE(f1->AlwaysFalse());
  // Make sure AlwaysFalse() is handled correctly.
  MinMaxFilter* always_false = MinMaxFilter::Create(string_type, &obj_pool, &mem_tracker);
  f1->Or(*always_false); // This is a no-op.
  EXPECT_EQ("a", reinterpret_cast<const StringValue*>(f1->GetMin())->DebugString());
  EXPECT_EQ("e", reinterpret_cast<const StringValue*>(f1->GetMax())->DebugString());
  EXPECT_FALSE(f1->AlwaysTrue());
  EXPECT_FALSE(f1->AlwaysFalse());
  always_false->Or(*f1); // Update the always false filter.
  EXPECT_EQ(
      "a", reinterpret_cast<const StringValue*>(always_false->GetMin())->DebugString());
  EXPECT_EQ(
      "e", reinterpret_cast<const StringValue*>(always_false->GetMax())->DebugString());
  EXPECT_FALSE(always_false->AlwaysTrue());
  EXPECT_FALSE(always_false->AlwaysFalse());

  // Make sure AlwaysTrue() is handled correctly.
  always_false = MinMaxFilter::Create(string_type, &obj_pool, &mem_tracker);
  // Merge always true into another filter.
  f1->Or(*always_true_filter);
  EXPECT_TRUE(f1->AlwaysTrue());
  EXPECT_FALSE(f1->AlwaysFalse());
  always_false->Or(*always_true_filter);
  EXPECT_TRUE(always_false->AlwaysTrue());
  EXPECT_FALSE(always_false->AlwaysFalse());
  always_true_filter->Or(*f2); // This is a no-op.
  EXPECT_TRUE(always_true_filter->AlwaysTrue());
  EXPECT_FALSE(always_true_filter->AlwaysFalse());

  filter->Close();
  empty_filter->Close();
  filter2->Close();
  limit_filter->Close();
  always_true_filter->Close();
  f1->Close();
  f2->Close();
  always_false->Close();
}

static TimestampValue ParseSimpleTimestamp(const char* s) {
  return TimestampValue::ParseSimpleDateFormat(s);
}

static DateValue ParseSimpleDate(const char* s) {
  return DateValue::ParseSimpleDateFormat(s, strlen(s), /* accept_time_toks */ true);
}

// Check overlaps on dates. The range is [min, max] in filter.
void CheckOverlapForDate(MinMaxFilter* filter, const ColumnType& col_type,
    const DateValue& min, const DateValue& max) {
  // Check overlapping with [min-10, min-1], which should be false.
  DateValue min_minus_1 = min.SubtractDays(1);
  DateValue min_minus_10 = min.SubtractDays(10);
  EXPECT_EQ(filter->EvalOverlap(col_type, &min_minus_10, &min_minus_1), false);

  // Check overlapping with [max+1, max+20], which should be false.
  DateValue max_plus_1 = max.AddDays(1);
  DateValue max_plus_20 = max.AddDays(20);
  EXPECT_EQ(filter->EvalOverlap(col_type, &max_plus_1, &max_plus_20), false);

  // Check overlapping with [min-10, middle_day(min, max)], which should be true.
  DateValue middle_date = DateValue::FindMiddleDate(min, max);
  EXPECT_EQ(filter->EvalOverlap(col_type, &min_minus_10, &middle_date), true);

  if (min < max) {
    // Check overlapping with [min+1, max], which should be true.
    DateValue min_plus_1 = min.AddDays(1);
    EXPECT_EQ(filter->EvalOverlap(col_type, &min_plus_1, (void*)&max), true);
  }
}

// Check overlaps on timestamps. The range is [min, max] in filter.
void CheckOverlapForTimestamp(MinMaxFilter* filter, const ColumnType& col_type,
    const TimestampValue& min, const TimestampValue& max) {
  // Check overlapping with [min-10, min-1], which should be false.
  TimestampValue min_minus_10 =
      min.Subtract(boost::posix_time::time_duration(0, 0, 0, 10));
  TimestampValue min_minus_1 = min.Subtract(boost::posix_time::time_duration(0, 0, 0, 1));
  EXPECT_EQ(filter->EvalOverlap(col_type, &min_minus_10, &min_minus_1), false);

  // Check overlapping with [max+1, max+20], which should be false.
  TimestampValue max_plus_1 = max.Add(boost::posix_time::time_duration(0, 0, 0, 1));
  TimestampValue max_plus_20 = max.Add(boost::posix_time::time_duration(0, 0, 0, 20));
  EXPECT_EQ(filter->EvalOverlap(col_type, &max_plus_1, &max_plus_20), false);

  // Check overlapping with [min-10, max], which should be true.
  EXPECT_EQ(filter->EvalOverlap(col_type, &min_minus_10, (void*)&max), true);

  if (min < max) {
    // Check overlapping with [min+1 ns, max], which should be true.
    TimestampValue min_plus_1 = min.Add(boost::posix_time::time_duration(0, 0, 0, 1));
    EXPECT_EQ(filter->EvalOverlap(col_type, &min_plus_1, (void*)&max), true);
  }

  // The overlap ratio with [min, max] should be 1.
  EXPECT_EQ(filter->ComputeOverlapRatio(col_type, (void*)&min, (void*)&max), 1.0);

  // The overlap ratio with [min-10, max+20] should be less than or very close to 1.0,
  // which is represented as 1.0.
  EXPECT_TRUE(
      (filter->ComputeOverlapRatio(col_type, (void*)&min_minus_10, (void*)&max_plus_20))
      <= 1.0);

  // No overlap. The overlap ratio should be 0.0.
  EXPECT_EQ(
      (filter->ComputeOverlapRatio(col_type, (void*)&min_minus_10, (void*)&min_minus_1)),
      0.0);
}

#define DATE_TIME_CHECK_VALS(NAME, TYPE)                                   \
  void Check##NAME##Vals(MinMaxFilter* filter, const ColumnType& col_type, \
      const TYPE& min, const TYPE& max) {                                  \
    EXPECT_EQ(*reinterpret_cast<const TYPE*>(filter->GetMin()), min);      \
    EXPECT_EQ(*reinterpret_cast<const TYPE*>(filter->GetMax()), max);      \
    EXPECT_FALSE(filter->AlwaysFalse());                                   \
    EXPECT_FALSE(filter->AlwaysTrue());                                    \
    CheckOverlapFor##NAME(filter, col_type, min, max);                     \
  }

DATE_TIME_CHECK_VALS(Timestamp, TimestampValue);
DATE_TIME_CHECK_VALS(Date, DateValue);

#define DATE_TIME_CHECK_FUNCS(NAME, TYPE, PROTOBUF_TYPE, PRIMITIVE_TYPE)            \
  do {                                                                              \
    ObjectPool obj_pool;                                                            \
    MemTracker mem_tracker;                                                         \
    ColumnType col_type(PrimitiveType::TYPE_##PRIMITIVE_TYPE);                      \
    MinMaxFilter* filter = MinMaxFilter::Create(col_type, &obj_pool, &mem_tracker); \
    /* Test the behavior of an empty filter. */                                     \
    EXPECT_TRUE(filter->AlwaysFalse());                                             \
    EXPECT_FALSE(filter->AlwaysTrue());                                             \
    MinMaxFilterPB pFilter;                                                         \
    filter->ToProtobuf(&pFilter);                                                   \
    EXPECT_TRUE(pFilter.always_false());                                            \
    EXPECT_FALSE(pFilter.always_true());                                            \
    EXPECT_FALSE(pFilter.min().has_##PROTOBUF_TYPE##_val());                        \
    EXPECT_FALSE(pFilter.max().has_##PROTOBUF_TYPE##_val());                        \
    MinMaxFilter* empty_filter =                                                    \
        MinMaxFilter::Create(pFilter, col_type, &obj_pool, &mem_tracker);           \
    EXPECT_TRUE(empty_filter->AlwaysFalse());                                       \
    EXPECT_FALSE(empty_filter->AlwaysTrue());                                       \
    empty_filter->Close();                                                          \
    /* Now insert some stuff. */                                                    \
    TYPE t1 = ParseSimple##NAME("2000-01-01 00:00:00");                             \
    filter->Insert(&t1);                                                            \
    Check##NAME##Vals(filter, col_type, t1, t1);                                    \
    TYPE t2 = ParseSimple##NAME("1990-01-01 12:30:00");                             \
    filter->Insert(&t2);                                                            \
    Check##NAME##Vals(filter, col_type, t2, t1);                                    \
    TYPE t3 = ParseSimple##NAME("2001-04-30 05:00:00");                             \
    filter->Insert(&t3);                                                            \
    Check##NAME##Vals(filter, col_type, t2, t3);                                    \
    TYPE t4 = ParseSimple##NAME("2001-04-30 01:00:00");                             \
    filter->Insert(&t4);                                                            \
    Check##NAME##Vals(filter, col_type, t2, t3);                                    \
    /* Check Protobuf. */                                                           \
    filter->ToProtobuf(&pFilter);                                                   \
    EXPECT_FALSE(pFilter.always_false());                                           \
    EXPECT_FALSE(pFilter.always_true());                                            \
    EXPECT_EQ(TYPE::FromColumnValuePB(pFilter.min()), t2);                          \
    EXPECT_EQ(TYPE::FromColumnValuePB(pFilter.max()), t3);                          \
    MinMaxFilter* filter2 =                                                         \
        MinMaxFilter::Create(pFilter, col_type, &obj_pool, &mem_tracker);           \
    Check##NAME##Vals(filter2, col_type, t2, t3);                                   \
    filter2->Close();                                                               \
    /* Check the behavior of Or. */                                                 \
    filter->ToProtobuf(&pFilter);                                                   \
    MinMaxFilterPB pFilter1;                                                        \
    t2.ToColumnValuePB(pFilter1.mutable_min());                                     \
    t4.ToColumnValuePB(pFilter1.mutable_max());                                     \
    MinMaxFilterPB pFilter2;                                                        \
    t1.ToColumnValuePB(pFilter2.mutable_min());                                     \
    t3.ToColumnValuePB(pFilter2.mutable_max());                                     \
    MinMaxFilter::Or(pFilter1, &pFilter2, col_type);                                \
    EXPECT_EQ(TYPE::FromColumnValuePB(pFilter2.min()), t2);                         \
    EXPECT_EQ(TYPE::FromColumnValuePB(pFilter2.max()), t3);                         \
    filter->Close();                                                                \
  } while (false)

// Tests that a TimestampMinMaxFilter returns the expected min/max after having values
// inserted into it, and that MinMaxFilter::Or works for timestamps.
TEST(MinMaxFilterTest, TestTimestampMinMaxFilter) {
  DATE_TIME_CHECK_FUNCS(Timestamp, TimestampValue, timestamp, TIMESTAMP);
}

// Tests that a DateMinMaxFilter returns the expected min/max after having values
// inserted into it, and that MinMaxFilter::Or works for dates.
TEST(MinMaxFilterTest, TestDateMinMaxFilter) {
  DATE_TIME_CHECK_FUNCS(Date, DateValue, date, DATE);
}

#define DECIMAL_ADD(SIZE, result_type)                                                 \
  Decimal##SIZE##Value Decimal##SIZE##Add(const Decimal##SIZE##Value& value,           \
      int precision, int scale, double x, bool* overflow) {                            \
    DCHECK(overflow);                                                                  \
    Decimal##SIZE##Value d =                                                           \
        Decimal##SIZE##Value::FromDouble(precision, scale, x, false, overflow);        \
    if (*overflow) return Decimal##SIZE##Value();                                      \
    return value.Add<result_type>(scale, d, scale, precision, scale, false, overflow); \
  }

DECIMAL_ADD(4, int32_t);
DECIMAL_ADD(8, int64_t);
DECIMAL_ADD(16, __int128_t);

#define DECIMAL_CHECK(SIZE)                                                           \
  do {                                                                                \
    EXPECT_EQ(*reinterpret_cast<const Decimal##SIZE##Value*>(filter->GetMin()), min); \
    EXPECT_EQ(*reinterpret_cast<const Decimal##SIZE##Value*>(filter->GetMax()), max); \
    EXPECT_FALSE(filter->AlwaysFalse());                                              \
    EXPECT_FALSE(filter->AlwaysTrue());                                               \
    /* Check overlaps. The range is [min, max] in filter.*/                           \
    /* Check overlapping with [min-10, min-1], which should be false.*/               \
    const int& precision = col_type.precision;                                        \
    const int& scale = col_type.scale;                                                \
    bool overflow = false;                                                            \
    Decimal##SIZE##Value min_minus_10 =                                               \
        Decimal##SIZE##Add(min, precision, scale, -10, &overflow);                    \
    EXPECT_EQ(overflow, false);                                                       \
    Decimal##SIZE##Value min_minus_1 =                                                \
        Decimal##SIZE##Add(min, precision, scale, -1, &overflow);                     \
    EXPECT_EQ(overflow, false);                                                       \
    EXPECT_EQ(filter->EvalOverlap(col_type, &min_minus_10, &min_minus_1), false);     \
    /* Check overlapping with [max+1, max+20], which should be false. */              \
    Decimal##SIZE##Value max_plus_1 =                                                 \
        Decimal##SIZE##Add(max, precision, scale, 1, &overflow);                      \
    EXPECT_EQ(overflow, false);                                                       \
    Decimal##SIZE##Value max_plus_20 =                                                \
        Decimal##SIZE##Add(max, precision, scale, 20, &overflow);                     \
    EXPECT_EQ(overflow, false);                                                       \
    EXPECT_EQ(filter->EvalOverlap(col_type, &max_plus_1, &max_plus_20), false);       \
    /* Check overlapping with [min-1, max], which should be true. */                  \
    EXPECT_EQ(filter->EvalOverlap(col_type, &min_minus_10, (void*)&max), true);       \
    /* Check overlapping with [min, max+20], which should be true. */                 \
    EXPECT_EQ(filter->EvalOverlap(col_type, (void*)&min, &max_plus_20), true);        \
  } while (false)

void CheckDecimalVals(MinMaxFilter* filter, const ColumnType& col_type,
    const Decimal4Value& min, const Decimal4Value& max) {
  DECIMAL_CHECK(4);
}

void CheckDecimalVals(MinMaxFilter* filter, const ColumnType& col_type,
    const Decimal8Value& min, const Decimal8Value& max) {
  DECIMAL_CHECK(8);
}

void CheckDecimalVals(MinMaxFilter* filter, const ColumnType& col_type,
    const Decimal16Value& min, const Decimal16Value& max) {
  DECIMAL_CHECK(16);
}

void CheckDecimalEmptyFilter(MinMaxFilter* filter, const ColumnType& column_type,
    MinMaxFilterPB* pFilter, ObjectPool* obj_pool, MemTracker* mem_tracker) {
  EXPECT_TRUE(filter->AlwaysFalse());
  EXPECT_FALSE(filter->AlwaysTrue());
  filter->ToProtobuf(pFilter);
  EXPECT_TRUE(pFilter->always_false());
  EXPECT_FALSE(pFilter->always_true());
  EXPECT_FALSE(pFilter->min().has_decimal_val());
  EXPECT_FALSE(pFilter->max().has_decimal_val());
  MinMaxFilter* empty_filter =
      MinMaxFilter::Create(*pFilter, column_type, obj_pool, mem_tracker);
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
    CheckDecimalVals(filter##SIZE, decimal##SIZE##_type, d1##SIZE, d1##SIZE);        \
    d2##SIZE =                                                                       \
        Decimal##SIZE##Value::FromDouble(PRECISION, SCALE, VALUE2, true, &overflow); \
    filter##SIZE->Insert(&d2##SIZE);                                                 \
    CheckDecimalVals(filter##SIZE, decimal##SIZE##_type, d1##SIZE, d2##SIZE);        \
    d3##SIZE =                                                                       \
        Decimal##SIZE##Value::FromDouble(PRECISION, SCALE, VALUE3, true, &overflow); \
    filter##SIZE->Insert(&d3##SIZE);                                                 \
    CheckDecimalVals(filter##SIZE, decimal##SIZE##_type, d3##SIZE, d2##SIZE);        \
  } while (false)

#define DECIMAL_CHECK_PROTOBUF(SIZE)                                                   \
  do {                                                                                 \
    filter##SIZE->ToProtobuf(&pFilter##SIZE);                                          \
    EXPECT_FALSE(pFilter##SIZE.always_false());                                        \
    EXPECT_FALSE(pFilter##SIZE.always_true());                                         \
    EXPECT_EQ(Decimal##SIZE##Value::FromColumnValuePB(pFilter##SIZE.min()), d3##SIZE); \
    EXPECT_EQ(Decimal##SIZE##Value::FromColumnValuePB(pFilter##SIZE.max()), d2##SIZE); \
    MinMaxFilter* filter##SIZE##2 = MinMaxFilter::Create(                              \
        pFilter##SIZE, decimal##SIZE##_type, &obj_pool, &mem_tracker);                 \
    CheckDecimalVals(filter##SIZE##2, decimal##SIZE##_type, d3##SIZE, d2##SIZE);       \
    filter##SIZE##2->Close();                                                          \
  } while (false)

#define DECIMAL_CHECK_OR(SIZE)                                                          \
  do {                                                                                  \
    MinMaxFilterPB pFilter1##SIZE;                                                      \
    d3##SIZE.ToColumnValuePB(pFilter1##SIZE.mutable_min());                             \
    d2##SIZE.ToColumnValuePB(pFilter1##SIZE.mutable_max());                             \
    MinMaxFilterPB pFilter2##SIZE;                                                      \
    d1##SIZE.ToColumnValuePB(pFilter2##SIZE.mutable_min());                             \
    d1##SIZE.ToColumnValuePB(pFilter2##SIZE.mutable_max());                             \
    MinMaxFilter::Or(pFilter1##SIZE, &pFilter2##SIZE, decimal##SIZE##_type);            \
    EXPECT_EQ(Decimal##SIZE##Value::FromColumnValuePB(pFilter2##SIZE.min()), d3##SIZE); \
    EXPECT_EQ(Decimal##SIZE##Value::FromColumnValuePB(pFilter2##SIZE.max()), d2##SIZE); \
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

  // Create protobuf minmax filters
  MinMaxFilterPB pFilter4, pFilter8, pFilter16;

  // Test the behavior of an empty filter.
  CheckDecimalEmptyFilter(filter4, decimal4_type, &pFilter4, &obj_pool, &mem_tracker);
  CheckDecimalEmptyFilter(filter8, decimal8_type, &pFilter8, &obj_pool, &mem_tracker);
  CheckDecimalEmptyFilter(filter16, decimal16_type, &pFilter16, &obj_pool, &mem_tracker);

  // Insert and check
  DECIMAL_INSERT_AND_CHECK(4, 9, 5, 2345.67891, 3456.78912, 1234.56789);
  DECIMAL_INSERT_AND_CHECK(
      8, 18, 9, 234567891.234567891, 345678912.345678912, 123456789.123456789);
  DECIMAL_INSERT_AND_CHECK(16, 38, 19, 2345678912345678912.2345678912345678912,
      3456789123456789123.3456789123456789123, 1234567891234567891.1234567891234567891);

  // Protobuf check
  DECIMAL_CHECK_PROTOBUF(4);
  DECIMAL_CHECK_PROTOBUF(8);
  DECIMAL_CHECK_PROTOBUF(16);

  // Check the behavior of Or.
  DECIMAL_CHECK_OR(4);
  DECIMAL_CHECK_OR(8);
  DECIMAL_CHECK_OR(16);

  // Close all filters
  filter4->Close();
  filter8->Close();
  filter16->Close();
}
