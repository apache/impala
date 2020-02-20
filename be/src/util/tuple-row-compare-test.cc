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

#include <boost/scoped_ptr.hpp>

#include "exprs/slot-ref.h"
#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"
#include "runtime/test-env.h"
#include "runtime/timestamp-value.inline.h"
#include "runtime/tuple-row.h"
#include "runtime/types.h"
#include "testutil/gtest-util.h"
#include "util/tuple-row-compare.h"

#include "common/names.h"

namespace impala {

class TupleRowCompareTest : public testing::Test {
 public:
  TupleRowCompareTest() : expr_perm_pool_(&tracker_), expr_results_pool_(&tracker_) {}

 protected:
  typedef __uint128_t uint128_t;
  /// Temporary runtime environment for the TupleRowComperator.
  scoped_ptr<TestEnv> test_env_;
  RuntimeState* runtime_state_;
  RowDescriptor desc_;

  ObjectPool pool_;
  /// A dummy MemTracker used for exprs and other things we don't need to have limits on.
  MemTracker tracker_;
  MemPool expr_perm_pool_;
  MemPool expr_results_pool_;
  scoped_ptr<TupleRowZOrderComparator> comperator_;

  vector<ScalarExpr*> ordering_exprs_;

  virtual void SetUp() {
    test_env_.reset(new TestEnv());
    ASSERT_OK(test_env_->Init());
  }

  virtual void TearDown() {
    comperator_->Close(runtime_state_);
    ScalarExpr::Close(ordering_exprs_);

    runtime_state_ = nullptr;
    test_env_.reset();
    expr_perm_pool_.FreeAll();
    expr_results_pool_.FreeAll();
    pool_.Clear();
  }

  void LoadComperator(int offset) {
    TSortInfo* tsort_info = pool_.Add(new TSortInfo);
    tsort_info->sorting_order = TSortingOrder::ZORDER;
    TupleRowComparatorConfig* config =
        pool_.Add(new TupleRowComparatorConfig(*tsort_info, ordering_exprs_));
    comperator_.reset(new TupleRowZOrderComparator(*config));
    ASSERT_OK(comperator_->Open(&pool_, runtime_state_, &expr_perm_pool_,
        &expr_results_pool_));
  }

  // Only ColumnType should be passed as template parameter.
  template <typename... Types>
  void LoadComperator(int offset, ColumnType type, Types... types) {
    //We are trying to fit into one slot, so the offset has to be < sizeof(int)*8.
    DCHECK_LT(offset, sizeof(int) * 8) << "Too many columns added.";
    SlotRef* build_expr = pool_.Add(new SlotRef(type, offset, true /* nullable */));
    ASSERT_OK(build_expr->Init(desc_, true, nullptr));
    ordering_exprs_.push_back(build_expr);
    LoadComperator(offset + type.GetSlotSize(), types...);
  }

  template <typename... Types>
  void CreateComperator(Types... types) {
    ordering_exprs_.clear();
    LoadComperator(1, types...);
  }

  template <bool IS_FIRST_SLOT_NULL = false, typename... Args>
  TupleRow* CreateTupleRow(Args... args) {
    // Only one null byte is allocated, so the joint slot size has to be < sizeof(int)*8.
    DCHECK_LE(sizeof...(args), 8);
    uint8_t* tuple_row_mem = expr_perm_pool_.Allocate(
        sizeof(char*) + sizeof(int32_t*) * sizeof...(args));
    Tuple* tuple_mem = Tuple::Create(sizeof(char) + GetSize(args...), &expr_perm_pool_);
    if (IS_FIRST_SLOT_NULL) tuple_mem->SetNull(NullIndicatorOffset(0, 1));
    FillMem(tuple_mem, 1, args...);
    TupleRow* row = reinterpret_cast<TupleRow*>(tuple_row_mem);
    row->SetTuple(0, tuple_mem);
    return row;
  }

  unsigned GetSize() { return 0; }

  template <typename T, typename... Args>
  unsigned GetSize(const T & head, const Args &... tail) {
      return sizeof(T) + GetSize(tail...);
  }

  template <typename T>
  void FillMem(Tuple* tuple_mem, int idx, T val) {
    memcpy(tuple_mem->GetSlot(idx), &val, sizeof(T));
  }

  template <typename T, typename... Args>
  void FillMem(Tuple* tuple_mem, int idx, T val, Args... args) {
    // Use memcpy to avoid gcc generating unaligned instructions like movaps
    // for int128_t. They will raise SegmentFault when addresses are not
    // aligned to 16 bytes.
    memcpy(tuple_mem->GetSlot(idx), &val, sizeof(T));
    FillMem(tuple_mem, idx + sizeof(T), args...);
  }

  template <typename T, bool IS_FIRST_SLOT_NULL = false>
  int CompareTest(T lval1, T lval2, T rval1, T rval2) {
    TupleRow* lhs = CreateTupleRow<IS_FIRST_SLOT_NULL>(lval1, lval2);
    TupleRow* rhs = CreateTupleRow(rval1, rval2);
    int result = comperator_->Compare(lhs, rhs);
    comperator_->Close(runtime_state_);
    return result;
  }

  // With this function, nullable entries can also be tested, by setting the
  // template parameter to true. This means that the first slot of the
  // left hand side is set to null, which should be equal to the
  // smallest possible value when comparing.
  template <bool IS_FIRST_SLOT_NULL = false>
  int IntIntTest(int32_t lval1, int32_t lval2, int32_t rval1, int32_t rval2) {
    CreateComperator(ColumnType(TYPE_INT), ColumnType(TYPE_INT));
    return CompareTest<int32_t, IS_FIRST_SLOT_NULL>(lval1, lval2, rval1, rval2);
  }

  int Int64Int64Test(int64_t lval1, int64_t lval2, int64_t rval1, int64_t rval2) {
    CreateComperator(ColumnType(TYPE_BIGINT), ColumnType(TYPE_BIGINT));
    return CompareTest<int64_t>(lval1, lval2, rval1, rval2);
  }

  int Int16Int16Test(int16_t lval1, int16_t lval2, int16_t rval1, int16_t rval2) {
    CreateComperator(ColumnType(TYPE_SMALLINT), ColumnType(TYPE_SMALLINT));
    return CompareTest<int16_t>(lval1, lval2, rval1, rval2);
  }

  int Int8Int8Test(int8_t lval1, int8_t lval2, int8_t rval1, int8_t rval2) {
    CreateComperator(ColumnType(TYPE_TINYINT), ColumnType(TYPE_TINYINT));
    return CompareTest<int8_t>(lval1, lval2, rval1, rval2);
  }

  int FloatFloatTest(float lval1, float lval2, float rval1, float rval2) {
    CreateComperator(ColumnType(TYPE_FLOAT), ColumnType(TYPE_FLOAT));
    return CompareTest<float>(lval1, lval2, rval1, rval2);
  }

  int DoubleDoubleTest(double lval1, double lval2, double rval1, double rval2) {
    CreateComperator(ColumnType(TYPE_DOUBLE), ColumnType(TYPE_DOUBLE));
    return CompareTest<double>(lval1, lval2, rval1, rval2);
  }

  int BoolBoolTest(bool lval1, bool lval2, bool rval1, bool rval2) {
    CreateComperator(ColumnType(TYPE_BOOLEAN), ColumnType(TYPE_BOOLEAN));
    return CompareTest<bool>(lval1, lval2, rval1, rval2);
  }

  // Char is a special case, so its tuples have to be created differently.
  // This function is responsible for only the char test below, therefore
  // the tuple will have a fix size of two slots.
  TupleRow* CreateCharArrayTupleRow(const char* ptr1, const char* ptr2) {
    uint8_t* tuple_row_mem = expr_perm_pool_.Allocate(
        sizeof(char*) + sizeof(int32_t*) * 2);
    Tuple* tuple_mem =
        Tuple::Create(sizeof(char) + strlen(ptr1) + strlen(ptr2), &expr_perm_pool_);
    memcpy(tuple_mem->GetSlot(1), ptr1, strlen(ptr1));
    memcpy(tuple_mem->GetSlot(1 + strlen(ptr1)), ptr2, strlen(ptr2));
    TupleRow* row = reinterpret_cast<TupleRow*>(tuple_row_mem);
    row->SetTuple(0, tuple_mem);
    return row;
  }

  int CharCharTest(const char *lval1, const char *lval2, const char *rval1,
      const char *rval2) {
    DCHECK_EQ(strlen(lval1), strlen(rval1));
    DCHECK_EQ(strlen(lval2), strlen(rval2));
    CreateComperator(ColumnType::CreateCharType(strlen(lval1)),
        ColumnType::CreateCharType(strlen(lval2)));

    TupleRow* lhs = CreateCharArrayTupleRow(lval1, lval2);
    TupleRow* rhs = CreateCharArrayTupleRow(rval1, rval2);
    int result = comperator_->Compare(lhs, rhs);
    comperator_->Close(runtime_state_);
    return result;
  }

  int DateDateTest(DateValue lval1, DateValue lval2, DateValue rval1, DateValue rval2) {
    CreateComperator(ColumnType(TYPE_DATE), ColumnType(TYPE_DATE));
    return CompareTest<DateValue>(lval1, lval2, rval1, rval2);
  }

  int TimestampTimestampTest(const std::string& lval1, const std::string& lval2,
      const std::string& rval1, const std::string& rval2) {
    return TimestampTimestampTest(
        TimestampValue::ParseSimpleDateFormat(lval1),
        TimestampValue::ParseSimpleDateFormat(lval2),
        TimestampValue::ParseSimpleDateFormat(rval1),
        TimestampValue::ParseSimpleDateFormat(rval2));
  }

  int TimestampTimestampTest(int64_t lval1, int64_t lval2, int64_t rval1, int64_t rval2) {
    return TimestampTimestampTest(TimestampValue::FromDaysSinceUnixEpoch(lval1),
        TimestampValue::FromDaysSinceUnixEpoch(lval2),
        TimestampValue::FromDaysSinceUnixEpoch(rval1),
        TimestampValue::FromDaysSinceUnixEpoch(rval2));
  }

  int TimestampTimestampTest(TimestampValue lval1, TimestampValue lval2,
      TimestampValue rval1, TimestampValue rval2) {
    CreateComperator(ColumnType(TYPE_TIMESTAMP), ColumnType(TYPE_TIMESTAMP));
    return CompareTest<TimestampValue>(lval1, lval2, rval1, rval2);
  }

  template<typename DECIMAL_T>
  int DecimalDecimalTest(int64_t lval1, int64_t lval2, int64_t rval1, int64_t rval2,
      int precision, int scale) {
    ColumnType decimal_column = ColumnType::CreateDecimalType(precision, scale);
    CreateComperator(decimal_column, decimal_column);
    bool overflow = false;
    DECIMAL_T l1 = DECIMAL_T::FromInt(precision, scale, lval1, &overflow);
    DECIMAL_T l2 = DECIMAL_T::FromInt(precision, scale, lval2, &overflow);
    DECIMAL_T r1 = DECIMAL_T::FromInt(precision, scale, rval1, &overflow);
    DECIMAL_T r2 = DECIMAL_T::FromInt(precision, scale, rval2, &overflow);
    TupleRow* lhs = CreateTupleRow(l1, l2);
    TupleRow* rhs = CreateTupleRow(r1, r2);
    int result = comperator_->Compare(lhs, rhs);
    comperator_->Close(runtime_state_);
    return result;
  }

  // The number of bytes to compare in a string depends on the size of the types present
  // in a row. If there are only strings present, only the first 4 bytes will be
  // considered. This is decided in TupleRowZOrderComparator::CompareInterpreted, where
  // the GetByteSize returns 0 for strings.
  int StringString4ByteTest(const std::string& lval1, const std::string& lval2,
      const std::string& rval1, const std::string& rval2) {
    CreateComperator(ColumnType(TYPE_STRING), ColumnType(TYPE_STRING));
    return CompareTest<StringValue>(StringValue(lval1), StringValue(lval2),
        StringValue(rval1), StringValue(rval2));
  }

  // Requires that the comparator has already been created with the two String/Varchar
  // columns and a column corresponding to T.
  template<typename T>
  int GenericStringTest(const std::string& lval1, const std::string& lval2,
      const std::string& rval1, const std::string& rval2, T dummyValue)  {
    TupleRow* lhs = CreateTupleRow(StringValue(lval1), StringValue(lval2), dummyValue);
    TupleRow* rhs = CreateTupleRow(StringValue(rval1), StringValue(rval2), dummyValue);
    int result = comperator_->Compare(lhs, rhs);
    comperator_->Close(runtime_state_);
    return result;
  }

  int StringString8ByteTest(const std::string& lval1, const std::string& lval2,
      const std::string& rval1, const std::string& rval2) {
    // TYPE_BIGINT will enforce checking the first 8 bytes of the strings.
    CreateComperator(
        ColumnType(TYPE_STRING), ColumnType(TYPE_STRING), ColumnType(TYPE_BIGINT));
    int64_t dummyValue = 0;
    return GenericStringTest<int64_t>(lval1, lval2, rval1, rval2, dummyValue);
  }

  int StringString16ByteTest(const std::string& lval1, const std::string& lval2,
      const std::string& rval1, const std::string& rval2) {
    // The Decimal16Value column will enforce checking the first 16 bytes of the strings.
    int precision = ColumnType::MAX_PRECISION;
    int scale = 0;
    bool overflow = false;
    Decimal16Value dummyValue = Decimal16Value::FromInt(precision, scale, 0, &overflow);
    CreateComperator(ColumnType(TYPE_STRING), ColumnType(TYPE_STRING),
        ColumnType::CreateDecimalType(precision, scale));
    return GenericStringTest<Decimal16Value>(lval1, lval2, rval1, rval2, dummyValue);
  }

  int VarcharVarchar4ByteTest(const std::string& lval1, const std::string& lval2,
      const std::string& rval1, const std::string& rval2) {
    CreateComperator(ColumnType::CreateVarcharType(32),
        ColumnType::CreateVarcharType(32));
    return CompareTest<StringValue>(StringValue(lval1), StringValue(lval2),
        StringValue(rval1), StringValue(rval2));
  }

  int VarcharVarchar8ByteTest(const std::string& lval1, const std::string& lval2,
      const std::string& rval1, const std::string& rval2) {
    // TYPE_BIGINT will enforce checking the first 8 bytes of the strings.
    CreateComperator(ColumnType::CreateVarcharType(32),
        ColumnType::CreateVarcharType(32), ColumnType(TYPE_BIGINT));
    int64_t dummyValue = 0;
    return GenericStringTest<Decimal16Value>(lval1, lval2, rval1, rval2, dummyValue);
  }

  int VarcharVarchar16ByteTest(const std::string& lval1, const std::string& lval2,
      const std::string& rval1, const std::string& rval2) {
    // The Decimal16Value column will enforce checking the first 16 bytes of the strings.
    int precision = ColumnType::MAX_PRECISION;
    int scale = 0;
    bool overflow = false;
    Decimal16Value dummyValue = Decimal16Value::FromInt(precision, scale, 0, &overflow);
    CreateComperator(ColumnType::CreateVarcharType(32),
        ColumnType::CreateVarcharType(32),
        ColumnType::CreateDecimalType(precision, scale));
    return GenericStringTest<Decimal16Value>(lval1, lval2, rval1, rval2, dummyValue);
  }
};

// The Z-values used and their order are visualized in the following image:
// https://en.wikipedia.org/wiki/File:Z-curve.svg
TEST_F(TupleRowCompareTest, Int32Test) {
  EXPECT_EQ(IntIntTest(0, 0, 0, 0), 0);
  EXPECT_EQ(IntIntTest(-5, 3, -5, 3), 0);

  EXPECT_EQ(IntIntTest(1, 0, 0, 1), 1);
  EXPECT_EQ(IntIntTest(0, 1, 1, 0), -1);

  EXPECT_EQ(IntIntTest(1, 0, 0, 1), 1);
  EXPECT_EQ(IntIntTest(2, 4, 1, 7), 1);
  EXPECT_EQ(IntIntTest(3, 7, 4, 0), -1);
  EXPECT_EQ(IntIntTest(6, 4, 5, 7), 1);
  EXPECT_EQ(IntIntTest(5, 5, 6, 4), -1);
  EXPECT_EQ(IntIntTest(6, 1, 3, 7), 1);

  EXPECT_EQ(IntIntTest(INT32_MAX / 2 + 2, 1, 1, INT32_MAX), 1);
  EXPECT_EQ(IntIntTest(INT32_MAX / 2, 1, 1, INT32_MAX), -1);

  // Some null tests (see details at IntIntTest)
  EXPECT_EQ(IntIntTest<true>(1, 1, 1, 1), -1);
  EXPECT_EQ(IntIntTest<true>(4242, 1, 1, 1), -1);
  EXPECT_EQ(IntIntTest<true>(1, 0, 0, 1), -1);
  EXPECT_EQ(IntIntTest<true>(1, 0, INT32_MIN, 0), 0);
}

TEST_F(TupleRowCompareTest, Int64Test) {
  EXPECT_EQ(Int64Int64Test(0, 0, 0, 0), 0);
  EXPECT_EQ(Int64Int64Test(-5, 3, -5, 3), 0);

  EXPECT_EQ(Int64Int64Test(1, 0, 0, 1), 1);
  EXPECT_EQ(Int64Int64Test(0, 1, 1, 0), -1);

  EXPECT_EQ(Int64Int64Test(1, 0, 0, 1), 1);
  EXPECT_EQ(Int64Int64Test(2, 4, 1, 7), 1);
  EXPECT_EQ(Int64Int64Test(3, 7, 4, 0), -1);
  EXPECT_EQ(Int64Int64Test(6, 4, 5, 7), 1);
  EXPECT_EQ(Int64Int64Test(5, 5, 6, 4), -1);
  EXPECT_EQ(Int64Int64Test(6, 1, 3, 7), 1);

  EXPECT_EQ(Int64Int64Test(INT64_MAX / 2 + 2, 1, 1, INT64_MAX), 1);
  EXPECT_EQ(Int64Int64Test(INT64_MAX / 2, 1, 1, INT64_MAX), -1);
}

TEST_F(TupleRowCompareTest, Int16Test) {
  EXPECT_EQ(Int16Int16Test(0, 0, 0, 0), 0);
  EXPECT_EQ(Int16Int16Test(-5, 3, -5, 3), 0);

  EXPECT_EQ(Int16Int16Test(1, 0, 0, 1), 1);
  EXPECT_EQ(Int64Int64Test(0, 1, 1, 0), -1);

  EXPECT_EQ(Int16Int16Test(1, 0, 0, 1), 1);
  EXPECT_EQ(Int16Int16Test(2, 4, 1, 7), 1);
  EXPECT_EQ(Int16Int16Test(3, 7, 4, 0), -1);
  EXPECT_EQ(Int16Int16Test(6, 4, 5, 7), 1);
  EXPECT_EQ(Int16Int16Test(5, 5, 6, 4), -1);
  EXPECT_EQ(Int16Int16Test(6, 1, 3, 7), 1);

  EXPECT_EQ(Int16Int16Test(INT16_MAX / 2 + 2, 1, 1, INT16_MAX), 1);
  EXPECT_EQ(Int16Int16Test(INT16_MAX / 2, 1, 1, INT16_MAX), -1);
}

TEST_F(TupleRowCompareTest, Int8Test) {
  EXPECT_EQ(Int8Int8Test(0, 0, 0, 0), 0);
  EXPECT_EQ(Int8Int8Test(-5, 3, -5, 3), 0);

  EXPECT_EQ(Int8Int8Test(1, 0, 0, 1), 1);
  EXPECT_EQ(Int8Int8Test(0, 1, 1, 0), -1);

  EXPECT_EQ(Int8Int8Test(1, 0, 0, 1), 1);
  EXPECT_EQ(Int8Int8Test(2, 4, 1, 7), 1);
  EXPECT_EQ(Int8Int8Test(3, 7, 4, 0), -1);
  EXPECT_EQ(Int8Int8Test(6, 4, 5, 7), 1);
  EXPECT_EQ(Int8Int8Test(5, 5, 6, 4), -1);
  EXPECT_EQ(Int8Int8Test(6, 1, 3, 7), 1);

  EXPECT_EQ(Int8Int8Test(INT8_MAX / 2 + 2, 1, 1, INT8_MAX), 1);
  EXPECT_EQ(Int8Int8Test(INT8_MAX / 2, 1, 1, INT8_MAX), -1);
}

TEST_F(TupleRowCompareTest, FloatTest) {
  EXPECT_EQ(FloatFloatTest(1.0f, 0.0f, 0.0f, 1.0f), 1);
  EXPECT_EQ(FloatFloatTest(0.0f, 1.0f, 1.0f, 0.0f), -1);

  EXPECT_EQ(FloatFloatTest(4.0f, 3.0f, 3.0f, 4.0f), 1);
  EXPECT_EQ(FloatFloatTest(5.0f, 7.0f, 4.0f, 10.0f), -1);
  EXPECT_EQ(FloatFloatTest(6.0f, 10.0f, 7.0f, 3.0f), 1);
  EXPECT_EQ(FloatFloatTest(9.0f, 7.0f, 8.0f, 10.0f), -1);
  EXPECT_EQ(FloatFloatTest(8.0f , 8.0f, 9.0f, 7.0f), 1);
  EXPECT_EQ(FloatFloatTest(9.0f, 4.0f, 6.0f, 10.0f), 1);

  EXPECT_EQ(FloatFloatTest(-4.0f, -3.0f, -3.0f, -4.0f), -1);
  EXPECT_EQ(FloatFloatTest(-5.0f, -7.0f, -4.0f, -10.0f), 1);
  EXPECT_EQ(FloatFloatTest(-6.0f, -10.0f, -7.0f, -3.0f), -1);
  EXPECT_EQ(FloatFloatTest(-9.0f, -7.0f, -8.0f, -10.0f), 1);
  EXPECT_EQ(FloatFloatTest(-8.0f, -8.0f, -9.0f, -7.0f), -1);
  EXPECT_EQ(FloatFloatTest(-9.0f, -4.0f, -6.0f, -10.0f), -1);

  EXPECT_EQ(FloatFloatTest(FLT_MAX / 2.0f + 2.0f, 1.0f, 1.0f, FLT_MAX), 1);
}

TEST_F(TupleRowCompareTest, DoubleTest) {
  EXPECT_EQ(DoubleDoubleTest(1.0, 0.0, 0.0, 1.0f), 1);
  EXPECT_EQ(DoubleDoubleTest(0.0, 1.0, 1.0, 0.0f), -1);

  EXPECT_EQ(DoubleDoubleTest(4.0, 3.0, 3.0, 4.0), 1);
  EXPECT_EQ(DoubleDoubleTest(5.0, 7.0, 4.0, 10.0), -1);
  EXPECT_EQ(DoubleDoubleTest(6.0, 10.0, 7.0, 3.0), 1);
  EXPECT_EQ(DoubleDoubleTest(9.0, 7.0, 8.0, 10.0), -1);
  EXPECT_EQ(DoubleDoubleTest(8.0, 8.0, 9.0, 7.0), 1);
  EXPECT_EQ(DoubleDoubleTest(9.0, 4.0, 6.0, 10.0), 1);

  EXPECT_EQ(DoubleDoubleTest(-4.0, -3.0, -3.0, -4.0), -1);
  EXPECT_EQ(DoubleDoubleTest(-5.0, -7.0, -4.0, -10.0), 1);
  EXPECT_EQ(DoubleDoubleTest(-6.0, -10.0, -7.0, -3.0), -1);
  EXPECT_EQ(DoubleDoubleTest(-9.0, -7.0, -8.0, -10.0), 1);
  EXPECT_EQ(DoubleDoubleTest(-8.0, -8.0, -9.0, -7.0), -1);
  EXPECT_EQ(DoubleDoubleTest(-9.0, -4.0, -6.0, -10.0), -1);

  EXPECT_EQ(DoubleDoubleTest(DBL_MAX / 2.0 + 2.0, 1.0, 1.0, DBL_MAX), 1);
}

TEST_F(TupleRowCompareTest, BoolTest) {
  EXPECT_EQ(BoolBoolTest(true, false, true, false), 0);
  EXPECT_EQ(BoolBoolTest(false, true, false, true), 0);

  EXPECT_EQ(BoolBoolTest(true, true, true, false), 1);
  EXPECT_EQ(BoolBoolTest(false, true, true, true), -1);
  EXPECT_EQ(BoolBoolTest(false, true, false, false), 1);
  EXPECT_EQ(BoolBoolTest(false, false, false, true), -1);
  EXPECT_EQ(BoolBoolTest(true, false, false, false), 1);
}

TEST_F(TupleRowCompareTest, CharTest) {
  EXPECT_EQ(CharCharTest("a", "b", "a", "b"), 0);
  EXPECT_EQ(CharCharTest("a", "b", "a", "b"), 0);
  EXPECT_EQ(CharCharTest("h", "0", "h", "0"), 0);

  EXPECT_EQ(CharCharTest("h", "z", "z", "h"), -1);
  EXPECT_EQ(CharCharTest("a", "0", "h", "0"), -1);
  EXPECT_EQ(CharCharTest("!", "{", "0", "K"), 1);
  EXPECT_EQ(CharCharTest("A", "~", "B", "Z"), 1);

  EXPECT_EQ(CharCharTest("aaa", "bbb", "aaa", "bbb"), 0);
  EXPECT_EQ(CharCharTest("abc", "bbc", "abc", "bbc"), 0);
  EXPECT_EQ(CharCharTest("aah", "aa0", "aah", "aa0"), 0);

  EXPECT_EQ(CharCharTest("aaa", "aa0", "aah", "aa0"), -1);
  EXPECT_EQ(CharCharTest("aah", "aaz", "aaz", "aah"), -1);
  EXPECT_EQ(CharCharTest("aa!", "aa{", "aa0", "aaK"), 1);
  EXPECT_EQ(CharCharTest("aaA", "aa~", "aaB", "aaZ"), 1);
}

TEST_F(TupleRowCompareTest, DateTest) {
  EXPECT_EQ(Int8Int8Test(0, 0, 0, 0), 0);
  EXPECT_EQ(Int8Int8Test(-5, 3, -5, 3), 0);

  EXPECT_EQ(Int8Int8Test(1, 0, 0, 1), 1);
  EXPECT_EQ(Int8Int8Test(0, 1, 1, 0), -1);

  EXPECT_EQ(Int8Int8Test(1, 0, 0, 1), 1);
  EXPECT_EQ(Int8Int8Test(2, 4, 1, 7), 1);
  EXPECT_EQ(Int8Int8Test(3, 7, 4, 0), -1);
  EXPECT_EQ(Int8Int8Test(6, 4, 5, 7), 1);
  EXPECT_EQ(Int8Int8Test(5, 5, 6, 4), -1);
  EXPECT_EQ(Int8Int8Test(6, 1, 3, 7), 1);

  EXPECT_EQ(Int8Int8Test(INT8_MAX / 2 + 2, 1, 1, INT8_MAX), 1);
  EXPECT_EQ(Int8Int8Test(INT8_MAX / 2, 1, 1, INT8_MAX), -1);
}

TEST_F(TupleRowCompareTest, TimestampTest) {
  EXPECT_EQ(TimestampTimestampTest(
      "2015-04-09 14:07:46.580465000", "2015-04-09 14:07:46.580465000",
      "2015-04-09 14:07:46.580465000", "2015-04-09 14:07:46.580465000"), 0);
  EXPECT_EQ(TimestampTimestampTest(
      "1415-12-09 10:07:44.314159265", "2015-04-09 14:07:46.580465000",
      "1415-12-09 10:07:44.314159265", "2015-04-09 14:07:46.580465000"), 0);

  EXPECT_EQ(TimestampTimestampTest(1, 0, 0, 1), 1);
  EXPECT_EQ(TimestampTimestampTest(0, 1, 1, 0), -1);

  EXPECT_EQ(TimestampTimestampTest(1, 0, 0, 1), 1);
  EXPECT_EQ(TimestampTimestampTest(2, 4, 1, 7), 1);
  EXPECT_EQ(TimestampTimestampTest(3, 7, 4, 0), -1);
  EXPECT_EQ(TimestampTimestampTest(6, 4, 5, 7), 1);
  EXPECT_EQ(TimestampTimestampTest(5, 5, 6, 4), -1);
  EXPECT_EQ(TimestampTimestampTest(6, 1, 3, 7), 1);

  EXPECT_EQ(TimestampTimestampTest(
      "1400-01-01 00:00:00.000000000", "9999-12-31 14:07:46.580465000",
      "8000-12-09 10:07:44.314159265", "2015-04-09 14:07:46.580465000"), -1);

  EXPECT_EQ(TimestampTimestampTest(
      "1400-01-01 00:00:00.000000001", "1400-01-01 00:00:00.000000000",
      "1400-01-01 00:00:00.000000000", "1400-01-01 00:00:00.000000001"), 1);
  EXPECT_EQ(TimestampTimestampTest(
      "1400-01-01 00:00:00.000000003", "1400-01-01 00:00:00.000000007",
      "1400-01-01 00:00:00.000000004", "1400-01-01 00:00:00.000000000"), -1);
  EXPECT_EQ(TimestampTimestampTest(
      "1400-01-01 00:00:00.000000006", "1400-01-01 00:00:00.000000004",
      "1400-01-01 00:00:00.000000005", "1400-01-01 00:00:00.000000007"), 1);
  EXPECT_EQ(TimestampTimestampTest(
      "1400-01-01 00:00:00.000000005", "1400-01-01 00:00:00.000000005",
      "1400-01-01 00:00:00.000000006", "1400-01-01 00:00:00.000000004"), -1);

  EXPECT_EQ(TimestampTimestampTest(
      "1400-01-01 23:59:59.999999999", "1400-01-01 00:00:00.000000000",
      "1400-01-02 00:00:00.000000000", "1400-01-01 00:00:00.000000000"), -1);
  EXPECT_EQ(TimestampTimestampTest(
      "3541-11-03 23:59:59.999999999", "3541-11-03 00:00:00.000000000",
      "3541-11-04 00:00:00.000000000", "3541-11-03 00:00:00.000000000"), -1);
}


TEST_F(TupleRowCompareTest, DecimalTest) {
  EXPECT_EQ(DecimalDecimalTest<Decimal4Value>(1, 1, 1, 1, 4, 2), 0);
  EXPECT_EQ(DecimalDecimalTest<Decimal4Value>(-5, 3, -5, 3, 2, 1), 0);

  EXPECT_EQ(DecimalDecimalTest<Decimal4Value>(1, 0, 0, 1, 1, 0), 1);
  EXPECT_EQ(DecimalDecimalTest<Decimal4Value>(0, 1, 1, 0, 1, 0), -1);

  EXPECT_EQ(DecimalDecimalTest<Decimal4Value>(256, 10, 255, 100, 4, 2), 1);
  EXPECT_EQ(DecimalDecimalTest<Decimal4Value>(3, 1024, 128, 1023, 9, 1), 1);
  EXPECT_EQ(DecimalDecimalTest<Decimal4Value>(1024, 511, 1023, 0, 5, 0), 1);
  EXPECT_EQ(DecimalDecimalTest<Decimal4Value>(5550, 0, 5000, 4097, 9, 3), -1);


  EXPECT_EQ(DecimalDecimalTest<Decimal8Value>(1, 1, 1, 1, 4, 2), 0);
  EXPECT_EQ(DecimalDecimalTest<Decimal8Value>(-5, 3, -5, 3, 2, 1), 0);

  EXPECT_EQ(DecimalDecimalTest<Decimal8Value>(1, 0, 0, 1, 1, 0), 1);
  EXPECT_EQ(DecimalDecimalTest<Decimal8Value>(0, 1, 1, 0, 1, 0), -1);

  EXPECT_EQ(DecimalDecimalTest<Decimal8Value>(256, 10, 255, 100, 18, 0), 1);
  EXPECT_EQ(DecimalDecimalTest<Decimal8Value>(3, 1024, 128, 1023, 18, 1), 1);
  EXPECT_EQ(DecimalDecimalTest<Decimal8Value>(1024, 511, 1023, 0, 18, 0), 1);
  EXPECT_EQ(DecimalDecimalTest<Decimal8Value>(5550, 0, 5000, 4097, 18, 3), -1);


  EXPECT_EQ(DecimalDecimalTest<Decimal16Value>(1, 1, 1, 1, 4, 2), 0);
  EXPECT_EQ(DecimalDecimalTest<Decimal16Value>(-5, 3, -5, 3, 2, 1), 0);

  EXPECT_EQ(DecimalDecimalTest<Decimal16Value>(1, 0, 0, 1, 1, 0), 1);
  EXPECT_EQ(DecimalDecimalTest<Decimal16Value>(0, 1, 1, 0, 1, 0), -1);

  EXPECT_EQ(DecimalDecimalTest<Decimal16Value>(256, 10, 255, 100,
      ColumnType::MAX_PRECISION, 0), 1);
  EXPECT_EQ(DecimalDecimalTest<Decimal16Value>(3, 1024, 128, 1023,
      ColumnType::MAX_PRECISION, 0), 1);
  EXPECT_EQ(DecimalDecimalTest<Decimal16Value>(1024, 511, 1023, 0,
      ColumnType::MAX_PRECISION, 0), 1);
  EXPECT_EQ(DecimalDecimalTest<Decimal16Value>(5550, 0, 5000, 4097,
      ColumnType::MAX_PRECISION, 0), -1);
}

TEST_F(TupleRowCompareTest, AllTypeTest)  {
  int precision = 9, scale = 0;
  bool overflow = false;
  CreateComperator(ColumnType(TYPE_TIMESTAMP), ColumnType(TYPE_BOOLEAN),
      ColumnType(TYPE_TINYINT), ColumnType(TYPE_BIGINT), ColumnType::CreateCharType(1),
      ColumnType(ColumnType::CreateDecimalType(precision, scale)));

  TupleRow* lhs = CreateTupleRow(
      TimestampValue::ParseSimpleDateFormat("1400-01-01 00:00:00.000000000"),
      false, static_cast<int8_t>(0), static_cast<int64_t>(0), '~',
      Decimal4Value::FromInt(precision, scale, 0, &overflow));
  TupleRow* rhs = CreateTupleRow(
      TimestampValue::ParseSimpleDateFormat("1400-01-01 00:00:00.000000000"),
      false, static_cast<int8_t>(0), static_cast<int64_t>(0), '~',
      Decimal4Value::FromInt(precision, scale, 0, &overflow));
  EXPECT_EQ(comperator_->Compare(lhs, rhs), 0);

  lhs = CreateTupleRow(
      TimestampValue::ParseSimpleDateFormat("1400-01-01 00:00:00.000000002"),
      false, static_cast<int8_t>(1), static_cast<int64_t>(1), '~',
      Decimal4Value::FromInt(precision, scale, 1, &overflow));
  rhs = CreateTupleRow(
      TimestampValue::ParseSimpleDateFormat("1400-01-01 00:00:00.000000001"),
      false, static_cast<int8_t>(1), static_cast<int64_t>(1), '~',
      Decimal4Value::FromInt(precision, scale, 1, &overflow));
  EXPECT_EQ(comperator_->Compare(lhs, rhs), 1);

  lhs = CreateTupleRow(
      TimestampValue::ParseSimpleDateFormat("1400-01-01 00:00:00.000000002"),
      false, static_cast<int8_t>(1), static_cast<int64_t>(1), '~',
      Decimal4Value::FromInt(precision, scale, 1, &overflow));
  rhs = CreateTupleRow(
      TimestampValue::ParseSimpleDateFormat("1400-01-01 00:00:00.000000001"),
      false, static_cast<int8_t>(42), static_cast<int64_t>(1), '~',
      Decimal4Value::FromInt(precision, scale, 1, &overflow));
  EXPECT_EQ(comperator_->Compare(lhs, rhs), -1);

  // Checking the dominance of types (bigger types should not dominate smaller ones)
  lhs = CreateTupleRow(
      TimestampValue::ParseSimpleDateFormat("1400-01-01 00:00:00.000000000"),
      true, static_cast<int8_t>(0), static_cast<int64_t>(0), '~',
      Decimal4Value::FromInt(precision, scale, 0, &overflow));
  rhs = CreateTupleRow(
      TimestampValue::ParseSimpleDateFormat("1400-01-01 23:59:59.999999999"),
      false, static_cast<int8_t>(0), static_cast<int64_t>(0), '~',
      Decimal4Value::FromInt(precision, scale, 0, &overflow));
  EXPECT_EQ(comperator_->Compare(lhs, rhs), 1);

  lhs = CreateTupleRow(
      TimestampValue::ParseSimpleDateFormat("1400-01-01 00:00:00.000000000"),
      false, static_cast<int8_t>(0), static_cast<int64_t>(1) << 5, '~',
      Decimal4Value::FromInt(precision, scale, 0, &overflow));
  rhs = CreateTupleRow(
      TimestampValue::ParseSimpleDateFormat("1400-01-01 23:59:59.999999999"),
      false, static_cast<int8_t>(0), static_cast<int64_t>(0), '~',
      Decimal4Value::FromInt(precision, scale, 0, &overflow));
  EXPECT_EQ(comperator_->Compare(lhs, rhs), 1);

  lhs = CreateTupleRow(
      TimestampValue::ParseSimpleDateFormat("1400-01-01 00:00:00.000000000"),
      false, static_cast<int8_t>(0), static_cast<int64_t>(1) << 5, '~',
      Decimal4Value::FromInt(precision, scale, 0, &overflow));
  rhs = CreateTupleRow(
      TimestampValue::ParseSimpleDateFormat("1400-01-01 00:00:00.000000000"),
      false, static_cast<int8_t>(1), static_cast<int64_t>(0), '~',
      Decimal4Value::FromInt(precision, scale, 0, &overflow));
  EXPECT_EQ(comperator_->Compare(lhs, rhs), -1);

  comperator_->Close(runtime_state_);
}

// Without converting into 128-bit representation, only 64-bit comparison
TEST_F(TupleRowCompareTest, All64TypeTest)  {
  int precision = 9, scale = 0;
  bool overflow = false;
  CreateComperator(ColumnType(TYPE_BOOLEAN), ColumnType(TYPE_TINYINT),
      ColumnType(TYPE_SMALLINT), ColumnType(TYPE_INT),
      ColumnType(TYPE_BIGINT), ColumnType::CreateCharType(1),
      ColumnType(ColumnType::CreateDecimalType(precision, scale)));

  TupleRow* lhs = CreateTupleRow(
      false, static_cast<int8_t>(0), static_cast<int16_t>(0), static_cast<int32_t>(0),
      static_cast<int64_t>(0), '~',
      Decimal4Value::FromInt(precision, scale, 0, &overflow));
  TupleRow* rhs = CreateTupleRow(
      false, static_cast<int8_t>(0), static_cast<int16_t>(0), static_cast<int32_t>(0),
      static_cast<int64_t>(0), '~',
      Decimal4Value::FromInt(precision, scale, 0, &overflow));
  EXPECT_EQ(comperator_->Compare(lhs, rhs), 0);

  lhs = CreateTupleRow(
        false, static_cast<int8_t>(1), static_cast<int16_t>(1), static_cast<int32_t>(1),
        static_cast<int64_t>(1), 'z',
        Decimal4Value::FromInt(precision, scale, 1, &overflow));
  rhs = CreateTupleRow(
        false, static_cast<int8_t>(1), static_cast<int16_t>(1), static_cast<int32_t>(1),
        static_cast<int64_t>(1), 'a',
        Decimal4Value::FromInt(precision, scale, 1, &overflow));
  EXPECT_EQ(comperator_->Compare(lhs, rhs), 1);

  lhs = CreateTupleRow(
        false, static_cast<int8_t>(1), static_cast<int16_t>(1), static_cast<int32_t>(1),
        static_cast<int64_t>(1), '~',
        Decimal4Value::FromInt(precision, scale, 1, &overflow));
  rhs = CreateTupleRow(
        false, static_cast<int8_t>(1), static_cast<int16_t>(56), static_cast<int32_t>(1),
        static_cast<int64_t>(1), '~',
        Decimal4Value::FromInt(precision, scale, 1, &overflow));
  EXPECT_EQ(comperator_->Compare(lhs, rhs), -1);

  comperator_->Close(runtime_state_);
}

// Without converting into 64-bit representation, only 32-bit comparison
TEST_F(TupleRowCompareTest, All32TypeTest)  {
  int precision = 9, scale = 0;
  bool overflow = false;
  CreateComperator(ColumnType(TYPE_BOOLEAN), ColumnType(TYPE_TINYINT),
      ColumnType(TYPE_SMALLINT), ColumnType(TYPE_INT), ColumnType::CreateCharType(1),
      ColumnType(ColumnType::CreateDecimalType(precision, scale)));

  TupleRow* lhs = CreateTupleRow(
      false, static_cast<int8_t>(0), static_cast<int16_t>(0), static_cast<int32_t>(0),
      '~', Decimal4Value::FromInt(precision, scale, 0, &overflow));
  TupleRow* rhs = CreateTupleRow(
      false, static_cast<int8_t>(0), static_cast<int16_t>(0), static_cast<int32_t>(0),
      '~', Decimal4Value::FromInt(precision, scale, 0, &overflow));
  EXPECT_EQ(comperator_->Compare(lhs, rhs), 0);

  lhs = CreateTupleRow(
        false, static_cast<int8_t>(1), static_cast<int16_t>(1), static_cast<int32_t>(1),
        '~', Decimal4Value::FromInt(precision, scale, 312, &overflow));
  rhs = CreateTupleRow(
        false, static_cast<int8_t>(1), static_cast<int16_t>(1), static_cast<int32_t>(1),
        '~', Decimal4Value::FromInt(precision, scale, 1, &overflow));
  EXPECT_EQ(comperator_->Compare(lhs, rhs), 1);

  lhs = CreateTupleRow(
        false, static_cast<int8_t>(1), static_cast<int16_t>(1), static_cast<int32_t>(1),
        '~', Decimal4Value::FromInt(precision, scale, 1, &overflow));
  rhs = CreateTupleRow(
        false, static_cast<int8_t>(1), static_cast<int16_t>(1), static_cast<int32_t>(56),
        '~', Decimal4Value::FromInt(precision, scale, 1, &overflow));
  EXPECT_EQ(comperator_->Compare(lhs, rhs), -1);

  comperator_->Close(runtime_state_);
}

TEST_F(TupleRowCompareTest, StringTest) {
  EXPECT_EQ(StringString4ByteTest("hello", "hello", "hello", "hello"), 0);
  EXPECT_EQ(StringString4ByteTest(std::string(255, 'a'), std::string(255, 'a'),
      std::string(255, 'a'), std::string(255, 'a')), 0);
  EXPECT_EQ(StringString4ByteTest(std::string(2550, 'a'), std::string(2550, 'a'),
      std::string(2550, 'a'), std::string(2550, 'a')), 0);

  EXPECT_EQ(StringString4ByteTest("hello1", "hello2", "hello3", "hello4"), 0);
  EXPECT_EQ(StringString4ByteTest("2", "h", "2", "h2"), -1);
  EXPECT_EQ(StringString4ByteTest("", "h", "", ""), 1);
  EXPECT_EQ(StringString4ByteTest("ab", "cd", "aw", "ca"), -1);
  EXPECT_EQ(StringString4ByteTest("ab", "yd", "ac", "ca"), 1);
  EXPECT_EQ(StringString4ByteTest("zz", "ydz", "a", "caaa"), 1);

  EXPECT_EQ(StringString8ByteTest("hello1", "hello2", "hello3", "hello4"), -1);
  EXPECT_EQ(StringString8ByteTest("12345678a", "12345678b",
      "12345678c", "12345678d"), 0);
  EXPECT_EQ(StringString8ByteTest("aa", "bbbbbbbb", "aa", "bbbbbbba"), 1);
  EXPECT_EQ(StringString8ByteTest("2", "h", "2", "h2"), -1);
  EXPECT_EQ(StringString8ByteTest("", "h", "", ""), 1);
  EXPECT_EQ(StringString8ByteTest("ab", "cd", "aw", "ca"), -1);
  EXPECT_EQ(StringString8ByteTest("ab", "yd", "ac", "ca"), 1);
  EXPECT_EQ(StringString8ByteTest("zz", "ydz", "a", "caaa"), 1);

  EXPECT_EQ(StringString16ByteTest("12345678a", "12345678b",
      "12345678c", "12345678d"), -1);
  EXPECT_EQ(StringString16ByteTest("1234567812345678a", "1234567812345678b",
      "1234567812345678c", "1234567812345678d"), 0);
  EXPECT_EQ(StringString16ByteTest("aa", "bbbbbbbb", "aa", "bbbbbbba"), 1);
  EXPECT_EQ(StringString16ByteTest("aa", "1234567812345678",
      "aa", "1234567812345679"), -1);
  EXPECT_EQ(StringString16ByteTest("2", "h", "2", "h2"), -1);
  EXPECT_EQ(StringString16ByteTest("", "h", "", ""), 1);
  EXPECT_EQ(StringString16ByteTest("ab", "cd", "aw", "ca"), -1);
  EXPECT_EQ(StringString16ByteTest("ab", "yd", "ac", "ca"), 1);
  EXPECT_EQ(StringString16ByteTest("zz", "ydz", "a", "caaa"), 1);
}

TEST_F(TupleRowCompareTest, VarcharTest) {
  EXPECT_EQ(VarcharVarchar4ByteTest("hello", "hello", "hello", "hello"), 0);

  EXPECT_EQ(VarcharVarchar4ByteTest("hello", "hello", "hello", "hello"), 0);
  EXPECT_EQ(VarcharVarchar4ByteTest(std::string(255, 'a'), std::string(255, 'a'),
      std::string(255, 'a'), std::string(255, 'a')), 0);
  EXPECT_EQ(VarcharVarchar4ByteTest(std::string(2550, 'a'), std::string(2550, 'a'),
      std::string(2550, 'a'), std::string(2550, 'a')), 0);

  EXPECT_EQ(VarcharVarchar4ByteTest("hello1", "hello2", "hello3", "hello4"), 0);
  EXPECT_EQ(VarcharVarchar4ByteTest("2", "h", "2", "h2"), -1);
  EXPECT_EQ(VarcharVarchar4ByteTest("", "h", "", ""), 1);
  EXPECT_EQ(VarcharVarchar4ByteTest("ab", "cd", "aw", "ca"), -1);
  EXPECT_EQ(VarcharVarchar4ByteTest("ab", "yd", "ac", "ca"), 1);
  EXPECT_EQ(VarcharVarchar4ByteTest("zz", "ydz", "a", "caaa"), 1);

  EXPECT_EQ(VarcharVarchar8ByteTest("hello1", "hello2", "hello3", "hello4"), -1);
  EXPECT_EQ(VarcharVarchar8ByteTest("12345678a", "12345678b",
      "12345678c", "12345678d"), 0);
  EXPECT_EQ(VarcharVarchar8ByteTest("aa", "bbbbbbbb", "aa", "bbbbbbba"), 1);
  EXPECT_EQ(VarcharVarchar8ByteTest("2", "h", "2", "h2"), -1);
  EXPECT_EQ(VarcharVarchar8ByteTest("", "h", "", ""), 1);
  EXPECT_EQ(VarcharVarchar8ByteTest("ab", "cd", "aw", "ca"), -1);
  EXPECT_EQ(VarcharVarchar8ByteTest("ab", "yd", "ac", "ca"), 1);
  EXPECT_EQ(VarcharVarchar8ByteTest("zz", "ydz", "a", "caaa"), 1);

  EXPECT_EQ(VarcharVarchar16ByteTest("12345678a", "12345678b",
      "12345678c", "12345678d"), -1);
  EXPECT_EQ(VarcharVarchar16ByteTest("1234567812345678a", "1234567812345678b",
      "1234567812345678c", "1234567812345678d"), 0);
  EXPECT_EQ(VarcharVarchar16ByteTest("aa", "bbbbbbbb", "aa", "bbbbbbba"), 1);
  EXPECT_EQ(VarcharVarchar16ByteTest("aa", "1234567812345678",
      "aa", "1234567812345679"), -1);
  EXPECT_EQ(VarcharVarchar16ByteTest("2", "h", "2", "h2"), -1);
  EXPECT_EQ(VarcharVarchar16ByteTest("", "h", "", ""), 1);
  EXPECT_EQ(VarcharVarchar16ByteTest("ab", "cd", "aw", "ca"), -1);
  EXPECT_EQ(VarcharVarchar16ByteTest("ab", "yd", "ac", "ca"), 1);
  EXPECT_EQ(VarcharVarchar16ByteTest("zz", "ydz", "a", "caaa"), 1);
}

} //namespace impala

