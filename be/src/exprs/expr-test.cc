// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include <string>
#include <gtest/gtest.h>
#include <boost/lexical_cast.hpp>

#include "runtime/primitive-type.h"
#include "runtime/string-value.h"
#include "testutil/query-executor.h"
#include "gen-cpp/ImpalaService_types.h"

using namespace std;
using namespace boost;

namespace impala {

class ExprTest : public testing::Test {
 protected:
  virtual void SetUp() {
    EXIT_IF_ERROR(executor_.Setup());
  }

  void GetValue(const string& expr, PrimitiveType expr_type, void** value) {
    string stmt = "select " + expr;
    vector<PrimitiveType> result_types;
    Status status = executor_.Exec(stmt, &result_types);
    ASSERT_TRUE(status.ok()) << status.GetErrorString();
    vector<void*> result_row;
    ASSERT_TRUE(executor_.FetchResult(&result_row).ok());
    ASSERT_EQ(1, result_row.size());
    EXPECT_EQ(TypeToString(expr_type), TypeToString(result_types[0]));
    *value = result_row[0];
  }

#if 0
  void TestBoolValue(const string& expr, bool expected_result) {
    void* result = GetValue(expr, TYPE_BOOLEAN);
    EXPECT_EQ(*reinterpret_cast<bool*>(result), expected_result);
  }

  void TestTinyintValue(const string& expr, int8_t expected_result) {
    void* result = GetValue(expr, TYPE_TINYINT);
    EXPECT_EQ(*reinterpret_cast<int8_t*>(result), expected_result);
  }

  void TestSmallintValue(const string& expr, int16_t expected_result) {
    void* result = GetValue(expr, TYPE_SMALLINT);
    EXPECT_EQ(*reinterpret_cast<int16_t*>(result), expected_result);
  }

  void TestIntValue(const string& expr, int32_t expected_result) {
    void* result = GetValue(expr, TYPE_INT);
    EXPECT_EQ(*reinterpret_cast<int32_t*>(result), expected_result);
  }

  void TestBigintValue(const string& expr, int64_t expected_result) {
    void* result = GetValue(expr, TYPE_BIGINT);
    EXPECT_EQ(*reinterpret_cast<int64_t*>(result), expected_result);
  }

  void TestFloatValue(const string& expr, float expected_result) {
    void* result = GetValue(expr, TYPE_FLOAT);
    EXPECT_EQ(*reinterpret_cast<float*>(result), expected_result);
  }

  void TestDoubleValue(const string& expr, double expected_result) {
    void* result = GetValue(expr, TYPE_DOUBLE);
    EXPECT_EQ(*reinterpret_cast<double*>(result), expected_result);
  }
#endif

  void TestStringValue(const string& expr, const string& expected_result) {
    StringValue* result;
    GetValue(expr, TYPE_STRING, reinterpret_cast<void**>(&result));
    string tmp(result->ptr, result->len);
    EXPECT_EQ(tmp, expected_result);
  }

  template <class T> void TestValue(const string& expr, PrimitiveType expr_type,
                                    const T& expected_result) {
    void* result;
    GetValue(expr, expr_type, &result);
    switch (expr_type) {
      case TYPE_BOOLEAN:
        EXPECT_EQ(*reinterpret_cast<bool*>(result), expected_result);
        break;
      case TYPE_TINYINT:
        EXPECT_EQ(*reinterpret_cast<int8_t*>(result), expected_result);
        break;
      case TYPE_SMALLINT:
        EXPECT_EQ(*reinterpret_cast<int16_t*>(result), expected_result);
        break;
      case TYPE_INT:
        EXPECT_EQ(*reinterpret_cast<int32_t*>(result), expected_result);
        break;
      case TYPE_BIGINT:
        EXPECT_EQ(*reinterpret_cast<int64_t*>(result), expected_result);
        break;
      case TYPE_FLOAT:
        EXPECT_EQ(*reinterpret_cast<float*>(result), expected_result);
        break;
      case TYPE_DOUBLE:
        EXPECT_EQ(*reinterpret_cast<double*>(result), expected_result);
        break;
      default:
        ASSERT_TRUE(false) << "invalid TestValue() type: " << TypeToString(expr_type);
    }
  }

  void TestIsNull(const string& expr, PrimitiveType expr_type) {
    void* result;
    GetValue(expr, expr_type, &result);
    EXPECT_TRUE(result == NULL);
  }

  template <typename T> void TestFixedPointComparisons(bool test_boundaries) {
    int64_t t_min = numeric_limits<T>::min();
    int64_t t_max = numeric_limits<T>::max();
    TestLessThan(lexical_cast<string>(t_min), lexical_cast<string>(t_max));
    TestEqual(lexical_cast<string>(t_min));
    TestEqual(lexical_cast<string>(t_max));
    if (test_boundaries) {
      // this requires a cast of the second operand to a higher-resolution type
      TestLessThan(lexical_cast<string>(t_min - 1),
                   lexical_cast<string>(t_max));
      // this requires a cast of the first operand to a higher-resolution type
      TestLessThan(lexical_cast<string>(t_min),
                   lexical_cast<string>(t_max + 1));
    }
  }


  template <typename T> void TestNumericComparisons(bool test_boundaries) {
    T t_min = numeric_limits<T>::min();
    T t_max = numeric_limits<T>::max();
    TestLessThan(lexical_cast<string>(t_min), lexical_cast<string>(t_max));
    TestEqual(lexical_cast<string>(t_min));
    TestEqual(lexical_cast<string>(t_max));
    if (test_boundaries) {
      // this requires a cast of the second operand to a higher-resolution type
      TestLessThan(lexical_cast<string>(numeric_limits<T>::min() - 1),
                   lexical_cast<string>(numeric_limits<T>::max()));
      // this requires a cast of the first operand to a higher-resolution type
      TestLessThan(lexical_cast<string>(numeric_limits<T>::min()),
                   lexical_cast<string>(numeric_limits<T>::max() + 1));
    }
  }

  // Generate all possible tests for combinations of <smaller> <op> <larger>
  void TestLessThan(const string& smaller, const string& larger) {
    string eq_pred = smaller + " = " + larger;
    TestValue(eq_pred, TYPE_BOOLEAN, false);
    string ne_pred = smaller + " != " + larger;
    TestValue(ne_pred, TYPE_BOOLEAN, true);
    string ne2_pred = smaller + " <> " + larger;
    TestValue(ne2_pred, TYPE_BOOLEAN, true);
    string lt_pred = smaller + " < " + larger;
    TestValue(lt_pred, TYPE_BOOLEAN, true);
    string le_pred = smaller + " <= " + larger;
    TestValue(le_pred, TYPE_BOOLEAN, true);
    string gt_pred = smaller + " > " + larger;
    TestValue(gt_pred, TYPE_BOOLEAN, false);
    string ge_pred = smaller + " >= " + larger;
    TestValue(ge_pred, TYPE_BOOLEAN, false);
  }

  // Generate all possible tests for combinations of <value> <op> <value>
  void TestEqual(const string& value) {
    string eq_pred = value + " = " + value;
    TestValue(eq_pred, TYPE_BOOLEAN, true);
    string ne_pred = value + " != " + value;
    TestValue(ne_pred, TYPE_BOOLEAN, false);
    string ne2_pred = value + " <> " + value;
    TestValue(ne2_pred, TYPE_BOOLEAN, false);
    string lt_pred = value + " < " + value;
    TestValue(lt_pred, TYPE_BOOLEAN, false);
    string le_pred = value + " <= " + value;
    TestValue(le_pred, TYPE_BOOLEAN, true);
    string gt_pred = value + " > " + value;
    TestValue(gt_pred, TYPE_BOOLEAN, false);
    string ge_pred = value + " >= " + value;
    TestValue(ge_pred, TYPE_BOOLEAN, true);
  }

  template <typename T> void TestFixedPointLimits(PrimitiveType type) {
    // cast to non-char type first, otherwise we might end up interpreting
    // the min/max as an ascii value
    int64_t t_min = numeric_limits<T>::min();
    int64_t t_max = numeric_limits<T>::max();
    TestValue(lexical_cast<string>(t_min), type, numeric_limits<T>::min());
    TestValue(lexical_cast<string>(t_max), type, numeric_limits<T>::max());
  }

  template <typename T> void TestFloatingPointLimits(PrimitiveType type) {
    TestValue(lexical_cast<string>(numeric_limits<T>::min()), type,
              numeric_limits<T>::min());
    TestValue(lexical_cast<string>(numeric_limits<T>::max() - 1.0), type,
              numeric_limits<T>::max());
  }

#if 0
  template <typename T> void TestArithmeticOps(const T& op1, const T& op2) {
    string add_expr = lexical_cast<string>(op1) + " + " + lexical_cast<string>(op2);
    TestValue(add_expr, op1 + op2);
  }
#endif


 private:
  QueryExecutor executor_;
};

TEST_F(ExprTest, LiteralExprs) {
  TestFixedPointLimits<int8_t>(TYPE_TINYINT);
  TestFixedPointLimits<int16_t>(TYPE_SMALLINT);
  TestFixedPointLimits<int32_t>(TYPE_INT);
  TestFixedPointLimits<int64_t>(TYPE_BIGINT);
  TestFloatingPointLimits<float>(TYPE_FLOAT);
  TestFloatingPointLimits<double>(TYPE_DOUBLE);

  // bool literals currently aren't supported
  //TestValue("true", TYPE_BOOLEAN, true);
  //TestValue("false", TYPE_BOOLEAN, false);
  TestStringValue("'test'", "test");
  // TODO: NULLs?
}

TEST_F(ExprTest, ArithmeticExprs) {
  TestValue("1 + 1", TYPE_BIGINT, 2);
  //TestValue("1 - 1", TYPE_TINYINT, 0);
}

#if 0
// There are two tests of ranges, the second of which requires a cast
// of the second operand to a higher-resolution type.
TEST_F(ExprTest, BinaryPredicates) {
  // bool
  TestLessThan("false", "true");
  TestEqual("false");
  TestEqual("true");
  TestFixedPointComparisons<int8_t>(true);
  TestFixedPointComparisons<int16_t>(true);
  TestFixedPointComparisons<int32_t>(true);
  TestFixedPointComparisons<int64_t>(false);
  TestNumericComparisons<float>(true);
  TestNumericComparisons<double>(false);
}

TEST_F(ExprTest, CompoundPredicates) {
  TestValue("TRUE AND TRUE", TYPE_BOOLEAN, true);
  TestValue("TRUE AND FALSE", TYPE_BOOLEAN, false);
  TestValue("FALSE AND TRUE", TYPE_BOOLEAN, false);
  TestValue("FALSE AND FALSE", TYPE_BOOLEAN, false);
  TestValue("TRUE OR TRUE", TYPE_BOOLEAN, true);
  TestValue("TRUE OR FALSE", TYPE_BOOLEAN, true);
  TestValue("FALSE OR TRUE", TYPE_BOOLEAN, true);
  TestValue("FALSE OR FALSE", TYPE_BOOLEAN, false);
  TestIsNull("TRUE AND NULL", TYPE_BOOLEAN);
  TestValue("FALSE AND NULL", TYPE_BOOLEAN, false);
  TestValue("TRUE OR NULL", TYPE_BOOLEAN, true);
  TestIsNull("FALSE OR NULL", TYPE_BOOLEAN);
  TestValue("NOT TRUE", TYPE_BOOLEAN, false);
  TestValue("NOT FALSE", TYPE_BOOLEAN, true);
  TestIsNull("NOT NULL", TYPE_BOOLEAN);
  TestValue("TRUE AND (TRUE OR FALSE)", TYPE_BOOLEAN, true);
  TestValue("(TRUE AND TRUE) OR FALSE", TYPE_BOOLEAN, true);
  TestValue("(TRUE OR FALSE) AND TRUE", TYPE_BOOLEAN, true);
  TestValue("TRUE OR (FALSE AND TRUE)", TYPE_BOOLEAN, false);
  TestValue("TRUE AND TRUE OR FALSE", TYPE_BOOLEAN, false);
}
#endif

}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
