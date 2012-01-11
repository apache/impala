// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include <string>
#include <boost/unordered_map.hpp>
#include <gtest/gtest.h>
#include <boost/lexical_cast.hpp>
#include <boost/random/mersenne_twister.hpp>

#include "common/object-pool.h"
#include "runtime/raw-value.h"
#include "runtime/primitive-type.h"
#include "runtime/string-value.h"
#include "testutil/query-executor.h"
#include "gen-cpp/ImpalaService_types.h"
#include "gen-cpp/Exprs_types.h"
#include "exprs/bool-literal.h"
#include "exprs/float-literal.h"
#include "exprs/function-call.h"
#include "exprs/int-literal.h"
#include "exprs/is-null-predicate.h"
#include "exprs/like-predicate.h"
#include "exprs/literal-predicate.h"
#include "exprs/null-literal.h"
#include "exprs/string-literal.h"


using namespace std;
using namespace boost;

namespace impala {

class ExprTest : public testing::Test {
 protected:
  // Maps from enum value of primitive integer type to
  // the minimum value that is outside of the next smaller-resolution type.
  // For example the value for type TYPE_SMALLINT is numeric_limits<int8_t>::max()+1.
  unordered_map<int, int64_t> min_int_values_;
  // Maps from primitive float type to smallest positive value that is larger
  // than the largest value of the next smaller-resolution type.
  unordered_map<int, double> min_float_values_;

  virtual void SetUp() {
    EXIT_IF_ERROR(executor_.Setup());

    min_int_values_[TYPE_TINYINT] = 1;
    min_int_values_[TYPE_SMALLINT] = static_cast<int64_t>(numeric_limits<int8_t>::max()) + 1;
    min_int_values_[TYPE_INT] = static_cast<int64_t>(numeric_limits<int16_t>::max()) + 1;
    min_int_values_[TYPE_BIGINT] = static_cast<int64_t>(numeric_limits<int32_t>::max()) + 1;

    min_float_values_[TYPE_FLOAT] = 1.1;
    min_float_values_[TYPE_DOUBLE] = static_cast<double>(numeric_limits<float>::max()) + 1.1;
  }

  void GetValue(const string& expr, PrimitiveType expr_type, void** value) {
    string stmt = "select " + expr;
    vector<PrimitiveType> result_types;
    Status status = executor_.Exec(stmt, &result_types);
    ASSERT_TRUE(status.ok()) << "stmt: " << stmt << "\nerror: " << status.GetErrorMsg();
    vector<void*> result_row;
    ASSERT_TRUE(executor_.FetchResult(&result_row).ok());
    ASSERT_EQ(1, result_row.size());
    EXPECT_EQ(TypeToString(expr_type), TypeToString(result_types[0]));
    *value = result_row[0];
  }

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
        EXPECT_EQ(*reinterpret_cast<bool*>(result), expected_result) << expr;
        break;
      case TYPE_TINYINT:
        EXPECT_EQ(*reinterpret_cast<int8_t*>(result), expected_result) << expr;
        break;
      case TYPE_SMALLINT:
        EXPECT_EQ(*reinterpret_cast<int16_t*>(result), expected_result) << expr;
        break;
      case TYPE_INT:
        EXPECT_EQ(*reinterpret_cast<int32_t*>(result), expected_result) << expr;
        break;
      case TYPE_BIGINT:
        EXPECT_EQ(*reinterpret_cast<int64_t*>(result), expected_result) << expr;
        break;
      case TYPE_FLOAT:
        EXPECT_EQ(*reinterpret_cast<float*>(result), expected_result) << expr;
        break;
      case TYPE_DOUBLE:
        EXPECT_EQ(*reinterpret_cast<double*>(result), expected_result) << expr;
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
    TestLessThan(lexical_cast<string>(t_min), lexical_cast<string>(t_max), true);
    TestEqual(lexical_cast<string>(t_min), true);
    TestEqual(lexical_cast<string>(t_max), true);
    if (test_boundaries) {
      // this requires a cast of the second operand to a higher-resolution type
      TestLessThan(lexical_cast<string>(t_min - 1),
                   lexical_cast<string>(t_max), true);
      // this requires a cast of the first operand to a higher-resolution type
      TestLessThan(lexical_cast<string>(t_min),
                   lexical_cast<string>(t_max + 1), true);
    }
  }

  template <typename T> void TestFloatingPointComparisons(bool test_boundaries) {
    // t_min is the smallest positive value
    T t_min = numeric_limits<T>::min();
    T t_max = numeric_limits<T>::max();
    TestLessThan(lexical_cast<string>(t_min), lexical_cast<string>(t_max), true);
    TestLessThan(lexical_cast<string>(-1.0 * t_max), lexical_cast<string>(t_max), true);
    TestEqual(lexical_cast<string>(t_min), true);
    TestEqual(lexical_cast<string>(t_max), true);
    if (test_boundaries) {
      // this requires a cast of the second operand to a higher-resolution type
      TestLessThan(lexical_cast<string>(numeric_limits<T>::min() - 1),
                   lexical_cast<string>(numeric_limits<T>::max()), true);
      // this requires a cast of the first operand to a higher-resolution type
      TestLessThan(lexical_cast<string>(numeric_limits<T>::min()),
                   lexical_cast<string>(numeric_limits<T>::max() + 1), true);
    }
  }

  // Generate all possible tests for combinations of <smaller> <op> <larger>.
  // Also test conversions from strings.
  void TestLessThan(const string& smaller, const string& larger, bool compare_strings) {
    // disabled for now, because our implicit casts from strings are broken
    // and might return analysis errors when they shouldn't
    // TODO: fix and re-enable tests
    compare_strings = false;
    string eq_pred = smaller + " = " + larger;
    TestValue(eq_pred, TYPE_BOOLEAN, false);
    if (compare_strings) {
      eq_pred = smaller + " = '" + larger + "'";
      TestValue(eq_pred, TYPE_BOOLEAN, false);
    }
    string ne_pred = smaller + " != " + larger;
    TestValue(ne_pred, TYPE_BOOLEAN, true);
    if (compare_strings) {
      ne_pred = smaller + " != '" + larger + "'";
      TestValue(ne_pred, TYPE_BOOLEAN, true);
    }
    string ne2_pred = smaller + " <> " + larger;
    TestValue(ne2_pred, TYPE_BOOLEAN, true);
    if (compare_strings) {
      ne2_pred = smaller + " <> '" + larger + "'";
      TestValue(ne2_pred, TYPE_BOOLEAN, true);
    }
    string lt_pred = smaller + " < " + larger;
    TestValue(lt_pred, TYPE_BOOLEAN, true);
    if (compare_strings) {
      lt_pred = smaller + " < '" + larger + "'";
      TestValue(lt_pred, TYPE_BOOLEAN, true);
    }
    string le_pred = smaller + " <= " + larger;
    TestValue(le_pred, TYPE_BOOLEAN, true);
    if (compare_strings) {
      le_pred = smaller + " <= '" + larger + "'";
      TestValue(le_pred, TYPE_BOOLEAN, true);
    }
    string gt_pred = smaller + " > " + larger;
    TestValue(gt_pred, TYPE_BOOLEAN, false);
    if (compare_strings) {
      gt_pred = smaller + " > '" + larger + "'";
      TestValue(gt_pred, TYPE_BOOLEAN, false);
    }
    string ge_pred = smaller + " >= " + larger;
    TestValue(ge_pred, TYPE_BOOLEAN, false);
    if (compare_strings) {
      ge_pred = smaller + " >= '" + larger + "'";
      TestValue(ge_pred, TYPE_BOOLEAN, false);
    }
  }

  // Generate all possible tests for combinations of <value> <op> <value>
  // Also test conversions from strings.
  void TestEqual(const string& value, bool compare_strings) {
    // disabled for now, because our implicit casts from strings are broken
    // and might return analysis errors when they shouldn't
    // TODO: fix and re-enable tests
    compare_strings = false;
    string eq_pred = value + " = " + value;
    TestValue(eq_pred, TYPE_BOOLEAN, true);
    if (compare_strings) {
      eq_pred = value + " = '" + value + "'";
      TestValue(eq_pred, TYPE_BOOLEAN, true);
    }
    string ne_pred = value + " != " + value;
    TestValue(ne_pred, TYPE_BOOLEAN, false);
    if (compare_strings)  {
      ne_pred = value + " != '" + value + "'";
      TestValue(ne_pred, TYPE_BOOLEAN, false);
    }
    string ne2_pred = value + " <> " + value;
    TestValue(ne2_pred, TYPE_BOOLEAN, false);
    if (compare_strings)  {
      ne2_pred = value + " <> '" + value + "'";
      TestValue(ne2_pred, TYPE_BOOLEAN, false);
    }
    string lt_pred = value + " < " + value;
    TestValue(lt_pred, TYPE_BOOLEAN, false);
    if (compare_strings)  {
      lt_pred = value + " < '" + value + "'";
      TestValue(lt_pred, TYPE_BOOLEAN, false);
    }
    string le_pred = value + " <= " + value;
    TestValue(le_pred, TYPE_BOOLEAN, true);
    if (compare_strings)  {
      le_pred = value + " <= '" + value + "'";
      TestValue(le_pred, TYPE_BOOLEAN, true);
    }
    string gt_pred = value + " > " + value;
    TestValue(gt_pred, TYPE_BOOLEAN, false);
    if (compare_strings)  {
      gt_pred = value + " > '" + value + "'";
      TestValue(gt_pred, TYPE_BOOLEAN, false);
    }
    string ge_pred = value + " >= " + value;
    TestValue(ge_pred, TYPE_BOOLEAN, true);
    if (compare_strings)  {
      ge_pred = value + " >= '" + value + "'";
      TestValue(ge_pred, TYPE_BOOLEAN, true);
    }
  }

  template <typename T> void TestFixedPointLimits(PrimitiveType type) {
    // cast to non-char type first, otherwise we might end up interpreting
    // the min/max as an ascii value
    // We need to add 1 to t_min because there are no negative integer literals.
    // The reason is that whether a minus belongs to an
    // arithmetic expr or a literal must be decided by the parser, not the lexer.
    int64_t t_min = numeric_limits<T>::min() + 1;
    int64_t t_max = numeric_limits<T>::max();
    TestValue(lexical_cast<string>(t_min), type, numeric_limits<T>::min() + 1);
    TestValue(lexical_cast<string>(t_max), type, numeric_limits<T>::max());
  }

  template <typename T> void TestFloatingPointLimits(PrimitiveType type) {
    // numeric_limits<>::min() is the smallest positive value
    TestValue(lexical_cast<string>(numeric_limits<T>::min()), type,
              numeric_limits<T>::min());
    TestValue(lexical_cast<string>(-1.0 * numeric_limits<T>::min()), type,
              -1.0 * numeric_limits<T>::min());
    TestValue(lexical_cast<string>(-1.0 * numeric_limits<T>::max()), type,
              -1.0 * numeric_limits<T>::max());
    TestValue(lexical_cast<string>(numeric_limits<T>::max() - 1.0), type,
              numeric_limits<T>::max());
  }

  // Test ops that that always promote to a fixed type (e.g., max resolution type):
  // PLUS, MINUS, MULTIPLY, DIVIDE.
  // Note that adding the " " when generating the expression is not just cosmetic.
  // We have "--" as a comment element in our lexer,
  // so subtraction of a negative value will be ignored without " ".
  template <typename LeftOp, typename RightOp, typename Result>
  void TestFixedResultTypeOps(LeftOp a, RightOp b, PrimitiveType expected_type) {
    Result cast_a = static_cast<Result>(a);
    Result cast_b = static_cast<Result>(b);
    string a_str = lexical_cast<string>(cast_a);
    string b_str = lexical_cast<string>(cast_b);
    TestValue(a_str + " + " + b_str, expected_type, cast_a + cast_b);
    TestValue(a_str + " - " + b_str, expected_type, cast_a - cast_b);
    TestValue(a_str + " * " + b_str, expected_type, cast_a * cast_b);
    TestValue(a_str + " / " + b_str, TYPE_DOUBLE,
        static_cast<double>(a) / static_cast<double>(b));
  }

  // Test int ops that promote to assignment compatible type: BITAND, BITOR, BITXOR,
  // BITNOT, INT_DIVIDE, MOD.
  // As a convention we use RightOp should denote the higher resolution type.
  template <typename LeftOp, typename RightOp>
  void TestVariableResultTypeIntOps(LeftOp a, RightOp b, PrimitiveType expected_type) {
    RightOp cast_a = static_cast<RightOp>(a);
    RightOp cast_b = static_cast<RightOp>(b);
    string a_str = lexical_cast<string>(static_cast<int64_t>(a));
    string b_str = lexical_cast<string>(static_cast<int64_t>(b));
    TestValue(a_str + " & " + b_str, expected_type, cast_a & cast_b);
    TestValue(a_str + " | " + b_str, expected_type, cast_a | cast_b);
    TestValue(a_str + " ^ " + b_str, expected_type, cast_a ^ cast_b);
    // Exclusively use b of type RightOp for unary op BITNOT.
    TestValue("~" + b_str, expected_type, ~cast_b);
    TestValue(a_str + " DIV " + b_str, expected_type, cast_a / cast_b);
    TestValue(a_str + " % " + b_str, expected_type, cast_a % cast_b);
  }

 private:
  QueryExecutor executor_;
};

// TODO: Remove this specialization once the parser supports
// int literals with value numeric_limits<int64_t>::min().
// Currently the parser can handle negative int literals up to
// numeric_limits<int64_t>::min()+1.
template <>
void ExprTest::TestFixedPointComparisons<int64_t>(bool test_boundaries) {
  int64_t t_min = numeric_limits<int64_t>::min() + 1;
  int64_t t_max = numeric_limits<int64_t>::max();
  TestLessThan(lexical_cast<string>(t_min), lexical_cast<string>(t_max), true);
  TestEqual(lexical_cast<string>(t_min), true);
  TestEqual(lexical_cast<string>(t_max), true);
  if (test_boundaries) {
    // this requires a cast of the second operand to a higher-resolution type
    TestLessThan(lexical_cast<string>(t_min - 1),
                 lexical_cast<string>(t_max), true);
    // this requires a cast of the first operand to a higher-resolution type
    TestLessThan(lexical_cast<string>(t_min),
                 lexical_cast<string>(t_max + 1), true);
  }
}

void TestSingleLiteralConstruction(PrimitiveType type, void* value, const string& string_val) {
  ObjectPool pool;
  RowDescriptor desc;

  Expr* expr = Expr::CreateLiteral(&pool, type, value);
  EXPECT_TRUE(expr != NULL);
  expr->Prepare(NULL, desc);
  EXPECT_EQ(RawValue::Compare(expr->GetValue(NULL), value, type), 0);

  expr = Expr::CreateLiteral(&pool, type, string_val);
  EXPECT_TRUE(expr != NULL);
  expr->Prepare(NULL, desc);
  EXPECT_EQ(RawValue::Compare(expr->GetValue(NULL), value, type), 0);
}

TEST_F(ExprTest, LiteralConstruction) {
  bool b_val = true;
  char c_val = 'f';
  short s_val = 123;
  int i_val = 234;
  long l_val = 345;
  float f_val = 3.14f;
  double d_val = 1.23;
  string str_input = "Hello";
  StringValue str_val(const_cast<char*>(str_input.data()), str_input.length());

  TestSingleLiteralConstruction(TYPE_BOOLEAN, &b_val, "1");
  TestSingleLiteralConstruction(TYPE_TINYINT, &c_val, "f");
  TestSingleLiteralConstruction(TYPE_SMALLINT, &s_val, "123");
  TestSingleLiteralConstruction(TYPE_INT, &i_val, "234");
  TestSingleLiteralConstruction(TYPE_BIGINT, &l_val, "345");
  TestSingleLiteralConstruction(TYPE_FLOAT, &f_val, "3.14");
  TestSingleLiteralConstruction(TYPE_DOUBLE, &d_val, "1.23");
  TestSingleLiteralConstruction(TYPE_STRING, &str_val, "Hello");
}


TEST_F(ExprTest, LiteralExprs) {
  TestFixedPointLimits<int8_t>(TYPE_TINYINT);
  TestFixedPointLimits<int16_t>(TYPE_SMALLINT);
  TestFixedPointLimits<int32_t>(TYPE_INT);
  TestFixedPointLimits<int64_t>(TYPE_BIGINT);
  TestFloatingPointLimits<float>(TYPE_FLOAT);
  TestFloatingPointLimits<double>(TYPE_DOUBLE);

  TestValue("true", TYPE_BOOLEAN, true);
  TestValue("false", TYPE_BOOLEAN, false);
  TestStringValue("'test'", "test");
  TestIsNull("null", TYPE_BOOLEAN);
}

TEST_F(ExprTest, ArithmeticExprs) {
  // Test float ops.
  TestFixedResultTypeOps<float, float, double>(min_float_values_[TYPE_FLOAT],
      min_float_values_[TYPE_FLOAT], TYPE_DOUBLE);
  TestFixedResultTypeOps<float, double, double>(min_float_values_[TYPE_FLOAT],
      min_float_values_[TYPE_DOUBLE], TYPE_DOUBLE);
  TestFixedResultTypeOps<double, double, double>(min_float_values_[TYPE_DOUBLE],
      min_float_values_[TYPE_DOUBLE], TYPE_DOUBLE);

  // Test behavior of float ops at max/min value boundaries.
  // The tests with float type should trivially pass, since their results are double.
  TestFixedResultTypeOps<float, float, double>(numeric_limits<float>::min(),
      numeric_limits<float>::min(), TYPE_DOUBLE);
  TestFixedResultTypeOps<float, float, double>(numeric_limits<float>::max(),
      numeric_limits<float>::max(), TYPE_DOUBLE);
  TestFixedResultTypeOps<float, float, double>(numeric_limits<float>::min(),
      numeric_limits<float>::max(), TYPE_DOUBLE);
  TestFixedResultTypeOps<float, float, double>(numeric_limits<float>::max(),
      numeric_limits<float>::min(), TYPE_DOUBLE);
  TestFixedResultTypeOps<double, double, double>(numeric_limits<double>::min(),
      numeric_limits<double>::min(), TYPE_DOUBLE);
  TestFixedResultTypeOps<double, double, double>(numeric_limits<double>::max(),
      numeric_limits<double>::max(), TYPE_DOUBLE);
  TestFixedResultTypeOps<double, double, double>(numeric_limits<double>::min(),
      numeric_limits<double>::max(), TYPE_DOUBLE);
  TestFixedResultTypeOps<double, double, double>(numeric_limits<double>::max(),
      numeric_limits<double>::min(), TYPE_DOUBLE);

  // Test behavior with zero (especially for division by zero).
  TestFixedResultTypeOps<float, float, double>(min_float_values_[TYPE_FLOAT],
      0.0f, TYPE_DOUBLE);
  TestFixedResultTypeOps<double, double, double>(min_float_values_[TYPE_DOUBLE],
      0.0, TYPE_DOUBLE);

  // Test ops that always promote to fixed type (e.g., max resolution type).
  TestFixedResultTypeOps<int8_t, int8_t, int64_t>(min_int_values_[TYPE_TINYINT],
      min_int_values_[TYPE_TINYINT], TYPE_BIGINT);
  TestFixedResultTypeOps<int8_t, int16_t, int64_t>(min_int_values_[TYPE_TINYINT],
      min_int_values_[TYPE_SMALLINT], TYPE_BIGINT);
  TestFixedResultTypeOps<int8_t, int32_t, int64_t>(min_int_values_[TYPE_TINYINT],
      min_int_values_[TYPE_INT], TYPE_BIGINT);
  TestFixedResultTypeOps<int8_t, int64_t, int64_t>(min_int_values_[TYPE_TINYINT],
      min_int_values_[TYPE_BIGINT], TYPE_BIGINT);
  TestFixedResultTypeOps<int16_t, int16_t, int64_t>(min_int_values_[TYPE_SMALLINT],
      min_int_values_[TYPE_SMALLINT], TYPE_BIGINT);
  TestFixedResultTypeOps<int16_t, int32_t, int64_t>(min_int_values_[TYPE_SMALLINT],
      min_int_values_[TYPE_INT], TYPE_BIGINT);
  TestFixedResultTypeOps<int16_t, int64_t, int64_t>(min_int_values_[TYPE_SMALLINT],
      min_int_values_[TYPE_BIGINT], TYPE_BIGINT);
  TestFixedResultTypeOps<int32_t, int32_t, int64_t>(min_int_values_[TYPE_INT],
      min_int_values_[TYPE_INT], TYPE_BIGINT);
  TestFixedResultTypeOps<int32_t, int64_t, int64_t>(min_int_values_[TYPE_INT],
      min_int_values_[TYPE_BIGINT], TYPE_BIGINT);
  TestFixedResultTypeOps<int64_t, int64_t, int64_t>(min_int_values_[TYPE_BIGINT],
      min_int_values_[TYPE_BIGINT], TYPE_BIGINT);

  // Test behavior on overflow/underflow.
  TestFixedResultTypeOps<int64_t, int64_t, int64_t>(numeric_limits<int64_t>::min()+1,
      numeric_limits<int64_t>::min()+1, TYPE_BIGINT);
  TestFixedResultTypeOps<int64_t, int64_t, int64_t>(numeric_limits<int64_t>::max(),
      numeric_limits<int64_t>::max(), TYPE_BIGINT);
  TestFixedResultTypeOps<int64_t, int64_t, int64_t>(numeric_limits<int64_t>::min()+1,
      numeric_limits<int64_t>::max(), TYPE_BIGINT);
  TestFixedResultTypeOps<int64_t, int64_t, int64_t>(numeric_limits<int64_t>::max(),
      numeric_limits<int64_t>::min()+1, TYPE_BIGINT);

  // Test int ops that promote to assignment compatible type.
  TestVariableResultTypeIntOps<int8_t, int8_t>(min_int_values_[TYPE_TINYINT],
      min_int_values_[TYPE_TINYINT], TYPE_TINYINT);
  TestVariableResultTypeIntOps<int8_t, int16_t>(min_int_values_[TYPE_TINYINT],
      min_int_values_[TYPE_SMALLINT], TYPE_SMALLINT);
  TestVariableResultTypeIntOps<int8_t, int32_t>(min_int_values_[TYPE_TINYINT],
      min_int_values_[TYPE_INT], TYPE_INT);
  TestVariableResultTypeIntOps<int8_t, int64_t>(min_int_values_[TYPE_TINYINT],
      min_int_values_[TYPE_BIGINT], TYPE_BIGINT);
  TestVariableResultTypeIntOps<int16_t, int16_t>(min_int_values_[TYPE_SMALLINT],
      min_int_values_[TYPE_SMALLINT], TYPE_SMALLINT);
  TestVariableResultTypeIntOps<int16_t, int32_t>(min_int_values_[TYPE_SMALLINT],
      min_int_values_[TYPE_INT],TYPE_INT);
  TestVariableResultTypeIntOps<int16_t, int64_t>(min_int_values_[TYPE_SMALLINT],
      min_int_values_[TYPE_BIGINT], TYPE_BIGINT);
  TestVariableResultTypeIntOps<int32_t, int32_t>(min_int_values_[TYPE_INT],
      min_int_values_[TYPE_INT], TYPE_INT);
  TestVariableResultTypeIntOps<int32_t, int64_t>(min_int_values_[TYPE_INT],
      min_int_values_[TYPE_BIGINT], TYPE_BIGINT);
  TestVariableResultTypeIntOps<int64_t, int64_t>(min_int_values_[TYPE_BIGINT],
      min_int_values_[TYPE_BIGINT], TYPE_BIGINT);

  // Tests for dealing with '-'.
  TestValue("-1", TYPE_TINYINT, -1);
  TestValue("1 - 1", TYPE_BIGINT, 0);
  TestValue("1 - - 1", TYPE_BIGINT, 2);
  TestValue("1 - - - 1", TYPE_BIGINT, 0);
  TestValue("- 1 - 1", TYPE_BIGINT, -2);
  TestValue("- 1 - - 1", TYPE_BIGINT, 0);
  // The "--" indicates a comment to be ignored.
  // Therefore, the result should be -1.
  TestValue("- 1 --1", TYPE_TINYINT, -1);
}

// There are two tests of ranges, the second of which requires a cast
// of the second operand to a higher-resolution type.
TEST_F(ExprTest, BinaryPredicates) {
  TestLessThan("false", "true", false);
  TestEqual("false", false);
  TestEqual("true", false);
  TestFixedPointComparisons<int8_t>(true);
  TestFixedPointComparisons<int16_t>(true);
  TestFixedPointComparisons<int32_t>(true);
  TestFixedPointComparisons<int64_t>(false);
  TestFloatingPointComparisons<float>(true);
  TestFloatingPointComparisons<double>(false);
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
  TestValue("NOT TRUE", TYPE_BOOLEAN, false);
  TestValue("NOT FALSE", TYPE_BOOLEAN, true);
  TestValue("TRUE AND (TRUE OR FALSE)", TYPE_BOOLEAN, true);
  TestValue("(TRUE AND TRUE) OR FALSE", TYPE_BOOLEAN, true);
  TestValue("(TRUE OR FALSE) AND TRUE", TYPE_BOOLEAN, true);
  TestValue("TRUE OR (FALSE AND TRUE)", TYPE_BOOLEAN, true);
  TestValue("TRUE AND TRUE OR FALSE", TYPE_BOOLEAN, true);
  TestIsNull("TRUE AND NULL", TYPE_BOOLEAN);
  TestValue("FALSE AND NULL", TYPE_BOOLEAN, false);
  TestValue("TRUE OR NULL", TYPE_BOOLEAN, true);
  TestIsNull("FALSE OR NULL", TYPE_BOOLEAN);
  TestIsNull("NOT NULL", TYPE_BOOLEAN);
}

TEST_F(ExprTest, IsNullPredicate) {
  TestValue("5 IS NULL", TYPE_BOOLEAN, false);
  TestValue("5 IS NOT NULL", TYPE_BOOLEAN, true);
}

TEST_F(ExprTest, LikePredicate) {
  TestValue("'a' LIKE '%a%'", TYPE_BOOLEAN, true);
  TestValue("'a' LIKE '_'", TYPE_BOOLEAN, true);
  TestValue("'a' LIKE 'a'", TYPE_BOOLEAN, true);
  TestValue("'a' LIKE 'b'", TYPE_BOOLEAN, false);
  TestValue("'prefix1234' LIKE 'prefix%'", TYPE_BOOLEAN, true);
  TestValue("'1234suffix' LIKE '%suffix'", TYPE_BOOLEAN, true);
  TestValue("'1234substr5678' LIKE '%substr%'", TYPE_BOOLEAN, true);
  TestValue("'a%a' LIKE 'a\\%a'", TYPE_BOOLEAN, true);
  TestValue("'a123a' LIKE 'a\\%a'", TYPE_BOOLEAN, false);
  TestValue("'a_a' LIKE 'a\\_a'", TYPE_BOOLEAN, true);
  TestValue("'a1a' LIKE 'a\\_a'", TYPE_BOOLEAN, false);
  TestValue("'abla' LIKE 'a%a'", TYPE_BOOLEAN, true);
  TestValue("'ablb' LIKE 'a%a'", TYPE_BOOLEAN, false);
  TestValue("'abxcy1234a' LIKE 'a_x_y%a'", TYPE_BOOLEAN, true);
  TestValue("'axcy1234a' LIKE 'a_x_y%a'", TYPE_BOOLEAN, false);
  TestValue("'abxcy1234a' REGEXP 'a.x.y.*a'", TYPE_BOOLEAN, true);
  TestValue("'a.x.y.*a' REGEXP 'a\\.x\\.y\\.\\*a'", TYPE_BOOLEAN, true);
  TestValue("'abxcy1234a' REGEXP 'a\\.x\\.y\\.\\*a'", TYPE_BOOLEAN, false);
  TestValue("'abxcy1234a' RLIKE 'a.x.y.*a'", TYPE_BOOLEAN, true);
  TestValue("'axcy1234a' REGEXP 'a.x.y.*a'", TYPE_BOOLEAN, false);
  TestValue("'axcy1234a' RLIKE 'a.x.y.*a'", TYPE_BOOLEAN, false);
  // regex escape chars; insert special character in the middle to prevent
  // it from being matched as a substring
  TestValue("'.[]{}()x\\*+?|^$' LIKE '.[]{}()_\\\\*+?|^$'", TYPE_BOOLEAN, true);
  // escaped _ matches single _
  TestValue("'\\_' LIKE '\\_'", TYPE_BOOLEAN, false);
  TestValue("'_' LIKE '\\_'", TYPE_BOOLEAN, true);
  TestValue("'a' LIKE '\\_'", TYPE_BOOLEAN, false);
  // escaped escape char
  TestValue("'\\a' LIKE '\\\\_'", TYPE_BOOLEAN, true);
  TestValue("'_' LIKE '\\\\_'", TYPE_BOOLEAN, false);
  // make sure the 3rd \ counts toward the _
  TestValue("'\\_' LIKE '\\\\\\_'", TYPE_BOOLEAN, true);
  TestValue("'\\\\a' LIKE '\\\\\\_'", TYPE_BOOLEAN, false);
}

TEST_F(ExprTest, StringFunctions) {
  TestStringValue("substring('Hello', 1)", "Hello");
  TestStringValue("substring('Hello', -2)", "lo");
  TestStringValue("substring('Hello', 0)", "");
  TestStringValue("substring('Hello', -5)", "Hello");
  TestStringValue("substring('Hello', -6)", "");
  TestStringValue("substring('Hello', 100)", "");
  TestStringValue("substring('Hello', 1, 1)", "H");
  TestStringValue("substring('Hello', 2, 100)", "ello");
  TestStringValue("substring('Hello', -3, 2)", "ll");

  TestStringValue("lower('')", "");
  TestStringValue("lower('HELLO')", "hello");
  TestStringValue("lower('Hello')", "hello");
  TestStringValue("lower('hello!')", "hello!");
  TestStringValue("lcase('HELLO')", "hello");

  TestStringValue("upper('')", "");
  TestStringValue("upper('HELLO')", "HELLO");
  TestStringValue("upper('Hello')", "HELLO");
  TestStringValue("upper('hello!')", "HELLO!");
  TestStringValue("ucase('hello')", "HELLO");

  TestValue("length('')", TYPE_INT, 0);
  TestValue("length('a')", TYPE_INT, 1);
  TestValue("length('abcdefg')", TYPE_INT, 7);

  // TODO: tests with NULL arguments, currently we can't parse them
  // inside function calls.
  // e.g.   TestValue("length(NULL)", TYPE_INT, NULL);
}

TEST_F(ExprTest, MathFunctions) {
  TestValue("pi()", TYPE_DOUBLE, M_PI);
}
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
