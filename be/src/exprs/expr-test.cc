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

#include <math.h>
#include <time.h>
#include <limits>
#include <map>
#include <string>

#include <boost/date_time/c_local_time_adjustor.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/regex.hpp>
#include <boost/unordered_map.hpp>

#include "testutil/gtest-util.h"
#include "codegen/llvm-codegen.h"
#include "common/init.h"
#include "common/object-pool.h"
#include "exprs/is-null-predicate.h"
#include "exprs/like-predicate.h"
#include "exprs/literal.h"
#include "exprs/null-literal.h"
#include "exprs/scalar-expr.h"
#include "exprs/scalar-expr-evaluator.h"
#include "exprs/string-functions.h"
#include "exprs/timestamp-functions.h"
#include "exprs/timezone_db.h"
#include "gen-cpp/Exprs_types.h"
#include "gen-cpp/hive_metastore_types.h"
#include "rpc/thrift-client.h"
#include "rpc/thrift-server.h"
#include "runtime/runtime-state.h"
#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"
#include "runtime/raw-value.inline.h"
#include "runtime/string-value.h"
#include "runtime/timestamp-parse-util.h"
#include "runtime/timestamp-value.h"
#include "runtime/timestamp-value.inline.h"
#include "service/fe-support.h"
#include "service/impala-server.h"
#include "testutil/impalad-query-executor.h"
#include "testutil/in-process-servers.h"
#include "udf/udf-test-harness.h"
#include "util/debug-util.h"
#include "util/string-parser.h"
#include "util/test-info.h"
#include "gen-cpp/ImpalaInternalService_types.h"

#include "common/names.h"

DECLARE_bool(abort_on_config_error);
DECLARE_bool(disable_optimization_passes);
DECLARE_bool(use_utc_for_unix_timestamp_conversions);

namespace posix_time = boost::posix_time;
using boost::bad_lexical_cast;
using boost::date_time::c_local_adjustor;
using boost::posix_time::from_time_t;
using boost::posix_time::ptime;
using boost::posix_time::to_tm;
using std::numeric_limits;
using namespace Apache::Hadoop::Hive;
using namespace impala;
using namespace llvm;

namespace impala {
ImpaladQueryExecutor* executor_;
bool disable_codegen_;
bool enable_expr_rewrites_;

template <typename ORIGINAL_TYPE, typename VAL_TYPE>
string LiteralToString(VAL_TYPE val) {
  return lexical_cast<string>(val);
}

template<>
string LiteralToString<float, float>(float val) {
  stringstream ss;
  ss << "cast("
     << lexical_cast<string>(val)
     << " as float)";
  return ss.str();
}

template<>
string LiteralToString<float, double>(double val) {
  stringstream ss;
  ss << "cast("
     << lexical_cast<string>(val)
     << " as float)";
  return ss.str();
}

template<>
string LiteralToString<double, double>(double val) {
  stringstream ss;
  ss << "cast("
     << lexical_cast<string>(val)
     << " as double)";
  return ss.str();
}

// For writing C++ std::strings as impala literals, we have to ensure that characters like
// single-quote are escaped properly. To do this, we escape every character into its octal
// equivalent: \PQR for some octal digits P, Q, and R. Currently, this only works for
// ASCII literals.
string StringToOctalLiteral(const string& s) {
  string result(4 * s.size(), 0);
  for (int i = 0; i < s.size(); ++i) {
    result[4 * i] = '\\';
    result[4 * i + 1] = '0' + (s[i] / 64);
    result[4 * i + 2] = '0' + ((s[i] / 8) % 8);
    result[4 * i + 3] = '0' + (s[i] % 8);
  }
  return result;
}

// Override the time zone for the duration of the scope. The time zone is overridden
// using an environment variable there is no risk of making a permanent system change
// and no special permissions are needed. This is not thread-safe.
class ScopedTimeZoneOverride {
 public:
  ScopedTimeZoneOverride(string time_zone) {
    original_time_zone_ = getenv("TZ");
    setenv("TZ", time_zone.c_str(), /*overwrite*/ true);
    tzset();
  }

  ~ScopedTimeZoneOverride() {
    if (original_time_zone_ == NULL) {
      unsetenv("TZ");
    } else {
      setenv("TZ", original_time_zone_, /*overwrite*/ true);
    }
    tzset();
  }

 private:
  char* original_time_zone_;
};

// Enable FLAGS_use_local_tz_for_unix_timestamp_conversions for the duration of the scope.
class ScopedLocalUnixTimestampConversionOverride {
 public:
  ScopedLocalUnixTimestampConversionOverride() {
    FLAGS_use_local_tz_for_unix_timestamp_conversions = true;
  }

  ~ScopedLocalUnixTimestampConversionOverride() {
    FLAGS_use_local_tz_for_unix_timestamp_conversions = false;
  }
};

class ExprTest : public testing::Test {
 protected:
  // Maps from enum value of primitive integer type to the minimum value that is
  // outside of the next smaller-resolution type. For example the value for type
  // TYPE_SMALLINT is numeric_limits<int8_t>::max()+1. There is a GREATEST test in
  // the MathFunctions tests that requires this to be an ordered map.
  typedef map<int, int64_t> IntValMap;
  IntValMap min_int_values_;

  // Maps from primitive float type to smallest positive value that is larger
  // than the largest value of the next smaller-resolution type.
  typedef unordered_map<int, double> FloatValMap;
  FloatValMap min_float_values_;

  // Maps from enum value of primitive type to a string representation of a default
  // value for testing. For int and float types the strings represent the corresponding
  // min values (in the maps above). For non-numeric types the default values are listed
  // below.
  unordered_map<int, string> default_type_strs_;
  string default_bool_str_;
  string default_string_str_;
  string default_timestamp_str_;
  string default_decimal_str_;
  // Corresponding default values.
  bool default_bool_val_;
  string default_string_val_;
  TimestampValue default_timestamp_val_;

  virtual void SetUp() {
    min_int_values_[TYPE_TINYINT] = 1;
    min_int_values_[TYPE_SMALLINT] =
        static_cast<int64_t>(numeric_limits<int8_t>::max()) + 1;
    min_int_values_[TYPE_INT] = static_cast<int64_t>(numeric_limits<int16_t>::max()) + 1;
    min_int_values_[TYPE_BIGINT] =
        static_cast<int64_t>(numeric_limits<int32_t>::max()) + 1;

    min_float_values_[TYPE_FLOAT] = 1.1;
    min_float_values_[TYPE_DOUBLE] =
        static_cast<double>(numeric_limits<float>::max()) + 1.1;

    // Set up default test types, values, and strings.
    default_bool_str_ = "false";
    default_string_str_ = "'abc'";
    default_timestamp_str_ = "cast('2011-01-01 09:01:01' as timestamp)";
    default_decimal_str_ = "1.23";
    default_bool_val_ = false;
    default_string_val_ = "abc";
    default_timestamp_val_ = TimestampValue::FromUnixTime(1293872461);
    default_type_strs_[TYPE_TINYINT] =
        lexical_cast<string>(min_int_values_[TYPE_TINYINT]);
    default_type_strs_[TYPE_SMALLINT] =
        lexical_cast<string>(min_int_values_[TYPE_SMALLINT]);
    default_type_strs_[TYPE_INT] =
        lexical_cast<string>(min_int_values_[TYPE_INT]);
    default_type_strs_[TYPE_BIGINT] =
        lexical_cast<string>(min_int_values_[TYPE_BIGINT]);
    // Don't use lexical cast here because it results
    // in a string 1.1000000000000001 that messes up the tests.
    stringstream ss;
    ss << "cast("
       << lexical_cast<string>(min_float_values_[TYPE_FLOAT]) << " as float)";
    default_type_strs_[TYPE_FLOAT] = ss.str();
    ss.str("");
    ss << "cast("
       << lexical_cast<string>(min_float_values_[TYPE_FLOAT]) << " as double)";
    default_type_strs_[TYPE_DOUBLE] = ss.str();
    default_type_strs_[TYPE_BOOLEAN] = default_bool_str_;
    default_type_strs_[TYPE_STRING] = default_string_str_;
    default_type_strs_[TYPE_TIMESTAMP] = default_timestamp_str_;
    default_type_strs_[TYPE_DECIMAL] = default_decimal_str_;
  }

  string GetValue(const string& expr, const ColumnType& expr_type,
      bool expect_error = false) {
    string stmt = "select " + expr;
    vector<FieldSchema> result_types;
    Status status = executor_->Exec(stmt, &result_types);
    if (!status.ok()) {
      EXPECT_TRUE(expect_error) << "stmt: " << stmt << "\nerror: " << status.GetDetail();
      return "";
    }
    string result_row;
    status = executor_->FetchResult(&result_row);
    if (expect_error) {
      EXPECT_FALSE(status.ok()) << "Expected error\nstmt: " << stmt;
      return "";
    }
    EXPECT_TRUE(status.ok()) << "stmt: " << stmt << "\nerror: " << status.GetDetail();
    EXPECT_EQ(TypeToOdbcString(expr_type.type), result_types[0].type) << expr;
    return result_row;
  }

  template <typename T>
  T ConvertValue(const string& value);

  void TestStringValue(const string& expr, const string& expected_result) {
    EXPECT_EQ(expected_result, GetValue(expr, TYPE_STRING)) << expr;
  }

  void TestCharValue(const string& expr, const string& expected_result,
                     const ColumnType& type) {
    EXPECT_EQ(expected_result, GetValue(expr, type)) << expr;
  }

  string TestStringValueRegex(const string& expr, const string& regex) {
    const string results = GetValue(expr, TYPE_STRING);
    static const boost::regex e(regex);
    const bool is_regex_match = regex_match(results, e);
    EXPECT_TRUE(is_regex_match);
    return results;
  }

  void TestLastDayFunction() {
    // Test common months (with and without time component).
    TestTimestampValue("last_day('2003-01-02 04:24:04.1579')",
      TimestampValue::Parse("2003-01-31 00:00:00", 19));
    TestTimestampValue("last_day('2003-02-02')",
      TimestampValue::Parse("2003-02-28 00:00:00"));
    TestTimestampValue("last_day('2003-03-02 03:21:12.0058')",
      TimestampValue::Parse("2003-03-31 00:00:00"));
    TestTimestampValue("last_day('2003-04-02')",
      TimestampValue::Parse("2003-04-30 00:00:00"));
    TestTimestampValue("last_day('2003-05-02')",
      TimestampValue::Parse("2003-05-31 00:00:00"));
    TestTimestampValue("last_day('2003-06-02')",
      TimestampValue::Parse("2003-06-30 00:00:00"));
    TestTimestampValue("last_day('2003-07-02 00:01:01.125')",
      TimestampValue::Parse("2003-07-31 00:00:00"));
    TestTimestampValue("last_day('2003-08-02')",
      TimestampValue::Parse("2003-08-31 00:00:00"));
    TestTimestampValue("last_day('2003-09-02')",
      TimestampValue::Parse("2003-09-30 00:00:00"));
    TestTimestampValue("last_day('2003-10-02')",
      TimestampValue::Parse("2003-10-31 00:00:00"));
    TestTimestampValue("last_day('2003-11-02 12:30:16')",
      TimestampValue::Parse("2003-11-30 00:00:00"));
    TestTimestampValue("last_day('2003-12-02')",
      TimestampValue::Parse("2003-12-31 00:00:00"));

    // Test leap years and special cases.
    TestTimestampValue("last_day('2004-02-13')",
      TimestampValue::Parse("2004-02-29 00:00:00"));
    TestTimestampValue("last_day('2008-02-13')",
      TimestampValue::Parse("2008-02-29 00:00:00"));
    TestTimestampValue("last_day('2000-02-13')",
      TimestampValue::Parse("2000-02-29 00:00:00"));
    TestTimestampValue("last_day('1900-02-13')",
      TimestampValue::Parse("1900-02-28 00:00:00"));
    TestTimestampValue("last_day('2100-02-13')",
      TimestampValue::Parse("2100-02-28 00:00:00"));

    // Test corner cases.
    TestTimestampValue("last_day('1400-01-01 00:00:00')",
      TimestampValue::Parse("1400-01-31 00:00:00"));
    TestTimestampValue("last_day('9999-12-31 23:59:59')",
      TimestampValue::Parse("9999-12-31 00:00:00"));

    // Test invalid input.
    TestIsNull("last_day('12202010')", TYPE_TIMESTAMP);
    TestIsNull("last_day('')", TYPE_TIMESTAMP);
    TestIsNull("last_day(NULL)", TYPE_TIMESTAMP);
    TestIsNull("last_day('02-13-2014')", TYPE_TIMESTAMP);
    TestIsNull("last_day('00:00:00')", TYPE_TIMESTAMP);
  }

  void TestNextDayFunction() {
    // Sequential test cases
    TestTimestampValue("next_day('2016-05-01','Sunday')",
      TimestampValue::Parse("2016-05-08 00:00:00", 19));
    TestTimestampValue("next_day('2016-05-01','Monday')",
      TimestampValue::Parse("2016-05-02 00:00:00", 19));
    TestTimestampValue("next_day('2016-05-01','Tuesday')",
      TimestampValue::Parse("2016-05-03 00:00:00", 19));
    TestTimestampValue("next_day('2016-05-01','Wednesday')",
      TimestampValue::Parse("2016-05-04 00:00:00", 19));
    TestTimestampValue("next_day('2016-05-01','Thursday')",
      TimestampValue::Parse("2016-05-05 00:00:00", 19));
    TestTimestampValue("next_day('2016-05-01','Friday')",
      TimestampValue::Parse("2016-05-06 00:00:00", 19));
    TestTimestampValue("next_day('2016-05-01','Saturday')",
      TimestampValue::Parse("2016-05-07 00:00:00", 19));

    // Random test cases
    TestTimestampValue("next_day('1910-01-18','SunDay')",
      TimestampValue::Parse("1910-01-23 00:00:00", 19));
    TestTimestampValue("next_day('1916-06-05', 'SUN')",
      TimestampValue::Parse("1916-06-11 00:00:00", 19));
    TestTimestampValue("next_day('1932-11-08','monday')",
      TimestampValue::Parse("1932-11-14 00:00:00", 19));
    TestTimestampValue("next_day('1933-09-11','Mon')",
      TimestampValue::Parse("1933-09-18 00:00:00", 19));
    TestTimestampValue("next_day('1934-03-21','TUeSday')",
      TimestampValue::Parse("1934-03-27 00:00:00", 19));
    TestTimestampValue("next_day('1954-02-25','tuE')",
      TimestampValue::Parse("1954-03-02 00:00:00", 19));
    TestTimestampValue("next_day('1965-04-18','WeDneSdaY')",
      TimestampValue::Parse("1965-04-21 00:00:00", 19));
    TestTimestampValue("next_day('1966-08-29','wed')",
      TimestampValue::Parse("1966-08-31 00:00:00", 19));
    TestTimestampValue("next_day('1968-07-23','tHurSday')",
      TimestampValue::Parse("1968-07-25 00:00:00", 19));
    TestTimestampValue("next_day('1969-05-28','thu')",
      TimestampValue::Parse("1969-05-29 00:00:00", 19));
    TestTimestampValue("next_day('1989-10-12','fRIDay')",
      TimestampValue::Parse("1989-10-13 00:00:00", 19));
    TestTimestampValue("next_day('1973-10-02','frI')",
      TimestampValue::Parse("1973-10-05 00:00:00", 19));
    TestTimestampValue("next_day('2000-02-29','saTUrDaY')",
      TimestampValue::Parse("2000-03-04 00:00:00", 19));
    TestTimestampValue("next_day('2013-04-12','sat')",
      TimestampValue::Parse("2013-04-13 00:00:00", 19));
    TestTimestampValue("next_day('2013-12-25','Saturday')",
      TimestampValue::Parse("2013-12-28 00:00:00", 19));

    // Explicit timestamp conversion tests
    TestTimestampValue("next_day(to_timestamp('12-27-2008', 'MM-dd-yyyy'), 'moN')",
      TimestampValue::Parse("2008-12-29 00:00:00", 19));
    TestTimestampValue("next_day(to_timestamp('2007-20-10 11:22', 'yyyy-dd-MM HH:mm'),\
      'TUeSdaY')", TimestampValue::Parse("2007-10-23 11:22:00", 19));
    TestTimestampValue("next_day(to_timestamp('18-11-2070 09:12', 'dd-MM-yyyy HH:mm'),\
      'WeDneSdaY')", TimestampValue::Parse("2070-11-19 09:12:00", 19));
    TestTimestampValue("next_day(to_timestamp('12-1900-05', 'dd-yyyy-MM'), 'tHurSday')",
      TimestampValue::Parse("1900-05-17 00:00:00", 19));
    TestTimestampValue("next_day(to_timestamp('08-1987-21', 'MM-yyyy-dd'), 'FRIDAY')",
      TimestampValue::Parse("1987-08-28 00:00:00", 19));
    TestTimestampValue("next_day(to_timestamp('02-04-2001', 'dd-MM-yyyy'), 'SAT')",
      TimestampValue::Parse("2001-04-07 00:00:00", 19));
    TestTimestampValue("next_day(to_timestamp('1970-01-31 00:00:00',\
      'yyyy-MM-dd HH:mm:ss'), 'SunDay')", TimestampValue::Parse("1970-02-01 00:00:00", 19));

    // Invalid input: unacceptable date parameter
    TestIsNull("next_day('12202010','Saturday')", TYPE_TIMESTAMP);
    TestIsNull("next_day('2011 02 11','thu')", TYPE_TIMESTAMP);
    TestIsNull("next_day('09-19-2012xyz','monDay')", TYPE_TIMESTAMP);
    TestIsNull("next_day('000000000000000','wed')", TYPE_TIMESTAMP);
    TestIsNull("next_day('hell world!','fRiDaY')", TYPE_TIMESTAMP);
    TestIsNull("next_day('t1c7t0c9','sunDAY')", TYPE_TIMESTAMP);
  }

// This macro adds a scoped trace to provide the line number of the caller upon failure.
#define EXPECT_BETWEEN(start, value, end) { \
    SCOPED_TRACE(""); \
    ExpectBetween(start, value, end); \
  }

  template <typename T>
  void ExpectBetween(T start, T value, T end) {
    EXPECT_LE(start, value);
    EXPECT_LE(value, end);
  }

  // Test conversions of Timestamps to and from string/int with values related to the
  // Unix epoch. The caller should set the current time zone before calling.
  // 'unix_time_at_local_epoch' should be the expected value of the Unix time when it
  // was 1970-01-01 in the current time zone. 'local_time_at_unix_epoch' should be the
  // local time at the Unix epoch (1970-01-01 UTC).
  void TestTimestampUnixEpochConversions(int64_t unix_time_at_local_epoch,
      string local_time_at_unix_epoch) {
    TestValue("unix_timestamp(cast('" + local_time_at_unix_epoch + "' as timestamp))",
        TYPE_BIGINT, 0);
    TestValue("unix_timestamp('" + local_time_at_unix_epoch + "')", TYPE_BIGINT, 0);
    TestValue("unix_timestamp('" + local_time_at_unix_epoch +
        "', 'yyyy-MM-dd HH:mm:ss')", TYPE_BIGINT, 0);
    TestValue("unix_timestamp('1970-01-01', 'yyyy-MM-dd')", TYPE_BIGINT,
        unix_time_at_local_epoch);
    TestValue("unix_timestamp('1970-01-01 10:10:10', 'yyyy-MM-dd')", TYPE_BIGINT,
        unix_time_at_local_epoch);
    TestValue("unix_timestamp('" + local_time_at_unix_epoch
        + " extra text', 'yyyy-MM-dd HH:mm:ss')", TYPE_BIGINT, 0);
    TestStringValue("cast(cast(0 as timestamp) as string)", local_time_at_unix_epoch);
    TestStringValue("cast(cast(0 as timestamp) as string)", local_time_at_unix_epoch);
    TestStringValue("from_unixtime(0)", local_time_at_unix_epoch);
    TestStringValue("from_unixtime(cast(0 as bigint))", local_time_at_unix_epoch);
    TestIsNull("from_unixtime(NULL)", TYPE_STRING);
    TestStringValue("from_unixtime(0, 'yyyy-MM-dd HH:mm:ss')",
        local_time_at_unix_epoch);
    TestStringValue("from_unixtime(cast(0 as bigint), 'yyyy-MM-dd HH:mm:ss')",
        local_time_at_unix_epoch);
    TestStringValue("from_unixtime(" + lexical_cast<string>(unix_time_at_local_epoch)
        + ", 'yyyy-MM-dd')", "1970-01-01");
    TestStringValue("from_unixtime(cast(" + lexical_cast<string>(unix_time_at_local_epoch)
        + " as bigint), 'yyyy-MM-dd')", "1970-01-01");
    TestIsNull("to_timestamp(NULL)", TYPE_TIMESTAMP);
    TestIsNull("to_timestamp(NULL, 'yyyy-MM-dd')", TYPE_TIMESTAMP);
    TestIsNull("from_timestamp(NULL, 'yyyy-MM-dd')", TYPE_STRING);
    TestStringValue("cast(to_timestamp(" + lexical_cast<string>(unix_time_at_local_epoch)
        + ") as string)", "1970-01-01 00:00:00");
    TestStringValue("cast(to_timestamp('1970-01-01 00:00:00', 'yyyy-MM-dd HH:mm:ss') \
        as string)", "1970-01-01 00:00:00");
    TestStringValue("from_timestamp('1970-01-01 00:00:00', 'yyyy-MM-dd HH:mm:ss')",
        "1970-01-01 00:00:00");
  }

  // Verify that output of 'query' has the same precision and scale as 'expected_type'.
  // 'query' is an expression, optionally followed by a from clause which is needed
  // for testing aggregate expressions.
  void TestDecimalResultType(const string& query, const ColumnType& expected_type) {
    // For the case with from clause, we need to generate the "typeof query" by first
    // extracting the select list.
    size_t from_offset = query.find("from");
    string typeof_query;
    if (from_offset != string::npos) {
      int query_len = query.length();
      typeof_query = "typeof(" + query.substr(0, from_offset) + ")" +
          query.substr(from_offset, query_len - from_offset);
    } else {
      typeof_query = "typeof(" + query + ")";
    }
    const string typeof_result = GetValue(typeof_query, TYPE_STRING);
    EXPECT_EQ(expected_type.DebugString(), typeof_result) << typeof_query;
  }

  // Decimals don't work with TestValue.
  // TODO: figure out what operators need to be implemented to work with EXPECT_EQ
  template<typename T>
  void TestDecimalValue(const string& query, const T& expected_result,
      const ColumnType& expected_type) {
    // Verify precision and scale of the expression match the expected type.
    TestDecimalResultType(query, expected_type);
    // Verify the expression result matches the expected result, for the given the
    // precision and scale.
    const string value = GetValue(query, expected_type);
    StringParser::ParseResult result;
    // These require that we've passed the correct type to StringToDecimal(), so these
    // results are valid only when TestDecimalResultType() succeeded.
    switch (expected_type.GetByteSize()) {
      case 4:
        EXPECT_EQ(expected_result.value(), StringParser::StringToDecimal<int32_t>(
            value.data(), value.size(), expected_type, &result).value()) << query;
        break;
      case 8:
        EXPECT_EQ(expected_result.value(), StringParser::StringToDecimal<int64_t>(
            value.data(), value.size(), expected_type, &result).value()) << query;
        break;
      case 16:
        EXPECT_EQ(expected_result.value(), StringParser::StringToDecimal<int128_t>(
            value.data(), value.size(), expected_type, &result).value()) << query;
        break;
      default:
        EXPECT_TRUE(false) << expected_type << " " << expected_type.GetByteSize();
    }
    EXPECT_EQ(result, StringParser::PARSE_SUCCESS);
  }

  template <class T> void TestValue(const string& expr, const ColumnType& expr_type,
                                    const T& expected_result) {
    const string result = GetValue(expr, expr_type);

    switch (expr_type.type) {
      case TYPE_BOOLEAN:
        EXPECT_EQ(expected_result, ConvertValue<bool>(result)) << expr;
        break;
      case TYPE_TINYINT:
        EXPECT_EQ(expected_result, ConvertValue<int8_t>(result)) << expr;
        break;
      case TYPE_SMALLINT:
        EXPECT_EQ(expected_result, ConvertValue<int16_t>(result)) << expr;
        break;
      case TYPE_INT:
        EXPECT_EQ(expected_result, ConvertValue<int32_t>(result)) << expr;
        break;
      case TYPE_BIGINT:
        EXPECT_EQ(expected_result, ConvertValue<int64_t>(result)) << expr;
        break;
      case TYPE_FLOAT: {
        // Converting the float back from a string is inaccurate so convert
        // the expected result to a string.
        // In case the expected_result was passed in as an int or double, convert it.
        string expected_str;
        float expected_float;
        expected_float = static_cast<float>(expected_result);
        RawValue::PrintValue(reinterpret_cast<const void*>(&expected_float),
                             TYPE_FLOAT, -1, &expected_str);
        EXPECT_EQ(expected_str, result) << expr;
        break;
      }
      case TYPE_DOUBLE: {
        string expected_str;
        double expected_double;
        expected_double = static_cast<double>(expected_result);
        RawValue::PrintValue(reinterpret_cast<const void*>(&expected_double),
                             TYPE_DOUBLE, -1, &expected_str);
        EXPECT_EQ(expected_str, result) << expr;
        break;
      }
      default:
        ASSERT_TRUE(false) << "invalid TestValue() type: " << expr_type;
    }
  }

  void TestIsNull(const string& expr, const ColumnType& expr_type) {
    EXPECT_TRUE(GetValue(expr, expr_type) == "NULL") << expr;
  }

  void TestIsNotNull(const string& expr, const ColumnType& expr_type) {
    EXPECT_TRUE(GetValue(expr, expr_type) != "NULL") << expr;
  }

  void TestError(const string& expr) {
    GetValue(expr, INVALID_TYPE, /* expect_error */ true);
  }

  void TestNonOkStatus(const string& expr) {
    string stmt = "select " + expr;
    vector<FieldSchema> result_types;
    Status status = executor_->Exec(stmt, &result_types);
    ASSERT_FALSE(status.ok()) << "stmt: " << stmt << "\nunexpected Status::OK.";
  }

  template <typename T> void TestFixedPointComparisons(bool test_boundaries) {
    int64_t t_min = numeric_limits<T>::min();
    int64_t t_max = numeric_limits<T>::max();
    TestComparison(lexical_cast<string>(t_min), lexical_cast<string>(t_max), true);
    TestBinaryPredicates(lexical_cast<string>(t_min), true);
    TestBinaryPredicates(lexical_cast<string>(t_max), true);
    if (test_boundaries) {
      // this requires a cast of the second operand to a higher-resolution type
      TestComparison(lexical_cast<string>(t_min - 1),
                   lexical_cast<string>(t_max), true);
      // this requires a cast of the first operand to a higher-resolution type
      TestComparison(lexical_cast<string>(t_min),
                   lexical_cast<string>(t_max + 1), true);
    }
  }

  template <typename T> void TestFloatingPointComparisons(bool test_boundaries) {
    // t_min is the smallest positive value
    T t_min = numeric_limits<T>::min();
    T t_max = numeric_limits<T>::max();
    TestComparison(lexical_cast<string>(t_min), lexical_cast<string>(t_max), true);
    TestComparison(lexical_cast<string>(-1.0 * t_max), lexical_cast<string>(t_max), true);
    TestBinaryPredicates(lexical_cast<string>(t_min), true);
    TestBinaryPredicates(lexical_cast<string>(t_max), true);
    if (test_boundaries) {
      // this requires a cast of the second operand to a higher-resolution type
      TestComparison(lexical_cast<string>(numeric_limits<T>::min() - 1),
                   lexical_cast<string>(numeric_limits<T>::max()), true);
      // this requires a cast of the first operand to a higher-resolution type
      TestComparison(lexical_cast<string>(numeric_limits<T>::min()),
                   lexical_cast<string>(numeric_limits<T>::max() + 1), true);
    }

    // Compare nan: not equal to, larger than or smaller than anything, including itself
    TestValue(lexical_cast<string>(t_min) + " < 0/0", TYPE_BOOLEAN, false);
    TestValue(lexical_cast<string>(t_min) + " > 0/0", TYPE_BOOLEAN, false);
    TestValue(lexical_cast<string>(t_min) + " = 0/0", TYPE_BOOLEAN, false);
    TestValue(lexical_cast<string>(t_max) + " < 0/0", TYPE_BOOLEAN, false);
    TestValue(lexical_cast<string>(t_max) + " > 0/0", TYPE_BOOLEAN, false);
    TestValue(lexical_cast<string>(t_max) + " = 0/0", TYPE_BOOLEAN, false);
    TestValue("0/0 < 0/0", TYPE_BOOLEAN, false);
    TestValue("0/0 > 0/0", TYPE_BOOLEAN, false);
    TestValue("0/0 = 0/0", TYPE_BOOLEAN, false);

    // Compare inf: larger than everything except nan (or smaller, for -inf)
    TestValue(lexical_cast<string>(t_max) + " < 1/0", TYPE_BOOLEAN, true);
    TestValue(lexical_cast<string>(t_min) + " > -1/0", TYPE_BOOLEAN, true);
    TestValue("1/0 = 1/0", TYPE_BOOLEAN, true);
    TestValue("1/0 < 0/0", TYPE_BOOLEAN, false);
    TestValue("0/0 < 1/0", TYPE_BOOLEAN, false);
  }

  void TestStringComparisons() {
    TestValue<bool>("'abc' = 'abc'", TYPE_BOOLEAN, true);
    TestValue<bool>("'abc' = 'abcd'", TYPE_BOOLEAN, false);
    TestValue<bool>("'abc' != 'abcd'", TYPE_BOOLEAN, true);
    TestValue<bool>("'abc' != 'abc'", TYPE_BOOLEAN, false);
    TestValue<bool>("'abc' < 'abcd'", TYPE_BOOLEAN, true);
    TestValue<bool>("'abcd' < 'abc'", TYPE_BOOLEAN, false);
    TestValue<bool>("'abcd' < 'abcd'", TYPE_BOOLEAN, false);
    TestValue<bool>("'abc' > 'abcd'", TYPE_BOOLEAN, false);
    TestValue<bool>("'abcd' > 'abc'", TYPE_BOOLEAN, true);
    TestValue<bool>("'abcd' > 'abcd'", TYPE_BOOLEAN, false);
    TestValue<bool>("'abc' <= 'abcd'", TYPE_BOOLEAN, true);
    TestValue<bool>("'abcd' <= 'abc'", TYPE_BOOLEAN, false);
    TestValue<bool>("'abcd' <= 'abcd'", TYPE_BOOLEAN, true);
    TestValue<bool>("'abc' >= 'abcd'", TYPE_BOOLEAN, false);
    TestValue<bool>("'abcd' >= 'abc'", TYPE_BOOLEAN, true);
    TestValue<bool>("'abcd' >= 'abcd'", TYPE_BOOLEAN, true);

    // Test some empty strings
    TestValue<bool>("'abcd' >= ''", TYPE_BOOLEAN, true);
    TestValue<bool>("'' > ''", TYPE_BOOLEAN, false);
    TestValue<bool>("'' = ''", TYPE_BOOLEAN, true);
  }

  void TestDecimalComparisons() {
    TestValue("1.23 = 1.23", TYPE_BOOLEAN, true);
    TestValue("1.23 = 1.230", TYPE_BOOLEAN, true);
    TestValue("1.23 = 1.234", TYPE_BOOLEAN, false);
    TestValue("1.23 != 1.234", TYPE_BOOLEAN, true);
    TestValue("1.23 < 1.234", TYPE_BOOLEAN, true);
    TestValue("1.23 > 1.234", TYPE_BOOLEAN, false);
    TestValue("1.23 = 1.230000000000000000000", TYPE_BOOLEAN, true);

    // Some values are too precise to be stored to full precision as doubles, but not too
    // precise to be stored as decimals.
    static const string not_too_precise = "1.25";
    // The closest double to 'too_precise' is 1.25 - the string as written cannot be
    // preresented exactly as a double.
    static const string too_precise = "1.250000000000000000001";
    TestValue(
        "cast(" + not_too_precise + " as double) != cast(" + too_precise + " as double)",
        TYPE_BOOLEAN, false);
    TestValue(not_too_precise + " != " + too_precise, TYPE_BOOLEAN, true);
    TestValue("cast(" + not_too_precise + " as double) IS DISTINCT FROM cast(" +
            too_precise + " as double)",
        TYPE_BOOLEAN, false);
    TestValue(not_too_precise + " IS DISTINCT FROM " + too_precise, TYPE_BOOLEAN, true);
    TestValue("cast(" + not_too_precise + " as double) IS NOT DISTINCT FROM cast(" +
            too_precise + " as double)",
        TYPE_BOOLEAN, true);
    TestValue(
        not_too_precise + " IS NOT DISTINCT FROM " + too_precise, TYPE_BOOLEAN, false);
    TestValue(
        "cast(" + not_too_precise + " as double) <=> cast(" + too_precise + " as double)",
        TYPE_BOOLEAN, true);
    TestValue(not_too_precise + " <=> " + too_precise, TYPE_BOOLEAN, false);

    TestValue("cast(1 as decimal(38,0)) = cast(1 as decimal(38,37))", TYPE_BOOLEAN, true);
    TestValue("cast(1 as decimal(38,0)) = cast(0.1 as decimal(38,38))",
              TYPE_BOOLEAN, false);
    TestValue("cast(1 as decimal(38,0)) > cast(0.1 as decimal(38,38))",
              TYPE_BOOLEAN, true);
    TestBinaryPredicates("cast(1 as decimal(8,0))", false);
    TestBinaryPredicates("cast(1 as decimal(10,0))", false);
    TestBinaryPredicates("cast(1 as decimal(38,0))", false);
  }

  // Test comparison operators with a left or right NULL operand against op.
  void TestNullComparison(const string& op) {
    // NULL as right operand.
    TestIsNull(op + " = NULL", TYPE_BOOLEAN);
    TestIsNull(op + " != NULL", TYPE_BOOLEAN);
    TestIsNull(op + " <> NULL", TYPE_BOOLEAN);
    TestIsNull(op + " < NULL", TYPE_BOOLEAN);
    TestIsNull(op + " > NULL", TYPE_BOOLEAN);
    TestIsNull(op + " <= NULL", TYPE_BOOLEAN);
    TestIsNull(op + " >= NULL", TYPE_BOOLEAN);
    // NULL as left operand.
    TestIsNull("NULL = " + op, TYPE_BOOLEAN);
    TestIsNull("NULL != " + op, TYPE_BOOLEAN);
    TestIsNull("NULL <> " + op, TYPE_BOOLEAN);
    TestIsNull("NULL < " + op, TYPE_BOOLEAN);
    TestIsNull("NULL > " + op, TYPE_BOOLEAN);
    TestIsNull("NULL <= " + op, TYPE_BOOLEAN);
    TestIsNull("NULL >= " + op, TYPE_BOOLEAN);
  }

  void TestTimestampValue(const string& expr, const TimestampValue& expected_result);
  void TestValidTimestampValue(const string& expr);

  // Test IS DISTINCT FROM operator and its variants
  void TestDistinctFrom() {
    static const string operators[] = {"<=>", "IS DISTINCT FROM", "IS NOT DISTINCT FROM"};
    static const string types[] = {"Boolean", "TinyInt", "SmallInt", "Int", "BigInt",
        "Float", "Double", "String", "Timestamp", "Decimal"};
    static const string operands1[] = {
        "true", "cast(1 as TinyInt)", "cast(1 as SmallInt)", "cast(1 as Int)",
        "cast(1 as BigInt)", "cast(1 as Float)", "cast(1 as Double)",
        "'this is a string'", "cast(1 as TimeStamp)", "cast(1 as Decimal)"
    };
    static const string operands2[] = {
        "false", "cast(2 as TinyInt)", "cast(2 as SmallInt)", "cast(2 as Int)",
        "cast(2 as BigInt)", "cast(2 as Float)", "cast(2 as Double)",
        "'this is ALSO a string'", "cast(2 as TimeStamp)", "cast(2 as Decimal)"
    };
    for (int i = 0; i < sizeof(operators) / sizeof(string); ++i) {
      // "IS DISTINCT FROM" and "<=>" are generalized equality, and
      // this fact is recorded in is_equal.
      const bool is_equal = operators[i] != "IS DISTINCT FROM";
      // Everything IS NOT DISTINCT FROM itself.
      for (int j = 0; j < sizeof(types) / sizeof(string); ++j) {
        const string operand = "cast(NULL as " + types[j] + ")";
        TestValue(operand + ' ' + operators[i] + ' ' + operand, TYPE_BOOLEAN, is_equal);
      }
      for (int j = 0; j < sizeof(operands1) / sizeof(string); ++j) {
        TestValue(operands1[j] + ' ' + operators[i] + ' ' + operands1[j], TYPE_BOOLEAN,
                  is_equal);
      }
      // NULL IS DISTINCT FROM all non-null things.
      for (int j = 0; j < sizeof(operands1) / sizeof(string); ++j) {
        TestValue("NULL " + operators[i] + ' ' + operands1[j], TYPE_BOOLEAN, !is_equal);
        TestValue(operands1[j] + ' ' + operators[i] + " NULL", TYPE_BOOLEAN, !is_equal);
      }
      // Non-null values can be DISTINCT.
      for (int j = 0; j < sizeof(operands1) / sizeof(string); ++j) {
        TestValue(operands1[j] + ' ' + operators[i] + ' ' + operands2[j], TYPE_BOOLEAN,
                  !is_equal);
      }
    }
  }

  // Test comparison operators with a left or right NULL operand on all types.
  void TestNullComparisons() {
    unordered_map<int, string>::iterator def_iter;
    for(def_iter = default_type_strs_.begin(); def_iter != default_type_strs_.end();
        ++def_iter) {
      TestNullComparison(def_iter->second);
    }
    TestNullComparison("NULL");
  }

  // Generate all possible tests for combinations of <smaller> <op> <larger>.
  // Also test conversions from strings.
  void TestComparison(const string& smaller, const string& larger, bool compare_strings) {
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
  void TestBinaryPredicates(const string& value, bool compare_strings) {
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

  template <typename T> void TestFixedPointLimits(const ColumnType& type) {
    // cast to non-char type first, otherwise we might end up interpreting
    // the min/max as an ascii value
    int64_t t_min = numeric_limits<T>::min();
    int64_t t_max = numeric_limits<T>::max();
    TestValue(lexical_cast<string>(t_min), type, numeric_limits<T>::min());
    TestValue(lexical_cast<string>(t_max), type, numeric_limits<T>::max());
  }

  template <typename T> void TestFloatingPointLimits(const ColumnType& type) {
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

  // Test ops that that always promote to a fixed type (e.g., next higher
  // resolution type): ADD, SUBTRACT, MULTIPLY, DIVIDE.
  // Note that adding the " " when generating the expression is not just cosmetic.
  // We have "--" as a comment element in our lexer,
  // so subtraction of a negative value will be ignored without " ".
  template <typename LeftOp, typename RightOp, typename Result>
  void TestFixedResultTypeOps(LeftOp a, RightOp b, const ColumnType& expected_type) {
    Result cast_a = static_cast<Result>(a);
    Result cast_b = static_cast<Result>(b);
    string a_str = LiteralToString<Result>(cast_a);
    string b_str = LiteralToString<Result>(cast_b);
    TestValue(a_str + " + " + b_str, expected_type,
        static_cast<Result>(cast_a + cast_b));
    TestValue(a_str + " - " + b_str, expected_type,
        static_cast<Result>(cast_a - cast_b));
    TestValue(a_str + " * " + b_str, expected_type,
        static_cast<Result>(cast_a * cast_b));
    TestValue(a_str + " / " + b_str, TYPE_DOUBLE,
        static_cast<double>(a) / static_cast<double>(b));
  }

  // Test int ops that promote to assignment compatible type: BITAND, BITOR, BITXOR,
  // BITNOT, INT_DIVIDE, MOD.
  // As a convention we use RightOp as the higher resolution type.
  template <typename LeftOp, typename RightOp>
  void TestVariableResultTypeIntOps(LeftOp a, RightOp b,
      const ColumnType& expected_type) {
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

  // Test ops that that always promote to a fixed type with NULL operands:
  // ADD, SUBTRACT, MULTIPLY, DIVIDE.
  // We need CastType to make lexical_cast work properly for low-resolution types.
  template <typename NonNullOp, typename CastType>
  void TestNullOperandFixedResultTypeOps(NonNullOp op, const ColumnType& expected_type) {
    CastType cast_op = static_cast<CastType>(op);
    string op_str = LiteralToString<CastType>(cast_op);
    // NULL as right operand.
    TestIsNull(op_str + " + NULL", expected_type);
    TestIsNull(op_str + " - NULL", expected_type);
    TestIsNull(op_str + " * NULL", expected_type);
    TestIsNull(op_str + " / NULL", TYPE_DOUBLE);
    // NULL as left operand.
    TestIsNull("NULL + " + op_str, expected_type);
    TestIsNull("NULL - " + op_str, expected_type);
    TestIsNull("NULL * " + op_str, expected_type);
    TestIsNull("NULL / " + op_str, TYPE_DOUBLE);
  }

  // Test binary int ops with NULL as left or right operand:
  // BITAND, BITOR, BITXOR, INT_DIVIDE, MOD.
  template <typename NonNullOp>
  void TestNullOperandVariableResultTypeIntOps(NonNullOp op,
      const ColumnType& expected_type) {
    string op_str = lexical_cast<string>(static_cast<int64_t>(op));
    // NULL as right operand.
    TestIsNull(op_str + " & NULL", expected_type);
    TestIsNull(op_str + " | NULL", expected_type);
    TestIsNull(op_str + " ^ NULL", expected_type);
    TestIsNull(op_str + " DIV NULL", expected_type);
    TestIsNull(op_str + " % NULL", expected_type);
    // NULL as left operand.
    TestIsNull("NULL & " + op_str, expected_type);
    TestIsNull("NULL | " + op_str, expected_type);
    TestIsNull("NULL ^ " + op_str, expected_type);
    TestIsNull("NULL DIV " + op_str, expected_type);
    TestIsNull("NULL % " + op_str, expected_type);
  }

  // Test all binary ops with both operands being NULL.
  // We expect such exprs to return TYPE_INT.
  void TestNullOperandsArithmeticOps() {
    TestIsNull("NULL + NULL", TYPE_DOUBLE);
    TestIsNull("NULL - NULL", TYPE_DOUBLE);
    TestIsNull("NULL * NULL", TYPE_DOUBLE);
    TestIsNull("NULL / NULL", TYPE_DOUBLE);
    TestIsNull("NULL & NULL", TYPE_INT);
    TestIsNull("NULL | NULL", TYPE_INT);
    TestIsNull("NULL ^ NULL", TYPE_INT);
    TestIsNull("NULL DIV NULL", TYPE_INT);
    TestIsNull("NULL % NULL", TYPE_DOUBLE);
    TestIsNull("~NULL", TYPE_INT);
  }

  // Test factorial operator.
  void TestFactorialArithmeticOp() {
    // Basic exprs
    TestValue("4!", TYPE_BIGINT, 24);
    TestValue("0!", TYPE_BIGINT, 1);
    TestValue("-20!", TYPE_BIGINT, 1);
    TestIsNull("NULL!", TYPE_BIGINT);
    TestValue("20!", TYPE_BIGINT, 2432902008176640000); // Largest valid value
    TestError("21!"); // Overflow

    // Compound exprs
    TestValue("4 + (3!)", TYPE_BIGINT, 10);
    // TestValue("5 + 3!", TYPE_BIGINT, 11); disabled b/c IMPALA-2149
    TestValue("4! + 3", TYPE_BIGINT, 27);
    TestValue("-1!", TYPE_BIGINT, 1); // Prefix takes precedence

    // ! = should not be parsed as not equal operator
    TestValue("4! = 25", TYPE_BOOLEAN, false);

    // != should be parsed as a single token to avoid wacky behavior
    TestValue("1 != 1", TYPE_BOOLEAN, false);

    // Check factorial function exists as alias
    TestValue("factorial(3!)", TYPE_BIGINT, 720);
  }

  template<typename T>
  TimestampValue CreateTestTimestamp(T val);

  // Parse the given string representation of a value into 'val' of type T.
  // Returns false on parse failure.
  template<class T>
  bool ParseString(const string& str, T* val);

  // Create a Literal expression out of 'str'.
  Literal* CreateLiteral(const ColumnType& type, const string& str);

  // Helper function for LiteralConstruction test. Creates a Literal expression
  // of 'type' from 'str' and verifies it compares equally to 'value'.
  template <typename T>
  void TestSingleLiteralConstruction(
      const ColumnType& type, const T& value, const string& string_val);

  // Test casting stmt to all types.  Expected result is val.
  template<typename T>
  void TestCast(const string& stmt, T val, bool timestamp_out_of_range = false) {
    TestValue("cast(" + stmt + " as boolean)", TYPE_BOOLEAN, static_cast<bool>(val));
    TestValue("cast(" + stmt + " as tinyint)", TYPE_TINYINT, static_cast<int8_t>(val));
    TestValue("cast(" + stmt + " as smallint)", TYPE_SMALLINT, static_cast<int16_t>(val));
    TestValue("cast(" + stmt + " as int)", TYPE_INT, static_cast<int32_t>(val));
    TestValue("cast(" + stmt + " as integer)", TYPE_INT, static_cast<int32_t>(val));
    TestValue("cast(" + stmt + " as bigint)", TYPE_BIGINT, static_cast<int64_t>(val));
    TestValue("cast(" + stmt + " as float)", TYPE_FLOAT, static_cast<float>(val));
    TestValue("cast(" + stmt + " as double)", TYPE_DOUBLE, static_cast<double>(val));
    TestValue("cast(" + stmt + " as real)", TYPE_DOUBLE, static_cast<double>(val));
    TestStringValue("cast(" + stmt + " as string)", lexical_cast<string>(val));
    if (!timestamp_out_of_range) {
      TestTimestampValue("cast(" + stmt + " as timestamp)", CreateTestTimestamp(val));
    } else {
      TestIsNull("cast(" + stmt + " as timestamp)", TYPE_TIMESTAMP);
    }
  }
};

template<>
TimestampValue ExprTest::CreateTestTimestamp(const string& val) {
  return TimestampValue::Parse(val);
}

template<>
TimestampValue ExprTest::CreateTestTimestamp(float val) {
  return TimestampValue::FromSubsecondUnixTime(val);
}

template<>
TimestampValue ExprTest::CreateTestTimestamp(double val) {
  return TimestampValue::FromSubsecondUnixTime(val);
}

template<>
TimestampValue ExprTest::CreateTestTimestamp(int val) {
  return TimestampValue::FromUnixTime(val);
}

template<>
TimestampValue ExprTest::CreateTestTimestamp(int64_t val) {
  return TimestampValue::FromUnixTime(val);
}

// Test casting 'stmt' to each of the native types.  The result should be 'val'
// 'stmt' is a partial stmt that could be of any valid type.
template<>
void ExprTest::TestCast(const string& stmt, const char* val,
                        bool timestamp_out_of_range) {
  try {
    int8_t val8 = static_cast<int8_t>(lexical_cast<int16_t>(val));
#if 0
    // Hive has weird semantics.  What do we want to do?
    TestValue(stmt + " as boolean)", TYPE_BOOLEAN, lexical_cast<bool>(val));
#endif
    TestValue("cast(" + stmt + " as tinyint)", TYPE_TINYINT, val8);
    TestValue("cast(" + stmt + " as smallint)", TYPE_SMALLINT, lexical_cast<int16_t>(val));
    TestValue("cast(" + stmt + " as int)", TYPE_INT, lexical_cast<int32_t>(val));
    TestValue("cast(" + stmt + " as bigint)", TYPE_BIGINT, lexical_cast<int64_t>(val));
    TestValue("cast(" + stmt + " as float)", TYPE_FLOAT, lexical_cast<float>(val));
    TestValue("cast(" + stmt + " as double)", TYPE_DOUBLE, lexical_cast<double>(val));
    TestStringValue("cast(" + stmt + " as string)", lexical_cast<string>(val));
  } catch (bad_lexical_cast& e) {
    EXPECT_TRUE(false) << e.what();
  }
}

template <>
bool ExprTest::ConvertValue<bool>(const string& value) {
  if (value.compare("false") == 0) {
    return false;
  } else {
    DCHECK(value.compare("true") == 0) << value;
    return true;
  }
}

template <>
int8_t ExprTest::ConvertValue<int8_t>(const string& value) {
  StringParser::ParseResult result;
  return StringParser::StringToInt<int8_t>(value.data(), value.size(), &result);
}

template <>
int16_t ExprTest::ConvertValue<int16_t>(const string& value) {
  StringParser::ParseResult result;
  return StringParser::StringToInt<int16_t>(value.data(), value.size(), &result);
}

template <>
int32_t ExprTest::ConvertValue<int32_t>(const string& value) {
  StringParser::ParseResult result;
  return StringParser::StringToInt<int32_t>(value.data(), value.size(), &result);
}

template <>
int64_t ExprTest::ConvertValue<int64_t>(const string& value) {
  StringParser::ParseResult result;
  return StringParser::StringToInt<int64_t>(value.data(), value.size(), &result);
}

template <>
TimestampValue ExprTest::ConvertValue<TimestampValue>(const string& value) {
  return TimestampValue::Parse(value.data(), value.size());
}

// We can't put this into TestValue() because GTest can't resolve
// the ambiguity in TimestampValue::operator==, even with the appropriate casts.
void ExprTest::TestTimestampValue(const string& expr, const TimestampValue& expected_result) {
  EXPECT_EQ(expected_result,
      ConvertValue<TimestampValue>(GetValue(expr, TYPE_TIMESTAMP)));
}

// Tests whether the returned TimestampValue is valid.
// We use this function for tests where the expected value is unknown, e.g., now().
void ExprTest::TestValidTimestampValue(const string& expr) {
  EXPECT_TRUE(
      ConvertValue<TimestampValue>(GetValue(expr, TYPE_TIMESTAMP)).HasDateOrTime());
}

template<class T>
bool ExprTest::ParseString(const string& str, T* val) {
  istringstream stream(str);
  stream >> *val;
  return !stream.fail();
}

template<>
bool ExprTest::ParseString(const string& str, TimestampValue* val) {
  boost::gregorian::date date;
  boost::posix_time::time_duration time;
  bool success = TimestampParser::Parse(str.data(), str.length(), &date, &time);
  val->set_date(date);
  val->set_time(time);
  return success;
}

Literal* ExprTest::CreateLiteral(const ColumnType& type, const string& str) {
  switch (type.type) {
    case TYPE_BOOLEAN: {
      bool v = false;
      EXPECT_TRUE(ParseString<bool>(str, &v));
      return new Literal(type, v);
    }
    case TYPE_TINYINT: {
      int8_t v = 0;
      EXPECT_TRUE(ParseString<int8_t>(str, &v));
      return new Literal(type, v);
    }
    case TYPE_SMALLINT: {
      int16_t v = 0;
      EXPECT_TRUE(ParseString<int16_t>(str, &v));
      return new Literal(type, v);
    }
    case TYPE_INT: {
      int32_t v = 0;
      EXPECT_TRUE(ParseString<int32_t>(str, &v));
      return new Literal(type, v);
    }
    case TYPE_BIGINT: {
      int64_t v = 0;
      EXPECT_TRUE(ParseString<int64_t>(str, &v));
      return new Literal(type, v);
    }
    case TYPE_FLOAT: {
      float v = 0;
      EXPECT_TRUE(ParseString<float>(str, &v));
      return new Literal(type, v);
    }
    case TYPE_DOUBLE: {
      double v = 0;
      EXPECT_TRUE(ParseString<double>(str, &v));
      return new Literal(type, v);
    }
    case TYPE_STRING:
    case TYPE_VARCHAR:
    case TYPE_CHAR:
      return new Literal(type, str);
    case TYPE_TIMESTAMP: {
      TimestampValue v;
      EXPECT_TRUE(ParseString<TimestampValue>(str, &v));
      return new Literal(type, v);
    }
    case TYPE_DECIMAL: {
      double v = 0;
      EXPECT_TRUE(ParseString<double>(str, &v));
      return new Literal(type, v);
    }
    default:
      DCHECK(false) << "Invalid type: " << type.DebugString();
      return nullptr;
  }
}

template <typename T>
void ExprTest::TestSingleLiteralConstruction(
    const ColumnType& type, const T& value, const string& string_val) {
  ObjectPool pool;
  RuntimeState state(TQueryCtx(), ExecEnv::GetInstance());
  MemTracker tracker;
  MemPool mem_pool(&tracker);

  Literal* expr = CreateLiteral(type, string_val);
  ScalarExprEvaluator* eval;
  EXPECT_OK(
      ScalarExprEvaluator::Create(*expr, &state, &pool, &mem_pool, &mem_pool, &eval));
  EXPECT_OK(eval->Open(&state));
  EXPECT_EQ(0, RawValue::Compare(eval->GetValue(nullptr), &value, type))
      << "type: " << type << ", value: " << value;
  eval->Close(&state);
  expr->Close();
  state.ReleaseResources();
}

TEST_F(ExprTest, NullLiteral) {
  for (int type = TYPE_BOOLEAN; type != TYPE_DATE; ++type) {
    RuntimeState state(TQueryCtx(), ExecEnv::GetInstance());
    ObjectPool pool;
    MemTracker tracker;
    MemPool mem_pool(&tracker);

    NullLiteral expr(static_cast<PrimitiveType>(type));
    ScalarExprEvaluator* eval;
    EXPECT_OK(
        ScalarExprEvaluator::Create(expr, &state, &pool, &mem_pool, &mem_pool, &eval));
    EXPECT_OK(eval->Open(&state));
    EXPECT_TRUE(eval->GetValue(nullptr) == nullptr);
    eval->Close(&state);
    state.ReleaseResources();
  }
}

TEST_F(ExprTest, LiteralConstruction) {
  bool b_val = true;
  int8_t c_val = 'f';
  int16_t s_val = 123;
  int32_t i_val = 234;
  int64_t l_val = 1234;
  float f_val = 3.14f;
  double d_val_1 = 1.23;
  double d_val_2 = 7e6;
  double d_val_3 = 5.9e-3;
  string str_input = "Hello";
  StringValue str_val(const_cast<char*>(str_input.data()), str_input.length());

  TestSingleLiteralConstruction(TYPE_BOOLEAN, b_val, "1");
  TestSingleLiteralConstruction(TYPE_TINYINT, c_val, "f");
  TestSingleLiteralConstruction(TYPE_SMALLINT, s_val, "123");
  TestSingleLiteralConstruction(TYPE_INT, i_val, "234");
  TestSingleLiteralConstruction(TYPE_INT, i_val, "+234");
  TestSingleLiteralConstruction(TYPE_BIGINT, l_val, "1234");
  TestSingleLiteralConstruction(TYPE_BIGINT, l_val, "+1234");
  TestSingleLiteralConstruction(TYPE_FLOAT, f_val, "3.14");
  TestSingleLiteralConstruction(TYPE_FLOAT, f_val, "+3.14");
  TestSingleLiteralConstruction(TYPE_DOUBLE, d_val_1, "1.23");
  TestSingleLiteralConstruction(TYPE_DOUBLE, d_val_1, "+1.23");
  TestSingleLiteralConstruction(TYPE_DOUBLE, d_val_2, "7e6");
  TestSingleLiteralConstruction(TYPE_DOUBLE, d_val_2, "+7e6");
  TestSingleLiteralConstruction(TYPE_DOUBLE, d_val_3, "5.9e-3");
  TestSingleLiteralConstruction(TYPE_DOUBLE, d_val_3, "+5.9e-3");
  TestSingleLiteralConstruction(TYPE_STRING, str_val, "Hello");

  // Min/Max Boundary value test for tiny/small/int/long
  c_val = 127;
  const char c_array_max[] = {(const char)127}; // avoid implicit casting
  string c_input_max(c_array_max, 1);
  s_val = 32767;
  i_val = 2147483647;
  l_val = 9223372036854775807l;
  TestSingleLiteralConstruction(TYPE_TINYINT, c_val, c_input_max);
  TestSingleLiteralConstruction(TYPE_SMALLINT, s_val, "32767");
  TestSingleLiteralConstruction(TYPE_INT, i_val, "2147483647");
  TestSingleLiteralConstruction(TYPE_BIGINT, l_val, "9223372036854775807");

  const char c_array_min[] = {(const char)(-128)}; // avoid implicit casting
  string c_input_min(c_array_min, 1);
  c_val = -128;
  s_val = -32768;
  i_val = -2147483648;
  l_val = -9223372036854775807l-1;
  TestSingleLiteralConstruction(TYPE_TINYINT, c_val, c_input_min);
  TestSingleLiteralConstruction(TYPE_SMALLINT, s_val, "-32768");
  TestSingleLiteralConstruction(TYPE_INT, i_val, "-2147483648");
  TestSingleLiteralConstruction(TYPE_BIGINT, l_val, "-9223372036854775808");
}


TEST_F(ExprTest, LiteralExprs) {
  TestFixedPointLimits<int8_t>(TYPE_TINYINT);
  TestFixedPointLimits<int16_t>(TYPE_SMALLINT);
  TestFixedPointLimits<int32_t>(TYPE_INT);
  TestFixedPointLimits<int64_t>(TYPE_BIGINT);
  // The value is not an exact FLOAT so it gets compared as a DOUBLE
  // and fails.  This needs to be researched.
  // TestFloatingPointLimits<float>(TYPE_FLOAT);
  TestFloatingPointLimits<double>(TYPE_DOUBLE);

  TestValue("true", TYPE_BOOLEAN, true);
  TestValue("false", TYPE_BOOLEAN, false);
  TestStringValue("'test'", "test");
  TestIsNull("null", TYPE_NULL);
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

  // Test ops that always promote to fixed type (e.g., next higher resolution type).
  TestFixedResultTypeOps<int8_t, int8_t, int16_t>(min_int_values_[TYPE_TINYINT],
      min_int_values_[TYPE_TINYINT], TYPE_SMALLINT);
  TestFixedResultTypeOps<int8_t, int16_t, int32_t>(min_int_values_[TYPE_TINYINT],
      min_int_values_[TYPE_SMALLINT], TYPE_INT);
  TestFixedResultTypeOps<int8_t, int32_t, int64_t>(min_int_values_[TYPE_TINYINT],
      min_int_values_[TYPE_INT], TYPE_BIGINT);
  TestFixedResultTypeOps<int8_t, int64_t, int64_t>(min_int_values_[TYPE_TINYINT],
      min_int_values_[TYPE_BIGINT], TYPE_BIGINT);
  TestFixedResultTypeOps<int16_t, int16_t, int32_t>(min_int_values_[TYPE_SMALLINT],
      min_int_values_[TYPE_SMALLINT], TYPE_INT);
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

  // Test behavior with NULLs.
  TestNullOperandFixedResultTypeOps<float, double>(min_float_values_[TYPE_FLOAT],
      TYPE_DOUBLE);
  TestNullOperandFixedResultTypeOps<double, double>(min_float_values_[TYPE_DOUBLE],
      TYPE_DOUBLE);
  TestNullOperandFixedResultTypeOps<int8_t, int64_t>(min_int_values_[TYPE_TINYINT],
      TYPE_SMALLINT);
  TestNullOperandFixedResultTypeOps<int16_t, int64_t>(min_int_values_[TYPE_SMALLINT],
      TYPE_INT);
  TestNullOperandFixedResultTypeOps<int32_t, int64_t>(min_int_values_[TYPE_INT],
      TYPE_BIGINT);
  TestNullOperandFixedResultTypeOps<int64_t, int64_t>(min_int_values_[TYPE_BIGINT],
      TYPE_BIGINT);

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

  // Test behavior of INT_DIVIDE and MOD with zero as second argument.
  IntValMap::iterator int_iter;
  for(int_iter = min_int_values_.begin(); int_iter != min_int_values_.end();
      ++int_iter) {
    string& val = default_type_strs_[int_iter->first];
    TestIsNull(val + " DIV 0", static_cast<PrimitiveType>(int_iter->first));
    TestIsNull(val + " % 0", static_cast<PrimitiveType>(int_iter->first));
  }

  // Test behavior with NULLs.
  TestNullOperandVariableResultTypeIntOps<int8_t>(min_int_values_[TYPE_TINYINT],
      TYPE_TINYINT);
  TestNullOperandVariableResultTypeIntOps<int16_t>(min_int_values_[TYPE_SMALLINT],
      TYPE_SMALLINT);
  TestNullOperandVariableResultTypeIntOps<int32_t>(min_int_values_[TYPE_INT],
      TYPE_INT);
  TestNullOperandVariableResultTypeIntOps<int64_t>(min_int_values_[TYPE_BIGINT],
      TYPE_BIGINT);

  // Tests for dealing with '-'.
  TestValue("-1", TYPE_TINYINT, -1);
  TestValue("1 - 1", TYPE_SMALLINT, 0);
  TestValue("1 - - 1", TYPE_SMALLINT, 2);
  TestValue("1 - - - 1", TYPE_SMALLINT, 0);
  TestValue("- 1 - 1", TYPE_SMALLINT, -2);
  TestValue("- 1 - - 1", TYPE_SMALLINT, 0);
  // The "--" indicates a comment to be ignored.
  // Therefore, the result should be -1.
  TestValue("- 1 --1", TYPE_TINYINT, -1);

  // Test all arithmetic exprs with only NULL operands.
  TestNullOperandsArithmeticOps();

  // Test behavior of factorial operator.
  TestFactorialArithmeticOp();
}

// Use a table-driven approach to test DECIMAL expressions to make it easier to test
// both the old (decimal_v2=false) and new (decimal_v2=true) DECIMAL semantics.
struct DecimalExpectedResult {
  bool error;
  bool null;
  int128_t scaled_val;
  int precision;
  int scale;
};

struct DecimalTestCase {

  // Return the expected result for the given DECIMAL version. When the expected V1 and
  // V2 results are the same, allows the V2 expected result to be unspecified in the
  // test case table. When V2 expected results aren't specified, V2 entries have both
  // 'null' set to false and 'precision' set to 0, which is an illegal combination and
  // signals that V1 results should be used instead.
  const DecimalExpectedResult& Expected(bool v2) const {
    if (v2 && (expected[1].null || expected[1].precision != 0)) return expected[1];
    // Either testing version 1, or the results for version 2 were not specified.
    return expected[0];
  }

  // Expression to execute.
  string expr;

  // Expected results for version 1 and Version 2. Version 2 results can be left unset
  // when the expected result is the same between versions.
  DecimalExpectedResult expected[2];
};

// Utility function to construct int128 types.
int128_t StringToInt128(string s) {
  int128_t result = 0;
  int sign = 1;
  int digit = 0;
  for (int i = 0; i < s.length(); ++i) {
    switch (s[i]) {
      case '-':
        digit = -1;
        break;
      case '0':
        digit = 0;
        break;
      case '1':
        digit = 1;
        break;
      case '2':
        digit = 2;
        break;
      case '3':
        digit = 3;
        break;
      case '4':
        digit = 4;
        break;
      case '5':
        digit = 5;
        break;
      case '6':
        digit = 6;
        break;
      case '7':
        digit = 7;
        break;
      case '8':
        digit = 8;
        break;
      case '9':
        digit = 9;
        break;
      default:
        DCHECK(false) << "Unexpected character.";
    }
    if (digit == -1) {
      DCHECK_EQ(sign, 1) << "Minus symbol appears multiple times.";
      sign = -1;
    } else {
      result = result * 10 + digit;
    }
  }
  return sign * result;
}

// Format is:
// { Test Expression (as a string),
//  { expected error, expected null, scaled_val, precision, scale for V1
//    expected error, expected null, scaled_val, precision, scale for V2 }}
DecimalTestCase decimal_cases[] = {
  // Test add/subtract operators
  { "1.23 + cast(1 as decimal(4,3))", {{ false, false, 2230, 5, 3 }}},
  { "1.23 - cast(0.23 as decimal(10,3))", {{ false, false, 1000, 11, 3 }}},
  { "cast(1.23 as decimal(8,2)) + cast(1 as decimal(20,3))",
    {{ false, false, 2230, 21, 3 }}},
  { "cast(1.23 as decimal(30,2)) - cast(1 as decimal(4,3))",
    {{ false, false, 230, 32, 3 }}},
  { "cast(1 as decimal(38,0)) + cast(.2 as decimal(38,1))",
    {{ false, false, 12, 38, 1 }}},
  // Test multiply operator
  { "cast(1.23 as decimal(30,2)) * cast(1 as decimal(10,3))",
    {{ false, false, 123000, 38, 5 }}},
  { "cast(1.23 as decimal(3,2)) * cast(1 as decimal(20,3))",
    {{ false, false, 123000, 23, 5 },
     { false, false, 123000, 24, 5 }}},
  { "cast(0.1 as decimal(20,20)) * cast(1 as decimal(20,19))",
    {{ false, false, StringToInt128("10000000000000000000000000000000000000"), 38, 38 },
     { false, false, StringToInt128("100000000000000000000000000000000000"), 38, 36 }}},
  { "cast(111.22 as decimal(5,2)) * cast(3333.444 as decimal(7,3))",
    {{ false, false, 37074564168, 12, 5 },
     { false, false, 37074564168, 13, 5 }}},
  { "cast(0.01 as decimal(38,38)) * cast(25 as decimal(38,0))",
    {{ false, false, StringToInt128("25000000000000000000000000000000000000"), 38, 38 },
     { false, false, 250000, 38, 6 }}},
  { "cast(-0.01 as decimal(38,38)) * cast(25 as decimal(38,0))",
    {{ false, false, StringToInt128("-25000000000000000000000000000000000000"), 38, 38 },
     { false, false, -250000, 38, 6 }}},
  { "cast(-0.01 as decimal(38,38)) * cast(-25 as decimal(38,0))",
    {{ false, false, StringToInt128("25000000000000000000000000000000000000"), 38, 38 },
     { false, false, 250000, 38, 6 }}},
  { "cast(0.1 as decimal(38,38)) * cast(25 as decimal(38,0))",
    {{ false, true, 0, 38, 38 },
     { false, false, 2500000, 38, 6 }}},
  { "cast(-0.1 as decimal(38,38)) * cast(25 as decimal(38,0))",
    {{ false, true, 0, 38, 38 },
     { false, false, -2500000, 38, 6 }}},
  { "cast(-0.1 as decimal(38,38)) * cast(-25 as decimal(38,0))",
    {{ false, true, 0, 38, 38 },
     { false, false, 2500000, 38, 6 }}},
  { "cast(9999999999999999.9999 as decimal(20,4)) * "
      "cast(9999999999999999.994 as decimal(19,3))",
    {{ false, true, 0, 38, 7 },
     { false, false, StringToInt128("99999999999999999939000000000000000001"), 38, 6 }}},
  { "cast(9.99999 as decimal(6,5)) * "
      "cast(9999999999999999999999999999999.94 as decimal(33,2))",
    {{ false, true, 0, 38, 7 },
     { false, false, StringToInt128("99999899999999999999999999999999400001"), 38, 6 }}},
  { "cast(0 as decimal(38,38)) * cast(1 as decimal(5,2))",
    {{ false, false, 0, 38, 38 },
     { false, false, 0, 38, 34 }}},
  { "cast(12345.67 as decimal(7,2)) * cast(12345.67 as decimal(7,2))",
    {{ false, false, 1524155677489, 14, 4 },
     { false, false, 1524155677489, 15, 4 }}},
  { "cast(2643918831543678.5772617359442611897419 as decimal(38,22)) * "
      "cast(3972211379387512.6946776728996748717839 as decimal(38,22))",
    {{ false, true, 0, 38, 38 },
     { false, false, StringToInt128("10502204468834736291038133384309593605"), 38, 6 }}},
  { "cast(-2643918831543678.5772617359442611897419 as decimal(38,22)) * "
      "cast(3972211379387512.6946776728996748717839 as decimal(38,22))",
    {{ false, true, 0, 38, 38 },
     { false, false, StringToInt128("-10502204468834736291038133384309593605"), 38, 6 }}},
  { "cast(-2643918831543678.5772617359442611897419 as decimal(38,22)) * "
      "cast(-3972211379387512.6946776728996748717839 as decimal(38,22))",
    {{ false, true, 0, 38, 38 },
     { false, false, StringToInt128("10502204468834736291038133384309593605"), 38, 6 }}},
  { "cast(2545664579818579.6268123468146829994472 as decimal(38,22)) * "
      "cast(8862165565622381.6689679519457799681439 as decimal(38,22))",
    {{ false, true, 0, 38, 38 },
     { false, false, StringToInt128("22560100980892785285767018366785939200"), 38, 6 }}},
  { "cast(2575543652687181.7412422395638291836214 as decimal(38,22)) * "
      "cast(9142529549684737.3986798331295277312253 as decimal(38,22))",
    {{ false, true, 0, 38, 38 },
     { false, false, StringToInt128("23546983951195523383767823614338114083"), 38, 6 }}},
  { "cast(6188696551164477.9944646378584981371524 as decimal(38,22)) * "
      "cast(9234914917975734.1781526147879384775182 as decimal(38,22))",
    {{ false, true, 0, 38, 38 },
     { false, false, StringToInt128("57152086103173814294786121078946977968"), 38, 6 }}},
  { "cast(-6188696551164477.9944646378584981371524 as decimal(38,22)) * "
      "cast(9234914917975734.1781526147879384775182 as decimal(38,22))",
    {{ false, true, 0, 38, 38 },
     { false, false, StringToInt128("-57152086103173814294786121078946977968"), 38, 6 }}},
  { "cast(1.006 as decimal(4,2)) * cast(1.1 as decimal(4,2))",
    {{ false, false, 11000, 8, 4 },
     { false, false, 11110, 9, 4 }}},
  { "cast(0.9994 as decimal(38,4)) * cast(0.999 as decimal(38,3))",
    {{ false, false, 9984006, 38, 7 },
     { false, false, 998401, 38, 6 }}},
  { "cast(0.000000000000000000000000000000000001 as decimal(38,38)) * "
      "cast(0.000000000000000000000000000000000001 as decimal(38,38))",
    {{ false, false, 0, 38, 38 },
     { false, false, 0, 38, 37 }}},
  { "cast(0.00000000000000000000000000000000001 as decimal(38,37)) * "
      "cast(0.00000000000000000000000000000000001 as decimal(38,37))",
    {{ false, false, 0, 38, 38 },
     { false, false, 0, 38, 35 }}},
  { "cast(0.00000000000000001 as decimal(38,37)) * "
      "cast(0.000000000000000001 as decimal(38,37))",
    {{ false, false, 1000, 38, 38 },
     { false, false, 1, 38, 35 }}},
  { "cast(9e-18 as decimal(38,38)) * cast(1e-21 as decimal(38,38))",
    {{ false, false, 0, 38, 38 },
     { false, false, 0, 38, 37 }}},
  { "cast(1e-37 as decimal(38,38)) * cast(0.1 as decimal(38,38))",
    {{ false, false, 1, 38, 38 },
     { false, false, 0, 38, 37 }}},
  { "cast(1e-37 as decimal(38,38)) * cast(-0.1 as decimal(38,38))",
    {{ false, false, -1, 38, 38 },
     { false, false, 0, 38, 37 }}},
  { "cast(9e-36 as decimal(38,38)) * cast(0.1 as decimal(38,38))",
    {{ false, false, 90, 38, 38 },
     { false, false, 9, 38, 37 }}},
  { "cast(9e-37 as decimal(38,38)) * cast(0.1 as decimal(38,38))",
    {{ false, false, 9, 38, 38 },
     { false, false, 1, 38, 37 }}},
  // We are multiplying (2^64 - 1) * (2^63 - 1), which produces the largest intermediate
  // result that does not require int256. It overflows because the result is larger than
  // MAX_UNSCALED_DECIMAL16.
  { "cast(18446744073709551615 as decimal(38,0)) * "
    "cast(9223372036854775807 as decimal(38,0))",
    {{ false, true, 0, 38, 0 }}},
  { "cast(-18446744073709551615 as decimal(38,0)) * "
    "cast(9223372036854775807 as decimal(38,0))",
    {{ false, true, 0, 38, 0 }}},
  // int256 is required. We are multiplying (2^64 - 1) * (2^63) here.
  { "cast(18446744073709551615 as decimal(38,0)) * "
    "cast(9223372036854775808 as decimal(38,0))",
    {{ false, true, 0, 38, 0 }}},
  { "cast(-18446744073709551615 as decimal(38,0)) * "
    "cast(9223372036854775808 as decimal(38,0))",
    {{ false, true, 0, 38, 0 }}},
  // Largest intermediate result that does not require int256.
  { "cast(1844674407370.9551615 as decimal(38,7)) * "
    "cast(9223372036854775807 as decimal(38,0))",
    {{ false, true, 0, 38, 7 },
     { false, false, StringToInt128("17014118346046923170401718760531977831"), 38, 6 }}},
  { "cast(-1844674407370.9551615 as decimal(38,7)) * "
    "cast(9223372036854775807 as decimal(38,0))",
    {{ false, true, 0, 38, 7 },
     { false, false, StringToInt128("-17014118346046923170401718760531977831"), 38, 6 }}},
  // int256 is required.
  { "cast(1844674407370.9551615 as decimal(38,7)) * "
    "cast(9223372036854775808 as decimal(38,0))",
    {{ false, true, 0, 38, 7 },
     { false, false, StringToInt128("17014118346046923172246393167902932992"), 38, 6 }}},
  { "cast(-1844674407370.9551615 as decimal(38,7)) * "
    "cast(9223372036854775808 as decimal(38,0))",
    {{ false, true, 0, 38, 7 },
     { false, false, StringToInt128("-17014118346046923172246393167902932992"), 38, 6 }}},
  // Smallest intermediate value that requires double checking if int256 is required.
  { "cast(9223372036854775808 as decimal(38,0)) * "
    "cast(9223372036854775808 as decimal(38,0))",
    {{ false, false, StringToInt128("85070591730234615865843651857942052864"), 38, 0 }}},
  { "cast(-9223372036854775808 as decimal(38,0)) * "
    "cast(9223372036854775808 as decimal(38,0))",
    {{ false, false, StringToInt128("-85070591730234615865843651857942052864"), 38, 0 }}},
  // Scale down the intermediate value by 10^39.
  { "cast(0.33333333333333333333333333333333333333 as decimal(38,38)) * "
      "cast(0.00000000000000000000000000000000000003 as decimal(38,38))",
    {{ false, false, 0, 38, 38 },
     { false, false, 0, 38, 37 }}},
  { "cast(0.33333333333333333333333333333333333333 as decimal(38,38)) * "
      "cast(0.3 as decimal(38,38))",
    {{ false, false, StringToInt128("9999999999999999999999999999999999999"), 38, 38 },
     { false, false, StringToInt128("1000000000000000000000000000000000000"), 38, 37 }}},
  { "cast(10000000000000000000 as decimal(21,0)) * "
      "cast(10000000000000000000 as decimal(21,0))",
    {{ false, true, 0, 38, 0 }}},
  { "cast(1000000000000000.0000 as decimal(38,4)) * "
      "cast(1000000000000000.0000 as decimal(38,4))",
    {{ false, true, 0, 38, 8 },
     { false, false, StringToInt128("1000000000000000000000000000000000000"), 38, 6 }}},
  { "cast(-1000000000000000.0000 as decimal(38,4)) * "
      "cast(1000000000000000.0000 as decimal(38,4))",
    {{ false, true, 0, 38, 8 },
     { false, false, StringToInt128("-1000000000000000000000000000000000000"), 38, 6 }}},
  // Smallest 256 bit intermediate result that can't be scaled down to 128 bits.
  { "cast(10000000000000000.0000 as decimal(38,4)) * "
      "cast(10000000000000000.0000 as decimal(38,4))",
    {{ false, true, 0, 38, 8 },
     { false, true, 0, 38, 6 }}},
  { "cast(-10000000000000000.0000 as decimal(38,4)) * "
      "cast(10000000000000000.0000 as decimal(38,4))",
    {{ false, true, 0, 38, 8 },
     { false, true, 0, 38, 6 }}},
  // The reason why the result of (38,38) * (38,38) is (38,37).
  { "cast(0.99999999999999999999999999999999999999 as decimal(38,38)) * "
      "cast(0.99999999999999999999999999999999999999 as decimal(38,38))",
    {{ false, false, StringToInt128("99999999999999999999999999999999999998"), 38, 38 },
     { false, false, StringToInt128("10000000000000000000000000000000000000"), 38, 37 }}},
  { "cast(-0.99999999999999999999999999999999999999 as decimal(38,38)) * "
      "cast(0.99999999999999999999999999999999999999 as decimal(38,38))",
    {{ false, false, StringToInt128("-99999999999999999999999999999999999998"), 38, 38 },
     { false, false, StringToInt128("-10000000000000000000000000000000000000"), 38, 37 }}},
  { "cast(99999999999999999999999999999999999999 as decimal(38,0)) * "
      "cast(1 as decimal(38,0))",
    {{ false, false, StringToInt128("99999999999999999999999999999999999999"), 38, 0 }}},
  { "cast(99999999999999999999999999999999999999 as decimal(38,0)) * "
      "cast(-1 as decimal(38,0))",
    {{ false, false, StringToInt128("-99999999999999999999999999999999999999"), 38, 0 }}},
  // Rounding.
  { "cast(0.000005 as decimal(38,6)) * cast(0.1 as decimal(38,1))",
    {{ false, false, 5, 38, 7 },
     { false, false, 1, 38, 6 }}},
  { "cast(-0.000005 as decimal(38,6)) * cast(0.1 as decimal(38,1))",
    {{ false, false, -5, 38, 7 },
     { false, false, -1, 38, 6 }}},
  { "cast(0.000004 as decimal(38,6)) * cast(0.1 as decimal(38,1))",
    {{ false, false, 4, 38, 7 },
     { false, false, 0, 38, 6 }}},
  { "cast(-0.000004 as decimal(38,6)) * cast(0.1 as decimal(38,1))",
    {{ false, false, -4, 38, 7 },
     { false, false, 0, 38, 6 }}},
  // Test divide operator
  { "cast(1.23 as decimal(8,2)) / cast(1 as decimal(4,3))",
    {{ false, false, 12300000, 16, 7}}},
  { "cast(1.23 as decimal(30,2)) / cast(1 as decimal(20,3))",
    {{ false, false,     1230, 38, 3 },
     { false, false, 12300000, 38, 7 }}},
  { "cast(1 as decimal(38,0)) / cast(.2 as decimal(38,1))",
    {{ false, false,      50, 38, 1 },
     { false, false, 5000000, 38, 6 }}},
  { "cast(1 as decimal(38,0)) / cast(3 as decimal(38,0))",
    {{ false, false,      0, 38, 0 },
     { false, false, 333333, 38, 6 }}},
  { "cast(99999999999999999999999999999999999999 as decimal(38,0)) / "
    "cast(99999999999999999999999999999999999999 as decimal(38,0))",
    {{ false, false,       1, 38, 0 },
     { false, false, 1000000, 38, 6 }}},
  { "cast(99999999999999999999999999999999999999 as decimal(38,0)) / "
    "cast(0.00000000000000000000000000000000000001 as decimal(38,38))",
    {{ false, true, 0, 38, 38 },
     { false, true, 0, 38,  6 }}},
  { "cast(0.00000000000000000000000000000000000001 as decimal(38,38)) / "
    "cast(99999999999999999999999999999999999999 as decimal(38,0))",
    {{ false, false, 0, 38, 38 }}},
  { "cast(0.00000000000000000000000000000000000001 as decimal(38,38)) / "
    "cast(0.00000000000000000000000000000000000001 as decimal(38,38))",
    {{ false, true,        0, 38, 38 },
     { false, false, 1000000, 38,  6 }}},
  { "cast(9999999999999999999.9999999999999999999 as decimal(38,19)) / "
    "cast(99999999999999999999999999999.999999999 as decimal(38,9))",
    {{ false, false, 1000000000, 38, 19 },
     { false, false,          1, 38, 10 }}},
  { "cast(999999999999999999999999999999999999.99 as decimal(38,2)) / "
    "cast(99999999999.999999999999999999999999999 as decimal(38,27))",
    {{ false, true,  0, 38, 27 },
     { false, false, static_cast<int128_t>(10) * 10000000000ll *
       10000000000ll * 10000000000ll, 38, 6 }}},
  { "cast(-2.12 as decimal(17,2)) / cast(12515.95 as decimal(17,2))",
    {{ false, false, -16938386618674571, 37, 20 }}},
  { "cast(-2.12 as decimal(18,2)) / cast(12515.95 as decimal(18,2))",
    {{ false, false,                  0, 38,  2 },
     { false, false, -16938386618674571, 38, 20 }}},
  { "cast(737373 as decimal(6,0)) / cast(.52525252 as decimal(38,38))",
    {{ false, true,              0, 38, 38 },
     { false, false, 1403844764038, 38,  6 }}},
  { "cast(0.000001 as decimal(6,6)) / "
    "cast(0.0000000000000000000000000000000000001 as decimal(38,38))",
    {{ false, true, 0, 38, 38 },
     { false, false, static_cast<int128_t>(10000000ll) *
       10000000000ll * 10000000000ll * 10000000000ll, 38, 6 }}},
  { "cast(98765432109876543210 as decimal(20,0)) / "
    "cast(98765432109876543211 as decimal(20,0))",
    {{ false, false,                   0, 38,  0 },
     { false, false, 1000000000000000000, 38, 18 }}},
  { "cast(111111.1111 as decimal(20, 10)) / cast(.7777 as decimal(38, 38))",
    {{ false, true,             0, 38, 38 },
     { false, false, 142871429986, 38,  6 }}},
  { "2.0 / 3.0",
    {{ false, false,   6666, 6, 4},
     { false, false, 666667, 8, 6}}},
  { "-2.0 / 3.0",
    {{ false, false,   -6666, 6, 4},
     { false, false, -666667, 8, 6}}},
  { "2.0 / -3.0",
    {{ false, false,   -6666, 6, 4},
     { false, false, -666667, 8, 6}}},
  { "-2.0 / -3.0",
    {{ false, false,   6666, 6, 4},
     { false, false, 666667, 8, 6}}},
  // Test divide rounding
  { "10.10 / 3.0",
    {{ false, false, 336666, 8, 5 },
     { false, false, 3366667, 9, 6 }}},
  { "cast(-10.10 as decimal(4,2)) / 3.0", // XXX JIRA: IMPALA-4877
    {{ false, false, -336666, 8, 5 },
     { false, false, -3366667, 9, 6 }}},
  { "10.10 / 20.3",
    {{ false, false, 497536, 9, 6 },
     { false, false, 497537, 9, 6 }}},
  { "10.10 / -20.3",
    {{ false, false, -497536, 9, 6 },
     { false, false, -497537, 9, 6 }}},
  // N.B. - Google and python both insist that 999999.998 / 999 is 1001.000999
  // However, multiplying the result back, 999 * 1001.000998999 gives the
  // original value exactly, while their answer does not, 999 * 10001.000999 =
  // 999999.998001. The same issue comes up many times during the following
  // computations.  Division is hard, let's go shopping.
  { "cast(999999.998 as decimal(9,3)) / 999",
    {{ false, false, 1001000998998, 15, 9 },
     { false, false, 1001000998999, 15, 9 }}},
  { "cast(999.999998 as decimal(9,3)) / 999",
    {{ false, false, 1001000000, 15, 9 },
     { false, false, 1001001001, 15, 9 }}},
  { "cast(999.999998 as decimal(9,6)) / 999",
    {{ false, false, 1001000998998, 15, 12 },
     { false, false, 1001000998999, 15, 12 }}},
  { "cast(0.999999998 as decimal(9,6)) / 999",
    {{ false, false, 1001000000, 15, 12 },
     { false, false, 1001001001, 15, 12 }}},
  { "cast(0.999999998 as decimal(9,9)) / 999",
    {{ false, false, 1001000998998, 15, 15 },
     { false, false, 1001000998999, 15, 15 }}},
  { "cast(-999999.998 as decimal(9,3)) / 999",
    {{ false, false, -1001000998998, 15, 9 },
     { false, false, -1001000998999, 15, 9 }}},
  { "cast(-999.999998 as decimal(9,3)) / 999",
    {{ false, false, -1001000000, 15, 9 },
     { false, false, -1001001001, 15, 9 }}},
  { "cast(-999.999998 as decimal(9,6)) / 999",
    {{ false, false, -1001000998998, 15, 12 },
     { false, false, -1001000998999, 15, 12 }}},
  { "cast(-0.999999998 as decimal(9,6)) / 999",
    {{ false, false, -1001000000, 15, 12 },
     { false, false, -1001001001, 15, 12 }}},
  { "cast(-0.999999998 as decimal(9,9)) / 999",
    {{ false, false, -1001000998998, 15, 15 },
     { false, false, -1001000998999, 15, 15 }}},
  { "cast(-999999.998 as decimal(9,3)) / -999",
    {{ false, false, 1001000998998, 15, 9 },
     { false, false, 1001000998999, 15, 9 }}},
  { "cast(-999.999998 as decimal(9,3)) / -999",
    {{ false, false, 1001000000, 15, 9 },
     { false, false, 1001001001, 15, 9 }}},
  { "cast(-999.999998 as decimal(9,6)) / -999",
    {{ false, false, 1001000998998, 15, 12 },
     { false, false, 1001000998999, 15, 12 }}},
  { "cast(-0.999999998 as decimal(9,6)) / -999",
    {{ false, false, 1001000000, 15, 12 },
     { false, false, 1001001001, 15, 12 }}},
  { "cast(-0.999999998 as decimal(9,9)) / -999",
    {{ false, false, 1001000998998, 15, 15 },
     { false, false, 1001000998999, 15, 15 }}},
  { "cast(999999.998 as decimal(9,3)) / -999",
    {{ false, false, -1001000998998, 15, 9 },
     { false, false, -1001000998999, 15, 9 }}},
  { "cast(999.999998 as decimal(9,3)) / -999",
    {{ false, false, -1001000000, 15, 9 },
     { false, false, -1001001001, 15, 9 }}},
  { "cast(999.999998 as decimal(9,6)) / -999",
    {{ false, false, -1001000998998, 15, 12 },
     { false, false, -1001000998999, 15, 12 }}},
  { "cast(0.999999998 as decimal(9,6)) / -999",
    {{ false, false, -1001000000, 15, 12 },
     { false, false, -1001001001, 15, 12 }}},
  { "cast(0.999999998 as decimal(9,9)) / -999",
    {{ false, false, -1001000998998, 15, 15 },
     { false, false, -1001000998999, 15, 15 }}},
  { "cast(999.999998 as decimal(9,3)) / 999999999",
    {{ false, false, 99999900, 20, 14 },
     { false, false, 100000000, 20, 14 }}},
  { "cast(999.999998 as decimal(9,6)) / 999999999",
    {{ false, false, 99999999899, 20, 17 },
     { false, false, 99999999900, 20, 17 }}},
  { "cast(0.999999998 as decimal(9,6)) / 999999999",
    {{ false, false, 99999900, 20, 17 },
     { false, false, 100000000, 20, 17 }}},
  { "cast(0.999999998 as decimal(9,9)) / 999999999",
    {{ false, false, 99999999899, 20, 20 },
     { false, false, 99999999900, 20, 20 }}},
  { "cast(-999999.998 as decimal(9,3)) / 999999999",
    {{ false, false, -99999999899, 20, 14 },
     { false, false, -99999999900, 20, 14 }}},
  { "cast(-999.999998 as decimal(9,3)) / 999999999",
    {{ false, false, -99999900, 20, 14 },
     { false, false, -100000000, 20, 14 }}},
  { "cast(-999.999998 as decimal(9,6)) / 999999999",
    {{ false, false, -99999999899, 20, 17 },
     { false, false, -99999999900, 20, 17 }}},
  { "cast(-0.999999998 as decimal(9,6)) / 999999999",
    {{ false, false, -99999900, 20, 17 },
     { false, false, -100000000, 20, 17 }}},
  { "cast(-0.999999998 as decimal(9,9)) / 999999999",
    {{ false, false, -99999999899, 20, 20 },
     { false, false, -99999999900, 20, 20 }}},
  { "cast(-999999.998 as decimal(9,3)) / 999999999",
    {{ false, false, -99999999899, 20, 14 },
     { false, false, -99999999900, 20, 14 }}},
  { "cast(-999.999998 as decimal(9,3)) / 999999999",
    {{ false, false, -99999900, 20, 14 },
     { false, false, -100000000, 20, 14 }}},
  { "cast(-999.999998 as decimal(9,6)) / 999999999",
    {{ false, false, -99999999899, 20, 17 },
     { false, false, -99999999900, 20, 17 }}},
  { "cast(-0.999999998 as decimal(9,6)) / 999999999",
    {{ false, false, -99999900, 20, 17 },
     { false, false, -100000000, 20, 17 }}},
  { "cast(-0.999999998 as decimal(9,9)) / 999999999",
    {{ false, false, -99999999899, 20, 20 },
     { false, false, -99999999900, 20, 20 }}},
  { "cast(999999.998 as decimal(9,3)) / -999999999",
    {{ false, false, -99999999899, 20, 14 },
     { false, false, -99999999900, 20, 14 }}},
  { "cast(999.999998 as decimal(9,3)) / -999999999",
    {{ false, false, -99999900, 20, 14 },
     { false, false, -100000000, 20, 14 }}},
  { "cast(999.999998 as decimal(9,6)) / -999999999",
    {{ false, false, -99999999899, 20, 17 },
     { false, false, -99999999900, 20, 17 }}},
  { "cast(0.999999998 as decimal(9,6)) / -999999999",
    {{ false, false, -99999900, 20, 17 },
     { false, false, -100000000, 20, 17 }}},
  { "cast(0.999999998 as decimal(9,9)) / -999999999",
    {{ false, false, -99999999899, 20, 20 },
     { false, false, -99999999900, 20, 20 }}},
  { "17014118346046923173168730371588.410/17014118346046923173168730371588.410",
    {{ false, false, 1000, 38, 3 },
     { false, false, 1000000, 38, 6 }}},
  { "17014118346046923173168730371588.410/17014118346046923173168730371588.409",
    {{ false, false, 1000, 38, 3 },
     { false, false, 1000000, 38, 6 }}},
  { "17014118346046923173168730371588.410/34028236692093846346337460743176820000",
    {{ false, false, 0, 38, 3 },
     { false, false, 1, 38, 6 }}},
  { "17014118346046923173168730371588.410/34028236692093846346337460743176820001",
    {{ false, false, 0, 38, 3 },
     { false, false, 0, 38, 6 }}},
  { "17014118346046923173168730371588.410/51042355038140769519506191114765230000",
    {{ false, false, 0, 38, 3 },
     { false, false, 0, 38, 6 }}},
  { "17014118346046923173168730371588.410/10208471007628153903901238222953046343",
    {{ false, false, 0, 38, 3 },
     { false, false, 2, 38, 6 }}},
  { "170141183460469231731687303715884.10/34028236692093846346337460743176820000",
    {{ false, false, 0, 38, 2 },
     { false, false, 5, 38, 6 }}},
  { "170141183460469231731687303715884.10/34028236692093846346337460743176820001",
    {{ false, false, 0, 38, 2 },
     { false, false, 5, 38, 6 }}},
  { "170141183460469231731687303715884.10/51042355038140769519506191114765230000",
    {{ false, false, 0, 38, 2 },
     { false, false, 3, 38, 6 }}},
  { "170141183460469231731687303715884.10/10208471007628153903901238222953046343",
    {{ false, false, 0, 38, 2 },
     { false, false, 17, 38, 6 }}},
  { "1701411834604692317316873037158841.0/34028236692093846346337460743176820000",
    {{ false, false, 0, 38, 1 },
     { false, false, 50, 38, 6 }}},
  { "1701411834604692317316873037158841.0/34028236692093846346337460743176820001",
    {{ false, false, 0, 38, 1 },
     { false, false, 50, 38, 6 }}},
  { "1701411834604692317316873037158841.0/51042355038140769519506191114765229999",
    {{ false, false, 0, 38, 1 },
     { false, false, 33, 38, 6 }}},
  { "1701411834604692317316873037158841.0/10208471007628153903901238222953046343",
    {{ false, false, 0, 38, 1 },
     { false, false, 167, 38, 6 }}},
  { "17014118346046923173168730371588410/34028236692093846346337460743176820000",
    {{ false, false, 0, 38, 0 },
     { false, false, 500, 38, 6 }}},
  { "17014118346046923173168730371588410/34028236692093846346337460743176820001",
    {{ false, false, 0, 38, 0 },
     { false, false, 500, 38, 6 }}},
  { "17014118346046923173168730371588410/51042355038140769519506191114765229999",
    {{ false, false, 0, 38, 0 },
     { false, false, 333, 38, 6 }}},
  { "17014118346046923173168730371588410/10208471007628153903901238222953046343",
    {{ false, false, 0, 38, 0 },
     { false, false, 1667, 38, 6 }}},
  { "17014118346046923173168730371588410/3402823669209384634633746074317682000.0",
    {{ false, false, 0, 38, 1 },
     { false, false, 5000, 38, 6 }}},
  { "17014118346046923173168730371588410/3402823669209384634633746074317682000.1",
    {{ false, false, 0, 38, 1 },
     { false, false, 5000, 38, 6 }}},
  { "17014118346046923173168730371588410/5104235503814076951950619111476522999.9",
    {{ false, false, 0, 38, 1 },
     { false, false, 3333, 38, 6 }}},
  { "17014118346046923173168730371588410/1020847100762815390390123822295304634.3",
    {{ false, false, 0, 38, 1 },
     { false, false, 16667, 38, 6 }}},
  { "15014118346046923173168730371588.410/34028236692093846346337460743176820000",
    {{ false, false, 0, 38, 3 },
     { false, false, 0, 38, 6 }}},
  { "150141183460469231731687303715884.10/34028236692093846346337460743176820000",
    {{ false, false, 0, 38, 2 },
     { false, false, 4, 38, 6 }}},
  { "1501411834604692317316873037158841.0/34028236692093846346337460743176820000",
    {{ false, false, 0, 38, 1 },
     { false, false, 44, 38, 6 }}},
  { "15014118346046923173168730371588410/34028236692093846346337460743176820000",
    {{ false, false, 0, 38, 0 },
     { false, false, 441, 38, 6 }}},
  { "16014118346046923173168730371588.410/34028236692093846346337460743176820000",
    {{ false, false, 0, 38, 3 },
     { false, false, 0, 38, 6 }}},
  { "160141183460469231731687303715884.10/34028236692093846346337460743176820000",
    {{ false, false, 0, 38, 2 },
     { false, false, 5, 38, 6 }}},
  { "1601411834604692317316873037158841.0/34028236692093846346337460743176820000",
    {{ false, false, 0, 38, 1 },
     { false, false, 47, 38, 6 }}},
  { "16014118346046923173168730371588410/34028236692093846346337460743176820000",
    {{ false, false, 0, 38, 0 },
     { false, false, 471, 38, 6 }}},
  { "16014118346046923173168730371588410/3402823669209384634633746074317682000",
    {{ false, false, 0, 38, 0 },
     { false, false, 4706, 38, 6 }}},
  { "18014118346046923173168730371588.410/34028236692093846346337460743176820000",
    {{ false, false, 0, 38, 3 },
     { false, false, 1, 38, 6 }}},
  { "180141183460469231731687303715884.10/34028236692093846346337460743176820000",
    {{ false, false, 0, 38, 2 },
     { false, false, 5, 38, 6 }}},
  { "1801411834604692317316873037158841.0/34028236692093846346337460743176820000",
    {{ false, false, 0, 38, 1 },
     { false, false, 53, 38, 6 }}},
  { "18014118346046923173168730371588410/34028236692093846346337460743176820000",
    {{ false, false, 0, 38, 0 },
     { false, false, 529, 38, 6 }}},
  { "18014118346046923173168730371588410/3402823669209384634633746074317682000",
    {{ false, false, 0, 38, 0 },
     { false, false, 5294, 38, 6 }}},
  { "18014118346046923173168730371588410/340282366920938463463374607431768200",
    {{ false, false, 0, 38, 0 },
     { false, false, 52939, 38, 6 }}},
  { "17014118346046923173168730371588.410/-17014118346046923173168730371588.410",
    {{ false, false, -1000, 38, 3 },
     { false, false, -1000000, 38, 6 }}},
  { "17014118346046923173168730371588.410/-17014118346046923173168730371588.409",
    {{ false, false, -1000, 38, 3 },
     { false, false, -1000000, 38, 6 }}},
  { "17014118346046923173168730371588.410/-34028236692093846346337460743176820000",
    {{ false, false, 0, 38, 3 },
     { false, false, -1, 38, 6 }}},
  { "17014118346046923173168730371588.410/-34028236692093846346337460743176820001",
    {{ false, false, 0, 38, 3 },
     { false, false, 0, 38, 6 }}},
  { "17014118346046923173168730371588.410/-51042355038140769519506191114765230000",
    {{ false, false, 0, 38, 3 },
     { false, false, 0, 38, 6 }}},
  { "17014118346046923173168730371588.410/-10208471007628153903901238222953046343",
    {{ false, false, 0, 38, 3 },
     { false, false, -2, 38, 6 }}},
  { "170141183460469231731687303715884.10/-34028236692093846346337460743176820000",
    {{ false, false, 0, 38, 2 },
     { false, false, -5, 38, 6 }}},
  { "170141183460469231731687303715884.10/-34028236692093846346337460743176820001",
    {{ false, false, 0, 38, 2 },
     { false, false, -5, 38, 6 }}},
  { "170141183460469231731687303715884.10/-51042355038140769519506191114765230000",
    {{ false, false, 0, 38, 2 },
     { false, false, -3, 38, 6 }}},
  { "170141183460469231731687303715884.10/-10208471007628153903901238222953046343",
    {{ false, false, 0, 38, 2 },
     { false, false, -17, 38, 6 }}},
  { "1701411834604692317316873037158841.0/-34028236692093846346337460743176820000",
    {{ false, false, 0, 38, 1 },
     { false, false, -50, 38, 6 }}},
  { "1701411834604692317316873037158841.0/-34028236692093846346337460743176820001",
    {{ false, false, 0, 38, 1 },
     { false, false, -50, 38, 6 }}},
  { "1701411834604692317316873037158841.0/-51042355038140769519506191114765229999",
    {{ false, false, 0, 38, 1 },
     { false, false, -33, 38, 6 }}},
  { "1701411834604692317316873037158841.0/-10208471007628153903901238222953046343",
    {{ false, false, 0, 38, 1 },
     { false, false, -167, 38, 6 }}},
  { "17014118346046923173168730371588410/-34028236692093846346337460743176820000",
    {{ false, false, 0, 38, 0 },
     { false, false, -500, 38, 6 }}},
  { "17014118346046923173168730371588410/-34028236692093846346337460743176820001",
    {{ false, false, 0, 38, 0 },
     { false, false, -500, 38, 6 }}},
  { "17014118346046923173168730371588410/-51042355038140769519506191114765229999",
    {{ false, false, 0, 38, 0 },
     { false, false, -333, 38, 6 }}},
  { "17014118346046923173168730371588410/-10208471007628153903901238222953046343",
    {{ false, false, 0, 38, 0 },
     { false, false, -1667, 38, 6 }}},
  { "17014118346046923173168730371588410/-3402823669209384634633746074317682000.0",
    {{ false, false, 0, 38, 1 },
     { false, false, -5000, 38, 6 }}},
  { "17014118346046923173168730371588410/-3402823669209384634633746074317682000.1",
    {{ false, false, 0, 38, 1 },
     { false, false, -5000, 38, 6 }}},
  { "17014118346046923173168730371588410/-5104235503814076951950619111476522999.9",
    {{ false, false, 0, 38, 1 },
     { false, false, -3333, 38, 6 }}},
  { "17014118346046923173168730371588410/-1020847100762815390390123822295304634.3",
    {{ false, false, 0, 38, 1 },
     { false, false, -16667, 38, 6 }}},
  { "15014118346046923173168730371588.410/-34028236692093846346337460743176820000",
    {{ false, false, 0, 38, 3 },
     { false, false, 0, 38, 6 }}},
  { "150141183460469231731687303715884.10/-34028236692093846346337460743176820000",
    {{ false, false, 0, 38, 2 },
     { false, false, -4, 38, 6 }}},
  { "1501411834604692317316873037158841.0/-34028236692093846346337460743176820000",
    {{ false, false, 0, 38, 1 },
     { false, false, -44, 38, 6 }}},
  { "15014118346046923173168730371588410/-34028236692093846346337460743176820000",
    {{ false, false, 0, 38, 0 },
     { false, false, -441, 38, 6 }}},
  { "16014118346046923173168730371588.410/-34028236692093846346337460743176820000",
    {{ false, false, 0, 38, 3 },
     { false, false, 0, 38, 6 }}},
  { "160141183460469231731687303715884.10/-34028236692093846346337460743176820000",
    {{ false, false, 0, 38, 2 },
     { false, false, -5, 38, 6 }}},
  { "1601411834604692317316873037158841.0/-34028236692093846346337460743176820000",
    {{ false, false, 0, 38, 1 },
     { false, false, -47, 38, 6 }}},
  { "16014118346046923173168730371588410/-34028236692093846346337460743176820000",
    {{ false, false, 0, 38, 0 },
     { false, false, -471, 38, 6 }}},
  { "16014118346046923173168730371588410/-3402823669209384634633746074317682000",
    {{ false, false, 0, 38, 0 },
     { false, false, -4706, 38, 6 }}},
  { "18014118346046923173168730371588.410/-34028236692093846346337460743176820000",
    {{ false, false, 0, 38, 3 },
     { false, false, -1, 38, 6 }}},
  { "180141183460469231731687303715884.10/-34028236692093846346337460743176820000",
    {{ false, false, 0, 38, 2 },
     { false, false, -5, 38, 6 }}},
  { "1801411834604692317316873037158841.0/-34028236692093846346337460743176820000",
    {{ false, false, 0, 38, 1 },
     { false, false, -53, 38, 6 }}},
  { "18014118346046923173168730371588410/-34028236692093846346337460743176820000",
    {{ false, false, 0, 38, 0 },
     { false, false, -529, 38, 6 }}},
  { "18014118346046923173168730371588410/-3402823669209384634633746074317682000",
    {{ false, false, 0, 38, 0 },
     { false, false, -5294, 38, 6 }}},
  { "18014118346046923173168730371588410/-340282366920938463463374607431768200",
    {{ false, false, 0, 38, 0 },
     { false, false, -52939, 38, 6 }}},
  // Test modulo operator
  { "cast(1.23 as decimal(8,2)) % cast(1 as decimal(10,3))",
    {{ false, false, 230, 9, 3 }}},
  { "cast(1 as decimal(38,0)) % cast(.2 as decimal(38,1))",
    {{ false, false, 0, 38, 1 }}},
  { "cast(1 as decimal(38,0)) % cast(3 as decimal(38,0))",
    {{ false, false, 1, 38, 0 }}},
  { "cast(-2.12 as decimal(17,2)) % cast(12515.95 as decimal(17,2))",
    {{ false, false, -212, 17, 2 }}},
  { "cast(-2.12 as decimal(18,2)) % cast(12515.95 as decimal(18,2))",
    {{ false, false, -212, 18, 2 }}},
  { "cast(99999999999999999999999999999999999999 as decimal(38,0)) % "
    "cast(99999999999999999999999999999999999999 as decimal(38,0))",
    {{ false, false, 0, 38, 0 }}},
  { "cast(998 as decimal(38,0)) % cast(0.999 as decimal(38,38))",
    {{ false, true, 0, 38, 38 },   // IMPALA-4964 - this should not overflow
     { false, true, 0, 38, 38 }}},
  { "cast(0.998 as decimal(38,38)) % cast(999 as decimal(38,0))",
    {{ false, true, 0, 38, 38 },   // IMPALA-4964 - this should not overflow
     { false, true, 0, 38, 38 }}},
  { "cast(0.00000000000000000000000000000000000001 as decimal(38,38)) % "
    "cast(0.0000000000000000000000000000000000001 as decimal(38,38))",
    {{ false, false, 1, 38, 38 }}},
  // Test MOD builtin
  { "mod(cast('1' as decimal(2,0)), cast('10' as decimal(2,0)))",
    {{ false, false, 1, 2, 0 }}},
  { "mod(cast('1.1' as decimal(2,1)), cast('1.0' as decimal(2,1)))",
    {{ false, false, 1, 2, 1 }}},
  { "mod(cast('-1.23' as decimal(5,2)), cast('1.0' as decimal(5,2)))",
    {{ false, false, -23, 5, 2 }}},
  { "mod(cast('1' as decimal(12,0)), cast('10' as decimal(12,0)))",
    {{ false, false, 1, 12, 0 }}},
  { "mod(cast('1.1' as decimal(12,1)), cast('1.0' as decimal(12,1)))",
    {{ false, false, 1, 12, 1 }}},
  { "mod(cast('-1.23' as decimal(12,2)), cast('1.0' as decimal(12,2)))",
    {{ false, false, -23, 12, 2 }}},
  { "mod(cast('1' as decimal(32,0)), cast('10' as decimal(32,0)))",
    {{ false, false, 1, 32, 0 }}},
  { "mod(cast('1.1' as decimal(32,1)), cast('1.0' as decimal(32,1)))",
    {{ false, false, 1, 32, 1 }}},
  { "mod(cast('-1.23' as decimal(32,2)), cast('1.0' as decimal(32,2)))",
    {{ false, false, -23, 32, 2 }}},
  { "mod(cast(NULL as decimal(2,0)), cast('10' as decimal(2,0)))",
    {{ false, true, 0, 2, 0 }}},
  { "mod(cast('10' as decimal(2,0)), cast(NULL as decimal(2,0)))",
    {{ false, true, 0, 2, 0 }}},
  { "mod(cast('10' as decimal(2,0)), cast('0' as decimal(2,0)))",
    {{ false, true, 0, 2, 0 },
     { true, false, 0, 2, 0 }}},
  { "mod(cast('10' as decimal(2,0)), cast('0' as decimal(2,0)))",
    {{ false, true, 0, 2, 0 },
     { true, false, 0, 2, 0 }}},
  { "cast('10' as decimal(2,0)) % cast('0' as decimal(2,0))",
    {{ false, true, 0, 2, 0 },
     { true, false, 0, 2, 0 }}},
  { "cast('10' as decimal(2,0)) % cast('0' as decimal(38,19))",
    {{ false, true, 0, 21, 19 },
     { true, false, 0, 21, 19 }}},
  { "cast('10' as decimal(2,0)) / cast('0' as decimal(2,0))",
    {{ false, true, 0, 6, 4 },
     { true, false, 0, 6, 4 }}},
  { "cast('10' as decimal(2,0)) / cast('0' as decimal(38,19))",
    {{ false, true, 0, 38, 19 },
     { true, false, 0, 38, 19 }}},
  { "mod(cast(NULL as decimal(2,0)), NULL)", {{ false, true, 0, 2, 0 }}},
  // Test CAST DECIMAL -> DECIMAL
  { "cast(cast(0.12344 as decimal(6,5)) as decimal(6,4))",
    {{ false, false, 1234, 6, 4 }}},
  { "cast(cast(0.12345 as decimal(6,5)) as decimal(6,4))",
    {{ false, false, 1234, 6, 4 },
     { false, false, 1235, 6, 4 }}},
  { "cast(cast('0.999' as decimal(4,3)) as decimal(1,0))",
    {{ false, false, 0, 1, 0 },
     { false, false, 1, 1, 0 }}},
  { "cast(cast(999999999.99 as DECIMAL(11,2)) as DECIMAL(9,0))",
    {{ false, false, 999999999, 9, 0 },
     { false, true, 0, 9, 0 }}},
  { "cast(cast(-999999999.99 as DECIMAL(11,2)) as DECIMAL(9,0))",
    {{ false, false, -999999999, 9, 0 },
     { false, true, 0, 9, 0 }}},
  // IMPALA-2233: Test that implicit casts do not lose precision.
  // The overload greatest(decimal(*,*)) is available and should be used.
  { "greatest(0, cast('99999.1111' as decimal(30,10)))",
    {{ false, false, 999991111000000, 30, 10 },
     { false, false, 999991111000000, 30, 10 }}},
  // Test AVG() with DECIMAL
  { "avg(d) from (values((cast(100000000000000000000000000000000.00000 as DECIMAL(38,5)) "
    "as d))) as t",
    {{ false, false, static_cast<int128_t>(10000000ll) *
       10000000000ll * 10000000000ll * 10000000000ll, 38, 5 },
     { false, true, 0, 38, 6}}},
  { "avg(d) from (values((cast(1234567890 as DECIMAL(10,0)) as d))) as t",
    {{ false, false, 1234567890, 10, 0},
     { false, false, 1234567890000000, 16, 6}}},
  { "avg(d) from (values((cast(1234567.89 as DECIMAL(10,2)) as d))) as t",
    {{ false, false, 123456789, 10, 2},
     { false, false, 1234567890000, 14, 6}}},
  { "avg(d) from (values((cast(10000000000000000000000000000000 as DECIMAL(32,0)) "
    "as d))) as t",
    {{ false, false, static_cast<int128_t>(10) *
      10000000000ll * 10000000000ll * 10000000000ll, 32, 0},
     { false, false, static_cast<int128_t>(10000000) *
      10000000000ll * 10000000000ll * 10000000000ll, 38, 6}}},
  { "avg(d) from (values((cast(100000000000000000000000000000000 as DECIMAL(33,0)) "
    "as d))) as t",
    {{ false, false, static_cast<int128_t>(100) *
      10000000000ll * 10000000000ll * 10000000000ll, 33, 0},
     { false, true, 0, 38, 6}}},
  { "avg(d) from (values((cast(100000000000000000000000000000000.0 as DECIMAL(34,1)) "
    "as d))) as t",
    {{ false, false, static_cast<int128_t>(1000) *
      10000000000ll * 10000000000ll * 10000000000ll, 34, 1},
     { false, true, 0, 38, 6}}},
  { "avg(d) from (values((cast(100000000000000000000000000000000.00000 as DECIMAL(38,5)) "
    "as d))) as t",
    {{ false, false, static_cast<int128_t>(10000000) *
      10000000000ll * 10000000000ll * 10000000000ll, 38, 5},
     { false, true, 0, 38, 6}}},
  { "avg(d) from (values((cast(10000000000000000000000000000000.000000 as DECIMAL(38,6)) "
    "as d))) as t",
    {{ false, false, static_cast<int128_t>(10000000) *
      10000000000ll * 10000000000ll * 10000000000ll, 38, 6}}},
  { "avg(d) from (values((cast("
    "0.10000000000000000000000000000000000000 as DECIMAL(38,38)) as d))) as t",
    {{ false, false, static_cast<int128_t>(10000000) *
      10000000000ll * 10000000000ll * 10000000000ll, 38, 38}}},
  // Test CAST DECIMAL -> INT
  { "cast(cast(0.5999999 AS tinyint) AS decimal(10,6))",
    {{ false, false, 0, 10, 6 },
     { false, false, 1000000, 10, 6 }}},
  { "cast(cast(99999999.4999999 AS int) AS decimal(10,2))",
    {{ false, false, 9999999900, 10, 2 }}},
  { "cast(cast(99999999.5999999 AS int) AS decimal(10,2))",
    {{ false, false, 9999999900, 10, 2 },
     { false, true, 0, 10, 2 }}},
  { "cast(cast(10000.5999999 as int) as decimal(30,6))",
    {{ false, false, 10000000000, 30, 6 },
     { false, false, 10001000000, 30, 6 }}},
  { "cast(cast(10000.5 AS int) AS decimal(6,1))",
    {{ false, false, 100000, 6, 1 },
     { false, false, 100010, 6, 1 }}},
  { "cast(cast(-10000.5 AS int) AS decimal(6,1))",
    {{ false, false, -100000, 6, 1 },
     { false, false, -100010, 6, 1 }}},
  { "cast(cast(9999.5 AS int) AS decimal(4,0))",
    {{ false, false, 9999, 4, 0 },
     { false, true, 0, 4, 0 }}},
  { "cast(cast(-9999.5 AS int) AS decimal(4,0))",
    {{ false, false, -9999, 4, 0 },
     { false, true, 0, 4, 0 }}},
  { "cast(cast(127.4999 AS tinyint) AS decimal(30,0))",
    {{ false, false, 127, 30, 0 }}},
  { "cast(cast(127.5 AS tinyint) AS decimal(30,0))",
    {{ false, false, 127, 30, 0 },
     { false, true, 0, 30, 0 }}},
  { "cast(cast(128.0 AS tinyint) AS decimal(30,0))",
    {{ false, false, -128, 30, 0 }, // BUG: JIRA: IMPALA-865
     { false, true, 0, 30, 0 }}},
  { "cast(cast(-128.4999 AS tinyint) AS decimal(30,0))",
    {{ false, false, -128, 30, 0 }}},
  { "cast(cast(-128.5 AS tinyint) AS decimal(30,0))",
    {{ false, false, -128, 30, 0 },
     { false, true, 0, 30, 0 }}},
  { "cast(cast(-129.0 AS tinyint) AS decimal(30,0))",
    {{ false, false, 127, 30, 0 }, // BUG: JIRA: IMPALA-865
     { false, true, 0, 30, 0 }}},
  { "cast(cast(32767.4999 AS smallint) AS decimal(30,0))",
    {{ false, false, 32767, 30, 0 }}},
  { "cast(cast(32767.5 AS smallint) AS decimal(30,0))",
    {{ false, false, 32767, 30, 0 },
     { false, true, 0, 30, 0 }}},
  { "cast(cast(32768.0 AS smallint) AS decimal(30,0))",
    {{ false, false, -32768, 30, 0 }, // BUG: JIRA: IMPALA-865
     { false, true, 0, 30, 0 }}},
  { "cast(cast(-32768.4999 AS smallint) AS decimal(30,0))",
    {{ false, false, -32768, 30, 0 }}},
  { "cast(cast(-32768.5 AS smallint) AS decimal(30,0))",
    {{ false, false, -32768, 30, 0 },
     { false, true, 0, 30, 0 }}},
  { "cast(cast(-32769.0 AS smallint) AS decimal(30,0))",
    {{ false, false, 32767, 30, 0 }, // BUG: JIRA: IMPALA-865
     { false, true, 0, 30, 0 }}},
  { "cast(cast(2147483647.4999 AS int) AS decimal(30,0))",
    {{ false, false, 2147483647, 30, 0 }}},
  { "cast(cast(2147483647.5 AS int) AS decimal(30,0))",
    {{ false, false, 2147483647, 30, 0 },
     { false, true, 0, 30, 0 }}},
  { "cast(cast(2147483648.0 AS int) AS decimal(30,0))",
    {{ false, false, -2147483648, 30, 0 }, // BUG: JIRA: IMPALA-865
     { false, true, 0, 30, 0 }}},
  { "cast(cast(-2147483648.4999 AS int) AS decimal(30,0))",
    {{ false, false, -2147483648, 30, 0 }}},
  { "cast(cast(-2147483648.5 AS int) AS decimal(30,0))",
    {{ false, false, -2147483648, 30, 0 },
     { false, true, 0, 30, 0 }}},
  { "cast(cast(-2147483649.0 AS int) AS decimal(30,0))",
    {{ false, false, 2147483647, 30, 0 }, // BUG: JIRA: IMPALA-865
     { false, true, 0, 30, 0 }}},
  { "cast(cast(9223372036854775807.4999 AS bigint) AS decimal(30,0))",
    {{ false, false, 9223372036854775807, 30, 0 }}},
  { "cast(cast(9223372036854775807.5 AS bigint) AS decimal(30,0))",
    {{ false, false, 9223372036854775807, 30, 0 },
     { false, true, 0, 30, 0 }}},
  { "cast(cast(9223372036854775808.0 AS bigint) AS decimal(30,0))",
    // BUG; also GCC workaround with -1
    {{ false, false, -9223372036854775807 - 1, 30, 0 },
     // error: integer constant is so large that it is unsigned
     { false, true, 0, 30, 0 }}},
  { "cast(cast(-9223372036854775808.4999 AS bigint) AS decimal(30,0))",
    {{ false, false, -9223372036854775807 - 1, 30, 0 }}},
  { "cast(cast(-9223372036854775808.5 AS bigint) AS decimal(30,0))",
    {{ false, false, -9223372036854775807 - 1, 30, 0 },
     { false, true, 0, 30, 0 }}},
  { "cast(cast(-9223372036854775809.0 AS bigint) AS decimal(30,0))",
    {{ false, false, 9223372036854775807, 30, 0 }, // BUG: JIRA: IMPALA-865
     { false, true, 0, 30, 0 }}},
  { "cast(cast(cast(pow(1, -38) as decimal(38,38)) as bigint) as decimal(18,10))",
    {{ false, false, 0, 18, 10 },
     { false, false, 10000000000, 18, 10 }}},
  { "cast(cast(cast(-pow(1, -38) as decimal(38,38)) as bigint) as decimal(18,10))",
    {{ false, false, 0, 18, 10 },
     { false, false, -10000000000, 18, 10 }}},
  // Test CAST FLOAT -> DECIMAL
  { "cast(cast(power(10, 3) - power(10, -1) as float) as decimal(4,1))",
    {{ false, false, 9999, 4, 1 }}},
  { "cast(cast(power(10, 3) - power(10, -2) as float) as decimal(5,1))",
    {{ false, false, 9999, 5, 1 },
     { false, false, 10000, 5, 1 }}},
  { "cast(cast(power(10, 3) - power(10, -2) as float) as decimal(4,1))",
    {{ false, false, 9999, 4, 1 },
     { false, true, 0, 4, 1 }}},
  { "cast(cast(-power(10, 3) + power(10, -1) as float) as decimal(4,1))",
    {{ false, false, -9999, 4, 1 }}},
  { "cast(cast(-power(10, 3) + power(10, -2) as float) as decimal(5,1))",
    {{ false, false, -9999, 5, 1 },
     { false, false, -10000, 5, 1 }}},
  { "cast(cast(-power(10, 3) + power(10, -2) as float) as decimal(4,1))",
    {{ false, false, -9999, 4, 1 },
     { false, true, 0, 4, 1 }}},
  { "cast(cast(power(10, 3) - 0.45 as double) as decimal(4,1))",
    {{ false, false, 9995, 4, 1 },
     { false, false, 9996, 4, 1 }}},
  { "cast(cast(power(10, 3) - 0.45 as double) as decimal(5,2))",
    {{ false, false, 99955, 5, 2 }}},
  { "cast(cast(power(10, 3) - 0.45 as double) as decimal(5,0))",
    {{ false, false, 999, 5, 0 },
     { false, false, 1000, 5, 0 }}},
  { "cast(cast(power(10, 3) - 0.45 as double) as decimal(3,0))",
    {{ false, false, 999, 3, 0 },
     { false, true, 0, 3, 0 }}},
  { "cast(cast(-power(10, 3) + 0.45 as double) as decimal(4,1))",
    {{ false, false, -9995, 4, 1 },
     { false, false, -9996, 4, 1 }}},
  { "cast(cast(-power(10, 3) + 0.45 as double) as decimal(5,2))",
    {{ false, false, -99955, 5, 2 }}},
  { "cast(cast(-power(10, 3) + 0.45 as double) as decimal(5,0))",
    {{ false, false, -999, 5, 0 },
     { false, false, -1000, 5, 0 }}},
  { "cast(cast(-power(10, 3) + 0.45 as double) as decimal(3,0))",
    {{ false, false, -999, 3, 0 },
     { false, true, 0, 3, 0 }}},
  { "cast(cast(power(10, 3) - 0.5 as double) as decimal(4,1))",
    {{ false, false, 9995, 4, 1 }}},
  { "cast(cast(power(10, 3) - 0.5 as double) as decimal(5,2))",
    {{ false, false, 99950, 5, 2 }}},
  { "cast(cast(power(10, 3) - 0.5 as double) as decimal(5,0))",
    {{ false, false, 999, 5, 0 },
     { false, false, 1000, 5, 0 }}},
  { "cast(cast(power(10, 3) - 0.5 as double) as decimal(3,0))",
    {{ false, false, 999, 3, 0 },
     { false, true, 0, 3, 0 }}},
  { "cast(cast(-power(10, 3) + 0.5 as double) as decimal(4,1))",
    {{ false, false, -9995, 4, 1 }}},
  { "cast(cast(-power(10, 3) + 0.5 as double) as decimal(5,2))",
    {{ false, false, -99950, 5, 2 }}},
  { "cast(cast(-power(10, 3) + 0.5 as double) as decimal(5,0))",
    {{ false, false, -999, 5, 0 },
     { false, false, -1000, 5, 0 }}},
  { "cast(cast(-power(10, 3) + 0.5 as double) as decimal(3,0))",
    {{ false, false, -999, 3, 0 },
     { false, true, 0, 3, 0 }}},
  { "cast(cast(power(10, 3) - 0.55 as double) as decimal(4,1))",
    {{ false, false, 9994, 4, 1 },
     { false, false, 9995, 4, 1 }}},
  { "cast(cast(power(10, 3) - 0.55 as double) as decimal(5,2))",
    {{ false, false, 99945, 5, 2 }}},
  { "cast(cast(power(10, 3) - 0.55 as double) as decimal(5,0))",
    {{ false, false, 999, 5, 0 }}},
  { "cast(cast(power(10, 3) - 0.55 as double) as decimal(3,0))",
    {{ false, false, 999, 3, 0 }}},
  { "cast(cast(-power(10, 3) + 0.55 as double) as decimal(4,1))",
    {{ false, false, -9994, 4, 1 },
     { false, false, -9995, 4, 1 }}},
  { "cast(cast(-power(10, 3) + 0.55 as double) as decimal(5,2))",
    {{ false, false, -99945, 5, 2 }}},
  { "cast(cast(-power(10, 3) + 0.55 as double) as decimal(5,0))",
    {{ false, false, -999, 5, 0 }}},
  { "cast(cast(-power(10, 3) + 0.55 as double) as decimal(3,0))",
    {{ false, false, -999, 3, 0 }}},
  { "cast(power(2, 1023) * 100 as decimal(38,0))",
    {{ false, true, 0, 38, 0 }}},
  { "cast(power(2, 1023) * 100 as decimal(18,0))",
    {{ false, true, 0, 18, 0 }}},
  { "cast(power(2, 1023) * 100 as decimal(9,0))",
    {{ false, true, 0, 9, 0 }}},
  { "cast(0/0 as decimal(38,0))",
    {{ false, true, 0, 38, 0 }}},
  { "cast(0/0 as decimal(18,0))",
    {{ false, true, 0, 18, 0 }}},
  { "cast(0/0 as decimal(9,0))",
    {{ false, true, 0, 9, 0 }}},
  // 39 5's - legal double but will overflow in decimal
  { "cast(555555555555555555555555555555555555555 as decimal(38,0))",
    {{ false, true, 0, 38, 0 }}},
};

TEST_F(ExprTest, DecimalArithmeticExprs) {
  // Test with both decimal_v2={false, true}
  for (int v2: { 0, 1 }) {
    string opt = "DECIMAL_V2=" + lexical_cast<string>(v2);
    executor_->PushExecOption(opt);
    for (const DecimalTestCase& c : decimal_cases) {
      const DecimalExpectedResult& r = c.Expected(v2);
      const ColumnType& type = ColumnType::CreateDecimalType(r.precision, r.scale);
      if (r.error) {
        TestError(c.expr);
      } else if (r.null) {
        TestDecimalResultType(c.expr, type);
        TestIsNull(c.expr, type);
      } else {
        TestIsNotNull(c.expr, type);
        switch (type.GetByteSize()) {
          case 4:
            TestDecimalValue(c.expr, Decimal4Value(r.scaled_val), type);
            break;
          case 8:
            TestDecimalValue(c.expr, Decimal8Value(r.scaled_val), type);
            break;
          case 16:
            TestDecimalValue(c.expr, Decimal16Value(r.scaled_val), type);
            break;
          default:
            DCHECK(false) << type << " " << type.GetByteSize();
        }
      }
    }
    executor_->PopExecOption();
  }
}

// Tests for expressions that mix decimal and non-decimal arguments with DECIMAL_V2=false.
TEST_F(ExprTest, DecimalV1MixedArithmeticExprs) {
  executor_->PushExecOption("DECIMAL_V2=false");
  // IMPALA-3437: decimal constants are implicitly converted to double.
  TestValue("10.0 + 3", TYPE_DOUBLE, 13.0);
  TestValue("10 + 3.0", TYPE_DOUBLE, 13.0);
  TestValue("10.0 - 3", TYPE_DOUBLE, 7.0);
  TestValue("10.0 * 3", TYPE_DOUBLE, 30.0);
  TestValue("10.0 / 3", TYPE_DOUBLE, 10.0 / 3);
  // Conversion to DOUBLE loses some precision.
  TestValue("0.999999999999999999999999999999 = 1", TYPE_BOOLEAN, true);
  TestValue("0.999999999999999999999999999999 != 1", TYPE_BOOLEAN, false);
  TestValue("0.999999999999999999999999999999 < 1", TYPE_BOOLEAN, false);
  TestValue("0.999999999999999999999999999999 >= 1", TYPE_BOOLEAN, true);
  TestValue("0.999999999999999999999999999999 > 1", TYPE_BOOLEAN, false);
  executor_->PopExecOption();
}

// Tests the same expressions as above with DECIMAL_V2=true.
TEST_F(ExprTest, DecimalV2MixedArithmeticExprs) {
  executor_->PushExecOption("DECIMAL_V2=true");
  // IMPALA-3437: decimal constants remain decimal.
  TestDecimalValue(
      "10.0 + 3", Decimal4Value(130), ColumnType::CreateDecimalType(5, 1));
  TestDecimalValue(
      "10 + 3.0", Decimal4Value(130), ColumnType::CreateDecimalType(5, 1));
  TestDecimalValue(
      "10.0 - 3", Decimal4Value(70), ColumnType::CreateDecimalType(5, 1));
  TestDecimalValue(
      "10.0 * 3", Decimal4Value(300), ColumnType::CreateDecimalType(7, 1));
  TestDecimalValue(
      "10.0 / 3", Decimal4Value(3333333), ColumnType::CreateDecimalType(8, 6));
  // Comparisons between DECIMAL values are precise.
  TestValue("0.999999999999999999999999999999 = 1", TYPE_BOOLEAN, false);
  TestValue("0.999999999999999999999999999999 != 1", TYPE_BOOLEAN, true);
  TestValue("0.999999999999999999999999999999 < 1", TYPE_BOOLEAN, true);
  TestValue("0.999999999999999999999999999999 >= 1", TYPE_BOOLEAN, false);
  TestValue("0.999999999999999999999999999999 > 1", TYPE_BOOLEAN, false);
  executor_->PopExecOption();
}

// There are two tests of ranges, the second of which requires a cast
// of the second operand to a higher-resolution type.
TEST_F(ExprTest, BinaryPredicates) {
  TestComparison("false", "true", false);
  TestBinaryPredicates("false", false);
  TestBinaryPredicates("true", false);
  TestFixedPointComparisons<int8_t>(true);
  TestFixedPointComparisons<int16_t>(true);
  TestFixedPointComparisons<int32_t>(true);
  TestFixedPointComparisons<int64_t>(false);
  TestFloatingPointComparisons<float>(true);
  TestFloatingPointComparisons<double>(false);
  TestStringComparisons();
  TestDecimalComparisons();
  TestNullComparisons();
  TestDistinctFrom();
}

// Test casting from all types to all other types
TEST_F(ExprTest, CastExprs) {
  // From tinyint
  TestCast("cast(0 as tinyint)", 0);
  TestCast("cast(5 as tinyint)", 5);
  TestCast("cast(-5 as tinyint)", -5);

  // From smallint
  TestCast("cast(0 as smallint)", 0);
  TestCast("cast(5 as smallint)", 5);
  TestCast("cast(-5 as smallint)", -5);

  // From int
  TestCast("cast(0 as int)", 0);
  TestCast("cast(5 as int)", 5);
  TestCast("cast(-5 as int)", -5);

  // From bigint
  TestCast("cast(0 as bigint)", 0);
  TestCast("cast(5 as bigint)", 5);
  TestCast("cast(-5 as bigint)", -5);

  // From boolean
  TestCast("cast(0 as boolean)", 0);
  TestCast("cast(5 as boolean)", 1);
  TestCast("cast(-5 as boolean)", 1);

  // From Float
  TestCast("cast(0.0 as float)", 0.0f);
  TestCast("cast(5.0 as float)", 5.0f);
  TestCast("cast(-5.0 as float)", -5.0f);
  TestCast("cast(0.1234567890123 as float)", 0.1234567890123f);
  TestCast("cast(0.1234567890123 as float)", 0.123456791f); // same as above
  TestCast("cast(0.00000000001234567890123 as float)", 0.00000000001234567890123f);
  TestCast("cast(123456 as float)", 123456.0f);

  // From http://en.wikipedia.org/wiki/Single-precision_floating-point_format
  // Min positive normal value
  TestCast("cast(1.1754944e-38 as float)", 1.1754944e-38f);
  // Max representable value
  TestCast("cast(3.4028234e38 as float)", 3.4028234e38f, true);


  // From Double
  TestCast("cast(0.0 as double)", 0.0);
  TestCast("cast(5.0 as double)", 5.0);
  TestCast("cast(-5.0 as double)", -5.0);
  TestCast("cast(0.123e10 as double)", 0.123e10);
  TestCast("cast(123.123e10 as double)", 123.123e10, true);
  TestCast("cast(1.01234567890123456789 as double)", 1.01234567890123456789);
  TestCast("cast(1.01234567890123456789 as double)", 1.0123456789012346); // same as above
  TestCast("cast(0.01234567890123456789 as double)", 0.01234567890123456789);
  TestCast("cast(0.1234567890123456789 as double)", 0.1234567890123456789);
  TestCast("cast(-2.2250738585072020E-308 as double)", -2.2250738585072020e-308);

  // From http://en.wikipedia.org/wiki/Double-precision_floating-point_format
  // Min subnormal positive double
  TestCast("cast(4.9406564584124654e-324 as double)", 4.9406564584124654e-324);
  // Max subnormal double
  TestCast("cast(2.2250738585072009e-308 as double)", 2.2250738585072009e-308);
  // Min normal positive double
  TestCast("cast(2.2250738585072014e-308 as double)", 2.2250738585072014e-308);
  // Max Double
  TestCast("cast(1.7976931348623157e+308 as double)", 1.7976931348623157e308, true);

  // From String
  TestCast("'0'", "0");
  TestCast("'5'", "5");
  TestCast("'-5'", "-5");
  TestStringValue("cast(\"abc\" as string)", "abc");

  // From Timestamp to Timestamp
  TestStringValue("cast(cast(cast('2012-01-01 09:10:11.123456789' as timestamp) as"
      " timestamp) as string)", "2012-01-01 09:10:11.123456789");

  // IMPALA-3163: Test precise conversion from Decimal to Timestamp.
  TestTimestampValue("cast(cast(1457473016.1230 as decimal(17,4)) as timestamp)",
      TimestampValue::Parse("2016-03-08 21:36:56.123000000", 29));
  // 32 bit Decimal.
  TestTimestampValue("cast(cast(123.45 as decimal(9,2)) as timestamp)",
      TimestampValue::Parse("1970-01-01 00:02:03.450000000", 29));
  TestTimestampValue("cast(cast(-123.45 as decimal(9,2)) as timestamp)",
      TimestampValue::Parse("1969-12-31 23:57:56.550000000", 29));
  // 64 bit Decimal.
  TestTimestampValue("cast(cast(123.45 as decimal(18,2)) as timestamp)",
      TimestampValue::Parse("1970-01-01 00:02:03.450000000", 29));
  TestTimestampValue("cast(cast(-123.45 as decimal(18,2)) as timestamp)",
      TimestampValue::Parse("1969-12-31 23:57:56.550000000", 29));
  TestTimestampValue("cast(cast(-0.1 as decimal(18,10)) as timestamp)",
      TimestampValue::Parse("1969-12-31 23:59:59.900000000", 29));
  TestTimestampValue("cast(cast(253402300799.99 as decimal(18, 2)) as timestamp)",
      TimestampValue::Parse("9999-12-31 23:59:59.990000000", 29));
  TestIsNull("cast(cast(260000000000.00 as decimal(18, 2)) as timestamp)",
      TYPE_TIMESTAMP);
  // 128 bit Decimal.
  TestTimestampValue("cast(cast(123.45 as decimal(38,2)) as timestamp)",
      TimestampValue::Parse("1970-01-01 00:02:03.450000000", 29));
  TestTimestampValue("cast(cast(-123.45 as decimal(38,2)) as timestamp)",
      TimestampValue::Parse("1969-12-31 23:57:56.550000000", 29));
  TestTimestampValue("cast(cast(-0.1 as decimal(38,20)) as timestamp)",
      TimestampValue::Parse("1969-12-31 23:59:59.900000000", 29));
  TestTimestampValue("cast(cast(253402300799.99 as decimal(38, 2)) as timestamp)",
      TimestampValue::Parse("9999-12-31 23:59:59.990000000", 29));
  TestTimestampValue("cast(cast(253402300799.99 as decimal(38, 26)) as timestamp)",
      TimestampValue::Parse("9999-12-31 23:59:59.990000000", 29));
  TestIsNull("cast(cast(260000000000.00 as decimal(38, 2)) as timestamp)",
      TYPE_TIMESTAMP);
  // numeric_limits<int64_t>::max()
  TestIsNull("cast(cast(9223372036854775807 as decimal(38, 0)) as timestamp)",
      TYPE_TIMESTAMP);
  // numeric_limits<int64_t>::max() + 1
  TestIsNull("cast(cast(9223372036854775808 as decimal(38, 0)) as timestamp)",
      TYPE_TIMESTAMP);
  // numeric_limits<int64_t>::min()
  TestIsNull("cast(cast(-9223372036854775808 as decimal(38, 0)) as timestamp)",
      TYPE_TIMESTAMP);
  // numeric_limits<int64_t>::min() - 1
  TestIsNull("cast(cast(-9223372036854775809 as decimal(38, 0)) as timestamp)",
      TYPE_TIMESTAMP);
  // 2^70 + 1
  TestIsNull("cast(cast(1180591620717411303425 as decimal(38, 0)) as timestamp)",
      TYPE_TIMESTAMP);

  // Out of range String <--> Timestamp - invalid boundary cases.
  TestIsNull("cast('1399-12-31 23:59:59' as timestamp)", TYPE_TIMESTAMP);
  TestIsNull("cast('10000-01-01 00:00:00' as timestamp)", TYPE_TIMESTAMP);

  // Timestamp <--> Int
  TestIsNull("cast(cast('09:10:11.000000' as timestamp) as int)", TYPE_INT);
  TestValue("cast(cast('2000-01-01' as timestamp) as int)", TYPE_INT, 946684800);
  // Check that casting to a TINYINT gives the same result as if the Unix time were
  // cast instead.
  TestValue("cast(cast('2000-01-01' as timestamp) as tinyint)", TYPE_TINYINT, -128);
  TestValue("cast(946684800 as tinyint)", TYPE_TINYINT, -128);
  TestValue("cast(cast('2000-01-01 09:10:11.000000' as timestamp) as int)", TYPE_INT,
      946717811);
  TestTimestampValue("cast(946717811 as timestamp)",
      TimestampValue::Parse("2000-01-01 09:10:11", 19));

  // Timestamp <--> Int conversions boundary cases
  TestValue("cast(cast('1400-01-01 00:00:00' as timestamp) as bigint)",
      TYPE_BIGINT, -17987443200);
  TestTimestampValue("cast(-17987443200 as timestamp)",
      TimestampValue::Parse("1400-01-01 00:00:00", 19));
  TestIsNull("cast(-17987443201 as timestamp)", TYPE_TIMESTAMP);
  TestValue("cast(cast('9999-12-31 23:59:59' as timestamp) as bigint)",
      TYPE_BIGINT, 253402300799);
  TestTimestampValue("cast(253402300799 as timestamp)",
      TimestampValue::Parse("9999-12-31 23:59:59", 19));
  TestIsNull("cast(253402300800 as timestamp)", TYPE_TIMESTAMP);

  // Timestamp <--> Float
  TestIsNull("cast(cast('09:10:11.000000' as timestamp) as float)", TYPE_FLOAT);
  TestValue("cast(cast('2000-01-01' as timestamp) as double)", TYPE_DOUBLE, 946684800);
  TestValue("cast(cast('2000-01-01' as timestamp) as float)", TYPE_FLOAT, 946684800);
  TestValue("cast(cast('2000-01-01 09:10:11.720000' as timestamp) as double)",
      TYPE_DOUBLE, 946717811.72);
  TestTimestampValue("cast(cast(946717811.033 as double) as timestamp)",
      TimestampValue::Parse("2000-01-01 09:10:11.032999992", 29));
  TestValue("cast(cast('1400-01-01' as timestamp) as double)", TYPE_DOUBLE,
      -17987443200);
  TestIsNull("cast(cast(-17987443201.03 as double) as timestamp)", TYPE_TIMESTAMP);
  TestTimestampValue("cast(253402300799 as timestamp)",
      TimestampValue::Parse("9999-12-31 23:59:59", 19));
  TestIsNull("cast(253433923200 as timestamp)", TYPE_TIMESTAMP);
  TestIsNull("cast(cast(null as bigint) as timestamp)", TYPE_TIMESTAMP);
  TestIsNull("cast(cast(null as timestamp) as bigint)", TYPE_BIGINT);

#if 0
  // Test overflow.  TODO: Hive casting rules are very weird here also.  It seems for
  // types < TYPE_INT, it will just overflow and keep the low order bits.  For TYPE_INT,
  // it caps it and int_max/int_min.  Is this what we want to do?
  int val = 10000000;
  TestValue<int8_t>("cast(10000000 as tinyint)", TYPE_TINYINT, val & 0xff);
  TestValue<int8_t>("cast(-10000000 as tinyint)", TYPE_TINYINT, -val & 0xff);
  TestValue<int16_t>("cast(10000000 as smallint)", TYPE_SMALLINT, val & 0xffff);
  TestValue<int16_t>("cast(-10000000 as smallint)", TYPE_SMALLINT, -val & 0xffff);
#endif
}

TEST_F(ExprTest, CompoundPredicates) {
  TestValue("TRUE AND TRUE", TYPE_BOOLEAN, true);
  TestValue("TRUE AND FALSE", TYPE_BOOLEAN, false);
  TestValue("FALSE AND TRUE", TYPE_BOOLEAN, false);
  TestValue("FALSE AND FALSE", TYPE_BOOLEAN, false);
  TestValue("TRUE && TRUE", TYPE_BOOLEAN, true);
  TestValue("TRUE && FALSE", TYPE_BOOLEAN, false);
  TestValue("FALSE && TRUE", TYPE_BOOLEAN, false);
  TestValue("FALSE && FALSE", TYPE_BOOLEAN, false);
  TestValue("TRUE OR TRUE", TYPE_BOOLEAN, true);
  TestValue("TRUE OR FALSE", TYPE_BOOLEAN, true);
  TestValue("FALSE OR TRUE", TYPE_BOOLEAN, true);
  TestValue("FALSE OR FALSE", TYPE_BOOLEAN, false);
  TestValue("TRUE || TRUE", TYPE_BOOLEAN, true);
  TestValue("TRUE || FALSE", TYPE_BOOLEAN, true);
  TestValue("FALSE || TRUE", TYPE_BOOLEAN, true);
  TestValue("FALSE || FALSE", TYPE_BOOLEAN, false);
  TestValue("NOT TRUE", TYPE_BOOLEAN, false);
  TestValue("NOT FALSE", TYPE_BOOLEAN, true);
  TestValue("!TRUE", TYPE_BOOLEAN, false);
  TestValue("!FALSE", TYPE_BOOLEAN, true);
  TestValue("TRUE AND (TRUE OR FALSE)", TYPE_BOOLEAN, true);
  TestValue("(TRUE AND TRUE) OR FALSE", TYPE_BOOLEAN, true);
  TestValue("(TRUE OR FALSE) AND TRUE", TYPE_BOOLEAN, true);
  TestValue("TRUE OR (FALSE AND TRUE)", TYPE_BOOLEAN, true);
  TestValue("TRUE AND TRUE OR FALSE", TYPE_BOOLEAN, true);
  TestValue("TRUE && (TRUE || FALSE)", TYPE_BOOLEAN, true);
  TestValue("(TRUE && TRUE) || FALSE", TYPE_BOOLEAN, true);
  TestValue("(TRUE || FALSE) && TRUE", TYPE_BOOLEAN, true);
  TestValue("TRUE || (FALSE && TRUE)", TYPE_BOOLEAN, true);
  TestValue("TRUE && TRUE || FALSE", TYPE_BOOLEAN, true);
  TestIsNull("TRUE AND NULL", TYPE_BOOLEAN);
  TestIsNull("NULL AND TRUE", TYPE_BOOLEAN);
  TestValue("FALSE AND NULL", TYPE_BOOLEAN, false);
  TestValue("NULL AND FALSE", TYPE_BOOLEAN, false);
  TestIsNull("NULL AND NULL", TYPE_BOOLEAN);
  TestValue("TRUE OR NULL", TYPE_BOOLEAN, true);
  TestValue("NULL OR TRUE", TYPE_BOOLEAN, true);
  TestIsNull("FALSE OR NULL", TYPE_BOOLEAN);
  TestIsNull("NULL OR FALSE", TYPE_BOOLEAN);
  TestIsNull("NULL OR NULL", TYPE_BOOLEAN);
  TestIsNull("NOT NULL", TYPE_BOOLEAN);
  TestIsNull("TRUE && NULL", TYPE_BOOLEAN);
  TestValue("FALSE && NULL", TYPE_BOOLEAN, false);
  TestIsNull("NULL && NULL", TYPE_BOOLEAN);
  TestValue("TRUE || NULL", TYPE_BOOLEAN, true);
  TestIsNull("FALSE || NULL", TYPE_BOOLEAN);
  TestIsNull("NULL || NULL", TYPE_BOOLEAN);
  TestIsNull("!NULL", TYPE_BOOLEAN);
}

TEST_F(ExprTest, IsNullPredicate) {
  TestValue("5 IS NULL", TYPE_BOOLEAN, false);
  TestValue("5 IS NOT NULL", TYPE_BOOLEAN, true);
  TestValue("NULL IS NULL", TYPE_BOOLEAN, true);
  TestValue("NULL IS NOT NULL", TYPE_BOOLEAN, false);
}

TEST_F(ExprTest, BoolTestExpr) {
  // Tests against constants.
  TestValue("TRUE IS TRUE", TYPE_BOOLEAN, true);
  TestValue("TRUE IS FALSE", TYPE_BOOLEAN, false);
  TestValue("TRUE IS UNKNOWN", TYPE_BOOLEAN, false);
  TestValue("TRUE IS NOT TRUE", TYPE_BOOLEAN, false);
  TestValue("TRUE IS NOT FALSE", TYPE_BOOLEAN, true);
  TestValue("TRUE IS NOT UNKNOWN", TYPE_BOOLEAN, true);
  TestValue("FALSE IS TRUE", TYPE_BOOLEAN, false);
  TestValue("FALSE IS FALSE", TYPE_BOOLEAN, true);
  TestValue("FALSE IS UNKNOWN", TYPE_BOOLEAN, false);
  TestValue("FALSE IS NOT TRUE", TYPE_BOOLEAN, true);
  TestValue("FALSE IS NOT FALSE", TYPE_BOOLEAN, false);
  TestValue("FALSE IS NOT UNKNOWN", TYPE_BOOLEAN, true);
  TestValue("NULL IS TRUE", TYPE_BOOLEAN, false);
  TestValue("NULL IS FALSE", TYPE_BOOLEAN, false);
  TestValue("NULL IS UNKNOWN", TYPE_BOOLEAN, true);
  TestValue("NULL IS NOT TRUE", TYPE_BOOLEAN, true);
  TestValue("NULL IS NOT FALSE", TYPE_BOOLEAN, true);
  TestValue("NULL IS NOT UNKNOWN", TYPE_BOOLEAN, false);

  // Tests against expressions
  TestValue("(2>1) IS TRUE", TYPE_BOOLEAN, true);
  TestValue("(2>1) IS FALSE", TYPE_BOOLEAN, false);
  TestValue("(2>1) IS UNKNOWN", TYPE_BOOLEAN, false);
  TestValue("(2>1) IS NOT TRUE", TYPE_BOOLEAN, false);
  TestValue("(2>1) IS NOT FALSE", TYPE_BOOLEAN, true);
  TestValue("(2>1) IS NOT UNKNOWN", TYPE_BOOLEAN, true);
  TestValue("(1>2) IS TRUE", TYPE_BOOLEAN, false);
  TestValue("(1>2) IS FALSE", TYPE_BOOLEAN, true);
  TestValue("(1>2) IS UNKNOWN", TYPE_BOOLEAN, false);
  TestValue("(1>2) IS NOT TRUE", TYPE_BOOLEAN, true);
  TestValue("(1>2) IS NOT FALSE", TYPE_BOOLEAN, false);
  TestValue("(1>2) IS NOT UNKNOWN", TYPE_BOOLEAN, true);
  TestValue("(NULL = 1) IS TRUE", TYPE_BOOLEAN, false);
  TestValue("(NULL = 1) IS FALSE", TYPE_BOOLEAN, false);
  TestValue("(NULL = 1) IS UNKNOWN", TYPE_BOOLEAN, true);
  TestValue("(NULL = 1) IS NOT TRUE", TYPE_BOOLEAN, true);
  TestValue("(NULL = 1) IS NOT FALSE", TYPE_BOOLEAN, true);
  TestValue("(NULL = 1) IS NOT UNKNOWN", TYPE_BOOLEAN, false);
}

TEST_F(ExprTest, LikePredicate) {
  TestValue("'a' LIKE '%a%'", TYPE_BOOLEAN, true);
  TestValue("'a' LIKE '%abcde'", TYPE_BOOLEAN, false);
  TestValue("'a' LIKE 'abcde%'", TYPE_BOOLEAN, false);
  TestValue("'abcde' LIKE 'abcde%'", TYPE_BOOLEAN, true);
  TestValue("'abcde' LIKE '%abcde'", TYPE_BOOLEAN, true);
  TestValue("'abcde' LIKE '%abcde%'", TYPE_BOOLEAN, true);
  // Test multiple wildcard characters
  TestValue("'abcde' LIKE '%%bc%%'", TYPE_BOOLEAN, true);
  TestValue("'abcde' LIKE '%%cb%%'", TYPE_BOOLEAN, false);
  TestValue("'abcde' LIKE 'abc%%'", TYPE_BOOLEAN, true);
  TestValue("'abcde' LIKE 'cba%%'", TYPE_BOOLEAN, false);
  TestValue("'abcde' LIKE '%%bcde'", TYPE_BOOLEAN, true);
  TestValue("'abcde' LIKE '%%cbde'", TYPE_BOOLEAN, false);
  TestValue("'abcde' LIKE '%%bc%%'", TYPE_BOOLEAN, true);
  TestValue("'abcde' LIKE '%%cb%%'", TYPE_BOOLEAN, false);
  TestValue("'abcde' LIKE '%%abcde%%'", TYPE_BOOLEAN, true);
  TestValue("'abcde' LIKE '%%edcba%%'", TYPE_BOOLEAN, false);
  TestValue("'abcde' LIKE '%%'", TYPE_BOOLEAN, true);
  TestValue("'abcde' NOT LIKE '%%'", TYPE_BOOLEAN, false);
  TestValue("'abcde' LIKE '%%%'", TYPE_BOOLEAN, true);
  TestValue("'abcde' NOT LIKE '%%%'", TYPE_BOOLEAN, false);
  TestValue("'abcde' LIKE '%%cd%%'", TYPE_BOOLEAN, true);
  TestValue("'abcde' LIKE '%%dc%%'", TYPE_BOOLEAN, false);
  TestValue("'abcde' LIKE '%%_%%'", TYPE_BOOLEAN, true);
  // IMP-117
  TestValue("'ab' LIKE '%a'", TYPE_BOOLEAN, false);
  TestValue("'ab' NOT LIKE '%a'", TYPE_BOOLEAN, true);
  // IMP-117
  TestValue("'ba' LIKE 'a%'", TYPE_BOOLEAN, false);
  TestValue("'ab' LIKE 'a'", TYPE_BOOLEAN, false);
  TestValue("'a' LIKE '_'", TYPE_BOOLEAN, true);
  TestValue("'a' NOT LIKE '_'", TYPE_BOOLEAN, false);
  TestValue("'a' LIKE 'a'", TYPE_BOOLEAN, true);
  TestValue("'a' LIKE 'b'", TYPE_BOOLEAN, false);
  TestValue("'a' NOT LIKE 'a'", TYPE_BOOLEAN, false);
  TestValue("'a' NOT LIKE 'b'", TYPE_BOOLEAN, true);
  // IMP-117 -- initial part of pattern appears earlier in string.
  TestValue("'LARGE BRUSHED BRASS' LIKE '%BRASS'", TYPE_BOOLEAN, true);
  TestValue("'BRASS LARGE BRUSHED' LIKE '%BRASS'", TYPE_BOOLEAN, false);
  TestValue("'BRASS LARGE BRUSHED' LIKE 'BRUSHED%'", TYPE_BOOLEAN, false);
  TestValue("'BRASS LARGE BRUSHED' LIKE 'BRASS%'", TYPE_BOOLEAN, true);
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
  TestValue("'a.x.y.*a' REGEXP 'a\\\\.x\\\\.y\\\\.\\\\*a'", TYPE_BOOLEAN, true);
  TestValue("'abxcy1234a' REGEXP '\\a\\.x\\\\.y\\\\.\\\\*a'", TYPE_BOOLEAN, false);
  TestValue("'abxcy1234a' RLIKE 'a.x.y.*a'", TYPE_BOOLEAN, true);
  TestValue("'axcy1234a' REGEXP 'a.x.y.*a'", TYPE_BOOLEAN, false);
  TestValue("'axcy1234a' RLIKE 'a.x.y.*a'", TYPE_BOOLEAN, false);
  // Regex patterns with constants strings
  TestValue("'english' REGEXP 'en'", TYPE_BOOLEAN, true);
  TestValue("'english' REGEXP 'lis'", TYPE_BOOLEAN, true);
  TestValue("'english' REGEXP 'english'", TYPE_BOOLEAN, true);
  TestValue("'english' REGEXP 'engilsh'", TYPE_BOOLEAN, false);
  TestValue("'english' REGEXP '^english$'", TYPE_BOOLEAN, true);
  TestValue("'english' REGEXP '^lish$'", TYPE_BOOLEAN, false);
  TestValue("'english' REGEXP '^eng'", TYPE_BOOLEAN, true);
  TestValue("'english' REGEXP '^ng'", TYPE_BOOLEAN, false);
  TestValue("'english' REGEXP 'lish$'", TYPE_BOOLEAN, true);
  TestValue("'english' REGEXP 'lis$'", TYPE_BOOLEAN, false);
  // regex escape chars; insert special character in the middle to prevent
  // it from being matched as a substring
  TestValue("'.[]{}()x\\\\*+?|^$' LIKE '.[]{}()_\\\\\\\\*+?|^$'", TYPE_BOOLEAN, true);
  // escaped _ matches single _
  TestValue("'\\\\_' LIKE '\\\\_'", TYPE_BOOLEAN, false);
  TestValue("'_' LIKE '\\\\_'", TYPE_BOOLEAN, true);
  TestValue("'a' LIKE '\\\\_'", TYPE_BOOLEAN, false);
  // escaped escape char
  TestValue("'\\\\a' LIKE '\\\\\\_'", TYPE_BOOLEAN, true);
  TestValue("'_' LIKE '\\\\\\_'", TYPE_BOOLEAN, false);
  // make sure the 3rd \ counts toward the _
  TestValue("'\\\\_' LIKE '\\\\\\\\\\_'", TYPE_BOOLEAN, true);
  TestValue("'\\\\\\\\a' LIKE '\\\\\\\\\\_'", TYPE_BOOLEAN, false);
  // Test invalid patterns, unmatched parenthesis.
  TestNonOkStatus("'a' RLIKE '(./'");
  TestNonOkStatus("'a' REGEXP '(./'");
  // Pattern is converted for LIKE, and should not throw.
  TestValue("'a' LIKE '(./'", TYPE_BOOLEAN, false);
  // Test NULLs.
  TestIsNull("NULL LIKE 'a'", TYPE_BOOLEAN);
  TestIsNull("'a' LIKE NULL", TYPE_BOOLEAN);
  TestIsNull("NULL LIKE NULL", TYPE_BOOLEAN);
  TestIsNull("NULL RLIKE 'a'", TYPE_BOOLEAN);
  TestIsNull("'a' RLIKE NULL", TYPE_BOOLEAN);
  TestIsNull("NULL RLIKE NULL", TYPE_BOOLEAN);
  TestIsNull("NULL REGEXP 'a'", TYPE_BOOLEAN);
  TestIsNull("'a' REGEXP NULL", TYPE_BOOLEAN);
  TestIsNull("NULL REGEXP NULL", TYPE_BOOLEAN);
  // Test multi-line strings.
  TestValue("'abc\n123' LIKE 'abc_123'", TYPE_BOOLEAN, true);
  TestValue("'abc\n\n123' LIKE 'abc_123'", TYPE_BOOLEAN, false);
  TestValue("'\n' LIKE 'a'", TYPE_BOOLEAN, false);
  TestValue("'\n' LIKE '\n'", TYPE_BOOLEAN, true);
  TestValue("'n\n' LIKE '\n'", TYPE_BOOLEAN, false);
  TestValue("'\n\n' LIKE '_\n'", TYPE_BOOLEAN, true);
  TestValue("'\n\n' LIKE '%\n'", TYPE_BOOLEAN, true);
  TestValue("'\n\n' LIKE '\n_'", TYPE_BOOLEAN, true);
  TestValue("'\n\n' LIKE '\n%'", TYPE_BOOLEAN, true);
  TestValue("'\n\n' LIKE '__\n'", TYPE_BOOLEAN, false);
  TestValue("'\n\n\n' LIKE '\n_\n'", TYPE_BOOLEAN, true);
  TestValue("'abc\n123' LIKE 'abc%123'", TYPE_BOOLEAN, true);
  TestValue("'abc\n\n123' LIKE 'abc%123'", TYPE_BOOLEAN, true);
  TestValue("'\nabc\n123' LIKE '%abc_123'", TYPE_BOOLEAN, true);
  TestValue("'abc\n123\nedf' LIKE 'abc%edf'", TYPE_BOOLEAN, true);
  // Make sure that constant match handles '\n' properly.
  TestValue("'abc\n123' LIKE 'abc%'", TYPE_BOOLEAN, true);
  TestValue("'123\nabc' LIKE '%abc'", TYPE_BOOLEAN, true);
  TestValue("'123\nabc\n123' LIKE '%abc%'", TYPE_BOOLEAN, true);
  // Test case-insensitivity.
  TestValue("'aBcde' ILIKE '%%bC%%'", TYPE_BOOLEAN, true);
  TestValue("'aBcde' ILIKE '%%Cb%%'", TYPE_BOOLEAN, false);
  TestValue("'aBcde' ILIKE 'abC%%'", TYPE_BOOLEAN, true);
  TestValue("'aBcde' ILIKE 'cbA%%'", TYPE_BOOLEAN, false);
  TestValue("'aBcde' ILIKE '%%bCde'", TYPE_BOOLEAN, true);
  TestValue("'aBcde' ILIKE '%%cBde'", TYPE_BOOLEAN, false);
  TestValue("'aBcde' ILIKE '%%abCde%%'", TYPE_BOOLEAN, true);
  TestValue("'aBcde' ILIKE '%%eDcba%%'", TYPE_BOOLEAN, false);
  TestValue("'aBcde' ILIKE 'A%%Cd%%'", TYPE_BOOLEAN, true);
  TestValue("'aBcde' ILIKE 'A%%Dc%%'", TYPE_BOOLEAN, false);
  TestValue("'aBcde' ILIKE 'AbCde'", TYPE_BOOLEAN, true);
  TestValue("'aBcde' ILIKE 'AbDCe'", TYPE_BOOLEAN, false);
  TestValue("'Abc\n123' ILIKE 'aBc%123'", TYPE_BOOLEAN, true);
  TestValue("'Abc\n\n123' ILIKE 'aBc%123'", TYPE_BOOLEAN, true);
  TestValue("'\nAbc\n123' ILIKE '%aBc_123'", TYPE_BOOLEAN, true);
  TestValue("'Abc\n123\nedf' ILIKE 'aBc%edf'", TYPE_BOOLEAN, true);
  TestValue("'.[]{}()x\\\\*+?|^$' ILIKE '.[]{}()_\\\\\\\\*+?|^$'", TYPE_BOOLEAN, true);
  TestValue("'.[]{}()xA\\\\*+?|^$' ILIKE '.[]{}()_a\\\\\\\\*+?|^$'", TYPE_BOOLEAN, true);
  TestValue("'abxcY1234a' IREGEXP 'a.X.y.*a'", TYPE_BOOLEAN, true);
  TestValue("'a.x.Y.*a' IREGEXP 'a\\\\.X\\\\.y\\\\.\\\\*a'", TYPE_BOOLEAN, true);
  TestValue("'abxcY1234a' IREGEXP '\\a\\.X\\\\.y\\\\.\\\\*a'", TYPE_BOOLEAN, false);
  TestValue("'Abxcy1234a' IREGEXP 'a.x.y.*a'", TYPE_BOOLEAN, true);
  TestValue("'English' IREGEXP 'en'", TYPE_BOOLEAN, true);
  TestValue("'engLish' IREGEXP 'lIs'", TYPE_BOOLEAN, true);
  TestValue("'English' IREGEXP 'englIsh'", TYPE_BOOLEAN, true);
  TestValue("'eNglish' IREGEXP '^enGlish$'", TYPE_BOOLEAN, true);
  TestValue("'enGlish' IREGEXP '^eNg'", TYPE_BOOLEAN, true);
  TestValue("'engLish' IREGEXP 'lIsh$'", TYPE_BOOLEAN, true);
}

TEST_F(ExprTest, BetweenPredicate) {
  // Between is rewritten into a conjunctive compound predicate.
  // Compound predicates are also tested elsewere, so we just do basic testing here.
  TestValue("5 between 0 and 10", TYPE_BOOLEAN, true);
  TestValue("5 between 5 and 5", TYPE_BOOLEAN, true);
  TestValue("5 between 6 and 10", TYPE_BOOLEAN, false);
  TestValue("5 not between 0 and 10", TYPE_BOOLEAN, false);
  TestValue("5 not between 5 and 5", TYPE_BOOLEAN, false);
  TestValue("5 not between 6 and 10", TYPE_BOOLEAN, true);
  // Test operator precedence.
  TestValue("5+1 between 4 and 10", TYPE_BOOLEAN, true);
  TestValue("5+1 not between 4 and 10", TYPE_BOOLEAN, false);
  // Test TimestampValues.
  TestValue("cast('2011-10-22 09:10:11' as timestamp) between "
      "cast('2011-09-22 09:10:11' as timestamp) and "
      "cast('2011-12-22 09:10:11' as timestamp)", TYPE_BOOLEAN, true);
  TestValue("cast('2011-10-22 09:10:11' as timestamp) between "
        "cast('2011-11-22 09:10:11' as timestamp) and "
        "cast('2011-12-22 09:10:11' as timestamp)", TYPE_BOOLEAN, false);
  TestValue("cast('2011-10-22 09:10:11' as timestamp) not between "
      "cast('2011-09-22 09:10:11' as timestamp) and "
      "cast('2011-12-22 09:10:11' as timestamp)", TYPE_BOOLEAN, false);
  TestValue("cast('2011-10-22 09:10:11' as timestamp) not between "
      "cast('2011-11-22 09:10:11' as timestamp) and "
      "cast('2011-12-22 09:10:11' as timestamp)", TYPE_BOOLEAN, true);
  // Test strings.
  TestValue("'abc' between 'a' and 'z'", TYPE_BOOLEAN, true);
  TestValue("'abc' between 'aaa' and 'aab'", TYPE_BOOLEAN, false);
  TestValue("'abc' not between 'a' and 'z'", TYPE_BOOLEAN, false);
  TestValue("'abc' not between 'aaa' and 'aab'", TYPE_BOOLEAN, true);
  // Test NULLs.
  TestIsNull("NULL between 0 and 10", TYPE_BOOLEAN);
  TestIsNull("1 between NULL and 10", TYPE_BOOLEAN);
  TestIsNull("1 between 0 and NULL", TYPE_BOOLEAN);
  TestIsNull("1 between NULL and NULL", TYPE_BOOLEAN);
  TestIsNull("NULL between NULL and NULL", TYPE_BOOLEAN);
}

// Tests with NULLs are in the FE QueryTest.
TEST_F(ExprTest, InPredicate) {
  // Test integers.
  IntValMap::iterator int_iter;
  for(int_iter = min_int_values_.begin(); int_iter != min_int_values_.end();
      ++int_iter) {
    string& val = default_type_strs_[int_iter->first];
    TestValue(val + " in (2, 3, " + val + ")", TYPE_BOOLEAN, true);
    TestValue(val + " in (2, 3, 4)", TYPE_BOOLEAN, false);
    TestValue(val + " not in (2, 3, " + val + ")", TYPE_BOOLEAN, false);
    TestValue(val + " not in (2, 3, 4)", TYPE_BOOLEAN, true);
  }

  // Test floats.
  unordered_map<int, double>::iterator float_iter;
  for(float_iter = min_float_values_.begin(); float_iter != min_float_values_.end();
      ++float_iter) {
    string& val = default_type_strs_[float_iter->first];
    TestValue(val + " in (2, 3, " + val + ")", TYPE_BOOLEAN, true);
    TestValue(val + " in (2, 3, 4)", TYPE_BOOLEAN, false);
    TestValue(val + " not in (2, 3, " + val + ")", TYPE_BOOLEAN, false);
    TestValue(val + " not in (2, 3, 4)", TYPE_BOOLEAN, true);
  }

  // Test bools.
  TestValue("true in (true, false, false)", TYPE_BOOLEAN, true);
  TestValue("true in (false, false, false)", TYPE_BOOLEAN, false);
  TestValue("true not in (true, false, false)", TYPE_BOOLEAN, false);
  TestValue("true not in (false, false, false)", TYPE_BOOLEAN, true);

  // Test strings.
  TestValue("'ab' in ('ab', 'cd', 'efg')", TYPE_BOOLEAN, true);
  TestValue("'ab' in ('cd', 'efg', 'h')", TYPE_BOOLEAN, false);
  TestValue("'ab' not in ('ab', 'cd', 'efg')", TYPE_BOOLEAN, false);
  TestValue("'ab' not in ('cd', 'efg', 'h')", TYPE_BOOLEAN, true);

  // test chars
  TestValue("cast('ab' as char(2)) in (cast('ab' as char(2)), cast('cd' as char(2)))",
            TYPE_BOOLEAN, true);

  // Test timestamps.
  TestValue(default_timestamp_str_ + " "
      "in (cast('2011-11-23 09:10:11' as timestamp), "
      "cast('2011-11-24 09:11:12' as timestamp), " +
      default_timestamp_str_ + ")", TYPE_BOOLEAN, true);
  TestValue(default_timestamp_str_ + " "
        "in (cast('2011-11-22 09:10:11' as timestamp), "
        "cast('2011-11-23 09:11:12' as timestamp), "
        "cast('2011-11-24 09:12:13' as timestamp))", TYPE_BOOLEAN, false);
  TestValue(default_timestamp_str_ + " "
      "not in (cast('2011-11-22 09:10:11' as timestamp), "
      "cast('2011-11-23 09:11:12' as timestamp), " +
      default_timestamp_str_ + ")", TYPE_BOOLEAN, false);
  TestValue(default_timestamp_str_ + " "
      "not in (cast('2011-11-22 09:10:11' as timestamp), "
      "cast('2011-11-23 09:11:12' as timestamp), "
      "cast('2011-11-24 09:12:13' as timestamp))", TYPE_BOOLEAN, true);

  // Test decimals
  vector<string> dec_strs; // Decimal of every physical type
  dec_strs.push_back("cast(-1.23 as decimal(8,2))");
  dec_strs.push_back("cast(-1.23 as decimal(9,2))");
  dec_strs.push_back("cast(-1.23 as decimal(10,2))");
  dec_strs.push_back("cast(-1.23 as decimal(17,2))");
  dec_strs.push_back("cast(-1.23 as decimal(18,2))");
  dec_strs.push_back("cast(-1.23 as decimal(19,2))");
  dec_strs.push_back("cast(-1.23 as decimal(32,2))");
  for (const string& dec_str: dec_strs) {
    TestValue(dec_str + "in (0)", TYPE_BOOLEAN, false);
    TestValue(dec_str + "in (-1.23)", TYPE_BOOLEAN, true);
    TestValue(dec_str + "in (-1.230)", TYPE_BOOLEAN, true);
    TestValue(dec_str + "in (-1.23, 1)", TYPE_BOOLEAN, true);
    TestValue(dec_str + "in (1, 1, 1, 1, 1, -1.23, 1, 1, 1, 1, 1, 1, -1.23)",
              TYPE_BOOLEAN, true);
    TestValue(dec_str + "in (1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, -1.23)",
              TYPE_BOOLEAN, true);
    TestValue(dec_str + "in (1)", TYPE_BOOLEAN, false);
    TestValue(dec_str + "in (1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1)", TYPE_BOOLEAN, false);
    TestIsNull(dec_str + "in (NULL)", TYPE_BOOLEAN);
    TestIsNull("NULL in (-1.23)", TYPE_BOOLEAN);
    TestIsNull(dec_str + "in (1, 1, 1, 1, 1, 1, 1, 1, 1, 1, NULL)", TYPE_BOOLEAN);
    TestValue(dec_str + "in (1, 1, 1, 1, 1, NULL, 1, 1, 1, 1, 1, 1, -1.23)",
              TYPE_BOOLEAN, true);
  }

  // Test operator precedence.
  TestValue("5+1 in (3, 6, 10)", TYPE_BOOLEAN, true);
  TestValue("5+1 not in (3, 6, 10)", TYPE_BOOLEAN, false);

  // Test NULLs
  TestIsNull("NULL in (1, 2, 3)", TYPE_BOOLEAN);
  TestIsNull("NULL in (NULL, 1)", TYPE_BOOLEAN);
  TestIsNull("3 in (NULL)", TYPE_BOOLEAN);
  TestIsNull("3 in (1, 2, NULL)", TYPE_BOOLEAN);
  TestValue("3 in (NULL, 3)", TYPE_BOOLEAN, true);
  TestIsNull("'hello' in (NULL)", TYPE_BOOLEAN);
  TestValue("'hello' in ('hello', NULL)", TYPE_BOOLEAN, true);
  TestIsNull("NULL in (NULL)", TYPE_BOOLEAN);
  TestIsNull("NULL in (NULL, NULL)", TYPE_BOOLEAN);
}

TEST_F(ExprTest, StringFunctions) {
  TestStringValue("substring('Hello', 1)", "Hello");
  TestStringValue("substring('Hello', -2)", "lo");
  TestStringValue("substring('Hello', cast(0 as bigint))", "");
  TestStringValue("substring('Hello', -5)", "Hello");
  TestStringValue("substring('Hello', cast(-6 as bigint))", "");
  TestStringValue("substring('Hello', 100)", "");
  TestStringValue("substring('Hello', -100)", "");
  TestIsNull("substring(NULL, 100)", TYPE_STRING);
  TestIsNull("substring('Hello', NULL)", TYPE_STRING);
  TestIsNull("substring(NULL, NULL)", TYPE_STRING);

  TestStringValue("substring('Hello', 1, 1)", "H");
  TestStringValue("substring('Hello', cast(2 as bigint), 100)", "ello");
  TestStringValue("substring('Hello', -3, cast(2 as bigint))", "ll");
  TestStringValue("substring('Hello', 1, 0)", "");
  TestStringValue("substring('Hello', cast(1 as bigint), cast(-1 as bigint))", "");
  TestIsNull("substring(NULL, 1, 100)", TYPE_STRING);
  TestIsNull("substring('Hello', NULL, 100)", TYPE_STRING);
  TestIsNull("substring('Hello', 1, NULL)", TYPE_STRING);
  TestIsNull("substring(NULL, NULL, NULL)", TYPE_STRING);

  TestStringValue("chr(93)", "]");
  TestStringValue("chr(96)", "`");
  TestStringValue("chr(97)", "a");
  TestStringValue("chr(48)", "0");
  TestStringValue("chr(65)", "A");
  TestStringValue("chr(300)", "");
  TestStringValue("chr(-1)", "");
  TestIsNull("chr(NULL)", TYPE_STRING);

  TestStringValue("split_part('abc~!def~!ghi', '~!', 1)", "abc");
  TestStringValue("split_part('abc~!~def~!~ghi', '~!', 2)", "~def");
  TestStringValue("split_part('abc@@def@@ghi', '@@', 3)", "ghi");
  TestStringValue("split_part('abc@@def@@@@ghi', '@@', 4)", "ghi");
  TestStringValue("split_part('abc@@def@@ghi', '@@', 4)", "");
  TestStringValue("split_part('', '@@', 1)", "");
  TestStringValue("split_part('abcdef', '', 1)", "abcdef");
  TestStringValue("split_part('', '', 1)", "");
  TestIsNull("split_part(NULL, NULL, 1)", TYPE_STRING);
  TestIsNull("split_part('abcdefabc', NULL, 1)", TYPE_STRING);
  TestIsNull("split_part(NULL, 'xyz', 1)", TYPE_STRING);
  TestError("split_part('abc@@def@@ghi', '@@', 0)");
  TestError("split_part('abc@@def@@ghi', '@@', -1)");

  TestStringValue("lower('')", "");
  TestStringValue("lower('HELLO')", "hello");
  TestStringValue("lower('Hello')", "hello");
  TestStringValue("lower('hello!')", "hello!");
  TestStringValue("lcase('HELLO')", "hello");
  TestIsNull("lower(NULL)", TYPE_STRING);
  TestIsNull("lcase(NULL)", TYPE_STRING);

  TestStringValue("upper('')", "");
  TestStringValue("upper('HELLO')", "HELLO");
  TestStringValue("upper('Hello')", "HELLO");
  TestStringValue("upper('hello!')", "HELLO!");
  // Regression test for fully builtin qualified function name (IMPALA-1951)
  TestStringValue("_impala_builtins.upper('hello!')", "HELLO!");
  TestStringValue("_impala_builtins.DECODE('hello!', 'hello!', 'HELLO!')", "HELLO!");
  TestStringValue("ucase('hello')", "HELLO");
  TestIsNull("upper(NULL)", TYPE_STRING);
  TestIsNull("ucase(NULL)", TYPE_STRING);

  TestStringValue("initcap('')", "");
  TestStringValue("initcap('a')", "A");
  TestStringValue("initcap('hello')", "Hello");
  TestStringValue("initcap('h e l l o')", "H E L L O");
  TestStringValue("initcap('hello this is a message')", "Hello This Is A Message");
  TestStringValue("initcap('Hello This Is A Message')", "Hello This Is A Message");
  TestStringValue("initcap(' hello    tHis  IS A _  MeSsAgE')", " Hello    This  "
      "Is A _  Message");
  TestStringValue("initcap('HELLO THIS IS A MESSAGE')", "Hello This Is A Message");
  TestStringValue("initcap(' hello\vthis\nis\ra\tlong\fmessage')", " Hello\vThis"
      "\nIs\rA\tLong\fMessage");
  TestIsNull("initcap(NULL)", TYPE_STRING);

  string length_aliases[] = {"length", "char_length", "character_length"};
  for (int i = 0; i < 3; i++) {
    TestValue(length_aliases[i] + "('')", TYPE_INT, 0);
    TestValue(length_aliases[i] + "('a')", TYPE_INT, 1);
    TestValue(length_aliases[i] + "('abcdefg')", TYPE_INT, 7);
    TestIsNull(length_aliases[i] + "(NULL)", TYPE_INT);
  }

  TestStringValue("replace('aaaaaa', 'a', 'b')", "bbbbbb");
  TestStringValue("replace('aaaaaa', 'aa', 'b')", "bbb");
  TestStringValue("replace('aaaaaaa', 'aa', 'b')", "bbba");
  TestStringValue("replace('zzzaaaaaaqqq', 'a', 'b')", "zzzbbbbbbqqq");
  TestStringValue("replace('zzzaaaaaaaqqq', 'aa', 'b')", "zzzbbbaqqq");
  TestStringValue("replace('aaaaaaaaaaaaaaaaaaaa', 'a', 'b')", "bbbbbbbbbbbbbbbbbbbb");
  TestStringValue("replace('bobobobobo', 'bobo', 'a')", "aabo");
  TestStringValue("replace('abc', 'abcd', 'a')", "abc");
  TestStringValue("replace('aaaaaa', '', 'b')", "aaaaaa");
  TestStringValue("replace('abc', 'abc', '')", "");
  TestStringValue("replace('abcdefg', 'z', '')", "abcdefg");
  TestStringValue("replace('', 'zoltan', 'sultan')", "");
  TestStringValue("replace('strstrstr', 'str', 'strstr')", "strstrstrstrstrstr");
  TestStringValue("replace('aaaaaa', 'a', '')", "");
  TestIsNull("replace(NULL, 'foo', 'bar')", TYPE_STRING);
  TestIsNull("replace('zomg', 'foo', NULL)", TYPE_STRING);
  TestIsNull("replace('abc', NULL, 'a')", TYPE_STRING);

  // Do some tests with huge strings
  const int huge_size = 500000;
  std::string huge_str;
  huge_str.reserve(huge_size);
  huge_str.append(huge_size, 'A');

  std::string huge_space;
  huge_space.reserve(huge_size);
  huge_space.append(huge_size, ' ');

  std::string huger_str;
  huger_str.reserve(3 * huge_size);
  huger_str.append(3 * huge_size, 'A');

  TestStringValue("replace('" + huge_str + "', 'A', ' ')", huge_space);
  TestStringValue("replace('" + huge_str + "', 'A', '')", "");

  TestStringValue("replace('" + huge_str + "', 'A', 'AAA')", huger_str);
  TestStringValue("replace('" + huger_str + "', 'AAA', 'A')", huge_str);

  auto* giga_buf = new std::array<uint8_t, StringVal::MAX_LENGTH>;
  giga_buf->fill('A');
  (*giga_buf)[0] = 'Z';
  (*giga_buf)[10] = 'Z';
  (*giga_buf)[100] = 'Z';
  (*giga_buf)[1000] = 'Z';
  (*giga_buf)[StringVal::MAX_LENGTH-1] = 'Z';

  // Hack up a function context so we can call Replace functions directly.
  MemTracker m;
  MemPool pool(&m);
  FunctionContext::TypeDesc str_desc;
  str_desc.type = FunctionContext::Type::TYPE_STRING;
  std::vector<FunctionContext::TypeDesc> v(3, str_desc);
  auto context = UdfTestHarness::CreateTestContext(str_desc, v, nullptr, &pool);

  StringVal giga(static_cast<uint8_t*>(giga_buf->data()), StringVal::MAX_LENGTH);
  StringVal a("A");
  StringVal z("Z");
  StringVal aaa("aaa");

  // Replace z's with a's on giga
  auto r1 = StringFunctions::Replace(context, giga, z, a);
  EXPECT_EQ(r1.ptr[0], 'A');
  EXPECT_EQ(r1.ptr[10], 'A');
  EXPECT_EQ(r1.ptr[100], 'A');
  EXPECT_EQ(r1.ptr[1000], 'A');
  EXPECT_EQ(r1.ptr[StringVal::MAX_LENGTH-1], 'A');

  // Entire string match is legal
  auto r2 = StringFunctions::Replace(context, giga, giga, a);
  EXPECT_EQ(r2, a);

  // So is replacing giga with itself
  auto r3 = StringFunctions::Replace(context, giga, giga, giga);
  EXPECT_EQ(r3.ptr[0], 'Z');
  EXPECT_EQ(r3.ptr[10], 'Z');
  EXPECT_EQ(r3.ptr[100], 'Z');
  EXPECT_EQ(r3.ptr[1000], 'Z');
  EXPECT_EQ(r3.ptr[StringVal::MAX_LENGTH-1], 'Z');

  // Expect expansion to fail as soon as possible; test with unallocated string space
  // This tests overflowing the first allocation.
  auto* short_buf = new std::array<uint8_t, 4096>;
  short_buf->fill('A');
  (*short_buf)[1000] = 'Z';
  StringVal bam(static_cast<uint8_t*>(short_buf->data()), StringVal::MAX_LENGTH);
  auto r4 = StringFunctions::Replace(context, bam, z, aaa);
  EXPECT_TRUE(r4.is_null);
  // Re-create context to clear the error from failed allocation.
  UdfTestHarness::CloseContext(context);
  context = UdfTestHarness::CreateTestContext(str_desc, v, nullptr, &pool);

  // Similar test for second overflow.  This tests overflowing on re-allocation.
  (*short_buf)[4095] = 'Z';
  StringVal bam2(static_cast<uint8_t*>(short_buf->data()), StringVal::MAX_LENGTH-2);
  auto r5 = StringFunctions::Replace(context, bam2, z, aaa);
  EXPECT_TRUE(r5.is_null);
  // Re-create context to clear the error from failed allocation.
  UdfTestHarness::CloseContext(context);
  context = UdfTestHarness::CreateTestContext(str_desc, v, nullptr, &pool);

  // Finally, test expanding to exactly MAX_LENGTH
  // There are 4 Zs in giga4 (not including the trailing one, as we truncate that)
  StringVal giga4(static_cast<uint8_t*>(giga_buf->data()), StringVal::MAX_LENGTH-8);
  auto r6 = StringFunctions::Replace(context, giga4, z, aaa);
  EXPECT_EQ(strncmp((char*)&r6.ptr[0], "aaaA", 4), 0);
  EXPECT_EQ(r6.len, 1 << 30);

  // Finally, an expansion in the last string position
  (*giga_buf)[StringVal::MAX_LENGTH-11] = 'Z';
  StringVal giga5(static_cast<uint8_t*>(giga_buf->data()), StringVal::MAX_LENGTH-10);
  auto r7 = StringFunctions::Replace(context, giga5, z, aaa);
  EXPECT_EQ(r7.len, 1 << 30);
  EXPECT_EQ(strncmp((char*)&r7.ptr[StringVal::MAX_LENGTH-4], "Aaaa", 4), 0);

  UdfTestHarness::CloseContext(context);
  delete giga_buf;
  delete short_buf;
  pool.FreeAll();

  TestStringValue("reverse('abcdefg')", "gfedcba");
  TestStringValue("reverse('')", "");
  TestIsNull("reverse(NULL)", TYPE_STRING);
  TestStringValue("strleft('abcdefg', 0)", "");
  TestStringValue("strleft('abcdefg', 3)", "abc");
  TestStringValue("strleft('abcdefg', cast(10 as bigint))", "abcdefg");
  TestStringValue("strleft('abcdefg', -1)", "");
  TestStringValue("strleft('abcdefg', cast(-9 as bigint))", "");
  TestIsNull("strleft(NULL, 3)", TYPE_STRING);
  TestIsNull("strleft('abcdefg', NULL)", TYPE_STRING);
  TestIsNull("strleft(NULL, NULL)", TYPE_STRING);
  TestStringValue("strright('abcdefg', 0)", "");
  TestStringValue("strright('abcdefg', 3)", "efg");
  TestStringValue("strright('abcdefg', cast(10 as bigint))", "abcdefg");
  TestStringValue("strright('abcdefg', -1)", "");
  TestStringValue("strright('abcdefg', cast(-9 as bigint))", "");
  TestIsNull("strright(NULL, 3)", TYPE_STRING);
  TestIsNull("strright('abcdefg', NULL)", TYPE_STRING);
  TestIsNull("strright(NULL, NULL)", TYPE_STRING);

  TestStringValue("translate('', '', '')", "");
  TestStringValue("translate('abcd', '', '')", "abcd");
  TestStringValue("translate('abcd', 'xyz', '')", "abcd");
  TestStringValue("translate('abcd', 'a', '')", "bcd");
  TestStringValue("translate('abcd', 'aa', '')", "bcd");
  TestStringValue("translate('abcd', 'aba', '')", "cd");
  TestStringValue("translate('abcd', 'cd', '')", "ab");
  TestStringValue("translate('abcd', 'cd', 'xy')", "abxy");
  TestStringValue("translate('abcdabcd', 'cd', 'xy')", "abxyabxy");
  TestStringValue("translate('abcd', 'abc', 'xy')", "xyd");
  TestStringValue("translate('abcd', 'abc', 'wxyz')", "wxyd");
  TestStringValue("translate('x', 'xx', 'ab')", "a");
  TestIsNull("translate(NULL, '', '')", TYPE_STRING);
  TestIsNull("translate('', NULL, '')", TYPE_STRING);
  TestIsNull("translate('', '', NULL)", TYPE_STRING);

  TestStringValue("trim('')", "");
  TestStringValue("trim('      ')", "");
  TestStringValue("trim('   abcdefg   ')", "abcdefg");
  TestStringValue("trim('abcdefg   ')", "abcdefg");
  TestStringValue("trim('   abcdefg')", "abcdefg");
  TestStringValue("trim('abc  defg')", "abc  defg");
  TestIsNull("trim(NULL)", TYPE_STRING);
  TestStringValue("ltrim('')", "");
  TestStringValue("ltrim('      ')", "");
  TestStringValue("ltrim('   abcdefg   ')", "abcdefg   ");
  TestStringValue("ltrim('abcdefg   ')", "abcdefg   ");
  TestStringValue("ltrim('   abcdefg')", "abcdefg");
  TestStringValue("ltrim('abc  defg')", "abc  defg");
  TestIsNull("ltrim(NULL)", TYPE_STRING);
  TestStringValue("rtrim('')", "");
  TestStringValue("rtrim('      ')", "");
  TestStringValue("rtrim('   abcdefg   ')", "   abcdefg");
  TestStringValue("rtrim('abcdefg   ')", "abcdefg");
  TestStringValue("rtrim('   abcdefg')", "   abcdefg");
  TestStringValue("rtrim('abc  defg')", "abc  defg");
  TestIsNull("rtrim(NULL)", TYPE_STRING);

  TestStringValue("btrim('     abcdefg   ')", "abcdefg");
  TestStringValue("btrim('     abcdefg')", "abcdefg");
  TestStringValue("btrim('abcdefg      ')", "abcdefg");
  TestStringValue("btrim('abcdefg')", "abcdefg");
  TestStringValue("btrim('abc defg')", "abc defg");
  TestStringValue("btrim('        ')", "");
  TestStringValue("btrim(',')", ",");
  TestIsNull("btrim(NULL)", TYPE_STRING);

  TestStringValue("btrim('%%%%%abcdefg%%%%%', '%')", "abcdefg");
  TestStringValue("btrim('%%%%%abcdefg', '%')", "abcdefg");
  TestStringValue("btrim('abcdefg%%%%%', '%')", "abcdefg");
  TestStringValue("btrim('abc%%defg%%%%%', '%')", "abc%%defg");
  TestStringValue("btrim('abcdefg', 'abc')", "defg");
  TestStringValue("btrim('abacdefg', 'abc')", "defg");
  TestStringValue("btrim('abacdefgcab', 'abc')", "defg");
  TestStringValue("btrim('abcacbbacbcacabcba', 'abc')", "");
  TestStringValue("btrim('', 'abc')", "");
  TestStringValue("btrim('abcdefg', NULL)", "abcdefg");
  TestStringValue("btrim('abcdabcdabc', 'abc')", "dabcd");
  TestStringValue("btrim('aaaaaaaaa', 'a')", "");
  TestStringValue("btrim('abcdefg', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaabg')", "cdef");
  TestStringValue("btrim('æeioü','æü')", "eio");

  TestStringValue("space(0)", "");
  TestStringValue("space(-1)", "");
  TestStringValue("space(cast(1 as bigint))", " ");
  TestStringValue("space(6)", "      ");
  TestIsNull("space(NULL)", TYPE_STRING);

  TestStringValue("repeat('', 0)", "");
  TestStringValue("repeat('', cast(6 as bigint))", "");
  TestStringValue("repeat('ab', 0)", "");
  TestStringValue("repeat('ab', -1)", "");
  TestStringValue("repeat('ab', -100)", "");
  TestStringValue("repeat('ab', 1)", "ab");
  TestStringValue("repeat('ab', cast(6 as bigint))", "abababababab");
  TestIsNull("repeat(NULL, 6)", TYPE_STRING);
  TestIsNull("repeat('ab', NULL)", TYPE_STRING);
  TestIsNull("repeat(NULL, NULL)", TYPE_STRING);

  TestValue("ascii('')", TYPE_INT, 0);
  TestValue("ascii('abcde')", TYPE_INT, 'a');
  TestValue("ascii('Abcde')", TYPE_INT, 'A');
  TestValue("ascii('dddd')", TYPE_INT, 'd');
  TestValue("ascii(' ')", TYPE_INT, ' ');
  TestIsNull("ascii(NULL)", TYPE_INT);

  TestStringValue("lpad('', 0, '')", "");
  TestStringValue("lpad('abc', 0, '')", "");
  TestStringValue("lpad('abc', cast(3 as bigint), '')", "abc");
  TestStringValue("lpad('abc', 2, 'xyz')", "ab");
  TestStringValue("lpad('abc', 6, 'xyz')", "xyzabc");
  TestStringValue("lpad('abc', cast(5 as bigint), 'xyz')", "xyabc");
  TestStringValue("lpad('abc', 10, 'xyz')", "xyzxyzxabc");
  TestIsNull("lpad('abc', -10, 'xyz')", TYPE_STRING);
  TestIsNull("lpad(NULL, 10, 'xyz')", TYPE_STRING);
  TestIsNull("lpad('abc', NULL, 'xyz')", TYPE_STRING);
  TestIsNull("lpad('abc', 10, NULL)", TYPE_STRING);
  TestIsNull("lpad(NULL, NULL, NULL)", TYPE_STRING);
  TestStringValue("rpad('', 0, '')", "");
  TestStringValue("rpad('abc', 0, '')", "");
  TestStringValue("rpad('abc', cast(3 as bigint), '')", "abc");
  TestStringValue("rpad('abc', 2, 'xyz')", "ab");
  TestStringValue("rpad('abc', 6, 'xyz')", "abcxyz");
  TestStringValue("rpad('abc', cast(5 as bigint), 'xyz')", "abcxy");
  TestStringValue("rpad('abc', 10, 'xyz')", "abcxyzxyzx");
  TestIsNull("rpad('abc', -10, 'xyz')", TYPE_STRING);
  TestIsNull("rpad(NULL, 10, 'xyz')", TYPE_STRING);
  TestIsNull("rpad('abc', NULL, 'xyz')", TYPE_STRING);
  TestIsNull("rpad('abc', 10, NULL)", TYPE_STRING);
  TestIsNull("rpad(NULL, NULL, NULL)", TYPE_STRING);

  // Note that Hive returns positions starting from 1.
  // Hive returns 0 if substr was not found in str (or on other error coditions).
  TestValue("instr('', '')", TYPE_INT, 0);
  TestValue("instr('', 'abc')", TYPE_INT, 0);
  TestValue("instr('abc', '')", TYPE_INT, 0);
  TestValue("instr('abcdef', 'xyz')", TYPE_INT, 0);
  TestValue("instr('xyz', 'abcdef')", TYPE_INT, 0);
  TestValue("instr('abc', 'abc')", TYPE_INT, 1);
  TestValue("instr('xyzabc', 'abc')", TYPE_INT, 4);
  TestValue("instr('xyzabcxyz', 'bcx')", TYPE_INT, 5);

  TestValue("instr('', '', -1)", TYPE_INT, 0);
  TestValue("instr('', 'abc', -1)", TYPE_INT, 0);
  TestValue("instr('abc', '', -1)", TYPE_INT, 0);
  TestValue("instr('abc', 'abc', -1)", TYPE_INT, 1);
  TestValue("instr('xyzabc', 'abc', -1)", TYPE_INT, 4);
  TestValue("instr('xyzabcxyz', 'bcx', -1)", TYPE_INT, 5);

  TestValue("instr('corporate floor', 'or', 0)", TYPE_INT, 0);
  TestValue("instr('corporate floor', 'or', 14)", TYPE_INT, 14);
  TestValue("instr('corporate floor', 'or', 15)", TYPE_INT, 0);
  TestValue("instr('corporate floor', 'or', -14)", TYPE_INT, 2);
  TestValue("instr('corporate floor', 'or', -15)", TYPE_INT, 0);

  TestValue("instr('corporate floor', 'or', 0, 1)", TYPE_INT, 0);
  TestValue("instr('corporate floor', 'or', 14, 1)", TYPE_INT, 14);
  TestValue("instr('corporate floor', 'or', 15, 1)", TYPE_INT, 0);
  TestValue("instr('corporate floor', 'or', -14, 1)", TYPE_INT, 2);
  TestValue("instr('corporate floor', 'or', -15, 1)", TYPE_INT, 0);

  TestValue("instr('corporate floor', 'or', 2, 2)", TYPE_INT, 5);
  TestValue("instr('corporate floor', 'or', 3, 2)", TYPE_INT, 14);
  TestValue("instr('corporate floor', 'or', -3, 2)", TYPE_INT, 2);
  TestValue("instr('corporate floor', 'or', -2, 2)", TYPE_INT, 5);

  TestValue("instr('corporate floor', 'or', 3, 3)", TYPE_INT, 0);
  TestValue("instr('corporate floor', 'or', -3, 3)", TYPE_INT, 0);
  TestValue("instr('corporate floor', '', 3, 3)", TYPE_INT, 0);
  TestValue("instr('corporate floor', '', -3, 3)", TYPE_INT, 0);

  TestValue("instr('abababa', 'aba', 1, 1)", TYPE_INT, 1);
  TestValue("instr('abababa', 'aba', 1, 2)", TYPE_INT, 3);
  TestValue("instr('abababa', 'aba', 1, 3)", TYPE_INT, 5);
  TestValue("instr('abababa', 'aba', cast(1 as bigint), cast(3 as bigint))", TYPE_INT, 5);

  TestError("instr('corporate floor', 'or', 0, 0)");
  TestError("instr('corporate floor', 'or', 0, -1)");
  TestError("instr('corporate floor', 'or', 1, 0)");
  TestError("instr('corporate floor', 'or', 1, -1)");

  TestIsNull("instr(NULL, 'or', 2)", TYPE_INT);
  TestIsNull("instr('corporate floor', NULL, 2)", TYPE_INT);
  TestIsNull("instr('corporate floor', 'or', NULL)", TYPE_INT);

  TestIsNull("instr(NULL, 'or', 2, 2)", TYPE_INT);
  TestIsNull("instr('corporate floor', NULL, 2, 2)", TYPE_INT);
  TestIsNull("instr('corporate floor', 'or', NULL, 2)", TYPE_INT);
  TestIsNull("instr('corporate floor', 'or', 2, NULL)", TYPE_INT);

  TestValue("instr('a', 'a', 1, 1)", TYPE_INT, 1);
  TestValue("instr('axyz', 'a', 1, 1)", TYPE_INT, 1);
  TestValue("instr('xyza', 'a', 1, 1)", TYPE_INT, 4);
  TestValue("instr('a', 'a', 1, 2)", TYPE_INT, 0);
  TestValue("instr('axyz', 'a', 1, 2)", TYPE_INT, 0);
  TestValue("instr('xyza', 'a', 1, 2)", TYPE_INT, 0);

  TestValue("instr('a', 'a', -1, 1)", TYPE_INT, 1);
  TestValue("instr('axyz', 'a', -1, 1)", TYPE_INT, 1);
  TestValue("instr('xyza', 'a', -1, 1)", TYPE_INT, 4);
  TestValue("instr('a', 'a', -1, 2)", TYPE_INT, 0);
  TestValue("instr('axyz', 'a', -1, 2)", TYPE_INT, 0);
  TestValue("instr('xyza', 'a', -1, 2)", TYPE_INT, 0);

  TestIsNull("instr(NULL, 'bcx')", TYPE_INT);
  TestIsNull("instr('xyzabcxyz', NULL)", TYPE_INT);
  TestIsNull("instr(NULL, NULL)", TYPE_INT);
  TestValue("locate('', '')", TYPE_INT, 0);
  TestValue("locate('abc', '')", TYPE_INT, 0);
  TestValue("locate('', 'abc')", TYPE_INT, 0);
  TestValue("locate('abc', 'abc')", TYPE_INT, 1);
  TestValue("locate('abc', 'xyzabc')", TYPE_INT, 4);
  TestValue("locate('bcx', 'xyzabcxyz')", TYPE_INT, 5);
  TestIsNull("locate(NULL, 'xyzabcxyz')", TYPE_INT);
  TestIsNull("locate('bcx', NULL)", TYPE_INT);
  TestIsNull("locate(NULL, NULL)", TYPE_INT);

  // Test locate with starting pos param.
  // Note that Hive expects positions starting from 1 as input.
  TestValue("locate('', '', 0)", TYPE_INT, 0);
  TestValue("locate('abc', '', cast(0 as bigint))", TYPE_INT, 0);
  TestValue("locate('', 'abc', 0)", TYPE_INT, 0);
  TestValue("locate('', 'abc', -1)", TYPE_INT, 0);
  TestValue("locate('', '', 1)", TYPE_INT, 0);
  TestValue("locate('', 'abcde', cast(10 as bigint))", TYPE_INT, 0);
  TestValue("locate('abcde', 'abcde', -1)", TYPE_INT, 0);
  TestValue("locate('abcde', 'abcde', 10)", TYPE_INT, 0);
  TestValue("locate('abc', 'abcdef', 0)", TYPE_INT, 0);
  TestValue("locate('abc', 'abcdef', 1)", TYPE_INT, 1);
  TestValue("locate('abc', 'xyzabcdef', 3)", TYPE_INT, 4);
  TestValue("locate('abc', 'xyzabcdef', 4)", TYPE_INT, 4);
  TestValue("locate('abc', 'abcabcabc', cast(5 as bigint))", TYPE_INT, 7);
  TestIsNull("locate(NULL, 'abcabcabc', 5)", TYPE_INT);
  TestIsNull("locate('abc', NULL, 5)", TYPE_INT);
  TestIsNull("locate('abc', 'abcabcabc', NULL)", TYPE_INT);
  TestIsNull("locate(NULL, NULL, NULL)", TYPE_INT);

  TestStringValue("concat('a')", "a");
  TestStringValue("concat('a', 'b')", "ab");
  TestStringValue("concat('a', 'b', 'cde')", "abcde");
  TestStringValue("concat('a', 'b', 'cde', 'fg')", "abcdefg");
  TestStringValue("concat('a', 'b', 'cde', '', 'fg', '')", "abcdefg");
  TestIsNull("concat(NULL)", TYPE_STRING);
  TestIsNull("concat('a', NULL, 'b')", TYPE_STRING);
  TestIsNull("concat('a', 'b', NULL)", TYPE_STRING);

  TestStringValue("concat_ws(',', 'a')", "a");
  TestStringValue("concat_ws(',', 'a', 'b')", "a,b");
  TestStringValue("concat_ws(',', 'a', 'b', 'cde')", "a,b,cde");
  TestStringValue("concat_ws('', 'a', '', 'b', 'cde')", "abcde");
  TestStringValue("concat_ws('%%', 'a', 'b', 'cde', 'fg')", "a%%b%%cde%%fg");
  TestStringValue("concat_ws('|','a', 'b', 'cde', '', 'fg', '')", "a|b|cde||fg|");
  TestStringValue("concat_ws('', '', '', '')", "");
  TestIsNull("concat_ws(NULL, NULL)", TYPE_STRING);
  TestIsNull("concat_ws(',', NULL, 'b')", TYPE_STRING);
  TestIsNull("concat_ws(',', 'b', NULL)", TYPE_STRING);

  TestValue("find_in_set('ab', 'ab,ab,ab,ade,cde')", TYPE_INT, 1);
  TestValue("find_in_set('ab', 'abc,xyz,abc,ade,ab')", TYPE_INT, 5);
  TestValue("find_in_set('ab', 'abc,ad,ab,ade,cde')", TYPE_INT, 3);
  TestValue("find_in_set('xyz', 'abc,ad,ab,ade,cde')", TYPE_INT, 0);
  TestValue("find_in_set('ab', ',,,,ab,,,,')", TYPE_INT, 5);
  TestValue("find_in_set('', ',ad,ab,ade,cde')", TYPE_INT,1);
  TestValue("find_in_set('', 'abc,ad,ab,ade,,')", TYPE_INT, 5);
  TestValue("find_in_set('', 'abc,ad,,ade,cde,')", TYPE_INT,3);
  // First param contains comma.
  TestValue("find_in_set('abc,def', 'abc,ad,,ade,cde,')", TYPE_INT, 0);
  TestIsNull("find_in_set(NULL, 'abc,ad,,ade,cde')", TYPE_INT);
  TestIsNull("find_in_set('abc,def', NULL)", TYPE_INT);
  TestIsNull("find_in_set(NULL, NULL)", TYPE_INT);

  TestStringValue("cast('HELLO' as VARCHAR(3))", "HEL");
  TestStringValue("cast('HELLO' as VARCHAR(15))", "HELLO");
  TestStringValue("lower(cast('HELLO' as VARCHAR(3)))", "hel");
  TestStringValue("lower(cast(123456 as VARCHAR(3)))", "123");
  TestIsNull("cast(NULL as VARCHAR(3))", TYPE_STRING);
  TestCharValue("cast('12345' as CHAR(130))",
      "12345                                                                  "
      "                                                           ",
      ColumnType::CreateCharType(130));

  TestCharValue("cast(cast('HELLO' as VARCHAR(3)) as CHAR(3))", "HEL",
                ColumnType::CreateCharType(3));
  TestStringValue("cast(cast('HELLO' as CHAR(3)) as VARCHAR(3))", "HEL");
  TestCharValue("cast(cast('HELLO' as VARCHAR(7)) as CHAR(7))", "HELLO  ",
                ColumnType::CreateCharType(7));
  TestCharValue("cast(cast('HELLO' as STRING) as CHAR(7))", "HELLO  ",
                ColumnType::CreateCharType(7));
  TestStringValue("cast(cast('HELLO' as CHAR(7)) as VARCHAR(7))", "HELLO  ");
  TestStringValue("cast(cast('HELLO' as CHAR(5)) as VARCHAR(3))", "HEL");
  TestCharValue("cast(cast('HELLO' as VARCHAR(7)) as CHAR(3))", "HEL",
                ColumnType::CreateCharType(3));

  TestCharValue("cast(5 as char(5))", "5    ", ColumnType::CreateCharType(5));
  TestCharValue("cast(5.1 as char(5))", "5.1  ", ColumnType::CreateCharType(5));
  TestCharValue("cast(cast(1 as decimal(2,1)) as char(5))", "1.0  ",
                ColumnType::CreateCharType(5));
  TestCharValue("cast(cast('2014-09-30 10:35:10.632995000' as TIMESTAMP) as char(35))",
                "2014-09-30 10:35:10.632995000      ",
                ColumnType::CreateCharType(35));

  TestCharValue("cast('HELLO' as CHAR(3))", "HEL",
                ColumnType::CreateCharType(3));
  TestCharValue("cast('HELLO' as CHAR(7))", "HELLO  ",
                ColumnType::CreateCharType(7));
  TestCharValue("cast('HELLO' as CHAR(70))",
      "HELLO                                                                 ",
      ColumnType::CreateCharType(70));
  TestValue("cast('HELLO' as CHAR(7)) = 'HELLO  '", TYPE_BOOLEAN, true);
  TestValue("cast('HELLO' as CHAR(7)) = cast('HELLO' as CHAR(5))", TYPE_BOOLEAN, true);
  TestStringValue("lower(cast('HELLO' as CHAR(3)))", "hel");
  TestStringValue("lower(cast(123456 as CHAR(3)))", "123");
  TestStringValue("cast(cast(123456 as CHAR(3)) as VARCHAR(3))", "123");
  TestStringValue("cast(cast(123456 as CHAR(3)) as VARCHAR(65535))", "123");
  TestIsNull("cast(NULL as CHAR(3))", ColumnType::CreateCharType(3));

  TestCharValue("cast('HELLO' as CHAR(255))",
      "HELLO                                                                        "
      "                                                                             "
      "                                                                             "
      "                        ", ColumnType::CreateCharType(255));

  /*
  TestCharValue("CASE cast('1.1' as char(3)) when cast('1.1' as char(3)) then "
      "cast('1' as char(1)) when cast('2.22' as char(4)) then "
      "cast('2' as char(1)) else cast('3' as char(1)) end", "1",
      ColumnType::CreateCharType(3));
  */

  // Test maximum VARCHAR value
  char query[ColumnType::MAX_VARCHAR_LENGTH + 1024];
  char big_str[ColumnType::MAX_VARCHAR_LENGTH+1];
  for (int i = 0 ; i < ColumnType::MAX_VARCHAR_LENGTH; i++) {
    big_str[i] = 'a';
  }
  big_str[ColumnType::MAX_VARCHAR_LENGTH] = '\0';
  sprintf(query, "cast('%sxxx' as VARCHAR(%d))", big_str, ColumnType::MAX_VARCHAR_LENGTH);
  TestStringValue(query, big_str);
}

TEST_F(ExprTest, StringBase64Coding) {
  // Test some known values of base64{en,de}code
  TestIsNull("base64encode(NULL)", TYPE_STRING);
  TestIsNull("base64decode(NULL)", TYPE_STRING);
  TestStringValue("base64encode('')", "");
  TestStringValue("base64decode('')", "");
  TestStringValue("base64encode('a')","YQ==");
  TestStringValue("base64decode('YQ==')","a");
  TestStringValue("base64encode('alpha')","YWxwaGE=");
  TestStringValue("base64decode('YWxwaGE=')","alpha");
  TestIsNull("base64decode('YWxwaGE')", TYPE_STRING);
  TestIsNull("base64decode('YWxwaGE%')", TYPE_STRING);

  // Test random short strings.
  srand(0);
  // Pick some 'interesting' (i.e. random, but include some powers of two, some primes,
  // and edge-cases) lengths to test.
  for (int length: {1, 2, 3, 5, 8, 32, 42, 50, 64, 71, 89, 99}) {
    for (int iteration = 0; iteration < 10; ++iteration) {
      string raw(length, ' ');
      for (int j = 0; j < length; ++j) raw[j] = rand() % 128;
      const string as_octal = StringToOctalLiteral(raw);
      TestValue("length(base64encode('" + as_octal + "')) > length('" + as_octal + "')",
          TYPE_BOOLEAN, true);
      TestValue("base64decode(base64encode('" + as_octal + "')) = '" + as_octal + "'",
          TYPE_BOOLEAN, true);
    }
  }
}

TEST_F(ExprTest, LongReverse) {
  static const int MAX_LEN = 2048;
  string to_reverse(MAX_LEN, ' '), reversed(MAX_LEN, ' ');
  // Pick some 'interesting' (i.e. random, but include some powers of two, some primes,
  // and edge-cases) lengths to test.
  for (int i: {1, 2, 3, 5, 8, 32, 42, 512, 1024, 1357, 1788, 2012, 2047}) {
    to_reverse[i] = reversed[MAX_LEN - 1 - i] = 'a' + (rand() % 26);
    TestStringValue("reverse('" + to_reverse.substr(0, i + 1) + "')",
        reversed.substr(MAX_LEN - 1 - i));
  }
}

TEST_F(ExprTest, StringRegexpFunctions) {
  // Single group.
  TestStringValue("regexp_extract('abxcy1234a', 'a.x', 0)", "abx");
  TestStringValue("regexp_extract('abxcy1234a', 'a.x.*a', 0)", "abxcy1234a");
  TestStringValue("regexp_extract('abxcy1234a', 'a.x.y.*a', 0)", "abxcy1234a");
  TestStringValue("regexp_extract('a.x.y.*a',"
      "'a\\\\.x\\\\.y\\\\.\\\\*a', 0)", "a.x.y.*a");
  TestStringValue("regexp_extract('abxcy1234a', 'abczy', cast(0 as bigint))", "");
  TestStringValue("regexp_extract('abxcy1234a', 'a\\\\.x\\\\.y\\\\.\\\\*a', 0)", "");
  TestStringValue("regexp_extract('axcy1234a', 'a.x.y.*a', 0)","");
  // Accessing non-existant group should return empty string.
  TestStringValue("regexp_extract('abxcy1234a', 'a.x', cast(2 as bigint))", "");
  TestStringValue("regexp_extract('abxcy1234a', 'a.x.*a', 1)", "");
  TestStringValue("regexp_extract('abxcy1234a', 'a.x.y.*a', 5)", "");
  TestStringValue("regexp_extract('abxcy1234a', 'a.x', -1)", "");
  // Multiple groups enclosed in ().
  TestStringValue("regexp_extract('abxcy1234a', '(a.x)(.y.*)(3.*a)', 0)", "abxcy1234a");
  TestStringValue("regexp_extract('abxcy1234a', '(a.x)(.y.*)(3.*a)', 1)", "abx");
  TestStringValue("regexp_extract('abxcy1234a', '(a.x)(.y.*)(3.*a)', 2)", "cy12");
  TestStringValue("regexp_extract('abxcy1234a', '(a.x)(.y.*)(3.*a)',"
      "cast(3 as bigint))", "34a");
  TestStringValue("regexp_extract('abxcy1234a', '(a.x)(.y.*)(3.*a)',"
      "cast(4 as bigint))", "");
  // Empty strings.
  TestStringValue("regexp_extract('', '', 0)", "");
  TestStringValue("regexp_extract('abxcy1234a', '', 0)", "");
  TestStringValue("regexp_extract('', 'abx', 0)", "");
  // Test finding of leftmost maximal match.
  TestStringValue("regexp_extract('I001=-200,I003=-210,I007=0', 'I001=-?[0-9]+', 0)",
      "I001=-200");
  // Invalid regex pattern, unmatched parenthesis.
  TestError("regexp_extract('abxcy1234a', '(/.', 0)");
  // NULL arguments.
  TestIsNull("regexp_extract(NULL, 'a.x', 2)", TYPE_STRING);
  TestIsNull("regexp_extract('abxcy1234a', NULL, 2)", TYPE_STRING);
  TestIsNull("regexp_extract('abxcy1234a', 'a.x', NULL)", TYPE_STRING);
  TestIsNull("regexp_extract(NULL, NULL, NULL)", TYPE_STRING);
  // Character classes.
  TestStringValue("regexp_extract('abxcy1234a', '[[:lower:]]*', 0)", "abxcy");
  TestStringValue("regexp_extract('abxcy1234a', '[[:digit:]]+', 0)", "1234");
  TestStringValue("regexp_extract('abxcy1234a', '[[:lower:]][[:digit:]]', 0)", "y1");
  TestStringValue("regexp_extract('aBcDeF', '[[:upper:]][[:lower:]]', 0)", "Bc");
  // "Single character" character classes.
  TestStringValue("regexp_extract('abxcy1234a', '\\\\w*', 0)", "abxcy1234a");
  TestStringValue("regexp_extract('abxcy1234a', '\\\\d+', 0)", "1234");
  TestStringValue("regexp_extract('abxcy1234a', '\\\\d\\\\D', 0)", "4a");
  // Leftmost longest match.
  TestStringValue("regexp_extract('abcabcd', '(a|ab|abc|abcd)', 0)", "abc");

  TestStringValue("regexp_replace('axcaycazc', 'a.c', 'a')", "aaa");
  TestStringValue("regexp_replace('axcaycazc', 'a.c', '')", "");
  TestStringValue("regexp_replace('axcaycazc', 'a.*', 'abcde')", "abcde");
  TestStringValue("regexp_replace('axcaycazc', 'a.*y.*z', 'xyz')", "xyzc");
  // No match for pattern.
  TestStringValue("regexp_replace('axcaycazc', 'a.z', 'xyz')", "axcaycazc");
  TestStringValue("regexp_replace('axcaycazc', 'a.*y.z', 'xyz')", "axcaycazc");
  // Empty strings.
  TestStringValue("regexp_replace('', '', '')", "");
  TestStringValue("regexp_replace('axcaycazc', '', '')", "axcaycazc");
  TestStringValue("regexp_replace('', 'err', '')", "");
  TestStringValue("regexp_replace('', '', 'abc')", "abc");
  TestStringValue("regexp_replace('axcaycazc', '', 'r')", "rarxrcraryrcrarzrcr");
  // Invalid regex pattern, unmatched parenthesis.
  TestError("regexp_replace('abxcy1234a', '(/.', 'x')");
  // NULL arguments.
  TestIsNull("regexp_replace(NULL, 'a.*', 'abcde')", TYPE_STRING);
  TestIsNull("regexp_replace('axcaycazc', NULL, 'abcde')", TYPE_STRING);
  TestIsNull("regexp_replace('axcaycazc', 'a.*', NULL)", TYPE_STRING);
  TestIsNull("regexp_replace(NULL, NULL, NULL)", TYPE_STRING);

  TestValue("regexp_like('abcabcd', '(a|ab|abc|abcd)')", TYPE_BOOLEAN, true);
  TestValue("regexp_like('axcayczc', 'a.*')", TYPE_BOOLEAN, true);
  TestValue("regexp_like('axcayczc', 'a.*y.*z')", TYPE_BOOLEAN, true);
  TestValue("regexp_like('lee', '[aEiou]{2}', 'i')", TYPE_BOOLEAN, true);
  TestValue("regexp_like('this\nis\nnewline', '^new.*$', 'm')", TYPE_BOOLEAN, true);
  TestValue("regexp_like('this\nis\nnewline', '^new.*$', 'n')", TYPE_BOOLEAN, false);
  TestValue("regexp_like('this\nis\nnewline', '^.*$')", TYPE_BOOLEAN, false);
  TestValue("regexp_like('this\nis\nnewline', '^.*$', 'n')", TYPE_BOOLEAN, true);
  TestError("regexp_like('abcabcdef', '*')");
  TestError("regexp_like('abcabcdef', '.*', 'qpl')");
  TestIsNull("regexp_like(NULL, NULL, NULL)", TYPE_BOOLEAN);
  TestIsNull("regexp_like(NULL, NULL)", TYPE_BOOLEAN);

  TestValue("regexp_match_count('aaa', 'a')", TYPE_INT, 3);
  TestValue("regexp_match_count('aaa', 'aa')", TYPE_INT, 1);
  TestValue("regexp_match_count('aaaa', 'aa')", TYPE_INT, 2);
  TestValue("regexp_match_count('', '')", TYPE_INT, 1);
  TestValue("regexp_match_count('', '.*')", TYPE_INT, 1);
  TestValue("regexp_match_count('abxcy1234a', 'a.x')", TYPE_INT, 1);
  TestValue("regexp_match_count('abxcy1234a', 'a.x.*a')", TYPE_INT, 1);
  TestValue("regexp_match_count('abxcy1234a', 'a.x.*k')", TYPE_INT, 0);
  TestValue("regexp_match_count('aaa123a', 'a*')", TYPE_INT, 6);
  TestValue("regexp_match_count('aaa123a', 'a?')", TYPE_INT, 8);
  TestValue("regexp_match_count('a.x.y.*a', 'a\\\\.x\\\\.y\\\\.\\\\*a')", TYPE_INT, 1);
  TestValue("regexp_match_count('0123456789', '.*')", TYPE_INT, 2);
  TestValue("regexp_match_count('0123456789', '.+')", TYPE_INT, 1);
  TestValue("regexp_match_count('0123456789', '.?')", TYPE_INT, 11);
  TestValue("regexp_match_count('abcab', '(a|bc|abc)')", TYPE_INT, 2);
  TestValue("regexp_match_count('abcab', '(a)b')", TYPE_INT, 2);
  TestValue("regexp_match_count('abc123efg', '[\\\\d]')", TYPE_INT, 3);
  TestValue("regexp_match_count('abc123efg', '[\\\\d]+')", TYPE_INT, 1);
  TestValue("regexp_match_count('abc123efg', '[\\^\\\\d]')", TYPE_INT, 6);
  TestValue("regexp_match_count('a1b2c3d4e5!!!', '[\\\\w\\\\d]')", TYPE_INT, 10);
  TestValue("regexp_match_count('a1b2c3d4e5!!!', '\\\\w\\\\d')", TYPE_INT, 5);
  TestValue("regexp_match_count('Steven and Stephen', 'Ste(v|ph)en')", TYPE_INT, 2);
  TestValue("regexp_match_count('aaa', 'A', 1, 'i')", TYPE_INT, 3);
  TestValue("regexp_match_count('aaa', 'A', 1, 'c')", TYPE_INT, 0);
  TestValue("regexp_match_count('this\nis\nnewline', '.*', 1, '')", TYPE_INT, 6);
  TestValue("regexp_match_count('this\nis\nnewline', '.*', 1, 'n')", TYPE_INT, 2);
  TestValue("regexp_match_count('IPhone\nIPad\nIPod', '^I.*$', 1, '')", TYPE_INT, 0);
  TestValue("regexp_match_count('IPhone\nIPad\nIPod', '^I.*$', 1, 'n')", TYPE_INT, 1);
  TestValue("regexp_match_count('IPhone\nIPad\nIPod', '^I.*$', 1, 'm')", TYPE_INT, 3);
  TestValue("regexp_match_count('iPhone\niPad\niPod', '^I.*$', 1, 'in')", TYPE_INT, 1);
  TestValue("regexp_match_count('iPhone\niPad\niPod', '^I.*$', 1, 'cin')", TYPE_INT, 1);
  TestValue("regexp_match_count('iPhone\niPad\niPod', '^I.*$', 1, 'im')", TYPE_INT, 3);
  TestValue("regexp_match_count('iPhone\niPad\niPod', '^I.*$', 1, 'imn')", TYPE_INT, 1);
  TestValue("regexp_match_count('aaa', 'a', 3, '')", TYPE_INT, 1);
  TestValue("regexp_match_count('aaa', 'a', 4, '')", TYPE_INT, 0);
  TestValue("regexp_match_count('aaa', 'a*', 4, '')", TYPE_INT, 1);
  TestValue("regexp_match_count('aaa', 'a+', NULL, NULL)", TYPE_INT, 1);
  TestValue("regexp_match_count('abc123', '(.)(.)')", TYPE_INT, 3);
  TestValue("regexp_match_count('.)(.', '.\\\\)\\\\(.')",  TYPE_INT, 1);
  TestError("regexp_match_count('.)(.', '.)(.')");
  TestError("regexp_match_count('a', 'a', 0, '')");
  TestError("regexp_match_count('a', 'a', -1, '')");
  TestError("regexp_match_count('a', 'a', 1, 'a123efgyz')");
  TestError("regexp_match_count('a', 'a', 1, 'cimnhk')");
  TestError("regexp_match_count('a', 'a', -1, '')");
  TestError("regexp_match_count(1, 1");
  TestError("regexp_match_count(1, 1, 1, '')");
  TestIsNull("regexp_match_count(NULL, '.*')", TYPE_INT);
  TestIsNull("regexp_match_count('a123', NULL)", TYPE_INT);
  TestIsNull("regexp_match_count(NULL, NULL)", TYPE_INT);
}

TEST_F(ExprTest, StringParseUrlFunction) {
  // TODO: For now, our parse_url my not behave exactly like Hive
  // when given malformed URLs.
  // If necessary, we can closely follow Java's URL implementation
  // to behave exactly like Hive.

  // AUTHORITY part.
  TestStringValue("parse_url('http://example.com', 'AUTHORITY')", "example.com");
  TestStringValue("parse_url('http://example.com:80', 'AUTHORITY')", "example.com:80");
  TestStringValue("parse_url('http://user:pass@example.com', 'AUTHORITY')",
      "user:pass@example.com");
  TestStringValue("parse_url('http://user:pass@example.com:80', 'AUTHORITY')",
      "user:pass@example.com:80");
  TestStringValue("parse_url('http://user:pass@example.com:80/docs/books/tutorial/"
      "index.html?name=networking#DOWNLOADING', 'AUTHORITY')",
      "user:pass@example.com:80");
  TestStringValue("parse_url('http://user:pass@example.com/docs/books/tutorial/"
      "index.html?name=networking#DOWNLOADING', 'AUTHORITY')", "user:pass@example.com");
  // Without user and pass.
  TestStringValue("parse_url('http://example.com:80/docs/books/tutorial/"
      "index.html?name=networking#DOWNLOADING', 'AUTHORITY')", "example.com:80");
  TestStringValue("parse_url('http://example.com/docs/books/tutorial/"
      "index.html?name=networking#DOWNLOADING', 'AUTHORITY')", "example.com");
  // Exactly what Hive returns as well.
  TestStringValue("parse_url('http://example.com_xyzabc^&*', 'AUTHORITY')",
      "example.com_xyzabc^&*");
  // Missing slash at the end of the authority.
  TestStringValue("parse_url('http://example.com:80?name=networking#DOWNLOADING',"
      "'AUTHORITY')", "example.com:80");
  // Missing protocol.
  TestIsNull("parse_url('example.com/docs/books/tutorial/"
      "index.html?name=networking#DOWNLOADING', 'HOST')", TYPE_STRING);

  // FILE part.
  TestStringValue("parse_url('http://user:pass@example.com:80/docs/books/tutorial/"
      "index.html?name=networking#DOWNLOADING', 'FILE')",
      "/docs/books/tutorial/index.html?name=networking");
  TestStringValue("parse_url('http://user:pass@example.com/docs/books/tutorial/"
      "index.html?name=networking#DOWNLOADING', 'FILE')",
      "/docs/books/tutorial/index.html?name=networking");
  // Without user and pass.
  TestStringValue("parse_url('http://example.com:80/docs/books/tutorial/"
      "index.html?name=networking#DOWNLOADING', 'FILE')",
      "/docs/books/tutorial/index.html?name=networking");
  TestStringValue("parse_url('http://example.com/docs/books/tutorial/"
      "index.html?name=networking#DOWNLOADING', 'FILE')",
      "/docs/books/tutorial/index.html?name=networking");
  // With trimming.
  TestStringValue("parse_url('http://example.com/docs/books/tutorial/"
      "index.html?name=networking   ', 'FILE')",
      "/docs/books/tutorial/index.html?name=networking");
  // No question mark but a hash (consistent with Hive).
  TestStringValue("parse_url('http://example.com/docs/books/tutorial/"
      "index.html#something', 'FILE')",
      "/docs/books/tutorial/index.html");
  // No hash or question mark (consistent with Hive).
  TestStringValue("parse_url('http://example.com/docs/books/tutorial/"
      "index.htmlsomething', 'FILE')",
      "/docs/books/tutorial/index.htmlsomething");
  // Missing protocol.
  TestIsNull("parse_url('example.com/docs/books/tutorial/"
      "index.html?name=networking#DOWNLOADING', 'FILE')", TYPE_STRING);

  // HOST part.
  TestStringValue("parse_url('http://user:pass@example.com:80/docs/books/tutorial/"
      "index.html?name=networking#DOWNLOADING', 'HOST')", "example.com");
  TestStringValue("parse_url('http://user:pass@example.com/docs/books/tutorial/"
      "index.html?name=networking#DOWNLOADING', 'HOST')", "example.com");
  // Without user and pass.
  TestStringValue("parse_url('http://example.com:80/docs/books/tutorial/"
      "index.html?name=networking#DOWNLOADING', 'HOST')", "example.com");
  TestStringValue("parse_url('http://example.com/docs/books/tutorial/"
      "index.html?name=networking#DOWNLOADING', 'HOST')", "example.com");
  // Exactly what Hive returns as well.
  TestStringValue("parse_url('http://example.com_xyzabc^&*', 'HOST')",
      "example.com_xyzabc^&*");
  // Colon after host:port/ (with port)
  TestStringValue("parse_url('http://user:pass@example.com:80/docs/books/tutorial/"
      "index.html?name:networking#DOWNLOADING', 'HOST')", "example.com");
  // Colon after host/ (without port)
  TestStringValue("parse_url('http://user:pass@example.com/docs/books/tutorial/"
      "index.html?name:networking#DOWNLOADING', 'HOST')", "example.com");
  // Colon in query without '/' no port
  TestStringValue("parse_url('http://user:pass@example.com"
      "?name:networking#DOWNLOADING', 'HOST')", "example.com");
  // Colon in query without '/' with port
  TestStringValue("parse_url('http://user:pass@example.com:80"
      "?name:networking#DOWNLOADING', 'HOST')", "example.com");
  // '/' in query
  TestStringValue("parse_url('http://user:pass@example.com:80"
      "?name:networking/DOWNLOADING', 'HOST')", "example.com");
  // Missing protocol.
  TestIsNull("parse_url('example.com/docs/books/tutorial/"
      "index.html?name=networking#DOWNLOADING', 'HOST')", TYPE_STRING);

  // IMPALA-1170: '@' character in PATH
  TestStringValue("parse_url('http://example.com/index.html@Top2,Right1',"
      "'HOST')", "example.com");
  TestStringValue("parse_url('http://user:pass@example.com/index.html@Top2,Right1',"
      "'HOST')", "example.com");
  TestStringValue("parse_url('http://user:pass@example.com/index.html?mail=foo@bar',"
      "'HOST')", "example.com");
  // '@' in path
  TestStringValue("parse_url('http://example.com/index.html@Top2,Right1',"
      "'FILE')", "/index.html@Top2,Right1");
  TestStringValue("parse_url('http://user:pass@example.com/index.html@Top2,Right1',"
      "'FILE')", "/index.html@Top2,Right1");
  // '@' in query
  TestStringValue("parse_url('http://example.com/index.html@Top2,Right1?foo@bar',"
      "'QUERY')", "foo@bar");
  TestStringValue("parse_url('http://user:pass@example.com/index.html@Top2,Right1?foo@bar',"
      "'QUERY')", "foo@bar");
  // '?' before '/'
  TestStringValue("parse_url('http://user:pass@example.com?dir=/etc',"
      "'HOST')", "example.com");
  TestStringValue("parse_url('http://user:pass@example.com?dir=/etc',"
      "'QUERY')", "dir=/etc");
  // '@' in forbidden places
  // TODO: Return NULL for invalid URLs like the ones below. Our parser currently does not
  // validate URLs.
  TestStringValue("parse_url('htt@p://example.com/docs',"
      "'PROTOCOL')", "htt@p");
  TestStringValue("parse_url('htt@p://example.com/docs',"
      "'HOST')", "example.com");
  TestStringValue("parse_url('http://foo@baa@example.com/docs',"
      "'HOST')", "baa@example.com");
  TestStringValue("parse_url('http://foo@baa@example.com/docs',"
      "'USERINFO')", "foo");

  // PATH part.
  TestStringValue("parse_url('http://user:pass@example.com:80/docs/books/tutorial/"
      "index.html?name=networking#DOWNLOADING', 'PATH')",
      "/docs/books/tutorial/index.html");
  TestStringValue("parse_url('http://user:pass@example.com/docs/books/tutorial/"
      "index.html?name=networking#DOWNLOADING', 'PATH')",
      "/docs/books/tutorial/index.html");
  // Without user and pass.
  TestStringValue("parse_url('http://example.com:80/docs/books/tutorial/"
      "index.html?name=networking#DOWNLOADING', 'PATH')",
      "/docs/books/tutorial/index.html");
  TestStringValue("parse_url('http://example.com/docs/books/tutorial/"
      "index.html?name=networking#DOWNLOADING', 'PATH')",
      "/docs/books/tutorial/index.html");
  // With trimming.
  TestStringValue("parse_url('http://user:pass@example.com/docs/books/tutorial/"
      "index.html   ', 'PATH')",
      "/docs/books/tutorial/index.html");
  // No question mark but a hash (consistent with Hive).
  TestStringValue("parse_url('http://example.com/docs/books/tutorial/"
      "index.html#something', 'PATH')",
      "/docs/books/tutorial/index.html");
  // No hash or question mark (consistent with Hive).
  TestStringValue("parse_url('http://example.com/docs/books/tutorial/"
      "index.htmlsomething', 'PATH')",
      "/docs/books/tutorial/index.htmlsomething");
  // Missing protocol.
  TestIsNull("parse_url('example.com/docs/books/tutorial/"
      "index.html?name=networking#DOWNLOADING', 'PATH')", TYPE_STRING);

  // PROTOCOL part.
  TestStringValue("parse_url('http://user:pass@example.com:80/docs/books/tutorial/"
      "index.html?name=networking#DOWNLOADING', 'PROTOCOL')", "http");
  TestStringValue("parse_url('https://user:pass@example.com/docs/books/tutorial/"
      "index.html?name=networking#DOWNLOADING', 'PROTOCOL')", "https");
  // Without user and pass.
  TestStringValue("parse_url('http://example.com:80/docs/books/tutorial/"
      "index.html?name=networking#DOWNLOADING', 'PROTOCOL')", "http");
  TestStringValue("parse_url('https://example.com/docs/books/tutorial/"
      "index.html?name=networking#DOWNLOADING', 'PROTOCOL')", "https");
  // With trimming.
  TestStringValue("parse_url('   https://user:pass@example.com/docs/books/tutorial/"
      "index.html?name=networking#DOWNLOADING', 'PROTOCOL')", "https");
  // Missing protocol.
  TestIsNull("parse_url('user:pass@example.com/docs/books/tutorial/"
      "index.html?name=networking#DOWNLOADING', 'PROTOCOL')", TYPE_STRING);

  // QUERY part.
  TestStringValue("parse_url('http://user:pass@example.com:80/docs/books/tutorial/"
      "index.html?name=networking#DOWNLOADING', 'QUERY')", "name=networking");
  TestStringValue("parse_url('http://user:pass@example.com/docs/books/tutorial/"
      "index.html?name=networking#DOWNLOADING', 'QUERY')", "name=networking");
  // Without user and pass.
  TestStringValue("parse_url('http://example.com:80/docs/books/tutorial/"
      "index.html?name=networking#DOWNLOADING', 'QUERY')", "name=networking");
  TestStringValue("parse_url('http://example.com/docs/books/tutorial/"
      "index.html?name=networking#DOWNLOADING', 'QUERY')", "name=networking");
  // With trimming.
  TestStringValue("parse_url('http://example.com/docs/books/tutorial/"
      "index.html?name=networking   ', 'QUERY')", "name=networking");
  // No '?'. Hive also returns NULL.
  TestIsNull("parse_url('http://example.com_xyzabc^&*', 'QUERY')", TYPE_STRING);
  // Missing protocol.
  TestIsNull("parse_url('example.com/docs/books/tutorial/"
      "index.html?name=networking#DOWNLOADING', 'QUERY')", TYPE_STRING);

  // PATH part.
  TestStringValue("parse_url('http://user:pass@example.com:80/docs/books/tutorial/"
      "index.html?name=networking#DOWNLOADING', 'PATH')",
      "/docs/books/tutorial/index.html");
  TestStringValue("parse_url('http://user:pass@example.com/docs/books/tutorial/"
      "index.html?name=networking#DOWNLOADING', 'PATH')",
      "/docs/books/tutorial/index.html");
  // Without user and pass.
  TestStringValue("parse_url('http://example.com:80/docs/books/tutorial/"
      "index.html?name=networking#DOWNLOADING', 'PATH')",
      "/docs/books/tutorial/index.html");
  TestStringValue("parse_url('http://example.com/docs/books/tutorial/"
      "index.html?name=networking#DOWNLOADING', 'PATH')",
      "/docs/books/tutorial/index.html");
  // With trimming.
  TestStringValue("parse_url('http://user:pass@example.com/docs/books/tutorial/"
      "index.html   ', 'PATH')",
      "/docs/books/tutorial/index.html");
  // No question mark but a hash (consistent with Hive).
  TestStringValue("parse_url('http://example.com/docs/books/tutorial/"
      "index.html#something', 'PATH')",
      "/docs/books/tutorial/index.html");
  // No hash or question mark (consistent with Hive).
  TestStringValue("parse_url('http://example.com/docs/books/tutorial/"
      "index.htmlsomething', 'PATH')",
      "/docs/books/tutorial/index.htmlsomething");
  // Missing protocol.
  TestIsNull("parse_url('example.com/docs/books/tutorial/"
      "index.html?name=networking#DOWNLOADING', 'PATH')", TYPE_STRING);

  // USERINFO part.
  TestStringValue("parse_url('http://user:pass@example.com:80/docs/books/tutorial/"
      "index.html?name=networking#DOWNLOADING', 'USERINFO')", "user:pass");
  TestStringValue("parse_url('http://user:pass@example.com/docs/books/tutorial/"
      "index.html?name=networking#DOWNLOADING', 'USERINFO')", "user:pass");
  // Only user given.
  TestStringValue("parse_url('http://user@example.com/docs/books/tutorial/"
      "index.html?name=networking#DOWNLOADING', 'USERINFO')", "user");
  // No user or pass. Hive also returns NULL.
  TestIsNull("parse_url('http://example.com:80/docs/books/tutorial/"
      "index.html?name=networking#DOWNLOADING', 'USERINFO')", TYPE_STRING);
  // No user or pass, but @ in query part (IMPALA-1920). Hive also returns NULL.
  TestIsNull("parse_url('http://example.com:80/docs/books/tutorial/"
      "index.html?name=@networking#DOWNLOADING', 'USERINFO')", TYPE_STRING);
  TestIsNull("parse_url('http://example.com:80?name=@networking#DOWNLOADING',"
      "'USERINFO')", TYPE_STRING);
  // Missing protocol.
  TestIsNull("parse_url('example.com/docs/books/tutorial/"
      "index.html?name=networking#DOWNLOADING', 'USERINFO')", TYPE_STRING);

  // Invalid part parameters.
  // All characters in the part parameter must be uppercase (consistent with Hive).
  TestError("parse_url('http://example.com', 'authority')");
  TestError("parse_url('http://example.com', 'Authority')");
  TestError("parse_url('http://example.com', 'AUTHORITYXYZ')");
  TestError("parse_url('http://example.com', 'file')");
  TestError("parse_url('http://example.com', 'File')");
  TestError("parse_url('http://example.com', 'FILEXYZ')");
  TestError("parse_url('http://example.com', 'host')");
  TestError("parse_url('http://example.com', 'Host')");
  TestError("parse_url('http://example.com', 'HOSTXYZ')");
  TestError("parse_url('http://example.com', 'path')");
  TestError("parse_url('http://example.com', 'Path')");
  TestError("parse_url('http://example.com', 'PATHXYZ')");
  TestError("parse_url('http://example.com', 'protocol')");
  TestError("parse_url('http://example.com', 'Protocol')");
  TestError("parse_url('http://example.com', 'PROTOCOLXYZ')");
  TestError("parse_url('http://example.com', 'query')");
  TestError("parse_url('http://example.com', 'Query')");
  TestError("parse_url('http://example.com', 'QUERYXYZ')");
  TestError("parse_url('http://example.com', 'ref')");
  TestError("parse_url('http://example.com', 'Ref')");
  TestError("parse_url('http://example.com', 'REFXYZ')");
  TestError("parse_url('http://example.com', 'userinfo')");
  TestError("parse_url('http://example.com', 'Userinfo')");
  TestError("parse_url('http://example.com', 'USERINFOXYZ')");

  // NULL arguments.
  TestIsNull("parse_url(NULL, 'AUTHORITY')", TYPE_STRING);
  TestIsNull("parse_url('http://user:pass@example.com:80/docs/books/tutorial/"
          "index.html?name=networking#DOWNLOADING', NULL)", TYPE_STRING);
  TestIsNull("parse_url(NULL, NULL)", TYPE_STRING);

  // Key's value is terminated by '#'.
  TestStringValue("parse_url('http://example.com:80/docs/books/tutorial/"
      "index.html?name=networking#DOWNLOADING', 'QUERY', 'name')", "networking");
  // Key's value is terminated by end of string.
  TestStringValue("parse_url('http://example.com:80/docs/books/tutorial/"
      "index.html?name=networking', 'QUERY', 'name')", "networking");
  // Key's value is terminated by end of string, with trimming.
  TestStringValue("parse_url('http://example.com:80/docs/books/tutorial/"
      "index.html?name=networking   ', 'QUERY', 'name')", "networking");
  // Key's value is terminated by '&'.
  TestStringValue("parse_url('http://example.com:80/docs/books/tutorial/"
      "index.html?name=networking&test=true', 'QUERY', 'name')", "networking");
  // Key's value is some query param in the middle.
  TestStringValue("parse_url('http://example.com:80/docs/books/tutorial/"
      "index.html?test=true&name=networking&op=true', 'QUERY', 'name')", "networking");
  // Key string appears in various parts of the url.
  TestStringValue("parse_url('http://name.name:80/name/books/tutorial/"
      "name.html?name_fake=true&name=networking&op=true#name', 'QUERY', 'name')",
      "networking");
  // We can still match this even though no '?' was given.
  TestStringValue("parse_url('http://example.com:80/docs/books/tutorial/"
      "index.htmltest=true&name=networking&op=true', 'QUERY', 'name')", "networking");
  // Requested key doesn't exist.
  TestIsNull("parse_url('http://example.com:80/docs/books/tutorial/"
      "index.html?test=true&name=networking&op=true', 'QUERY', 'error')", TYPE_STRING);
  // Requested key doesn't exist in query part.
  TestIsNull("parse_url('http://example.com:80/docs/books/tutorial/"
      "name.html?test=true&op=true', 'QUERY', 'name')", TYPE_STRING);
  // Requested key doesn't exist in query part, but matches at end of string.
  TestIsNull("parse_url('http://example.com:80/docs/books/tutorial/"
      "name.html?test=true&op=name', 'QUERY', 'name')", TYPE_STRING);
  // Malformed urls with incorrectly positioned '?' or '='.
  TestStringValue("parse_url('http://example.com:80/docs/books/tutorial/"
      "index.html?test=true&name=net?working&op=true', 'QUERY', 'name')", "net?working");
  TestStringValue("parse_url('http://example.com:80/docs/books/tutorial/"
      "index.html?test=true&name=net=working&op=true', 'QUERY', 'name')", "net=working");
  // Key paremeter given without QUERY part.
  TestIsNull("parse_url('http://example.com:80/docs/books/tutorial/"
      "index.html?test=true&name=networking&op=true', 'AUTHORITY', 'name')", TYPE_STRING);
  TestIsNull("parse_url('http://example.com:80/docs/books/tutorial/"
      "index.html?test=true&name=networking&op=true', 'FILE', 'name')", TYPE_STRING);
  TestIsNull("parse_url('http://example.com:80/docs/books/tutorial/"
      "index.html?test=true&name=networking&op=true', 'PATH', 'name')", TYPE_STRING);
  TestIsNull("parse_url('http://example.com:80/docs/books/tutorial/"
      "index.html?test=true&name=networking&op=true', 'PROTOCOL', 'name')", TYPE_STRING);
  TestIsNull("parse_url('http://example.com:80/docs/books/tutorial/"
      "index.html?test=true&name=networking&op=true', 'REF', 'name')", TYPE_STRING);
  TestError("parse_url('http://example.com:80/docs/books/tutorial/"
      "index.html?test=true&name=networking&op=true', 'XYZ', 'name')");
}

TEST_F(ExprTest, UtilityFunctions) {
  TestStringValue("current_database()", "default");
  TestStringValue("current_catalog()", "default");
  TestStringValue("user()", "impala_test_user");
  TestStringValue("current_user()", "impala_test_user");
  TestStringValue("effective_user()",  "impala_test_user");
  TestStringValue("session_user()",  "impala_test_user");
  TestStringValue("version()", GetVersionString());
  TestValue("sleep(100)", TYPE_BOOLEAN, true);
  TestIsNull("sleep(NULL)", TYPE_BOOLEAN);

  // Test typeOf
  TestStringValue("typeOf(!true)", "BOOLEAN");
  TestStringValue("typeOf(1)", "TINYINT");
  TestStringValue("typeOf(0)", "TINYINT");
  TestStringValue("typeOf(-1)", "TINYINT");
  TestStringValue("typeOf(128)", "SMALLINT");
  TestStringValue("typeOf(32768)", "INT");
  TestStringValue("typeOf(2147483648)", "BIGINT");
  TestStringValue("typeOf(4294967296)", "BIGINT");
  TestStringValue("typeOf(-4294967296)", "BIGINT");
  TestStringValue("typeOf(9223372036854775807)", "BIGINT");
  TestStringValue("typeOf(-9223372036854775808)", "BIGINT");
  TestStringValue("typeOf(cast(10 as FLOAT))", "FLOAT");
  TestStringValue("typeOf(cast(10 as DOUBLE))", "DOUBLE");
  TestStringValue("typeOf(current_database())", "STRING");
  TestStringValue("typeOf(now())", "TIMESTAMP");
  TestStringValue("typeOf(utc_timestamp())", "TIMESTAMP");
  TestStringValue("typeOf(cast(10 as DECIMAL))", "DECIMAL(9,0)");
  TestStringValue("typeOf(0.0)", "DECIMAL(1,1)");
  TestStringValue("typeOf(3.14)", "DECIMAL(3,2)");
  TestStringValue("typeOf(-1.23)", "DECIMAL(3,2)");
  TestStringValue("typeOf(cast(NULL as STRING))", "STRING");
  TestStringValue("typeOf(\"\")", "STRING");
  TestStringValue("typeOf(NULL)", "BOOLEAN");
  TestStringValue("typeOf(34 < NULL)", "BOOLEAN");
  TestStringValue("typeOf(cast('a' as CHAR(2)))", "CHAR(2)");
  TestStringValue("typeOf(cast('abcdef' as CHAR(4)))", "CHAR(4)");
  TestStringValue("typeOf(cast('a' as VARCHAR(2)))", "VARCHAR(2)");
  TestStringValue("typeOf(cast('abcdef' as VARCHAR(4)))", "VARCHAR(4)");
  TestStringValue("typeOf(cast(NULL as CHAR(2)))", "CHAR(2)");
  TestStringValue("typeOf(cast(NULL as VARCHAR(4)))", "VARCHAR(4)");

  // Test fnv_hash
  string s("hello world");
  uint64_t expected = HashUtil::FnvHash64(s.data(), s.size(), HashUtil::FNV_SEED);
  TestValue("fnv_hash('hello world')", TYPE_BIGINT, expected);
  s = string("");
  expected = HashUtil::FnvHash64(s.data(), s.size(), HashUtil::FNV_SEED);
  TestValue("fnv_hash('')", TYPE_BIGINT, expected);

  IntValMap::iterator int_iter;
  for(int_iter = min_int_values_.begin(); int_iter != min_int_values_.end();
      ++int_iter) {
    ColumnType t = ColumnType(static_cast<PrimitiveType>(int_iter->first));
    expected = HashUtil::FnvHash64(
        &int_iter->second, t.GetByteSize(), HashUtil::FNV_SEED);
    string& val = default_type_strs_[int_iter->first];
    TestValue("fnv_hash(" + val + ")", TYPE_BIGINT, expected);
  }

  // Don't use min_float_values_ for testing floats and doubles due to improper float
  // and double literal handling, see IMPALA-669.
  float float_val = 42;
  expected = HashUtil::FnvHash64(&float_val, sizeof(float), HashUtil::FNV_SEED);
  TestValue("fnv_hash(CAST(42 as FLOAT))", TYPE_BIGINT, expected);

  double double_val = 42;
  expected = HashUtil::FnvHash64(&double_val, sizeof(double), HashUtil::FNV_SEED);
  TestValue("fnv_hash(CAST(42 as DOUBLE))", TYPE_BIGINT, expected);

  expected = HashUtil::FnvHash64(&default_timestamp_val_, 12, HashUtil::FNV_SEED);
  TestValue("fnv_hash(" + default_timestamp_str_ + ")", TYPE_BIGINT, expected);

  bool bool_val = false;
  expected = HashUtil::FnvHash64(&bool_val, 1, HashUtil::FNV_SEED);
  TestValue("fnv_hash(FALSE)", TYPE_BIGINT, expected);

  // Test NULL input returns NULL
  TestIsNull("fnv_hash(NULL)", TYPE_BIGINT);
}

TEST_F(ExprTest, SessionFunctions) {
  enum Session {S1, S2};
  enum Query {Q1, Q2};

  map<Session, map<Query, string>> results;
  for (Session session: {S1, S2}) {
    ASSERT_OK(executor_->Setup()); // Starts new session
    results[session][Q1] = GetValue("current_session()", TYPE_STRING);
    results[session][Q2] = GetValue("current_sid()", TYPE_STRING);
  }

  // The sessions IDs from the same session must be the same.
  EXPECT_EQ(results[S1][Q1], results[S1][Q2]);
  EXPECT_EQ(results[S2][Q1], results[S2][Q2]);
  // The sessions IDs from different sessions must be different.
  EXPECT_NE(results[S1][Q1], results[S2][Q1]);
}

TEST_F(ExprTest, NonFiniteFloats) {
  TestValue("is_inf(0.0)", TYPE_BOOLEAN, false);
  TestValue("is_inf(-1/0)", TYPE_BOOLEAN, true);
  TestValue("is_inf(1/0)", TYPE_BOOLEAN, true);
  TestValue("is_inf(0/0)", TYPE_BOOLEAN, false);
  TestValue("is_inf(NULL)", TYPE_BOOLEAN, false);
  TestValue("is_nan(NULL)", TYPE_BOOLEAN, false);

  TestValue("is_nan(0.0)", TYPE_BOOLEAN, false);
  TestValue("is_nan(1/0)", TYPE_BOOLEAN, false);
  TestValue("is_nan(0/0)", TYPE_BOOLEAN, true);

  TestValue("CAST(1/0 AS FLOAT)", TYPE_FLOAT, numeric_limits<float>::infinity());
  TestValue("CAST(1/0 AS DOUBLE)", TYPE_DOUBLE, numeric_limits<double>::infinity());
  TestValue("CAST(CAST(1/0 as FLOAT) as DOUBLE)", TYPE_DOUBLE,
      numeric_limits<double>::infinity());
  TestValue("CAST(CAST(1/0 as DOUBLE) as FLOAT)", TYPE_FLOAT,
      numeric_limits<float>::infinity());
  TestStringValue("CAST(1/0 AS STRING)", "inf");
  TestStringValue("CAST(CAST(1/0 AS FLOAT) AS STRING)", "inf");

  TestValue("CAST('inf' AS FLOAT)", TYPE_FLOAT, numeric_limits<float>::infinity());
  TestValue("CAST('inf' AS DOUBLE)", TYPE_DOUBLE, numeric_limits<double>::infinity());
  TestValue("CAST('  inf  ' AS FLOAT)", TYPE_FLOAT,
      numeric_limits<float>::infinity());
  TestValue("CAST('  inf  ' AS DOUBLE)", TYPE_DOUBLE,
      numeric_limits<double>::infinity());
  TestValue("CAST('+inf' AS FLOAT)", TYPE_FLOAT,
      numeric_limits<float>::infinity());
  TestValue("CAST('+inf' AS DOUBLE)", TYPE_DOUBLE,
      numeric_limits<double>::infinity());
  TestValue("CAST('-inf' AS FLOAT)", TYPE_FLOAT,
      -numeric_limits<float>::infinity());
  TestValue("CAST('-inf' AS DOUBLE)", TYPE_DOUBLE,
      -numeric_limits<double>::infinity());
  TestValue("CAST('Infinity' AS FLOAT)", TYPE_FLOAT,
      numeric_limits<float>::infinity());
  TestValue("CAST('-Infinity' AS DOUBLE)", TYPE_DOUBLE,
      -numeric_limits<double>::infinity());
  // Parsing inf values is case-insensitive.
  TestValue("CAST('iNf' AS FLOAT)", TYPE_FLOAT,
      numeric_limits<float>::infinity());
  TestValue("CAST('iNf' AS DOUBLE)", TYPE_DOUBLE,
      numeric_limits<double>::infinity());
  TestValue("CAST('INf' AS FLOAT)", TYPE_FLOAT,
      numeric_limits<float>::infinity());
  TestValue("CAST('INf' AS DOUBLE)", TYPE_DOUBLE,
      numeric_limits<double>::infinity());
  TestValue("CAST('inF' AS FLOAT)", TYPE_FLOAT,
      numeric_limits<float>::infinity());
  TestValue("CAST('inF' AS DOUBLE)", TYPE_DOUBLE,
      numeric_limits<double>::infinity());

  // NaN != NaN, so we have to wrap the value in a string
  TestStringValue("CAST(CAST('nan' AS FLOAT) AS STRING)", string("nan"));
  TestStringValue("CAST(CAST('nan' AS DOUBLE) AS STRING)", string("nan"));
  TestStringValue("CAST(CAST('  nan  ' AS FLOAT) AS STRING)", string("nan"));
  TestStringValue("CAST(CAST('  nan  ' AS DOUBLE) AS STRING)", string("nan"));
  TestStringValue("CAST(CAST('-nan' AS FLOAT) AS STRING)", string("nan"));
  TestStringValue("CAST(CAST('-nan' AS DOUBLE) AS STRING)", string("nan"));
  TestStringValue("CAST(CAST('+nan' AS FLOAT) AS STRING)", string("nan"));
  TestStringValue("CAST(CAST('+nan' AS DOUBLE) AS STRING)", string("nan"));
  // Parsing NaN values is case-insensitive
  TestStringValue("CAST(CAST('nAn' AS FLOAT) AS STRING)", string("nan"));
  TestStringValue("CAST(CAST('nAn' AS DOUBLE) AS STRING)", string("nan"));
  TestStringValue("CAST(CAST('NAn' AS FLOAT) AS STRING)", string("nan"));
  TestStringValue("CAST(CAST('NAn' AS DOUBLE) AS STRING)", string("nan"));
  TestStringValue("CAST(CAST('naN' AS FLOAT) AS STRING)", string("nan"));
  TestStringValue("CAST(CAST('naN' AS DOUBLE) AS STRING)", string("nan"));
  // 0/0 evalutes to -nan, test that we return "nan"
  TestStringValue("CAST(0/0 AS STRING)", string("nan"));
}

TEST_F(ExprTest, InvalidFloats) {
  // IMPALA-1731: Test that leading/trailing garbage is not allowed when parsing inf.
  TestIsNull("CAST('1.23inf' AS FLOAT)", TYPE_FLOAT);
  TestIsNull("CAST('1.23inf' AS DOUBLE)", TYPE_DOUBLE);
  TestIsNull("CAST('1.23inf456' AS FLOAT)", TYPE_FLOAT);
  TestIsNull("CAST('1.23inf456' AS DOUBLE)", TYPE_DOUBLE);
  TestIsNull("CAST('inf123' AS FLOAT)", TYPE_FLOAT);
  TestIsNull("CAST('inf123' AS DOUBLE)", TYPE_DOUBLE);
  TestIsNull("CAST('infinity2' AS FLOAT)", TYPE_FLOAT);
  TestIsNull("CAST('infinity2' AS DOUBLE)", TYPE_DOUBLE);
  TestIsNull("CAST('infinite' AS FLOAT)", TYPE_FLOAT);
  TestIsNull("CAST('infinite' AS DOUBLE)", TYPE_DOUBLE);
  TestIsNull("CAST('inf123nan' AS FLOAT)", TYPE_FLOAT);
  TestIsNull("CAST('inf123nan' AS DOUBLE)", TYPE_DOUBLE);

  // IMPALA-1731: Test that leading/trailing garbage is not allowed when parsing NaN.
  TestIsNull("CAST('1.23nan' AS FLOAT)", TYPE_FLOAT);
  TestIsNull("CAST('1.23nan' AS DOUBLE)", TYPE_DOUBLE);
  TestIsNull("CAST('1.23nan456' AS FLOAT)", TYPE_FLOAT);
  TestIsNull("CAST('1.23nan456' AS DOUBLE)", TYPE_DOUBLE);
  TestIsNull("CAST('nana' AS FLOAT)", TYPE_FLOAT);
  TestIsNull("CAST('nana' AS DOUBLE)", TYPE_DOUBLE);
  TestIsNull("CAST('nan123' AS FLOAT)", TYPE_FLOAT);
  TestIsNull("CAST('nan123' AS DOUBLE)", TYPE_DOUBLE);
  TestIsNull("CAST('nan123inf' AS FLOAT)", TYPE_FLOAT);
  TestIsNull("CAST('nan123inf' AS DOUBLE)", TYPE_DOUBLE);

  // IMPALA-3868: Test that multiple dots are not allowed in float values
  TestIsNull("CAST('1.2.3.4.5' AS FLOAT)", TYPE_FLOAT);
  TestIsNull("CAST('1.2.3.4.5' AS DOUBLE)", TYPE_DOUBLE);
  TestIsNull("CAST('1..e' AS FLOAT)", TYPE_FLOAT);
  TestIsNull("CAST('1..e' AS DOUBLE)", TYPE_DOUBLE);

  // Broken string with null-character in the middle
  string s1("CAST('in\0f' AS DOUBLE)", 22);
  TestIsNull(s1, TYPE_DOUBLE);
  string s2("CAST('N\0aN' AS FLOAT)", 21);
  TestIsNull(s2, TYPE_FLOAT);

  // Empty string
  TestIsNull("CAST('' AS DOUBLE)", TYPE_DOUBLE);
  TestIsNull("CAST('' AS FLOAT)", TYPE_FLOAT);
}

TEST_F(ExprTest, MathTrigonometricFunctions) {
  // It is important to calculate the expected values
  // using math functions, and not simply use constants.
  // Otherwise, floating point imprecisions may lead to failed tests.
  TestValue("sin(0.0)", TYPE_DOUBLE, sin(0.0));
  TestValue("sin(pi())", TYPE_DOUBLE, sin(M_PI));
  TestValue("sin(pi() / 2.0)", TYPE_DOUBLE, sin(M_PI / 2.0));
  TestValue("asin(-1.0)", TYPE_DOUBLE, asin(-1.0));
  TestValue("asin(1.0)", TYPE_DOUBLE, asin(1.0));
  TestValue("cos(0.0)", TYPE_DOUBLE, cos(0.0));
  TestValue("cos(pi())", TYPE_DOUBLE, cos(M_PI));
  TestValue("acos(-1.0)", TYPE_DOUBLE, acos(-1.0));
  TestValue("acos(1.0)", TYPE_DOUBLE, acos(1.0));
  TestValue("tan(pi() * -1.0)", TYPE_DOUBLE, tan(M_PI * -1.0));
  TestValue("tan(pi())", TYPE_DOUBLE, tan(M_PI));
  TestValue("atan(pi())", TYPE_DOUBLE, atan(M_PI));
  TestValue("atan(pi() * - 1.0)", TYPE_DOUBLE, atan(M_PI * -1.0));
  TestValue("cosh(0)", TYPE_DOUBLE, cosh(0));
  TestValue("tanh(0)", TYPE_DOUBLE, tanh(0));
  TestValue("sinh(0)", TYPE_DOUBLE, sinh(0));
  TestValue("cosh(pi())", TYPE_DOUBLE, cosh(M_PI));
  TestValue("tanh(pi())", TYPE_DOUBLE, tanh(M_PI));
  TestValue("sinh(pi())", TYPE_DOUBLE, sinh(M_PI));
  TestValue("cosh(-pi())", TYPE_DOUBLE, cosh(M_PI * -1.0));
  TestValue("tanh(-pi())", TYPE_DOUBLE, tanh(M_PI * -1.0));
  TestValue("sinh(-pi())", TYPE_DOUBLE, sinh(M_PI * -1.0));
  TestValue("atan2(1,0)", TYPE_DOUBLE, atan2(1,0));
  TestValue("atan2(-1,0)", TYPE_DOUBLE, atan2(-1,0));
  TestValue("cot(pi() / 2.0)", TYPE_DOUBLE, tan(0.0));
  TestValue("cot(pi())", TYPE_DOUBLE, tan(M_PI_2 - M_PI));
  // this gets a very very small number rather than 0.
  // TestValue("radians(0)", TYPE_DOUBLE, 0);
  TestValue("radians(180.0)", TYPE_DOUBLE, M_PI);
  TestValue("degrees(0)", TYPE_DOUBLE, 0.0);
  TestValue("degrees(pi())", TYPE_DOUBLE, 180.0);

  // NULL arguments.
  TestIsNull("sin(NULL)", TYPE_DOUBLE);
  TestIsNull("asin(NULL)", TYPE_DOUBLE);
  TestIsNull("cos(NULL)", TYPE_DOUBLE);
  TestIsNull("acos(NULL)", TYPE_DOUBLE);
  TestIsNull("tan(NULL)", TYPE_DOUBLE);
  TestIsNull("atan(NULL)", TYPE_DOUBLE);
  TestIsNull("cot(NULL)", TYPE_DOUBLE);
  TestIsNull("radians(NULL)", TYPE_DOUBLE);
  TestIsNull("degrees(NULL)", TYPE_DOUBLE);
}

TEST_F(ExprTest, MathConversionFunctions) {
  TestStringValue("bin(0)", "0");
  TestStringValue("bin(1)", "1");
  TestStringValue("bin(12)", "1100");
  TestStringValue("bin(1234567)", "100101101011010000111");
  TestStringValue("bin(" + lexical_cast<string>(numeric_limits<int64_t>::max()) + ")",
      "111111111111111111111111111111111111111111111111111111111111111");
  TestStringValue("bin(" + lexical_cast<string>(numeric_limits<int64_t>::min()+1) + ")",
      "1000000000000000000000000000000000000000000000000000000000000001");

  TestStringValue("hex(0)", "0");
  TestStringValue("hex(15)", "F");
  TestStringValue("hex(16)", "10");
  TestStringValue("hex(" + lexical_cast<string>(numeric_limits<int64_t>::max()) + ")",
      "7FFFFFFFFFFFFFFF");
  TestStringValue("hex(" + lexical_cast<string>(numeric_limits<int64_t>::min()+1) + ")",
      "8000000000000001");
  TestStringValue("hex('0')", "30");
  TestStringValue("hex('aAzZ')", "61417A5A");
  TestStringValue("hex('Impala')", "496D70616C61");
  TestStringValue("hex('impalA')", "696D70616C41");
  // Test non-ASCII characters
  TestStringValue("hex(unhex('D3'))", "D3");
  // Test width(2) and fill('0') for multiple characters < 16
  TestStringValue("hex(unhex('0303'))", "0303");
  TestStringValue("hex(unhex('D303D303'))", "D303D303");

  TestStringValue("unhex('30')", "0");
  TestStringValue("unhex('61417A5A')", "aAzZ");
  TestStringValue("unhex('496D70616C61')", "Impala");
  TestStringValue("unhex('696D70616C41')", "impalA");
  // Character not in hex alphabet results in empty string.
  TestStringValue("unhex('30GA')", "");
  // Uneven number of chars results in empty string.
  TestStringValue("unhex('30A')", "");

  // Run the test suite twice, once with a bigint parameter, and once with
  // string parameters.
  for (int i = 0; i < 2; ++i) {
    // First iteration is with bigint, second with string parameter.
    string q = (i == 0) ? "" : "'";
    // Invalid input: Base below -36 or above 36.
    TestIsNull("conv(" + q + "10" + q + ", 10, 37)", TYPE_STRING);
    TestIsNull("conv(" + q + "10" + q + ", 37, 10)", TYPE_STRING);
    TestIsNull("conv(" + q + "10" + q + ", 10, -37)", TYPE_STRING);
    TestIsNull("conv(" + q + "10" + q + ", -37, 10)", TYPE_STRING);
    // Invalid input: Base between -2 and 2.
    TestIsNull("conv(" + q + "10" + q + ", 10, 1)", TYPE_STRING);
    TestIsNull("conv(" + q + "10" + q + ", 1, 10)", TYPE_STRING);
    TestIsNull("conv(" + q + "10" + q + ", 10, -1)", TYPE_STRING);
    TestIsNull("conv(" + q + "10" + q + ", -1, 10)", TYPE_STRING);
    // Invalid input: Positive number but negative src base.
    TestIsNull("conv(" + q + "10" + q + ", -10, 10)", TYPE_STRING);
    // Test positive numbers.
    TestStringValue("conv(" + q + "10" + q + ", 10, 10)", "10");
    TestStringValue("conv(" + q + "10" + q + ", 2, 10)", "2");
    TestStringValue("conv(" + q + "11" + q + ", 36, 10)", "37");
    TestStringValue("conv(" + q + "11" + q + ", 36, 2)", "100101");
    TestStringValue("conv(" + q + "100101" + q + ", 2, 36)", "11");
    TestStringValue("conv(" + q + "0" + q + ", 10, 2)", "0");
    // Test for very large big int
    TestStringValue("conv(" + q + "2061013007" + q + ", 16, 10)", "139066421255");
    // Test negative numbers (tests from Hive).
    // If to_base is positive, the number should be handled as a 2's complement (64-bit).
    TestStringValue("conv(" + q + "-641" + q + ", 10, -10)", "-641");
    TestStringValue("conv(" + q + "1011" + q + ", 2, -16)", "B");
    TestStringValue("conv(" + q + "-1" + q + ", 10, 16)", "FFFFFFFFFFFFFFFF");
    TestStringValue("conv(" + q + "-15" + q + ", 10, 16)", "FFFFFFFFFFFFFFF1");
    // Test digits that are not available in srcbase. We expect those digits
    // from left-to-right that can be interpreted in srcbase to form the result
    // (i.e., the paring bails only when it encounters a digit not in srcbase).
    TestStringValue("conv(" + q + "17" + q + ", 7, 10)", "1");
    TestStringValue("conv(" + q + "371" + q + ", 7, 10)", "3");
    TestStringValue("conv(" + q + "371" + q + ", 7, 10)", "3");
    TestStringValue("conv(" + q + "445" + q + ", 5, 10)", "24");
    // Test overflow (tests from Hive).
    // If a number is two large, the result should be -1 (if signed),
    // or MAX_LONG (if unsigned).
    TestStringValue("conv(" + q + lexical_cast<string>(numeric_limits<int64_t>::max())
        + q + ", 36, 16)", "FFFFFFFFFFFFFFFF");
    TestStringValue("conv(" + q + lexical_cast<string>(numeric_limits<int64_t>::max())
        + q + ", 36, -16)", "-1");
    TestStringValue("conv(" + q + lexical_cast<string>(numeric_limits<int64_t>::min()+1)
        + q + ", 36, 16)", "FFFFFFFFFFFFFFFF");
    TestStringValue("conv(" + q + lexical_cast<string>(numeric_limits<int64_t>::min()+1)
        + q + ", 36, -16)", "-1");
  }
  // Test invalid input strings that start with an invalid digit.
  // Hive returns "0" in such cases.
  TestStringValue("conv('@', 16, 10)", "0");
  TestStringValue("conv('$123', 12, 2)", "0");
  TestStringValue("conv('*12g', 32, 5)", "0");

  // NULL arguments.
  TestIsNull("bin(NULL)", TYPE_STRING);
  TestIsNull("hex(NULL)", TYPE_STRING);
  TestIsNull("unhex(NULL)", TYPE_STRING);
  TestIsNull("conv(NULL, 10, 10)", TYPE_STRING);
  TestIsNull("conv(10, NULL, 10)", TYPE_STRING);
  TestIsNull("conv(10, 10, NULL)", TYPE_STRING);
  TestIsNull("conv(NULL, NULL, NULL)", TYPE_STRING);
}

TEST_F(ExprTest, MathFunctions) {
  TestValue("pi()", TYPE_DOUBLE, M_PI);
  TestValue("e()", TYPE_DOUBLE, M_E);
  TestValue("abs(cast(-1.0 as double))", TYPE_DOUBLE, 1.0);
  TestValue("abs(cast(1.0 as double))", TYPE_DOUBLE, 1.0);
  TestValue("abs(-127)", TYPE_SMALLINT, 127);
  TestValue("abs(-128)", TYPE_SMALLINT, 128);
  TestValue("abs(127)", TYPE_SMALLINT, 127);
  TestValue("abs(128)", TYPE_INT, 128);
  TestValue("abs(-32767)", TYPE_INT, 32767);
  TestValue("abs(-32768)", TYPE_INT, 32768);
  TestValue("abs(32767)", TYPE_INT, 32767);
  TestValue("abs(32768)", TYPE_BIGINT, 32768);
  TestValue("abs(-1 * cast(pow(2, 31) as int))", TYPE_BIGINT, 2147483648);
  TestValue("abs(cast(pow(2, 31) as int))", TYPE_BIGINT, 2147483648);
  TestValue("abs(2147483647)", TYPE_BIGINT, 2147483647);
  TestValue("abs(2147483647)", TYPE_BIGINT, 2147483647);
  TestValue("abs(-9223372036854775807)", TYPE_BIGINT,  9223372036854775807);
  TestValue("abs(9223372036854775807)", TYPE_BIGINT,  9223372036854775807);
  TestIsNull("abs(-9223372036854775808)", TYPE_BIGINT);
  TestValue("sign(0.0)", TYPE_FLOAT, 0.0f);
  TestValue("sign(-0.0)", TYPE_FLOAT, 0.0f);
  TestValue("sign(+0.0)", TYPE_FLOAT, 0.0f);
  TestValue("sign(10.0)", TYPE_FLOAT, 1.0f);
  TestValue("sign(-10.0)", TYPE_FLOAT, -1.0f);
  TestIsNull("sign(NULL)", TYPE_FLOAT);

  // It is important to calculate the expected values
  // using math functions, and not simply use constants.
  // Otherwise, floating point imprecisions may lead to failed tests.
  TestValue("exp(2)", TYPE_DOUBLE, exp(2));
  TestValue("exp(e())", TYPE_DOUBLE, exp(M_E));
  TestValue("ln(e())", TYPE_DOUBLE, 1.0);
  TestValue("ln(255.0)", TYPE_DOUBLE, log(255.0));
  TestValue("dlog1(10)", TYPE_DOUBLE, log(10.0));
  TestValue("log10(1000.0)", TYPE_DOUBLE, 3.0);
  TestValue("log10(50.0)", TYPE_DOUBLE, log10(50.0));
  TestValue("dlog10(100.0)", TYPE_DOUBLE, 2.0);
  TestValue("log2(64.0)", TYPE_DOUBLE, 6.0);
  TestValue("log2(678.0)", TYPE_DOUBLE, log(678.0) / log(2.0));
  TestValue("log(10.0, 1000.0)", TYPE_DOUBLE, log(1000.0) / log(10.0));
  TestValue("log(2.0, 64.0)", TYPE_DOUBLE, 6.0);
  TestValue("pow(2.0, 10.0)", TYPE_DOUBLE, pow(2.0, 10.0));
  TestValue("pow(e(), 2.0)", TYPE_DOUBLE, M_E * M_E);
  TestValue("power(2.0, 10.0)", TYPE_DOUBLE, pow(2.0, 10.0));
  TestValue("power(e(), 2.0)", TYPE_DOUBLE, M_E * M_E);
  TestValue("dpow(3, 3)", TYPE_DOUBLE, 27.0);
  TestValue("fpow(3, 3)", TYPE_DOUBLE, 27.0);
  TestValue("sqrt(121.0)", TYPE_DOUBLE, 11.0);
  TestValue("sqrt(2.0)", TYPE_DOUBLE, sqrt(2.0));
  TestValue("dsqrt(81.0)", TYPE_DOUBLE, 9);

  // Run twice to test deterministic behavior.
  uint32_t seed = 0;
  double expected = static_cast<double>(rand_r(&seed)) / static_cast<double>(RAND_MAX);
  TestValue("rand()", TYPE_DOUBLE, expected);
  TestValue("rand()", TYPE_DOUBLE, expected);
  TestValue("random()", TYPE_DOUBLE, expected); // Test alias
  seed = 1234;
  expected = static_cast<double>(rand_r(&seed)) / static_cast<double>(RAND_MAX);
  TestValue("rand(1234)", TYPE_DOUBLE, expected);
  TestValue("rand(1234)", TYPE_DOUBLE, expected);
  TestValue("random(1234)", TYPE_DOUBLE, expected); // Test alias

  // Test bigint param.
  TestValue("pmod(10, 3)", TYPE_BIGINT, 1);
  TestValue("pmod(-10, 3)", TYPE_BIGINT, 2);
  TestValue("pmod(10, -3)", TYPE_BIGINT, -2);
  TestValue("pmod(-10, -3)", TYPE_BIGINT, -1);
  TestValue("pmod(1234567890, 13)", TYPE_BIGINT, 10);
  TestValue("pmod(-1234567890, 13)", TYPE_BIGINT, 3);
  TestValue("pmod(1234567890, -13)", TYPE_BIGINT, -3);
  TestValue("pmod(-1234567890, -13)", TYPE_BIGINT, -10);
  // Test double param.
  TestValue("pmod(12.3, 4.0)", TYPE_DOUBLE, fmod(fmod(12.3, 4.0) + 4.0, 4.0));
  TestValue("pmod(-12.3, 4.0)", TYPE_DOUBLE, fmod(fmod(-12.3, 4.0) + 4.0, 4.0));
  TestValue("pmod(12.3, -4.0)", TYPE_DOUBLE, fmod(fmod(12.3, -4.0) - 4.0, -4.0));
  TestValue("pmod(-12.3, -4.0)", TYPE_DOUBLE, fmod(fmod(-12.3, -4.0) - 4.0, -4.0));
  TestValue("pmod(123456.789, 13.456)", TYPE_DOUBLE,
      fmod(fmod(123456.789, 13.456) + 13.456, 13.456));
  TestValue("pmod(-123456.789, 13.456)", TYPE_DOUBLE,
      fmod(fmod(-123456.789, 13.456) + 13.456, 13.456));
  TestValue("pmod(123456.789, -13.456)", TYPE_DOUBLE,
      fmod(fmod(123456.789, -13.456) - 13.456, -13.456));
  TestValue("pmod(-123456.789, -13.456)", TYPE_DOUBLE,
      fmod(fmod(-123456.789, -13.456) - 13.456, -13.456));

  // Test floating-point modulo function.
  TestValue("fmod(cast(12345.345 as float), cast(7 as float))",
      TYPE_FLOAT, fmodf(12345.345f, 7.0f));
  TestValue("fmod(cast(-12345.345 as float), cast(7 as float))",
      TYPE_FLOAT, fmodf(-12345.345f, 7.0f));
  TestValue("fmod(cast(-12345.345 as float), cast(-7 as float))",
      TYPE_FLOAT, fmodf(-12345.345f, -7.0f));
  TestValue("fmod(cast(12345.345 as double), 7)", TYPE_DOUBLE, fmod(12345.345, 7.0));
  TestValue("fmod(-12345.345, cast(7 as double))", TYPE_DOUBLE, fmod(-12345.345, 7.0));
  TestValue("fmod(cast(-12345.345 as double), -7)", TYPE_DOUBLE, fmod(-12345.345, -7.0));
  // Test floating-point modulo operator.
  TestValue("cast(12345.345 as float) % cast(7 as float)",
      TYPE_FLOAT, fmodf(12345.345f, 7.0f));
  TestValue("cast(-12345.345 as float) % cast(7 as float)",
      TYPE_FLOAT, fmodf(-12345.345f, 7.0f));
  TestValue("cast(-12345.345 as float) % cast(-7 as float)",
      TYPE_FLOAT, fmodf(-12345.345f, -7.0f));
  TestValue("cast(12345.345 as double) % 7", TYPE_DOUBLE, fmod(12345.345, 7.0));
  TestValue("-12345.345 % cast(7 as double)", TYPE_DOUBLE, fmod(-12345.345, 7.0));
  TestValue("cast(-12345.345 as double) % -7", TYPE_DOUBLE, fmod(-12345.345, -7.0));
  // Test floating-point modulo by zero.
  TestIsNull("fmod(cast(-12345.345 as float), cast(0 as float))", TYPE_FLOAT);
  TestIsNull("fmod(cast(-12345.345 as double), 0)", TYPE_DOUBLE);
  TestIsNull("cast(-12345.345 as float) % cast(0 as float)", TYPE_FLOAT);
  TestIsNull("cast(-12345.345 as double) % 0", TYPE_DOUBLE);

  // Test int param.
  TestValue("mod(cast(10 as tinyint), cast(3 as tinyint))", TYPE_TINYINT, 10 % 3);
  TestValue("mod(cast(10 as smallint), cast(3 as smallint))", TYPE_SMALLINT, 10 % 3);
  TestValue("mod(cast(10 as int), cast(3 as int))", TYPE_INT, 10 % 3);
  TestValue("mod(cast(10 as bigint), cast(3 as bigint))", TYPE_BIGINT, 10 % 3);
  TestIsNull("mod(cast(123 as tinyint), 0)", TYPE_TINYINT);
  TestIsNull("mod(cast(123 as smallint), 0)", TYPE_SMALLINT);
  TestIsNull("mod(cast(123 as int), 0)", TYPE_INT);
  TestIsNull("mod(cast(123 as bigint), 0)", TYPE_BIGINT);
  TestIsNull("mod(cast(123 as tinyint), NULL)", TYPE_TINYINT);
  TestIsNull("mod(cast(123 as smallint), NULL)", TYPE_SMALLINT);
  TestIsNull("mod(cast(123 as int), NULL)", TYPE_INT);
  TestIsNull("mod(cast(123 as bigint), NULL)", TYPE_BIGINT);
  TestIsNull("mod(cast(NULL as int), NULL)", TYPE_INT);
  // Test numeric param.
  TestValue("mod(cast(12.3 as float), cast(4.0 as float))", TYPE_FLOAT, fmodf(12.3f, 4.0f));
  TestValue("mod(cast(12.3 as double), cast(4.0 as double))", TYPE_DOUBLE, fmod(12.3, 4.0));
  TestIsNull("mod(cast(12345.345 as float), cast(0 as float))", TYPE_FLOAT);
  TestIsNull("mod(cast(12345.345 as double), cast(0 as double))", TYPE_DOUBLE);
  TestIsNull("mod(cast(12345.345 as float), NULL)", TYPE_FLOAT);
  TestIsNull("mod(cast(12345.345 as double), NULL)", TYPE_DOUBLE);
  TestIsNull("mod(cast(NULL as float), NULL)", TYPE_FLOAT);

  // Test positive().
  TestValue("positive(cast(123 as tinyint))", TYPE_TINYINT, 123);
  TestValue("positive(cast(123 as smallint))", TYPE_SMALLINT, 123);
  TestValue("positive(cast(123 as int))", TYPE_INT, 123);
  TestValue("positive(cast(123 as bigint))", TYPE_BIGINT, 123);
  TestValue("positive(cast(3.1415 as float))", TYPE_FLOAT, 3.1415f);
  TestValue("positive(cast(3.1415 as double))", TYPE_DOUBLE, 3.1415);
  TestValue("positive(cast(-123 as tinyint))", TYPE_TINYINT, -123);
  TestValue("positive(cast(-123 as smallint))", TYPE_SMALLINT, -123);
  TestValue("positive(cast(-123 as int))", TYPE_INT, -123);
  TestValue("positive(cast(-123 as bigint))", TYPE_BIGINT, -123);
  TestValue("positive(cast(-3.1415 as float))", TYPE_FLOAT, -3.1415f);
  TestValue("positive(cast(-3.1415 as double))", TYPE_DOUBLE, -3.1415);
  // Test negative().
  TestValue("negative(cast(123 as tinyint))", TYPE_TINYINT, -123);
  TestValue("negative(cast(123 as smallint))", TYPE_SMALLINT, -123);
  TestValue("negative(cast(123 as int))", TYPE_INT, -123);
  TestValue("negative(cast(123 as bigint))", TYPE_BIGINT, -123);
  TestValue("negative(cast(3.1415 as float))", TYPE_FLOAT, -3.1415f);
  TestValue("negative(cast(3.1415 as double))", TYPE_DOUBLE, -3.1415);
  TestValue("negative(cast(-123 as tinyint))", TYPE_TINYINT, 123);
  TestValue("negative(cast(-123 as smallint))", TYPE_SMALLINT, 123);
  TestValue("negative(cast(-123 as int))", TYPE_INT, 123);
  TestValue("negative(cast(-123 as bigint))", TYPE_BIGINT, 123);
  TestValue("negative(cast(-3.1415 as float))", TYPE_FLOAT, 3.1415f);
  TestValue("negative(cast(-3.1415 as double))", TYPE_DOUBLE, 3.1415);

  // Test bigint param.
  TestValue("quotient(12, 6)", TYPE_BIGINT, 2);
  TestValue("quotient(-12, 6)", TYPE_BIGINT, -2);
  TestIsNull("quotient(-12, 0)", TYPE_BIGINT);
  // Test double param.
  TestValue("quotient(30.5, 2.5)", TYPE_BIGINT, 15);
  TestValue("quotient(-30.5, 2.5)", TYPE_BIGINT, -15);
  TestIsNull("quotient(-30.5, 0.000999)", TYPE_BIGINT);

  // Tests to verify logic of least(). All types but STRING use the same
  // templated function, so there is no need to run all tests with all types.
  // Test single value.
  TestValue("least(1)", TYPE_TINYINT, 1);
  TestValue<float>("least(cast(1.25 as float))", TYPE_FLOAT, 1.25f);
  // Test ordering
  TestValue("least(10, 20)", TYPE_TINYINT, 10);
  TestValue("least(20, 10)", TYPE_TINYINT, 10);
  TestValue<float>("least(cast(500.25 as float), 300.25)", TYPE_FLOAT, 300.25f);
  TestValue<float>("least(cast(300.25 as float), 500.25)", TYPE_FLOAT, 300.25f);
  // Test to make sure least value is found in a mixed order set
  TestValue("least(1, 3, 4, 0, 6)", TYPE_TINYINT, 0);
  TestValue<float>("least(cast(1.25 as float), 3.25, 4.25, 0.25, 6.25)",
                   TYPE_FLOAT, 0.25f);
  // Test to make sure the least value is found from a list of duplicates
  TestValue("least(1, 1, 1, 1)", TYPE_TINYINT, 1);
  TestValue<float>("least(cast(1.0 as float), 1.0, 1.0, 1.0)", TYPE_FLOAT, 1.0f);
  // Test repeating groups and ordering
  TestValue("least(2, 2, 1, 1)", TYPE_TINYINT, 1);
  TestValue("least(0, -2, 1)", TYPE_TINYINT, -2);
  TestValue<float>("least(cast(2.0 as float), 2.0, 1.0, 1.0)", TYPE_FLOAT, 1.0f);
  TestValue<float>("least(cast(0.0 as float), -2.0, 1.0)", TYPE_FLOAT, -2.0f);
  // Test all int types.
  string val_list;
  val_list = "0";
  for (IntValMap::value_type& entry: min_int_values_) {
    string val_str = lexical_cast<string>(entry.second);
    val_list.append(", " + val_str);
    PrimitiveType t = static_cast<PrimitiveType>(entry.first);
    TestValue<int64_t>("least(" + val_list + ")", t, 0);
  }
  // Test double type.
  TestValue<double>("least(0.0, cast(-2.0 as double), 1.0)", TYPE_DOUBLE, -2.0f);
  // Test timestamp param
  TestStringValue("cast(least(cast('2014-09-26 12:00:00' as timestamp), "
      "cast('2013-09-26 12:00:00' as timestamp)) as string)", "2013-09-26 12:00:00");
  // Test string param.
  TestStringValue("least('2', '5', '12', '3')", "12");
  TestStringValue("least('apples', 'oranges', 'bananas')", "apples");
  TestStringValue("least('apples', 'applis', 'applas')", "applas");
  TestStringValue("least('apples', '!applis', 'applas')", "!applis");
  TestStringValue("least('apples', 'apples', 'apples')", "apples");
  TestStringValue("least('apples')", "apples");
  TestStringValue("least('A')", "A");
  TestStringValue("least('A', 'a')", "A");
  // Test ordering
  TestStringValue("least('a', 'A')", "A");
  TestStringValue("least('APPLES', 'APPLES')", "APPLES");
  TestStringValue("least('apples', 'APPLES')", "APPLES");
  TestStringValue("least('apples', 'app\nles')", "app\nles");
  TestStringValue("least('apples', 'app les')", "app les");
  TestStringValue("least('apples', 'app\nles')", "app\nles");
  TestStringValue("least('apples', 'app\tles')", "app\tles");
  TestStringValue("least('apples', 'app\fles')", "app\fles");
  TestStringValue("least('apples', 'app\vles')", "app\vles");
  TestStringValue("least('apples', 'app\rles')", "app\rles");

  // Tests to verify logic of greatest(). All types but STRING use the same
  // templated function, so there is no need to run all tests with all types.
  TestValue("greatest(1)", TYPE_TINYINT, 1);
  TestValue<float>("greatest(cast(1.25 as float))", TYPE_FLOAT, 1.25f);
  // Test ordering
  TestValue("greatest(10, 20)", TYPE_TINYINT, 20);
  TestValue("greatest(20, 10)", TYPE_TINYINT, 20);
  TestValue<float>("greatest(cast(500.25 as float), 300.25)", TYPE_FLOAT, 500.25f);
  TestValue<float>("greatest(cast(300.25 as float), 500.25)", TYPE_FLOAT, 500.25f);
  // Test to make sure least value is found in a mixed order set
  TestValue("greatest(1, 3, 4, 0, 6)", TYPE_TINYINT, 6);
  TestValue<float>("greatest(cast(1.25 as float), 3.25, 4.25, 0.25, 6.25)",
                   TYPE_FLOAT, 6.25f);
  // Test to make sure the least value is found from a list of duplicates
  TestValue("greatest(1, 1, 1, 1)", TYPE_TINYINT, 1);
  TestValue<float>("greatest(cast(1.0 as float), 1.0, 1.0, 1.0)", TYPE_FLOAT, 1.0f);
  // Test repeating groups and ordering
  TestValue("greatest(2, 2, 1, 1)", TYPE_TINYINT, 2);
  TestValue("greatest(0, -2, 1)", TYPE_TINYINT, 1);
  TestValue<float>("greatest(cast(2.0 as float), 2.0, 1.0, 1.0)", TYPE_FLOAT, 2.0f);
  TestValue<float>("greatest(cast(0.0 as float), -2.0, 1.0)", TYPE_FLOAT, 1.0f);
  // Test all int types. A list of values will be built, each iteration adds a bigger
  // value. This requires min_int_values_ to be an ordered map.
  val_list = "0";
  for (IntValMap::value_type& entry: min_int_values_) {
    string val_str = lexical_cast<string>(entry.second);
    val_list.append(", " + val_str);
    PrimitiveType t = static_cast<PrimitiveType>(entry.first);
    TestValue<int64_t>("greatest(" + val_list + ")", t, entry.second);
  }
  // Test double type.
  TestValue<double>("greatest(cast(0.0 as float), cast(-2.0 as double), 1.0)",
                    TYPE_DOUBLE, 1.0);
  // Test timestamp param
  TestStringValue("cast(greatest(cast('2014-09-26 12:00:00' as timestamp), "
      "cast('2013-09-26 12:00:00' as timestamp)) as string)", "2014-09-26 12:00:00");
  // Test string param
  TestStringValue("greatest('2', '5', '12', '3')", "5");
  TestStringValue("greatest('apples', 'oranges', 'bananas')", "oranges");
  TestStringValue("greatest('apples', 'applis', 'applas')", "applis");
  TestStringValue("greatest('apples', '!applis', 'applas')", "apples");
  TestStringValue("greatest('apples', 'apples', 'apples')", "apples");
  TestStringValue("greatest('apples')", "apples");
  TestStringValue("greatest('A')", "A");
  TestStringValue("greatest('A', 'a')", "a");
  // Test ordering
  TestStringValue("greatest('a', 'A')", "a");
  TestStringValue("greatest('APPLES', 'APPLES')", "APPLES");
  TestStringValue("greatest('apples', 'APPLES')", "apples");
  TestStringValue("greatest('apples', 'app\nles')", "apples");
  TestStringValue("greatest('apples', 'app les')", "apples");
  TestStringValue("greatest('apples', 'app\nles')", "apples");
  TestStringValue("greatest('apples', 'app\tles')", "apples");
  TestStringValue("greatest('apples', 'app\fles')", "apples");
  TestStringValue("greatest('apples', 'app\vles')", "apples");
  TestStringValue("greatest('apples', 'app\rles')", "apples");

  // NULL arguments. In some cases the NULL can match multiple overloads so the result
  // type depends on the order in which function overloads are considered.
  TestIsNull("abs(NULL)", TYPE_SMALLINT);
  TestIsNull("sign(NULL)", TYPE_FLOAT);
  TestIsNull("exp(NULL)", TYPE_DOUBLE);
  TestIsNull("ln(NULL)", TYPE_DOUBLE);
  TestIsNull("log10(NULL)", TYPE_DOUBLE);
  TestIsNull("log2(NULL)", TYPE_DOUBLE);
  TestIsNull("log(NULL, 64.0)", TYPE_DOUBLE);
  TestIsNull("log(2.0, NULL)", TYPE_DOUBLE);
  TestIsNull("log(NULL, NULL)", TYPE_DOUBLE);
  TestIsNull("pow(NULL, 10.0)", TYPE_DOUBLE);
  TestIsNull("pow(2.0, NULL)", TYPE_DOUBLE);
  TestIsNull("pow(NULL, NULL)", TYPE_DOUBLE);
  TestIsNull("power(NULL, 10.0)", TYPE_DOUBLE);
  TestIsNull("power(2.0, NULL)", TYPE_DOUBLE);
  TestIsNull("power(NULL, NULL)", TYPE_DOUBLE);
  TestIsNull("sqrt(NULL)", TYPE_DOUBLE);
  TestIsNull("rand(NULL)", TYPE_DOUBLE);
  TestIsNull("pmod(NULL, 3)", TYPE_BIGINT);
  TestIsNull("pmod(10, NULL)", TYPE_BIGINT);
  TestIsNull("pmod(NULL, NULL)", TYPE_BIGINT);
  TestIsNull("fmod(NULL, cast(3.2 as float))", TYPE_FLOAT);
  TestIsNull("fmod(cast(10.3 as float), NULL)", TYPE_FLOAT);
  TestIsNull("fmod(NULL, cast(3.2 as double))", TYPE_DOUBLE);
  TestIsNull("fmod(cast(10.3 as double), NULL)", TYPE_DOUBLE);
  TestIsNull("NULL % cast(3.2 as float)", TYPE_FLOAT);
  TestIsNull("cast(10.3 as float) % NULL", TYPE_FLOAT);
  TestIsNull("NULL % cast(3.2 as double)", TYPE_DOUBLE);
  TestIsNull("cast(10.3 as double) % NULL", TYPE_DOUBLE);
  TestIsNull("fmod(NULL, NULL)", TYPE_FLOAT);
  TestIsNull("NULL % NULL", TYPE_DOUBLE);
  TestIsNull("positive(NULL)", TYPE_TINYINT);
  TestIsNull("negative(NULL)", TYPE_TINYINT);
  TestIsNull("quotient(NULL, 1.0)", TYPE_BIGINT);
  TestIsNull("quotient(1.0, NULL)", TYPE_BIGINT);
  TestIsNull("quotient(NULL, NULL)", TYPE_BIGINT);
  TestIsNull("least(NULL)", TYPE_TINYINT);
  TestIsNull("least(cast(NULL as tinyint))", TYPE_TINYINT);
  TestIsNull("least(cast(NULL as smallint))", TYPE_SMALLINT);
  TestIsNull("least(cast(NULL as int))", TYPE_INT);
  TestIsNull("least(cast(NULL as bigint))", TYPE_BIGINT);
  TestIsNull("least(cast(NULL as float))", TYPE_FLOAT);
  TestIsNull("least(cast(NULL as double))", TYPE_DOUBLE);
  TestIsNull("least(cast(NULL as timestamp))", TYPE_TIMESTAMP);
  TestIsNull("greatest(NULL)", TYPE_TINYINT);
  TestIsNull("greatest(cast(NULL as tinyint))", TYPE_TINYINT);
  TestIsNull("greatest(cast(NULL as smallint))", TYPE_SMALLINT);
  TestIsNull("greatest(cast(NULL as int))", TYPE_INT);
  TestIsNull("greatest(cast(NULL as bigint))", TYPE_BIGINT);
  TestIsNull("greatest(cast(NULL as float))", TYPE_FLOAT);
  TestIsNull("greatest(cast(NULL as double))", TYPE_DOUBLE);
  TestIsNull("greatest(cast(NULL as timestamp))", TYPE_TIMESTAMP);
}

TEST_F(ExprTest, MathRoundingFunctions) {
  TestValue("ceil(cast(0.1 as double))", TYPE_BIGINT, 1);
  TestValue("ceil(cast(-10.05 as double))", TYPE_BIGINT, -10);
  TestValue("ceil(cast(23.6 as double))", TYPE_BIGINT, 24);
  TestValue("ceiling(cast(0.1 as double))", TYPE_BIGINT, 1);
  TestValue("ceiling(cast(-10.05 as double))", TYPE_BIGINT, -10);
  TestValue("floor(cast(0.1 as double))", TYPE_BIGINT, 0);
  TestValue("floor(cast(-10.007 as double))", TYPE_BIGINT, -11);
  TestValue("dfloor(cast(123.456 as double))", TYPE_BIGINT, 123);
  TestValue("truncate(cast(0.1 as double))", TYPE_BIGINT, 0);
  TestValue("truncate(cast(-10.007 as double))", TYPE_BIGINT, -10);
  TestValue("dtrunc(cast(10.99 as double))", TYPE_BIGINT, 10);

  TestValue("round(cast(1.499999 as double))", TYPE_BIGINT, 1);
  TestValue("round(cast(1.5 as double))", TYPE_BIGINT, 2);
  TestValue("round(cast(1.500001 as double))", TYPE_BIGINT, 2);
  TestValue("round(cast(-1.499999 as double))", TYPE_BIGINT, -1);
  TestValue("round(cast(-1.5 as double))", TYPE_BIGINT, -2);
  TestValue("round(cast(-1.500001 as double))", TYPE_BIGINT, -2);
  TestValue("dround(cast(2.500001 as double))", TYPE_BIGINT, 3);

  TestValue("round(cast(3.14159265 as double), 0)", TYPE_DOUBLE, 3.0);
  TestValue("round(cast(3.14159265 as double), 1)", TYPE_DOUBLE, 3.1);
  TestValue("round(cast(3.14159265 as double), 2)", TYPE_DOUBLE, 3.14);
  TestValue("round(cast(3.14159265 as double), 3)", TYPE_DOUBLE, 3.142);
  TestValue("round(cast(3.14159265 as double), 4)", TYPE_DOUBLE, 3.1416);
  TestValue("round(cast(3.14159265 as double), 5)", TYPE_DOUBLE, 3.14159);
  TestValue("round(cast(-3.14159265 as double), 0)", TYPE_DOUBLE, -3.0);
  TestValue("round(cast(-3.14159265 as double), 1)", TYPE_DOUBLE, -3.1);
  TestValue("round(cast(-3.14159265 as double), 2)", TYPE_DOUBLE, -3.14);
  TestValue("round(cast(-3.14159265 as double), 3)", TYPE_DOUBLE, -3.142);
  TestValue("round(cast(-3.14159265 as double), 4)", TYPE_DOUBLE, -3.1416);
  TestValue("round(cast(-3.14159265 as double), 5)", TYPE_DOUBLE, -3.14159);
  TestValue("dround(cast(3.14159265 as double), 5)", TYPE_DOUBLE, 3.14159);

  // NULL arguments.
  TestIsNull("ceil(cast(NULL as double))", TYPE_BIGINT);
  TestIsNull("ceiling(cast(NULL as double))", TYPE_BIGINT);
  TestIsNull("floor(cast(NULL as double))", TYPE_BIGINT);
  TestIsNull("truncate(cast(NULL as double))", TYPE_BIGINT);
  TestIsNull("round(cast(NULL as double))", TYPE_BIGINT);
  TestIsNull("round(cast(NULL as double), 1)", TYPE_DOUBLE);
  TestIsNull("round(cast(3.14159265 as double), NULL)", TYPE_DOUBLE);
  TestIsNull("round(cast(NULL as double), NULL)", TYPE_DOUBLE);
}

TEST_F(ExprTest, UnaryOperators) {
  TestValue("+1", TYPE_TINYINT, 1);
  TestValue("-1", TYPE_TINYINT, -1);
  TestValue("- -1", TYPE_TINYINT, 1);
  TestValue("+-1", TYPE_TINYINT, -1);
  TestValue("++1", TYPE_TINYINT, 1);
  TestValue("~1", TYPE_TINYINT, -2);

  TestValue("+cast(1. as float)", TYPE_FLOAT, 1.0f);
  TestValue("+cast(1.0 as float)", TYPE_FLOAT, 1.0f);
  TestValue("-cast(1.0 as float)", TYPE_DOUBLE, -1.0);

  TestValue("1 - - - 1", TYPE_SMALLINT, 0);

  // IMPALA-4877: Verify that unary minus has high precedence and is integrated into
  // literals.
  TestValue("-1 & 8", TYPE_TINYINT, 8);
}

TEST_F(ExprTest, MoscowTimezoneConversion) {
#pragma push_macro("UTC_TO_MSC")
#pragma push_macro("MSC_TO_UTC")
#define UTC_TO_MSC(X) ("cast(from_utc_timestamp('" X "', 'Europe/Moscow') as string)")
#define MSC_TO_UTC(X) ("cast(to_utc_timestamp('" X "', 'Europe/Moscow') as string)")

  // IMPALA-4209: Moscow time change in 2011.
  // Last DST change before the transition.
  TestStringValue(UTC_TO_MSC("2010-10-30 22:59:59"), "2010-10-31 02:59:59");
  TestStringValue(UTC_TO_MSC("2010-10-30 23:00:00"), "2010-10-31 02:00:00");
  TestStringValue(MSC_TO_UTC("2010-10-31 01:59:59"), "2010-10-30 21:59:59");
  // Since 2am to 2:59:59.999...am MSC happens twice, the ambiguity gets resolved by
  // returning null.
  TestIsNull(MSC_TO_UTC("2010-10-31 02:00:00"), TYPE_STRING);
  TestIsNull(MSC_TO_UTC("2010-10-31 02:59:59"), TYPE_STRING);
  TestStringValue(MSC_TO_UTC("2010-10-31 03:00:00"), "2010-10-31 00:00:00");

  // Moscow time transitions to UTC+4.
  TestStringValue(UTC_TO_MSC("2011-03-26 22:59:59"), "2011-03-27 01:59:59");
  TestStringValue(UTC_TO_MSC("2011-03-26 23:00:00"), "2011-03-27 03:00:00");
  TestStringValue(MSC_TO_UTC("2011-03-27 01:59:59"), "2011-03-26 22:59:59");
  // Since 2am to 2:59:59.999...am MSC happens twice, the ambiguity gets resolved by
  // returning null.
  TestIsNull(MSC_TO_UTC("2011-03-27 02:00:00"), TYPE_STRING);
  TestIsNull(MSC_TO_UTC("2011-03-27 02:59:59"), TYPE_STRING);
  TestStringValue(MSC_TO_UTC("2011-03-27 03:00:00"), "2011-03-26 23:00:00");

  // No more DST after the transition.
  TestStringValue(UTC_TO_MSC("2011-12-20 09:00:00"), "2011-12-20 13:00:00");
  TestStringValue(UTC_TO_MSC("2012-06-20 09:00:00"), "2012-06-20 13:00:00");
  TestStringValue(UTC_TO_MSC("2012-12-20 09:00:00"), "2012-12-20 13:00:00");
  TestStringValue(MSC_TO_UTC("2011-12-20 13:00:00"), "2011-12-20 09:00:00");
  TestStringValue(MSC_TO_UTC("2012-06-20 13:00:00"), "2012-06-20 09:00:00");
  TestStringValue(MSC_TO_UTC("2012-12-20 13:00:00"), "2012-12-20 09:00:00");

  // IMPALA-4546: Moscow time change in 2014.
  // UTC+4 is changed to UTC+3
  TestStringValue(UTC_TO_MSC("2014-10-25 21:59:59"), "2014-10-26 01:59:59");
  TestStringValue(UTC_TO_MSC("2014-10-25 22:00:00"), "2014-10-26 01:00:00");
  TestStringValue(UTC_TO_MSC("2014-10-25 23:00:00"), "2014-10-26 02:00:00");
  TestStringValue(MSC_TO_UTC("2014-10-26 00:59:59"), "2014-10-25 20:59:59");
  // Since 1am to 1:59:59.999...am MSC happens twice, the ambiguity gets resolved by
  // returning null.
  TestIsNull(MSC_TO_UTC("2014-10-26 01:00:00"), TYPE_STRING);
  TestIsNull(MSC_TO_UTC("2014-10-26 01:59:59"), TYPE_STRING);
  TestStringValue(MSC_TO_UTC("2014-10-26 02:00:00"), "2014-10-25 23:00:00");

  // Still no DST after the transition.
  TestStringValue(UTC_TO_MSC("2014-12-20 09:00:00"), "2014-12-20 12:00:00");
  TestStringValue(UTC_TO_MSC("2015-06-20 09:00:00"), "2015-06-20 12:00:00");
  TestStringValue(UTC_TO_MSC("2015-12-20 09:00:00"), "2015-12-20 12:00:00");
  TestStringValue(MSC_TO_UTC("2014-12-20 12:00:00"), "2014-12-20 09:00:00");
  TestStringValue(MSC_TO_UTC("2015-06-20 12:00:00"), "2015-06-20 09:00:00");
  TestStringValue(MSC_TO_UTC("2015-12-20 12:00:00"), "2015-12-20 09:00:00");

  // Timestamp conversions of "dateless" times should return null (and not crash,
  // see IMPALA-5983).
  TestIsNull(UTC_TO_MSC("10:00:00"), TYPE_STRING);
  TestIsNull(MSC_TO_UTC("10:00:00"), TYPE_STRING);

#pragma pop_macro("MSC_TO_UTC")
#pragma pop_macro("UTC_TO_MSC")
}

TEST_F(ExprTest, TimestampFunctions) {
  // Regression for IMPALA-1105
  TestIsNull("cast(cast('NOTATIMESTAMP' as timestamp) as string)", TYPE_STRING);

  TestStringValue("cast(cast('2012-01-01 09:10:11.123456789' as timestamp) as string)",
      "2012-01-01 09:10:11.123456789");
  // Add/sub years.
  TestStringValue("cast(date_add(cast('2012-01-01 09:10:11.123456789' "
      "as timestamp), interval 10 years) as string)",
      "2022-01-01 09:10:11.123456789");
  TestStringValue("cast(date_sub(cast('2012-01-01 09:10:11.123456789' "
      "as timestamp), interval 10 years) as string)",
      "2002-01-01 09:10:11.123456789");
  TestStringValue("cast(date_add(cast('2012-01-01 09:10:11.123456789' "
      "as timestamp), interval cast(10 as bigint) years) as string)",
      "2022-01-01 09:10:11.123456789");
  TestStringValue("cast(date_sub(cast('2012-01-01 09:10:11.123456789' "
      "as timestamp), interval cast(10 as bigint) years) as string)",
      "2002-01-01 09:10:11.123456789");
  // These return NULL because the resulting year is out of range.
  TestIsNull(
      "CAST('2005-10-11 00:00:00' AS TIMESTAMP) - INTERVAL 718 YEAR", TYPE_TIMESTAMP);
  TestIsNull(
      "CAST('2005-10-11 00:00:00' AS TIMESTAMP) + INTERVAL -718 YEAR", TYPE_TIMESTAMP);
  TestIsNull(
      "CAST('2005-10-11 00:00:00' AS TIMESTAMP) + INTERVAL 9718 YEAR", TYPE_TIMESTAMP);
  TestIsNull(
      "CAST('2005-10-11 00:00:00' AS TIMESTAMP) - INTERVAL -9718 YEAR", TYPE_TIMESTAMP);
  TestIsNull(
      "CAST('1405-01-11 00:00:00' AS TIMESTAMP) + INTERVAL -61 MONTH", TYPE_TIMESTAMP);
  TestIsNull(
      "CAST('1405-01-11 00:00:00' AS TIMESTAMP) - INTERVAL 61 MONTH", TYPE_TIMESTAMP);
  TestIsNull(
      "CAST('9995-12-11 00:00:00' AS TIMESTAMP) + INTERVAL 61 MONTH", TYPE_TIMESTAMP);
  TestIsNull(
      "CAST('9995-12-11 00:00:00' AS TIMESTAMP) - INTERVAL -61 MONTH", TYPE_TIMESTAMP);
  TestIsNull(
      "CAST('9999-12-31 21:00:00' AS TIMESTAMP) + INTERVAL 1000 DAYS", TYPE_TIMESTAMP);
  TestIsNull(
      "CAST('1400-01-01 00:12:00' AS TIMESTAMP) - INTERVAL 1 DAYS", TYPE_TIMESTAMP);
  TestIsNull(
      "CAST('9999-12-31 21:00:00' AS TIMESTAMP) + INTERVAL 10000 HOURS", TYPE_TIMESTAMP);
  TestIsNull(
      "CAST('1400-01-01 00:12:00' AS TIMESTAMP) - INTERVAL 24 HOURS", TYPE_TIMESTAMP);
  TestIsNull("CAST('9999-12-31 21:00:00' AS TIMESTAMP) + INTERVAL 1000000 MINUTES",
      TYPE_TIMESTAMP);
  TestIsNull(
      "CAST('1400-01-01 00:12:00' AS TIMESTAMP) - INTERVAL 13 MINUTES", TYPE_TIMESTAMP);
  TestIsNull("CAST('9999-12-31 23:59:59' AS TIMESTAMP) + INTERVAL 1 SECONDS",
      TYPE_TIMESTAMP);
  TestIsNull(
      "CAST('1400-01-01 00:00:00' AS TIMESTAMP) - INTERVAL 1 SECONDS", TYPE_TIMESTAMP);
  TestIsNull(
      "CAST('9999-12-31 21:00:00' AS TIMESTAMP) + INTERVAL 100000000000 MILLISECONDS",
      TYPE_TIMESTAMP);
  TestIsNull("CAST('1400-01-01 00:00:00' AS TIMESTAMP) - INTERVAL 1 MILLISECONDS",
      TYPE_TIMESTAMP);
  TestIsNull(
      "CAST('9999-12-31 21:00:00' AS TIMESTAMP) + INTERVAL 100000000000000 MICROSECONDS",
      TYPE_TIMESTAMP);
  TestIsNull("CAST('1400-01-01 00:00:00' AS TIMESTAMP) - INTERVAL 1 MICROSECONDS",
      TYPE_TIMESTAMP);
  TestIsNull("CAST('9999-12-31 21:00:00' AS TIMESTAMP) + INTERVAL 100000000000000000 "
      "NANOSECONDS", TYPE_TIMESTAMP);
  TestIsNull("CAST('1400-01-01 00:00:00' AS TIMESTAMP) - INTERVAL 1 NANOSECONDS",
      TYPE_TIMESTAMP);
  // Add/sub months.
  TestStringValue("cast(date_add(cast('2012-01-01 09:10:11.123456789' "
      "as timestamp), interval 13 months) as string)",
      "2013-02-01 09:10:11.123456789");
  TestStringValue("cast(date_sub(cast('2013-02-01 09:10:11.123456789' "
      "as timestamp), interval 13 months) as string)",
      "2012-01-01 09:10:11.123456789");
  TestStringValue("cast(date_add(cast('2012-01-31 09:10:11.123456789' "
      "as timestamp), interval cast(1 as bigint) month) as string)",
      "2012-02-29 09:10:11.123456789");
  TestStringValue("cast(date_sub(cast('2012-02-29 09:10:11.123456789' "
      "as timestamp), interval cast(1 as bigint) month) as string)",
      "2012-01-29 09:10:11.123456789");
  TestStringValue("cast(add_months(cast('1405-01-29 09:10:11.123456789' "
      "as timestamp), -60) as string)",
      "1400-01-29 09:10:11.123456789");
  TestStringValue("cast(add_months(cast('9995-01-29 09:10:11.123456789' "
      "as timestamp), 59) as string)",
      "9999-12-29 09:10:11.123456789");
  // Add/sub weeks.
  TestStringValue("cast(date_add(cast('2012-01-01 09:10:11.123456789' "
      "as timestamp), interval 2 weeks) as string)",
      "2012-01-15 09:10:11.123456789");
  TestStringValue("cast(date_sub(cast('2012-01-15 09:10:11.123456789' "
      "as timestamp), interval 2 weeks) as string)",
      "2012-01-01 09:10:11.123456789");
  TestStringValue("cast(date_add(cast('2012-01-01 09:10:11.123456789' "
      "as timestamp), interval cast(53 as bigint) weeks) as string)",
      "2013-01-06 09:10:11.123456789");
  TestStringValue("cast(date_sub(cast('2013-01-06 09:10:11.123456789' "
      "as timestamp), interval cast(53 as bigint) weeks) as string)",
      "2012-01-01 09:10:11.123456789");
  // Add/sub days.
  TestStringValue("cast(date_add(cast('2012-01-01 09:10:11.123456789' "
      "as timestamp), interval 10 days) as string)",
      "2012-01-11 09:10:11.123456789");
  TestStringValue("cast(date_sub(cast('2012-01-01 09:10:11.123456789' "
      "as timestamp), interval 10 days) as string)",
      "2011-12-22 09:10:11.123456789");
  TestStringValue("cast(date_add(cast('2011-12-22 09:10:11.12345678' "
      "as timestamp), interval cast(10 as bigint) days) as string)",
      "2012-01-01 09:10:11.123456780");
  TestStringValue("cast(date_sub(cast('2011-12-22 09:10:11.12345678' "
      "as timestamp), interval cast(365 as bigint) days) as string)",
      "2010-12-22 09:10:11.123456780");
  // Add/sub days (HIVE's date_add/sub variant).
  TestStringValue("cast(date_add(cast('2012-01-01 09:10:11.123456789' "
      "as timestamp), 10) as string)",
      "2012-01-11 09:10:11.123456789");
  TestStringValue(
      "cast(date_sub(cast('2012-01-01 09:10:11.123456789' as timestamp), 10) as string)",
      "2011-12-22 09:10:11.123456789");
  TestStringValue(
      "cast(date_add(cast('2011-12-22 09:10:11.12345678' as timestamp),"
      "cast(10 as bigint)) as string)", "2012-01-01 09:10:11.123456780");
  TestStringValue(
      "cast(date_sub(cast('2011-12-22 09:10:11.12345678' as timestamp),"
      "cast(365 as bigint)) as string)", "2010-12-22 09:10:11.123456780");
  // Add/sub hours.
  TestStringValue("cast(date_add(cast('2012-01-01 00:00:00.123456789' "
      "as timestamp), interval 25 hours) as string)",
      "2012-01-02 01:00:00.123456789");
  TestStringValue("cast(date_sub(cast('2012-01-02 01:00:00.123456789' "
      "as timestamp), interval 25 hours) as string)",
      "2012-01-01 00:00:00.123456789");
  TestStringValue("cast(date_add(cast('2012-01-01 00:00:00.123456789' "
      "as timestamp), interval cast(25 as bigint) hours) as string)",
      "2012-01-02 01:00:00.123456789");
  TestStringValue("cast(date_sub(cast('2012-01-02 01:00:00.123456789' "
      "as timestamp), interval cast(25 as bigint) hours) as string)",
      "2012-01-01 00:00:00.123456789");
  // Add/sub minutes.
  TestStringValue("cast(date_add(cast('2012-01-01 00:00:00.123456789' "
      "as timestamp), interval 1533 minutes) as string)",
      "2012-01-02 01:33:00.123456789");
  TestStringValue("cast(date_sub(cast('2012-01-02 01:33:00.123456789' "
      "as timestamp), interval 1533 minutes) as string)",
      "2012-01-01 00:00:00.123456789");
  TestStringValue("cast(date_add(cast('2012-01-01 00:00:00.123456789' "
      "as timestamp), interval cast(1533 as bigint) minutes) as string)",
      "2012-01-02 01:33:00.123456789");
  TestStringValue("cast(date_sub(cast('2012-01-02 01:33:00.123456789' "
      "as timestamp), interval cast(1533 as bigint) minutes) as string)",
      "2012-01-01 00:00:00.123456789");
  // Add/sub seconds.
  TestStringValue("cast(date_add(cast('2012-01-01 00:00:00.123456789' "
      "as timestamp), interval 90033 seconds) as string)",
      "2012-01-02 01:00:33.123456789");
  TestStringValue("cast(date_sub(cast('2012-01-02 01:00:33.123456789' "
      "as timestamp), interval 90033 seconds) as string)",
      "2012-01-01 00:00:00.123456789");
  TestStringValue("cast(date_add(cast('2012-01-01 00:00:00.123456789' "
      "as timestamp), interval cast(90033 as bigint) seconds) as string)",
      "2012-01-02 01:00:33.123456789");
  TestStringValue("cast(date_sub(cast('2012-01-02 01:00:33.123456789' "
      "as timestamp), interval cast(90033 as bigint) seconds) as string)",
      "2012-01-01 00:00:00.123456789");
  // Add/sub milliseconds.
  TestStringValue("cast(date_add(cast('2012-01-01 00:00:00.000000001' "
      "as timestamp), interval 90000033 milliseconds) as string)",
      "2012-01-02 01:00:00.033000001");
  TestStringValue("cast(date_sub(cast('2012-01-02 01:00:00.033000001' "
      "as timestamp), interval 90000033 milliseconds) as string)",
      "2012-01-01 00:00:00.000000001");
  TestStringValue("cast(date_add(cast('2012-01-01 00:00:00.000000001' "
      "as timestamp), interval cast(90000033 as bigint) milliseconds) as string)",
      "2012-01-02 01:00:00.033000001");
  TestStringValue("cast(date_sub(cast('2012-01-02 01:00:00.033000001' "
      "as timestamp), interval cast(90000033 as bigint) milliseconds) as string)",
      "2012-01-01 00:00:00.000000001");
  // Add/sub microseconds.
  TestStringValue("cast(date_add(cast('2012-01-01 00:00:00.000000001' "
      "as timestamp), interval 1033 microseconds) as string)",
      "2012-01-01 00:00:00.001033001");
  TestStringValue("cast(date_sub(cast('2012-01-01 00:00:00.001033001' "
      "as timestamp), interval 1033 microseconds) as string)",
      "2012-01-01 00:00:00.000000001");
  TestStringValue("cast(date_add(cast('2012-01-01 00:00:00.000000001' "
      "as timestamp), interval cast(1033 as bigint) microseconds) as string)",
      "2012-01-01 00:00:00.001033001");
  TestStringValue("cast(date_sub(cast('2012-01-01 00:00:00.001033001' "
      "as timestamp), interval cast(1033 as bigint) microseconds) as string)",
      "2012-01-01 00:00:00.000000001");
  // Add/sub nanoseconds.
  TestStringValue("cast(date_add(cast('2012-01-01 00:00:00.000000001' "
      "as timestamp), interval 1033 nanoseconds) as string)",
      "2012-01-01 00:00:00.000001034");
  TestStringValue("cast(date_sub(cast('2012-01-01 00:00:00.000001034' "
      "as timestamp), interval 1033 nanoseconds) as string)",
      "2012-01-01 00:00:00.000000001");
  TestStringValue("cast(date_add(cast('2012-01-01 00:00:00.000000001' "
      "as timestamp), interval cast(1033 as bigint) nanoseconds) as string)",
      "2012-01-01 00:00:00.000001034");
  TestStringValue("cast(date_sub(cast('2012-01-01 00:00:00.000001034' "
      "as timestamp), interval cast(1033 as bigint) nanoseconds) as string)",
      "2012-01-01 00:00:00.000000001");

  // NULL arguments.
  TestIsNull("date_add(NULL, interval 10 years)", TYPE_TIMESTAMP);
  TestIsNull("date_add(cast('2012-01-01 09:10:11.123456789' as timestamp),"
      "interval NULL years)", TYPE_TIMESTAMP);
  TestIsNull("date_add(NULL, interval NULL years)", TYPE_TIMESTAMP);
  TestIsNull("date_sub(NULL, interval 10 years)", TYPE_TIMESTAMP);
  TestIsNull("date_sub(cast('2012-01-01 09:10:11.123456789' as timestamp),"
      "interval NULL years)", TYPE_TIMESTAMP);
  TestIsNull("date_sub(NULL, interval NULL years)", TYPE_TIMESTAMP);

  // ADD_MONTHS() differs from '... + INTERVAL 1 MONTH'. The function version treats
  // an input value that falls on the last day of the month specially -- the result will
  // also always be the last day of the month.
  TestStringValue("cast(add_months(cast('2000-02-29' as timestamp), 1) as string)",
      "2000-03-31 00:00:00");
  TestStringValue("cast(cast('2000-02-29' as timestamp) + interval 1 month as string)",
      "2000-03-29 00:00:00");
  TestStringValue("cast(add_months(cast('1999-02-28' as timestamp), 12) as string)",
      "2000-02-29 00:00:00");
  TestStringValue("cast(cast('1999-02-28' as timestamp) + interval 12 month as string)",
      "2000-02-28 00:00:00");

  // Try a few cases in which ADD_MONTHS() and INTERVAL produce the same result.
  TestStringValue("cast(months_sub(cast('2000-03-31' as timestamp), 1) as string)",
      "2000-02-29 00:00:00");
  TestStringValue("cast(cast('2000-03-31' as timestamp) - interval 1 month as string)",
      "2000-02-29 00:00:00");
  TestStringValue("cast(months_add(cast('2000-03-31' as timestamp), -2) as string)",
      "2000-01-31 00:00:00");
  TestStringValue("cast(cast('2000-03-31' as timestamp) + interval -2 month as string)",
      "2000-01-31 00:00:00");

  // Test add/sub behavior with edge case time interval values.
  string max_int = lexical_cast<string>(numeric_limits<int32_t>::max());
  string max_long = lexical_cast<string>(numeric_limits<int64_t>::max());
  typedef map<string, int64_t> MaxIntervals;
  MaxIntervals max_intervals;
  max_intervals["years"] = TimestampFunctions::MAX_YEAR_INTERVAL;
  max_intervals["months"] = TimestampFunctions::MAX_MONTH_INTERVAL;
  max_intervals["weeks"] = TimestampFunctions::MAX_WEEK_INTERVAL;
  max_intervals["days"] = TimestampFunctions::MAX_DAY_INTERVAL;
  max_intervals["hours"] = TimestampFunctions::MAX_HOUR_INTERVAL;
  max_intervals["minutes"] = TimestampFunctions::MAX_MINUTE_INTERVAL;
  max_intervals["seconds"] = TimestampFunctions::MAX_SEC_INTERVAL;
  max_intervals["microseconds"] = TimestampFunctions::MAX_MILLI_INTERVAL;
  max_intervals["nanoseconds"] = numeric_limits<int64_t>::max();
  string year_5000 = "cast('5000-01-01' as timestamp)";
  string gt_year_5000 = "cast('5000-01-01 00:00:00.1' as timestamp)";
  string lt_year_5000 = "cast('4999-12-31 23:59:59.9' as timestamp)";
  for (MaxIntervals::iterator it = max_intervals.begin(); it != max_intervals.end();
      ++it) {
    const string& unit = it->first;
    const string& lt_max_interval =
        lexical_cast<string>(static_cast<int64_t>(0.9 * it->second));
    // Test that pushing a value beyond the max/min values results in a NULL.
    TestIsNull(unit + "_add(cast('9999-12-31 23:59:59' as timestamp), "
        + lt_max_interval + ")", TYPE_TIMESTAMP);
    TestIsNull(unit + "_sub(cast('1400-01-01 00:00:00' as timestamp), "
        + lt_max_interval + ")", TYPE_TIMESTAMP);

    // Same as above but with edge case values of max int/long.
    TestIsNull(unit + "_add(years_add(cast('9999-12-31 23:59:59' as timestamp), 1), "
        + max_int + ")", TYPE_TIMESTAMP);
    TestIsNull(unit + "_sub(cast('1400-01-01 00:00:00' as timestamp), " + max_int + ")",
        TYPE_TIMESTAMP);
    TestIsNull(unit + "_add(years_add(cast('9999-12-31 23:59:59' as timestamp), 1), "
        + max_long + ")", TYPE_TIMESTAMP);
    TestIsNull(unit + "_sub(cast('1400-01-01 00:00:00' as timestamp), " + max_long
        + ")", TYPE_TIMESTAMP);

    // Test that adding/subtracting a value slightly less than the MAX_*_INTERVAL
    // can result in a non-NULL.
    TestIsNotNull(unit + "_add(cast('1400-01-01 00:00:00' as timestamp), "
        + lt_max_interval + ")", TYPE_TIMESTAMP);
    TestIsNotNull(unit + "_sub(cast('9999-12-31 23:59:59' as timestamp), "
        + lt_max_interval + ")", TYPE_TIMESTAMP);

    // Test that adding/subtracting either results in NULL or a value more/less than
    // the original value.
    TestValue("isnull(" + unit + "_add(" + year_5000 + ", " + max_int
        + "), " + gt_year_5000 + ") > " + year_5000, TYPE_BOOLEAN, true);
    TestValue("isnull(" + unit + "_sub(" + year_5000 + ", " + max_int
        + "), " + lt_year_5000 + ") < " + year_5000, TYPE_BOOLEAN, true);
    TestValue("isnull(" + unit + "_add(" + year_5000 + ", " + max_long
        + "), " + gt_year_5000 + ") > " + year_5000, TYPE_BOOLEAN, true);
    TestValue("isnull(" + unit + "_sub(" + year_5000 + ", " + max_long
        + "), " + lt_year_5000 + ") < " + year_5000, TYPE_BOOLEAN, true);
  }

  // Regression test for IMPALA-2260, a seemingly non-edge case value results in an
  // overflow causing the 9999999 interval to become negative.
  for (MaxIntervals::iterator it = max_intervals.begin(); it != max_intervals.end();
      ++it) {
    const string& unit = it->first;

    // The static max interval definitions aren't exact so (max interval - 1) may still
    // produce a NULL. The static max interval definitions are within an order of
    // magnitude of the real max values so testing can start at max / 10.
    for (int64_t interval = it->second / 10; interval > 0; interval /= 10) {
      const string& sql_interval = lexical_cast<string>(interval);
      TestIsNotNull(unit + "_add(cast('1400-01-01 00:00:00' as timestamp), "
          + sql_interval + ")", TYPE_TIMESTAMP);
      TestIsNotNull(unit + "_sub(cast('9999-12-31 23:59:59' as timestamp), "
          + sql_interval + ")", TYPE_TIMESTAMP);
    }
  }

  // Test Unix epoch conversions.
  TestTimestampUnixEpochConversions(0, "1970-01-01 00:00:00");

  // Regression tests for IMPALA-1579, Unix times should be BIGINTs instead of INTs.
  TestValue("unix_timestamp('2038-01-19 03:14:07')", TYPE_BIGINT, 2147483647);
  TestValue("unix_timestamp('2038-01-19 03:14:08')", TYPE_BIGINT, 2147483648);
  TestValue("unix_timestamp('2038/01/19 03:14:08', 'yyyy/MM/dd HH:mm:ss')", TYPE_BIGINT,
      2147483648);
  TestValue("unix_timestamp(cast('2038-01-19 03:14:08' as timestamp))", TYPE_BIGINT,
      2147483648);


  // Test Unix epoch conversions again but now converting into local timestamp values.
  {
    ScopedLocalUnixTimestampConversionOverride use_local;
    // Determine what the local time would have been when it was 1970-01-01 GMT
    ptime local_time_at_epoch = c_local_adjustor<ptime>::utc_to_local(from_time_t(0));
    // ... and as an Impala compatible string.
    string local_time_at_epoch_as_str = to_iso_extended_string(local_time_at_epoch.date())
        + " " + to_simple_string(local_time_at_epoch.time_of_day());
    // Determine what the Unix timestamp would have been when it was 1970-01-01 in the
    // local time zone.
    int64_t unix_time_at_local_epoch =
        (from_time_t(0) - local_time_at_epoch).total_seconds();
    TestTimestampUnixEpochConversions(unix_time_at_local_epoch,
        local_time_at_epoch_as_str);

    // Check that daylight savings calculation is done.
    {
      ScopedTimeZoneOverride time_zone("PST8PDT");
      TestValue("unix_timestamp('2015-01-01')", TYPE_BIGINT, 1420099200);   // PST applies
      TestValue("unix_timestamp('2015-07-01')", TYPE_BIGINT, 1435734000);   // PDT applies
    }
    {
      ScopedTimeZoneOverride time_zone("EST5EDT");
      TestValue("unix_timestamp('2015-01-01')", TYPE_BIGINT, 1420088400);   // EST applies
      TestValue("unix_timestamp('2015-07-01')", TYPE_BIGINT, 1435723200);   // EDT applies
    }
  }

  TestIsNull("from_unixtime(NULL, 'yyyy-MM-dd')", TYPE_STRING);
  TestStringValue("from_unixtime(unix_timestamp('1999-01-01 10:10:10'), \
      'yyyy-MM-dd')", "1999-01-01");
  TestStringValue("from_unixtime(unix_timestamp('1999-01-01 10:10:10'), \
      'yyyy-MM-dd HH:mm:ss')", "1999-01-01 10:10:10");
  TestStringValue("from_unixtime(unix_timestamp('1999-01-01 10:10:10') + (60*60*24), \
        'yyyy-MM-dd')", "1999-01-02");
  TestStringValue("from_unixtime(unix_timestamp('1999-01-01 10:10:10') + 10, \
        'yyyy-MM-dd HH:mm:ss')", "1999-01-01 10:10:20");
  TestStringValue("from_timestamp(cast('1999-01-01 10:10:10' as timestamp), \
      'yyyy-MM-dd')", "1999-01-01");
  TestStringValue("from_timestamp(cast('1999-01-01 10:10:10' as timestamp), \
      'yyyy-MM-dd HH:mm:ss')", "1999-01-01 10:10:10");
  TestStringValue("from_timestamp(to_timestamp(unix_timestamp('1999-01-01 10:10:10') \
      + 60*60*24), 'yyyy-MM-dd')", "1999-01-02");
  TestStringValue("from_timestamp(to_timestamp(unix_timestamp('1999-01-01 10:10:10') \
      + 10), 'yyyy-MM-dd HH:mm:ss')", "1999-01-01 10:10:20");
  TestValue("cast('2011-12-22 09:10:11.123456789' as timestamp) > \
      cast('2011-12-22 09:10:11.12345678' as timestamp)", TYPE_BOOLEAN, true);
  TestValue("cast('2011-12-22 08:10:11.123456789' as timestamp) > \
      cast('2011-12-22 09:10:11.12345678' as timestamp)", TYPE_BOOLEAN, false);
  TestValue("cast('2011-12-22 09:10:11.000000' as timestamp) = \
      cast('2011-12-22 09:10:11' as timestamp)", TYPE_BOOLEAN, true);
  TestValue("year(cast('2011-12-22 09:10:11.000000' as timestamp))", TYPE_INT, 2011);
  TestValue("month(cast('2011-12-22 09:10:11.000000' as timestamp))", TYPE_INT, 12);
  TestValue("dayofmonth(cast('2011-12-22 09:10:11.000000' as timestamp))", TYPE_INT, 22);
  TestValue("day(cast('2011-12-22 09:10:11.000000' as timestamp))", TYPE_INT, 22);
  TestValue("dayofyear(cast('2011-12-22 09:10:11.000000' as timestamp))", TYPE_INT, 356);
  TestValue("weekofyear(cast('2011-12-22 09:10:11.000000' as timestamp))", TYPE_INT, 51);
  TestValue("dayofweek(cast('2011-12-18 09:10:11.000000' as timestamp))", TYPE_INT, 1);
  TestValue("dayofweek(cast('2011-12-22 09:10:11.000000' as timestamp))", TYPE_INT, 5);
  TestValue("dayofweek(cast('2011-12-24 09:10:11.000000' as timestamp))", TYPE_INT, 7);
  TestValue("hour(cast('2011-12-22 09:10:11.000000' as timestamp))", TYPE_INT, 9);
  TestValue("minute(cast('2011-12-22 09:10:11.000000' as timestamp))", TYPE_INT, 10);
  TestValue("second(cast('2011-12-22 09:10:11.000000' as timestamp))", TYPE_INT, 11);
  TestValue("millisecond(cast('2011-12-22 09:10:11.123456' as timestamp))",
      TYPE_INT, 123);
  TestValue("millisecond(cast('2011-12-22 09:10:11' as timestamp))", TYPE_INT, 0);
  TestValue("millisecond(cast('2011-12-22' as timestamp))", TYPE_INT, 0);
  TestValue("year(cast('2011-12-22' as timestamp))", TYPE_INT, 2011);
  TestValue("month(cast('2011-12-22' as timestamp))", TYPE_INT, 12);
  TestValue("dayofmonth(cast('2011-12-22' as timestamp))", TYPE_INT, 22);
  TestValue("day(cast('2011-12-22' as timestamp))", TYPE_INT, 22);
  TestValue("dayofyear(cast('2011-12-22' as timestamp))", TYPE_INT, 356);
  TestValue("weekofyear(cast('2011-12-22' as timestamp))", TYPE_INT, 51);
  TestValue("dayofweek(cast('2011-12-18' as timestamp))", TYPE_INT, 1);
  TestValue("dayofweek(cast('2011-12-22' as timestamp))", TYPE_INT, 5);
  TestValue("dayofweek(cast('2011-12-24' as timestamp))", TYPE_INT, 7);
  TestValue("hour(cast('09:10:11.000000' as timestamp))", TYPE_INT, 9);
  TestValue("minute(cast('09:10:11.000000' as timestamp))", TYPE_INT, 10);
  TestValue("second(cast('09:10:11.000000' as timestamp))", TYPE_INT, 11);
  TestValue("millisecond(cast('09:10:11.123456' as timestamp))", TYPE_INT, 123);
  TestStringValue(
      "to_date(cast('2011-12-22 09:10:11.12345678' as timestamp))", "2011-12-22");

  // Check that timeofday() does not crash or return incorrect results
  TestIsNotNull("timeofday()", TYPE_STRING);

  TestValue("timestamp_cmp('1964-05-04 15:33:45','1966-05-04 15:33:45')", TYPE_INT, -1);
  TestValue("timestamp_cmp('1966-09-04 15:33:45','1966-05-04 15:33:45')", TYPE_INT, 1);
  TestValue("timestamp_cmp('1966-05-04 15:33:45','1966-05-04 15:33:45')", TYPE_INT, 0);
  TestValue("timestamp_cmp('1967-06-05','1966-05-04')", TYPE_INT, 1);
  TestValue("timestamp_cmp('15:33:45','16:34:45')", TYPE_INT, -1);
  TestValue("timestamp_cmp('1966-05-04','1966-05-04 15:33:45')", TYPE_INT, -1);

  TestIsNull("timestamp_cmp('','1966-05-04 15:33:45')", TYPE_INT);
  TestIsNull("timestamp_cmp('','1966-05-04 15:33:45')", TYPE_INT);
  TestIsNull("timestamp_cmp(NULL,'1966-05-04 15:33:45')", TYPE_INT);
  // Invalid timestamp test case
  TestIsNull("timestamp_cmp('1966-5-4 5:33:45','1966-5-4 15:33:45')", TYPE_INT);

  TestValue("int_months_between('1967-07-19','1966-06-04')", TYPE_INT, 13);
  TestValue("int_months_between('1966-06-04 16:34:45','1967-07-19 15:33:46')",
      TYPE_INT, -13);
  TestValue("int_months_between('1967-07-19','1967-07-19')", TYPE_INT, 0);
  TestValue("int_months_between('2015-07-19','2015-08-18')", TYPE_INT, 0);

  TestIsNull("int_months_between('23:33:45','15:33:45')", TYPE_INT);
  TestIsNull("int_months_between('','1966-06-04')", TYPE_INT);

  TestValue("months_between('1967-07-19','1966-06-04')", TYPE_DOUBLE,
      13.48387096774194);
  TestValue("months_between('1966-06-04 16:34:45','1967-07-19 15:33:46')",
      TYPE_DOUBLE, -13.48387096774194);
  TestValue("months_between('1967-07-19','1967-07-19')", TYPE_DOUBLE, 0);
  TestValue("months_between('2015-02-28','2015-05-31')", TYPE_DOUBLE, -3);
  TestValue("months_between('2012-02-29','2012-01-31')", TYPE_DOUBLE, 1);

  TestIsNull("months_between('23:33:45','15:33:45')", TYPE_DOUBLE);
  TestIsNull("months_between('','1966-06-04')", TYPE_DOUBLE);

  TestValue("datediff(cast('2011-12-22 09:10:11.12345678' as timestamp), \
      cast('2012-12-22' as timestamp))", TYPE_INT, -366);
  TestValue("datediff(cast('2012-12-22' as timestamp), \
      cast('2011-12-22 09:10:11.12345678' as timestamp))", TYPE_INT, 366);

  TestIsNull("cast('24:59:59' as timestamp)", TYPE_TIMESTAMP);
  TestIsNull("cast('10000-12-31' as timestamp)", TYPE_TIMESTAMP);
  TestIsNull("cast('10000-12-31 23:59:59' as timestamp)", TYPE_TIMESTAMP);
  TestIsNull("cast('2000-12-31 24:59:59' as timestamp)", TYPE_TIMESTAMP);

  TestIsNull("year(cast('09:10:11.000000' as timestamp))", TYPE_INT);
  TestIsNull("month(cast('09:10:11.000000' as timestamp))", TYPE_INT);
  TestIsNull("dayofmonth(cast('09:10:11.000000' as timestamp))", TYPE_INT);
  TestIsNull("day(cast('09:10:11.000000' as timestamp))", TYPE_INT);
  TestIsNull("dayofyear(cast('09:10:11.000000' as timestamp))", TYPE_INT);
  TestIsNull("dayofweek(cast('09:10:11.000000' as timestamp))", TYPE_INT);
  TestIsNull("weekofyear(cast('09:10:11.000000' as timestamp))", TYPE_INT);
  TestIsNull("datediff(cast('09:10:11.12345678' as timestamp), "
      "cast('2012-12-22' as timestamp))", TYPE_INT);

  TestIsNull("year(NULL)", TYPE_INT);
  TestIsNull("month(NULL)", TYPE_INT);
  TestIsNull("dayofmonth(NULL)", TYPE_INT);
  TestIsNull("day(NULL)", TYPE_INT);
  TestIsNull("dayofweek(NULL)", TYPE_INT);
  TestIsNull("dayofyear(NULL)", TYPE_INT);
  TestIsNull("weekofyear(NULL)", TYPE_INT);
  TestIsNull("datediff(NULL, cast('2011-12-22 09:10:11.12345678' as timestamp))",
      TYPE_INT);
  TestIsNull("datediff(cast('2012-12-22' as timestamp), NULL)", TYPE_INT);
  TestIsNull("datediff(NULL, NULL)", TYPE_INT);
  TestIsNull("millisecond(NULL)", TYPE_INT);

  TestStringValue("dayname(cast('2011-12-18 09:10:11.000000' as timestamp))", "Sunday");
  TestStringValue("dayname(cast('2011-12-19 09:10:11.000000' as timestamp))", "Monday");
  TestStringValue("dayname(cast('2011-12-20 09:10:11.000000' as timestamp))", "Tuesday");
  TestStringValue("dayname(cast('2011-12-21 09:10:11.000000' as timestamp))",
      "Wednesday");
  TestStringValue("dayname(cast('2011-12-22 09:10:11.000000' as timestamp))",
      "Thursday");
  TestStringValue("dayname(cast('2011-12-23 09:10:11.000000' as timestamp))", "Friday");
  TestStringValue("dayname(cast('2011-12-24 09:10:11.000000' as timestamp))",
      "Saturday");
  TestStringValue("dayname(cast('2011-12-25 09:10:11.000000' as timestamp))", "Sunday");
  TestIsNull("dayname(NULL)", TYPE_STRING);

  // Tests from Hive
  // The hive documentation states that timestamps are timezoneless, but the tests
  // show that they treat them as being in the current timezone so these tests
  // use the utc conversion to correct for that and get the same answers as
  // are in the hive test output.
  TestValue("cast(to_utc_timestamp(cast('2011-01-01 01:01:01' as timestamp), 'PST') "
      "as boolean)", TYPE_BOOLEAN, true);
  TestValue("cast(to_utc_timestamp(cast('2011-01-01 01:01:01' as timestamp), 'PST') "
      "as tinyint)", TYPE_TINYINT, 77);
  TestValue("cast(to_utc_timestamp(cast('2011-01-01 01:01:01' as timestamp), 'PST') "
      "as smallint)", TYPE_SMALLINT, -4787);
  TestValue("cast(to_utc_timestamp(cast('2011-01-01 01:01:01' as timestamp), 'PST') "
      "as int)", TYPE_INT, 1293872461);
  TestValue("cast(to_utc_timestamp(cast('2011-01-01 01:01:01' as timestamp), 'PST') "
      "as bigint)", TYPE_BIGINT, 1293872461);
  // We have some rounding errors going backend to front, so do it as a string.
  TestStringValue("cast(cast (to_utc_timestamp(cast('2011-01-01 01:01:01' "
      "as timestamp), 'PST') as float) as string)", "1.29387251e+09");
  TestValue("cast(to_utc_timestamp(cast('2011-01-01 01:01:01' as timestamp), 'PST') "
      "as double)", TYPE_DOUBLE, 1.293872461E9);
  TestValue("cast(to_utc_timestamp(cast('2011-01-01 01:01:01.1' as timestamp), 'PST') "
      "as double)", TYPE_DOUBLE, 1.2938724611E9);
  TestValue("cast(to_utc_timestamp(cast('2011-01-01 01:01:01.0001' as timestamp), 'PST') "
      "as double)", TYPE_DOUBLE, 1.2938724610001E9);
  TestStringValue("cast(from_utc_timestamp(cast(1.3041352164485E9 as timestamp), 'PST') "
      "as string)", "2011-04-29 20:46:56.448500000");
  // NULL arguments.
  TestIsNull("from_utc_timestamp(NULL, 'PST')", TYPE_TIMESTAMP);
  TestIsNull("from_utc_timestamp(cast('2011-01-01 01:01:01.1' as timestamp), NULL)",
      TYPE_TIMESTAMP);
  TestIsNull("from_utc_timestamp(NULL, NULL)", TYPE_TIMESTAMP);

  // Tests from Hive. When casting from timestamp to numeric, timestamps are considered
  // to be local values.
  {
    ScopedLocalUnixTimestampConversionOverride use_local;
    ScopedTimeZoneOverride time_zone("PST8PDT");
    TestValue("cast(cast('2011-01-01 01:01:01' as timestamp) as boolean)", TYPE_BOOLEAN,
        true);
    TestValue("cast(cast('2011-01-01 01:01:01' as timestamp) as tinyint)", TYPE_TINYINT,
        77);
    TestValue("cast(cast('2011-01-01 01:01:01' as timestamp) as smallint)", TYPE_SMALLINT,
        -4787);
    TestValue("cast(cast('2011-01-01 01:01:01' as timestamp) as int)", TYPE_INT,
        1293872461);
    TestValue("cast(cast('2011-01-01 01:01:01' as timestamp) as bigint)", TYPE_BIGINT,
        1293872461);
    // We have some rounding errors going backend to front, so do it as a string.
    TestStringValue("cast(cast(cast('2011-01-01 01:01:01' as timestamp) as float)"
        " as string)", "1.29387251e+09");
    TestValue("cast(cast('2011-01-01 01:01:01' as timestamp) as double)", TYPE_DOUBLE,
        1.293872461E9);
    TestValue("cast(cast('2011-01-01 01:01:01.1' as timestamp) as double)", TYPE_DOUBLE,
        1.2938724611E9);
    TestValue("cast(cast('2011-01-01 01:01:01.0001' as timestamp) as double)",
        TYPE_DOUBLE, 1.2938724610001E9);
    TestStringValue("cast(cast(1.3041352164485E9 as timestamp) as string)",
        "2011-04-29 20:46:56.448500000");

    // NULL arguments.
    TestIsNull("from_utc_timestamp(NULL, 'PST')", TYPE_TIMESTAMP);
    TestIsNull("from_utc_timestamp(cast('2011-01-01 01:01:01.1' as timestamp), NULL)",
        TYPE_TIMESTAMP);
    TestIsNull("from_utc_timestamp(NULL, NULL)", TYPE_TIMESTAMP);
  }

  // Hive silently ignores bad timezones.  We log a problem.
  TestStringValue("cast(from_utc_timestamp("
                  "cast('1970-01-01 00:00:00' as timestamp), 'FOOBAR') as string)",
      "1970-01-01 00:00:00");

  // These return NULL because timezone conversion makes the value out
  // of range.
  TestIsNull("to_utc_timestamp(CAST(\"1400-01-01 05:00:00\" as TIMESTAMP), \"AEST\")",
      TYPE_TIMESTAMP);
  TestIsNull("from_utc_timestamp(CAST(\"1400-01-01 05:00:00\" as TIMESTAMP), \"PST\")",
      TYPE_TIMESTAMP);
  TestIsNull("from_utc_timestamp(CAST(\"9999-12-31 21:00:00\" as TIMESTAMP), \"JST\")",
      TYPE_TIMESTAMP);
  TestIsNull("to_utc_timestamp(CAST(\"9999-12-31 21:00:00\" as TIMESTAMP), \"PST\")",
      TYPE_TIMESTAMP);

  // With support of date strings this generates a date and 0 time.
  TestStringValue(
      "cast(cast('1999-01-10' as timestamp) as string)", "1999-01-10 00:00:00");

  // Test functions with unknown expected value.
  TestValidTimestampValue("now()");
  TestValidTimestampValue("utc_timestamp()");
  TestValidTimestampValue("current_timestamp()");
  TestValidTimestampValue("cast(unix_timestamp() as timestamp)");

  // Test that the epoch is reasonable. The default behavior of UNIX_TIMESTAMP()
  // is incorrect but wasn't changed for compatibility reasons. The function returns
  // a value as though the current timezone is UTC. Or in other words, 1970-01-01
  // in the current timezone is the effective epoch. A flag was introduced to enable
  // the correct behavior. The first test below checks the default/incorrect behavior.
  time_t unix_start_time =
      (posix_time::microsec_clock::local_time() - from_time_t(0)).total_seconds();
  int64_t unix_timestamp_result = ConvertValue<int64_t>(GetValue("unix_timestamp()",
      TYPE_BIGINT));
  EXPECT_BETWEEN(unix_start_time, unix_timestamp_result, static_cast<int64_t>(
      (posix_time::microsec_clock::local_time() - from_time_t(0)).total_seconds()));

  // Check again with the flag enabled.
  {
    ScopedLocalUnixTimestampConversionOverride use_local;
    tm before = to_tm(posix_time::microsec_clock::local_time());
    unix_start_time = mktime(&before);
    unix_timestamp_result = ConvertValue<int64_t>(GetValue("unix_timestamp()",
        TYPE_BIGINT));
    tm after = to_tm(posix_time::microsec_clock::local_time());
    EXPECT_BETWEEN(unix_start_time, unix_timestamp_result,
        static_cast<int64_t>(mktime(&after)));
  }

  // Test that the other current time functions are also reasonable.
  TimestampValue timestamp_result;
  TimestampValue start_time = TimestampValue::FromUnixTimeMicros(UnixMicros());
  timestamp_result = ConvertValue<TimestampValue>(GetValue("now()", TYPE_TIMESTAMP));
  EXPECT_BETWEEN(start_time, timestamp_result,
      TimestampValue::FromUnixTimeMicros(UnixMicros()));
  timestamp_result = ConvertValue<TimestampValue>(GetValue("current_timestamp()",
      TYPE_TIMESTAMP));
  EXPECT_BETWEEN(start_time, timestamp_result,
      TimestampValue::FromUnixTimeMicros(UnixMicros()));
  const TimestampValue utc_start_time =
      TimestampValue::UtcFromUnixTimeMicros(UnixMicros());
  timestamp_result = ConvertValue<TimestampValue>(GetValue("utc_timestamp()",
      TYPE_TIMESTAMP));
  EXPECT_BETWEEN(utc_start_time, timestamp_result,
      TimestampValue::UtcFromUnixTimeMicros(UnixMicros()));
  // UNIX_TIMESTAMP() has second precision so the comparison start time is shifted back
  // a second to ensure an earlier value.
  unix_start_time =
      (posix_time::microsec_clock::local_time() - from_time_t(0)).total_seconds();
  timestamp_result = ConvertValue<TimestampValue>(GetValue(
      "cast(unix_timestamp() as timestamp)", TYPE_TIMESTAMP));
  EXPECT_BETWEEN(TimestampValue::FromUnixTime(unix_start_time - 1), timestamp_result,
      TimestampValue::FromUnixTimeMicros(UnixMicros()));

  // Test that UTC and local time represent the same point in time
  {
    const string stmt = "select now(), utc_timestamp()";
    vector<FieldSchema> result_types;
    Status status = executor_->Exec(stmt, &result_types);
    EXPECT_TRUE(status.ok()) << "stmt: " << stmt << "\nerror: " << status.GetDetail();
    DCHECK(result_types.size() == 2);
    EXPECT_EQ(TypeToOdbcString(TYPE_TIMESTAMP), result_types[0].type)
        << "invalid type returned by now()";
    EXPECT_EQ(TypeToOdbcString(TYPE_TIMESTAMP), result_types[1].type)
        << "invalid type returned by utc_timestamp()";
    string result_row;
    status = executor_->FetchResult(&result_row);
    EXPECT_TRUE(status.ok()) << "stmt: " << stmt << "\nerror: " << status.GetDetail();
    vector<string> result_cols;
    boost::split(result_cols, result_row, boost::is_any_of("\t"));
    // To ensure this fails if columns are not tab separated
    DCHECK(result_cols.size() == 2);
    const TimestampValue local_time = ConvertValue<TimestampValue>(result_cols[0]);
    const TimestampValue utc_timestamp = ConvertValue<TimestampValue>(result_cols[1]);
    TimestampValue utc_converted_to_local(utc_timestamp);
    utc_converted_to_local.UtcToLocal();
    EXPECT_EQ(utc_converted_to_local, local_time);
  }

  // Test alias
  TestValue("now() = current_timestamp()", TYPE_BOOLEAN, true);

  // Test custom formats
  TestValue("unix_timestamp('1970|01|01 00|00|00', 'yyyy|MM|dd HH|mm|ss')", TYPE_BIGINT,
      0);
  TestValue("unix_timestamp('01,Jan,1970,00,00,00', 'dd,MMM,yyyy,HH,mm,ss')", TYPE_BIGINT,
      0);
  // This time format is misleading because a trailing Z means UTC but a timestamp can
  // have no time zone association. unix_timestamp() expects inputs to be in local time.
  TestValue<int64_t>("unix_timestamp('1983-08-05T05:00:00.000Z', "
      "'yyyy-MM-ddTHH:mm:ss.SSSZ')", TYPE_BIGINT, 428907600);

  TestStringValue("from_unixtime(0, 'yyyy|MM|dd HH|mm|ss')", "1970|01|01 00|00|00");
  TestStringValue("from_unixtime(0, 'dd,MMM,yyyy,HH,mm,ss')", "01,Jan,1970,00,00,00");

  // Test invalid formats returns error
  TestError("unix_timestamp('1970-01-01 00:00:00', 'yyyy-MM-dd hh:mm:ss')");
  TestError("unix_timestamp('1970-01-01 10:10:10', NULL)");
  TestError("unix_timestamp(NULL, NULL)");
  TestError("from_unixtime(0, NULL)");
  TestError("from_unixtime(cast(0 as bigint), NULL)");
  TestError("from_unixtime(NULL, NULL)");
  TestError("to_timestamp('1970-01-01 00:00:00', NULL)");
  TestError("to_timestamp(NULL, NULL)");
  TestError("from_timestamp(cast('1970-01-01 00:00:00' as timestamp), NULL)");
  TestError("from_timestamp(NULL, NULL)");
  TestError("unix_timestamp('1970-01-01 00:00:00', ' ')");
  TestError("unix_timestamp('1970-01-01 00:00:00', ' -===-')");
  TestError("unix_timestamp('1970-01-01', '\"foo\"')");
  TestError("from_unixtime(0, 'YY-MM-dd HH:mm:dd')");
  TestError("from_unixtime(0, 'yyyy-MM-dd hh::dd')");
  TestError("from_unixtime(cast(0 as bigint), 'YY-MM-dd HH:mm:dd')");
  TestError("from_unixtime(cast(0 as bigint), 'yyyy-MM-dd hh::dd')");
  TestError("from_unixtime(0, '')");
  TestError("from_unixtime(0, NULL)");
  TestError("from_unixtime(0, ' ')");
  TestError("from_unixtime(0, ' -=++=- ')");
  TestError("to_timestamp('1970-01-01 00:00:00', ' ')");
  TestError("to_timestamp('1970-01-01 00:00:00', ' -===-')");
  TestError("to_timestamp('1970-01-01', '\"foo\"')");
  TestError("from_timestamp(cast('1970-01-01 00:00:00' as timestamp), ' ')");
  TestError("from_timestamp(cast('1970-01-01 00:00:00' as timestamp), ' -===-')");
  TestError("from_timestamp(cast('1970-01-01' as timestamp), '\"foo\"')");
  TestError("to_timestamp('1970-01-01', 'YY-MM-dd HH:mm:dd')");
  TestError("to_timestamp('1970-01-01', 'yyyy-MM-dd hh::dd')");
  TestError("to_timestamp('1970-01-01', 'YY-MM-dd HH:mm:dd')");
  TestError("to_timestamp('1970-01-01', 'yyyy-MM-dd hh::dd')");
  TestError("to_timestamp('1970-01-01', '')");
  TestError("to_timestamp('1970-01-01', NULL)");
  TestError("to_timestamp('1970-01-01', ' ')");
  TestError("to_timestamp('1970-01-01', ' -=++=- ')");
  TestError("from_timestamp(cast('1970-01-01' as timestamp), 'YY-MM-dd HH:mm:dd')");
  TestError("from_timestamp(cast('1970-01-01' as timestamp), 'yyyy-MM-dd hh::dd')");
  TestError("from_timestamp(cast('1970-01-01' as timestamp), 'YY-MM-dd HH:mm:dd')");
  TestError("from_timestamp(cast('1970-01-01' as timestamp), 'yyyy-MM-dd hh::dd')");
  TestError("from_timestamp(cast('1970-01-01' as timestamp), '')");
  TestError("from_timestamp(cast('1970-01-01' as timestamp), NULL)");
  TestError("from_timestamp(cast('1970-01-01' as timestamp), ' ')");
  TestError("from_timestamp(cast('1970-01-01' as timestamp), ' -=++=- ')");

  // Valid format string, but invalid Timestamp, should return null;
  TestIsNull("unix_timestamp('1970-01-01', 'yyyy-MM-dd HH:mm:ss')", TYPE_BIGINT);
  TestIsNull("unix_timestamp('1970', 'yyyy-MM-dd')", TYPE_BIGINT);
  TestIsNull("unix_timestamp('', 'yyyy-MM-dd')", TYPE_BIGINT);
  TestIsNull("unix_timestamp('|1|1 00|00|00', 'yyyy|M|d HH|MM|ss')", TYPE_BIGINT);
  TestIsNull("to_timestamp('1970-01-01', 'yyyy-MM-dd HH:mm:ss')", TYPE_TIMESTAMP);
  TestIsNull("to_timestamp('1970', 'yyyy-MM-dd')", TYPE_TIMESTAMP);
  TestIsNull("to_timestamp('', 'yyyy-MM-dd')", TYPE_TIMESTAMP);
  TestIsNull("to_timestamp('|1|1 00|00|00', 'yyyy|M|d HH|MM|ss')", TYPE_TIMESTAMP);
  TestIsNull("from_timestamp(cast('1970' as timestamp), 'yyyy-MM-dd')", TYPE_STRING);
  TestIsNull("from_timestamp(cast('' as timestamp), 'yyyy-MM-dd')", TYPE_STRING);
  TestIsNull("from_timestamp(cast('|1|1 00|00|00' as timestamp), 'yyyy|M|d HH|MM|ss')", TYPE_STRING);

  TestIsNull("unix_timestamp('1970-01', 'yyyy-MM-dd')", TYPE_BIGINT);
  TestIsNull("unix_timestamp('1970-20-01', 'yyyy-MM-dd')", TYPE_BIGINT);

  // regression test for IMPALA-1105
  TestIsNull("cast(trunc('2014-07-22 01:34:55 +0100', 'year') as STRING)", TYPE_STRING);
  TestStringValue("cast(trunc(cast('2014-04-01' as timestamp), 'SYYYY') as string)",
          "2014-01-01 00:00:00");
  TestIsNull("cast(trunc('01:34:55', 'year') as STRING)", TYPE_STRING);
  TestStringValue("cast(trunc(cast('07:02:03' as timestamp), 'MI') as string)",
          "07:02:00");
  // note: no time value in string defaults to 00:00:00
  TestStringValue("cast(trunc(cast('2014-01-01' as timestamp), 'MI') as string)",
          "2014-01-01 00:00:00");

  TestStringValue(
        "cast(trunc(cast('2014-04-01 01:01:01' as timestamp), 'SYYYY') as string)",
          "2014-01-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2014-04-01 01:01:01' as timestamp), 'YYYY') as string)",
          "2014-01-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2014-04-01 01:01:01' as timestamp), 'YEAR') as string)",
          "2014-01-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2014-04-01 01:01:01' as timestamp), 'SYEAR') as string)",
          "2014-01-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2014-04-01 01:01:01' as timestamp), 'YYY') as string)",
          "2014-01-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2014-04-01 01:01:01' as timestamp), 'YY') as string)",
          "2014-01-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2014-04-01 01:01:01' as timestamp), 'Y') as string)",
          "2014-01-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2000-01-01 00:00:00' as timestamp), 'Y') as string)",
          "2000-01-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2000-01-01 00:00:00' as timestamp), 'Q') as string)",
          "2000-01-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2000-01-01 01:00:00' as timestamp), 'Q') as string)",
          "2000-01-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2000-02-01 01:00:00' as timestamp), 'Q') as string)",
          "2000-01-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2000-03-01 01:00:00' as timestamp), 'Q') as string)",
          "2000-01-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2000-04-01 01:00:00' as timestamp), 'Q') as string)",
          "2000-04-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2000-05-01 01:00:00' as timestamp), 'Q') as string)",
          "2000-04-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2000-06-01 01:00:00' as timestamp), 'Q') as string)",
          "2000-04-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2000-07-01 01:00:00' as timestamp), 'Q') as string)",
          "2000-07-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2000-08-01 01:00:00' as timestamp), 'Q') as string)",
          "2000-07-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2000-09-01 01:00:00' as timestamp), 'Q') as string)",
          "2000-07-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2000-10-01 01:00:00' as timestamp), 'Q') as string)",
          "2000-10-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2000-11-01 01:00:00' as timestamp), 'Q') as string)",
          "2000-10-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2000-12-01 01:00:00' as timestamp), 'Q') as string)",
          "2000-10-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2001-02-05 01:01:01' as timestamp), 'MONTH') as string)",
          "2001-02-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2001-02-05 01:01:01' as timestamp), 'MON') as string)",
          "2001-02-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2001-02-05 01:01:01' as timestamp), 'MM') as string)",
          "2001-02-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2001-02-05 01:01:01' as timestamp), 'RM') as string)",
          "2001-02-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2001-01-01 00:00:00' as timestamp), 'MM') as string)",
          "2001-01-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2001-12-29 00:00:00' as timestamp), 'MM') as string)",
          "2001-12-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2014-01-08 01:02:03' as timestamp), 'WW') as string)",
          "2014-01-08 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2014-01-07 00:00:00' as timestamp), 'WW') as string)",
          "2014-01-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2014-01-08 00:00:00' as timestamp), 'WW') as string)",
          "2014-01-08 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2014-01-09 00:00:00' as timestamp), 'WW') as string)",
          "2014-01-08 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2014-01-14 00:00:00' as timestamp), 'WW') as string)",
          "2014-01-08 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2014-01-08 01:02:03' as timestamp), 'W') as string)",
          "2014-01-08 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2014-01-07 00:00:00' as timestamp), 'W') as string)",
          "2014-01-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2014-01-08 00:00:00' as timestamp), 'W') as string)",
          "2014-01-08 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2014-01-09 00:00:00' as timestamp), 'W') as string)",
          "2014-01-08 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2014-01-14 00:00:00' as timestamp), 'W') as string)",
          "2014-01-08 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2014-02-01 01:02:03' as timestamp), 'W') as string)",
          "2014-02-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2014-02-02 00:00:00' as timestamp), 'W') as string)",
          "2014-02-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2014-02-03 00:00:00' as timestamp), 'W') as string)",
          "2014-02-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2014-02-07 00:00:00' as timestamp), 'W') as string)",
          "2014-02-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2014-02-08 00:00:00' as timestamp), 'W') as string)",
          "2014-02-08 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2014-02-24 00:00:00' as timestamp), 'W') as string)",
          "2014-02-22 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2014-01-08 01:02:03' as timestamp), 'DDD') as string)",
          "2014-01-08 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2014-01-08 01:02:03' as timestamp), 'DD') as string)",
          "2014-01-08 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2014-02-08 01:02:03' as timestamp), 'J') as string)",
          "2014-02-08 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2014-02-08 00:00:00' as timestamp), 'J') as string)",
          "2014-02-08 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2014-02-19 00:00:00' as timestamp), 'J') as string)",
          "2014-02-19 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2012-09-10 01:02:03' as timestamp), 'DAY') as string)",
          "2012-09-10 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2012-09-10 01:02:03' as timestamp), 'DY') as string)",
          "2012-09-10 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2012-09-10 01:02:03' as timestamp), 'D') as string)",
          "2012-09-10 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2012-09-11 01:02:03' as timestamp), 'D') as string)",
          "2012-09-10 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2012-09-12 01:02:03' as timestamp), 'D') as string)",
          "2012-09-10 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2012-09-16 01:02:03' as timestamp), 'D') as string)",
          "2012-09-10 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2012-09-10 07:02:03' as timestamp), 'HH') as string)",
          "2012-09-10 07:00:00");
  TestStringValue(
        "cast(trunc(cast('2012-09-10 07:02:03' as timestamp), 'HH12') as string)",
          "2012-09-10 07:00:00");
  TestStringValue(
        "cast(trunc(cast('2012-09-10 07:02:03' as timestamp), 'HH24') as string)",
          "2012-09-10 07:00:00");
  TestStringValue(
        "cast(trunc(cast('2012-09-10 00:02:03' as timestamp), 'HH') as string)",
          "2012-09-10 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2012-09-10 23:02:03' as timestamp), 'HH') as string)",
          "2012-09-10 23:00:00");
  TestStringValue(
        "cast(trunc(cast('2012-09-10 23:59:59' as timestamp), 'HH') as string)",
          "2012-09-10 23:00:00");
  TestStringValue(
        "cast(trunc(cast('2012-09-10 07:02:03' as timestamp), 'MI') as string)",
          "2012-09-10 07:02:00");
  TestStringValue(
        "cast(trunc(cast('2012-09-10 07:00:03' as timestamp), 'MI') as string)",
          "2012-09-10 07:00:00");
  TestStringValue(
        "cast(trunc(cast('2012-09-10 07:00:00' as timestamp), 'MI') as string)",
          "2012-09-10 07:00:00");
  TestStringValue(
        "cast(trunc(cast('2012-09-10 07:59:03' as timestamp), 'MI') as string)",
          "2012-09-10 07:59:00");
  TestStringValue(
        "cast(trunc(cast('2012-09-10 07:59:59' as timestamp), 'MI') as string)",
          "2012-09-10 07:59:00");
  TestNonOkStatus(
      "cast(trunc(cast('2012-09-10 07:59:59' as timestamp), 'MIN') as string)");
  TestNonOkStatus(
      "cast(trunc(cast('2012-09-10 07:59:59' as timestamp), 'XXYYZZ') as string)");

  // Extract as a regular function
  TestValue("extract(cast('2006-05-12 18:27:28.12345' as timestamp), 'YEAR')",
            TYPE_INT, 2006);
  TestValue("extract('2006-05-12 18:27:28.12345', 'YEAR')", TYPE_INT, 2006);
  TestValue("extract(cast('2006-05-12 18:27:28.12345' as timestamp), 'MoNTH')",
            TYPE_INT, 5);
  TestValue("extract(cast('2006-05-12 18:27:28.12345' as timestamp), 'DaY')",
            TYPE_INT, 12);
  TestValue("extract(cast('2006-05-12 06:27:28.12345' as timestamp), 'hour')",
            TYPE_INT, 6);
  TestValue("extract(cast('2006-05-12 18:27:28.12345' as timestamp), 'MINUTE')",
            TYPE_INT, 27);
  TestValue("extract(cast('2006-05-12 18:27:28.12345' as timestamp), 'SECOND')",
            TYPE_INT, 28);
  TestValue("extract(cast('2006-05-12 18:27:28.12345' as timestamp), 'MILLISECOND')",
            TYPE_INT, 123);
  TestValue("extract(cast('2006-05-13 01:27:28.12345' as timestamp), 'EPOCH')",
            TYPE_INT, 1147483648);
  TestNonOkStatus("extract(cast('2006-05-13 01:27:28.12345' as timestamp), 'foo')");
  TestNonOkStatus("extract(cast('2006-05-13 01:27:28.12345' as timestamp), NULL)");
  TestIsNull("extract(NULL, 'EPOCH')", TYPE_INT);
  TestNonOkStatus("extract(NULL, NULL)");

  // Extract using FROM keyword
  TestValue("extract(YEAR from cast('2006-05-12 18:27:28.12345' as timestamp))",
            TYPE_INT, 2006);
  TestValue("extract(MoNTH from cast('2006-05-12 18:27:28.12345' as timestamp))",
            TYPE_INT, 5);
  TestValue("extract(DaY from cast('2006-05-12 18:27:28.12345' as timestamp))",
            TYPE_INT, 12);
  TestValue("extract(hour from cast('2006-05-12 06:27:28.12345' as timestamp))",
            TYPE_INT, 6);
  TestValue("extract(MINUTE from cast('2006-05-12 18:27:28.12345' as timestamp))",
            TYPE_INT, 27);
  TestValue("extract(SECOND from cast('2006-05-12 18:27:28.12345' as timestamp))",
            TYPE_INT, 28);
  TestValue("extract(MILLISECOND from cast('2006-05-12 18:27:28.12345' as timestamp))",
            TYPE_INT, 123);
  TestValue("extract(EPOCH from cast('2006-05-13 01:27:28.12345' as timestamp))",
            TYPE_INT, 1147483648);
  TestNonOkStatus("extract(foo from cast('2006-05-13 01:27:28.12345' as timestamp))");
  TestNonOkStatus("extract(NULL from cast('2006-05-13 01:27:28.12345' as timestamp))");
  TestIsNull("extract(EPOCH from NULL)", TYPE_INT);

  // Date_part, same as extract function but with arguments swapped
  TestValue("date_part('YEAR', cast('2006-05-12 18:27:28.12345' as timestamp))",
            TYPE_INT, 2006);
  TestValue("date_part('MoNTH', cast('2006-05-12 18:27:28.12345' as timestamp))",
            TYPE_INT, 5);
  TestValue("date_part('DaY', cast('2006-05-12 18:27:28.12345' as timestamp))",
            TYPE_INT, 12);
  TestValue("date_part('hour', cast('2006-05-12 06:27:28.12345' as timestamp))",
            TYPE_INT, 6);
  TestValue("date_part('MINUTE', cast('2006-05-12 18:27:28.12345' as timestamp))",
            TYPE_INT, 27);
  TestValue("date_part('SECOND', cast('2006-05-12 18:27:28.12345' as timestamp))",
            TYPE_INT, 28);
  TestValue("date_part('MILLISECOND', cast('2006-05-12 18:27:28.12345' as timestamp))",
            TYPE_INT, 123);
  TestValue("date_part('EPOCH', cast('2006-05-13 01:27:28.12345' as timestamp))",
            TYPE_INT, 1147483648);
  TestNonOkStatus("date_part('foo', cast('2006-05-13 01:27:28.12345' as timestamp))");
  TestNonOkStatus("date_part(NULL, cast('2006-05-13 01:27:28.12345' as timestamp))");
  TestIsNull("date_part('EPOCH', NULL)", TYPE_INT);
  TestNonOkStatus("date_part(NULL, NULL)");

  // Test with timezone offset
  TestStringValue("cast(cast('2012-01-01T09:10:11Z' as timestamp) as string)",
                  "2012-01-01 09:10:11");
  TestStringValue("cast(cast('2012-01-01T09:10:11+01:30' as timestamp) as string)",
                  "2012-01-01 09:10:11");
  TestStringValue("cast(cast('2012-01-01T09:10:11-01:30' as timestamp) as string)",
                  "2012-01-01 09:10:11");
  TestStringValue("cast(cast('2012-01-01T09:10:11+0130' as timestamp) as string)",
                  "2012-01-01 09:10:11");
  TestStringValue("cast(cast('2012-01-01T09:10:11-0130' as timestamp) as string)",
                  "2012-01-01 09:10:11");
  TestStringValue("cast(cast('2012-01-01T09:10:11+01' as timestamp) as string)",
                  "2012-01-01 09:10:11");
  TestStringValue("cast(cast('2012-01-01T09:10:11-01' as timestamp) as string)",
                  "2012-01-01 09:10:11");
  TestStringValue(
      "cast(cast('2012-01-01T09:10:11.12345+01:30' as timestamp) as string)",
      "2012-01-01 09:10:11.123450000");
  TestStringValue(
      "cast(cast('2012-01-01T09:10:11.12345-01:30' as timestamp) as string)",
      "2012-01-01 09:10:11.123450000");
  TestStringValue("cast(cast('09:10:11+01:30' as timestamp) as string)", "09:10:11");
  TestStringValue("cast(cast('09:10:11-01:30' as timestamp) as string)", "09:10:11");

  TestValue("unix_timestamp('2038-01-19T03:14:08-0100')", TYPE_BIGINT, 2147483648);
  TestValue("unix_timestamp('2038/01/19T03:14:08+01:00', 'yyyy/MM/ddTHH:mm:ss')",
            TYPE_BIGINT, 2147483648);

  TestValue("unix_timestamp('2038/01/19T03:14:08+01:00', 'yyyy/MM/ddTHH:mm:ss+hh:mm')",
            TYPE_BIGINT, 2147480048);
  TestError("unix_timestamp('1990-01-01+01:00', 'yyyy-MM-dd+hh:mm')");
  TestError("unix_timestamp('1970-01-01 00:00:00+01:10', 'yyyy-MM-dd HH:mm:ss+hh:dd')");

  TestStringValue("cast(trunc('2014-07-22T01:34:55+0100', 'year') as STRING)",
                  "2014-01-01 00:00:00");
  TestStringValue("cast(trunc(cast('07:02:03+01:30' as timestamp), 'MI') as string)",
                  "07:02:00");

  // Test timezone offset format
  TestStringValue("from_unixtime(unix_timestamp('2012-01-01 19:10:11+02:30', \
      'yyyy-MM-dd HH:mm:ss+hh:mm'))", "2012-01-01 16:40:11");
  TestStringValue("from_unixtime(unix_timestamp('2012-01-01 19:10:11-0630', \
      'yyyy-MM-dd HH:mm:ss-hhmm'))", "2012-01-02 01:40:11");
  TestStringValue("from_unixtime(unix_timestamp('2012-01-01 01:10:11+02', \
      'yyyy-MM-dd HH:mm:ss+hh'))", "2011-12-31 23:10:11");
  TestStringValue("from_unixtime(unix_timestamp('2012-12-31 11:10:11-1430', \
      'yyyy-MM-dd HH:mm:ss+hhmm'))", "2013-01-01 01:40:11");
  TestStringValue("from_unixtime(unix_timestamp('2012-01-01 11:10:11+1430', \
      'yyyy-MM-dd HH:mm:ss+hhmm'))", "2011-12-31 20:40:11");
  TestStringValue("from_unixtime(unix_timestamp('2012-02-28 11:10:11-1430', \
      'yyyy-MM-dd HH:mm:ss+hhmm'))", "2012-02-29 01:40:11");
  TestStringValue("from_unixtime(unix_timestamp('1970-01-01 00:00:00+05:00', \
      'yyyy-MM-dd HH:mm:ss+hh:mm'))", "1969-12-31 19:00:00");
  TestStringValue("from_unixtime(unix_timestamp('1400-01-01 19:00:00+1500', \
      'yyyy-MM-dd HH:mm:ss+hhmm'))", "1400-01-01 04:00:00");
  TestStringValue("from_unixtime(unix_timestamp('1400-01-01 02:00:00+0200', \
      'yyyy-MM-dd HH:mm:ss+hhmm'))", "1400-01-01 00:00:00");
  TestIsNull("from_unixtime(unix_timestamp('1400-01-01 00:00:00+0100', \
      'yyyy-MM-dd HH:mm:ss+hhmm'))", TYPE_STRING);
  TestIsNull("from_unixtime(unix_timestamp('1399-12-31 23:00:00+0500', \
      'yyyy-MM-dd HH:mm:ss+hhmm'))", TYPE_STRING);
  TestStringValue("from_unixtime(unix_timestamp('9999-12-31 01:00:00-05:00', \
      'yyyy-MM-dd HH:mm:ss+hh:mm'))", "9999-12-31 06:00:00");
  TestStringValue("from_unixtime(unix_timestamp('9999-12-31 22:59:59-01:00', \
      'yyyy-MM-dd HH:mm:ss+hh:mm'))", "9999-12-31 23:59:59");
  TestIsNull("from_unixtime(unix_timestamp('9999-12-31 22:59:00-01:01', \
      'yyyy-MM-dd HH:mm:ss+hh:mm'))", TYPE_STRING);
  TestIsNull("from_unixtime(unix_timestamp('9999-12-31 23:00:00-05:00', \
      'yyyy-MM-dd HH:mm:ss+hh:mm'))", TYPE_STRING);
  TestIsNull("from_unixtime(unix_timestamp('10000-01-01 02:00:00+02:00', \
      'yyyy-MM-dd HH:mm:ss+hh:mm'))", TYPE_STRING);
  TestIsNull("from_unixtime(unix_timestamp('2012-02-28 11:10:11', \
      'yyyy-MM-dd HH:mm:ss+hhmm'))", TYPE_STRING);
  TestIsNull("from_unixtime(unix_timestamp('2012-02-28 11:10:11-14', \
      'yyyy-MM-dd HH:mm:ss+hhmm'))", TYPE_STRING);
  TestIsNull("from_unixtime(unix_timestamp('2012-02-28 11:10:11*1430', \
      'yyyy-MM-dd HH:mm:ss+hhmm'))", TYPE_STRING);
  TestIsNull("from_unixtime(unix_timestamp('2012-02-28 11:10:11+1587', \
      'yyyy-MM-dd HH:mm:ss+hhmm'))", TYPE_STRING);
  TestIsNull("from_unixtime(unix_timestamp('2012-02-28 11:10:11+2530', \
      'yyyy-MM-dd HH:mm:ss+hhmm'))", TYPE_STRING);
  TestIsNull("from_unixtime(unix_timestamp('2012-02-28 11:10:11+1530', \
      'yyyy-MM-dd HH:mm:ss+hh:mm'))", TYPE_STRING);
  TestIsNull("from_unixtime(unix_timestamp('2012-02-28 11:10:11+2430', \
      'yyyy-MM-dd HH:mm:ss+hhmm'))", TYPE_STRING);
  TestIsNull("from_unixtime(unix_timestamp('2012-02-28 11:10:11+1560', \
      'yyyy-MM-dd HH:mm:ss+hhmm'))", TYPE_STRING);
  TestIsNull("from_unixtime(unix_timestamp('1400-01-01 00:00:00+1500', \
      'yyyy-MM-dd HH:mm:ss+hhmm'))", TYPE_STRING);
  TestStringValue("cast(to_timestamp('2012-01-01 19:10:11+02:30', \
      'yyyy-MM-dd HH:mm:ss+hh:mm') as string)", "2012-01-01 16:40:11");
  TestStringValue("cast(to_timestamp('2012-01-01 19:10:11-0630', \
      'yyyy-MM-dd HH:mm:ss-hhmm') as string)", "2012-01-02 01:40:11");
  TestStringValue("cast(to_timestamp('2012-01-01 01:10:11+02', \
      'yyyy-MM-dd HH:mm:ss+hh') as string)", "2011-12-31 23:10:11");
  TestStringValue("cast(to_timestamp('2012-12-31 11:10:11-1430', \
      'yyyy-MM-dd HH:mm:ss+hhmm') as string)", "2013-01-01 01:40:11");
  TestStringValue("cast(to_timestamp('2012-01-01 11:10:11+1430', \
      'yyyy-MM-dd HH:mm:ss+hhmm') as string)", "2011-12-31 20:40:11");
  TestStringValue("cast(to_timestamp('2012-02-28 11:10:11-1430', \
      'yyyy-MM-dd HH:mm:ss+hhmm') as string)", "2012-02-29 01:40:11");
  TestStringValue("cast(to_timestamp('1970-01-01 00:00:00+05:00', \
      'yyyy-MM-dd HH:mm:ss+hh:mm') as string)", "1969-12-31 19:00:00");
  TestStringValue("cast(to_timestamp('1400-01-01 19:00:00+1500', \
      'yyyy-MM-dd HH:mm:ss+hhmm') as string)", "1400-01-01 04:00:00");
  TestStringValue("cast(to_timestamp('1400-01-01 02:00:00+0200', \
      'yyyy-MM-dd HH:mm:ss+hhmm') as string)", "1400-01-01 00:00:00");

  TestError("from_unixtime(unix_timestamp('2012-02-28 11:10:11+0530', \
      'yyyy-MM-dd HH:mm:ss+hhdd'))");
  TestError("from_unixtime(unix_timestamp('2012-02-28+0530', 'yyyy-MM-dd+hhmm'))");
  TestError("from_unixtime(unix_timestamp('10:00:00+0530 2010-01-01', \
      'HH:mm:ss+hhmm yyyy-MM-dd'))");

  TestError("to_timestamp('2012-02-28 11:10:11+0530', 'yyyy-MM-dd HH:mm:ss+hhdd')");
  TestError("to_timestamp('2012-02-28+0530', 'yyyy-MM-dd+hhmm')");
  TestError("to_timestamp('10:00:00+0530 2010-01-01', 'HH:mm:ss+hhmm yyyy-MM-dd')");
  TestError("from_timestamp(cast('2012-02-28 11:10:11+0530' as timestamp), 'yyyy-MM-dd HH:mm:ss+hhdd')");
  TestError("from_timestamp(cast('2012-02-28+0530' as timestamp), 'yyyy-MM-dd+hhmm')");
  TestError("from_timestamp(cast('10:00:00+0530 2010-01-01' as timestamp), 'HH:mm:ss+hhmm yyyy-MM-dd')");

  // Regression test for IMPALA-2732, can't parse custom date formats with non-zero-padded
  // values
  TestValue("unix_timestamp('12/2/2015', 'MM/d/yyyy')", TYPE_BIGINT, 1449014400);
  TestIsNull("unix_timestamp('12/2/2015', 'MM/dd/yyyy')", TYPE_BIGINT);
  TestValue("unix_timestamp('12/31/2015', 'MM/d/yyyy')", TYPE_BIGINT, 1451520000);
  TestValue("unix_timestamp('12/31/2015', 'MM/dd/yyyy')", TYPE_BIGINT, 1451520000);

  // next_day udf test for IMPALA-2459
  TestNextDayFunction();

  // last_day udf test for IMPALA-5316
  TestLastDayFunction();

  // Test microsecond unix time conversion functions.
  TestValue("utc_to_unix_micros(\"1400-01-01 00:00:00\")", TYPE_BIGINT,
      -17987443200000000);
  TestValue("utc_to_unix_micros(\"1970-01-01 00:00:00\")", TYPE_BIGINT,
      0);
  TestValue("utc_to_unix_micros(\"9999-01-01 23:59:59.9999999\")", TYPE_BIGINT,
      253370851200000000);

  TestStringValue("cast(unix_micros_to_utc_timestamp(-17987443200000000) as string)",
      "1400-01-01 00:00:00");
  TestIsNull("unix_micros_to_utc_timestamp(-17987443200000001)", TYPE_TIMESTAMP);
  TestStringValue("cast(unix_micros_to_utc_timestamp(253402300799999999) as string)",
      "9999-12-31 23:59:59.999999000");
  TestIsNull("unix_micros_to_utc_timestamp(253402300800000000)", TYPE_TIMESTAMP);
}

TEST_F(ExprTest, ConditionalFunctions) {
  // If first param evaluates to true, should return second parameter,
  // false or NULL should return the third.
  TestValue("if(TRUE, FALSE, TRUE)", TYPE_BOOLEAN, false);
  TestValue("if(FALSE, FALSE, TRUE)", TYPE_BOOLEAN, true);
  TestValue("if(TRUE, 10, 20)", TYPE_TINYINT, 10);
  TestValue("if(FALSE, 10, 20)", TYPE_TINYINT, 20);
  TestValue("if(TRUE, cast(5.5 as double), cast(8.8 as double))", TYPE_DOUBLE, 5.5);
  TestValue("if(FALSE, cast(5.5 as double), cast(8.8 as double))", TYPE_DOUBLE, 8.8);
  TestStringValue("if(TRUE, 'abc', 'defgh')", "abc");
  TestStringValue("if(FALSE, 'abc', 'defgh')", "defgh");
  TimestampValue then_val = TimestampValue::FromUnixTime(1293872461);
  TimestampValue else_val = TimestampValue::FromUnixTime(929387245);
  TestTimestampValue("if(TRUE, cast('2011-01-01 09:01:01' as timestamp), "
      "cast('1999-06-14 19:07:25' as timestamp))", then_val);
  TestTimestampValue("if(FALSE, cast('2011-01-01 09:01:01' as timestamp), "
      "cast('1999-06-14 19:07:25' as timestamp))", else_val);

  // Test nvl2(), which is rewritten to if() before analysis.
  // Returns 2nd arg if 1st arg is not NULL, otherwise it returns 3rd arg.
  // Output of nvl2(x,y,z) should be identical to one of if(x is not null,y,z).
  TestValue("nvl2(now(), FALSE, TRUE)", TYPE_BOOLEAN, false);
  TestValue("nvl2(NULL, FALSE, TRUE)", TYPE_BOOLEAN, true);
  TestValue("nvl2(now(), 10, 20)", TYPE_TINYINT, 10);
  TestValue("nvl2(NULL, 10, 20)", TYPE_TINYINT, 20);
  TestValue("nvl2(TRUE, cast(5.5 as double), cast(8.8 as double))", TYPE_DOUBLE, 5.5);
  TestValue("nvl2(NULL, cast(5.5 as double), cast(8.8 as double))", TYPE_DOUBLE, 8.8);
  TestStringValue("nvl2('some string', 'abc', 'defgh')", "abc");
  TestStringValue("nvl2(NULL, 'abc', 'defgh')", "defgh");
  TimestampValue first_val = TimestampValue::FromUnixTime(1293872461);
  TimestampValue second_val = TimestampValue::FromUnixTime(929387245);
  TestTimestampValue("nvl2(FALSE, cast('2011-01-01 09:01:01' as timestamp), "
      "cast('1999-06-14 19:07:25' as timestamp))", first_val);
  TestTimestampValue("nvl2(NULL, cast('2011-01-01 09:01:01' as timestamp), "
      "cast('1999-06-14 19:07:25' as timestamp))", second_val);

  // Test nullif(). Return NULL if lhs equals rhs, lhs otherwise.
  TestIsNull("nullif(NULL, NULL)", TYPE_BOOLEAN);
  TestIsNull("nullif(TRUE, TRUE)", TYPE_BOOLEAN);
  TestValue("nullif(TRUE, FALSE)", TYPE_BOOLEAN, true);
  TestIsNull("nullif(NULL, TRUE)", TYPE_BOOLEAN);
  TestValue("nullif(TRUE, NULL)", TYPE_BOOLEAN, true);
  TestIsNull("nullif(NULL, 10)", TYPE_TINYINT);
  TestValue("nullif(10, NULL)", TYPE_TINYINT, 10);
  TestIsNull("nullif(10, 10)", TYPE_TINYINT);
  TestValue("nullif(10, 20)", TYPE_TINYINT, 10);
  TestIsNull("nullif(cast(10.10 as double), cast(10.10 as double))", TYPE_DOUBLE);
  TestValue("nullif(cast(10.10 as double), cast(20.20 as double))", TYPE_DOUBLE, 10.10);
  TestIsNull("nullif(cast(NULL as double), 10.10)", TYPE_DOUBLE);
  TestValue("nullif(cast(10.10 as double), NULL)", TYPE_DOUBLE, 10.10);
  TestIsNull("nullif('abc', 'abc')", TYPE_STRING);
  TestStringValue("nullif('abc', 'def')", "abc");
  TestIsNull("nullif(NULL, 'abc')", TYPE_STRING);
  TestStringValue("nullif('abc', NULL)", "abc");
  TestIsNull("nullif(cast('2011-01-01 09:01:01' as timestamp), "
      "cast('2011-01-01 09:01:01' as timestamp))", TYPE_TIMESTAMP);
  TimestampValue testlhs = TimestampValue::FromUnixTime(1293872461);
  TestTimestampValue("nullif(cast('2011-01-01 09:01:01' as timestamp), "
      "cast('1999-06-14 19:07:25' as timestamp))", testlhs);
  TestIsNull("nullif(NULL, "
      "cast('2011-01-01 09:01:01' as timestamp))", TYPE_TIMESTAMP);
  TestTimestampValue("nullif(cast('2011-01-01 09:01:01' as timestamp), "
      "NULL)", testlhs);

  // Test IsNull() function and its aliases on all applicable types and NULL.
  string isnull_aliases[] = {"IsNull", "IfNull", "Nvl"};
  for (int i = 0; i < 3; ++i) {
    string& f = isnull_aliases[i];
    TestValue(f + "(true, NULL)", TYPE_BOOLEAN, true);
    TestValue(f + "(1, NULL)", TYPE_TINYINT, 1);
    TestValue(f + "(cast(1 as smallint), NULL)", TYPE_SMALLINT, 1);
    TestValue(f + "(cast(1 as int), NULL)", TYPE_INT, 1);
    TestValue(f + "(cast(1 as bigint), NULL)", TYPE_BIGINT, 1);
    TestValue(f + "(cast(10.0 as float), NULL)", TYPE_FLOAT, 10.0f);
    TestValue(f + "(cast(10.0 as double), NULL)", TYPE_DOUBLE, 10.0);
    TestStringValue(f + "('abc', NULL)", "abc");
    TestTimestampValue(f + "(" + default_timestamp_str_ + ", NULL)",
        default_timestamp_val_);
    // Test first argument is NULL.
    TestValue(f + "(NULL, true)", TYPE_BOOLEAN, true);
    TestValue(f + "(NULL, 1)", TYPE_TINYINT, 1);
    TestValue(f + "(NULL, cast(1 as smallint))", TYPE_SMALLINT, 1);
    TestValue(f + "(NULL, cast(1 as int))", TYPE_INT, 1);
    TestValue(f + "(NULL, cast(1 as bigint))", TYPE_BIGINT, 1);
    TestValue(f + "(NULL, cast(10.0 as float))", TYPE_FLOAT, 10.0f);
    TestValue(f + "(NULL, cast(10.0 as double))", TYPE_DOUBLE, 10.0);
    TestStringValue(f + "(NULL, 'abc')", "abc");
    TestTimestampValue(f + "(NULL, " + default_timestamp_str_ + ")",
        default_timestamp_val_);
    // Test NULL. The return type is boolean to avoid a special NULL function signature.
    TestIsNull(f + "(NULL, NULL)", TYPE_BOOLEAN);
  }

  TestIsNull("coalesce(NULL)", TYPE_BOOLEAN);
  TestIsNull("coalesce(NULL, NULL)", TYPE_BOOLEAN);
  TestValue("coalesce(TRUE)", TYPE_BOOLEAN, true);
  TestValue("coalesce(NULL, TRUE, NULL)", TYPE_BOOLEAN, true);
  TestValue("coalesce(FALSE, NULL, TRUE, NULL)", TYPE_BOOLEAN, false);
  TestValue("coalesce(NULL, NULL, NULL, TRUE, NULL, NULL)", TYPE_BOOLEAN, true);
  TestValue("coalesce(10)", TYPE_TINYINT, 10);
  TestValue("coalesce(NULL, 10)", TYPE_TINYINT, 10);
  TestValue("coalesce(10, NULL)", TYPE_TINYINT, 10);
  TestValue("coalesce(NULL, 10, NULL)", TYPE_TINYINT, 10);
  TestValue("coalesce(20, NULL, 10, NULL)", TYPE_TINYINT, 20);
  TestValue("coalesce(20, NULL, cast(10 as smallint), NULL)", TYPE_SMALLINT, 20);
  TestValue("coalesce(20, NULL, cast(10 as int), NULL)", TYPE_INT, 20);
  TestValue("coalesce(20, NULL, cast(10 as bigint), NULL)", TYPE_BIGINT, 20);
  TestValue("coalesce(cast(5.5 as float))", TYPE_FLOAT, 5.5f);
  TestValue("coalesce(NULL, cast(5.5 as float))", TYPE_FLOAT, 5.5f);
  TestValue("coalesce(cast(5.5 as float), NULL)", TYPE_FLOAT, 5.5f);
  TestValue("coalesce(NULL, cast(5.5 as float), NULL)", TYPE_FLOAT, 5.5f);
  TestValue("coalesce(cast(9.8 as float), NULL, cast(5.5 as float), NULL)",
      TYPE_FLOAT, 9.8f);
  TestValue("coalesce(cast(9.8 as double), NULL, cast(5.5 as double), NULL)",
      TYPE_DOUBLE, 9.8);
  TestStringValue("coalesce('abc')", "abc");
  TestStringValue("coalesce(NULL, 'abc', NULL)", "abc");
  TestStringValue("coalesce('defgh', NULL, 'abc', NULL)", "defgh");
  TestStringValue("coalesce(NULL, NULL, NULL, 'abc', NULL, NULL)", "abc");
  TimestampValue ats = TimestampValue::FromUnixTime(1293872461);
  TimestampValue bts = TimestampValue::FromUnixTime(929387245);
  TestTimestampValue("coalesce(cast('2011-01-01 09:01:01' as timestamp))", ats);
  TestTimestampValue("coalesce(NULL, cast('2011-01-01 09:01:01' as timestamp),"
      "NULL)", ats);
  TestTimestampValue("coalesce(cast('1999-06-14 19:07:25' as timestamp), NULL,"
      "cast('2011-01-01 09:01:01' as timestamp), NULL)", bts);
  TestTimestampValue("coalesce(NULL, NULL, NULL,"
      "cast('2011-01-01 09:01:01' as timestamp), NULL, NULL)", ats);

  // Test logic of case expr using int types.
  // The different types and casting are tested below.
  TestValue("case when true then 1 end", TYPE_TINYINT, 1);
  TestValue("case when false then 1 when true then 2 end", TYPE_TINYINT, 2);
  TestValue("case when false then 1 when false then 2 when true then 3 end",
      TYPE_TINYINT, 3);
  // Test else expr.
  TestValue("case when false then 1 else 10 end", TYPE_TINYINT, 10);
  TestValue("case when false then 1 when false then 2 else 10 end", TYPE_TINYINT, 10);
  TestValue("case when false then 1 when false then 2 when false then 3 else 10 end",
      TYPE_TINYINT, 10);
  TestIsNull("case when false then 1 end", TYPE_TINYINT);
  // Test with case expr.
  TestValue("case 21 when 21 then 1 end", TYPE_TINYINT, 1);
  TestValue("case 21 when 20 then 1 when 21 then 2 end", TYPE_TINYINT, 2);
  TestValue("case 21 when 20 then 1 when 19 then 2 when 21 then 3 end", TYPE_TINYINT, 3);
  // Should skip when-exprs that are NULL
  TestIsNull("case when NULL then 1 end", TYPE_TINYINT);
  TestIsNull("case when NULL then 1 else NULL end", TYPE_TINYINT);
  TestValue("case when NULL then 1 else 2 end", TYPE_TINYINT, 2);
  TestValue("case when NULL then 1 when true then 2 else 3 end", TYPE_TINYINT, 2);
  // Should return else expr, if case-expr is NULL.
  TestIsNull("case NULL when 1 then 1 end", TYPE_TINYINT);
  TestIsNull("case NULL when 1 then 1 else NULL end", TYPE_TINYINT);
  TestValue("case NULL when 1 then 1 else 2 end", TYPE_TINYINT, 2);
  TestValue("case 10 when NULL then 1 else 2 end", TYPE_TINYINT, 2);
  TestValue("case 10 when NULL then 1 when 10 then 2 else 3 end", TYPE_TINYINT, 2);
  TestValue("case 'abc' when NULL then 1 when NULL then 2 else 3 end", TYPE_TINYINT, 3);
  // Not statically known that it will return NULL.
  TestIsNull("case NULL when NULL then true end", TYPE_BOOLEAN);
  TestIsNull("case NULL when NULL then true else NULL end", TYPE_BOOLEAN);
  // Statically known that it will return NULL.
  TestIsNull("case NULL when NULL then NULL end", TYPE_BOOLEAN);
  TestIsNull("case NULL when NULL then NULL else NULL end", TYPE_BOOLEAN);

  // Test all types in case/when exprs, without casts.
  unordered_map<int, string>::iterator def_iter;
  for(def_iter = default_type_strs_.begin(); def_iter != default_type_strs_.end();
      ++def_iter) {
    TestValue("case " + def_iter->second + " when " + def_iter->second +
        " then true else true end", TYPE_BOOLEAN, true);
  }

  // Test all int types in then and else exprs.
  // Also tests implicit casting in all exprs.
  IntValMap::iterator int_iter;
  for (int_iter = min_int_values_.begin(); int_iter != min_int_values_.end();
      ++int_iter) {
    PrimitiveType t = static_cast<PrimitiveType>(int_iter->first);
    string& s = default_type_strs_[t];
    TestValue("case when true then " + s + " end", t, int_iter->second);
    TestValue("case when false then 1 else " + s + " end", t, int_iter->second);
    TestValue("case when true then 1 else " + s + " end", t, 1);
    TestValue("case 0 when " + s + " then true else false end", TYPE_BOOLEAN, false);
  }

  // Test for zeroifnull
  // zeroifnull(NULL) returns 0, zeroifnull(non-null) returns the argument
  TestValue("zeroifnull(NULL)", TYPE_TINYINT, 0);
  TestValue("zeroifnull(cast (NULL as TINYINT))", TYPE_TINYINT, 0);
  TestValue("zeroifnull(cast (5 as TINYINT))", TYPE_TINYINT, 5);
  TestValue("zeroifnull(cast (NULL as SMALLINT))", TYPE_SMALLINT, 0);
  TestValue("zeroifnull(cast (5 as SMALLINT))", TYPE_SMALLINT, 5);
  TestValue("zeroifnull(cast (NULL as INT))", TYPE_INT, 0);
  TestValue("zeroifnull(cast (5 as INT))", TYPE_INT, 5);
  TestValue("zeroifnull(cast (NULL as BIGINT))", TYPE_BIGINT, 0);
  TestValue("zeroifnull(cast (5 as BIGINT))", TYPE_BIGINT, 5);
  TestValue<float>("zeroifnull(cast (NULL as FLOAT))", TYPE_FLOAT, 0.0f);
  TestValue<float>("zeroifnull(cast (5 as FLOAT))", TYPE_FLOAT, 5.0f);
  TestValue<double>("zeroifnull(cast (NULL as DOUBLE))", TYPE_DOUBLE, 0.0);
  TestValue<double>("zeroifnull(cast (5 as DOUBLE))", TYPE_DOUBLE, 5.0);

  // Test for NullIfZero
  // Test that 0 converts to NULL and NULL remains NULL
  TestIsNull("nullifzero(cast (0 as TINYINT))", TYPE_TINYINT);
  TestIsNull("nullifzero(cast (NULL as TINYINT))", TYPE_TINYINT);
  TestIsNull("nullifzero(cast (0 as SMALLINT))", TYPE_SMALLINT);
  TestIsNull("nullifzero(cast (NULL as SMALLINT))", TYPE_SMALLINT);
  TestIsNull("nullifzero(cast (0 as INT))", TYPE_INT);
  TestIsNull("nullifzero(cast (NULL as INT))", TYPE_INT);
  TestIsNull("nullifzero(cast (0 as BIGINT))", TYPE_BIGINT);
  TestIsNull("nullifzero(cast (NULL as BIGINT))", TYPE_BIGINT);
  TestIsNull("nullifzero(cast (0 as FLOAT))", TYPE_FLOAT);
  TestIsNull("nullifzero(cast (NULL as FLOAT))", TYPE_FLOAT);
  TestIsNull("nullifzero(cast (0 as DOUBLE))", TYPE_DOUBLE);
  TestIsNull("nullifzero(cast (NULL as DOUBLE))", TYPE_DOUBLE);

  // test that non-zero args are returned unchanged.
  TestValue("nullifzero(cast (5 as TINYINT))", TYPE_TINYINT, 5);
  TestValue("nullifzero(cast (5 as SMALLINT))", TYPE_SMALLINT, 5);
  TestValue("nullifzero(cast (5 as INT))", TYPE_INT, 5);
  TestValue("nullifzero(cast (5 as BIGINT))", TYPE_BIGINT, 5);
  TestValue<float>("nullifzero(cast (5 as FLOAT))", TYPE_FLOAT, 5.0f);
  TestValue<double>("nullifzero(cast (5 as DOUBLE))", TYPE_DOUBLE, 5.0);

  // Test all float types in then and else exprs.
  // Also tests implicit casting in all exprs.
  // TODO: Something with our float literals is broken:
  // 1.1 gets recognized as a DOUBLE, but numeric_limits<float>::max()) + 1.1 as a FLOAT.
#if 0
  unordered_map<int, double>::iterator float_iter;
  for (float_iter = min_float_values_.begin(); float_iter != min_float_values_.end();
      ++float_iter) {
    PrimitiveType t = static_cast<PrimitiveType>(float_iter->first);
    string& s = default_type_strs_[t];
    TestValue("case when true then " + s + " end", t, float_iter->second);
    TestValue("case when false then 1 else " + s + " end", t, float_iter->second);
    TestValue("case when true then 1 else " + s + " end", t, 1.0);
    TestValue("case 0 when " + s + " then true else false end", TYPE_BOOLEAN, false);
  }
#endif

  // Test all other types.
  // We don't tests casts because these types don't allow casting up to them.
  TestValue("case when true then " + default_bool_str_ + " end", TYPE_BOOLEAN,
      default_bool_val_);
  TestValue("case when false then true else " + default_bool_str_ + " end", TYPE_BOOLEAN,
      default_bool_val_);
  // String type.
  TestStringValue("case when true then " + default_string_str_ + " end",
      default_string_val_);
  TestStringValue("case when false then '1' else " + default_string_str_ + " end",
      default_string_val_);
  // Timestamp type.
  TestTimestampValue("case when true then " + default_timestamp_str_ + " end",
      default_timestamp_val_);
  TestTimestampValue("case when false then cast('1999-06-14 19:07:25' as timestamp) "
      "else " + default_timestamp_str_ + " end", default_timestamp_val_);

  // Test Decode. This function is internalized as a CaseExpr so no
  // extra testing should be needed. To be safe, a sanity test will be done.
  TestValue("decode(1, 2, 3, 4)", TYPE_TINYINT, 4);
  // In Decode NULLs are equal
  TestValue("decode(NULL + 1, NULL + 2, 3)", TYPE_TINYINT, 3);
  TestValue("decode(NULL, NULL, 2)", TYPE_TINYINT, 2);
  TestIsNull("decode(1, NULL, 2)", TYPE_TINYINT);
  TestIsNull("decode(NULL, 1, 2)", TYPE_TINYINT);
}

TEST_F(ExprTest, ConditionalFunctionIsTrue) {
  TestValue("istrue(cast(false as boolean))", TYPE_BOOLEAN, false);
  TestValue("istrue(cast(true as boolean))", TYPE_BOOLEAN, true);
  TestValue("istrue(cast(NULL as boolean))", TYPE_BOOLEAN, false);
  TestValue("istrue(cast(0 as boolean))", TYPE_BOOLEAN, false);
  TestValue("istrue(cast(5 as boolean))", TYPE_BOOLEAN, true);
  TestValue("istrue(cast(-5 as boolean))", TYPE_BOOLEAN, true);
  TestValue("istrue(cast(0.0 as boolean))", TYPE_BOOLEAN, false);
  TestValue("istrue(cast(5.0 as boolean))", TYPE_BOOLEAN, true);
  TestValue("istrue(cast(-5.0 as boolean))", TYPE_BOOLEAN, true);

  TestError("istrue(0)");
  TestError("istrue(5)");
  TestError("istrue(-5)");
  TestError("istrue(0.0)");
  TestError("istrue(5.0)");
  TestError("istrue(-5.0)");
  TestError("istrue(\"\")");
  TestError("istrue(\"abc\")");
  TestError("istrue(cast('2012-01-01 09:10:11.123456789' as timestamp))");

  TestError("istrue(999999999999999999999999999999999999999)");
  TestError("istrue(-99999999999999999999999999999999999999)");
  TestError("istrue(99999999999999999999999999999999999999.9)");
  TestError("istrue(-9999999999999999999999999999999999999.9)");
}

TEST_F(ExprTest, ConditionalFunctionIsFalse) {
  TestValue("isfalse(cast(false as boolean))", TYPE_BOOLEAN, true);
  TestValue("isfalse(cast(true as boolean))", TYPE_BOOLEAN, false);
  TestValue("isfalse(cast(NULL as boolean))", TYPE_BOOLEAN, false);
  // The output of cast(0 as boolean) is false.
  TestValue("isfalse(cast(0 as boolean))", TYPE_BOOLEAN, true);
  TestValue("isfalse(cast(5 as boolean))", TYPE_BOOLEAN, false);
  TestValue("isfalse(cast(-5 as boolean))", TYPE_BOOLEAN, false);
  // The output of cast(0.0 as boolean) is false.
  TestValue("isfalse(cast(0.0 as boolean))", TYPE_BOOLEAN, true);
  TestValue("isfalse(cast(5.0 as boolean))", TYPE_BOOLEAN, false);
  TestValue("isfalse(cast(-5.0 as boolean))", TYPE_BOOLEAN, false);

  TestError("isfalse(0)");
  TestError("isfalse(5)");
  TestError("isfalse(-5)");
  TestError("isfalse(0.0)");
  TestError("isfalse(5.0)");
  TestError("isfalse(-5.0)");
  TestError("isfalse(\"\")");
  TestError("isfalse(\"abc\")");
  TestError("isfalse(cast('2012-01-01 09:10:11.123456789' as timestamp))");

  TestError("isfalse(999999999999999999999999999999999999999)");
  TestError("isfalse(-99999999999999999999999999999999999999)");
  TestError("isfalse(99999999999999999999999999999999999999.9)");
  TestError("isfalse(-9999999999999999999999999999999999999.9)");
}

TEST_F(ExprTest, ConditionalFunctionIsNotTrue) {
  TestValue("isnottrue(cast(false as boolean))", TYPE_BOOLEAN, true);
  TestValue("isnottrue(cast(true as boolean))", TYPE_BOOLEAN, false);
  TestValue("isnottrue(cast(NULL as boolean))", TYPE_BOOLEAN, true);
  // The output of cast(0 as boolean) is false.
  TestValue("isnottrue(cast(0 as boolean))", TYPE_BOOLEAN, true);
  TestValue("isnottrue(cast(5 as boolean))", TYPE_BOOLEAN, false);
  TestValue("isnottrue(cast(-5 as boolean))", TYPE_BOOLEAN, false);
  // The output of cast(0.0 as boolean) is false.
  TestValue("isnottrue(cast(0.0 as boolean))", TYPE_BOOLEAN, true);
  TestValue("isnottrue(cast(5.0 as boolean))", TYPE_BOOLEAN, false);
  TestValue("isnottrue(cast(-5.0 as boolean))", TYPE_BOOLEAN, false);

  TestError("isnottrue(0)");
  TestError("isnottrue(5)");
  TestError("isnottrue(-5)");
  TestError("isnottrue(0.0)");
  TestError("isnottrue(5.0)");
  TestError("isnottrue(-5.0)");
  TestError("isnottrue(\"\")");
  TestError("isnottrue(\"abc\")");
  TestError("isnottrue(cast('2012-01-01 09:10:11.123456789' as timestamp))");

  TestError("isnottrue(999999999999999999999999999999999999999)");
  TestError("isnottrue(-99999999999999999999999999999999999999)");
  TestError("isnottrue(99999999999999999999999999999999999999.9)");
  TestError("isnottrue(-9999999999999999999999999999999999999.9)");
}

TEST_F(ExprTest, ConditionalFunctionIsNotFalse) {
  TestValue("isnotfalse(cast(false as boolean))", TYPE_BOOLEAN, false);
  TestValue("isnotfalse(cast(true as boolean))", TYPE_BOOLEAN, true);
  TestValue("isnotfalse(cast(NULL as boolean))", TYPE_BOOLEAN, true);
  TestValue("isnotfalse(cast(0 as boolean))", TYPE_BOOLEAN, false);
  TestValue("isnotfalse(cast(5 as boolean))", TYPE_BOOLEAN, true);
  TestValue("isnotfalse(cast(-5 as boolean))", TYPE_BOOLEAN, true);
  TestValue("isnotfalse(cast(0.0 as boolean))", TYPE_BOOLEAN, false);
  TestValue("isnotfalse(cast(5.0 as boolean))", TYPE_BOOLEAN, true);
  TestValue("isnotfalse(cast(-5.0 as boolean))", TYPE_BOOLEAN, true);

  TestError("isnotfalse(0)");
  TestError("isnotfalse(5)");
  TestError("isnotfalse(-5)");
  TestError("isnotfalse(0.0)");
  TestError("isnotfalse(5.0)");
  TestError("isnotfalse(-5.0)");
  TestError("isnotfalse(\"\")");
  TestError("isnotfalse(\"abc\")");
  TestError("isnotfalse(cast('2012-01-01 09:10:11.123456789' as timestamp))");

  TestError("isnotfalse(999999999999999999999999999999999999999)");
  TestError("isnotfalse(-99999999999999999999999999999999999999)");
  TestError("isnotfalse(99999999999999999999999999999999999999.9)");
  TestError("isnotfalse(-9999999999999999999999999999999999999.9)");
}

// Validates that Expr::ComputeResultsLayout() for 'exprs' is correct.
//   - expected_byte_size: total byte size to store all results for exprs
//   - expected_var_begin: byte offset where variable length types begin
//   - expected_offsets: mapping of byte sizes to a set valid offsets
//     exprs that have the same byte size can end up in a number of locations
void ValidateLayout(const vector<ScalarExpr*>& exprs, int expected_byte_size,
    int expected_var_begin, const map<int, set<int>>& expected_offsets) {
  vector<int> offsets;
  set<int> offsets_found;

  int var_begin;
  int byte_size = ScalarExpr::ComputeResultsLayout(exprs, &offsets, &var_begin);

  EXPECT_EQ(expected_byte_size, byte_size);
  EXPECT_EQ(expected_var_begin, var_begin);

  // Walk the computed offsets and make sure the resulting sets match expected_offsets
  for (int i = 0; i < exprs.size(); ++i) {
    int expr_byte_size = exprs[i]->type().GetByteSize();
    map<int, set<int>>::const_iterator iter = expected_offsets.find(expr_byte_size);
    EXPECT_TRUE(iter != expected_offsets.end());

    const set<int>& possible_offsets = iter->second;
    int computed_offset = offsets[i];
    // The computed offset has to be one of the possible.  Exprs types with the
    // same size are not ordered wrt each other.
    EXPECT_TRUE(possible_offsets.find(computed_offset) != possible_offsets.end());
    // The offset should not have been found before
    EXPECT_TRUE(offsets_found.find(computed_offset) == offsets_found.end());
    offsets_found.insert(computed_offset);
  }
}

TEST_F(ExprTest, ResultsLayoutTest) {
  ObjectPool pool;

  vector<ScalarExpr*> exprs;
  map<int, set<int>> expected_offsets;

  // Test empty exprs
  ValidateLayout(exprs, 0, -1, expected_offsets);

  // Test single Expr case
  vector<ColumnType> types;
  types.push_back(TYPE_BOOLEAN);
  types.push_back(TYPE_TINYINT);
  types.push_back(TYPE_SMALLINT);
  types.push_back(TYPE_INT);
  types.push_back(TYPE_BIGINT);
  types.push_back(TYPE_FLOAT);
  types.push_back(TYPE_DOUBLE);
  types.push_back(TYPE_TIMESTAMP);
  types.push_back(TYPE_STRING);

  types.push_back(ColumnType::CreateDecimalType(1,0));
  types.push_back(ColumnType::CreateDecimalType(8,0));
  types.push_back(ColumnType::CreateDecimalType(18,0));

  types.push_back(ColumnType::CreateCharType(1));
  types.push_back(ColumnType::CreateVarcharType(1));
  types.push_back(ColumnType::CreateCharType(2));
  types.push_back(ColumnType::CreateVarcharType(2));
  types.push_back(ColumnType::CreateCharType(3));
  types.push_back(ColumnType::CreateVarcharType(3));
  types.push_back(ColumnType::CreateCharType(128));
  types.push_back(ColumnType::CreateVarcharType(128));
  types.push_back(ColumnType::CreateCharType(255));
  types.push_back(ColumnType::CreateVarcharType(255));

  expected_offsets.clear();
  for (const ColumnType& t: types) {
    exprs.clear();
    expected_offsets.clear();
    // With one expr, all offsets should be 0.
    expected_offsets[t.GetByteSize()] = set<int>({0});
    if (t.type != TYPE_TIMESTAMP) {
      exprs.push_back(pool.Add(CreateLiteral(t, "0")));
    } else {
      exprs.push_back(pool.Add(CreateLiteral(t, "2016-11-09")));
    }
    if (t.IsVarLenStringType()) {
      ValidateLayout(exprs, 16, 0, expected_offsets);
    } else {
      ValidateLayout(exprs, t.GetByteSize(), -1, expected_offsets);
    }
  }

  int expected_byte_size = 0;
  int expected_var_begin = 0;
  expected_offsets.clear();
  exprs.clear();

  // Test layout adding a bunch of exprs.  This is designed to trigger padding.
  // The expected result is computed along the way
  exprs.push_back(pool.Add(CreateLiteral(TYPE_BOOLEAN, "0")));
  exprs.push_back(pool.Add(CreateLiteral(TYPE_TINYINT, "0")));
  exprs.push_back(pool.Add(CreateLiteral(ColumnType::CreateCharType(1), "0")));
  expected_offsets[1].insert(expected_byte_size);
  expected_offsets[1].insert(expected_byte_size + 1);
  expected_offsets[1].insert(expected_byte_size + 2);
  expected_byte_size += 3 * 1 + 1;  // 1 byte of padding

  exprs.push_back(pool.Add(CreateLiteral(TYPE_SMALLINT, "0")));
  expected_offsets[2].insert(expected_byte_size);
  expected_byte_size += 2; // No padding before CHAR

  exprs.push_back(pool.Add(CreateLiteral(ColumnType::CreateCharType(3), "0")));
  expected_offsets[3].insert(expected_byte_size);
  expected_byte_size += 3 + 3; // 3 byte of padding
  ASSERT_EQ(expected_byte_size % 4, 0);

  exprs.push_back(pool.Add(CreateLiteral(TYPE_INT, "0")));
  exprs.push_back(pool.Add(CreateLiteral(TYPE_FLOAT, "0")));
  exprs.push_back(pool.Add(CreateLiteral(TYPE_FLOAT, "0")));
  exprs.push_back(pool.Add(
      CreateLiteral(ColumnType::CreateDecimalType(9, 0), "0")));
  expected_offsets[4].insert(expected_byte_size);
  expected_offsets[4].insert(expected_byte_size + 4);
  expected_offsets[4].insert(expected_byte_size + 8);
  expected_offsets[4].insert(expected_byte_size + 12);
  expected_byte_size += 4 * 4 + 4;  // 4 bytes of padding
  ASSERT_EQ(expected_byte_size % 8, 0);

  exprs.push_back(pool.Add(CreateLiteral(TYPE_BIGINT, "0")));
  exprs.push_back(pool.Add(CreateLiteral(TYPE_BIGINT, "0")));
  exprs.push_back(pool.Add(CreateLiteral(TYPE_BIGINT, "0")));
  exprs.push_back(pool.Add(CreateLiteral(TYPE_DOUBLE, "0")));
  exprs.push_back(pool.Add(
      CreateLiteral(ColumnType::CreateDecimalType(18, 0), "0")));
  expected_offsets[8].insert(expected_byte_size);
  expected_offsets[8].insert(expected_byte_size + 8);
  expected_offsets[8].insert(expected_byte_size + 16);
  expected_offsets[8].insert(expected_byte_size + 24);
  expected_offsets[8].insert(expected_byte_size + 32);
  expected_byte_size += 5 * 8;      // No more padding
  ASSERT_EQ(expected_byte_size % 8, 0);

  exprs.push_back(pool.Add(CreateLiteral(TYPE_TIMESTAMP, "2016-11-09")));
  exprs.push_back(pool.Add(CreateLiteral(TYPE_TIMESTAMP, "2016-11-09")));
  exprs.push_back(pool.Add(
      CreateLiteral(ColumnType::CreateDecimalType(20, 0), "0")));
  expected_offsets[16].insert(expected_byte_size);
  expected_offsets[16].insert(expected_byte_size + 16);
  expected_offsets[16].insert(expected_byte_size + 32);
  expected_byte_size += 3 * 16;
  ASSERT_EQ(expected_byte_size % 8, 0);

  exprs.push_back(pool.Add(CreateLiteral(TYPE_STRING, "0")));
  exprs.push_back(pool.Add(CreateLiteral(TYPE_STRING, "0")));
  exprs.push_back(pool.Add(
      CreateLiteral(ColumnType::CreateVarcharType(1), "0")));
  expected_offsets[0].insert(expected_byte_size);
  expected_offsets[0].insert(expected_byte_size + 16);
  expected_offsets[0].insert(expected_byte_size + 32);
  expected_var_begin = expected_byte_size;
  expected_byte_size += 3 * 16;
  ASSERT_EQ(expected_byte_size % 8, 0);

  // Validate computed layout
  ValidateLayout(exprs, expected_byte_size, expected_var_begin, expected_offsets);

  // Randomize the expr order and validate again.  This is implemented by a
  // sort when the layout is computed so it shouldn't be very sensitive to
  // a particular order.

  srand(0);   // Seed rand to get repeatable results
  for (int i = 0; i < 10; ++i) {
    std::random_shuffle(exprs.begin(), exprs.end());
    ValidateLayout(exprs, expected_byte_size, expected_var_begin, expected_offsets);
  }
}

// TODO: is there an easy way to templatize/parametrize these tests?
TEST_F(ExprTest, DecimalFunctions) {
  TestValue("precision(cast (1 as decimal(10,2)))", TYPE_INT, 10);
  TestValue("scale(cast(1 as decimal(10,2)))", TYPE_INT, 2);

  TestValue("precision(1)", TYPE_INT, 3);
  TestValue("precision(cast(1 as smallint))", TYPE_INT, 5);
  TestValue("precision(cast(123 as bigint))", TYPE_INT, 19);
  TestValue("precision(123.45)", TYPE_INT, 5);
  TestValue("scale(123.45)", TYPE_INT, 2);
  TestValue("precision(1 + 1)", TYPE_INT, 5);
  TestValue("scale(1 + 1)", TYPE_INT, 0);
  TestValue("precision(1 + 1)", TYPE_INT, 5);

  TestValue("scale(cast(NULL as decimal(10, 2)))", TYPE_INT, 2);

  // Test result scale/precision from round()/truncate()
  TestValue("scale(round(123.456, 3))", TYPE_INT, 3);
  TestValue("precision(round(cast(\"123.456\" as decimal(6, 3)), 3))", TYPE_INT, 6);

  TestValue("scale(truncate(123.456, 1))", TYPE_INT, 1);
  TestValue("precision(truncate(123.456, 1))", TYPE_INT, 4);

  TestValue("scale(round(cast(\"123.456\" as decimal(6, 3)), -2))", TYPE_INT, 0);
  TestValue("precision(round(123.456, -2))", TYPE_INT, 4);

  TestValue("scale(truncate(123.456, 10))", TYPE_INT, 10);
  TestValue("precision(truncate(cast(\"123.456\" as decimal(6, 3)), 10))", TYPE_INT, 13);

  TestValue("scale(round(123.456, -10))", TYPE_INT, 0);
  TestValue("precision(round(cast(\"123.456\" as decimal(6, 3)), -10))", TYPE_INT, 4);

  // Abs()
  TestDecimalValue("abs(cast('0' as decimal(2,0)))", Decimal4Value(0),
      ColumnType::CreateDecimalType(2, 0));
  TestDecimalValue("abs(cast('1.1' as decimal(2,1)))", Decimal4Value(11),
      ColumnType::CreateDecimalType(2,1));
  TestDecimalValue("abs(cast('-1.23' as decimal(5,2)))", Decimal4Value(123),
      ColumnType::CreateDecimalType(5,2));
  TestDecimalValue("abs(cast('0' as decimal(12,0)))", Decimal8Value(0),
      ColumnType::CreateDecimalType(12, 0));
  TestDecimalValue("abs(cast('1.1' as decimal(12,1)))", Decimal8Value(11),
      ColumnType::CreateDecimalType(12,1));
  TestDecimalValue("abs(cast('-1.23' as decimal(12,2)))", Decimal8Value(123),
      ColumnType::CreateDecimalType(12,2));
  TestDecimalValue("abs(cast('0' as decimal(32,0)))", Decimal16Value(0),
      ColumnType::CreateDecimalType(32, 0));
  TestDecimalValue("abs(cast('1.1' as decimal(32,1)))", Decimal8Value(11),
      ColumnType::CreateDecimalType(32,1));
  TestDecimalValue("abs(cast('-1.23' as decimal(32,2)))", Decimal8Value(123),
      ColumnType::CreateDecimalType(32,2));
  TestIsNull("abs(cast(NULL as decimal(2,0)))", ColumnType::CreateDecimalType(2,0));

  // Tests take a while and at this point we've cycled through each type. No reason
  // to keep testing the codegen path.
  if (!disable_codegen_) return;

  // IsNull()
  TestDecimalValue("isnull(cast('0' as decimal(2,0)), NULL)", Decimal4Value(0),
      ColumnType::CreateDecimalType(2, 0));
  TestDecimalValue("isnull(cast('1.1' as decimal(18,1)), NULL)", Decimal8Value(11),
      ColumnType::CreateDecimalType(18,1));
  TestDecimalValue("isnull(cast('-1.23' as decimal(32,2)), NULL)", Decimal8Value(-123),
      ColumnType::CreateDecimalType(32,2));
  TestDecimalValue("isnull(NULL, cast('0' as decimal(2,0)))", Decimal4Value(0),
      ColumnType::CreateDecimalType(2, 0));
  TestDecimalValue("isnull(NULL, cast('1.1' as decimal(18,1)))", Decimal8Value(11),
      ColumnType::CreateDecimalType(18,1));
  TestDecimalValue("isnull(NULL, cast('-1.23' as decimal(32,2)))", Decimal8Value(-123),
      ColumnType::CreateDecimalType(32,2));
  TestIsNull("isnull(cast(NULL as decimal(2,0)), NULL)",
      ColumnType::CreateDecimalType(2,0));

  // NullIf()
  TestDecimalValue("isnull(cast('0' as decimal(2,0)), NULL)", Decimal4Value(0),
      ColumnType::CreateDecimalType(2, 0));
  TestDecimalValue("isnull(cast('1.1' as decimal(18,1)), NULL)", Decimal8Value(11),
      ColumnType::CreateDecimalType(18,1));
  TestDecimalValue("isnull(cast('-1.23' as decimal(32,2)), NULL)", Decimal8Value(-123),
      ColumnType::CreateDecimalType(32,2));
  TestDecimalValue("isnull(NULL, cast('0' as decimal(2,0)))", Decimal4Value(0),
      ColumnType::CreateDecimalType(2, 0));
  TestDecimalValue("isnull(NULL, cast('1.1' as decimal(18,1)))", Decimal8Value(11),
      ColumnType::CreateDecimalType(18,1));
  TestDecimalValue("isnull(NULL, cast('-1.23' as decimal(32,2)))", Decimal8Value(-123),
      ColumnType::CreateDecimalType(32,2));
  TestIsNull("isnull(cast(NULL as decimal(2,0)), NULL)",
      ColumnType::CreateDecimalType(2,0));

  // NullIfZero()
  TestDecimalValue("nullifzero(cast('10' as decimal(2,0)))", Decimal4Value(10),
      ColumnType::CreateDecimalType(2, 0));
  TestDecimalValue("nullifzero(cast('1.1' as decimal(18,1)))", Decimal8Value(11),
      ColumnType::CreateDecimalType(18,1));
  TestDecimalValue("nullifzero(cast('-1.23' as decimal(32,2)))", Decimal8Value(-123),
      ColumnType::CreateDecimalType(32,2));
  TestIsNull("nullifzero(cast('0' as decimal(2,0)))",
      ColumnType::CreateDecimalType(2, 0));
  TestIsNull("nullifzero(cast('0' as decimal(18,1)))",
      ColumnType::CreateDecimalType(18,1));
  TestIsNull("nullifzero(cast('0' as decimal(32,2)))",
      ColumnType::CreateDecimalType(32,2));

  // IfFn()
  TestDecimalValue("if(TRUE, cast('0' as decimal(2,0)), NULL)", Decimal4Value(0),
      ColumnType::CreateDecimalType(2, 0));
  TestDecimalValue("if(TRUE, cast('1.1' as decimal(18,1)), NULL)", Decimal8Value(11),
      ColumnType::CreateDecimalType(18,1));
  TestDecimalValue("if(TRUE, cast('-1.23' as decimal(32,2)), NULL)", Decimal8Value(-123),
      ColumnType::CreateDecimalType(32,2));
  TestDecimalValue("if(FALSE, NULL, cast('0' as decimal(2,0)))", Decimal4Value(0),
      ColumnType::CreateDecimalType(2, 0));
  TestDecimalValue("if(FALSE, NULL, cast('1.1' as decimal(18,1)))", Decimal8Value(11),
      ColumnType::CreateDecimalType(18,1));
  TestDecimalValue("if(FALSE, NULL, cast('-1.23' as decimal(32,2)))", Decimal8Value(-123),
      ColumnType::CreateDecimalType(32,2));
  TestIsNull("if(TRUE, cast(NULL as decimal(32,2)), NULL)",
      ColumnType::CreateDecimalType(32,2));
  TestIsNull("if(FALSE, cast('-1.23' as decimal(32,2)), NULL)",
      ColumnType::CreateDecimalType(32,2));

  // ZeroIfNull()
  TestDecimalValue("zeroifnull(cast('10' as decimal(2,0)))", Decimal4Value(10),
      ColumnType::CreateDecimalType(2, 0));
  TestDecimalValue("zeroifnull(cast('1.1' as decimal(18,1)))", Decimal8Value(11),
      ColumnType::CreateDecimalType(18,1));
  TestDecimalValue("zeroifnull(cast('-1.23' as decimal(32,2)))", Decimal8Value(-123),
      ColumnType::CreateDecimalType(32,2));
  TestDecimalValue("zeroifnull(cast(NULL as decimal(2,0)))", Decimal4Value(0),
      ColumnType::CreateDecimalType(2, 0));
  TestDecimalValue("zeroifnull(cast(NULL as decimal(18,1)))", Decimal8Value(0),
      ColumnType::CreateDecimalType(18,1));
  TestDecimalValue("zeroifnull(cast(NULL as decimal(32,2)))", Decimal16Value(0),
      ColumnType::CreateDecimalType(32,2));

  // Coalesce()
  TestDecimalValue("coalesce(NULL, cast('0' as decimal(2,0)))", Decimal4Value(0),
      ColumnType::CreateDecimalType(2, 0));
  TestDecimalValue("coalesce(NULL, cast('0' as decimal(18,0)))", Decimal8Value(0),
      ColumnType::CreateDecimalType(18, 0));
  TestDecimalValue("coalesce(NULL, cast('1.1' as decimal(18,1)))", Decimal8Value(11),
      ColumnType::CreateDecimalType(18,1));
  TestDecimalValue("coalesce(NULL, cast('-1.23' as decimal(18,2)))", Decimal8Value(-123),
      ColumnType::CreateDecimalType(18,2));
  TestDecimalValue("coalesce(NULL, cast('0' as decimal(32,0)))", Decimal16Value(0),
      ColumnType::CreateDecimalType(32, 0));
  TestDecimalValue("coalesce(NULL, cast('1.1' as decimal(32,1)))", Decimal8Value(11),
      ColumnType::CreateDecimalType(32,1));
  TestDecimalValue("coalesce(NULL, cast('-1.23' as decimal(32,2)))", Decimal8Value(-123),
      ColumnType::CreateDecimalType(32,2));
  TestIsNull("coalesce(cast(NULL as decimal(2,0)), NULL)",
      ColumnType::CreateDecimalType(2,0));

  // Case
  TestDecimalValue("CASE when true then cast('10' as decimal(2,0)) end",
      Decimal4Value(10), ColumnType::CreateDecimalType(2, 0));
  TestDecimalValue("CASE when true then cast('1.1' as decimal(18,1)) end",
      Decimal8Value(11), ColumnType::CreateDecimalType(18,1));
  TestDecimalValue("CASE when true then cast('-1.23' as decimal(32,2)) end",
      Decimal8Value(-123), ColumnType::CreateDecimalType(32,2));
  TestDecimalValue("CASE when false then NULL else cast('10' as decimal(2,0)) end",
      Decimal4Value(10), ColumnType::CreateDecimalType(2, 0));
  TestDecimalValue("CASE when false then NULL else cast('1.1' as decimal(18,1)) end",
      Decimal8Value(11), ColumnType::CreateDecimalType(18,1));
  TestDecimalValue("CASE when false then NULL else cast('-1.23' as decimal(32,2)) end",
      Decimal8Value(-123), ColumnType::CreateDecimalType(32,2));

  TestValue("CASE 1.1 when 1.1 then 1 when 2.22 then 2 else 3 end", TYPE_TINYINT, 1);
  TestValue("CASE 2.22 when 1.1 then 1 when 2.22 then 2 else 3 end", TYPE_TINYINT, 2);
  TestValue("CASE 2.21 when 1.1 then 1 when 2.22 then 2 else 3 end", TYPE_TINYINT, 3);
  TestValue("CASE NULL when 1.1 then 1 when 2.22 then 2 else 3 end", TYPE_TINYINT, 3);

  TestDecimalValue("CASE 2.21 when 1.1 then 1.1 when 2.21 then 2.2 else 3.3 end",
      Decimal4Value(22), ColumnType::CreateDecimalType(2, 1));

  // Positive()
  TestDecimalValue("positive(cast('10' as decimal(2,0)))",
      Decimal4Value(10), ColumnType::CreateDecimalType(2, 0));
  TestDecimalValue("positive(cast('1.1' as decimal(18,1)))",
      Decimal8Value(11), ColumnType::CreateDecimalType(18,1));
  TestDecimalValue("positive(cast('-1.23' as decimal(32,2)))",
      Decimal8Value(-123), ColumnType::CreateDecimalType(32,2));
  TestIsNull("positive(cast(NULL as decimal(32,2)))",
      ColumnType::CreateDecimalType(32,2));

  // Negative()
  TestDecimalValue("negative(cast('10' as decimal(2,0)))",
      Decimal4Value(-10), ColumnType::CreateDecimalType(2, 0));
  TestDecimalValue("negative(cast('1.1' as decimal(18,1)))",
      Decimal8Value(-11), ColumnType::CreateDecimalType(18,1));
  TestDecimalValue("negative(cast('-1.23' as decimal(32,2)))",
      Decimal8Value(123), ColumnType::CreateDecimalType(32,2));
  TestIsNull("negative(cast(NULL as decimal(32,2)))",
      ColumnType::CreateDecimalType(32,2));

  // TODO: Disabled due to IMPALA-1111.
  // Least()
  TestDecimalValue("least(cast('10' as decimal(2,0)), cast('-10' as decimal(2,0)))",
      Decimal4Value(-10), ColumnType::CreateDecimalType(2, 0));
  TestDecimalValue("least(cast('1.1' as decimal(18,1)), cast('-1.1' as decimal(18,1)))",
      Decimal8Value(-11), ColumnType::CreateDecimalType(18,1));
  TestDecimalValue("least(cast('-1.23' as decimal(32,2)), cast('1.23' as decimal(32,2)))",
      Decimal8Value(-123), ColumnType::CreateDecimalType(32,2));
  TestIsNull("least(cast(NULL as decimal(32,2)), cast('1.23' as decimal(32,2)))",
      ColumnType::CreateDecimalType(32,2));
  TestIsNull("least(cast('1.23' as decimal(32,2)), NULL)",
      ColumnType::CreateDecimalType(32,2));
  TestIsNull("least(cast(NULl as decimal(32,2)), NULL)",
      ColumnType::CreateDecimalType(32,2));

  // Greatest()
  TestDecimalValue("greatest(cast('10' as decimal(2,0)), cast('-10' as decimal(2,0)))",
      Decimal4Value(10), ColumnType::CreateDecimalType(2, 0));
  TestDecimalValue(
      "greatest(cast('1.1' as decimal(18,1)), cast('-1.1' as decimal(18,1)))",
      Decimal8Value(11), ColumnType::CreateDecimalType(18,1));
  TestDecimalValue(
      "greatest(cast('-1.23' as decimal(32,2)), cast('1.23' as decimal(32,2)))",
      Decimal8Value(123), ColumnType::CreateDecimalType(32,2));
  TestIsNull("greatest(cast(NULL as decimal(32,2)), cast('1.23' as decimal(32,2)))",
      ColumnType::CreateDecimalType(32,2));
  TestIsNull("greatest(cast('1.23' as decimal(32,2)), NULL)",
      ColumnType::CreateDecimalType(32,2));
  TestIsNull("greatest(cast(NULl as decimal(32,2)), NULL)",
      ColumnType::CreateDecimalType(32,2));

  Decimal4Value dec4(10);
  // DecimalFunctions::FnvHash hashes both the unscaled value and scale
  uint64_t expected = HashUtil::FnvHash64(&dec4, 4, HashUtil::FNV_SEED);
  TestValue("fnv_hash(cast('10' as decimal(2,0)))", TYPE_BIGINT, expected);

  Decimal8Value dec8 = Decimal8Value(11);
  expected = HashUtil::FnvHash64(&dec8, 8, HashUtil::FNV_SEED);
  TestValue("fnv_hash(cast('1.1' as decimal(18,1)))", TYPE_BIGINT, expected);

  Decimal16Value dec16 = Decimal16Value(-123);
  expected = HashUtil::FnvHash64(&dec16, 16, HashUtil::FNV_SEED);
  TestValue("fnv_hash(cast('-1.23' as decimal(32,2)))", TYPE_BIGINT, expected);


  // In these tests we iterate through the underlying decimal types and alternate
  // between positive and negative values.

  // Ceil()
  TestDecimalValue("ceil(cast('0' as decimal(6,5)))", Decimal4Value(0),
      ColumnType::CreateDecimalType(2, 0));
  TestDecimalValue("ceil(cast('3.14159' as decimal(6,5)))", Decimal4Value(4),
      ColumnType::CreateDecimalType(2, 0));
  TestDecimalValue("ceil(cast('-3.14159' as decimal(6,5)))", Decimal4Value(-3),
      ColumnType::CreateDecimalType(2, 0));
  TestDecimalValue("ceil(cast('3' as decimal(6,5)))", Decimal4Value(3),
      ColumnType::CreateDecimalType(2, 0));
  TestDecimalValue("ceil(cast('3.14159' as decimal(13,5)))", Decimal8Value(4),
      ColumnType::CreateDecimalType(9, 0));
  TestDecimalValue("ceil(cast('-3.14159' as decimal(13,5)))", Decimal8Value(-3),
      ColumnType::CreateDecimalType(9, 0));
  TestDecimalValue("ceil(cast('3' as decimal(13,5)))", Decimal8Value(3),
      ColumnType::CreateDecimalType(9, 0));
  TestDecimalValue("ceil(cast('3.14159' as decimal(33,5)))", Decimal16Value(4),
      ColumnType::CreateDecimalType(29, 0));
  TestDecimalValue("ceil(cast('-3.14159' as decimal(33,5)))", Decimal16Value(-3),
      ColumnType::CreateDecimalType(29, 0));
  TestDecimalValue("ceil(cast('3' as decimal(33,5)))", Decimal16Value(3),
      ColumnType::CreateDecimalType(29, 0));
  TestDecimalValue("ceil(cast('9.14159' as decimal(6,5)))", Decimal4Value(10),
      ColumnType::CreateDecimalType(2, 0));
  TestIsNull("ceil(cast(NULL as decimal(2,0)))", ColumnType::CreateDecimalType(2,0));

  // Floor()
  TestDecimalValue("floor(cast('3.14159' as decimal(6,5)))", Decimal4Value(3),
      ColumnType::CreateDecimalType(2, 0));
  TestDecimalValue("floor(cast('-3.14159' as decimal(6,5)))", Decimal4Value(-4),
      ColumnType::CreateDecimalType(2, 0));
  TestDecimalValue("floor(cast('3' as decimal(6,5)))", Decimal4Value(3),
      ColumnType::CreateDecimalType(2, 0));
  TestDecimalValue("floor(cast('3.14159' as decimal(13,5)))", Decimal8Value(3),
      ColumnType::CreateDecimalType(9, 0));
  TestDecimalValue("floor(cast('-3.14159' as decimal(13,5)))", Decimal8Value(-4),
      ColumnType::CreateDecimalType(9, 0));
  TestDecimalValue("floor(cast('3' as decimal(13,5)))", Decimal8Value(3),
      ColumnType::CreateDecimalType(9, 0));
  TestDecimalValue("floor(cast('3.14159' as decimal(33,5)))", Decimal16Value(3),
      ColumnType::CreateDecimalType(29, 0));
  TestDecimalValue("floor(cast('-3.14159' as decimal(33,5)))", Decimal16Value(-4),
      ColumnType::CreateDecimalType(29, 0));
  TestDecimalValue("floor(cast('3' as decimal(33,5)))", Decimal16Value(3),
      ColumnType::CreateDecimalType(29, 0));
  TestDecimalValue("floor(cast('-9.14159' as decimal(6,5)))", Decimal4Value(-10),
      ColumnType::CreateDecimalType(2, 0));
  TestIsNull("floor(cast(NULL as decimal(2,0)))", ColumnType::CreateDecimalType(2,0));

  // Dfloor() alias
  TestDecimalValue("dfloor(cast('3.14159' as decimal(6,5)))", Decimal4Value(3),
      ColumnType::CreateDecimalType(2, 0));

  // Round()
  TestDecimalValue("round(cast('3.14159' as decimal(6,5)))", Decimal4Value(3),
      ColumnType::CreateDecimalType(2, 0));
  TestDecimalValue("round(cast('-3.14159' as decimal(6,5)))", Decimal4Value(-3),
      ColumnType::CreateDecimalType(2, 0));
  TestDecimalValue("round(cast('3' as decimal(6,5)))", Decimal4Value(3),
      ColumnType::CreateDecimalType(2, 0));
  TestDecimalValue("round(cast('3.14159' as decimal(13,5)))", Decimal8Value(3),
      ColumnType::CreateDecimalType(9, 0));
  TestDecimalValue("round(cast('-3.14159' as decimal(13,5)))", Decimal8Value(-3),
      ColumnType::CreateDecimalType(9, 0));
  TestDecimalValue("round(cast('3' as decimal(13,5)))", Decimal8Value(3),
      ColumnType::CreateDecimalType(9, 0));
  TestDecimalValue("round(cast('3.14159' as decimal(33,5)))", Decimal16Value(3),
      ColumnType::CreateDecimalType(29, 0));
  TestDecimalValue("round(cast('-3.14159' as decimal(33,5)))", Decimal16Value(-3),
      ColumnType::CreateDecimalType(29, 0));
  TestDecimalValue("round(cast('3' as decimal(33,5)))", Decimal16Value(3),
      ColumnType::CreateDecimalType(29, 0));
  TestDecimalValue("round(cast('9.54159' as decimal(6,5)))", Decimal4Value(10),
      ColumnType::CreateDecimalType(2, 0));
  TestDecimalValue("round(cast('-9.54159' as decimal(6,5)))", Decimal4Value(-10),
      ColumnType::CreateDecimalType(2, 0));
  TestIsNull("round(cast(NULL as decimal(2,0)))", ColumnType::CreateDecimalType(2,0));

  // Truncate()
  TestDecimalValue("truncate(cast('3.54159' as decimal(6,5)))", Decimal4Value(3),
      ColumnType::CreateDecimalType(1, 0));
  TestDecimalValue("truncate(cast('-3.54159' as decimal(6,5)))", Decimal4Value(-3),
      ColumnType::CreateDecimalType(1, 0));
  TestDecimalValue("truncate(cast('3' as decimal(6,5)))", Decimal4Value(3),
      ColumnType::CreateDecimalType(1, 0));
  TestDecimalValue("truncate(cast('3.54159' as decimal(13,5)))", Decimal8Value(3),
      ColumnType::CreateDecimalType(8, 0));
  TestDecimalValue("truncate(cast('-3.54159' as decimal(13,5)))", Decimal8Value(-3),
      ColumnType::CreateDecimalType(8, 0));
  TestDecimalValue("truncate(cast('3' as decimal(13,5)))", Decimal8Value(3),
      ColumnType::CreateDecimalType(8, 0));
  TestDecimalValue("truncate(cast('3.54159' as decimal(33,5)))", Decimal16Value(3),
      ColumnType::CreateDecimalType(28, 0));
  TestDecimalValue("truncate(cast('-3.54159' as decimal(33,5)))", Decimal16Value(-3),
      ColumnType::CreateDecimalType(28, 0));
  TestDecimalValue("truncate(cast('3' as decimal(33,5)))", Decimal16Value(3),
      ColumnType::CreateDecimalType(28, 0));
  TestDecimalValue("truncate(cast('9.54159' as decimal(6,5)))", Decimal4Value(9),
      ColumnType::CreateDecimalType(1, 0));
  TestIsNull("truncate(cast(NULL as decimal(2,0)))", ColumnType::CreateDecimalType(2,0));

  // RoundTo()
  TestIsNull("round(cast(NULL as decimal(2,0)), 1)", ColumnType::CreateDecimalType(2,0));

  TestDecimalValue("round(cast('3.1615' as decimal(6,4)), 0)", Decimal4Value(3),
      ColumnType::CreateDecimalType(3, 0));
  TestDecimalValue("round(cast('-3.1615' as decimal(6,4)), 1)", Decimal4Value(-32),
      ColumnType::CreateDecimalType(4, 1));
  TestDecimalValue("round(cast('3.1615' as decimal(6,4)), 2)", Decimal4Value(316),
      ColumnType::CreateDecimalType(5, 2));
  TestDecimalValue("round(cast('3.1615' as decimal(6,4)), 3)", Decimal4Value(3162),
      ColumnType::CreateDecimalType(6, 3));
  TestDecimalValue("round(cast('-3.1615' as decimal(6,4)), 3)", Decimal4Value(-3162),
      ColumnType::CreateDecimalType(6, 3));
  TestDecimalValue("round(cast('3.1615' as decimal(6,4)), 4)", Decimal4Value(31615),
      ColumnType::CreateDecimalType(6, 4));
  TestDecimalValue("round(cast('-3.1615' as decimal(6,4)), 5)", Decimal4Value(-316150),
      ColumnType::CreateDecimalType(7, 5));
  TestDecimalValue("round(cast('175.0' as decimal(6,1)), 0)", Decimal4Value(175),
      ColumnType::CreateDecimalType(6, 0));
  TestDecimalValue("round(cast('-175.0' as decimal(6,1)), -1)", Decimal4Value(-180),
      ColumnType::CreateDecimalType(6, 0));
  TestDecimalValue("round(cast('175.0' as decimal(6,1)), -2)", Decimal4Value(200),
      ColumnType::CreateDecimalType(6, 0));
  TestDecimalValue("round(cast('-175.0' as decimal(6,1)), -3)", Decimal4Value(0),
      ColumnType::CreateDecimalType(6, 0));
  TestDecimalValue("round(cast('175.0' as decimal(6,1)), -4)", Decimal4Value(0),
      ColumnType::CreateDecimalType(6, 0));
  TestDecimalValue("round(cast('999.951' as decimal(6,3)), 1)", Decimal4Value(10000),
      ColumnType::CreateDecimalType(5, 1));

  TestDecimalValue("round(cast('-3.1615' as decimal(16,4)), 0)", Decimal8Value(-3),
      ColumnType::CreateDecimalType(13, 0));
  TestDecimalValue("round(cast('3.1615' as decimal(16,4)), 1)", Decimal8Value(32),
      ColumnType::CreateDecimalType(14, 1));
  TestDecimalValue("round(cast('-3.1615' as decimal(16,4)), 2)", Decimal8Value(-316),
      ColumnType::CreateDecimalType(15, 2));
  TestDecimalValue("round(cast('3.1615' as decimal(16,4)), 3)", Decimal8Value(3162),
      ColumnType::CreateDecimalType(16, 3));
  TestDecimalValue("round(cast('-3.1615' as decimal(16,4)), 3)", Decimal8Value(-3162),
      ColumnType::CreateDecimalType(16, 3));
  TestDecimalValue("round(cast('-3.1615' as decimal(16,4)), 4)", Decimal8Value(-31615),
      ColumnType::CreateDecimalType(16, 4));
  TestDecimalValue("round(cast('3.1615' as decimal(16,4)), 5)", Decimal8Value(316150),
      ColumnType::CreateDecimalType(17, 5));
  TestDecimalValue("round(cast('-999.951' as decimal(16,3)), 1)", Decimal8Value(-10000),
      ColumnType::CreateDecimalType(15, 1));

  TestDecimalValue("round(cast('-175.0' as decimal(15,1)), 0)", Decimal8Value(-175),
      ColumnType::CreateDecimalType(15, 0));
  TestDecimalValue("round(cast('175.0' as decimal(15,1)), -1)", Decimal8Value(180),
      ColumnType::CreateDecimalType(15, 0));
  TestDecimalValue("round(cast('-175.0' as decimal(15,1)), -2)", Decimal8Value(-200),
      ColumnType::CreateDecimalType(15, 0));
  TestDecimalValue("round(cast('175.0' as decimal(15,1)), -3)", Decimal8Value(0),
      ColumnType::CreateDecimalType(15, 0));
  TestDecimalValue("round(cast('-175.0' as decimal(15,1)), -4)", Decimal8Value(0),
      ColumnType::CreateDecimalType(15, 0));

  TestDecimalValue("round(cast('3.1615' as decimal(32,4)), 0)", Decimal16Value(3),
      ColumnType::CreateDecimalType(29, 0));
  TestDecimalValue("round(cast('-3.1615' as decimal(32,4)), 1)", Decimal16Value(-32),
      ColumnType::CreateDecimalType(30, 1));
  TestDecimalValue("round(cast('3.1615' as decimal(32,4)), 2)", Decimal16Value(316),
      ColumnType::CreateDecimalType(31, 2));
  TestDecimalValue("round(cast('3.1615' as decimal(32,4)), 3)", Decimal16Value(3162),
      ColumnType::CreateDecimalType(32, 3));
  TestDecimalValue("round(cast('-3.1615' as decimal(32,4)), 3)", Decimal16Value(-3162),
      ColumnType::CreateDecimalType(32, 3));
  TestDecimalValue("round(cast('3.1615' as decimal(32,4)), 4)", Decimal16Value(31615),
      ColumnType::CreateDecimalType(32, 4));
  TestDecimalValue("round(cast('-3.1615' as decimal(32,5)), 5)", Decimal16Value(-316150),
      ColumnType::CreateDecimalType(32, 5));
  TestDecimalValue("round(cast('-175.0' as decimal(35,1)), 0)", Decimal16Value(-175),
      ColumnType::CreateDecimalType(35, 0));
  TestDecimalValue("round(cast('175.0' as decimal(35,1)), -1)", Decimal16Value(180),
      ColumnType::CreateDecimalType(35, 0));
  TestDecimalValue("round(cast('-175.0' as decimal(35,1)), -2)", Decimal16Value(-200),
      ColumnType::CreateDecimalType(35, 0));
  TestDecimalValue("round(cast('175.0' as decimal(35,1)), -3)", Decimal16Value(0),
      ColumnType::CreateDecimalType(35, 0));
  TestDecimalValue("round(cast('-175.0' as decimal(35,1)), -4)", Decimal16Value(0),
      ColumnType::CreateDecimalType(35, 0));
  TestDecimalValue("round(cast('99999.9951' as decimal(35,4)), 2)",
      Decimal16Value(10000000), ColumnType::CreateDecimalType(34, 2));

  // Dround() alias
  TestDecimalValue("dround(cast('3.14159' as decimal(6,5)))", Decimal4Value(3),
      ColumnType::CreateDecimalType(2, 0));
  TestDecimalValue("dround(cast('99999.9951' as decimal(35,4)), 2)",
      Decimal16Value(10000000), ColumnType::CreateDecimalType(34, 2));

  // TruncateTo()
  TestIsNull("truncate(cast(NULL as decimal(2,0)), 1)",
      ColumnType::CreateDecimalType(2,0));

  TestDecimalValue("truncate(cast('-3.1615' as decimal(6,4)), 0)", Decimal4Value(-3),
      ColumnType::CreateDecimalType(2, 0));
  TestDecimalValue("truncate(cast('3.1615' as decimal(6,4)), 1)", Decimal4Value(31),
      ColumnType::CreateDecimalType(3, 1));
  TestDecimalValue("truncate(cast('-3.1615' as decimal(6,4)), 2)", Decimal4Value(-316),
      ColumnType::CreateDecimalType(4, 2));
  TestDecimalValue("truncate(cast('3.1615' as decimal(6,4)), 3)", Decimal4Value(3161),
      ColumnType::CreateDecimalType(5, 3));
  TestDecimalValue("truncate(cast('-3.1615' as decimal(6,4)), 4)", Decimal4Value(-31615),
      ColumnType::CreateDecimalType(6, 4));
  TestDecimalValue("truncate(cast('3.1615' as decimal(6,4)), 5)", Decimal4Value(316150),
      ColumnType::CreateDecimalType(7, 5));
  TestDecimalValue("truncate(cast('175.0' as decimal(6,1)), 0)", Decimal4Value(175),
      ColumnType::CreateDecimalType(5, 0));
  TestDecimalValue("truncate(cast('-175.0' as decimal(6,1)), -1)", Decimal4Value(-170),
      ColumnType::CreateDecimalType(5, 0));
  TestDecimalValue("truncate(cast('175.0' as decimal(6,1)), -2)", Decimal4Value(100),
      ColumnType::CreateDecimalType(5, 0));
  TestDecimalValue("truncate(cast('-175.0' as decimal(6,1)), -3)", Decimal4Value(0),
      ColumnType::CreateDecimalType(5, 0));
  TestDecimalValue("truncate(cast('175.0' as decimal(6,1)), -4)", Decimal4Value(0),
      ColumnType::CreateDecimalType(5, 0));

  TestDecimalValue("truncate(cast('-3.1615' as decimal(16,4)), 0)", Decimal8Value(-3),
      ColumnType::CreateDecimalType(12, 0));
  TestDecimalValue("truncate(cast('3.1615' as decimal(16,4)), 1)", Decimal8Value(31),
      ColumnType::CreateDecimalType(13, 1));
  TestDecimalValue("truncate(cast('-3.1615' as decimal(16,4)), 2)", Decimal8Value(-316),
      ColumnType::CreateDecimalType(14, 2));
  TestDecimalValue("truncate(cast('3.1615' as decimal(16,4)), 3)", Decimal8Value(3161),
      ColumnType::CreateDecimalType(15, 3));
  TestDecimalValue("truncate(cast('3.1615' as decimal(16,4)), 4)",
      Decimal8Value(31615), ColumnType::CreateDecimalType(16, 4));
  TestDecimalValue("truncate(cast('-3.1615' as decimal(16,4)), 5)",
      Decimal8Value(-316150), ColumnType::CreateDecimalType(17, 5));
  TestDecimalValue("truncate(cast('-175.0' as decimal(15,1)), 0)", Decimal8Value(-175),
      ColumnType::CreateDecimalType(14, 0));
  TestDecimalValue("truncate(cast('175.0' as decimal(15,1)), -1)", Decimal8Value(170),
      ColumnType::CreateDecimalType(14, 0));
  TestDecimalValue("truncate(cast('-175.0' as decimal(15,1)), -2)", Decimal8Value(-100),
      ColumnType::CreateDecimalType(14, 0));
  TestDecimalValue("truncate(cast('175.0' as decimal(15,1)), -3)", Decimal8Value(0),
      ColumnType::CreateDecimalType(14, 0));
  TestDecimalValue("truncate(cast('-175.0' as decimal(15,1)), -4)", Decimal8Value(0),
      ColumnType::CreateDecimalType(14, 0));

  TestDecimalValue("truncate(cast('-3.1615' as decimal(32,4)), 0)",
      Decimal16Value(-3), ColumnType::CreateDecimalType(28, 0));
  TestDecimalValue("truncate(cast('3.1615' as decimal(32,4)), 1)",
      Decimal16Value(31), ColumnType::CreateDecimalType(29, 1));
  TestDecimalValue("truncate(cast('-3.1615' as decimal(32,4)), 2)",
      Decimal16Value(-316), ColumnType::CreateDecimalType(30, 2));
  TestDecimalValue("truncate(cast('3.1615' as decimal(32,4)), 3)",
      Decimal16Value(3161), ColumnType::CreateDecimalType(31, 3));
  TestDecimalValue("truncate(cast('-3.1615' as decimal(32,4)), 4)",
      Decimal16Value(-31615), ColumnType::CreateDecimalType(32, 4));
  TestDecimalValue("truncate(cast('3.1615' as decimal(32,4)), 5)",
      Decimal16Value(316150), ColumnType::CreateDecimalType(33, 5));
  TestDecimalValue("truncate(cast('-175.0' as decimal(35,1)), 0)",
      Decimal16Value(-175), ColumnType::CreateDecimalType(34, 0));
  TestDecimalValue("truncate(cast('175.0' as decimal(35,1)), -1)",
      Decimal16Value(170), ColumnType::CreateDecimalType(34, 0));
  TestDecimalValue("truncate(cast('-175.0' as decimal(35,1)), -2)",
      Decimal16Value(-100), ColumnType::CreateDecimalType(34, 0));
  TestDecimalValue("truncate(cast('175.0' as decimal(35,1)), -3)",
      Decimal16Value(0), ColumnType::CreateDecimalType(34, 0));
  TestDecimalValue("truncate(cast('-175.0' as decimal(35,1)), -4)",
      Decimal16Value(0), ColumnType::CreateDecimalType(34, 0));

  // Dtrunc() alias
  TestDecimalValue("dtrunc(cast('3.54159' as decimal(6,5)))", Decimal4Value(3),
      ColumnType::CreateDecimalType(1, 0));
  TestDecimalValue("dtrunc(cast('-3.1615' as decimal(6,4)), 0)", Decimal4Value(-3),
      ColumnType::CreateDecimalType(2, 0));

  // Overflow on Round()/etc. This can only happen when the input is has enough
  // leading 9's.
  // Rounding this value requires a precision of 39 so it overflows.
  TestIsNull("round(99999999999999999999999999999999999999., -1)",
      ColumnType::CreateDecimalType(38, 0));
  TestIsNull("round(-99999999999999999999999999999999000000., -7)",
      ColumnType::CreateDecimalType(38, 0));
}

// Sanity check some overflow casting. We have a random test framework that covers
// this more thoroughly.
TEST_F(ExprTest, DecimalOverflowCasts) {
  TestDecimalValue("cast(123.456 as decimal(6,3))",
      Decimal4Value(123456), ColumnType::CreateDecimalType(6, 3));
  TestDecimalValue("cast(-123.456 as decimal(6,1))",
      Decimal4Value(-1234), ColumnType::CreateDecimalType(6, 1));
  TestDecimalValue("cast(123.456 as decimal(6,0))",
      Decimal4Value(123), ColumnType::CreateDecimalType(6, 0));
  TestDecimalValue("cast(-123.456 as decimal(3,0))",
      Decimal4Value(-123), ColumnType::CreateDecimalType(3, 0));

  TestDecimalValue("cast(123.4567890 as decimal(10,7))",
      Decimal8Value(1234567890L), ColumnType::CreateDecimalType(10, 7));

  TestDecimalValue("cast(cast(\"123.01234567890123456789\" as decimal(23,20))\
      as decimal(12,9))", Decimal8Value(123012345678L),
      ColumnType::CreateDecimalType(12, 9));
  TestDecimalValue("cast(cast(\"123.01234567890123456789\" as decimal(23,20))\
      as decimal(4,1))", Decimal4Value(1230), ColumnType::CreateDecimalType(4, 1));

  TestDecimalValue("cast(cast(\"123.0123456789\" as decimal(13,10))\
      as decimal(5,2))", Decimal4Value(12301), ColumnType::CreateDecimalType(5, 2));

  // Overflow
  TestIsNull("cast(123.456 as decimal(2,0))", ColumnType::CreateDecimalType(2, 0));
  TestIsNull("cast(123.456 as decimal(2,1))", ColumnType::CreateDecimalType(2, 2));
  TestIsNull("cast(123.456 as decimal(2,2))", ColumnType::CreateDecimalType(2, 2));
  TestIsNull("cast(99.99 as decimal(2,2))", ColumnType::CreateDecimalType(2, 2));
  TestDecimalValue("cast(99.99 as decimal(2,0))",
      Decimal4Value(99), ColumnType::CreateDecimalType(2, 0));
  TestIsNull("cast(-99.99 as decimal(2,2))", ColumnType::CreateDecimalType(2, 2));
  TestDecimalValue("cast(-99.99 as decimal(3,1))",
      Decimal4Value(-999), ColumnType::CreateDecimalType(3, 1));

  TestDecimalValue("cast(999.99 as decimal(6,3))",
      Decimal4Value(999990), ColumnType::CreateDecimalType(6, 3));
  TestDecimalValue("cast(-999.99 as decimal(7,4))",
      Decimal4Value(-9999900), ColumnType::CreateDecimalType(7, 4));
  TestIsNull("cast(9990.99 as decimal(6,3))", ColumnType::CreateDecimalType(6, 3));
  TestIsNull("cast(-9990.99 as decimal(7,4))", ColumnType::CreateDecimalType(7, 4));

  TestDecimalValue("cast(123.4567890 as decimal(4, 1))",
      Decimal4Value(1234), ColumnType::CreateDecimalType(4, 1));
  TestDecimalValue("cast(-123.4567890 as decimal(5, 2))",
      Decimal4Value(-12345), ColumnType::CreateDecimalType(5, 2));
  TestIsNull("cast(123.4567890 as decimal(2, 1))", ColumnType::CreateDecimalType(2, 1));
  TestIsNull("cast(123.4567890 as decimal(6, 5))", ColumnType::CreateDecimalType(6, 5));

  TestDecimalValue("cast(pi() as decimal(1, 0))",
      Decimal4Value(3), ColumnType::CreateDecimalType(1,0));
  TestDecimalValue("cast(pi() as decimal(4, 1))",
      Decimal4Value(31), ColumnType::CreateDecimalType(4,1));
  TestDecimalValue("cast(pi() as decimal(30, 1))",
      Decimal8Value(31), ColumnType::CreateDecimalType(30,1));
  TestIsNull("cast(pi() as decimal(4, 4))", ColumnType::CreateDecimalType(4, 4));
  TestIsNull("cast(pi() as decimal(11, 11))", ColumnType::CreateDecimalType(11, 11));
  TestIsNull("cast(pi() as decimal(31, 31))", ColumnType::CreateDecimalType(31, 31));

  TestIsNull("cast(140573315541874605.4665184383287 as decimal(17, 13))",
      ColumnType::CreateDecimalType(17, 13));
  TestIsNull("cast(140573315541874605.4665184383287 as decimal(9, 3))",
      ColumnType::CreateDecimalType(17, 13));

  // value has 30 digits before the decimal, casting to 29 is an overflow.
  TestIsNull("cast(99999999999999999999999999999.9 as decimal(29, 1))",
      ColumnType::CreateDecimalType(29, 1));

  // Tests converting a non-trivial empty string to a decimal (IMPALA-1566).
  TestIsNull("cast(regexp_replace('','a','b') as decimal(15,2))",
      ColumnType::CreateDecimalType(15,2));
}

TEST_F(ExprTest, NullValueFunction) {
  TestValue("nullvalue(cast(NULL as boolean))", TYPE_BOOLEAN, true);
  TestValue("nullvalue(cast(0 as boolean))", TYPE_BOOLEAN, false);
  TestValue("nullvalue(cast(5 as boolean))", TYPE_BOOLEAN, false);
  TestValue("nullvalue(cast(-5 as boolean))", TYPE_BOOLEAN, false);
  TestValue("nullvalue(cast(NULL as int))", TYPE_BOOLEAN, true);

  TestValue("nullvalue(cast(0 as int))", TYPE_BOOLEAN, false);
  TestValue("nullvalue(cast(5 as int))", TYPE_BOOLEAN, false);
  TestValue("nullvalue(cast(-5 as int))", TYPE_BOOLEAN, false);

  TestValue("nullvalue(cast(NULL as tinyint))", TYPE_BOOLEAN, true);
  TestValue("nullvalue(cast(0 as tinyint))", TYPE_BOOLEAN, false);
  TestValue("nullvalue(cast(5 as tinyint))", TYPE_BOOLEAN, false);
  TestValue("nullvalue(cast(-5 as tinyint))", TYPE_BOOLEAN, false);

  TestValue("nullvalue(cast(NULL as smallint))", TYPE_BOOLEAN, true);
  TestValue("nullvalue(cast(0 as smallint))", TYPE_BOOLEAN, false);
  TestValue("nullvalue(cast(5 as smallint))", TYPE_BOOLEAN, false);
  TestValue("nullvalue(cast(-5 as smallint))", TYPE_BOOLEAN, false);

  TestValue("nullvalue(cast(NULL as bigint))", TYPE_BOOLEAN, true);
  TestValue("nullvalue(cast(0 as bigint))", TYPE_BOOLEAN, false);
  TestValue("nullvalue(cast(5 as bigint))", TYPE_BOOLEAN, false);
  TestValue("nullvalue(cast(-5 as bigint))", TYPE_BOOLEAN, false);

  TestValue("nullvalue(cast(NULL as float))", TYPE_BOOLEAN, true);
  TestValue("nullvalue(cast(0 as float))", TYPE_BOOLEAN, false);
  TestValue("nullvalue(cast(5.0 as float))", TYPE_BOOLEAN, false);
  TestValue("nullvalue(cast(-5.0 as float))", TYPE_BOOLEAN, false);

  TestValue("nullvalue(cast(NULL as double))", TYPE_BOOLEAN, true);
  TestValue("nullvalue(cast(0.0 as double))", TYPE_BOOLEAN, false);
  TestValue("nullvalue(cast(5.0 as double))", TYPE_BOOLEAN, false);
  TestValue("nullvalue(cast(-5.0 as double))", TYPE_BOOLEAN, false);

  TestValue("nullvalue(cast(NULL as decimal(38,0)))", TYPE_BOOLEAN, true);
  TestValue("nullvalue(cast(0 as decimal(38,0)))", TYPE_BOOLEAN, false);
  TestValue("nullvalue(cast(5 as decimal(38,0)))", TYPE_BOOLEAN, false);
  TestValue("nullvalue(cast(-5 as decimal(38,0)))", TYPE_BOOLEAN, false);
  TestValue("nullvalue(cast(0.0 as decimal(38,38)))", TYPE_BOOLEAN, false);
  TestValue("nullvalue(cast(0.1 as decimal(38,38)))", TYPE_BOOLEAN, false);
  TestValue("nullvalue(cast(-0.1 as decimal(38,38)))", TYPE_BOOLEAN, false);

  TestValue("nullvalue(cast(NULL as string))", TYPE_BOOLEAN, true);
  TestValue("nullvalue(cast('0' as string))", TYPE_BOOLEAN, false);
  TestValue("nullvalue(cast('5' as string))", TYPE_BOOLEAN, false);
  TestValue("nullvalue(cast('-5' as string))", TYPE_BOOLEAN, false);
  TestValue("nullvalue(cast(\"abc\" as string))", TYPE_BOOLEAN, false);
  TestValue("nullvalue(cast(\"\" as string))", TYPE_BOOLEAN, false);

  TestValue("nullvalue(cast(NULL as timestamp))", TYPE_BOOLEAN, true);
  TestValue("nullvalue(cast('2012-01-01 09:10:11.123456789' as timestamp))",
      TYPE_BOOLEAN, false);

  TestValue("nullvalue(0)", TYPE_BOOLEAN, false);
  TestValue("nullvalue(-5)", TYPE_BOOLEAN, false);
  TestValue("nullvalue(5)", TYPE_BOOLEAN, false);
  TestValue("nullvalue(0.0)", TYPE_BOOLEAN, false);
  TestValue("nullvalue(-1.2345)", TYPE_BOOLEAN, false);
  TestValue("nullvalue(1.2345)", TYPE_BOOLEAN, false);
  TestValue("nullvalue(\"\")", TYPE_BOOLEAN, false);
  TestValue("nullvalue(\"abc\")", TYPE_BOOLEAN, false);

  TestValue("nullvalue(99999999999999999999999999999999999)", TYPE_BOOLEAN, false);
  TestValue("nullvalue(-99999999999999999999999999999999999)", TYPE_BOOLEAN, false);
  TestValue("nullvalue(99999999999999999999999999999999999.9)", TYPE_BOOLEAN, false);
  TestValue("nullvalue(-99999999999999999999999999999999999.9)", TYPE_BOOLEAN, false);
}

TEST_F(ExprTest, NonNullValueFunction) {
  TestValue("nonnullvalue(cast(NULL as boolean))", TYPE_BOOLEAN, false);
  TestValue("nonnullvalue(cast(0 as boolean))", TYPE_BOOLEAN, true);
  TestValue("nonnullvalue(cast(5 as boolean))", TYPE_BOOLEAN, true);
  TestValue("nonnullvalue(cast(-5 as boolean))", TYPE_BOOLEAN, true);

  TestValue("nonnullvalue(cast(NULL as int))", TYPE_BOOLEAN, false);
  TestValue("nonnullvalue(cast(0 as int))", TYPE_BOOLEAN, true);
  TestValue("nonnullvalue(cast(5 as int))", TYPE_BOOLEAN, true);
  TestValue("nonnullvalue(cast(-5 as int))", TYPE_BOOLEAN, true);

  TestValue("nonnullvalue(cast(NULL as tinyint))", TYPE_BOOLEAN, false);
  TestValue("nonnullvalue(cast(0 as tinyint))", TYPE_BOOLEAN, true);
  TestValue("nonnullvalue(cast(5 as tinyint))", TYPE_BOOLEAN, true);
  TestValue("nonnullvalue(cast(-5 as tinyint))", TYPE_BOOLEAN, true);

  TestValue("nonnullvalue(cast(NULL as smallint))", TYPE_BOOLEAN, false);
  TestValue("nonnullvalue(cast(0 as smallint))", TYPE_BOOLEAN, true);
  TestValue("nonnullvalue(cast(5 as smallint))", TYPE_BOOLEAN, true);
  TestValue("nonnullvalue(cast(-5 as smallint))", TYPE_BOOLEAN, true);

  TestValue("nonnullvalue(cast(NULL as bigint))", TYPE_BOOLEAN, false);
  TestValue("nonnullvalue(cast(0 as bigint))", TYPE_BOOLEAN, true);
  TestValue("nonnullvalue(cast(5 as bigint))", TYPE_BOOLEAN, true);
  TestValue("nonnullvalue(cast(-5 as bigint))", TYPE_BOOLEAN, true);

  TestValue("nonnullvalue(cast(NULL as float))", TYPE_BOOLEAN, false);
  TestValue("nonnullvalue(cast(0 as float))", TYPE_BOOLEAN, true);
  TestValue("nonnullvalue(cast(5.0 as float))", TYPE_BOOLEAN, true);
  TestValue("nonnullvalue(cast(-5.0 as float))", TYPE_BOOLEAN, true);

  TestValue("nonnullvalue(cast(NULL as double))", TYPE_BOOLEAN, false);
  TestValue("nonnullvalue(cast(0.0 as double))", TYPE_BOOLEAN, true);
  TestValue("nonnullvalue(cast(5.0 as double))", TYPE_BOOLEAN, true);
  TestValue("nonnullvalue(cast(-5.0 as double))", TYPE_BOOLEAN, true);

  TestValue("nonnullvalue(cast(NULL as decimal(38,0)))", TYPE_BOOLEAN, false);
  TestValue("nonnullvalue(cast(0 as decimal(38,0)))", TYPE_BOOLEAN, true);
  TestValue("nonnullvalue(cast(5 as decimal(38,0)))", TYPE_BOOLEAN, true);
  TestValue("nonnullvalue(cast(-5 as decimal(38,0)))", TYPE_BOOLEAN, true);
  TestValue("nonnullvalue(cast(0.0 as decimal(38,38)))", TYPE_BOOLEAN, true);
  TestValue("nonnullvalue(cast(0.1 as decimal(38,38)))", TYPE_BOOLEAN, true);
  TestValue("nonnullvalue(cast(-0.1 as decimal(38,38)))", TYPE_BOOLEAN, true);

  TestValue("nonnullvalue(cast(NULL as string))", TYPE_BOOLEAN, false);
  TestValue("nonnullvalue(cast('0' as string))", TYPE_BOOLEAN, true);
  TestValue("nonnullvalue(cast('5' as string))", TYPE_BOOLEAN, true);
  TestValue("nonnullvalue(cast('-5' as string))", TYPE_BOOLEAN, true);
  TestValue("nonnullvalue(cast(\"abc\" as string))", TYPE_BOOLEAN, true);
  TestValue("nonnullvalue(cast(\"\" as string))", TYPE_BOOLEAN, true);

  TestValue("nonnullvalue(cast(NULL as timestamp))", TYPE_BOOLEAN, false);
  TestValue("nonnullvalue(cast('2012-01-01 09:10:11.123456789' as timestamp))",
      TYPE_BOOLEAN, true);

  TestValue("nonnullvalue(0)", TYPE_BOOLEAN, true);
  TestValue("nonnullvalue(-5)", TYPE_BOOLEAN, true);
  TestValue("nonnullvalue(5)", TYPE_BOOLEAN, true);
  TestValue("nonnullvalue(0.0)", TYPE_BOOLEAN, true);
  TestValue("nonnullvalue(-1.2345)", TYPE_BOOLEAN, true);
  TestValue("nonnullvalue(1.2345)", TYPE_BOOLEAN, true);
  TestValue("nonnullvalue(\"\")", TYPE_BOOLEAN, true);
  TestValue("nonnullvalue(\"abc\")", TYPE_BOOLEAN, true);

  TestValue("nonnullvalue(99999999999999999999999999999999999)", TYPE_BOOLEAN, true);
  TestValue("nonnullvalue(-99999999999999999999999999999999999)", TYPE_BOOLEAN, true);
  TestValue("nonnullvalue(99999999999999999999999999999999999.9)", TYPE_BOOLEAN, true);
  TestValue("nonnullvalue(-99999999999999999999999999999999999.9)", TYPE_BOOLEAN, true);
}

TEST_F(ExprTest, UdfInterfaceBuiltins) {
  TestValue("udf_pi()", TYPE_DOUBLE, M_PI);
  TestValue("udf_abs(-1)", TYPE_DOUBLE, 1.0);
  TestStringValue("udf_lower('Hello_WORLD')", "hello_world");

  TestValue("max_tinyint()", TYPE_TINYINT, numeric_limits<int8_t>::max());
  TestValue("max_smallint()", TYPE_SMALLINT, numeric_limits<int16_t>::max());
  TestValue("max_int()", TYPE_INT, numeric_limits<int32_t>::max());
  TestValue("max_bigint()", TYPE_BIGINT, numeric_limits<int64_t>::max());

  TestValue("min_tinyint()", TYPE_TINYINT, numeric_limits<int8_t>::min());
  TestValue("min_smallint()", TYPE_SMALLINT, numeric_limits<int16_t>::min());
  TestValue("min_int()", TYPE_INT, numeric_limits<int32_t>::min());
  TestValue("min_bigint()", TYPE_BIGINT, numeric_limits<int64_t>::min());
}

TEST_F(ExprTest, MADlib) {
  TestStringValue("madlib_encode_vector(madlib_vector(1.0, 2.0, 3.0))",
                  "aaaaaipdaaaaaaaeaaaaaeae");
  TestStringValue("madlib_print_vector(madlib_vector(1, 2, 3))", "<1, 2, 3>");
  TestStringValue(
    "madlib_encode_vector(madlib_decode_vector(madlib_encode_vector("
      "madlib_vector(1.0, 2.0, 3.0))))",
    "aaaaaipdaaaaaaaeaaaaaeae");
  TestValue("madlib_vector_get(0, madlib_vector(1.0, 2.0, 3.0))", TYPE_DOUBLE, 1.0);
  TestValue("madlib_vector_get(1, madlib_vector(1.0, 2.0, 3.0))", TYPE_DOUBLE, 2.0);
  TestValue("madlib_vector_get(2, madlib_vector(1.0, 2.0, 3.0))", TYPE_DOUBLE, 3.0);
  TestIsNull("madlib_vector_get(3, madlib_vector(1.0, 2.0, 3.0))", TYPE_DOUBLE);
  TestIsNull("madlib_vector_get(-1, madlib_vector(1.0, 2.0, 3.0))", TYPE_DOUBLE);
  TestValue(
    "madlib_vector_get(2, madlib_decode_vector(madlib_encode_vector("
      "madlib_vector(1.0, 2.0, 3.0))))",
    TYPE_DOUBLE, 3.0);
}

TEST_F(ExprTest, BitByteBuiltins) {
  TestIsNull("bitand(1,NULL)", TYPE_TINYINT);
  TestIsNull("bitand(NULL,3)", TYPE_TINYINT);
  // And of numbers differing in 2nd bit position gives min of two numbers
  TestValue("bitand(1,3)", TYPE_TINYINT, 1);
  TestValue("bitand(129,131)", TYPE_SMALLINT, 129);
  TestValue("bitand(32769,32771)", TYPE_INT, 32769);
  TestValue("bitand(2147483649,2147483651)", TYPE_BIGINT, 2147483649);

  TestIsNull("bitor(1,NULL)", TYPE_TINYINT);
  TestIsNull("bitor(NULL,3)", TYPE_TINYINT);
  // Or of numbers differing in 2nd bit position gives max of two numbers
  TestValue("bitor(1,3)", TYPE_TINYINT, 3);
  TestValue("bitor(129,131)", TYPE_SMALLINT, 131);
  TestValue("bitor(32769,32771)", TYPE_INT, 32771);
  TestValue("bitor(2147483649,2147483651)", TYPE_BIGINT, 2147483651);

  TestIsNull("bitxor(1,NULL)", TYPE_TINYINT);
  TestIsNull("bitxor(NULL,3)", TYPE_TINYINT);
  // Xor of numbers differing in 2nd bit position gives 2
  TestValue("bitxor(1,3)", TYPE_TINYINT, 2);
  TestValue("bitxor(129,131)", TYPE_SMALLINT, 2);
  TestValue("bitxor(32769,32771)", TYPE_INT, 2);
  TestValue("bitxor(2147483649,2147483651)", TYPE_BIGINT, 2);

  TestIsNull("bitnot(NULL)", TYPE_TINYINT);
  TestValue("bitnot(1)", TYPE_TINYINT, -2);
  TestValue("bitnot(129)", TYPE_SMALLINT, -130);
  TestValue("bitnot(32769)", TYPE_INT, -32770);
  TestValue("bitnot(2147483649)", TYPE_BIGINT, -2147483650);

  // basic bit patterns
  TestValue("countset(0)", TYPE_INT, 0);
  TestValue("countset(1)", TYPE_INT, 1);
  TestValue("countset(2)", TYPE_INT, 1);
  TestValue("countset(3)", TYPE_INT, 2);
  // 0101... bit pattern
  TestValue("countset(" + lexical_cast<string>(0x55) + ")", TYPE_INT, 4);
  TestValue("countset(" + lexical_cast<string>(0x5555) + ")", TYPE_INT, 8);
  TestValue("countset(" + lexical_cast<string>(0x55555555) + ")", TYPE_INT, 16);
  TestValue("countset(" + lexical_cast<string>(0x5555555555555555) + ")", TYPE_INT, 32);
  // 1111... bit pattern to test signed/unsigned conversion
  TestValue("countset(cast(-1 as TINYINT))", TYPE_INT, 8);
  TestValue("countset(cast(-1 as SMALLINT))", TYPE_INT, 16);
  TestValue("countset(cast(-1 as INT))", TYPE_INT, 32);
  TestValue("countset(cast(-1 as BIGINT))", TYPE_INT, 64);
  // NULL arguments
  TestIsNull("countset(cast(NULL as TINYINT))", TYPE_INT);
  TestIsNull("countset(cast(NULL as SMALLINT))", TYPE_INT);
  TestIsNull("countset(cast(NULL as INT))", TYPE_INT);
  TestIsNull("countset(cast(NULL as BIGINT))", TYPE_INT);

  // Check with optional argument
  TestIsNull("countset(0, NULL)", TYPE_INT);
  TestValue("countset(0, 0)", TYPE_INT, 8);
  TestValue("countset(0, 1)", TYPE_INT, 0);
  TestError("countset(0, 2)");

  // getbit for all integer types
  TestIsNull("getbit(NULL, 1)", TYPE_TINYINT);
  TestIsNull("getbit(1, NULL)", TYPE_TINYINT);
  TestValue("getbit(1, 0)", TYPE_TINYINT, 1);
  TestValue("getbit(1, 1)", TYPE_TINYINT, 0);
  string int8_min = lexical_cast<string>((int16_t)numeric_limits<int8_t>::min());
  TestValue("getbit(" + int8_min + ", 7)", TYPE_TINYINT, 1);
  TestValue("getbit(" + int8_min + ", 6)", TYPE_TINYINT, 0);
  string int16_min = lexical_cast<string>(numeric_limits<int16_t>::min());
  TestValue("getbit(" + int16_min + ", 15)", TYPE_TINYINT, 1);
  TestValue("getbit(" + int16_min + ", 14)", TYPE_TINYINT, 0);
  string int32_min = lexical_cast<string>(numeric_limits<int32_t>::min());
  TestValue("getbit(" + int32_min + ", 31)", TYPE_TINYINT, 1);
  TestValue("getbit(" + int32_min + ", 30)", TYPE_TINYINT, 0);
  string int64_min = lexical_cast<string>(numeric_limits<int64_t>::min());
  TestValue("getbit(" + int64_min + ", 63)", TYPE_TINYINT, 1);
  TestValue("getbit(" + int64_min + ", 62)", TYPE_TINYINT, 0);
  // Out of range bitpos causes errors
  // The following TestError() calls also test IMPALA-2141 and IMPALA-2188
   TestError("getbit(0, -1)");
   TestError("getbit(0, 8)");
   TestError("getbit(" + int16_min + ", 16)");
   TestError("getbit(" + int32_min + ", 32)");
   TestError("getbit(" + int64_min + ", 64)");

  // Set bits for all integer types
  TestIsNull("setbit(cast(NULL as INT), 1)", TYPE_INT);
  TestIsNull("setbit(1, NULL)", TYPE_TINYINT);
  TestIsNull("setbit(cast(NULL as INT), 1, 1)", TYPE_INT);
  TestIsNull("setbit(1, NULL, 1)", TYPE_TINYINT);
  TestIsNull("setbit(1, 1, NULL)", TYPE_TINYINT);
  // The following TestError() calls also test IMPALA-2141 and IMPALA-2188
  TestError("setbit(1, 1, -1)");
  TestError("setbit(1, 1, 2)");

  TestValue("setbit(0, 0)", TYPE_TINYINT, 1);
  TestValue("setbit(0, 0, 1)", TYPE_TINYINT, 1);
  TestValue("setbit(1, 0, 0)", TYPE_TINYINT, 0);
  TestValue("setbit(cast(1 as INT), 8)", TYPE_INT, 257);
  TestValue("setbit(cast(1 as INT), 8, 1)", TYPE_INT, 257);
  TestValue("setbit(cast(257 as INT), 8, 0)", TYPE_INT, 1);
  TestValue("setbit(cast(-1 as BIGINT), 63, 0)", TYPE_BIGINT,
      numeric_limits<int64_t>::max());
  // Out of range bitpos causes errors
  // The following TestError() calls also test IMPALA-2141 and IMPALA-2188
  TestError("setbit(0, -1)");
  TestError("setbit(0, 8)");
  TestError("setbit(0, -1, 1)");
  TestError("setbit(0, 8, 1)");

  // Shift and rotate null checks
  TestIsNull("shiftleft(1, NULL)", TYPE_TINYINT);
  TestIsNull("shiftleft(cast(NULL as INT), 2)", TYPE_INT);
  TestIsNull("rotateleft(cast(NULL as INT), 2)", TYPE_INT);
  TestIsNull("shiftright(1, NULL)", TYPE_TINYINT);
  TestIsNull("shiftright(cast(NULL as INT), 2)", TYPE_INT);
  TestIsNull("rotateright(cast(NULL as INT), 2)", TYPE_INT);

  // Basic left shift/rotate tests for all integer types
  TestValue("shiftleft(1, 2)", TYPE_TINYINT, 4);
  TestValue("rotateleft(1, 2)", TYPE_TINYINT, 4);
  string pow2_6 = lexical_cast<string>(1 << 6);
  TestValue("shiftleft(" + pow2_6 + ", 2)", TYPE_TINYINT, 0);
  TestValue("rotateleft(" + pow2_6 + ", 2)", TYPE_TINYINT, 1);
  TestValue("shiftleft(" + pow2_6 + ", 1)", TYPE_TINYINT, -(1 << 7));
  TestValue("rotateleft(" + pow2_6 + ", 1)", TYPE_TINYINT, -(1 << 7));
  TestValue("shiftleft(cast(1 as SMALLINT), 2)", TYPE_SMALLINT, 4);
  string pow2_14 = lexical_cast<string>(1 << 14);
  TestValue("shiftleft(" + pow2_14 + ", 2)", TYPE_SMALLINT, 0);
  TestValue("rotateleft(" + pow2_14 + ", 2)", TYPE_SMALLINT, 1);
  TestValue("rotateleft(" + pow2_14 + ", 34)", TYPE_SMALLINT, 1); // Wraparound
  TestValue("shiftleft(" + pow2_14 + ", 1)", TYPE_SMALLINT, -(1 << 15));
  TestValue("shiftleft(cast(1 as INT), 2)", TYPE_INT, 4);
  string pow2_30 = lexical_cast<string>(1 << 30);
  TestValue("shiftleft(" + pow2_30 + ", 2)", TYPE_INT, 0);
  TestValue("shiftleft(" + pow2_30 + ", 1)", TYPE_INT, numeric_limits<int32_t>::min());
  TestValue("shiftleft(cast(1 as BIGINT), 2)", TYPE_BIGINT, 4);
  string pow2_62 = lexical_cast<string>(((int64_t)1) << 62);
  TestValue("shiftleft(" + pow2_62 + ", 2)", TYPE_BIGINT, 0);
  TestValue("rotateleft(" + pow2_62 + ", 2)", TYPE_BIGINT, 1);
  TestValue("shiftleft(" + pow2_62 + ", 1)", TYPE_BIGINT, numeric_limits<int64_t>::min());

  // Basic right shift/rotate tests for all integer types
  TestValue("shiftright(4, 2)", TYPE_TINYINT, 1);
  TestValue("shiftright(4, 3)", TYPE_TINYINT, 0);
  TestValue("rotateright(4, 3)", TYPE_TINYINT, -128);
  TestValue("shiftright(4, 4)", TYPE_TINYINT, 0);
  TestValue("rotateright(4, 132)", TYPE_TINYINT, 64);
  string pow2_8 = lexical_cast<string>(1 << 8);
  TestValue("shiftright(" + pow2_8 + ", 1)", TYPE_SMALLINT, 1 << 7);
  TestValue("shiftright(" + pow2_8 + ", 9)", TYPE_SMALLINT, 0);
  string pow2_16 = lexical_cast<string>(1 << 16);
  TestValue("shiftright(" + pow2_16 + ", 1)", TYPE_INT, 1 << 15);
  TestValue("rotateright(" + pow2_16 + ", 1)", TYPE_INT, 1 << 15);
  TestValue("shiftright(" + pow2_16 + ", 17)", TYPE_INT, 0);
  string pow2_32 = lexical_cast<string>(((int64_t)1) << 32);
  TestValue("shiftright(" + pow2_32 + ", 1)", TYPE_BIGINT, ((int64_t)1) << 31);
  TestValue("rotateright(" + pow2_32 + ", 1)", TYPE_BIGINT, ((int64_t)1) << 31);
  TestValue("shiftright(" + pow2_32 + ", 33)", TYPE_BIGINT, 0);
  TestValue("rotateright(" + pow2_32 + ", 33)", TYPE_BIGINT,
      numeric_limits<int64_t>::min());

  // Check that no sign extension happens for negative numbers
  TestValue("shiftright(cast(-1 as INT), 1)", TYPE_INT, 0x7FFFFFFF);
  TestValue("rotateright(-128, 1)", TYPE_TINYINT, 1 << 6);

  // Test shifting/rotating negative amount - should reverse direction
  TestValue("shiftleft(4, -2)", TYPE_TINYINT, 1);
  TestValue("shiftright(cast(1 as BIGINT), -2)", TYPE_BIGINT, 4);
  TestValue("rotateleft(4, -3)", TYPE_TINYINT, -128);
  TestValue("rotateright(256, -2)", TYPE_SMALLINT, 1024);
}

TEST_F(ExprTest, UuidTest) {
  boost::unordered_set<string> string_set;
  const unsigned int NUM_UUIDS = 10;
  const string regex(
      "[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}");
  for (int i = 0; i < NUM_UUIDS; ++i) {
    const string uuid_str = TestStringValueRegex("uuid()", regex);
    string_set.insert(uuid_str);
  }
  EXPECT_TRUE(string_set.size() == NUM_UUIDS);
}

TEST_F(ExprTest, DateTruncTest) {
  TestTimestampValue("date_trunc('MILLENNIUM', '2016-05-08 10:30:00')",
      TimestampValue::Parse("2000-01-01 00:00:00"));
  TestTimestampValue("date_trunc('MILLENNIUM', '2000-01-01 00:00:00')",
      TimestampValue::Parse("2000-01-01 00:00:00"));
  TestTimestampValue("date_trunc('CENTURY', '2016-05-08 10:30:00')",
      TimestampValue::Parse("2000-01-01 00:00:00  "));
  TestTimestampValue("date_trunc('CENTURY', '2116-05-08 10:30:00')",
      TimestampValue::Parse("2100-01-01 00:00:00"));
  TestTimestampValue("date_trunc('DECADE', '2116-05-08 10:30:00')",
      TimestampValue::Parse("2110-01-01 00:00:00"));
  TestTimestampValue("date_trunc('YEAR', '2016-05-08 10:30:00')",
      TimestampValue::Parse("2016-01-01 00:00:00"));
  TestTimestampValue("date_trunc('MONTH', '2016-05-08 00:00:00')",
      TimestampValue::Parse("2016-05-01 00:00:00"));
  TestTimestampValue("date_trunc('WEEK', '2116-05-08 10:30:00')",
      TimestampValue::Parse("2116-05-04 00:00:00"));
  TestTimestampValue("date_trunc('WEEK', '2017-01-01 10:37:03.455722111')",
      TimestampValue::Parse("2016-12-26 00:00:00"));
  TestTimestampValue("date_trunc('WEEK', '2017-01-02 10:37:03.455722111')",
      TimestampValue::Parse("2017-01-02 00:00:00"));
  TestTimestampValue("date_trunc('WEEK', '2017-01-07 10:37:03.455722111')",
      TimestampValue::Parse("2017-01-02 00:00:00"));
  TestTimestampValue("date_trunc('WEEK', '2017-01-08 10:37:03.455722111')",
      TimestampValue::Parse("2017-01-02 00:00:00"));
  TestTimestampValue("date_trunc('WEEK', '2017-01-09 10:37:03.455722111')",
      TimestampValue::Parse("2017-01-09 00:00:00"));
  TestTimestampValue("date_trunc('DAY', '1416-05-08 10:37:03.455722111')",
      TimestampValue::Parse("1416-05-08 00:00:00"));

  TestTimestampValue("date_trunc('HOUR', '1416-05-08 10:30:03.455722111')",
      TimestampValue::Parse("1416-05-08 10:00:00"));
  TestTimestampValue("date_trunc('HOUR', '1416-05-08 23:30:03.455722111')",
      TimestampValue::Parse("1416-05-08 23:00:00"));
  TestTimestampValue("date_trunc('MINUTE', '1416-05-08 10:37:03.455722111')",
      TimestampValue::Parse("1416-05-08 10:37:00"));
  TestTimestampValue("date_trunc('SECOND', '1416-05-08 10:37:03.455722111')",
      TimestampValue::Parse("1416-05-08 10:37:03"));
  TestTimestampValue("date_trunc('MILLISECONDS', '1416-05-08 10:37:03.455722111')",
      TimestampValue::Parse("1416-05-08 10:37:03.455000000"));
  TestTimestampValue("date_trunc('MICROSECONDS', '1416-05-08 10:37:03.455722111')",
      TimestampValue::Parse("1416-05-08 10:37:03.455722000"));

  // Test corner cases.
  TestTimestampValue("date_trunc('MILLENNIUM', '9999-12-31 23:59:59.999999999')",
      TimestampValue::Parse("9000-01-01 00:00:00"));
  TestTimestampValue("date_trunc('CENTURY', '9999-12-31 23:59:59.999999999')",
      TimestampValue::Parse("9900-01-01 00:00:00"));
  TestTimestampValue("date_trunc('DECADE', '9999-12-31 23:59:59.999999999')",
      TimestampValue::Parse("9990-01-01 00:00:00"));
  TestTimestampValue("date_trunc('YEAR', '9999-12-31 23:59:59.999999999')",
      TimestampValue::Parse("9999-01-01 00:00:00"));
  TestTimestampValue("date_trunc('MONTH', '9999-12-31 23:59:59.999999999')",
      TimestampValue::Parse("9999-12-01 00:00:00"));
  TestTimestampValue("date_trunc('WEEK', '9999-12-31 23:59:59.999999999')",
      TimestampValue::Parse("9999-12-27 00:00:00"));
  TestTimestampValue("date_trunc('WEEK', '1400-01-06 23:59:59.999999999')",
      TimestampValue::Parse("1400-01-06 00:00:00"));
  TestTimestampValue("date_trunc('WEEK', '1400-01-07 23:59:59.999999999')",
      TimestampValue::Parse("1400-01-06 00:00:00"));
  TestTimestampValue("date_trunc('DAY', '9999-12-31 23:59:59.999999999')",
      TimestampValue::Parse("9999-12-31 00:00:00"));
  TestTimestampValue("date_trunc('HOUR', '9999-12-31 23:59:59.999999999')",
      TimestampValue::Parse("9999-12-31 23:00:00"));
  TestTimestampValue("date_trunc('MINUTE', '9999-12-31 23:59:59.999999999')",
      TimestampValue::Parse("9999-12-31 23:59:00"));
  TestTimestampValue("date_trunc('SECOND', '9999-12-31 23:59:59.999999999')",
      TimestampValue::Parse("9999-12-31 23:59:59"));
  TestTimestampValue("date_trunc('MILLISECONDS', '9999-12-31 23:59:59.999999999')",
      TimestampValue::Parse("9999-12-31 23:59:59.999"));
  TestTimestampValue("date_trunc('MICROSECONDS', '9999-12-31 23:59:59.999999999')",
      TimestampValue::Parse("9999-12-31 23:59:59.999999"));

  TestTimestampValue("date_trunc('CENTURY', '1400-01-01 00:00:00')",
      TimestampValue::Parse("1400-01-01 00:00:00"));
  TestTimestampValue("date_trunc('DECADE', '1400-01-01 00:00:00')",
      TimestampValue::Parse("1400-01-01 00:00:00"));
  TestTimestampValue("date_trunc('YEAR', '1400-01-01 00:00:00')",
      TimestampValue::Parse("1400-01-01 00:00:00"));
  TestTimestampValue("date_trunc('MONTH', '1400-01-01 00:00:00')",
      TimestampValue::Parse("1400-01-01 00:00:00"));
  TestTimestampValue("date_trunc('DAY', '1400-01-01 00:00:00')",
      TimestampValue::Parse("1400-01-01 00:00:00"));
  TestTimestampValue("date_trunc('HOUR', '1400-01-01 00:00:00')",
      TimestampValue::Parse("1400-01-01 00:00:00"));
  TestTimestampValue("date_trunc('MINUTE', '1400-01-01 00:00:00')",
      TimestampValue::Parse("1400-01-01 00:00:00"));
  TestTimestampValue("date_trunc('SECOND', '1400-01-01 00:00:00')",
      TimestampValue::Parse("1400-01-01 00:00:00"));
  TestTimestampValue("date_trunc('MILLISECONDS', '1400-01-01 00:00:00')",
      TimestampValue::Parse("1400-01-01 00:00:00"));
  TestTimestampValue("date_trunc('MICROSECONDS', '1400-01-01 00:00:00')",
      TimestampValue::Parse("1400-01-01 00:00:00"));

  // valid input with invalid output
  TestIsNull("date_trunc('MILLENNIUM', '1416-05-08 10:30:00')", TYPE_TIMESTAMP);
  TestIsNull("date_trunc('MILLENNIUM', '1999-12-31 11:59:59.999999')", TYPE_TIMESTAMP);
  TestIsNull("date_trunc('WEEK', '1400-01-01 00:00:00')", TYPE_TIMESTAMP);
  TestIsNull("date_trunc('WEEK', '1400-01-05 00:00:00')", TYPE_TIMESTAMP);

  // Test invalid input.
  TestIsNull("date_trunc('HOUR', '12202010')", TYPE_TIMESTAMP);
  TestIsNull("date_trunc('HOUR', '')", TYPE_TIMESTAMP);
  TestIsNull("date_trunc('HOUR', NULL)", TYPE_TIMESTAMP);
  TestIsNull("date_trunc('HOUR', '02-13-2014')", TYPE_TIMESTAMP);
  TestIsNull("date_trunc('CENTURY', '16-05-08 10:30:00')", TYPE_TIMESTAMP);
  TestIsNull("date_trunc('CENTURY', '1116-05-08 10:30:00')", TYPE_TIMESTAMP);
  TestIsNull("date_trunc('DAY', '00:00:00')", TYPE_TIMESTAMP);
  TestError("date_trunc('YsEAR', '2016-05-08 10:30:00')");
  TestError("date_trunc('D', '2116-05-08 10:30:00')");
  TestError("date_trunc('2017-01-09', '2017-01-09 10:37:03.455722111' )");
  TestError("date_trunc('2017-01-09 10:00:00', 'HOUR')");
}
} // namespace impala

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  InitCommonRuntime(argc, argv, true, TestInfo::BE_TEST);
  ABORT_IF_ERROR(TimezoneDatabase::Initialize());
  InitFeSupport(false);
  ABORT_IF_ERROR(impala::LlvmCodeGen::InitializeLlvm());

  // Disable llvm optimization passes if the env var is no set to true. Running without
  // the optimizations makes the tests run much faster.
  char* optimizations = getenv("EXPR_TEST_ENABLE_OPTIMIZATIONS");
  if (optimizations != NULL && strcmp(optimizations, "true") == 0) {
    cout << "Running with optimization passes." << endl;
    FLAGS_disable_optimization_passes = false;
  } else {
    cout << "Running without optimization passes." << endl;
    FLAGS_disable_optimization_passes = true;
  }

  // Create an in-process Impala server and in-process backends for test environment
  // without any startup validation check
  FLAGS_abort_on_config_error = false;
  VLOG_CONNECTION << "creating test env";
  VLOG_CONNECTION << "starting backends";
  InProcessStatestore* ips = InProcessStatestore::StartWithEphemeralPorts();
  InProcessImpalaServer* impala_server =
      InProcessImpalaServer::StartWithEphemeralPorts(FLAGS_hostname, ips->port());
  executor_ = new ImpaladQueryExecutor(impala_server->hostname(),
      impala_server->beeswax_port());
  ABORT_IF_ERROR(executor_->Setup());

  // Disable FE expr rewrites to make sure the Exprs get executed exactly as specified
  // in the tests here.
  int ret;
  disable_codegen_ = true;
  enable_expr_rewrites_ = false;
  executor_->ClearExecOptions();
  executor_->PushExecOption("ENABLE_EXPR_REWRITES=0");
  executor_->PushExecOption("DISABLE_CODEGEN=1");
  cout << "Running without codegen" << endl;
  ret = RUN_ALL_TESTS();
  if (ret != 0) return ret;

  disable_codegen_ = false;
  enable_expr_rewrites_ = false;
  executor_->ClearExecOptions();
  executor_->PushExecOption("ENABLE_EXPR_REWRITES=0");
  executor_->PushExecOption("DISABLE_CODEGEN=0");
  executor_->PushExecOption("EXEC_SINGLE_NODE_ROWS_THRESHOLD=0");
  executor_->PushExecOption("DISABLE_CODEGEN_ROWS_THRESHOLD=0");
  cout << endl << "Running with codegen" << endl;
  ret = RUN_ALL_TESTS();
  if (ret != 0) return ret;

  // Enable FE expr rewrites to get test for constant folding over all exprs.
  disable_codegen_ = true;
  enable_expr_rewrites_ = true;
  executor_->ClearExecOptions();
  executor_->PushExecOption("ENABLE_EXPR_REWRITES=1");
  executor_->PushExecOption("DISABLE_CODEGEN=1");
  cout << endl << "Running without codegen and expr rewrites" << endl;
  return RUN_ALL_TESTS();
}
