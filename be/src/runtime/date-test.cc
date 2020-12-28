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

#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

#include "cctz/civil_time.h"
#include "common/status.h"
#include "runtime/datetime-simple-date-format-parser.h"
#include "runtime/date-value.h"
#include "runtime/raw-value.inline.h"
#include "runtime/timestamp-value.h"
#include "testutil/gtest-util.h"

#include "common/names.h"

using boost::gregorian::date;
using boost::posix_time::time_duration;

namespace impala {

using namespace datetime_parse_util;

inline void ValidateDate(const DateValue& dv, int exp_year, int exp_month, int exp_day,
    const string& desc) {
  int year, month, day;
  EXPECT_TRUE(dv.ToYearMonthDay(&year, &month, &day)) << desc;
  EXPECT_EQ(exp_year, year);
  EXPECT_EQ(exp_month, month);
  EXPECT_EQ(exp_day, day);
}

inline DateValue ParseValidateDate(const char* s, bool accept_time_toks, int exp_year,
    int exp_month, int exp_day) {
  DCHECK(s != nullptr);
  DateValue v = DateValue::ParseSimpleDateFormat(s, strlen(s), accept_time_toks);
  ValidateDate(v, exp_year, exp_month, exp_day, s);
  return v;
}

TEST(DateTest, ParseDefault) {
  // Parse with time tokens rejected.
  const DateValue v1 = ParseValidateDate("2012-01-20", false, 2012, 1, 20);
  const DateValue v2 = ParseValidateDate("1990-10-20", false, 1990, 10, 20);
  const DateValue v3 = ParseValidateDate("1990-10-20", false, 1990, 10, 20);
  // Parse with time tokens accepted.
  const DateValue v4 = ParseValidateDate("1990-10-20 23:59:59.999999999", true, 1990, 10,
      20);
  const DateValue v5 = ParseValidateDate("1990-10-20 00:01:02.9", true, 1990, 10, 20);

  // Test comparison operators.
  EXPECT_NE(v1, v2);
  EXPECT_EQ(v2, v3);
  EXPECT_LT(v2, v1);
  EXPECT_LE(v2, v1);
  EXPECT_GT(v1, v2);
  EXPECT_GE(v2, v3);

  // Time components are not part of the date value
  EXPECT_EQ(v3, v4);
  EXPECT_EQ(v3, v5);

  EXPECT_NE(RawValue::GetHashValue(&v1, ColumnType(TYPE_DATE), 0),
      RawValue::GetHashValue(&v2, ColumnType(TYPE_DATE), 0));
  EXPECT_EQ(RawValue::GetHashValue(&v3, ColumnType(TYPE_DATE), 0),
      RawValue::GetHashValue(&v2, ColumnType(TYPE_DATE), 0));

  // 1-digit months and days are ok in date string.
  ParseValidateDate("2012-1-20", false, 2012, 1, 20);
  ParseValidateDate("2012-9-8", false, 2012, 9, 8);
  // 1-digit hours/minutes/seconds are ok if time components are accepted.
  ParseValidateDate("2012-09-8 01:1:2.9", true, 2012, 9, 8);
  ParseValidateDate("2012-9-8 1:01:02", true, 2012, 9, 8);
  // Different fractional seconds are accepted
  ParseValidateDate("2012-09-8 01:01:2", true, 2012, 9, 8);
  ParseValidateDate("2012-09-8 01:01:2.9", true, 2012, 9, 8);
  ParseValidateDate("2012-09-8 01:01:02.9", true, 2012, 9, 8);
  ParseValidateDate("2012-09-8 01:01:2.999", true, 2012, 9, 8);
  ParseValidateDate("2012-09-8 01:01:02.999", true, 2012, 9, 8);
  ParseValidateDate("2012-09-8 01:01:2.999999999", true, 2012, 9, 8);
  ParseValidateDate("2012-09-8 01:01:02.999999999", true, 2012, 9, 8);

  // Bad formats: invalid date component.
  for (const char* s: {"1990-10", "1991-10-32", "1990-10-", "10:11:12 1991-10-10",
      "02011-01-01", "999-01-01", "2012-01-200", "2011-001-01"}) {
    EXPECT_FALSE(DateValue::ParseSimpleDateFormat(s, strlen(s), false).IsValid()) << s;
  }
  // Bad formats: valid date and time components but time component is rejected.
  for (const char* s: {"2012-01-20 10:11:12", "2012-1-2 10:11:12"}) {
    EXPECT_FALSE(DateValue::ParseSimpleDateFormat(s, strlen(s), false).IsValid()) << s;
  }
  // Bad formats: valid date component, invalid time component.
  for (const char* s: {"2012-01-20 10:11:", "2012-1-2 10::12", "2012-01-20 :11:12",
      "2012-01-20 24:11:12", "2012-01-20 23:60:12"}) {
    EXPECT_FALSE(DateValue::ParseSimpleDateFormat(s, strlen(s), true).IsValid()) << s;
  }
  // Bad formats: missing date component, valid time component.
  for (const char* s: {"10:11:12", "1:11:12", "10:1:12", "10:1:2.999"}) {
    EXPECT_FALSE(DateValue::ParseSimpleDateFormat(s, strlen(s), true).IsValid()) << s;
  }
}

// Used to represent a parsed date token. For example, it may represent a year.
struct DateToken {
  const char* fmt;
  int val;
  const char* month_name;

  DateToken(const char* fmt, int val)
    : fmt(fmt),
      val(val),
      month_name(nullptr) {
  }

  DateToken(const char* month_fmt, int month_val, const char* month_name)
    : fmt(month_fmt),
      val(month_val),
      month_name(month_name) {
  }

  friend bool operator<(const DateToken& lhs, const DateToken& rhs) {
    return strcmp(lhs.fmt, rhs.fmt) < 0;
  }
};

void TestDateTokens(const vector<DateToken>& toks, int year, int month, int day,
    const char* separator) {
  string fmt, val;
  for (int i = 0; i < toks.size(); ++i) {
    fmt.append(toks[i].fmt);
    if (separator != nullptr && i + 1 < toks.size()) fmt.push_back(*separator);

    if (toks[i].month_name != nullptr) {
      val.append(string(toks[i].month_name));
    } else {
      val.append(lexical_cast<string>(toks[i].val));
    }
    if (separator != nullptr && i + 1 < toks.size()) val.push_back(*separator);
  }

  string fmt_val = "Format: " + fmt + ", Val: " + val;
  DateTimeFormatContext dt_ctx(fmt.c_str());
  ASSERT_TRUE(SimpleDateFormatTokenizer::Tokenize(&dt_ctx, PARSE, false)) << fmt_val;
  DateValue dv = DateValue::ParseSimpleDateFormat(val.c_str(), val.length(), dt_ctx);
  ValidateDate(dv, year, month, day, fmt_val);

  string buff = dv.Format(dt_ctx);
  EXPECT_TRUE(!buff.empty()) << fmt_val;
  EXPECT_LE(buff.length(), dt_ctx.fmt_len) << fmt_val;
  EXPECT_EQ(buff, val) << fmt_val <<  " " << buff;
}

// This function will generate all permutations of tokens to test that the parsing and
// formatting is correct (position of tokens should be irrelevant). Note that separators
// are also combined with EACH token permutation to get the widest coverage on formats.
// This forces out the parsing and format logic edge cases.
void TestDateTokenPermutations(vector<DateToken>* toks, int year, int month, int day) {
  sort(toks->begin(), toks->end());

  const char* SEPARATORS = " ~!@%^&*_+-:;|\\,./";
  do {
    // Validate we can parse date raw tokens (no separators)
    TestDateTokens(*toks, year, month, day, nullptr);

    // Validate we can parse date with separators
    for (const char* separator = SEPARATORS; *separator != 0; ++separator) {
      TestDateTokens(*toks, year, month, day, separator);
    }
  } while (next_permutation(toks->begin(), toks->end()));
}

TEST(DateTest, ParseFormatCustomFormats) {
  // Test custom formats by generating all permutations of tokens to check parsing and
  // formatting is behaving correctly (position of tokens should be irrelevant). Note
  // that separators are also combined with EACH token permutation to get the widest
  // coverage on formats.
  const int YEAR = 2013;
  const int MONTH = 10;
  const int DAY = 14;
  // Test parsing/formatting with numeric date tokens
  vector<DateToken> dt_toks{
      DateToken("dd", DAY),
      DateToken("MM", MONTH),
      DateToken("yyyy", YEAR)};
  TestDateTokenPermutations(&dt_toks, YEAR, MONTH, DAY);
}

TEST(DateTest, ParseFormatLiteralMonths) {
  // Test literal months
  const char* months[] = { "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep",
      "Oct", "Nov", "Dec"
  };

  // Test parsing/formatting of literal months (short)
  const int MONTH_CNT = (sizeof(months) / sizeof(char**));

  const int YEAR = 2013;
  const int DAY = 14;
  for (int i = 0; i < MONTH_CNT; ++i) {
    // Test parsing/formatting with short literal months
    vector<DateToken> dt_lm_toks{
        DateToken("dd", DAY),
        DateToken("MMM", i + 1, months[i]),
        DateToken("yyyy", YEAR)};
    TestDateTokenPermutations(&dt_lm_toks, YEAR, i + 1, DAY);
  }
}

// Used for defining a custom date format test. The structure can be used to indicate
// whether the format or value is expected to fail. In a happy path test, the values for
// year, month, day will be validated against the parsed result.
// Further validation will also be performed if the should_format flag is enabled,
// whereby the parsed date will be translated back to a string and checked against the
// expected value.
struct DateTC {
  const char* fmt;
  const char* str;
  bool fmt_should_fail;
  bool str_should_fail;
  bool should_format;
  int expected_year;
  int expected_month;
  int expected_day;

  DateTC(const char* fmt, const char* str, bool fmt_should_fail = true,
      bool str_should_fail = true)
    : fmt(fmt),
      str(str),
      fmt_should_fail(fmt_should_fail),
      str_should_fail(str_should_fail),
      should_format(true),
      expected_year(0),
      expected_month(0),
      expected_day(0) {
  }

  DateTC(const char* fmt, const char* str, bool should_format,
      int expected_year, int expected_month, int expected_day)
    : fmt(fmt),
      str(str),
      fmt_should_fail(false),
      str_should_fail(false),
      should_format(should_format),
      expected_year(expected_year),
      expected_month(expected_month),
      expected_day(expected_day) {
  }

  void Run(int id, const TimestampValue& now) const {
    DateTimeFormatContext dt_ctx(fmt);
    dt_ctx.SetCenturyBreakAndCurrentTime(now);

    stringstream desc;
    desc << "DateTC [" << id << "]: " << " fmt:" << fmt << " str:" << str
      << " expected date:" << expected_year << "/" << expected_month << "/"
      << expected_day;

    bool parse_result = SimpleDateFormatTokenizer::Tokenize(&dt_ctx, PARSE, false);
    if (fmt_should_fail) {
      EXPECT_FALSE(parse_result) << desc.str();
      return;
    } else {
      ASSERT_TRUE(parse_result) << desc.str();
    }

    DateValue cust_dv = DateValue::ParseSimpleDateFormat(str, strlen(str), dt_ctx);
    if (str_should_fail) {
      EXPECT_FALSE(cust_dv.IsValid()) << desc.str();
      return;
    }

    // Check the date (based on any date format tokens being present)
    ValidateDate(cust_dv, expected_year, expected_month, expected_day, desc.str());

    // Check formatted date
    if (!should_format) return;

    string buff = cust_dv.Format(dt_ctx);
    EXPECT_TRUE(!buff.empty()) << desc.str();
    EXPECT_LE(buff.length(), dt_ctx.fmt_len) << desc.str();
    EXPECT_EQ(string(str, strlen(str)), buff) << desc.str();
  }
};

TEST(DateTest, ParseFormatEdgeCases) {
  const TimestampValue now(date(1980, 2, 28), time_duration(16, 14, 24));

  vector<DateTC> test_cases{
      // Test year lower/upper bound
      DateTC("yyyy-MM-dd", "0001-01-01", true, 1, 1, 1),
      DateTC("yyyy-MM-dd", "0000-01-01", false, true),
      DateTC("yyyy-MM-dd", "-001-01-01", false, true),
      DateTC("yyyy-MM-dd", "9999-12-31", true, 9999, 12, 31),
      DateTC("yyyyy-MM-dd", "10000-12-31", false, true),
      // Test Feb 29 in leap years
      DateTC("yyyy-MM-dd", "0004-02-29", true, 4, 2, 29),
      DateTC("yyyy-MM-dd", "1904-02-29", true, 1904, 2, 29),
      DateTC("yyyy-MM-dd", "2000-02-29", true, 2000, 2, 29),
      // Test Feb 29 in non-leap years
      DateTC("yyyy-MM-dd", "0001-02-29", false, true),
      DateTC("yyyy-MM-dd", "1900-02-29", false, true),
      DateTC("yyyy-MM-dd", "1999-02-29", false, true)};

  for (int i = 0; i < test_cases.size(); ++i) {
    test_cases[i].Run(i, now);
  }
}

TEST(DateTest, ParseFormatSmallYear) {
  // Fix current time to determine the behavior parsing 2-digit year format.
  const TimestampValue now(date(1980, 2, 28), time_duration(16, 14, 24));

  // Test year < 1000
  vector<DateTC> test_cases{
      DateTC("yyyy-MM-dd", "0999-10-31", true, 999, 10, 31),
      DateTC("yyyy-MM-dd", "0099-10-31", true, 99, 10, 31),
      DateTC("yyyy-MM-dd", "0009-10-31", true, 9, 10, 31),
      // Format token yyy works when parsing years < 1000.
      // On the other hand when yyy is used for formatting years, modulo 100 will be
      // applied
      DateTC("yyy-MM-dd", "999-10-31", false, 999, 10, 31),
      DateTC("yyy-MM-dd", "099-10-31", true, 99, 10, 31),
      DateTC("yyy-MM-dd", "009-10-31", true, 9, 10, 31),
      // Year is aligned when yy format token is used and we have a 2-difgit year. 3-digit
      // years are not parsed correctly.
      DateTC("yy-MM-dd", "999-10-31", false, true),
      DateTC("yy-MM-dd", "99-10-31", true, 1999, 10, 31),
      DateTC("yy-MM-dd", "09-10-31", true, 1909, 10, 31),
      // Year is aligned when y format token is used and we have a 2-digit year.
      DateTC("y-MM-dd", "999-10-31", false, 999, 10, 31),
      DateTC("y-MM-dd", "99-10-31", false, 1999, 10, 31),
      DateTC("y-MM-dd", "09-10-31", false, 1909, 10, 31),
      DateTC("y-MM-dd", "9-10-31", false, 1909, 10, 31)};

  for (int i = 0; i < test_cases.size(); ++i) {
    test_cases[i].Run(i, now);
  }
}

TEST(DateTest, ParseFormatAlignedYear) {
  // Fix current time to determine the behavior parsing 2-digit year format.
  // Set it to 02/28 to test 02/29 edge cases.
  // The corresponding century break will be 1900-02-28.
  const TimestampValue now(date(1980, 2, 28), time_duration(16, 14, 24));

  // Test year alignment for 1- and 2-digit year format.
  vector<DateTC> test_cases{
      // Test 2-digit year format
      DateTC("yy-MM-dd", "17-08-31", true, 1917, 8, 31),
      DateTC("yy-MM-dd", "99-08-31", true, 1999, 8, 31),
      // Test 02/29 edge cases of 2-digit year format
      DateTC("yy-MM-dd", "00-02-28", true, 2000, 2, 28),
      // After the cutoff year is 1900, but 1900/02/29 is invalid
      DateTC("yy-MM-dd", "00-02-29", false, true),
      // After the cutoff year is 1900
      DateTC("yy-MM-dd", "00-03-01", true, 1900, 3, 1),
      DateTC("yy-MM-dd", "04-02-29", true, 1904, 2, 29),
      DateTC("yy-MM-dd", "99-02-29", false, true),
      // Test 1-digit year format with time to show the exact boundary
      // Before the cutoff, year should be 2000
      DateTC("y-MM-dd", "00-02-28", false, 2000, 2, 28),
      // After the cutoff year is 1900, but 1900/02/29 is invalid
      DateTC("y-MM-dd", "00-02-29", false, true),
      // After the cutoff year is 1900.
      DateTC("y-MM-dd", "00-03-01", false, 1900, 3, 1)};

  for (int i = 0; i < test_cases.size(); ++i) {
    test_cases[i].Run(i, now);
  }

  // Test year realignment with a different 'now' timestamp.
  // This time the corresponding century break will be 1938-09-25.
  const TimestampValue now2(date(2018, 9, 25), time_duration(16, 14, 24));

  vector<DateTC> test_cases2{
      // Before the cutoff, year is 2004.
      DateTC("yy-MM-dd", "04-02-29", true, 2004, 2, 29),
      // Still before the cutoff, year is 2038.
      DateTC("yy-MM-dd", "38-09-25", true, 2038, 9, 25),
      // After the cutoff, year is 1938.
      DateTC("yy-MM-dd", "38-09-26", true, 1938, 9, 26),
      // Test parsing again with 'y' format token.
      DateTC("y-MM-dd", "04-02-29", false, 2004, 2, 29),
      DateTC("y-MM-dd", "38-09-25", false, 2038, 9, 25),
      DateTC("y-MM-dd", "38-09-26", false, 1938, 9, 26)};

  for (int i = 0; i < test_cases2.size(); ++i) {
    test_cases2[i].Run(i + test_cases.size(), now2);
  }
}

TEST(DateTest, ParseFormatComplexFormats) {
  const TimestampValue now(date(1980, 2, 28), time_duration(16, 14, 24));

  // Test parsing/formatting of complex date formats
  vector<DateTC> test_cases{
      // Test case on literal short months
      DateTC("yyyy-MMM-dd", "2013-OCT-01", false, 2013, 10, 1),
      // Test case on literal short months
      DateTC("yyyy-MMM-dd", "2013-oct-01", false, 2013, 10, 1),
      // Test case on literal short months
      DateTC("yyyy-MMM-dd", "2013-oCt-01", false, 2013, 10, 1),
      // Test padding on numeric and literal tokens (short,
      DateTC("MMMyyyyyydd", "Apr00201309", true, 2013, 4, 9),
      // Test duplicate tokens
      DateTC("yyyy MM dd ddMMMyyyy", "2013 05 12 16Apr1952", false, 1952, 4, 16),
      // Test missing separator on short date format
      DateTC("Myyd", "4139", true, true),
      // Test bad year format
      DateTC("YYYYmmdd", "20131001"),
      // Test unknown formatting character
      DateTC("yyyyUUdd", "2013001001"),
      // Test that T|Z markers and time tokens are rejected
      DateTC("yyyy-MM-ddT", "2013-11-12T"),
      DateTC("yyyy-MM-ddZ", "2013-11-12Z"),
      DateTC("yyyy-MM-dd HH:mm:ss", "2013-11-12 12:23:36"),
      DateTC("HH:mm:ss", "12:23:36"),
      // Test numeric formatting character
      DateTC("yyyyMM1dd", "201301111"),
      // Test out of range year
      DateTC("yyyyyMMdd", "120130101", false, true),
      // Test out of range month
      DateTC("yyyyMMdd", "20131301", false, true),
      // Test out of range month
      DateTC("yyyyMMdd", "20130001", false, true),
      // Test out of range day
      DateTC("yyyyMMdd", "20130132", false, true),
      // Test out of range day
      DateTC("yyyyMMdd", "20130100", false, true),
      // Test characters where numbers should be
      DateTC("yyyyMMdd", "201301aa", false, true),
      // Test missing year
      DateTC("MMdd", "1201", false, true),
      // Test missing month
      DateTC("yyyydd", "201301", false, true),
      DateTC("yydd", "1301", false, true),
      // Test missing day
      DateTC("yyyyMM", "201301", false, true),
      DateTC("yyMM", "8512", false, true),
      // Test missing month and day
      DateTC("yyyy", "2013", false, true),
      DateTC("yy", "13", false, true),
      // Test short year token
      DateTC("y-MM-dd", "2013-11-13", false, 2013, 11, 13),
      DateTC("y-MM-dd", "13-11-13", false, 1913, 11, 13),
      // Test short month token
      DateTC("yyyy-M-dd", "2013-11-13", false, 2013, 11, 13),
      DateTC("yyyy-M-dd", "2013-1-13", false, 2013, 1, 13),
      // Test short day token
      DateTC("yyyy-MM-d", "2013-11-13", false, 2013, 11, 13),
      DateTC("yyyy-MM-d", "2013-11-3", false, 2013, 11, 3),
      // Test short all date tokens
      DateTC("y-M-d", "2013-11-13", false, 2013, 11, 13),
      DateTC("y-M-d", "13-1-3", false, 1913, 1, 3)};

  // Loop through custom parse/format test cases and execute each one. Each test case
  // will be explicitly set with a pass/fail expectation related to either the format
  // or literal value.
  for (int i = 0; i < test_cases.size(); ++i) {
    test_cases[i].Run(i, now);
  }
}

// Used to test custom date output test cases i.e. date value -> string.
struct DateFormatTC {
  const int32_t days_since_epoch;
  const char* fmt;
  const char* str;
  bool should_fail;

  DateFormatTC(int32_t days_since_epoch, const char* fmt, const char* str,
      bool should_fail = false)
    : days_since_epoch(days_since_epoch),
      fmt(fmt),
      str(str),
      should_fail(should_fail) {
  }

  void Run(int id, const TimestampValue& now) const {
    DateTimeFormatContext dt_ctx(fmt);
    dt_ctx.SetCenturyBreakAndCurrentTime(now);

    stringstream desc;
    desc << "DateFormatTC [" << id << "]: " << "days_since_epoch:" << days_since_epoch
         << " fmt:" << fmt << " str:" << str;

    ASSERT_TRUE(SimpleDateFormatTokenizer::Tokenize(&dt_ctx, PARSE, false)) << desc.str();

    const DateValue cust_dv(days_since_epoch);
    EXPECT_TRUE(cust_dv.IsValid()) << desc.str();
    EXPECT_GE(dt_ctx.fmt_out_len, dt_ctx.fmt_len) << desc.str();

    string buff = cust_dv.Format(dt_ctx);
    EXPECT_TRUE(!buff.empty()) << desc.str();
    EXPECT_LE(buff.length(), dt_ctx.fmt_out_len) << desc.str();
    EXPECT_EQ(buff, string(str, strlen(str))) << desc.str();
  }

};

TEST(DateTest, FormatComplexFormats) {
  const TimestampValue now(date(1980, 2, 28), time_duration(16, 14, 24));

  // Test complex formatting of dates
  vector<DateFormatTC> fmt_test_cases{
      // Test just formatting date tokens
      DateFormatTC(11178, "yyyy-MM-dd", "2000-08-09"),
      // Test short form date tokens
      DateFormatTC(11178, "yyyy-M-d", "2000-8-9"),
      // Test short form tokens on wide dates
      DateFormatTC(15999, "d", "21"),
      // Test month expansion
      DateFormatTC(11178, "MMM/MM/M", "Aug/08/8"),
      // Test padding on single digits
      DateFormatTC(11178, "dddddd/dd/d", "000009/09/9"),
      // Test padding on double digits
      DateFormatTC(15999, "dddddd/dd/dd", "000021/21/21")};

  // Loop through format test cases
  for (int i = 0; i < fmt_test_cases.size(); ++i) {
    fmt_test_cases[i].Run(i, now);
  }
}

TEST(DateTest, DateValueEdgeCases) {
  // Test min supported date.
  // MIN_DATE_DAYS_SINCE_EPOCH was calculated using the Proleptic Gregorian calendar. This
  // is expected to be different then how Hive2 written Parquet files represent
  // 0001-01-01.
  const int32_t MIN_DATE_DAYS_SINCE_EPOCH = -719162;
  const DateValue min_date1 = ParseValidateDate("0001-01-01", true, 1, 1, 1);
  const DateValue min_date2 = ParseValidateDate("0001-01-01 00:00:00", true, 1, 1, 1);
  EXPECT_EQ(min_date1, min_date2);
  int32_t min_days;
  EXPECT_TRUE(min_date1.ToDaysSinceEpoch(&min_days));
  EXPECT_EQ(MIN_DATE_DAYS_SINCE_EPOCH, min_days);
  EXPECT_EQ("0001-01-01", min_date1.ToString());
  EXPECT_EQ("0001-01-01", min_date2.ToString());

  const DateValue min_date3(MIN_DATE_DAYS_SINCE_EPOCH);
  EXPECT_TRUE(min_date3.IsValid());
  EXPECT_EQ(min_date1, min_date3);

  const DateValue too_early(MIN_DATE_DAYS_SINCE_EPOCH - 1);
  EXPECT_FALSE(too_early.IsValid());

  // Test max supported date.
  const int32_t MAX_DATE_DAYS_SINCE_EPOCH = 2932896;
  const DateValue max_date1 = ParseValidateDate("9999-12-31", true, 9999, 12, 31);
  const DateValue max_date2 = ParseValidateDate("9999-12-31 23:59:59.999999999", true,
      9999, 12, 31);
  EXPECT_EQ(max_date1, max_date2);
  int32_t max_days;
  EXPECT_TRUE(max_date1.ToDaysSinceEpoch(&max_days));
  EXPECT_EQ(MAX_DATE_DAYS_SINCE_EPOCH, max_days);
  EXPECT_EQ("9999-12-31", max_date1.ToString());
  EXPECT_EQ("9999-12-31", max_date2.ToString());

  const DateValue max_date3(MAX_DATE_DAYS_SINCE_EPOCH);
  EXPECT_TRUE(max_date3.IsValid());
  EXPECT_EQ(max_date1, max_date3);

  const DateValue too_late(MAX_DATE_DAYS_SINCE_EPOCH + 1);
  EXPECT_FALSE(too_late.IsValid());

  // Test that Feb 29 is valid in leap years.
  for (int leap_year: {4, 1904, 1980, 1996, 2000, 2004, 2104, 9996}) {
    EXPECT_TRUE(DateValue(leap_year, 2, 29).IsValid()) << "year:" << leap_year;
  }

  // Test that Feb 29 is invalid in non-leap years.
  for (int non_leap_year: {1, 1900, 1981, 1999, 2001, 2100, 9999}) {
    EXPECT_TRUE(DateValue(non_leap_year, 2, 28).IsValid()) << "year:" << non_leap_year;
    EXPECT_FALSE(DateValue(non_leap_year, 2, 29).IsValid()) << "year:" << non_leap_year;
    EXPECT_TRUE(DateValue(non_leap_year, 3, 1).IsValid()) << "year:" << non_leap_year;
  }
}

TEST(DateTest, AddDays) {
  // Adding days to an invalid DateValue instance returns an invalid DateValue.
  DateValue invalid_dv;
  EXPECT_FALSE(invalid_dv.IsValid());
  EXPECT_FALSE(invalid_dv.AddDays(1).IsValid());

  // AddDays works with 0, > 0 and < 0 number of days.
  DateValue dv(2019, 5, 16);
  EXPECT_EQ(DateValue(2019, 5, 17), dv.AddDays(1));
  EXPECT_EQ(DateValue(2019, 5, 15), dv.AddDays(-1));
  // May has 31 days, April has 30 days.
  EXPECT_EQ(DateValue(2019, 6, 16), dv.AddDays(31));
  EXPECT_EQ(DateValue(2019, 4, 16), dv.AddDays(-30));
  // 2019 is not a leap year, 2020 is a leap year.
  EXPECT_EQ(DateValue(2020, 5, 16), dv.AddDays(366));
  EXPECT_EQ(DateValue(2018, 5, 16), dv.AddDays(-365));

  // Test upper limit.
  dv = DateValue(9999, 12, 20);
  EXPECT_EQ(DateValue(9999, 12, 31), dv.AddDays(11));
  EXPECT_FALSE(dv.AddDays(12).IsValid());
  EXPECT_FALSE(dv.AddDays(13).IsValid());

  // Test lower limit.
  dv = DateValue(1, 1, 10);
  EXPECT_EQ(DateValue(1, 1, 1), dv.AddDays(-9));
  EXPECT_FALSE(dv.AddDays(-10).IsValid());
  EXPECT_FALSE(dv.AddDays(-11).IsValid());

  // Test adding days to cover the entire range.
  int32_t min_dse, max_dse;
  EXPECT_TRUE(DateValue(1, 1, 1).ToDaysSinceEpoch(&min_dse));
  EXPECT_GT(0, min_dse);
  min_dse = -min_dse;
  EXPECT_TRUE(DateValue(9999, 12, 31).ToDaysSinceEpoch(&max_dse));
  EXPECT_LT(0, max_dse);

  dv = DateValue(1, 1, 1);
  EXPECT_EQ(DateValue(9999, 12, 31), dv.AddDays(min_dse + max_dse));
  EXPECT_FALSE(dv.AddDays(min_dse + max_dse + 1).IsValid());
  EXPECT_FALSE(dv.AddDays(std::numeric_limits<int64_t>::max()).IsValid());

  dv = DateValue(9999, 12, 31);
  EXPECT_EQ(DateValue(1, 1, 1), dv.AddDays(-(min_dse + max_dse)));
  EXPECT_FALSE(dv.AddDays(-(min_dse + max_dse + 1)).IsValid());
  EXPECT_FALSE(dv.AddDays(std::numeric_limits<int64_t>::min()).IsValid());

  // Test leap year.
  dv = DateValue(2000, 2, 20);
  EXPECT_EQ(DateValue(2000, 2, 28), dv.AddDays(8));
  EXPECT_EQ(DateValue(2000, 2, 29), dv.AddDays(9));
  EXPECT_EQ(DateValue(2000, 3, 1), dv.AddDays(10));

  // Test non-leap year.
  dv = DateValue(2001, 2, 20);
  EXPECT_EQ(DateValue(2001, 2, 28), dv.AddDays(8));
  EXPECT_EQ(DateValue(2001, 3, 1), dv.AddDays(9));
}

TEST(DateTest, AddMonths) {
  // Adding days to an invalid DateValue instance returns an invalid DateValue.
  DateValue invalid_dv;
  EXPECT_FALSE(invalid_dv.IsValid());
  EXPECT_FALSE(invalid_dv.AddMonths(1, true).IsValid());

  // AddMonths works with 0, > 0 and < 0 number of months.
  DateValue dv(2019, 5, 16);
  EXPECT_EQ(DateValue(2019, 6, 16), dv.AddMonths(1, true));
  EXPECT_EQ(DateValue(2019, 4, 16), dv.AddMonths(-1, true));

  // Test that result dates are always capped at the end of the month regardless of
  // whether 'keep_last_day' is set or not.
  dv = DateValue(2019, 5, 31);
  EXPECT_EQ(DateValue(2019, 6, 30), dv.AddMonths(1, true));
  EXPECT_EQ(DateValue(2019, 7, 31), dv.AddMonths(2, true));
  EXPECT_EQ(DateValue(2020, 2, 29), dv.AddMonths(9, true));
  EXPECT_EQ(DateValue(2019, 6, 30), dv.AddMonths(1, false));
  EXPECT_EQ(DateValue(2019, 7, 31), dv.AddMonths(2, false));
  EXPECT_EQ(DateValue(2020, 2, 29), dv.AddMonths(9, false));

  // Test that resulting date falls on the last day iff 'keep_last_day' is set.
  dv = DateValue(1999, 2, 28);
  EXPECT_EQ(DateValue(1999, 3, 31), dv.AddMonths(1, true));
  EXPECT_EQ(DateValue(1999, 4, 30), dv.AddMonths(2, true));
  EXPECT_EQ(DateValue(2000, 2, 29), dv.AddMonths(12, true));
  EXPECT_EQ(DateValue(1999, 3, 28), dv.AddMonths(1, false));
  EXPECT_EQ(DateValue(1999, 4, 28), dv.AddMonths(2, false));
  EXPECT_EQ(DateValue(2000, 2, 28), dv.AddMonths(12, false));

  // Test that leap year is handled correctly.
  dv = DateValue(2016, 2, 29);
  EXPECT_EQ(DateValue(2016, 3, 31), dv.AddMonths(1, true));
  EXPECT_EQ(DateValue(2016, 4, 30), dv.AddMonths(2, true));
  EXPECT_EQ(DateValue(2017, 2, 28), dv.AddMonths(12, true));
  EXPECT_EQ(DateValue(2016, 3, 29), dv.AddMonths(1, false));
  EXPECT_EQ(DateValue(2016, 4, 29), dv.AddMonths(2, false));
  EXPECT_EQ(DateValue(2017, 2, 28), dv.AddMonths(12, false));

  // Test upper limit.
  dv = DateValue(9998, 11, 30);
  EXPECT_EQ(DateValue(9999, 12, 31), dv.AddMonths(13, true));
  EXPECT_FALSE(dv.AddMonths(14, true).IsValid());

  // Test lower limit.
  dv = DateValue(1, 11, 30);
  EXPECT_EQ(DateValue(1, 1, 31), dv.AddMonths(-10, true));
  EXPECT_FALSE(dv.AddMonths(-11, true).IsValid());

  // Test adding months to cover the entire range.
  dv = DateValue(1, 1, 1);
  EXPECT_EQ(DateValue(9999, 12, 1), dv.AddMonths(9998 * 12 + 11, false));
  EXPECT_FALSE(dv.AddMonths(9998 * 12 + 12, false).IsValid());
  EXPECT_FALSE(dv.AddMonths(std::numeric_limits<int64_t>::max(), false).IsValid());

  dv = DateValue(9999, 12, 31);
  EXPECT_EQ(DateValue(1, 1, 31), dv.AddMonths(-9998 * 12 - 11, false));
  EXPECT_FALSE(dv.AddMonths(-9998 * 12 - 12, false).IsValid());
  EXPECT_FALSE(dv.AddMonths(std::numeric_limits<int64_t>::min(), false).IsValid());
}

TEST(DateTest, AddYears) {
  // Adding years to an invalid DateValue instance returns an invalid DateValue.
  DateValue invalid_dv;
  EXPECT_FALSE(invalid_dv.IsValid());
  EXPECT_FALSE(invalid_dv.AddYears(1).IsValid());

  // AddYears works with 0, > 0 and < 0 number of days.
  DateValue dv(2019, 5, 16);
  EXPECT_EQ(DateValue(2020, 5, 16), dv.AddYears(1));
  EXPECT_EQ(DateValue(2018, 5, 16), dv.AddYears(-1));

  // Test upper limit.
  dv = DateValue(9990, 12, 31);
  EXPECT_EQ(DateValue(9999, 12, 31), dv.AddYears(9));
  EXPECT_FALSE(dv.AddYears(10).IsValid());
  EXPECT_FALSE(dv.AddYears(11).IsValid());

  // Test lower limit.
  dv = DateValue(11, 1, 1);
  EXPECT_EQ(DateValue(1, 1, 1), dv.AddYears(-10));
  EXPECT_FALSE(dv.AddYears(-11).IsValid());
  EXPECT_FALSE(dv.AddYears(-12).IsValid());

  // Test adding years to cover the entire range.
  dv = DateValue(1, 1, 1);
  EXPECT_EQ(DateValue(9999, 1, 1), dv.AddYears(9998));
  EXPECT_FALSE(dv.AddYears(9998 + 1).IsValid());
  EXPECT_FALSE(dv.AddYears(std::numeric_limits<int64_t>::max()).IsValid());

  dv = DateValue(9999, 12, 31);
  EXPECT_EQ(DateValue(1, 12, 31), dv.AddYears(-9998));
  EXPECT_FALSE(dv.AddYears(-9998 - 1).IsValid());
  EXPECT_FALSE(dv.AddYears(std::numeric_limits<int64_t>::min()).IsValid());

  // Test leap year.
  dv = DateValue(2000, 2, 29);
  EXPECT_EQ(DateValue(2001, 2, 28), dv.AddYears(1));
  EXPECT_EQ(DateValue(2002, 2, 28), dv.AddYears(2));
  EXPECT_EQ(DateValue(2003, 2, 28), dv.AddYears(3));
  EXPECT_EQ(DateValue(2004, 2, 29), dv.AddYears(4));
}

TEST(DateTest, WeekDay) {
  // WeekDay() returns -1 for invalid dates.
  DateValue invalid_dv;
  EXPECT_FALSE(invalid_dv.IsValid());
  EXPECT_EQ(-1, invalid_dv.WeekDay());

  // 2019-05-01 is Wednesday.
  DateValue dv(2019, 5, 1);
  for (int i = 0; i <= 31; ++i) {
    // 0 = Monday, 2 = Wednesday and 6 = Sunday.
    EXPECT_EQ((i + 2) % 7, dv.AddDays(i).WeekDay());
  }

  // Test upper limit. 9999-12-31 is Friday.
  EXPECT_EQ(4, DateValue(9999, 12, 31).WeekDay());

  // Test lower limit.
  // 0001-01-01 is Monday.
  EXPECT_EQ(0, DateValue(1, 1, 1).WeekDay());
  // 0002-01-01 is Tuesday.
  EXPECT_EQ(1, DateValue(2, 1, 1).WeekDay());
}

TEST(DateTest, ToYearMonthDay) {
  // Test that ToYearMonthDay() and ToYear() return false for invalid dates.
  DateValue invalid_dv;
  EXPECT_FALSE(invalid_dv.IsValid());
  int y1, m1, d1;
  EXPECT_FALSE(invalid_dv.ToYearMonthDay(&y1, &m1, &d1));
  int y2;
  EXPECT_FALSE(invalid_dv.ToYear(&y2));

  // Test that ToYearMonthDay() and ToYear() return the same values as
  // cctz::civil_day::year()/month()/day().
  // The following loop iterates through all valid dates (0001-01-01..9999-12-31):
  cctz::civil_day epoch(1970, 1, 1);
  cctz::civil_day cd(1, 1, 1);
  do {
    DateValue dv(cd - epoch);
    EXPECT_TRUE(dv.IsValid());

    EXPECT_TRUE(dv.ToYearMonthDay(&y1, &m1, &d1));
    EXPECT_EQ(cd.year(), y1);
    EXPECT_EQ(cd.month(), m1);
    EXPECT_EQ(cd.day(), d1);

    EXPECT_TRUE(dv.ToYear(&y2));
    EXPECT_EQ(cd.year(), y2);

    cd++;
  } while (cd.year() < 10000);
}

TEST(DateTest, DayOfYear) {
  DateValue invalid_dv;
  EXPECT_EQ(-1, invalid_dv.DayOfYear());

  // Test lower limit.
  EXPECT_EQ(1, DateValue(1, 1, 1).DayOfYear());
  // Test upper limit.
  EXPECT_EQ(365, DateValue(9999, 12,31).DayOfYear());

  // Test leap year.
  EXPECT_EQ(1, DateValue(2000, 1, 1).DayOfYear());
  EXPECT_EQ(31, DateValue(2000, 1, 31).DayOfYear());
  EXPECT_EQ(32, DateValue(2000, 2, 1).DayOfYear());
  EXPECT_EQ(59, DateValue(2000, 2, 28).DayOfYear());
  EXPECT_EQ(60, DateValue(2000, 2, 29).DayOfYear());
  EXPECT_EQ(61, DateValue(2000, 3, 1).DayOfYear());
  EXPECT_EQ(366, DateValue(2000, 12, 31).DayOfYear());
}

TEST(DateTest, Iso8601WeekOfYear) {
  // Test that it returns -1 for invalid dates.
  DateValue invalid_dv;
  EXPECT_EQ(-1, invalid_dv.Iso8601WeekOfYear());

  // Iterate through days of 2019.
  // 2019-01-01 is Tuesday and 2019-12-31 is Tuesday too.
  DateValue jan1(2019, 1, 1);
  int weekday_offset = 1;
  for (DateValue dv = jan1;
      dv <= DateValue(2019, 12, 29);
      dv = dv.AddDays(1)) {
    EXPECT_EQ(weekday_offset / 7 + 1, dv.Iso8601WeekOfYear());
    ++weekday_offset;
  }

  // Year 2015 has 53 weeks. 2015-12-31 is Thursday.
  EXPECT_EQ(53, DateValue(2015, 12, 31).Iso8601WeekOfYear());

  // 2019-12-30 (Monday) and 2019-12-31 (Tuesday) belong to year 2020.
  EXPECT_EQ(1, DateValue(2019, 12, 30).Iso8601WeekOfYear());
  EXPECT_EQ(1, DateValue(2019, 12, 31).Iso8601WeekOfYear());
  EXPECT_EQ(1, DateValue(2020, 1, 1).Iso8601WeekOfYear());
  EXPECT_EQ(1, DateValue(2020, 1, 5).Iso8601WeekOfYear());
  EXPECT_EQ(2, DateValue(2020, 1, 6).Iso8601WeekOfYear());

  // 0002-01-01 is Tuesday. Test days around 0002-01-01.
  EXPECT_EQ(51, DateValue(1, 12, 23).Iso8601WeekOfYear());
  EXPECT_EQ(52, DateValue(1, 12, 30).Iso8601WeekOfYear());
  EXPECT_EQ(1, DateValue(1, 12, 31).Iso8601WeekOfYear());
  EXPECT_EQ(1, DateValue(2, 1, 1).Iso8601WeekOfYear());
  EXPECT_EQ(1, DateValue(2, 1, 6).Iso8601WeekOfYear());
  EXPECT_EQ(2, DateValue(2, 1, 7).Iso8601WeekOfYear());
  // 0001-01-01 is Monday. Test days around 0001-01-01.
  EXPECT_EQ(1, DateValue(1, 1, 1).Iso8601WeekOfYear());
  EXPECT_EQ(1, DateValue(1, 1, 2).Iso8601WeekOfYear());
  EXPECT_EQ(2, DateValue(1, 1, 8).Iso8601WeekOfYear());

  // 9999-12-31 is Friday. Test days around 9999-12-31.
  EXPECT_EQ(52, DateValue(9999, 12, 31).Iso8601WeekOfYear());
  EXPECT_EQ(52, DateValue(9999, 12, 27).Iso8601WeekOfYear());
  EXPECT_EQ(51, DateValue(9999, 12, 26).Iso8601WeekOfYear());
}

TEST(DateTest, Iso8601WeekNumberingYear) {
  // Test that it returns -1 for invalid dates.
  DateValue invalid_dv;
  EXPECT_EQ(-1, invalid_dv.Iso8601WeekNumberingYear());

  // Iterate through days of 2019.
  // 2019-01-01 is Tuesday and 2019-12-29 is Sunday.
  DateValue jan1(2019, 1, 1);
  for (DateValue dv = jan1;
      dv <= DateValue(2019, 12, 29);
      dv = dv.AddDays(1)) {
    EXPECT_EQ(2019, dv.Iso8601WeekNumberingYear());
  }
  // 2019-12-30 (Monday) and 2019-12-31 (Tuesday) belong to year 2020.
  EXPECT_EQ(2020, DateValue(2019, 12, 30).Iso8601WeekNumberingYear());
  EXPECT_EQ(2020, DateValue(2019, 12, 31).Iso8601WeekNumberingYear());
  EXPECT_EQ(2020, DateValue(2020, 1, 1).Iso8601WeekNumberingYear());

  // 2015-01-01 is Thursday and 2015-12-31 is Thursday too.
  // Both days belong to year 2015.
  EXPECT_EQ(2015, DateValue(2015, 1, 1).Iso8601WeekNumberingYear());
  EXPECT_EQ(2015, DateValue(2015, 12, 31).Iso8601WeekNumberingYear());

  // 2040-01-01 is Sunday and 2040-12-31 is Monday.
  // Neither days belong to year 2040.
  EXPECT_EQ(2039, DateValue(2040, 1, 1).Iso8601WeekNumberingYear());
  EXPECT_EQ(2041, DateValue(2040, 12, 31).Iso8601WeekNumberingYear());

  // 0002-01-01 is Tuesday. Test days around 0002-01-01.
  EXPECT_EQ(1, DateValue(1, 12, 29).Iso8601WeekNumberingYear());
  EXPECT_EQ(1, DateValue(1, 12, 30).Iso8601WeekNumberingYear());
  EXPECT_EQ(2, DateValue(1, 12, 31).Iso8601WeekNumberingYear());
  EXPECT_EQ(2, DateValue(2, 1, 1).Iso8601WeekNumberingYear());
  EXPECT_EQ(2, DateValue(2, 1, 2).Iso8601WeekNumberingYear());
  // 0001-01-01 is Monday. Test days around 0001-01-01.
  EXPECT_EQ(1, DateValue(1, 1, 1).Iso8601WeekNumberingYear());
  EXPECT_EQ(1, DateValue(1, 1, 2).Iso8601WeekNumberingYear());

  // 9999-12-31 is Friday. Test days around 9999-12-31.
  EXPECT_EQ(9999, DateValue(9999, 12, 30).Iso8601WeekNumberingYear());
  EXPECT_EQ(9999, DateValue(9999, 12, 31).Iso8601WeekNumberingYear());
}

TEST(DateTest, CreateFromIso8601WeekBasedDateVals) {
  // Invalid week numbering year.
  EXPECT_FALSE(DateValue::CreateFromIso8601WeekBasedDateVals(-1, 1, 1).IsValid());
  EXPECT_FALSE(DateValue::CreateFromIso8601WeekBasedDateVals(0, 1, 1).IsValid());
  EXPECT_FALSE(DateValue::CreateFromIso8601WeekBasedDateVals(10000, 1, 1).IsValid());

  // Test invalid week of year.
  // Year 2020 has 53 weeks.
  EXPECT_FALSE(DateValue::CreateFromIso8601WeekBasedDateVals(2020, 54, 1).IsValid());
  EXPECT_FALSE(DateValue::CreateFromIso8601WeekBasedDateVals(2020, 0, 1).IsValid());
  // Year 2019 has 52 weeks.
  EXPECT_FALSE(DateValue::CreateFromIso8601WeekBasedDateVals(2019, 53, 1).IsValid());

  // Test invalid week day.
  EXPECT_FALSE(DateValue::CreateFromIso8601WeekBasedDateVals(2020, 1, 0).IsValid());
  EXPECT_FALSE(DateValue::CreateFromIso8601WeekBasedDateVals(2020, 1, 8).IsValid());

  // 0001-01-01 is Monday. It belongs to the first week of year 1.
  // Test days around 0001-01-01.
  EXPECT_EQ(DateValue(1, 1, 1), DateValue::CreateFromIso8601WeekBasedDateVals(1, 1, 1));
  EXPECT_EQ(DateValue(1, 1, 2), DateValue::CreateFromIso8601WeekBasedDateVals(1, 1, 2));
  EXPECT_EQ(DateValue(1, 1, 7), DateValue::CreateFromIso8601WeekBasedDateVals(1, 1, 7));
  EXPECT_EQ(DateValue(1, 1, 8), DateValue::CreateFromIso8601WeekBasedDateVals(1, 2, 1));
  // 0001-12-30 is Sunday, belongs to week 52 of year 1.
  EXPECT_EQ(DateValue(1, 12, 30),
      DateValue::CreateFromIso8601WeekBasedDateVals(1, 52, 7));
  // 0001-12-31 is Monday, belongs to year 2.
  EXPECT_EQ(DateValue(1, 12, 31), DateValue::CreateFromIso8601WeekBasedDateVals(2, 1, 1));
  EXPECT_EQ(DateValue(2, 1, 1), DateValue::CreateFromIso8601WeekBasedDateVals(2, 1, 2));

  // Test 2020 ISO 8601 week numbering year.
  // 2019-12-30 is Monday, belongs to week 1 of year 2020.
  // 2021-01-03 is Sunday, belongs to week 53 of year 2020.
  int week_of_year = 1, day_of_week = 1;
  for (DateValue dv(2019, 12, 30);
      dv <= DateValue(2021, 1, 3);
      dv = dv.AddDays(1)) {
    DateValue dv_iso8601 = DateValue::CreateFromIso8601WeekBasedDateVals(2020,
        week_of_year, day_of_week);
    EXPECT_TRUE(dv_iso8601.IsValid());
    EXPECT_EQ(dv, dv_iso8601);

    if (day_of_week == 7) ++week_of_year;
    day_of_week = day_of_week % 7 + 1;
  }
  EXPECT_EQ(54, week_of_year);
  EXPECT_EQ(1, day_of_week);

  // 9998-12-31 is Thursday, belongs to week 53 of year 9998.
  EXPECT_EQ(DateValue(9998, 12, 31),
      DateValue::CreateFromIso8601WeekBasedDateVals(9998, 53, 4));
  EXPECT_EQ(DateValue(9999, 1, 1),
      DateValue::CreateFromIso8601WeekBasedDateVals(9998, 53, 5));
  EXPECT_EQ(DateValue(9999, 1, 2),
      DateValue::CreateFromIso8601WeekBasedDateVals(9998, 53, 6));
  EXPECT_EQ(DateValue(9999, 1, 3),
      DateValue::CreateFromIso8601WeekBasedDateVals(9998, 53, 7));
  EXPECT_EQ(DateValue(9999, 1, 4),
      DateValue::CreateFromIso8601WeekBasedDateVals(9999, 1, 1));
  // 9999-12-31 is Friday, belongs to week 52 of year 9999.
  EXPECT_EQ(DateValue(9999, 12, 31),
      DateValue::CreateFromIso8601WeekBasedDateVals(9999, 52, 5));
  EXPECT_FALSE(DateValue::CreateFromIso8601WeekBasedDateVals(9999, 52, 6).IsValid());
}

TEST(DateTest, LastDay) {
  // Test that it returns invalid DateValue for invalid dates.
  DateValue invalid_dv;
  EXPECT_FALSE(invalid_dv.LastDay().IsValid());

  // Test a non-leap year.
  int month_days[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  for (DateValue dv(2019, 1, 1); dv <= DateValue(2019, 12, 31); dv = dv.AddDays(1)) {
    int year, month, day;
    EXPECT_TRUE(dv.ToYearMonthDay(&year, &month, &day));
    EXPECT_EQ(DateValue(year, month, month_days[month - 1]), dv.LastDay());
  }

  // Test a leap year.
  month_days[1] = 29;
  for (DateValue dv(2016, 1, 1); dv <= DateValue(2016, 12, 31); dv = dv.AddDays(1)) {
    int year, month, day;
    EXPECT_TRUE(dv.ToYearMonthDay(&year, &month, &day));
    EXPECT_EQ(DateValue(year, month, month_days[month - 1]), dv.LastDay());
  }

  // Test upper limit.
  EXPECT_EQ(DateValue(9999, 12, 31), DateValue(9999, 12, 1).LastDay());
  EXPECT_EQ(DateValue(9999, 12, 31), DateValue(9999, 12, 31).LastDay());

  // Test lower limit.
  EXPECT_EQ(DateValue(1, 1, 31), DateValue(1, 1, 1).LastDay());
  EXPECT_EQ(DateValue(1, 1, 31), DateValue(1, 1, 31).LastDay());
}

// These macros add scoped trace to provide the line number of the caller upon failure.
#define TEST_MONTHS_BW_RANGE(date1, date2, min_expected, max_expected) { \
    SCOPED_TRACE(""); \
    TestMonthsBetween((date1), (date2), (min_expected), (max_expected)); \
  }

#define TEST_MONTHS_BW(date1, date2, expected) { \
    SCOPED_TRACE(""); \
    TestMonthsBetween((date1), (date2), (expected), (expected)); \
  }

void TestMonthsBetween(const DateValue& dv1, const DateValue& dv2, double min_expected,
    double max_expected) {
  double months_between;
  EXPECT_TRUE(dv1.MonthsBetween(dv2, &months_between));
  EXPECT_LE(min_expected, months_between);
  EXPECT_LE(months_between, max_expected);
}

TEST(DateTest, MonthsBetween) {
  DateValue invalid_dv;
  double months_between;
  EXPECT_FALSE(invalid_dv.MonthsBetween(DateValue(), &months_between));
  EXPECT_FALSE(invalid_dv.MonthsBetween(DateValue(2001, 1, 1), &months_between));
  EXPECT_FALSE(DateValue(2001, 1, 1).MonthsBetween(invalid_dv, &months_between));

  // Test that if both dates are on the same day of the month, the result has no
  // fractional part.
  TEST_MONTHS_BW(DateValue(2016, 2, 29), DateValue(2016, 1, 29), 1);
  TEST_MONTHS_BW(DateValue(2016, 2, 29), DateValue(2016, 3, 29), -1);

  // Test that if both dates are on the last day of the month, the result has no
  // fractional part.
  TEST_MONTHS_BW(DateValue(2016, 2, 29), DateValue(2016, 1, 31), 1);
  TEST_MONTHS_BW(DateValue(2016, 2, 29), DateValue(2016, 3, 31), -1);

  // Otherwise, there's a fractional part.
  // There are 30/31.0 months between 2016-02-29 and 2016-01-30.
  TEST_MONTHS_BW_RANGE(DateValue(2016, 2, 29), DateValue(2016, 1, 30), 29/31.0, 1.0);
  // There are -32/31.0 months between 2016-02-29 and 2016-03-30.
  TEST_MONTHS_BW_RANGE(DateValue(2016, 2, 29), DateValue(2016, 3, 30), -33/31.0, -1.0);
  // There are 28/31.0 months between 2016-02-29 and 2016-02-01.
  TEST_MONTHS_BW_RANGE(DateValue(2016, 2, 29), DateValue(2016, 2, 1), 27/31.0, 29/31.0);
  // There are -30/31.0 months between 2016-02-29 and 2016-03-28.
  TEST_MONTHS_BW_RANGE(DateValue(2016, 2, 29), DateValue(2016, 3, 28), -31/31.0,
      -29/31.0);

  // Test entire range w/o fractional part.
  TEST_MONTHS_BW(DateValue(1, 1, 1), DateValue(9999, 12, 1), -9998 * 12 - 11);
  TEST_MONTHS_BW(DateValue(9999, 12, 31), DateValue(1, 1, 31), 9998 * 12 + 11);

  // Test entire range w/ fractional part.
  // There are (-9998*12 - 11 - 30/31.0) months between 0001-01-01 and 9999-12-31.
  TEST_MONTHS_BW_RANGE(DateValue(1, 1, 1), DateValue(9999, 12, 31), -9999 * 12.0,
      -9998 * 12 - 11 - 29/31.0);
  // There are (9998*12 + 11 + 30/31.0) months between 9999-12-31 and 0001-01-01.
  TEST_MONTHS_BW_RANGE(DateValue(9999, 12, 31), DateValue(1, 1, 1),
      9998 * 12 + 11 + 29/31.0, 9999 * 12.0);
}

}
