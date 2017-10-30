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

#include <algorithm>
#include <cstring>
#include <boost/assign/list_of.hpp>
#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

#include "common/status.h"
#include "runtime/raw-value.inline.h"
#include "runtime/timestamp-parse-util.h"
#include "runtime/timestamp-value.h"
#include "runtime/timestamp-value.inline.h"
#include "testutil/gtest-util.h"
#include "util/string-parser.h"

#include "common/names.h"

using boost::assign::list_of;
using boost::date_time::Dec;
using boost::date_time::not_a_date_time;
using boost::gregorian::date;
using boost::posix_time::ptime;
using boost::posix_time::time_duration;

namespace impala {

// Used for defining a custom date/time format test. The structure can be used to
// indicate whether the format or value is expected to fail. In a happy path test,
// the values for year, month, day etc will be validated against the parsed result.
// Further validation will also be performed if the should_format flag is enabled,
// whereby the parsed date/time will be translated back to a string and checked
// against the expected value.
struct TimestampTC {
  const char* fmt;
  const char* str;
  bool fmt_should_fail;
  bool str_should_fail;
  bool should_format;
  int expected_year;
  int expected_month;
  int expected_day;
  int expected_hours;
  int expected_minutes;
  int expected_seconds;
  int expected_fraction;
  bool fmt_has_date_toks;
  bool fmt_has_time_toks;

  TimestampTC(const char* fmt, const char* str, bool fmt_should_fail = true,
      bool str_should_fail = true)
    : fmt(fmt),
      str(str),
      fmt_should_fail(fmt_should_fail),
      str_should_fail(str_should_fail),
      should_format(true),
      expected_year(0),
      expected_month(0),
      expected_day(0),
      expected_hours(0),
      expected_minutes(0),
      expected_seconds(0),
      expected_fraction(0),
      fmt_has_date_toks(false),
      fmt_has_time_toks(false) {
  }

  TimestampTC(const char* fmt, const char* str, bool should_format,
      bool fmt_has_date_toks, bool fmt_has_time_toks, int expected_year,
      int expected_month, int expected_day, int expected_hours = 0,
      int expected_minutes = 0, int expected_seconds = 0,
      int expected_fraction = 0)
    : fmt(fmt),
      str(str),
      fmt_should_fail(false),
      str_should_fail(false),
      should_format(should_format),
      expected_year(expected_year),
      expected_month(expected_month),
      expected_day(expected_day),
      expected_hours(expected_hours),
      expected_minutes(expected_minutes),
      expected_seconds(expected_seconds),
      expected_fraction(expected_fraction),
      fmt_has_date_toks(fmt_has_date_toks),
      fmt_has_time_toks(fmt_has_time_toks) {
  }
};

// Used to test custom date/time output test cases i.e. timestamp value -> string.
struct TimestampFormatTC {
  const long ts;
  const char* fmt;
  const char* str;
  bool should_fail;

  TimestampFormatTC(long ts, const char* fmt, const char* str, bool should_fail = false)
    : ts(ts),
      fmt(fmt),
      str(str),
      should_fail(should_fail) {
  }
};

// Used to represent a parsed timestamp token. For example, it may represent a year.
struct TimestampToken {
  const char* fmt;
  int val;
  const char* str;

  TimestampToken(const char* fmt, int val)
    : fmt(fmt),
      val(val),
      str(NULL) {
    }

  TimestampToken(const char* fmt, int val, const char* str)
    : fmt(fmt),
      val(val),
      str(str) {
    }

  friend bool operator<(const TimestampToken& lhs, const TimestampToken& rhs) {
    return strcmp(lhs.fmt, rhs.fmt) < 0;
  }
};

inline void ValidateTimestamp(TimestampValue& tv, string& fmt, string& val,
    string& fmt_val, int year, int month, int day, int hours, int mins, int secs,
    int frac) {
  boost::gregorian::date not_a_date;
  boost::gregorian::date cust_date = tv.date();
  boost::posix_time::time_duration cust_time = tv.time();
  EXPECT_NE(not_a_date, cust_date) << fmt_val;
  EXPECT_NE(not_a_date_time, cust_time) << fmt_val;
  EXPECT_EQ(year, cust_date.year()) << fmt_val;
  EXPECT_EQ(month, cust_date.month()) << fmt_val;
  EXPECT_EQ(day, cust_date.day()) << fmt_val;
  EXPECT_EQ(hours, cust_time.hours()) << fmt_val;
  EXPECT_EQ(mins, cust_time.minutes()) << fmt_val;
  EXPECT_EQ(secs, cust_time.seconds()) << fmt_val;
  EXPECT_EQ(frac, cust_time.fractional_seconds()) << fmt_val;
}

// This function will generate all permutations of tokens to test that the parsing and
// formatting is correct (position of tokens should be irrelevant). Note that separators
// are also combined with EACH token permutation to get the widest coverage on formats.
// This forces out the parsing and format logic edge cases.
void TestTimestampTokens(vector<TimestampToken>* toks, int year, int month,
    int day, int hours, int mins, int secs, int frac) {
  const char* SEPARATORS = " ~!@%^&*_+-:;|\\,./";
  int toks_len = toks->size();
  sort(toks->begin(), toks->end());
  string fmt;
  string val;
  do {
    // Validate we can parse date/time raw tokens (no separators)
    {
      for (int i = 0; i < toks_len; ++i) {
        fmt.append((*toks)[i].fmt);
        if ((*toks)[i].str != NULL) {
          val.append(string((*toks)[i].str));
        } else {
          val.append(lexical_cast<string>((*toks)[i].val));
        }
      }
      string fmt_val = "Format: " + fmt + ", Val: " + val;
      DateTimeFormatContext dt_ctx(fmt.c_str(), fmt.length());
      ASSERT_TRUE(TimestampParser::ParseFormatTokens(&dt_ctx)) << fmt_val;
      TimestampValue tv = TimestampValue::Parse(val.c_str(), val.length(), dt_ctx);
      ValidateTimestamp(tv, fmt, val, fmt_val, year, month, day, hours, mins, secs,
          frac);
      int buff_len = dt_ctx.fmt_out_len + 1;
      char buff[buff_len];
      int actual_len = tv.Format(dt_ctx, buff_len, buff);
      EXPECT_GT(actual_len, 0) << fmt_val;
      EXPECT_LE(actual_len, dt_ctx.fmt_len) << fmt_val;
      string buff_str(buff);
      EXPECT_EQ(buff_str, val) << fmt_val <<  " " << buff_str;
      fmt.clear();
      val.clear();
    }
    // Validate we can parse date/time with separators
    {
      for (const char* separator = SEPARATORS; *separator != 0;
          ++separator) {
        for (int i = 0; i < toks_len; ++i) {
          fmt.append((*toks)[i].fmt);
          if (i + 1 < toks_len) fmt.push_back(*separator);
          if ((*toks)[i].str != NULL) {
            val.append(string((*toks)[i].str));
          } else {
            val.append(lexical_cast<string>((*toks)[i].val));
          }
          if (i + 1 < toks_len) val.push_back(*separator);
        }
        string fmt_val = "Format: " + fmt + ", Val: " + val;
        DateTimeFormatContext dt_ctx(fmt.c_str(), fmt.length());
        ASSERT_TRUE(TimestampParser::ParseFormatTokens(&dt_ctx)) << fmt_val;
        TimestampValue tv = TimestampValue::Parse(val.c_str(), val.length(), dt_ctx);
        ValidateTimestamp(tv, fmt, val, fmt_val, year, month, day, hours, mins, secs,
            frac);
        int buff_len = dt_ctx.fmt_out_len + 1;
        char buff[buff_len];
        int actual_len = tv.Format(dt_ctx, buff_len, buff);
        EXPECT_GT(actual_len, 0) << fmt_val;
        EXPECT_LE(actual_len, dt_ctx.fmt_len) << fmt_val;
        string buff_str(buff);
        EXPECT_EQ(buff_str, val) << fmt_val <<  " " << buff_str;
        fmt.clear();
        val.clear();
      }
    }
  } while (next_permutation(toks->begin(), toks->end()));
}

TEST(TimestampTest, Basic) {
  // Fix current time to determine the behavior parsing 2-digit year format
  // Set it to 03/01 to test 02/29 edge cases.
  TimestampValue now(date(1980, 3, 1), time_duration(16, 14, 24));

  char s1[] = "2012-01-20 01:10:01";
  char s2[] = "1990-10-20 10:10:10.123456789  ";
  char s3[] = "  1990-10-20 10:10:10.123456789";

  TimestampValue v1 = TimestampValue::Parse(s1, strlen(s1));
  TimestampValue v2 = TimestampValue::Parse(s2, strlen(s2));
  TimestampValue v3 = TimestampValue::Parse(s3, strlen(s3));

  EXPECT_EQ(v1.date().year(), 2012);
  EXPECT_EQ(v1.date().month(), 1);
  EXPECT_EQ(v1.date().day(), 20);
  EXPECT_EQ(v1.time().hours(), 1);
  EXPECT_EQ(v1.time().minutes(), 10);
  EXPECT_EQ(v1.time().seconds(), 1);
  EXPECT_EQ(v1.time().fractional_seconds(), 0);
  EXPECT_EQ(v2.time().fractional_seconds(), 123456789);

  EXPECT_NE(v1, v2);
  EXPECT_EQ(v2, v3);
  EXPECT_LT(v2, v1);
  EXPECT_LE(v2, v1);
  EXPECT_GT(v1, v2);
  EXPECT_GE(v2, v3);

  EXPECT_NE(RawValue::GetHashValue(&v1, TYPE_TIMESTAMP, 0),
            RawValue::GetHashValue(&v2, TYPE_TIMESTAMP, 0));
  EXPECT_EQ(RawValue::GetHashValue(&v3, TYPE_TIMESTAMP, 0),
            RawValue::GetHashValue(&v2, TYPE_TIMESTAMP, 0));

  char s4[] = "2012-01-20T01:10:01";
  char s5[] = "1990-10-20T10:10:10.123456789";

  TimestampValue v4 = TimestampValue::Parse(s4, strlen(s4));
  TimestampValue v5 = TimestampValue::Parse(s5, strlen(s5));

  EXPECT_EQ(v4.date().year(), 2012);
  EXPECT_EQ(v4.date().month(), 1);
  EXPECT_EQ(v4.date().day(), 20);
  EXPECT_EQ(v4.time().hours(), 1);
  EXPECT_EQ(v4.time().minutes(), 10);
  EXPECT_EQ(v4.time().seconds(), 1);
  EXPECT_EQ(v4.time().fractional_seconds(), 0);
  EXPECT_EQ(v5.date().year(), 1990);
  EXPECT_EQ(v5.date().month(), 10);
  EXPECT_EQ(v5.date().day(), 20);
  EXPECT_EQ(v5.time().hours(), 10);
  EXPECT_EQ(v5.time().minutes(), 10);
  EXPECT_EQ(v5.time().seconds(), 10);
  EXPECT_EQ(v5.time().fractional_seconds(), 123456789);

  // Test Dates and Times as timestamps.
  char d1[] = "2012-01-20";
  char d2[] = "1990-10-20";
  TimestampValue dv1 = TimestampValue::Parse(d1, strlen(d1));
  TimestampValue dv2 = TimestampValue::Parse(d2, strlen(d2));

  EXPECT_NE(dv1, dv2);
  EXPECT_LT(dv1, v1);
  EXPECT_LE(dv1, v1);
  EXPECT_GT(v1, dv1);
  EXPECT_GE(v1, dv1);
  EXPECT_NE(dv2, v2);

  EXPECT_EQ(dv1.date().year(), 2012);
  EXPECT_EQ(dv1.date().month(), 1);
  EXPECT_EQ(dv1.date().day(), 20);

  char t1[] = "10:11:12.123456789";
  char t2[] = "00:00:00";
  TimestampValue tv1 = TimestampValue::Parse(t1, strlen(t1));
  TimestampValue tv2 = TimestampValue::Parse(t2, strlen(t2));

  EXPECT_NE(tv1, tv2);
  EXPECT_NE(tv1, v2);

  EXPECT_EQ(tv1.time().hours(), 10);
  EXPECT_EQ(tv1.time().minutes(), 11);
  EXPECT_EQ(tv1.time().seconds(), 12);
  EXPECT_EQ(tv1.time().fractional_seconds(), 123456789);
  EXPECT_EQ(tv2.time().fractional_seconds(), 0);

  // Test variable fraction lengths
  const char* FRACTION_MAX_STR = "123456789";
  const char* TEST_VALS[] = { "2013-12-10 12:04:17.", "2013-12-10T12:04:17.",
      "12:04:17." };
  const int TEST_VAL_CNT = sizeof(TEST_VALS) / sizeof(char*);
  for (int i = 0; i < TEST_VAL_CNT; ++i) {
    const int VAL_LEN = strlen(TEST_VALS[i]);
    int fraction_len = strlen(FRACTION_MAX_STR);
    char frac_buff[VAL_LEN + fraction_len + 1];
    while (fraction_len > 0) {
      memcpy(frac_buff, TEST_VALS[i], VAL_LEN);
      memcpy(frac_buff + VAL_LEN, FRACTION_MAX_STR, fraction_len);
      *(frac_buff + VAL_LEN + fraction_len) = '\0';
      TimestampValue tv_frac = TimestampValue::Parse(frac_buff, strlen(frac_buff));
      if (frac_buff[4] == '-') {
        EXPECT_EQ(tv_frac.date().year(), 2013);
        EXPECT_EQ(tv_frac.date().month(), 12);
        EXPECT_EQ(tv_frac.date().day(), 10);
      }
      EXPECT_EQ(tv_frac.time().hours(), 12);
      EXPECT_EQ(tv_frac.time().minutes(), 4);
      EXPECT_EQ(tv_frac.time().seconds(), 17);
      StringParser::ParseResult status;
      int32_t fraction =
          StringParser::StringToInt<int32_t>(FRACTION_MAX_STR, fraction_len, &status);
      EXPECT_TRUE(StringParser::PARSE_SUCCESS == status);
      for (int i = fraction_len; i < 9; ++i) fraction *= 10;
      EXPECT_EQ(tv_frac.time().fractional_seconds(), fraction);
      --fraction_len;
    }
  }

  // Bad formats
  char b1[] = "1990-10 10:10:10.123456789";
  TimestampValue bv1 = TimestampValue::Parse(b1, strlen(b1));
  boost::gregorian::date not_a_date;

  EXPECT_EQ(bv1.date(), not_a_date);
  EXPECT_EQ(bv1.time(), not_a_date_time);

  char b2[] = "1991-10-10 99:10:10.123456789";
  TimestampValue bv2 = TimestampValue::Parse(b2, strlen(b2));

  EXPECT_EQ(bv2.time(), not_a_date_time);
  EXPECT_EQ(bv2.date(), not_a_date);

  char b3[] = "1990-10- 10:10:10.123456789";
  TimestampValue bv3 = TimestampValue::Parse(b3, strlen(b3));

  EXPECT_EQ(bv3.date(), not_a_date);
  EXPECT_EQ(bv3.time(), not_a_date_time);

  char b4[] = "10:1010.123456789";
  TimestampValue bv4 = TimestampValue::Parse(b4, strlen(b4));

  EXPECT_EQ(bv4.date(), not_a_date);
  EXPECT_EQ(bv4.time(), not_a_date_time);

  char b5[] = "10:11:12.123456 1991-10-10";
  TimestampValue bv5 = TimestampValue::Parse(b5, strlen(b5));

  EXPECT_EQ(bv5.date(), not_a_date);
  EXPECT_EQ(bv5.time(), not_a_date_time);

  char b6[] = "2012-01-20 01:10:00.123.466";
  TimestampValue bv6 = TimestampValue::Parse(b6, strlen(b6));

  EXPECT_EQ(bv6.date(), not_a_date);
  EXPECT_EQ(bv6.time(), not_a_date_time);

  char b7[] = "2012-01-20 01:10:00.123 477 ";
  TimestampValue bv7 = TimestampValue::Parse(b7, strlen(b7));

  EXPECT_EQ(bv7.date(), not_a_date);
  EXPECT_EQ(bv7.time(), not_a_date_time);

  // Test custom formats by generating all permutations of tokens to check parsing and
  // formatting is behaving correctly (position of tokens should be irrelevant). Note
  // that separators are also combined with EACH token permutation to get the widest
  // coverage on formats.
  const int YEAR = 2013;
  const int MONTH = 10;
  const int DAY = 14;
  const int HOURS = 14;
  const int MINS = 25;
  const int SECS = 44;
  const int FRAC = 123456789;
  // Test parsing/formatting with numeric date/time tokens
  vector<TimestampToken> dt_toks = list_of
      (TimestampToken("dd", DAY))(TimestampToken("MM", MONTH))
      (TimestampToken("yyyy", YEAR))(TimestampToken("HH", HOURS))
      (TimestampToken("mm", MINS))(TimestampToken("ss", SECS))
      (TimestampToken("SSSSSSSSS", FRAC));

  TestTimestampTokens(&dt_toks, YEAR, MONTH, DAY, HOURS, MINS, SECS, FRAC);
  // Test literal months
  const char* months[] = { "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug",
      "Sep", "Oct", "Nov", "Dec"
  };
  // Test parsing/formatting of literal months (short)
  const int MONTH_CNT = (sizeof(months) / sizeof(char**));
  for (int i = 0; i < MONTH_CNT; ++i) {
    // Test parsing/formatting with short literal months
    vector<TimestampToken> dt_lm_toks = list_of
        (TimestampToken("dd", DAY))(TimestampToken("MMM", i + 1, months[i]))
        (TimestampToken("yyyy", YEAR))(TimestampToken("HH", HOURS))
        (TimestampToken("mm", MINS))(TimestampToken("ss", SECS))
        (TimestampToken("SSSSSSSSS", FRAC));
    TestTimestampTokens(&dt_lm_toks, YEAR, i + 1, DAY, HOURS, MINS, SECS, FRAC);
  }
  // Test parsing/formatting of complex date/time formats
  vector<TimestampTC> test_cases = boost::assign::list_of
    // Test year upper/lower bound
    (TimestampTC("yyyy-MM-dd HH:mm:ss", "1400-01-01 00:00:00",
        false, true, false, 1400, 1, 1))
    (TimestampTC("yyyy-MM-dd HH:mm:ss", "1399-12-31 23:59:59",
        false, true))
    (TimestampTC("yyyy-MM-dd HH:mm:ss", "9999-12-31 23:59:59",
        false, true, false, 9999, 12, 31, 23, 59, 59))
    (TimestampTC("yyyy-MM-dd HH:mm:ss +hh", "1400-01-01 01:00:00 +01", false, true, false,
        1400, 1, 1, 0, 0, 0))
    (TimestampTC("yyyy-MM-dd HH:mm:ss +hh", "1400-01-01 01:00:00 +02", false, true))
    (TimestampTC("yyyy-MM-dd HH:mm:ss +hh", "9999-12-31 22:00:00 -01", false, true, false,
        9999, 12, 31, 23, 0, 0))
    (TimestampTC("yyyy-MM-dd HH:mm:ss +hh", "9999-12-31 22:00:00 -02", false, true))
    // Test case on literal short months
    (TimestampTC("yyyy-MMM-dd", "2013-OCT-01", false, true, false, 2013, 10, 1))
    // Test case on literal short months
    (TimestampTC("yyyy-MMM-dd", "2013-oct-01", false, true, false, 2013, 10, 1))
    // Test case on literal short months
    (TimestampTC("yyyy-MMM-dd", "2013-oCt-01", false, true, false, 2013, 10, 1))
    // Test padding on numeric and literal tokens (short)
    (TimestampTC("MMMyyyyyydd", "Apr00201309", true, true, false, 2013, 4, 9))
    // Test duplicate tokens
    (TimestampTC("yyyy MM dd ddMMMyyyy (HH:mm:ss.SSSS)",
        "2013 05 12 16Apr1952 (16:53:21.1234)", false, true, true, 1952, 4, 16, 16,
        53, 21, 123400000))
    // Test missing separator on short date format
    (TimestampTC("Myyd", "4139", true, true))
    // Test bad year format
    (TimestampTC("YYYYmmdd", "20131001"))
    // Test unknown formatting character
    (TimestampTC("yyyyUUdd", "2013001001"))
    // Test markers
    (TimestampTC("TTZZ", "TTZZ"))
    // Test numeric formatting character
    (TimestampTC("yyyyMM1dd", "201301111"))
    // Test out of range year
    (TimestampTC("yyyyyMMdd", "120130101", false, true))
    // Test out of range month
    (TimestampTC("yyyyMMdd", "20131301", false, true))
    // Test out of range month
    (TimestampTC("yyyyMMdd", "20130001", false, true))
    // Test out of range day
    (TimestampTC("yyyyMMdd", "20130132", false, true))
    // Test out of range day
    (TimestampTC("yyyyMMdd", "20130100", false, true))
    // Test out of range hour
    (TimestampTC("HH:mm:ss", "24:01:01", false, true))
    // Test out of range minute
    (TimestampTC("HH:mm:ss", "23:60:01", false, true))
    // Test out of range second
    (TimestampTC("HH:mm:ss", "23:01:60", false, true))
    // Test characters where numbers should be
    (TimestampTC("HH:mm:ss", "aa:01:01", false, true))
    // Test missing year
    (TimestampTC("MMdd", "1201", false, true))
    // Test missing month
    (TimestampTC("yyyydd", "201301", false, true))
    (TimestampTC("yydd", "1301", false, true))
    // Test missing day
    (TimestampTC("yyyyMM", "201301", false, true))
    (TimestampTC("yyMM", "8512", false, true))
    // Test missing month and day
    (TimestampTC("yyyy", "2013", false, true))
    (TimestampTC("yy", "13", false, true))
    // Test short year token
    (TimestampTC("y-MM-dd", "2013-11-13", false, true, false, 2013, 11, 13))
    (TimestampTC("y-MM-dd", "13-11-13", false, true, false, 1913, 11, 13))
    // Test 2-digit year format
    (TimestampTC("yy-MM-dd", "17-08-31", false, true, false, 1917, 8, 31))
    (TimestampTC("yy-MM-dd", "99-08-31", false, true, false, 1999, 8, 31))
    // Test 02/29 edge cases of 2-digit year format
    (TimestampTC("yy-MM-dd", "00-02-28", false, true, false, 2000, 2, 28))
    (TimestampTC("yy-MM-dd", "00-02-29", false, true, false, 2000, 2, 29))
    (TimestampTC("yy-MM-dd", "00-03-01", false, true, false, 2000, 3, 1))
    (TimestampTC("yy-MM-dd", "00-03-02", false, true, false, 1900, 3, 2))
    (TimestampTC("yy-MM-dd", "04-02-29", false, true, false, 1904, 2, 29))
    (TimestampTC("yy-MM-dd", "99-02-29", false, true))
    // Test 1-digit year format with time to show the exact boundary
    // Before the cutoff. Year should be 2000
    (TimestampTC("y-MM-dd HH:mm:ss", "00-02-29 16:14:23", false, true, false,
        2000, 2, 29, 16, 14, 23))
    // After the cutoff but 02/29/1900 is invalid
    (TimestampTC("y-MM-dd HH:mm:ss", "00-02-29 16:14:24", false, true))
    // Test short month token
    (TimestampTC("yyyy-M-dd", "2013-11-13", false, true, false, 2013, 11, 13))
    (TimestampTC("yyyy-M-dd", "2013-1-13", false, true, false, 2013, 1, 13))
    // Test short day token
    (TimestampTC("yyyy-MM-d", "2013-11-13", false, true, false, 2013, 11, 13))
    (TimestampTC("yyyy-MM-d", "2013-11-3", false, true, false, 2013, 11, 3))
    // Test short all date tokens
    (TimestampTC("y-M-d", "2013-11-13", false, true, false, 2013, 11, 13))
    (TimestampTC("y-M-d", "13-1-3", false, true, false, 1913, 1, 3))
    // Test short hour token
    (TimestampTC("H:mm:ss", "14:24:34", false, false, true, 0, 0, 0, 14, 24, 34))
    (TimestampTC("H:mm:ss", "4:24:34", false, false, true, 0, 0, 0, 4, 24, 34))
    // Test short minute token
    (TimestampTC("HH:m:ss", "14:24:34", false, false, true, 0, 0, 0, 14, 24, 34))
    (TimestampTC("HH:m:ss", "1:24:34", false, true))
    // Test short second token
    (TimestampTC("HH:mm:s", "14:24:34", false, false, true, 0, 0, 0, 14, 24, 34))
    // Test short all time tokens
    (TimestampTC("H:m:s", "11:22:33", false, false, true, 0, 0, 0, 11, 22, 33))
    (TimestampTC("H:m:s", "1:2:3", false, false, true, 0, 0, 0, 1, 2, 3))
    // Test short fraction token
    (TimestampTC("HH:mm:ss:S", "14:24:34:1234", false, false, true, 0, 0, 0, 14, 24, 34,
        123400000));
  // Loop through custom parse/format test cases and execute each one. Each test case
  // will be explicitly set with a pass/fail expectation related to either the format
  // or literal value.
  for (int i = 0; i < test_cases.size(); ++i) {
    TimestampTC test_case = test_cases[i];
    DateTimeFormatContext dt_ctx(test_case.fmt, strlen(test_case.fmt));
    dt_ctx.SetCenturyBreak(now);
    bool parse_result = TimestampParser::ParseFormatTokens(&dt_ctx);
    if (test_case.fmt_should_fail) {
      EXPECT_FALSE(parse_result) << "TC: " << i;
      continue;
    } else {
      ASSERT_TRUE(parse_result) << "TC: " << i;
    }
    TimestampValue cust_tv = TimestampValue::Parse(test_case.str, strlen(test_case.str), dt_ctx);
    boost::gregorian::date cust_date = cust_tv.date();
    boost::posix_time::time_duration cust_time = cust_tv.time();
    if (test_case.str_should_fail) {
      EXPECT_EQ(not_a_date, cust_date) << "TC: " << i;
      EXPECT_EQ(cust_time, not_a_date_time) << "TC: " << i;
      continue;
    }
    // Check that we have something valid in the timestamp value
    EXPECT_TRUE((!cust_date.is_special())
        || (cust_time != not_a_date_time)) << "TC: " << i;
    // Check the date component (based on any date format tokens being present)
    if (test_case.fmt_has_date_toks) {
      EXPECT_NE(cust_date, not_a_date) << "TC: " << i;
      EXPECT_EQ(test_case.expected_year, cust_date.year()) << "TC: " << i;
      EXPECT_EQ(test_case.expected_month, cust_date.month()) << "TC: " << i;
      EXPECT_EQ(test_case.expected_day, cust_date.day()) << "TC: " << i;
    } else {
      EXPECT_EQ(not_a_date, cust_date) << "TC: " << i;
    }
    // Check the time component (based on any time format tokens being present). Note
    // that if the date is specified, the time will at least be 00:00:00.0
    if (test_case.fmt_has_time_toks || test_case.fmt_has_date_toks) {
      EXPECT_NE(cust_time, not_a_date_time) << "TC: " << i;
      EXPECT_EQ(test_case.expected_hours, cust_time.hours()) << "TC: " << i;
      EXPECT_EQ(test_case.expected_minutes, cust_time.minutes()) << "TC: " << i;
      EXPECT_EQ(test_case.expected_seconds, cust_time.seconds()) << "TC: " << i;
      EXPECT_EQ(test_case.expected_fraction, cust_time.fractional_seconds()) << "TC: "
          << i;
      if (!test_case.should_format) continue;
      int buff_len = dt_ctx.fmt_out_len + 1;
      char buff[buff_len];
      int actual_len = cust_tv.Format(dt_ctx, buff_len, buff);
      EXPECT_GT(actual_len, 0) << "TC: " << i;
      EXPECT_LE(actual_len, dt_ctx.fmt_len) << "TC: " << i;
      EXPECT_EQ(string(test_case.str, strlen(test_case.str)), string(buff, actual_len))
          << "TC: " << i;
    } else {
      EXPECT_EQ(cust_time, not_a_date_time) << test_case.fmt << " " << test_case.str;
    }
  }
  // Test complex formatting of date/times
  vector<TimestampFormatTC> fmt_test_cases = list_of
    (TimestampFormatTC(1382337792, "yyyy-MM-dd HH:mm:ss.SSSSSSSSS",
        "2013-10-21 06:43:12.000000000"))
    // Test just formatting time tokens
    (TimestampFormatTC(1382337792, "HH:mm:ss.SSSSSSSSS", "06:43:12.000000000"))
    (TimestampFormatTC(0, "yyyy-MM-ddTHH:mm:SS.SSSSSSSSSZ",
        "1970-01-01T00:00:00.000000000Z"))
    // Test just formatting date tokens
    (TimestampFormatTC(965779200, "yyyy-MM-dd", "2000-08-09"))
    // Test short form date tokens
    (TimestampFormatTC(965779200, "yyyy-M-d", "2000-8-9"))
    // Test short form tokens on wide dates
    (TimestampFormatTC(1382337792, "d", "21"))
    // Test month expansion
    (TimestampFormatTC(965779200, "MMM/MM/M", "Aug/08/8"))
    // Test padding on single digits
    (TimestampFormatTC(965779200, "dddddd/dd/d", "000009/09/9"))
    // Test padding on double digits
    (TimestampFormatTC(1382337792, "dddddd/dd/dd", "000021/21/21"))
    // Test just formatting time tokens on a ts value generated from a date
    (TimestampFormatTC(965779200, "HH:mm:ss", "00:00:00"));
  // Loop through format test cases
  for (int i = 0; i < fmt_test_cases.size(); ++i) {
    TimestampFormatTC test_case = fmt_test_cases[i];
    DateTimeFormatContext dt_ctx(test_case.fmt, strlen(test_case.fmt));
    ASSERT_TRUE(TimestampParser::ParseFormatTokens(&dt_ctx))  << "TC: " << i;
    TimestampValue cust_tv = TimestampValue::FromUnixTime(test_case.ts);
    EXPECT_NE(cust_tv.date(), not_a_date) << "TC: " << i;
    EXPECT_NE(cust_tv.time(), not_a_date_time) << "TC: " << i;
    EXPECT_GE(dt_ctx.fmt_out_len, dt_ctx.fmt_len);
    int buff_len = dt_ctx.fmt_out_len + 1;
    char buff[buff_len];
    int actual_len = cust_tv.Format(dt_ctx, buff_len, buff);
    EXPECT_GT(actual_len, 0) << "TC: " << i;
    EXPECT_LE(actual_len, dt_ctx.fmt_out_len) << "TC: " << i;
    EXPECT_EQ(string(buff, actual_len),
        string(test_case.str, strlen(test_case.str))) << "TC: " << i;
  }

  // Test edge cases
  const int64_t MIN_DATE_AS_UNIX_TIME = -17987443200;
  TimestampValue min_date = TimestampValue::Parse("1400-01-01");
  EXPECT_TRUE(min_date.HasDate());
  EXPECT_TRUE(min_date.HasTime());
  time_t tm_min;
  EXPECT_TRUE(min_date.ToUnixTime(&tm_min));
  EXPECT_EQ(MIN_DATE_AS_UNIX_TIME, tm_min);
  int64_t tm_min_micros;
  EXPECT_TRUE(min_date.UtcToUnixTimeMicros(&tm_min_micros));
  EXPECT_EQ(MIN_DATE_AS_UNIX_TIME * MICROS_PER_SEC, tm_min_micros);

  // Add 250ns and check the value is rounded down
  min_date = TimestampValue::FromUnixTimeNanos(MIN_DATE_AS_UNIX_TIME, 250);
  EXPECT_TRUE(min_date.ToUnixTime(&tm_min));
  EXPECT_EQ(MIN_DATE_AS_UNIX_TIME, tm_min);
  EXPECT_TRUE(min_date.UtcToUnixTimeMicros(&tm_min_micros));
  EXPECT_EQ(MIN_DATE_AS_UNIX_TIME * MICROS_PER_SEC, tm_min_micros);

  // Add another 250ns and check the value is rounded up to the nearest microsecond.
  EXPECT_TRUE(min_date.ToUnixTime(&tm_min));
  EXPECT_EQ(MIN_DATE_AS_UNIX_TIME, tm_min);
  min_date.set_time(min_date.time() + boost::posix_time::nanoseconds(250));
  EXPECT_TRUE(min_date.UtcToUnixTimeMicros(&tm_min_micros));
  EXPECT_EQ(MIN_DATE_AS_UNIX_TIME * MICROS_PER_SEC + 1, tm_min_micros);

  EXPECT_EQ("1400-01-01 00:00:00",
      TimestampValue::FromUnixTime(MIN_DATE_AS_UNIX_TIME).ToString());
  TimestampValue too_early = TimestampValue::FromUnixTime(MIN_DATE_AS_UNIX_TIME - 1);
  EXPECT_FALSE(too_early.HasDate());
  EXPECT_FALSE(too_early.HasTime());
  EXPECT_FALSE(too_early.UtcToUnixTimeMicros(&tm_min_micros));

  // Sub-second FromUnixTime functions incorrectly accepted the last second of 1399
  // as valid, because validation logic checked the nearest second rounded towards 0
  // (IMPALA-5664).
  EXPECT_FALSE(TimestampValue::FromSubsecondUnixTime(
      MIN_DATE_AS_UNIX_TIME - 0.1).HasDate());
  EXPECT_FALSE(TimestampValue::UtcFromUnixTimeMicros(
      MIN_DATE_AS_UNIX_TIME * MICROS_PER_SEC - 2000).HasDate());
  EXPECT_FALSE(TimestampValue::FromUnixTimeNanos(
      MIN_DATE_AS_UNIX_TIME, -NANOS_PER_MICRO * 100).HasDate());

  // Test the max supported date that can be represented in seconds.
  const int64_t MAX_DATE_AS_UNIX_TIME = 253402300799;
  TimestampValue max_date =
      TimestampValue(date(9999, Dec, 31), time_duration(23, 59, 59));
  EXPECT_TRUE(max_date.HasDate());
  EXPECT_TRUE(max_date.HasTime());
  time_t tm_max;
  EXPECT_TRUE(max_date.ToUnixTime(&tm_max));
  EXPECT_EQ(MAX_DATE_AS_UNIX_TIME, tm_max);
  int64_t tm_max_micros;
  EXPECT_TRUE(max_date.UtcToUnixTimeMicros(&tm_max_micros));
  EXPECT_EQ(MAX_DATE_AS_UNIX_TIME * MICROS_PER_SEC, tm_max_micros);

  // Add 250 nanoseconds and test the result of UtcToUnixTimeMicros
  max_date.set_time(max_date.time() + boost::posix_time::nanoseconds(250));
  EXPECT_TRUE(max_date.UtcToUnixTimeMicros(&tm_max_micros));
  EXPECT_EQ(MAX_DATE_AS_UNIX_TIME * MICROS_PER_SEC, tm_max_micros);
  // Adding another 250ns will result in the timestamp being rounded up.
  max_date.set_time(max_date.time() + boost::posix_time::nanoseconds(250));
  EXPECT_TRUE(max_date.UtcToUnixTimeMicros(&tm_max_micros));
  EXPECT_EQ(MAX_DATE_AS_UNIX_TIME * MICROS_PER_SEC + 1, tm_max_micros);

  // The max date that can be represented with the maximum number of nanoseconds. Unlike
  // the cases above, converting to microseconds does not round up to the next
  // microsecond because that time is not supported by Impala.
  max_date = TimestampValue::FromUnixTimeNanos(MAX_DATE_AS_UNIX_TIME, 999999999);
  EXPECT_TRUE(max_date.HasDate());
  EXPECT_TRUE(max_date.HasTime());
  // The result is the maximum date with the maximum number of microseconds supported by
  // Impala.
  EXPECT_TRUE(max_date.UtcToUnixTimeMicros(&tm_max_micros));
  EXPECT_EQ(MAX_DATE_AS_UNIX_TIME * MICROS_PER_SEC + 999999, tm_max_micros);

  EXPECT_EQ("9999-12-31 23:59:59",
      TimestampValue::FromUnixTime(MAX_DATE_AS_UNIX_TIME).ToString());
  TimestampValue too_late = TimestampValue::FromUnixTime(MAX_DATE_AS_UNIX_TIME + 1);
  EXPECT_FALSE(too_late.HasDate());
  EXPECT_FALSE(too_late.HasTime());

  // Checking sub-second FromUnixTime functions near 10000.01.01
  EXPECT_TRUE(TimestampValue::FromSubsecondUnixTime(
      MAX_DATE_AS_UNIX_TIME + 0.99).HasDate());
  EXPECT_FALSE(TimestampValue::FromSubsecondUnixTime(
      MAX_DATE_AS_UNIX_TIME + 1).HasDate());

  EXPECT_TRUE(TimestampValue::UtcFromUnixTimeMicros(
      MAX_DATE_AS_UNIX_TIME * MICROS_PER_SEC + MICROS_PER_SEC - 1).HasDate());
  EXPECT_FALSE(TimestampValue::UtcFromUnixTimeMicros(
      MAX_DATE_AS_UNIX_TIME * MICROS_PER_SEC + MICROS_PER_SEC).HasDate());

  EXPECT_TRUE(TimestampValue::FromUnixTimeNanos(
      MAX_DATE_AS_UNIX_TIME, NANOS_PER_SEC - 1).HasDate());
  EXPECT_FALSE(TimestampValue::FromUnixTimeNanos(
      MAX_DATE_AS_UNIX_TIME, NANOS_PER_SEC).HasDate());

  // Regression tests for IMPALA-1676, Unix times overflow int32 during year 2038
  EXPECT_EQ("2038-01-19 03:14:08",
      TimestampValue::FromUnixTime(2147483648).ToString());
  EXPECT_EQ("2038-01-19 03:14:09",
      TimestampValue::FromUnixTime(2147483649).ToString());

  // Test FromUnixTime around the boundary of the values that are converted via boost via
  // gmtime (IMPALA-5357). Tests 1 second before and after the values supported by the
  // boost conversion logic.
  const int64_t MIN_BOOST_CONVERT_UNIX_TIME = -9223372036;
  const int64_t MAX_BOOST_CONVERT_UNIX_TIME = 9223372036;
  EXPECT_EQ("1677-09-21 00:12:43",
      TimestampValue::FromUnixTime(MIN_BOOST_CONVERT_UNIX_TIME - 1).ToString());
  EXPECT_EQ("1677-09-21 00:12:44",
      TimestampValue::FromUnixTime(MIN_BOOST_CONVERT_UNIX_TIME).ToString());
  EXPECT_EQ("2262-04-11 23:47:16",
      TimestampValue::FromUnixTime(MAX_BOOST_CONVERT_UNIX_TIME).ToString());
  EXPECT_EQ("2262-04-11 23:47:17",
      TimestampValue::FromUnixTime(MAX_BOOST_CONVERT_UNIX_TIME + 1).ToString());

  // Test a leap second in 1998 represented by the UTC time 1998-12-31 23:59:60.
  // Unix time cannot represent the leap second, which repeats 915148800.
  EXPECT_EQ("1998-12-31 23:59:59",
      TimestampValue::FromUnixTime(915148799).ToString());
  EXPECT_EQ("1999-01-01 00:00:00",
      TimestampValue::FromUnixTime(915148800).ToString());
  // The leap second doesn't parse in Impala.
  TimestampValue leap_tv = TimestampValue::Parse("1998-12-31 23:59:60.00");
  EXPECT_FALSE(leap_tv.HasDateAndTime());

  // The leap second can be parsed by ptime, though it is just converted to the time
  // that the Unix time would represent (i.e. the second after the new year). This shows
  // both times constructed via ptime compare equally.
  ptime leap_ptime1 = boost::posix_time::time_from_string("1998-12-31 23:59:60");
  ptime leap_ptime2 = boost::posix_time::time_from_string("1999-01-01 00:00:00");
  TimestampValue leap_tv1 = TimestampValue(leap_ptime1);
  TimestampValue leap_tv2 = TimestampValue(leap_ptime2);
  EXPECT_TRUE(leap_tv1.HasDateAndTime());
  EXPECT_TRUE(leap_tv1 == TimestampValue(leap_ptime2));
  time_t leap_time_t;
  EXPECT_TRUE(leap_tv1.ToUnixTime(&leap_time_t));
  EXPECT_EQ(915148800, leap_time_t);
  EXPECT_EQ("1999-01-01 00:00:00", leap_tv1.ToString());
  EXPECT_TRUE(leap_tv2.ToUnixTime(&leap_time_t));
  EXPECT_EQ(915148800, leap_time_t);
  EXPECT_EQ("1999-01-01 00:00:00", leap_tv2.ToString());

  // Test Unix time as a float
  double result;
  EXPECT_TRUE(
      TimestampValue::Parse("2013-10-21 06:43:12.07").ToSubsecondUnixTime(&result));
  EXPECT_EQ(1382337792.07, result);
  EXPECT_EQ("1970-01-01 00:00:00.008000000",
      TimestampValue::FromSubsecondUnixTime(0.008).ToString());
}

}

IMPALA_TEST_MAIN();
