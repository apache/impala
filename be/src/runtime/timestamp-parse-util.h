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


#pragma once

#include <boost/date_time/gregorian/gregorian.hpp>

#include "gutil/macros.h"
#include "runtime/datetime-parser-common.h"

namespace boost {
  namespace posix_time {
    class time_duration;
  }
}

namespace impala {

/// Used for parsing both default and custom formatted timestamp values.
class TimestampParser {
 public:
  /// Parse a default date/time string. The default timestamp format is:
  /// yyyy-MM-dd HH:mm:ss.SSSSSSSSS or yyyy-MM-ddTHH:mm:ss.SSSSSSSSS. Just the
  /// date may be specified. All components are required in either the
  /// date or time except for the fractional seconds following the period. In the case
  /// of just a date, the time will be set to 00:00:00.
  /// In case accept_time_toks_only=true, HH:mm:ss.SSSSSSSSS is also accepted and if
  /// there is no data part in the string, the output date is set to invalid.
  /// str -- valid pointer to the string to parse
  /// len -- length of the string to parse (must be > 0)
  /// d -- the date value where the results of the parsing will be placed
  /// t -- the time value where the results of the parsing will be placed
  /// accept_time_toks_only -- also accepts time of the day string without date part
  /// Returns true if the date/time was successfully parsed.
  static bool ParseSimpleDateFormat(const char* str, int len, boost::gregorian::date* d,
      boost::posix_time::time_duration* t,
      bool accept_time_toks_only = false) WARN_UNUSED_RESULT;

  /// Parse a date/time string. The data must adhere to SimpleDateFormat, otherwise it
  /// will be rejected i.e. no missing tokens. In the case of just a date, the time will
  /// be set to 00:00:00.
  /// str -- valid pointer to the string to parse
  /// len -- length of the string to parse (must be > 0)
  /// dt_ctx -- date/time format context (must contain valid tokens)
  /// d -- the date value where the results of the parsing will be placed
  /// t -- the time value where the results of the parsing will be placed
  /// Returns true if the date/time was successfully parsed.
  static bool ParseSimpleDateFormat(const char* str, int len,
      const datetime_parse_util::DateTimeFormatContext& dt_ctx, boost::gregorian::date* d,
      boost::posix_time::time_duration* t) WARN_UNUSED_RESULT;

  /// Parse 'str' into date and time objects following ISO:SQL:2016 datetime pattern
  /// format. This function is used when a user specifies a datetime format string for
  /// the cast. 'dt_ctx' holds the format tokens produced by the tokenizer.
  /// Returns true if the parsing succeeded. Otherwise it sets 'd' and 't' to
  /// invalid values and returns false.
  static bool ParseIsoSqlFormat(const char* str, int len,
      const datetime_parse_util::DateTimeFormatContext& dt_ctx, boost::gregorian::date* d,
      boost::posix_time::time_duration* t) WARN_UNUSED_RESULT;

  /// Optimized formatter for default short and long formats
  static int FormatDefault(const boost::gregorian::date& d,
      const boost::posix_time::time_duration& t, char* dst);

  /// Format the date/time values using the given format context.
  /// Caller must make sure that it has enough buffer space in 'dst' to hold the output.
  /// Return total output length that is written into 'dst'. Return -1 If 'd' or 't' is
  /// invalid.
  /// dt_ctx -- date/time format context
  /// d -- the date value
  /// t -- the time value
  /// max_length -- the maximum length of characters that 'dst' can hold. Only used for
  ///   assertion in debug build. A DCHECK error will be raised if 'max_length' is less
  ///   than required space. However, this method will not write more than 'max_length'.
  /// dst -- pointer to destination buffer to write the result
  static int Format(const datetime_parse_util::DateTimeFormatContext& dt_ctx,
      const boost::gregorian::date& d, const boost::posix_time::time_duration& t,
      int max_length, char* dst);

 private:
  /// Helper function finding the correct century for 1 or 2 digit year according to
  /// century break. Throws bad_year, bad_day_of_month, or bad_day_month if the date is
  /// invalid. The century break behavior is copied from Java SimpleDateFormat in order to
  /// be consistent with Hive.
  /// In SimpleDateFormat, the century for 2-digit-year breaks at current_time - 80 years.
  /// https://docs.oracle.com/javase/6/docs/api/java/text/SimpleDateFormat.html
  static boost::gregorian::date RealignYear(
      const datetime_parse_util::DateTimeParseResult& dt_result,
      const datetime_parse_util::DateTimeFormatContext& dt_ctx, int day_offset,
      const boost::posix_time::time_duration& t);

  /// Populates 'd' and 't' based on the outcome of parsing the input string into
  /// 'dt_result'. 'dt_ctx' is used for checking if date, time and timezone tokens were
  /// provided.
  static bool PopulateParseResult(
      const datetime_parse_util::DateTimeFormatContext& dt_ctx,
      const datetime_parse_util::DateTimeParseResult& dt_result,
      boost::gregorian::date* d, boost::posix_time::time_duration* t) WARN_UNUSED_RESULT;

  /// Adjusts the time in 't' with 'tz_offset' and handles overflow cases. After adjusting
  /// with 'tz_offset' returns -1 if 't' is negative, +1 if 't' is greater than or equals
  /// to 24 or zero otherwise.
  static int AdjustWithTimezone(boost::posix_time::time_duration* t,
      const boost::posix_time::time_duration& tz_offset);

  /// Returns the year that the current week belongs to. Returned value is in the
  /// [d.year() - 1, d.year() + 1] range.
  /// Weeks start with Monday. Each week's year is the Gregorian year in which the
  /// Thursday falls.
  /// 'd' date is expected to fall in the [1400, 9999] year range. The returned week
  /// numbering year must also fall in the [1400, 9999] range.
  static int GetIso8601WeekNumberingYear(const boost::gregorian::date& d);

  /// Helper function for FormatStringVal.
  static ALWAYS_INLINE void AppendToBuffer(
      const char* buff, int length_to_copy, char* dst, int& pos, int max_length) {
    int max_to_copy = std::min(length_to_copy, max_length - pos);
    std::copy(buff, buff + max_to_copy, dst + pos);
    pos += length_to_copy;
  }
  static ALWAYS_INLINE void AppendToBuffer(
      const string& str, char* dst, int& pos, int max_length) {
    int max_to_copy = std::min((int)str.length(), max_length - pos);
    std::copy(str.cbegin(), str.cbegin() + max_to_copy, dst + pos);
    pos += str.length();
  }
};

}
