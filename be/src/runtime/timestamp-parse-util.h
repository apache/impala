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

#include "runtime/datetime-parse-util.h"

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
  /// yyyy-MM-dd HH:mm:ss.SSSSSSSSS or yyyy-MM-ddTHH:mm:ss.SSSSSSSSS. Either just the
  /// date or just the time may be specified. All components are required in either the
  /// date or time except for the fractional seconds following the period. In the case
  /// of just a date, the time will be set to 00:00:00. In the case of just a time, the
  /// date will be set to invalid.
  /// str -- valid pointer to the string to parse
  /// len -- length of the string to parse (must be > 0)
  /// d -- the date value where the results of the parsing will be placed
  /// t -- the time value where the results of the parsing will be placed
  /// Returns true if the date/time was successfully parsed.
  static bool Parse(const char* str, int len, boost::gregorian::date* d,
      boost::posix_time::time_duration* t);

  /// Parse a date/time string. The data must adhere to the format, otherwise it will be
  /// rejected i.e. no missing tokens. In the case of just a date, the time will be set
  /// to 00:00:00. In the case of just a time, the date will be set to invalid.
  /// str -- valid pointer to the string to parse
  /// len -- length of the string to parse (must be > 0)
  /// dt_ctx -- date/time format context (must contain valid tokens)
  /// d -- the date value where the results of the parsing will be placed
  /// t -- the time value where the results of the parsing will be placed
  /// Returns true if the date/time was successfully parsed.
  static bool Parse(const char* str, int len,
      const datetime_parse_util::DateTimeFormatContext& dt_ctx, boost::gregorian::date* d,
      boost::posix_time::time_duration* t);

  /// Format the date/time values using the given format context. Note that a string
  /// terminator will be appended to the string.
  /// dt_ctx -- date/time format context
  /// d -- the date value
  /// t -- the time value
  /// len -- the output buffer length (should be at least dt_ctx.fmt_exp_len + 1)
  /// buff -- the output string buffer (must be large enough to hold value)
  /// Return the number of characters copied in to the buffer (excluding terminator).
  static int Format(const datetime_parse_util::DateTimeFormatContext& dt_ctx,
      const boost::gregorian::date& d, const boost::posix_time::time_duration& t, int len,
      char* buff);

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
};

}
