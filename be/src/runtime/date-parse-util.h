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

#include "runtime/date-value.h"
#include "runtime/datetime-parse-util.h"

namespace impala {

/// Used for parsing both default and custom formatted date values.
class DateParser {
 public:
  /// Parse a default date string. The default timestamp format is: yyyy-MM-dd.
  /// str -- valid pointer to the string to parse
  /// len -- length of the string to parse (must be > 0)
  /// accept_time_toks -- if true, time tokens are accepted as well. Otherwise, they are
  /// rejected.
  /// date -- the date value where the results of the parsing will be placed
  /// Returns true if the date was successfully parsed.
  static bool Parse(const char* str, int len, bool accept_time_toks, DateValue* date)
      WARN_UNUSED_RESULT;

  /// Parse a date string. The date string must adhere to the format, otherwise it will be
  /// rejected i.e. no missing tokens.
  /// str -- valid pointer to the string to parse
  /// len -- length of the string to parse (must be > 0)
  /// dt_ctx -- format context (must contain valid date tokens, time tokens are accepted
  /// and ignored)
  /// date -- the date value where the results of the parsing will be placed
  /// Returns true if the date was successfully parsed.
  static bool Parse(const char* str, int len,
      const datetime_parse_util::DateTimeFormatContext& dt_ctx, DateValue* date)
      WARN_UNUSED_RESULT;

  /// Format the date values using the given format context. Note that a string terminator
  /// will be appended to the string.
  /// dt_ctx -- date format context
  /// date -- the date value
  /// len -- the output buffer length (should be at least dt_ctx.fmt_exp_len + 1)
  /// buff -- the output string buffer (must be large enough to hold value)
  /// Return the number of characters copied in to the buffer (excluding terminator).
  static int Format(const datetime_parse_util::DateTimeFormatContext& dt_ctx,
      const DateValue& date, int len, char* buff);

 private:
  /// Helper function finding the correct century for 1 or 2 digit year according to
  /// century break. The century break behavior is copied from Java SimpleDateFormat in
  /// order to be consistent with Hive.
  /// In SimpleDateFormat, the century for 2-digit-year breaks at current_time - 80 years.
  /// https://docs.oracle.com/javase/6/docs/api/java/text/SimpleDateFormat.html
  /// Returns the realigned year value for dt_result.year.
  /// This function should be called only if dt_result.realign_year is true and
  /// dt_result.year < 100.
  static int RealignYear(const datetime_parse_util::DateTimeParseResult& dt_result,
      const boost::posix_time::ptime& century_break);
};

}
