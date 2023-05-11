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

#include "runtime/timestamp-parse-util.h"

#include <cctype>
#include <cstdint>
#include <ostream>
#include <vector>

#include <boost/date_time/gregorian/greg_calendar.hpp>
#include <boost/date_time/gregorian/greg_duration.hpp>
#include <boost/date_time/posix_time/posix_time_config.hpp>
#include <boost/date_time/posix_time/posix_time_duration.hpp>
#include <boost/date_time/posix_time/ptime.hpp>
#include <boost/date_time/special_defs.hpp>
#include <boost/exception/exception.hpp>
#include <gutil/strings/numbers.h>

#include "runtime/datetime-iso-sql-format-parser.h"
#include "runtime/datetime-simple-date-format-parser.h"
#include "runtime/date-value.h"
#include "runtime/runtime-state.h"
#include "runtime/string-value.inline.h"
#include "udf/udf-internal.h"

#include "common/names.h"

using boost::gregorian::date;
using boost::gregorian::date_duration;
using boost::gregorian::gregorian_calendar;
using boost::posix_time::hours;
using boost::posix_time::not_a_date_time;
using boost::posix_time::ptime;
using boost::posix_time::time_duration;

namespace impala {

using namespace datetime_parse_util;

// Helper for parse functions to produce return value and set output parameters when
// parsing fails. 'd' and 't' must be non-NULL.
static bool IndicateTimestampParseFailure(date* d, time_duration* t) {
  DCHECK(d != nullptr);
  DCHECK(t != nullptr);
  *d = date();
  *t = time_duration(not_a_date_time);
  return false;
}

bool TimestampParser::ParseSimpleDateFormat(const char* str, int len,
    boost::gregorian::date* d, boost::posix_time::time_duration* t,
    bool accept_time_toks_only) {
  DCHECK(d != nullptr);
  DCHECK(t != nullptr);
  if (UNLIKELY(str == nullptr)) return IndicateTimestampParseFailure(d, t);

  int trimmed_len = len;
  // Remove leading white space.
  while (trimmed_len > 0 && isspace(*str)) {
    ++str;
    --trimmed_len;
  }
  // Strip the trailing blanks.
  while (trimmed_len > 0 && isspace(str[trimmed_len - 1])) --trimmed_len;
  // Strip if there is a 'Z' suffix
  if (trimmed_len > 0 && str[trimmed_len - 1] == 'Z') {
    --trimmed_len;
  } else if (trimmed_len > SimpleDateFormatTokenizer::DEFAULT_SHORT_DATE_TIME_FMT_LEN
      && str[4] == '-') {
    // Strip timezone offset if it seems like a valid timestamp string.
    int curr_pos = SimpleDateFormatTokenizer::DEFAULT_SHORT_DATE_TIME_FMT_LEN;
    // Timezone offset will be at least two bytes long, no need to check last
    // two bytes.
    while (curr_pos < trimmed_len - 2) {
      if (str[curr_pos] == '+' || str[curr_pos] == '-') {
        trimmed_len = curr_pos;
        break;
      }
      ++curr_pos;
    }
  }
  if (UNLIKELY(trimmed_len <= 0)) return IndicateTimestampParseFailure(d, t);

  // Determine the length of relevant input, if we're using one of the default formats.
  int default_fmt_len = min(trimmed_len,
      SimpleDateFormatTokenizer::DEFAULT_DATE_TIME_FMT_LEN);
  // Determine the default formatting context that's required for parsing.
  const DateTimeFormatContext* dt_ctx =
      SimpleDateFormatTokenizer::GetDefaultFormatContext(
          str, default_fmt_len, true, accept_time_toks_only);
  if (dt_ctx != nullptr) {
    return ParseSimpleDateFormat(str, default_fmt_len, *dt_ctx, d, t);
  }
  // Generating context lazily as a fall back if default formats fail.
  // ParseFormatTokenByStr() does not require a template format string.
  DateTimeFormatContext lazy_ctx(str, trimmed_len);
  if (!SimpleDateFormatTokenizer::TokenizeByStr(&lazy_ctx)) {
    return IndicateTimestampParseFailure(d, t);
  }
  dt_ctx = &lazy_ctx;
  return ParseSimpleDateFormat(str, trimmed_len, *dt_ctx, d, t);
}

date TimestampParser::RealignYear(const DateTimeParseResult& dt_result,
    const DateTimeFormatContext& dt_ctx, int day_offset, const time_duration& t) {
  DCHECK(!dt_ctx.century_break_ptime.is_special());
  // Let the century start at AABB and the year parsed be YY, this gives us AAYY.
  int year = dt_result.year + (dt_ctx.century_break_ptime.date().year() / 100) * 100;
  date unshifted_date;
  // The potential actual date (02/29 in unshifted year + 100 years) might be valid
  // even if unshifted date is not, so try to make unshifted date valid by adding 1 day.
  // This makes the behavior closer to Hive.
  if (dt_result.month == 2 && dt_result.day == 29 &&
      !gregorian_calendar::is_leap_year(year)) {
    unshifted_date = date(year, 3, 1);
  } else {
    unshifted_date = date(year, dt_result.month, dt_result.day);
  }
  unshifted_date += date_duration(day_offset);
  // Advance 100 years if parsed time is before the century break.
  // For example if the century breaks at 1937 but dt_result->year = 1936,
  // the correct year would be 2036.
  if (ptime(unshifted_date, t) < dt_ctx.century_break_ptime) {
    return date(year + 100, dt_result.month, dt_result.day) + date_duration(day_offset);
  } else {
    return date(year, dt_result.month, dt_result.day) + date_duration(day_offset);
  }
}

int TimestampParser::AdjustWithTimezone(time_duration* t,
    const time_duration& tz_offset) {
  *t -= tz_offset;
  if (t->is_negative()) {
    *t += hours(24);
    return -1;
  } else if (t->hours() >= 24) {
    *t -= hours(24);
    return 1;
  }
  return 0;
}

bool TimestampParser::PopulateParseResult(const DateTimeFormatContext& dt_ctx,
    const DateTimeParseResult& dt_result, date* d, time_duration* t) {
  int day_offset = 0;
  if (dt_ctx.has_time_toks) {
    *t = time_duration(dt_result.hour, dt_result.minute,
        dt_result.second, dt_result.fraction);
    day_offset = AdjustWithTimezone(t, dt_result.tz_offset);
  } else {
    *t = time_duration(0, 0, 0, 0);
  }
  if (dt_ctx.has_date_toks) {
    try {
      DCHECK(-1 <= day_offset && day_offset <= 1);
      if (dt_result.realign_year) {
        *d = RealignYear(dt_result, dt_ctx, day_offset, *t);
      } else {
        *d = date(dt_result.year, dt_result.month, dt_result.day)
             + date_duration(day_offset);
      }
      // Have to check year lower/upper bound [1400, 9999] here because
      // operator + (date, date_duration) won't throw an exception even if the result is
      // out-of-range.
      if (d->year() < 1400 || d->year() > 9999) {
        // Calling year() on out-of-range date throws an exception itself. This branch is
        // to describe the checking logic but is never taken.
        DCHECK(false);
      }
    } catch (boost::exception&) {
      VLOG_ROW << "Invalid date: " << dt_result.year << "-" << dt_result.month << "-"
          << dt_result.day;
      return false;
    }
  } else {
    *d = date();
  }
  return true;
}

bool TimestampParser::ParseSimpleDateFormat(const char* str, int len,
    const DateTimeFormatContext& dt_ctx, date* d, time_duration* t) {
  DCHECK(dt_ctx.toks.size() > 0);
  DCHECK(d != nullptr);
  DCHECK(t != nullptr);
  DateTimeParseResult dt_result;
  if (UNLIKELY(str == nullptr || len <= 0 ||
      !SimpleDateFormatParser::ParseDateTime(str, len, dt_ctx, &dt_result))) {
    return IndicateTimestampParseFailure(d, t);
  }
  if (!PopulateParseResult(dt_ctx, dt_result, d, t)) {
    return IndicateTimestampParseFailure(d, t);
  }
  return true;
}

bool TimestampParser::ParseIsoSqlFormat(const char* str, int len,
    const DateTimeFormatContext& dt_ctx, date* d, time_duration* t) {
  DCHECK(dt_ctx.toks.size() > 0);
  DCHECK(d != nullptr);
  DCHECK(t != nullptr);
  if (UNLIKELY(str == nullptr || len <= 0)) return IndicateTimestampParseFailure(d, t);

  DateTimeParseResult dt_result;
  if (!IsoSqlFormatParser::ParseDateTime(str, len, dt_ctx, &dt_result)) {
    return IndicateTimestampParseFailure(d, t);
  }

  if (!PopulateParseResult(dt_ctx, dt_result, d, t)) {
    return IndicateTimestampParseFailure(d, t);
  }
  return true;
}

// Formats date and time into dst using the default format
// Short:   yyyy-MM-dd HH:mm:ss
//  Long:   yyyy-MM-dd HH:mm:ss.SSSSSSSSS
// Offsets: 01234567890123456789012345678

int TimestampParser::FormatDefault(const date& d, const time_duration& t, char* dst) {
  if (UNLIKELY(d.is_special() || t.is_special())) return -1;
  const auto ymd = d.year_month_day();
  ZeroPad(dst, ymd.year, 4);
  ZeroPad(dst + 5, ymd.month, 2);
  ZeroPad(dst + 8, ymd.day, 2);
  const auto tot_sec = t.total_seconds();
  ZeroPad(dst + 11, tot_sec / 3600, 2);
  ZeroPad(dst + 14, (tot_sec / 60) % 60, 2);
  ZeroPad(dst + 17, tot_sec % 60, 2);
  dst[7] = dst[4] = '-';
  dst[10] = ' ';
  dst[16] = dst[13] = ':';

  if (LIKELY(t.fractional_seconds() > 0)) {
    dst[19] = '.';
    ZeroPad(dst + 20, t.fractional_seconds(), 9);
    return SimpleDateFormatTokenizer::DEFAULT_DATE_TIME_FMT_LEN;
  }
  return SimpleDateFormatTokenizer::DEFAULT_SHORT_DATE_TIME_FMT_LEN;
}

int TimestampParser::Format(const DateTimeFormatContext& dt_ctx, const date& d,
    const time_duration& t, int max_length, char* dst) {
  DCHECK(dt_ctx.toks.size() > 0);
  if (dt_ctx.has_date_toks && d.is_special()) return -1;
  if (dt_ctx.has_time_toks && t.is_special()) return -1;
  int pos = 0;
  char buff[12];
  for (const DateTimeFormatToken& tok: dt_ctx.toks) {
    int32_t num_val = -1;
    switch (tok.type) {
      case YEAR:
      case ROUND_YEAR: {
        num_val = AdjustYearToLength(d.year(), tok.divisor);
        break;
      }
      case QUARTER_OF_YEAR: {
        num_val = GetQuarter(d.month());
        break;
      }
      case MONTH_IN_YEAR: num_val = d.month().as_number(); break;
      case MONTH_NAME:
      case MONTH_NAME_SHORT: {
        AppendToBuffer(FormatMonthName(d.month().as_number(), tok), dst, pos, max_length);
        break;
      }
      case WEEK_OF_YEAR: {
        num_val = GetWeekOfYear(d.year(), d.month(), d.day());
        break;
      }
      case WEEK_OF_MONTH: {
        num_val = GetWeekOfMonth(d.day());
        break;
      }
      case DAY_OF_WEEK: {
        // Value in [1-7] where 1 represents Sunday, 2 represents Monday, etc.
        num_val = d.day_of_week() + 1;
        break;
      }
      case DAY_IN_MONTH: num_val = d.day(); break;
      case DAY_IN_YEAR: {
        num_val = GetDayInYear(d.year(), d.month(), d.day());
        break;
      }
      case DAY_NAME:
      case DAY_NAME_SHORT: {
        AppendToBuffer(FormatDayName(d.day_of_week() + 1, tok), dst, pos, max_length);
        break;
      }
      case HOUR_IN_DAY: num_val = t.hours(); break;
      case HOUR_IN_HALF_DAY: {
        num_val = t.hours();
        if (num_val == 0) num_val = 12;
        if (num_val > 12) num_val -= 12;
        break;
      }
      case MERIDIEM_INDICATOR: {
        const MERIDIEM_INDICATOR_TEXT* indicator_txt = (tok.len == 2) ? &AM : &AM_LONG;
        if (t.hours() >= 12) {
          indicator_txt = (tok.len == 2) ? &PM : &PM_LONG;
        }
        AppendToBuffer((isupper(*tok.val)) ? indicator_txt->first : indicator_txt->second,
            tok.len, dst, pos, max_length);
        break;
      }
      case MINUTE_IN_HOUR: num_val = t.minutes(); break;
      case SECOND_IN_MINUTE: num_val = t.seconds(); break;
      case SECOND_IN_DAY: {
          num_val = t.hours() * 3600 + t.minutes() * 60 + t.seconds();
          break;
      }
      case FRACTION: {
        num_val = t.fractional_seconds();
        if (num_val > 0 && tok.divisor > 1) num_val /= tok.divisor;
        break;
      }
      case SEPARATOR:
      case ISO8601_TIME_INDICATOR:
      case ISO8601_ZULU_INDICATOR: {
        AppendToBuffer(tok.val, tok.len, dst, pos, max_length);
        break;
      }
      case TZ_OFFSET: {
        break;
      }
      case TEXT: {
        AppendToBuffer(FormatTextToken(tok), dst, pos, max_length);
        break;
      }
      case ISO8601_WEEK_NUMBERING_YEAR: {
        num_val = AdjustYearToLength(GetIso8601WeekNumberingYear(d), tok.divisor);
        break;
      }
      case ISO8601_WEEK_OF_YEAR: {
        num_val = d.week_number();
        break;
      }
      case ISO8601_DAY_OF_WEEK: {
        // day_of_week() returns 0 for Sunday, 1 for Monday and 6 for Saturday.
        num_val = d.day_of_week();
        // We need to output 1 for Monday and 7 for Sunday.
        if (num_val == 0) num_val = 7;
        break;
      }
      default: DCHECK(false) << "Unknown date/time format token";
    }
    if (num_val > -1) {
      char* buff_end = FastInt32ToBufferLeft(num_val, &buff[0]);
      int written_length = buff_end - (&buff[0]);
      DCHECK_GT(written_length, 0);
      if (!tok.fm_modifier && written_length < tok.len) {
        for (int i = (tok.len - written_length); (i > 0) && (pos < max_length); i--) {
          *(dst + pos) = '0';
          pos++;
        }
      }
      AppendToBuffer(&buff[0], written_length, dst, pos, max_length);
    }
    DCHECK_LE(pos, max_length) << "Maximum buffer length exceeded!";
  }
  return pos;
}

int TimestampParser::GetIso8601WeekNumberingYear(const boost::gregorian::date& d) {
  DCHECK(!d.is_special());
  DCHECK(1400 <= d.year() && d.year() <= 9999);

  static const boost::gregorian::date epoch(1970, 1, 1);
  DateValue dv((d - epoch).days());
  DCHECK(dv.IsValid());

  int week_numbering_year = dv.Iso8601WeekNumberingYear();
  // 1400.01.01 is Wednesday. 9999.12.31 is Friday.
  // This means that week_numbering_year must fall in the [1400, 9999] range.
  DCHECK(1400 <= week_numbering_year && week_numbering_year <= 9999);
  return week_numbering_year;
}

}
