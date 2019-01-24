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

#include "common/names.h"
#include "runtime/datetime-iso-sql-format-parser.h"
#include "runtime/datetime-simple-date-format-parser.h"
#include "runtime/runtime-state.h"
#include "runtime/string-value.inline.h"
#include "runtime/timestamp-value.h"
#include "udf/udf-internal.h"
#include "util/string-parser.h"

#include "cctype"

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
    boost::gregorian::date* d, boost::posix_time::time_duration* t) {
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
  } else if (trimmed_len > SimpleDateFormatTokenizer::DEFAULT_TIME_FMT_LEN &&
      (str[4] == '-' || str[2] == ':')) {
    // Strip timezone offset if it seems like a valid timestamp string.
    int curr_pos = SimpleDateFormatTokenizer::DEFAULT_TIME_FMT_LEN;
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
      SimpleDateFormatTokenizer::GetDefaultFormatContext(str, default_fmt_len, true,
          true);

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

string TimestampParser::Format(const DateTimeFormatContext& dt_ctx, const date& d,
      const time_duration& t) {
  DCHECK(dt_ctx.toks.size() > 0);
  if (dt_ctx.has_date_toks && d.is_special()) return "";
  if (dt_ctx.has_time_toks && t.is_special()) return "";
  string result;
  result.reserve(dt_ctx.fmt_out_len);
  for (const DateTimeFormatToken& tok: dt_ctx.toks) {
    int32_t num_val = -1;
    const char* str_val = NULL;
    int str_val_len = 0;
    switch (tok.type) {
      case YEAR:
      case ROUND_YEAR: {
        num_val = d.year();
        if (tok.len < 4) {
          int adjust_factor = std::pow(10, tok.len);
          num_val %= adjust_factor;
        }
        break;
      }
      case MONTH_IN_YEAR: num_val = d.month().as_number(); break;
      case MONTH_IN_YEAR_SLT: {
        str_val = d.month().as_short_string();
        str_val_len = 3;
        break;
      }
      case DAY_IN_MONTH: num_val = d.day(); break;
      case DAY_IN_YEAR: {
        num_val = GetDayInYear(d.year(), d.month(), d.day());
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
        str_val_len = tok.len;
        str_val = (isupper(*tok.val)) ? indicator_txt->first : indicator_txt->second;
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
        if (num_val > 0) for (int j = tok.len; j < 9; ++j) num_val /= 10;
        break;
      }
      case SEPARATOR:
      case ISO8601_TIME_INDICATOR:
      case ISO8601_ZULU_INDICATOR: {
        str_val = tok.val;
        str_val_len = tok.len;
        break;
      }
      case TZ_OFFSET: {
        break;
      }
      default: DCHECK(false) << "Unknown date/time format token";
    }
    if (num_val > -1) {
      string tmp_str = std::to_string(num_val);
      if (tmp_str.length() < tok.len) tmp_str.insert(0, tok.len - tmp_str.length(), '0');
      result.append(tmp_str);
    } else {
      DCHECK(str_val != nullptr && str_val_len > 0);
      result.append(str_val, str_val_len);
    }
  }
  return result;
}

}
