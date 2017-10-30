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

#include <boost/assign/list_of.hpp>
#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/unordered_map.hpp>

#include "runtime/string-value.inline.h"
#include "runtime/timestamp-value.h"
#include "util/string-parser.h"

namespace assign = boost::assign;
using boost::unordered_map;
using boost::gregorian::date;
using boost::gregorian::date_duration;
using boost::gregorian::gregorian_calendar;
using boost::posix_time::hours;
using boost::posix_time::not_a_date_time;
using boost::posix_time::ptime;
using boost::posix_time::time_duration;

namespace impala {

/// Stores the results of parsing a date/time string.
struct DateTimeParseResult {
  int year;
  int month;
  int day;
  int hour;
  int minute;
  int second;
  int32_t fraction;
  boost::posix_time::time_duration tz_offset;
  // Whether to realign the year for 2-digit year format
  bool realign_year;

  DateTimeParseResult()
    : year(0),
      month(0),
      day(0),
      hour(0),
      minute(0),
      second(0),
      fraction(0),
      tz_offset(0,0,0,0),
      realign_year(false) {
  }
};

void DateTimeFormatContext::SetCenturyBreak(const TimestampValue &now) {
  auto& now_date = now.date();
  // If the century break is at an invalid 02/29, set it to 02/28 for consistency with
  // Hive.
  if (now_date.month() == 2 && now_date.day() == 29 &&
      !gregorian_calendar::is_leap_year(now_date.year() - 80)) {
    century_break_ptime = ptime(date(now_date.year() - 80, 2, 28), now.time());
  } else {
    century_break_ptime = ptime(
        date(now_date.year() - 80, now_date.month(), now_date.day()), now.time());
  }
}

bool TimestampParser::initialized_ = false;

/// Lazily initialized pseudo-constant hashmap for mapping month names to an index.
static unordered_map<StringValue, int> REV_MONTH_INDEX;

DateTimeFormatContext TimestampParser::DEFAULT_SHORT_DATE_TIME_CTX;
DateTimeFormatContext TimestampParser::DEFAULT_SHORT_ISO_DATE_TIME_CTX;
DateTimeFormatContext TimestampParser::DEFAULT_DATE_CTX;
DateTimeFormatContext TimestampParser::DEFAULT_TIME_CTX;
DateTimeFormatContext TimestampParser::DEFAULT_DATE_TIME_CTX[10];
DateTimeFormatContext TimestampParser::DEFAULT_ISO_DATE_TIME_CTX[10];
DateTimeFormatContext TimestampParser::DEFAULT_TIME_FRAC_CTX[10];

void TimestampParser::Init() {
  if (TimestampParser::initialized_) return;
  // This needs to be lazily init'd because a StringValues hash function will be invoked
  // for each entry that's placed in the map. The hash function expects that
  // CpuInfo::Init() has already been called.
  REV_MONTH_INDEX = boost::unordered_map<StringValue, int>({
      {StringValue("jan"), 1}, {StringValue("feb"), 2},
      {StringValue("mar"), 3}, {StringValue("apr"), 4},
      {StringValue("may"), 5}, {StringValue("jun"), 6},
      {StringValue("jul"), 7}, {StringValue("aug"), 8},
      {StringValue("sep"), 9}, {StringValue("oct"), 10},
      {StringValue("nov"), 11}, {StringValue("dec"), 12}
  });

  // Setup the default date/time context yyyy-MM-dd HH:mm:ss.SSSSSSSSS
  const char* DATE_TIME_CTX_FMT = "yyyy-MM-dd HH:mm:ss.SSSSSSSSS";
  const int FRACTIONAL_MAX_LEN = 9;
  for (int i = FRACTIONAL_MAX_LEN; i >= 0; --i) {
    DEFAULT_DATE_TIME_CTX[i].Reset(DATE_TIME_CTX_FMT,
        DEFAULT_DATE_TIME_FMT_LEN - (FRACTIONAL_MAX_LEN - i));
    ParseFormatTokens(&DEFAULT_DATE_TIME_CTX[i]);
  }

  // Setup the default ISO date/time context yyyy-MM-ddTHH:mm:ss.SSSSSSSSS
  for (int i = FRACTIONAL_MAX_LEN; i >= 0; --i) {
    DEFAULT_ISO_DATE_TIME_CTX[i].Reset("yyyy-MM-ddTHH:mm:ss.SSSSSSSSS",
        DEFAULT_DATE_TIME_FMT_LEN - (FRACTIONAL_MAX_LEN - i));
    ParseFormatTokens(&DEFAULT_ISO_DATE_TIME_CTX[i]);
  }

  // Setup the short default date/time context yyyy-MM-dd HH:mm:ss
  DEFAULT_SHORT_DATE_TIME_CTX.Reset("yyyy-MM-dd HH:mm:ss",
      DEFAULT_SHORT_DATE_TIME_FMT_LEN);
  ParseFormatTokens(&DEFAULT_SHORT_DATE_TIME_CTX);

  // Setup the short default ISO date/time context yyyy-MM-ddTHH:mm:ss
  DEFAULT_SHORT_ISO_DATE_TIME_CTX.Reset("yyyy-MM-ddTHH:mm:ss",
      DEFAULT_SHORT_DATE_TIME_FMT_LEN);
  ParseFormatTokens(&DEFAULT_SHORT_ISO_DATE_TIME_CTX);

  // Setup the default short date context yyyy-MM-dd
  DEFAULT_DATE_CTX.Reset("yyyy-MM-dd", DEFAULT_DATE_FMT_LEN);
  ParseFormatTokens(&DEFAULT_DATE_CTX);

  // Setup the default short time context HH:mm:ss
  DEFAULT_TIME_CTX.Reset("HH:mm:ss", DEFAULT_TIME_FMT_LEN);
  ParseFormatTokens(&DEFAULT_TIME_CTX);

  // Setup the default short time context with fractional seconds HH:mm:ss.SSSSSSSSS
  for (int i = FRACTIONAL_MAX_LEN; i >= 0; --i) {
    DEFAULT_TIME_FRAC_CTX[i].Reset(DATE_TIME_CTX_FMT + 11,
        DEFAULT_TIME_FRAC_FMT_LEN - (FRACTIONAL_MAX_LEN - i));
    ParseFormatTokens(&DEFAULT_TIME_FRAC_CTX[i]);
  }
  // Flag that the parser is ready.
  TimestampParser::initialized_ = true;
}

bool TimestampParser::ParseFormatTokens(DateTimeFormatContext* dt_ctx) {
  DCHECK(dt_ctx != NULL);
  DCHECK(dt_ctx->fmt != NULL);
  DCHECK(dt_ctx->fmt_len > 0);
  DCHECK(dt_ctx->toks.size() == 0);
  const char* str_begin = dt_ctx->fmt;
  const char* str_end = str_begin + dt_ctx->fmt_len;
  const char* str = str_begin;
  // Parse the tokens from the format string
  while (str < str_end) {
    if (isdigit(*str)) return false;
    // Ignore T|Z|non aA-zZ chars but track them as separators (required for printing).
    if ((*str == 'T') || (*str == 'Z') || (!isalpha(*str))) {
      if (dt_ctx->has_time_toks && IsValidTZOffset(str, str_end)) {
        // TZ offset must come at the end of the format.
        dt_ctx->toks.push_back(DateTimeFormatToken(TZ_OFFSET, str - str_begin,
            str_end - str, str));
        break;
      }
      dt_ctx->toks.push_back(DateTimeFormatToken(SEPARATOR, str - str_begin, 1, str));
      ++str;
      continue;
    }
    // Not a separator, verify that the previous token is either a separator or has
    // length >1, i.e., it is not a variable length token.
    if (!dt_ctx->toks.empty()) {
      const DateTimeFormatToken& prev = dt_ctx->toks.back();
      if (UNLIKELY(prev.type != SEPARATOR && prev.len == 1)) return false;
    }
    DateTimeFormatTokenType tok_type = UNKNOWN;
    switch (*str) {
      case 'y': tok_type = YEAR; break;
      case 'M': tok_type = MONTH_IN_YEAR; break;
      case 'd': tok_type = DAY_IN_MONTH; break;
      case 'H': tok_type = HOUR_IN_DAY; break;
      case 'm': tok_type = MINUTE_IN_HOUR; break;
      case 's': tok_type = SECOND_IN_MINUTE; break;
      case 'S': tok_type = FRACTION; break;
      // Error on aA-zZ reserved characters that are not used yet.
      default: return false;
    }
    dt_ctx->has_date_toks |= tok_type < HOUR_IN_DAY;
    dt_ctx->has_time_toks |= tok_type >= HOUR_IN_DAY;
    // Get the token group length
    int tok_len = 1;
    char tok_chr = *str;
    const char* curr_tok_chr = str + 1;
    while (curr_tok_chr < str_end) {
      if (*curr_tok_chr != tok_chr) break;
      ++tok_len;
      ++curr_tok_chr;
    }
    if (tok_type == MONTH_IN_YEAR) {
      if (UNLIKELY(tok_len > 3)) return false;
      if (tok_len == 3) tok_type = MONTH_IN_YEAR_SLT;
    }
    // In an output scenario, fmt_out_len is used to determine the print buffer size.
    // If the format uses short token groups e.g. yyyy-MM-d, there must to be enough
    // room in the buffer for wider values e.g. 2013-12-16.
    if (tok_len == 1) ++dt_ctx->fmt_out_len;
    DateTimeFormatToken tok(tok_type, str - str_begin, tok_len, str);
    str += tok.len;
    dt_ctx->toks.push_back(tok);
  }
  return dt_ctx->has_date_toks || dt_ctx->has_time_toks;
}

bool TimestampParser::Parse(const char* str, int len, boost::gregorian::date* d,
    boost::posix_time::time_duration* t) {
  DCHECK(TimestampParser::initialized_);
  DCHECK(d != NULL);
  DCHECK(t != NULL);
  if (UNLIKELY(str == NULL || len <= 0)) {
    *d = boost::gregorian::date();
    *t = boost::posix_time::time_duration(boost::posix_time::not_a_date_time);
    return false;
  }
  // Remove leading white space.
  while (len > 0 && isspace(*str)) {
    ++str;
    --len;
  }
  // Strip the trailing blanks.
  while (len > 0 && isspace(str[len - 1])) --len;
  // Strip if there is a 'Z' suffix
  if (len > 0 && str[len - 1] == 'Z') {
    --len;
  } else if (len > DEFAULT_TIME_FMT_LEN && (str[4] == '-' || str[2] == ':')) {
    // Strip timezone offset if it seems like a valid timestamp string.
    int curr_pos = DEFAULT_TIME_FMT_LEN;
    // Timezone offset will be at least two bytes long, no need to check last
    // two bytes.
    while (curr_pos < len - 2) {
      if (str[curr_pos] == '+' || str[curr_pos] == '-') {
        len = curr_pos;
        break;
      }
      ++curr_pos;
    }
  }

  // Only process what we have to.
  if (len > DEFAULT_DATE_TIME_FMT_LEN) len = DEFAULT_DATE_TIME_FMT_LEN;
  // Determine the default formatting context that's required for parsing.
  DateTimeFormatContext* dt_ctx = NULL;
  if (LIKELY(len >= DEFAULT_TIME_FMT_LEN)) {
    // This string starts with a date component
    if (str[4] == '-') {
      switch (len) {
        case DEFAULT_DATE_FMT_LEN: {
          dt_ctx = &DEFAULT_DATE_CTX;
          break;
        }
        case DEFAULT_SHORT_DATE_TIME_FMT_LEN:  {
          switch (str[10]) {
            case ' ': dt_ctx = &DEFAULT_SHORT_DATE_TIME_CTX; break;
            case 'T': dt_ctx = &DEFAULT_SHORT_ISO_DATE_TIME_CTX; break;
          }
          break;
        }
        case DEFAULT_DATE_TIME_FMT_LEN: {
          switch (str[10]) {
            case ' ': dt_ctx = &DEFAULT_DATE_TIME_CTX[9]; break;
            case 'T': dt_ctx = &DEFAULT_ISO_DATE_TIME_CTX[9]; break;
          }
          break;
        }
        default: {
          // There is likely a fractional component that's below the expected 9 chars.
          // We will need to work out which default context to use that corresponds to
          // the fractional length in the string.
          if (LIKELY(len > DEFAULT_SHORT_DATE_TIME_FMT_LEN)) {
            switch (str[10]) {
              case ' ': {
                dt_ctx =
                    &DEFAULT_DATE_TIME_CTX[len - DEFAULT_SHORT_DATE_TIME_FMT_LEN - 1];
                break;
              }
              case 'T': {
                dt_ctx = &DEFAULT_ISO_DATE_TIME_CTX
                    [len - DEFAULT_SHORT_DATE_TIME_FMT_LEN - 1];
                break;
              }
            }
          }
          break;
        }
      }
    } else if (str[2] == ':') {
      if (len > DEFAULT_TIME_FRAC_FMT_LEN) len = DEFAULT_TIME_FRAC_FMT_LEN;
      if (len > DEFAULT_TIME_FMT_LEN && str[8] == '.') {
        dt_ctx = &DEFAULT_TIME_FRAC_CTX[len - DEFAULT_TIME_FMT_LEN - 1];
      } else {
        dt_ctx = &DEFAULT_TIME_CTX;
      }
    }
  }
  if (LIKELY(dt_ctx != NULL)) {
    return Parse(str, len, *dt_ctx, d, t);
  } else {
    *d = boost::gregorian::date();
    *t = boost::posix_time::time_duration(boost::posix_time::not_a_date_time);
    return false;
  }
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

bool TimestampParser::Parse(const char* str, int len, const DateTimeFormatContext& dt_ctx,
    date* d, time_duration* t) {
  DCHECK(TimestampParser::initialized_);
  DCHECK(dt_ctx.toks.size() > 0);
  DCHECK(d != NULL);
  DCHECK(t != NULL);
  DateTimeParseResult dt_result;
  int day_offset = 0;
  if (UNLIKELY(str == NULL || len <= 0 ||
          !ParseDateTime(str, len, dt_ctx, &dt_result))) {
    *d = date();
    *t = time_duration(not_a_date_time);
    return false;
  }
  if (dt_ctx.has_time_toks) {
    *t = time_duration(dt_result.hour, dt_result.minute,
        dt_result.second, dt_result.fraction);
    *t -= dt_result.tz_offset;
    if (t->is_negative()) {
      *t += hours(24);
      day_offset = -1;
    } else if (t->hours() >= 24) {
      *t -= hours(24);
      day_offset = 1;
    }
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
      *d = date();
      *t = time_duration(not_a_date_time);
      return false;
    }
  } else {
    *d = date();
  }
  return true;
}

int TimestampParser::Format(const DateTimeFormatContext& dt_ctx,
    const boost::gregorian::date& d, const boost::posix_time::time_duration& t,
    int len, char* buff) {
  DCHECK(TimestampParser::initialized_);
  DCHECK(dt_ctx.toks.size() > 0);
  DCHECK(len > dt_ctx.fmt_out_len);
  DCHECK(buff != NULL);
  if (dt_ctx.has_date_toks && d.is_special()) return -1;
  if (dt_ctx.has_time_toks && t.is_special()) return -1;
  char* str = buff;
  for (const DateTimeFormatToken& tok: dt_ctx.toks) {
    int32_t num_val = -1;
    const char* str_val = NULL;
    int str_val_len = 0;
    switch (tok.type) {
      case YEAR: {
        num_val = d.year();
        if (tok.len <= 3) num_val %= 100;
        break;
      }
      case MONTH_IN_YEAR: num_val = d.month().as_number(); break;
      case MONTH_IN_YEAR_SLT: {
        str_val = d.month().as_short_string();
        str_val_len = 3;
        break;
      }
      case DAY_IN_MONTH: num_val = d.day(); break;
      case HOUR_IN_DAY: num_val = t.hours(); break;
      case MINUTE_IN_HOUR: num_val = t.minutes(); break;
      case SECOND_IN_MINUTE: num_val = t.seconds(); break;
      case FRACTION: {
        num_val = t.fractional_seconds();
        if (num_val > 0) for (int j = tok.len; j < 9; ++j) num_val /= 10;
        break;
      }
      case SEPARATOR: {
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
      str += sprintf(str, "%0*d", tok.len, num_val);
    } else {
      memcpy(str, str_val, str_val_len);
      str += str_val_len;
    }
  }
  /// Terminate the string
  *str = '\0';
  return str - buff;
}

bool TimestampParser::ParseDateTime(const char* str, int str_len,
    const DateTimeFormatContext& dt_ctx, DateTimeParseResult* dt_result) {
  DCHECK(dt_ctx.fmt_len > 0);
  DCHECK(dt_ctx.toks.size() > 0);
  DCHECK(dt_result != NULL);
  if (str_len <= 0 || str_len < dt_ctx.fmt_len || str == NULL) return false;
  StringParser::ParseResult status;
  // Keep track of the number of characters we need to shift token positions by.
  // Variable-length tokens will result in values > 0;
  int shift_len = 0;
  for (const DateTimeFormatToken& tok: dt_ctx.toks) {
    const char* tok_val = str + tok.pos + shift_len;
    if (tok.type == SEPARATOR) {
      if (UNLIKELY(*tok_val != *tok.val)) return false;
      continue;
    }
    int tok_len = tok.len;
    const char* str_end = str + str_len;
    // In case of single-character tokens we scan ahead to the next separator.
    if (UNLIKELY(tok_len == 1)) {
      while ((tok_val + tok_len < str_end) && isdigit(*(tok_val + tok_len))) {
        ++tok_len;
        ++shift_len;
      }
    }
    switch (tok.type) {
      case YEAR: {
        dt_result->year = StringParser::StringToInt<int>(tok_val, tok_len, &status);
        if (UNLIKELY(StringParser::PARSE_SUCCESS != status)) return false;
        if (UNLIKELY(dt_result->year < 0 || dt_result->year > 9999)) return false;
        // Year in "Y" and "YY" format should be in the interval
        // [current time - 80 years, current time + 20 years)
        if (tok_len <= 2) dt_result->realign_year = true;
        break;
      }
      case MONTH_IN_YEAR: {
        dt_result->month = StringParser::StringToInt<int>(tok_val, tok_len, &status);
        if (UNLIKELY(StringParser::PARSE_SUCCESS != status)) return false;
        if (UNLIKELY(dt_result->month < 1 || dt_result->month > 12)) return false;
        break;
      }
      case MONTH_IN_YEAR_SLT: {
        char raw_buff[tok.len];
        std::transform(tok_val, tok_val + tok.len, raw_buff, ::tolower);
        StringValue buff(raw_buff, tok.len);
        boost::unordered_map<StringValue, int>::const_iterator iter =
            REV_MONTH_INDEX.find(buff);
        if (UNLIKELY(iter == REV_MONTH_INDEX.end())) return false;
        dt_result->month = iter->second;
        break;
      }
      case DAY_IN_MONTH: {
        dt_result->day = StringParser::StringToInt<int>(tok_val, tok_len, &status);
        if (UNLIKELY(StringParser::PARSE_SUCCESS != status)) return false;
        // TODO: Validate that the value of day is correct for the given month.
        if (UNLIKELY(dt_result->day < 1 || dt_result->day > 31)) return false;
        break;
      }
      case HOUR_IN_DAY: {
        dt_result->hour = StringParser::StringToInt<int>(tok_val, tok_len, &status);
        if (UNLIKELY(StringParser::PARSE_SUCCESS != status)) return false;
        if (UNLIKELY(dt_result->hour < 0 || dt_result->hour > 23)) return false;
        break;
      }
      case MINUTE_IN_HOUR: {
        dt_result->minute = StringParser::StringToInt<int>(tok_val, tok_len, &status);
        if (UNLIKELY(StringParser::PARSE_SUCCESS != status)) return false;
        if (UNLIKELY(dt_result->minute < 0 || dt_result->minute > 59)) return false;
        break;
      }
      case SECOND_IN_MINUTE: {
        dt_result->second = StringParser::StringToInt<int>(tok_val, tok_len, &status);
        if (UNLIKELY(StringParser::PARSE_SUCCESS != status)) return false;
        if (UNLIKELY(dt_result->second < 0 || dt_result->second > 59)) return false;
        break;
      }
      case FRACTION: {
        dt_result->fraction =
            StringParser::StringToInt<int32_t>(tok_val, tok_len, &status);
        if (UNLIKELY(StringParser::PARSE_SUCCESS != status)) return false;
        // A user may specify a time of 04:30:22.1238, the parser will return 1238 for
        // the fractional portion. This does not represent the intended value of
        // 123800000, therefore the number must be scaled up.
        for (int i = tok_len; i < 9; ++i) dt_result->fraction *= 10;
        break;
      }
      case TZ_OFFSET: {
        if (tok_val[0] != '+' && tok_val[0] != '-') return false;
        int sign = tok_val[0] == '-' ? -1 : 1;
        int minute = 0;
        int hour = StringParser::StringToInt<int>(tok_val + 1, 2, &status);
        if (UNLIKELY(StringParser::PARSE_SUCCESS != status ||
            hour < 0 || hour > 23)) {
          return false;
        }
        switch (tok_len) {
          case 6: {
            // +hh:mm
            minute = StringParser::StringToInt<int>(tok_val + 4, 2, &status);
            break;
          }
          case 5: {
            // +hh:mm
            minute = StringParser::StringToInt<int>(tok_val + 3, 2, &status);
            break;
          }
          case 3: {
            // +hh
            break;
          }
          default: {
            // Invalid timezone offset length.
            return false;
          }
        }
        if (UNLIKELY(StringParser::PARSE_SUCCESS != status ||
            minute < 0 || minute > 59)) {
          return false;
        }
        dt_result->tz_offset = boost::posix_time::time_duration(sign * hour,
            sign * minute, 0, 0);
        break;
      }
      default: DCHECK(false) << "Unknown date/time format token";
    }
  }
  return true;
}

bool TimestampParser::IsValidTZOffset(const char* str_begin, const char* str_end) {
  if (*str_begin == '+' || *str_begin == '-') {
    ++str_begin;
    switch(str_end - str_begin) {
      case 5:   // hh:mm
        return strncmp(str_begin, "hh:mm", 5) == 0;
      case 4:   // hhmm
        return strncmp(str_begin, "hhmm", 4) == 0;
      case 2:   // hh
        return strncmp(str_begin, "hh", 2) == 0;
      default:
        break;
    }
  }
  return false;
}


}
