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

#include "runtime/datetime-parse-util.h"

#include <algorithm>

#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/unordered_map.hpp>

#include "cctz/civil_time.h"
#include "exprs/timestamp-functions.h"
#include "runtime/timestamp-value.h"
#include "util/string-parser.h"

#include "common/names.h"

using boost::unordered_map;
using boost::gregorian::date;
using boost::gregorian::gregorian_calendar;
using boost::posix_time::ptime;
using boost::posix_time::time_duration;

namespace impala {

namespace datetime_parse_util {

void DateTimeFormatContext::SetCenturyBreak(const TimestampValue& now) {
  const date& now_date = now.date();
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

/// Used to indicate if the parsing state has been initialized.
bool initialized = false;

/// Lazily initialized pseudo-constant hashmap for mapping month names to an index.
unordered_map<StringValue, int> REV_MONTH_INDEX;

DateTimeFormatContext DEFAULT_SHORT_DATE_TIME_CTX;
DateTimeFormatContext DEFAULT_SHORT_ISO_DATE_TIME_CTX;
DateTimeFormatContext DEFAULT_DATE_CTX;
DateTimeFormatContext DEFAULT_TIME_CTX;
DateTimeFormatContext DEFAULT_DATE_TIME_CTX[10];
DateTimeFormatContext DEFAULT_ISO_DATE_TIME_CTX[10];
DateTimeFormatContext DEFAULT_TIME_FRAC_CTX[10];

void InitParseCtx() {
  if (initialized) return;
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
  DEFAULT_SHORT_DATE_TIME_CTX.Reset("yyyy-MM-dd HH:mm:ss");
  ParseFormatTokens(&DEFAULT_SHORT_DATE_TIME_CTX);

  // Setup the short default ISO date/time context yyyy-MM-ddTHH:mm:ss
  DEFAULT_SHORT_ISO_DATE_TIME_CTX.Reset("yyyy-MM-ddTHH:mm:ss");
  ParseFormatTokens(&DEFAULT_SHORT_ISO_DATE_TIME_CTX);

  // Setup the default short date context yyyy-MM-dd
  DEFAULT_DATE_CTX.Reset("yyyy-MM-dd");
  ParseFormatTokens(&DEFAULT_DATE_CTX);

  // Setup the default short time context HH:mm:ss
  DEFAULT_TIME_CTX.Reset("HH:mm:ss");
  ParseFormatTokens(&DEFAULT_TIME_CTX);

  // Setup the default short time context with fractional seconds HH:mm:ss.SSSSSSSSS
  for (int i = FRACTIONAL_MAX_LEN; i >= 0; --i) {
    DEFAULT_TIME_FRAC_CTX[i].Reset(DATE_TIME_CTX_FMT + 11,
        DEFAULT_TIME_FRAC_FMT_LEN - (FRACTIONAL_MAX_LEN - i));
    ParseFormatTokens(&DEFAULT_TIME_FRAC_CTX[i]);
  }
  // Flag that the parser is ready.
  initialized = true;
}

bool IsParseCtxInitialized() {
  return initialized;
}

bool IsValidTZOffset(const char* str_begin, const char* str_end) {
  if (*str_begin == '+' || *str_begin == '-') {
    ++str_begin;
    switch (str_end - str_begin) {
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

bool ParseFormatTokens(DateTimeFormatContext* dt_ctx, bool accept_time_toks) {
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

    // If time tokens are accepted, track T|Z as separators.
    if (*str == 'T' || *str == 'Z') {
      if (!accept_time_toks) return false;
      dt_ctx->toks.push_back(DateTimeFormatToken(SEPARATOR, str - str_begin, 1, str));
      ++str;
      continue;
    }

    // A non-alphanumerical char could be the first char of a timezone-offset token.
    // If it is not the beginning of a time-zone offset token, track it as a separator.
    if (!isalpha(*str)) {
      if (dt_ctx->has_time_toks && IsValidTZOffset(str, str_end)) {
        // TZ offset must come at the end of the format.
        dt_ctx->toks.push_back(DateTimeFormatToken(TZ_OFFSET, str - str_begin,
            str_end - str, str));
        break;
      } else {
        dt_ctx->toks.push_back(DateTimeFormatToken(SEPARATOR, str - str_begin, 1, str));
        ++str;
        continue;
      }
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
    if (!accept_time_toks && dt_ctx->has_time_toks) return false;

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

// Parse out the next digit token from the date/time string by checking for contiguous
// digit characters and return a pointer to the end of that token.
// str -- pointer to the string to be parsed
// str_end -- the pointer to the end of the string to be parsed
// Returns the pointer within the string to the end of the valid digit token.
const char* ParseDigitToken(const char* str, const char* str_end) {
  const char* tok_end = str;
  while (tok_end < str_end) {
    if (!isdigit(*tok_end)) return tok_end;
    ++tok_end;
  }
  return tok_end;
}

// Parse out the next separator token from the date/time string against an expected
// character.
// str -- pointer to the string to be parsed
// str_end -- the pointer to the end of the string to be parsed
// sep -- the separator char to compare the token to
// Returns the pointer within the string to the end of the valid separator token.
const char* ParseSeparatorToken(const char* str, const char* str_end, const char sep) {
  const char* tok_end = str;
  while (tok_end < str_end) {
    if (*tok_end != sep) return tok_end;
    ++tok_end;
  }
  return tok_end;
}

bool ParseFormatTokensByStr(DateTimeFormatContext* dt_ctx, bool accept_time_toks,
    bool accept_time_toks_only) {
  DCHECK(dt_ctx != NULL);
  DCHECK(dt_ctx->fmt != NULL);
  DCHECK_GT(dt_ctx->fmt_len, 0);
  DCHECK_EQ(dt_ctx->toks.size(), 0);
  const char* str_begin = dt_ctx->fmt;
  const char* str_end = str_begin + dt_ctx->fmt_len;
  const char* str = str_begin;
  const char* tok_end;

  // Parse the 4-digit year
  tok_end = ParseDigitToken(str, str_end);
  if (tok_end - str == 4) {
    dt_ctx->toks.push_back(
        DateTimeFormatToken(YEAR, str - str_begin, tok_end - str, str));
    str = tok_end;

    // Check for the date separator '-'
    tok_end = ParseSeparatorToken(str, str_end, '-');
    if (tok_end - str != 1) return false;
    dt_ctx->toks.push_back(
        DateTimeFormatToken(SEPARATOR, str - str_begin, tok_end - str, str));
    str = tok_end;

    // Parse the 1 or 2 digit month.
    tok_end = ParseDigitToken(str, str_end);
    if (tok_end - str != 1 && tok_end - str != 2) return false;
    dt_ctx->toks.push_back(
        DateTimeFormatToken(MONTH_IN_YEAR, str - str_begin, tok_end - str, str));
    str = tok_end;

    // Check for the date separator '-'
    tok_end = ParseSeparatorToken(str, str_end, '-');
    if (tok_end - str != 1) return false;
    dt_ctx->toks.push_back(
        DateTimeFormatToken(SEPARATOR, str - str_begin, tok_end - str, str));
    str = tok_end;

    // Parse the 1 or 2 digit day in month
    tok_end = ParseDigitToken(str, str_end);
    if (tok_end - str != 1 && tok_end - str != 2) return false;
    dt_ctx->toks.push_back(
        DateTimeFormatToken(DAY_IN_MONTH, str - str_begin, tok_end - str, str));
    str = tok_end;
    dt_ctx->has_date_toks = true;

    // If the string ends here, we only have a date component
    if (str == str_end) return true;
    // If time tokens are not accepted, string should have ended here.
    if (!accept_time_toks) return false;

    // Check for the space between date and time component
    if (*str != ' ' && *str != 'T') return false;
    char sep = *str;
    tok_end = ParseSeparatorToken(str, str_end, sep);
    if (tok_end - str < 1) return false;
    // IMPALA-6641: Multiple spaces are okay, 'T' separator must be single
    if (sep == 'T' && tok_end - str > 1) return false;
    dt_ctx->toks.push_back(
        DateTimeFormatToken(SEPARATOR, str - str_begin, tok_end - str, str));
    str = tok_end;

    // Invalid format if date-time separator is not followed by more digits
    if (str > str_end) return false;
    tok_end = ParseDigitToken(str, str_end);
  }

  // If time tokens are not accepted, no need to proceed.
  if (!accept_time_toks) return false;
  // If no date tokens were found and time tokens on their own are not allowed, return
  // false.
  if (!dt_ctx->has_date_toks && !accept_time_toks_only) return false;

  // Parse the 1 or 2 digit hour
  if (tok_end - str != 1 && tok_end - str != 2) return false;
  dt_ctx->toks.push_back(
      DateTimeFormatToken(HOUR_IN_DAY, str - str_begin, tok_end - str, str));
  str = tok_end;

  // Check for the time component separator ':'
  tok_end = ParseSeparatorToken(str, str_end, ':');
  if (tok_end - str != 1) return false;
  dt_ctx->toks.push_back(
      DateTimeFormatToken(SEPARATOR, str - str_begin, tok_end - str, str));
  str = tok_end;

  // Parse the 1 or 2 digit minute
  tok_end = ParseDigitToken(str, str_end);
  if (tok_end - str != 1 && tok_end - str != 2) return false;
  dt_ctx->toks.push_back(
      DateTimeFormatToken(MINUTE_IN_HOUR, str - str_begin, tok_end - str, str));
  str = tok_end;

  // Check for the time component separator ':'
  tok_end = ParseSeparatorToken(str, str_end, ':');
  if (tok_end - str != 1) return false;
  dt_ctx->toks.push_back(
      DateTimeFormatToken(SEPARATOR, str - str_begin, tok_end - str, str));
  str = tok_end;

  // Parse the 1 or 2 digit second
  tok_end = ParseDigitToken(str, str_end);
  if (tok_end - str != 1 && tok_end - str != 2) return false;
  dt_ctx->toks.push_back(
      DateTimeFormatToken(SECOND_IN_MINUTE, str - str_begin, tok_end - str, str));
  str = tok_end;
  dt_ctx->has_time_toks = true;

  // There is more to parse, there maybe a fractional component.
  if (str < str_end) {
    tok_end = ParseSeparatorToken(str, str_end, '.');
    if (tok_end - str != 1) return false;
    dt_ctx->toks.push_back(
        DateTimeFormatToken(SEPARATOR, str - str_begin, tok_end - str, str));
    str = tok_end;

    // Invalid format when there is no fractional component following '.'
    if (str > str_end) return false;

    // Parse the fractional component.
    // Like the non-lazy path, this will parse up to 9 fractional digits
    tok_end = ParseDigitToken(str, str_end);
    int num_digits = std::min<int>(9, tok_end - str);
    dt_ctx->toks.push_back(
        DateTimeFormatToken(FRACTION, str - str_begin, num_digits, str));
    str = tok_end;

    // Invalid format if there is more to parse after the fractional component
    if (str < str_end) return false;
  }
  return true;
}

const DateTimeFormatContext* ParseDefaultFormatTokensByStr(const char* str, int len,
    bool accept_time_toks, bool accept_time_toks_only) {
  DCHECK(IsParseCtxInitialized());
  DCHECK(str != nullptr);
  DCHECK(len > 0);

  if (LIKELY(len >= DEFAULT_TIME_FMT_LEN)) {
    // Check if this string starts with a date component
    if (str[4] == '-' && str[7] == '-') {
      // Do we have a date component only?
      if (len == DEFAULT_DATE_FMT_LEN) {
        return &DEFAULT_DATE_CTX;
      }

      // We have a time component as well. Do we accept it?
      if (!accept_time_toks) return nullptr;

      switch (len) {
        case DEFAULT_SHORT_DATE_TIME_FMT_LEN: {
          if (LIKELY(str[13] == ':')) {
            switch (str[10]) {
              case ' ':
                return &DEFAULT_SHORT_DATE_TIME_CTX;
              case 'T':
                return &DEFAULT_SHORT_ISO_DATE_TIME_CTX;
            }
          }
          break;
        }
        case DEFAULT_DATE_TIME_FMT_LEN: {
          if (LIKELY(str[13] == ':')) {
            switch (str[10]) {
              case ' ':
                return &DEFAULT_DATE_TIME_CTX[9];
              case 'T':
                return &DEFAULT_ISO_DATE_TIME_CTX[9];
            }
          }
          break;
        }
        default: {
          // There is likely a fractional component that's below the expected 9 chars.
          // We will need to work out which default context to use that corresponds to
          // the fractional length in the string.
          if (LIKELY(len > DEFAULT_SHORT_DATE_TIME_FMT_LEN)
              && LIKELY(str[19] == '.') && LIKELY(str[13] == ':')) {
            switch (str[10]) {
              case ' ': {
                return &DEFAULT_DATE_TIME_CTX[len - DEFAULT_SHORT_DATE_TIME_FMT_LEN - 1];
              }
              case 'T': {
                return &DEFAULT_ISO_DATE_TIME_CTX
                    [len - DEFAULT_SHORT_DATE_TIME_FMT_LEN - 1];
              }
            }
          }
          break;
        }
      }
    } else {
      // 'str' string does not start with a date component.
      // Do we accept time component only?
      if (!accept_time_toks || !accept_time_toks_only) return nullptr;

      // Parse time component.
      if (str[2] == ':' && str[5] == ':' && isdigit(str[7])) {
        len = min(len, DEFAULT_TIME_FRAC_FMT_LEN);
        if (len > DEFAULT_TIME_FMT_LEN && str[8] == '.') {
          return &DEFAULT_TIME_FRAC_CTX[len - DEFAULT_TIME_FMT_LEN - 1];
        } else {
          return &DEFAULT_TIME_CTX;
        }
      }
    }
  }

  return nullptr;
}

bool ParseDateTime(const char* str, int str_len, const DateTimeFormatContext& dt_ctx,
    DateTimeParseResult* dt_result) {
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
        dt_result->tz_offset = time_duration(sign * hour, sign * minute, 0, 0);
        break;
      }
      default: DCHECK(false) << "Unknown date/time format token";
    }
  }
  return true;
}

} // namespace datetime_parse_util

} // nmespace impala
