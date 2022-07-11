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

#include "runtime/datetime-simple-date-format-parser.h"

#include <algorithm>

#include "cctz/civil_time.h"
#include "common/names.h"
#include "runtime/string-value.h"
#include "util/string-parser.h"

using boost::unordered_map;
using boost::posix_time::time_duration;

namespace impala {

namespace datetime_parse_util {

bool SimpleDateFormatTokenizer::initialized = false;

const int SimpleDateFormatTokenizer::DEFAULT_DATE_FMT_LEN = 10;
const int SimpleDateFormatTokenizer::DEFAULT_TIME_FMT_LEN = 8;
const int SimpleDateFormatTokenizer::DEFAULT_TIME_FRAC_FMT_LEN = 18;
const int SimpleDateFormatTokenizer::DEFAULT_SHORT_DATE_TIME_FMT_LEN = 19;
const int SimpleDateFormatTokenizer::DEFAULT_DATE_TIME_FMT_LEN = 29;
const int SimpleDateFormatTokenizer::FRACTIONAL_MAX_LEN = 9;

DateTimeFormatContext SimpleDateFormatTokenizer::DEFAULT_SHORT_DATE_TIME_CTX;
DateTimeFormatContext SimpleDateFormatTokenizer::DEFAULT_SHORT_ISO_DATE_TIME_CTX;
DateTimeFormatContext SimpleDateFormatTokenizer::DEFAULT_DATE_CTX;
DateTimeFormatContext SimpleDateFormatTokenizer::DEFAULT_TIME_CTX;
DateTimeFormatContext SimpleDateFormatTokenizer::DEFAULT_DATE_TIME_CTX[10];
DateTimeFormatContext SimpleDateFormatTokenizer::DEFAULT_ISO_DATE_TIME_CTX[10];
DateTimeFormatContext SimpleDateFormatTokenizer::DEFAULT_TIME_FRAC_CTX[10];

void SimpleDateFormatTokenizer::InitCtx() {
  if (initialized) return;

  // Setup the default date/time context yyyy-MM-dd HH:mm:ss.SSSSSSSSS
  const char* DATE_TIME_CTX_FMT = "yyyy-MM-dd HH:mm:ss.SSSSSSSSS";
  for (int i = FRACTIONAL_MAX_LEN; i >= 0; --i) {
    DEFAULT_DATE_TIME_CTX[i].Reset(DATE_TIME_CTX_FMT,
        DEFAULT_DATE_TIME_FMT_LEN - (FRACTIONAL_MAX_LEN - i));
    Tokenize(&DEFAULT_DATE_TIME_CTX[i], PARSE);
  }

  // Setup the default ISO date/time context yyyy-MM-ddTHH:mm:ss.SSSSSSSSS
  for (int i = FRACTIONAL_MAX_LEN; i >= 0; --i) {
    DEFAULT_ISO_DATE_TIME_CTX[i].Reset("yyyy-MM-ddTHH:mm:ss.SSSSSSSSS",
        DEFAULT_DATE_TIME_FMT_LEN - (FRACTIONAL_MAX_LEN - i));
    Tokenize(&DEFAULT_ISO_DATE_TIME_CTX[i], PARSE);
  }

  // Setup the short default date/time context yyyy-MM-dd HH:mm:ss
  DEFAULT_SHORT_DATE_TIME_CTX.Reset("yyyy-MM-dd HH:mm:ss");
  Tokenize(&DEFAULT_SHORT_DATE_TIME_CTX, PARSE);

  // Setup the short default ISO date/time context yyyy-MM-ddTHH:mm:ss
  DEFAULT_SHORT_ISO_DATE_TIME_CTX.Reset("yyyy-MM-ddTHH:mm:ss");
  Tokenize(&DEFAULT_SHORT_ISO_DATE_TIME_CTX, PARSE);

  // Setup the default short date context yyyy-MM-dd
  DEFAULT_DATE_CTX.Reset("yyyy-MM-dd");
  Tokenize(&DEFAULT_DATE_CTX, PARSE);

  // Setup the default short time context HH:mm:ss
  DEFAULT_TIME_CTX.Reset("HH:mm:ss");
  Tokenize(&DEFAULT_TIME_CTX, PARSE, true, true);

  // Setup the default short time context with fractional seconds HH:mm:ss.SSSSSSSSS
  for (int i = FRACTIONAL_MAX_LEN; i >= 0; --i) {
    DEFAULT_TIME_FRAC_CTX[i].Reset(DATE_TIME_CTX_FMT + 11,
        DEFAULT_TIME_FRAC_FMT_LEN - (FRACTIONAL_MAX_LEN - i));
    Tokenize(&DEFAULT_TIME_FRAC_CTX[i], PARSE, true, true);
  }

  // Flag that the parser is ready.
  initialized = true;
}

bool SimpleDateFormatTokenizer::IsValidTZOffset(const char* str_begin,
    const char* str_end) {
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

bool SimpleDateFormatTokenizer::Tokenize(
    DateTimeFormatContext* dt_ctx, CastDirection cast_mode, bool accept_time_toks,
    bool accept_time_toks_only) {
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

    // Get the token length
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
      if (tok_len == 3) tok_type = MONTH_NAME_SHORT;
    }
    // In an output scenario, fmt_out_len is used to determine the print buffer size.
    // If the format uses short tokens e.g. yyyy-MM-d, there must to be enough room in
    // the buffer for wider values e.g. 2013-12-16.
    if (tok_len == 1) ++dt_ctx->fmt_out_len;
    DateTimeFormatToken tok(tok_type, str - str_begin, tok_len, str);
    str += tok.len;
    if (tok_type == YEAR) {
      tok.divisor = std::pow(10, tok_len);
    } else if (tok_type == FRACTION) {
      tok.divisor = std::pow(10, FRACTIONAL_MAX_LEN - tok_len);
    }
    dt_ctx->toks.push_back(tok);
  }
  if (cast_mode == PARSE && !accept_time_toks_only) return dt_ctx->has_date_toks;
  return dt_ctx->has_date_toks || dt_ctx->has_time_toks;
}

const char* SimpleDateFormatTokenizer::ParseDigitToken(const char* str,
    const char* str_end) {
  const char* tok_end = str;
  while (tok_end < str_end) {
    if (!isdigit(*tok_end)) return tok_end;
    ++tok_end;
  }
  return tok_end;
}

const char* SimpleDateFormatTokenizer::ParseSeparatorToken(const char* str,
    const char* str_end, const char sep) {
  const char* tok_end = str;
  while (tok_end < str_end) {
    if (*tok_end != sep) return tok_end;
    ++tok_end;
  }
  return tok_end;
}

bool SimpleDateFormatTokenizer::TokenizeByStr( DateTimeFormatContext* dt_ctx,
    bool accept_time_toks) {
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
  if (!dt_ctx->has_date_toks) return false;

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
    if (num_digits == 0) return false;
    dt_ctx->toks.push_back(
        DateTimeFormatToken(FRACTION, str - str_begin, num_digits, str));
    str = tok_end;

    // Invalid format if there is more to parse after the fractional component
    if (str < str_end) return false;
  }
  return true;
}

const DateTimeFormatContext* SimpleDateFormatTokenizer::GetDefaultFormatContext(
    const char* str, int len, bool accept_time_toks, bool accept_time_toks_only) {
  DCHECK(initialized);
  DCHECK(str != nullptr);
  DCHECK(len > 0);
  DCHECK(!accept_time_toks_only || accept_time_toks);

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
    } else if (accept_time_toks_only && str[2] == ':' && str[5] == ':') {
      if (len == DEFAULT_TIME_FMT_LEN) return &DEFAULT_TIME_CTX;
      // There is only time component.
      len = min(len, DEFAULT_TIME_FRAC_FMT_LEN);
      if (len > DEFAULT_TIME_FMT_LEN && str[8] == '.') {
        return &DEFAULT_TIME_FRAC_CTX[len - DEFAULT_TIME_FMT_LEN - 1];
      }
    }
  }
  return nullptr;
}

bool SimpleDateFormatParser::ParseDateTime(const char* str, int str_len,
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
        if (!ParseAndValidate(tok_val, tok_len, 0, 9999, &dt_result->year)) return false;
        // Year in "Y" and "YY" format should be in the interval
        // [current time - 80 years, current time + 20 years)
        if (tok_len <= 2) dt_result->realign_year = true;
        break;
      }
      case MONTH_IN_YEAR: {
        if (!ParseAndValidate(tok_val, tok_len, 1, 12, &dt_result->month)) return false;
        break;
      }
      case MONTH_NAME_SHORT: {
        const char* tok_end = tok_val + tok_len;
        if (!ParseMonthNameToken(tok, tok_val, &tok_end, dt_ctx.fx_modifier,
            &dt_result->month)) {
          return false;
        }
        break;
      }
      case DAY_IN_MONTH: {
        if (!ParseAndValidate(tok_val, tok_len, 1, 31, &dt_result->day)) return false;
        break;
      }
      case HOUR_IN_DAY: {
        if (!ParseAndValidate(tok_val, tok_len, 0, 23, &dt_result->hour)) return false;
        break;
      }
      case MINUTE_IN_HOUR: {
        if (!ParseAndValidate(tok_val, tok_len, 0, 59, &dt_result->minute)) return false;
        break;
      }
      case SECOND_IN_MINUTE: {
        if (!ParseAndValidate(tok_val, tok_len, 0, 59, &dt_result->second)) return false;
        break;
      }
      case FRACTION: {
        if (!ParseFractionToken(tok_val, tok_len, dt_result)) return false;
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

} // namespace impala
