// Copyright 2013 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef IMPALA_RUNTIME_TIMESTAMP_PARSE_UTIL_H
#define IMPALA_RUNTIME_TIMESTAMP_PARSE_UTIL_H

#include <boost/assign/list_of.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/foreach.hpp>
#include <boost/unordered_map.hpp>
#include "common/status.h"
#include "runtime/string-value.inline.h"
#include "util/string-parser.h"

namespace impala {

/// Add support for dealing with custom date/time formats in Impala. The following
/// date/time tokens are supported:
///   y – Year
///   M – Month
///   d – Day
///   H – Hour
///   m – Minute
///   s – second
///   S – Fractional second
///
///   TimeZone offset formats (Must be at the end of format string):
///   +/-hh:mm
///   +/-hhmm
///   +/-hh
///
///
/// The token names and usage have been modeled after the SimpleDateFormat class used in
/// Java, with only the above list of tokens being supported. All fields will consume
/// variable length inputs when parsing an input string and must therefore use separators
/// to specify the boundaries of the fields, with the exception of TimeZone values, which
/// have to be of fixed width. Repeating tokens can be used to specify fields of exact
/// witdh, e.g. in yy-MM both fields must be of exactly length two. When using fixed width
/// fields values must be zero-padded and output values will be zero padded during
/// formatting. There is one exception to this: a month field of length 3 will specify
/// literal month names instead of zero padding, i.e., yyyy-MMM-dd will parse from and
/// format to strings like 2013-Nov-21. When using fields of fixed width the separators
/// can be omitted.
///
///
/// Formatting character groups can appear in any order along with any separators
/// except TimeZone offset.
/// e.g.
///   yyyy/MM/dd
///   dd-MMM-yy
///   (dd)(MM)(yyyy) HH:mm:ss
///   yyyy-MM-dd HH:mm:ss+hh:mm
/// ..etc..
///
/// The following features are not supported:
///   Long literal months e.g. MMMM
///   Nested strings e.g. “Year: “ yyyy “Month: “ mm “Day: “ dd
///   Lazy formatting

/// Used to indicate the type of a date/time format token group.
enum DateTimeFormatTokenType {
  UNKNOWN = 0,
  SEPARATOR,
  YEAR,
  MONTH_IN_YEAR,
  /// Indicates a short literal month e.g. MMM (Aug). Note that the month name is case
  /// insensitive for an input scenario and printed in camel case for an output scenario.
  MONTH_IN_YEAR_SLT,
  DAY_IN_MONTH,
  HOUR_IN_DAY,
  MINUTE_IN_HOUR,
  SECOND_IN_MINUTE,
  /// Indicates fractional seconds e.g.14:52:36.2334. By default this provides nanosecond
  /// resolution.
  FRACTION,
  TZ_OFFSET,
};

/// Used to store metadata about a token group within a date/time format.
struct DateTimeFormatToken {
  /// Indicates the type of date/time format token e.g. year
  DateTimeFormatTokenType type;
  /// The position of where this token group is supposed to start in the date/time string
  /// to be parsed
  int pos;
  /// The length of the token group
  int len;
  /// A pointer to the date/time format string that is positioned at the start of this
  /// token group
  const char* val;

  DateTimeFormatToken(DateTimeFormatTokenType type, int pos, int len, const char* val)
    : type(type),
      pos(pos),
      len(len),
      val(val) {
  }
};

/// This structure is used to hold metadata for a date/time format. Each token group
/// within the raw format is parsed and placed in this structure along with other high
/// level information e.g. if the format contains date and/or time tokens. This context
/// is used during date/time parsing.
struct DateTimeFormatContext {
  const char* fmt;
  int fmt_len;
  /// Holds the expanded length of fmt_len plus any required space when short format
  /// tokens are used. The output buffer size is driven from this value. For example, in
  /// an output scenario a user may provide the format yyyy-M-d, if the day and month
  /// equates to 12, 21 then extra space is needed in the buffer to hold the values. The
  /// short format type e.g. yyyy-M-d is valid where no zero padding is required on single
  /// digits.
  int fmt_out_len;
  std::vector<DateTimeFormatToken> toks;
  bool has_date_toks;
  bool has_time_toks;

  DateTimeFormatContext() {
    Reset(NULL, 0);
  }

  DateTimeFormatContext(const char* fmt, int fmt_len) {
    Reset(fmt, fmt_len);
  }

  void Reset(const char* fmt, int fmt_len) {
    this->fmt = fmt;
    this->fmt_len = fmt_len;
    this->fmt_out_len = fmt_len;
    this->has_date_toks = false;
    this->has_time_toks = false;
    this->toks.clear();
  }
};

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

  DateTimeParseResult()
    : year(0),
      month(0),
      day(0),
      hour(0),
      minute(0),
      second(0),
      fraction(0),
      tz_offset(0,0,0,0) {
  }
};

/// Used for parsing both default and custom formatted timestamp values.
class TimestampParser {
 public:
  /// Initializes the static parser context which includes default date/time formats and
  /// lookup tables. This *must* be called before any of the Parse* related functions can
  /// be used.
  static void Init();

  /// Parse the date/time format into tokens and place them in the context.
  /// dt_ctx -- date/time format context
  /// Return true if the parse was successful.
  static inline bool ParseFormatTokens(DateTimeFormatContext* dt_ctx) {
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

  /// Parse a default date/time string. The default timestamp format is:
  /// yyyy-MM-dd HH:mm:ss.SSSSSSSSS or yyyy-MM-ddTHH:mm:ss.SSSSSSSSS. Either just the
  /// date or just the time may be specified. All components are required in either the
  /// date or time except for the fractional seconds following the period. In the case
  /// of just a date, the time will be set to 00:00:00. In the case of just a time, the
  /// date will be set to invalid.
  /// str -- valid pointer to the string to parse
  /// len -- length of the string to parse (must be > 0)
  /// dt_ctx -- date/time format context (must contain valid tokens)
  /// d -- the date value where the results of the parsing will be placed
  /// t -- the time value where the results of the parsing will be placed
  /// Returns true if the date/time was successfully parsed.
  static inline bool Parse(const char* str, int len, boost::gregorian::date* d,
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

  /// Parse a date/time string. The data must adhere to the format, otherwise it will be
  /// rejected i.e. no missing tokens. In the case of just a date, the time will be set
  /// to 00:00:00. In the case of just a time, the date will be set to invalid.
  /// str -- valid pointer to the string to parse
  /// len -- length of the string to parse (must be > 0)
  /// d -- the date value where the results of the parsing will be placed
  /// t -- the time value where the results of the parsing will be placed
  /// Returns true if the date/time was successfully parsed.
  static bool Parse(const char* str, int len, const DateTimeFormatContext& dt_ctx,
      boost::gregorian::date* d, boost::posix_time::time_duration* t);

  /// Format the date/time values using the given format context. Note that a string
  /// terminator will be appended to the string.
  /// dt_ctx -- date/time format context
  /// d -- the date value
  /// t -- the time value
  /// len -- the output buffer length (should be at least dt_ctx.fmt_exp_len + 1)
  /// buff -- the output string buffer (must be large enough to hold value)
  /// Return the number of characters copied in to the buffer (excluding terminator).
  static inline int Format(const DateTimeFormatContext& dt_ctx,
      const boost::gregorian::date& d, const boost::posix_time::time_duration& t,
      int len, char* buff) {
    DCHECK(TimestampParser::initialized_);
    DCHECK(dt_ctx.toks.size() > 0);
    DCHECK(len > dt_ctx.fmt_out_len);
    DCHECK(buff != NULL);
    if (dt_ctx.has_date_toks && d.is_special()) return -1;
    if (dt_ctx.has_time_toks && t.is_special()) return -1;
    char* str = buff;
    BOOST_FOREACH(const DateTimeFormatToken& tok, dt_ctx.toks) {
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

 private:
  static inline bool ParseDateTime(const char* str, int str_len,
      const DateTimeFormatContext& dt_ctx, DateTimeParseResult* dt_result) {
    DCHECK(dt_ctx.fmt_len > 0);
    DCHECK(dt_ctx.toks.size() > 0);
    DCHECK(dt_result != NULL);
    if (str_len <= 0 || str_len < dt_ctx.fmt_len || str == NULL) return false;
    StringParser::ParseResult status;
    // Keep track of the number of characters we need to shift token positions by.
    // Variable-length tokens will result in values > 0;
    int shift_len = 0;
    BOOST_FOREACH(const DateTimeFormatToken& tok, dt_ctx.toks) {
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
          if (UNLIKELY(dt_result->year < 1 || dt_result->year > 9999)) return false;
          if (tok_len < 4 && dt_result->year < 99) dt_result->year += 2000;
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

  /// Check if the string is a TimeZone offset token.
  /// Valid offset token format are 'hh:mm', 'hhmm', 'hh'.
  static bool IsValidTZOffset(const char* str_begin, const char* str_end) {
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

  /// Constants to hold default format lengths.
  static const int DEFAULT_DATE_FMT_LEN = 10;
  static const int DEFAULT_TIME_FMT_LEN = 8;
  static const int DEFAULT_TIME_FRAC_FMT_LEN = 18;
  static const int DEFAULT_SHORT_DATE_TIME_FMT_LEN = 19;
  static const int DEFAULT_DATE_TIME_FMT_LEN = 29;

  /// Used to indicate if the parsing state has been initialized.
  static bool initialized_;

  /// Lazily initialized pseudo-constant hashmap for mapping month names to an index.
  static boost::unordered_map<StringValue, int> REV_MONTH_INDEX;

  /// Pseudo-constant default date/time contexts. Backwards compatibility is provided on
  /// variable length fractional components by defining a format context for each expected
  /// length (0 - 9). This logic will be refactored when the parser supports lazy token
  /// groups.
  static DateTimeFormatContext DEFAULT_SHORT_DATE_TIME_CTX;
  static DateTimeFormatContext DEFAULT_SHORT_ISO_DATE_TIME_CTX;
  static DateTimeFormatContext DEFAULT_DATE_CTX;
  static DateTimeFormatContext DEFAULT_TIME_CTX;
  static DateTimeFormatContext DEFAULT_DATE_TIME_CTX[10];
  static DateTimeFormatContext DEFAULT_ISO_DATE_TIME_CTX[10];
  static DateTimeFormatContext DEFAULT_TIME_FRAC_CTX[10];
};

}

#endif
