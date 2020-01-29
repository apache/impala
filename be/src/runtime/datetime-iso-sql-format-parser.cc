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

#include "runtime/datetime-iso-sql-format-parser.h"

#include <cmath>

#include "common/names.h"
#include "runtime/datetime-iso-sql-format-tokenizer.h"
#include "util/string-parser.h"

namespace impala {

namespace datetime_parse_util {

bool IsoSqlFormatParser::ParseDateTime(const char* input_str, int input_len,
      const DateTimeFormatContext& dt_ctx, DateTimeParseResult* result) {
  DCHECK(dt_ctx.toks.size() > 0);
  DCHECK(dt_ctx.current_time != nullptr);
  DCHECK(dt_ctx.current_time->HasDateAndTime());
  DCHECK(result != nullptr);
  DCHECK(result->hour == 0);
  if (input_str == nullptr || input_len <= 0) return false;

  int day_in_year = -1;
  Iso8601WeekBasedDateParseResult iso8601_week_based_date_values;

  const char* current_pos = input_str;
  const char* end_pos = input_str + input_len;
  for (int i = 0; i < dt_ctx.toks.size(); ++i) {
    const DateTimeFormatToken* tok = &dt_ctx.toks[i];
    if (current_pos >= end_pos) {
      // Accept empty text tokens at the end of the format.
      if (tok->type == TEXT && tok->len == 0) continue;
      return false;
    }

    if (tok->type == SEPARATOR) {
      if (dt_ctx.fx_modifier) {
        DCHECK(tok->len == 1);
        if (*current_pos != *tok->val) return false;
        ++current_pos;
        continue;
      } else {
        bool res = ProcessSeparatorSequence(&current_pos, end_pos, dt_ctx, &i);
        if (!res || current_pos >= end_pos) return res;
        DCHECK(i < dt_ctx.toks.size());
        // Next token, following the separator sequence.
        tok = &dt_ctx.toks[i];
      }
    }

    if (tok->type == TEXT) {
      const char* format_it = tok->val;
      const char* format_end = tok->val + tok->len;
      while (format_it < format_end && current_pos < end_pos) {
        char format_char_to_compare = GetNextCharFromTextToken(&format_it, tok);
        if (format_char_to_compare != *current_pos) return false;
        ++format_it;
        ++current_pos;
      }
      if (format_it < format_end) return false;
      continue;
    }

    const char* token_end_pos =
        FindEndOfToken(current_pos, end_pos - current_pos, *tok, dt_ctx.fx_modifier);
    if (token_end_pos == nullptr) return false;
    int token_len = token_end_pos - current_pos;

    if (dt_ctx.fx_modifier && !tok->fm_modifier && token_len != tok->len) return false;

    switch(tok->type) {
      case YEAR: {
        if (!ParseAndValidate(current_pos, token_len, 0, 9999, &result->year)) {
          return false;
        }
        if (token_len < 4) {
          result->year = PrefixYearFromCurrentYear(result->year, token_len,
              dt_ctx.current_time->date().year());
        }
        break;
      }
      case ROUND_YEAR: {
        if (!ParseAndValidate(current_pos, token_len, 0, 9999, &result->year)) {
          return false;
        }
        if (token_len == 2) {
          result->year = RoundYearFromCurrentYear(result->year,
              dt_ctx.current_time->date().year());
        }
        if (token_len == 3 || token_len == 1) {
          result->year = PrefixYearFromCurrentYear(result->year, token_len,
              dt_ctx.current_time->date().year());
        }
        break;
      }
      case MONTH_IN_YEAR: {
        if (!ParseAndValidate(current_pos, token_len, 1, 12, &result->month)) {
          return false;
        }
        break;
      }
      case MONTH_NAME:
      case MONTH_NAME_SHORT: {
        if (!ParseMonthNameToken(*tok, current_pos, &token_end_pos, dt_ctx.fx_modifier,
            &result->month)) {
          return false;
        }
        break;
      }
      case DAY_NAME:
      case DAY_NAME_SHORT: {
        if (!ParseDayNameToken(*tok, current_pos, &token_end_pos, dt_ctx.fx_modifier,
            &iso8601_week_based_date_values.day_of_week)) {
          return false;
        }
        break;
      }
      case DAY_IN_MONTH: {
        if (!ParseAndValidate(current_pos, token_len, 1, 31, &result->day)) return false;
        break;
      }
      case DAY_IN_YEAR: {
        if (!ParseAndValidate(current_pos, token_len, 1, 366, &day_in_year)) return false;
        // Can't figure out the value of MONTH_IN_YEAR and DAY_IN_MONTH here as YEAR
        // token is also required for that and it might come later in the input string.
        break;
      }
      case HOUR_IN_HALF_DAY: {
        int hour;
        if (!ParseAndValidate(current_pos, token_len, 1, 12, &hour)) return false;
        if (hour == 12) hour = 0;
        // Note the addition instead of assignment here. PM and HOUR_IN_HALF_DAY can be
        // in any order in the format token list and PM might add another 12 hours.
        result->hour += hour;
        break;
      }
      case HOUR_IN_DAY: {
        if (!ParseAndValidate(current_pos, token_len, 0, 23, &result->hour)) return false;
        break;
      }
      case MINUTE_IN_HOUR: {
        if (!ParseAndValidate(current_pos, token_len, 0, 59, &result->minute)) {
          return false;
        }
        break;
      }
      case SECOND_IN_MINUTE: {
        if (!ParseAndValidate(current_pos, token_len, 0, 59, &result->second)) {
          return false;
        }
        break;
      }
      case SECOND_IN_DAY: {
        int second_in_day;
        if (!ParseAndValidate(current_pos, token_len, 0, 86399, &second_in_day)) {
          return false;
        }
        result->second = second_in_day % 60;
        int minutes_in_day = second_in_day / 60;
        result->minute = minutes_in_day % 60;
        result->hour = minutes_in_day / 60;
        break;
      }
      case FRACTION: {
        if (!ParseFractionToken(current_pos, token_len, result)) return false;
        break;
      }
      case MERIDIEM_INDICATOR: {
        // Input has already been validated in ParseMeridiemIndicatorFromInput().
        string indicator(current_pos, token_len);
        boost::to_upper(indicator);
        if (indicator == "PM" || indicator == "P.M.") result->hour += 12;
        break;
      }
      case TIMEZONE_HOUR: {
        // Deliberately ignore the timezone offsets.
        int dummy_result;
        if (!ParseAndValidate(current_pos, token_len, -15, 15, &dummy_result)) {
          return false;
        }
        break;
      }
      case TIMEZONE_MIN: {
        // Deliberately ignore the timezone offsets.
        int dummy_result;
        if (!ParseAndValidate(current_pos, token_len, 0, 59, &dummy_result)) {
          return false;
        }
        break;
      }
      case ISO8601_TIME_INDICATOR:
      case ISO8601_ZULU_INDICATOR: {
        DCHECK(token_len == 1);
        if (toupper(*current_pos) != toupper(*tok->val)) return false;
        break;
      }
      case ISO8601_WEEK_NUMBERING_YEAR: {
        if (!ParseAndValidate(current_pos, token_len, 0, 9999,
            &iso8601_week_based_date_values.year)) {
          return false;
        }
        if (token_len < 4) {
          iso8601_week_based_date_values.year = PrefixYearFromCurrentYear(
              iso8601_week_based_date_values.year, token_len,
              GetIso8601WeekNumberingYear(dt_ctx.current_time));
        }
        break;
      }
      case ISO8601_WEEK_OF_YEAR: {
        if (!ParseAndValidate(current_pos, token_len, 1, 53,
            &iso8601_week_based_date_values.week_of_year)) {
          return false;
        }
        break;
      }
      case ISO8601_DAY_OF_WEEK: {
        if (!ParseAndValidate(current_pos, token_len, 1, 7,
            &iso8601_week_based_date_values.day_of_week)) {
          return false;
        }
        break;
      }
      default: {
        return false;
      }
    }
    current_pos = token_end_pos;
  }

  // If the format string is over but there are tokens left in the input.
  if (current_pos < end_pos) return false;

  // Get month and day values from "day in year" and year tokens
  if (day_in_year != -1) {
    DCHECK(result->year >= 0 && result->year <= 9999);
    if (!GetMonthAndDayFromDaysSinceJan1(result->year, day_in_year - 1, &result->month,
        &result->day)) {
      return false;
    }
  }

  if (iso8601_week_based_date_values.IsSet()) {
    if (!iso8601_week_based_date_values.ExtractYearMonthDay(result)) return false;
  }

  return true;
}

char IsoSqlFormatParser::GetNextCharFromTextToken(const char** format,
    const DateTimeFormatToken* tok) {
  DCHECK(format != nullptr && *format != nullptr);
  DCHECK(tok != nullptr);
  DCHECK(tok->val <= *format && *format < tok->val + tok->len);
  if (**format != '\\') return **format;
  const char* format_end = tok->val + tok->len;
  // Take care of the double escaped quotes.
  if (tok->is_double_escaped && format_end - *format >= 4 &&
      (strncmp(*format, "\\\\\\\"", 4) == 0)) {
    *format += 3;
    return **format;
  }
  // Skip the escaping backslash.
  ++(*format);
  switch (**format) {
    case 'b': return '\b';
    case 'n': return '\n';
    case 'r': return '\r';
    case 't': return '\t';
  }
  return **format;
}

bool IsoSqlFormatParser::ProcessSeparatorSequence(const char** current_pos,
    const char* end_pos, const DateTimeFormatContext& dt_ctx, int* current_tok_idx) {
  DCHECK(current_pos != nullptr && *current_pos != nullptr);
  DCHECK(end_pos != nullptr);
  DCHECK(current_tok_idx != nullptr && *current_tok_idx < dt_ctx.toks.size());
  DCHECK(dt_ctx.toks[*current_tok_idx].type == SEPARATOR);
  if (!IsoSqlFormatTokenizer::IsSeparator(current_pos, end_pos, false)) return false;
  const char* begin_pos = *current_pos;
  // Advance to the end of the separator sequence.
  ++(*current_pos);
  while (*current_pos < end_pos &&
      IsoSqlFormatTokenizer::IsSeparator(current_pos, end_pos, false)) {
    ++(*current_pos);
  }
  // Advance to the end of the separator sequence in the expected tokens list.
  ++(*current_tok_idx);
  while (*current_tok_idx < dt_ctx.toks.size() &&
         dt_ctx.toks[*current_tok_idx].type == SEPARATOR) {
    ++(*current_tok_idx);
  }

  // If we reached the end of input or the end of token sequence, we can return.
  if (*current_pos >= end_pos || *current_tok_idx >= dt_ctx.toks.size()) {
    // Skip trailing empty text tokens in format.
    if (*current_pos >= end_pos && *current_tok_idx < dt_ctx.toks.size()) {
      while (*current_tok_idx < dt_ctx.toks.size() &&
          dt_ctx.toks[*current_tok_idx].type == TEXT &&
          dt_ctx.toks[*current_tok_idx].len == 0) {
        ++(*current_tok_idx);
      }
    }
    return (*current_pos >= end_pos && *current_tok_idx >= dt_ctx.toks.size());
  }

  // The last '-' of a separator sequence might be taken as a sign for timezone hour.
  // If the TZH token itself doesn't start with a '+' sign and the separator sequence
  // contains more than one separators then the last '-' of the separator sequence is
  // taken as the sign of TZH.
  if (*(*current_pos - 1) == '-' && dt_ctx.toks[*current_tok_idx].type == TIMEZONE_HOUR
      && *(*current_pos) != '+' && begin_pos != (*current_pos - 1)) {
    --(*current_pos);
  }
  return true;
}

const char* IsoSqlFormatParser::FindEndOfToken(const char* input_str,
    int input_len, const DateTimeFormatToken& tok, bool fx_provided) {
  DCHECK(input_str != nullptr);
  DCHECK(input_len >= 0);

  if (input_len == 0) return nullptr;

  // Handle separately the meridiem indicators for two reasons.
  // 1: They might contain '.' that is not meant to be a separator character.
  // 2: The length of the token in the pattern might differ from the length of the token
  // in the input. E.g. "AM" should match with "P.M.".
  if (tok.type == MERIDIEM_INDICATOR) {
    return ParseMeridiemIndicatorFromInput(input_str, input_len);
  }

  if (tok.type == MONTH_NAME && fx_provided && !tok.fm_modifier) {
    if (input_len < MAX_MONTH_NAME_LENGTH) return nullptr;
    return input_str + MAX_MONTH_NAME_LENGTH;
  }
  if (tok.type == DAY_NAME && fx_provided && !tok.fm_modifier) {
    if (input_len < MAX_DAY_NAME_LENGTH) return nullptr;
    return input_str + MAX_DAY_NAME_LENGTH;
  }

  int max_tok_len = min(input_len, tok.len);
  const char* start_of_token = input_str;
  if (tok.type == TIMEZONE_HOUR) {
    if (max_tok_len > 2) max_tok_len = 2;
    if (*start_of_token == '-' || *start_of_token == '+') {
      ++start_of_token;
      if (input_len - 1 < max_tok_len) --max_tok_len;
    }
  }

  const char* end_pos = start_of_token;
  while (end_pos < start_of_token + max_tok_len &&
      !IsoSqlFormatTokenizer::IsSeparator(&end_pos, input_str + input_len, false)) {
    ++end_pos;
  }
  if (end_pos == input_str) return nullptr;
  return end_pos;
}

const char* IsoSqlFormatParser::ParseMeridiemIndicatorFromInput(
    const char* input_str, int input_len) {
  DCHECK(input_str != nullptr);
  if (input_len >= 4 &&
      (strncasecmp(input_str, AM_LONG.first, 4) == 0 ||
       strncasecmp(input_str, PM_LONG.first, 4) == 0 )) {
    return input_str + 4;
  }
  if (input_len >= 2 &&
      (strncasecmp(input_str, AM.first, 2) == 0 ||
       strncasecmp(input_str, PM.first, 2) == 0 )) {
    return input_str + 2;
  }
  return nullptr;
}

int IsoSqlFormatParser::PrefixYearFromCurrentYear(int year, int year_token_len,
    int current_year) {
  DCHECK(year >= 0 && year < 1000);
  DCHECK(year_token_len > 0 && year_token_len < 4);
  int adjust_factor = pow(10, year_token_len);
  int adjustment = (current_year / adjust_factor) * adjust_factor;
  return year + adjustment;
}

int IsoSqlFormatParser::RoundYearFromCurrentYear(int year, int current_year) {
  DCHECK(year >= 0 && year < 100);
  int postfix_of_curr_year = current_year % 100;
  if (year < 50 && postfix_of_curr_year > 49) year += 100;
  if (year > 49 && postfix_of_curr_year < 50) year -= 100;
  return year + (current_year / 100) * 100;
}

int IsoSqlFormatParser::GetIso8601WeekNumberingYear(const TimestampValue* now) {
  DCHECK(now != nullptr);
  DCHECK(now->HasDate());

  static const boost::gregorian::date EPOCH(1970, 1, 1);
  DateValue dv = DateValue((now->date() - EPOCH).days());
  DCHECK(dv.IsValid());
  return dv.Iso8601WeekNumberingYear();
}

bool IsoSqlFormatParser::Iso8601WeekBasedDateParseResult::ExtractYearMonthDay(
    DateTimeParseResult* result) const {
  DCHECK(result != nullptr);
  DCHECK(IsSet());
  DateValue dv = DateValue::CreateFromIso8601WeekBasedDateVals(year, week_of_year,
      day_of_week);
  return dv.ToYearMonthDay(&result->year, &result->month, &result->day);
}

}
}
