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

#include "runtime/datetime-iso-sql-format-tokenizer.h"

#include <strings.h>
#include <algorithm>
#include <cstring>
#include <vector>

#include <boost/algorithm/string/case_conv.hpp>

#include "common/logging.h"

namespace impala {

namespace datetime_parse_util {

using std::min;
using std::string;
using std::unordered_map;
using std::unordered_set;

const unordered_map<string, IsoSqlFormatTokenizer::TokenItem>
    IsoSqlFormatTokenizer::VALID_TOKENS({
  {"YYYY", IsoSqlFormatTokenizer::TokenItem(YEAR, true, false)},
  {"YYY", IsoSqlFormatTokenizer::TokenItem(YEAR, true, false)},
  {"YY", IsoSqlFormatTokenizer::TokenItem(YEAR, true, false)},
  {"Y", IsoSqlFormatTokenizer::TokenItem(YEAR, true, false)},
  {"RRRR", IsoSqlFormatTokenizer::TokenItem(ROUND_YEAR, true, false)},
  {"RR", IsoSqlFormatTokenizer::TokenItem(ROUND_YEAR, true, false)},
  {"MM", IsoSqlFormatTokenizer::TokenItem(MONTH_IN_YEAR, true, false)},
  {"DDD", IsoSqlFormatTokenizer::TokenItem(DAY_IN_YEAR, true, false)},
  {"DD", IsoSqlFormatTokenizer::TokenItem(DAY_IN_MONTH, true, false)},
  {"HH", IsoSqlFormatTokenizer::TokenItem(HOUR_IN_HALF_DAY, false, true)},
  {"HH12", IsoSqlFormatTokenizer::TokenItem(HOUR_IN_HALF_DAY, false, true)},
  {"HH24", IsoSqlFormatTokenizer::TokenItem(HOUR_IN_DAY, false, true)},
  {"MI", IsoSqlFormatTokenizer::TokenItem(MINUTE_IN_HOUR, false, true)},
  {"SS", IsoSqlFormatTokenizer::TokenItem(SECOND_IN_MINUTE, false, true)},
  {"SSSSS", IsoSqlFormatTokenizer::TokenItem(SECOND_IN_DAY, false, true)},
  {"FF", IsoSqlFormatTokenizer::TokenItem(FRACTION, false, true)},
  {"FF1", IsoSqlFormatTokenizer::TokenItem(FRACTION, false, true)},
  {"FF2", IsoSqlFormatTokenizer::TokenItem(FRACTION, false, true)},
  {"FF3", IsoSqlFormatTokenizer::TokenItem(FRACTION, false, true)},
  {"FF4", IsoSqlFormatTokenizer::TokenItem(FRACTION, false, true)},
  {"FF5", IsoSqlFormatTokenizer::TokenItem(FRACTION, false, true)},
  {"FF6", IsoSqlFormatTokenizer::TokenItem(FRACTION, false, true)},
  {"FF7", IsoSqlFormatTokenizer::TokenItem(FRACTION, false, true)},
  {"FF8", IsoSqlFormatTokenizer::TokenItem(FRACTION, false, true)},
  {"FF9", IsoSqlFormatTokenizer::TokenItem(FRACTION, false, true)},
  {"AM", IsoSqlFormatTokenizer::TokenItem(MERIDIEM_INDICATOR, false, true)},
  {"A.M.", IsoSqlFormatTokenizer::TokenItem(MERIDIEM_INDICATOR, false, true)},
  {"PM", IsoSqlFormatTokenizer::TokenItem(MERIDIEM_INDICATOR, false, true)},
  {"P.M.", IsoSqlFormatTokenizer::TokenItem(MERIDIEM_INDICATOR, false, true)},
  {"TZH", IsoSqlFormatTokenizer::TokenItem(TIMEZONE_HOUR, false, true)},
  {"TZM", IsoSqlFormatTokenizer::TokenItem(TIMEZONE_MIN, false, true)},
  {"T", IsoSqlFormatTokenizer::TokenItem(ISO8601_TIME_INDICATOR, false, true)},
  {"Z", IsoSqlFormatTokenizer::TokenItem(ISO8601_ZULU_INDICATOR, false, true)},
  {"FM", IsoSqlFormatTokenizer::TokenItem(FM_MODIFIER, false, false)},
  {"FX", IsoSqlFormatTokenizer::TokenItem(FX_MODIFIER, false, false)},
  {"MONTH", IsoSqlFormatTokenizer::TokenItem(MONTH_NAME, true, false)},
  {"MON", IsoSqlFormatTokenizer::TokenItem(MONTH_NAME_SHORT, true, false)},
  {"DAY", IsoSqlFormatTokenizer::TokenItem(DAY_NAME, true, false)},
  {"DY", IsoSqlFormatTokenizer::TokenItem(DAY_NAME_SHORT, true, false)},
  {"D", IsoSqlFormatTokenizer::TokenItem(DAY_OF_WEEK, true, false)},
  {"Q", IsoSqlFormatTokenizer::TokenItem(QUARTER_OF_YEAR, true, false)},
  {"WW", IsoSqlFormatTokenizer::TokenItem(WEEK_OF_YEAR, true, false)},
  {"W", IsoSqlFormatTokenizer::TokenItem(WEEK_OF_MONTH, true, false)},
  {"IYYY", IsoSqlFormatTokenizer::TokenItem(ISO8601_WEEK_NUMBERING_YEAR, true, false)},
  {"IYY", IsoSqlFormatTokenizer::TokenItem(ISO8601_WEEK_NUMBERING_YEAR, true, false)},
  {"IY", IsoSqlFormatTokenizer::TokenItem(ISO8601_WEEK_NUMBERING_YEAR, true, false)},
  {"I", IsoSqlFormatTokenizer::TokenItem(ISO8601_WEEK_NUMBERING_YEAR, true, false)},
  {"IW", IsoSqlFormatTokenizer::TokenItem(ISO8601_WEEK_OF_YEAR, true, false)},
  {"ID", IsoSqlFormatTokenizer::TokenItem(ISO8601_DAY_OF_WEEK, true, false)}
});

const unordered_map<string, int> IsoSqlFormatTokenizer::SPECIAL_LENGTHS({
  {"HH12", 2}, {"HH24", 2}, {"FF", 9}, {"FF1", 1}, {"FF2", 2}, {"FF4", 4}, {"FF5", 5},
  {"FF6", 6}, {"FF7", 7}, {"FF8", 8}, {"FF9", 9}, {"TZM", 2},
  {"MONTH", MAX_MONTH_NAME_LENGTH}, {"DAY", MAX_DAY_NAME_LENGTH},
  {"DY", SHORT_DAY_NAME_LENGTH}});

const unsigned IsoSqlFormatTokenizer::MAX_TOKEN_SIZE = 5;

const int IsoSqlFormatTokenizer::MAX_FORMAT_LENGTH = 100;

const int IsoSqlFormatTokenizer::FRACTIONAL_MAX_LEN = 9;

FormatTokenizationResult IsoSqlFormatTokenizer::Tokenize() {
  DCHECK(dt_ctx_ != nullptr);
  DCHECK(dt_ctx_->fmt != nullptr);
  DCHECK(dt_ctx_->fmt_len > 0);
  DCHECK(dt_ctx_->toks.size() == 0);
  DCHECK(used_tokens_.empty());
  if (dt_ctx_->fmt_len > MAX_FORMAT_LENGTH) return TOO_LONG_FORMAT_ERROR;
  const char* str_end = dt_ctx_->fmt + dt_ctx_->fmt_len;
  const char* current_pos = dt_ctx_->fmt;
  ProcessFXModifier(&current_pos);
  while (current_pos < str_end) {
    ProcessSeparators(&current_pos);
    if (current_pos == str_end) break;
    FormatTokenizationResult parse_result = ProcessNextToken(&current_pos);
    if (parse_result != SUCCESS) return parse_result;
  }
  if (dt_ctx_->toks.empty()) return GENERAL_ERROR;
  return CheckIncompatibilities();
}

void IsoSqlFormatTokenizer::ProcessSeparators(const char** current_pos) {
  DCHECK(current_pos != nullptr && *current_pos != nullptr);
  const char* str_begin = dt_ctx_->fmt;
  const char* str_end = dt_ctx_->fmt + dt_ctx_->fmt_len;
  while (*current_pos < str_end && IsSeparator(current_pos, str_end)) {
    dt_ctx_->toks.push_back(DateTimeFormatToken(SEPARATOR, *current_pos - str_begin, 1,
        *current_pos));
    ++(*current_pos);
  }
}

FormatTokenizationResult IsoSqlFormatTokenizer::ProcessNextToken(
    const char** current_pos) {
  DCHECK(current_pos != nullptr && *current_pos != nullptr);
  const char* str_begin = dt_ctx_->fmt;
  const char* str_end = dt_ctx_->fmt + dt_ctx_->fmt_len;
  DCHECK(str_begin <= *current_pos && *current_pos < str_end);
  if (IsStartOfTextToken(*current_pos)) {
    fm_modifier_active_ = false;
    return ProcessTextToken(current_pos, str_begin, str_end);
  }
  unsigned curr_token_size =
      min(MAX_TOKEN_SIZE, static_cast<unsigned>(str_end - *current_pos));
  string token_to_probe(*current_pos, curr_token_size);
  boost::to_upper(token_to_probe);
  while (curr_token_size > 0) {
    token_to_probe.resize(curr_token_size);
    const auto token = VALID_TOKENS.find(token_to_probe);
    if (token != VALID_TOKENS.end()) {
      if (token->second.type == FX_MODIFIER) return MISPLACED_FX_MODIFIER_ERROR;
      if (token->second.type == FM_MODIFIER) {
        fm_modifier_active_ = true;
        *current_pos += curr_token_size;
        return SUCCESS;
      }
      if (cast_mode_ == PARSE && IsUsedToken(token_to_probe)) return DUPLICATE_FORMAT;
      if (!accept_time_toks_ && token->second.time_token) return DATE_WITH_TIME_ERROR;
      int max_len = GetMaxTokenLength(token->first);
      DateTimeFormatToken format_token(DateTimeFormatToken(
          token->second.type, *current_pos - str_begin, max_len, *current_pos));
      dt_ctx_->fmt_out_len += max_len;
      if (token->second.type == YEAR || token->second.type == ROUND_YEAR
          || token->second.type == ISO8601_WEEK_NUMBERING_YEAR) {
        format_token.divisor = std::pow(10, max_len);
      } else if (token->second.type == FRACTION) {
        format_token.divisor = std::pow(10, FRACTIONAL_MAX_LEN - max_len);
      }
      if (fm_modifier_active_) {
        fm_modifier_active_ = false;
        format_token.fm_modifier = true;
      }
      dt_ctx_->toks.push_back(format_token);
      dt_ctx_->has_date_toks |= token->second.date_token;
      dt_ctx_->has_time_toks |= token->second.time_token;
      used_tokens_.insert(token_to_probe);
      *current_pos += curr_token_size;
      return SUCCESS;
    }
    --curr_token_size;
  }
  return GENERAL_ERROR;
}

int IsoSqlFormatTokenizer::GetMaxTokenLength(const string& token) const {
  const auto length_it = SPECIAL_LENGTHS.find(token);
  if (length_it != SPECIAL_LENGTHS.end()) return length_it->second;
  return token.length();
}

bool IsoSqlFormatTokenizer::IsUsedToken(const string& token) const {
  return used_tokens_.find(token) != used_tokens_.end();
}

bool IsoSqlFormatTokenizer::IsMeridiemIndicatorProvided() const {
  return IsUsedToken("AM") || IsUsedToken("A.M.") || IsUsedToken("PM") ||
      IsUsedToken("P.M.");
}

FormatTokenizationResult IsoSqlFormatTokenizer::CheckIncompatibilities() const {
  DCHECK(dt_ctx_ != nullptr);
  if (!dt_ctx_->has_date_toks && !dt_ctx_->has_time_toks) return NO_DATETIME_TOKENS_ERROR;

  if (cast_mode_ == FORMAT) {
    if (IsUsedToken("TZH") || IsUsedToken("TZM")) {
      return TIMEZONE_OFFSET_NOT_ALLOWED_ERROR;
    }
    return SUCCESS;
  }

  DCHECK(cast_mode_ == PARSE);
  if(!dt_ctx_->has_date_toks) return NO_DATE_TOKENS_ERROR;

  short provided_year_count = IsUsedToken("YYYY") + IsUsedToken("YYY") +
      IsUsedToken("YY") + IsUsedToken("Y");
  short provided_round_year_count = IsUsedToken("RRRR") + IsUsedToken("RR");
  short provided_iso8601_week_numbering_year_count = IsUsedToken("IYYY") +
      IsUsedToken("IYY") + IsUsedToken("IY") + IsUsedToken("I");

  if (provided_year_count > 1 || provided_round_year_count > 1 ||
      provided_iso8601_week_numbering_year_count > 1) {
    return CONFLICTING_YEAR_TOKENS_ERROR;
  }

  if (provided_year_count == 1 && provided_round_year_count == 1) {
    return YEAR_WITH_ROUNDED_YEAR_ERROR;
  }

  if (IsUsedToken("Q")) return QUARTER_NOT_ALLOWED_FOR_PARSING;

  if (IsUsedToken("WW") || IsUsedToken("W")) return WEEK_NUMBER_NOT_ALLOWED_FOR_PARSING;

  short provided_month_count = IsUsedToken("MM") + IsUsedToken("MONTH")
      + IsUsedToken("MON");
  if (provided_month_count > 1) return CONFLICTING_MONTH_TOKENS_ERROR;

  bool is_used_day_of_year = IsUsedToken("DDD");
  bool is_used_day_of_month = IsUsedToken("DD");
  if (is_used_day_of_year && (is_used_day_of_month || provided_month_count == 1)) {
    return DAY_OF_YEAR_TOKEN_CONFLICT;
  }

  if (IsUsedToken("D")) return DAY_OF_WEEK_NOT_ALLOWED_FOR_PARSING;

  short provided_hour_tokens = IsUsedToken("HH") + IsUsedToken("HH12") +
      IsUsedToken("HH24");
  if (provided_hour_tokens > 1) {
    return CONFLICTING_HOUR_TOKENS_ERROR;
  }

  short provided_median_tokens = IsUsedToken("AM") + IsUsedToken("A.M.") +
      IsUsedToken("PM") + IsUsedToken("P.M.");
  if (provided_median_tokens > 1) return CONFLICTING_MERIDIEM_TOKENS_ERROR;

  if (IsMeridiemIndicatorProvided() && IsUsedToken("HH24")) {
    return MERIDIEM_CONFLICTS_WITH_HOUR_ERROR;
  }

  if (IsMeridiemIndicatorProvided() && !IsUsedToken("HH") && !IsUsedToken("HH12")) {
    return MISSING_HOUR_TOKEN_ERROR;
  }

  if (IsUsedToken("SSSSS") &&
       (IsUsedToken("HH") || IsUsedToken("HH12") || IsUsedToken("HH24") ||
        IsUsedToken("MI") || IsUsedToken("SS") || IsMeridiemIndicatorProvided())) {
    return SECOND_IN_DAY_CONFLICT;
  }

  if (IsUsedToken("TZM") && !IsUsedToken("TZH")) return MISSING_TZH_TOKEN_ERROR;

  short provided_fractional_second_count = IsUsedToken("FF") + IsUsedToken("FF1") +
      IsUsedToken("FF2") + IsUsedToken("FF3") + IsUsedToken("FF4") + IsUsedToken("FF5") +
      IsUsedToken("FF6") + IsUsedToken("FF7") + IsUsedToken("FF8") + IsUsedToken("FF9");
  if (provided_fractional_second_count > 1) {
    return CONFLICTING_FRACTIONAL_SECOND_TOKENS_ERROR;
  }

  short provided_day_of_week_count = IsUsedToken("ID") + IsUsedToken("DAY") +
      IsUsedToken("DY");
  if (provided_day_of_week_count > 1) return CONFLICTING_DAY_OF_WEEK_TOKENS_ERROR;

  bool is_used_iso8601_week_of_year = IsUsedToken("IW");
  if (provided_iso8601_week_numbering_year_count == 1 || is_used_iso8601_week_of_year ||
      provided_day_of_week_count == 1) {
    if (provided_year_count == 1 || provided_round_year_count == 1 ||
        provided_month_count == 1 || is_used_day_of_year || is_used_day_of_month) {
      if (IsUsedToken("DAY") || IsUsedToken("DY")) {
        return DAY_NAME_NOT_ALLOWED_FOR_PARSING;
      }
      return CONFLICTING_DATE_TOKENS_ERROR;
    }
    if (provided_iso8601_week_numbering_year_count == 0 ||
        !is_used_iso8601_week_of_year || provided_day_of_week_count == 0) {
      return MISSING_ISO8601_WEEK_BASED_TOKEN_ERROR;
    }
  }

  return SUCCESS;
}

bool IsoSqlFormatTokenizer::IsSeparator(const char** current_pos, const char* str_end,
    bool read_escaped_single_quotes) {
  DCHECK(str_end != nullptr);
  DCHECK(current_pos != nullptr && *current_pos != nullptr && *current_pos < str_end);
  char c = **current_pos;
  if (c == '-' || c == ':' || c == ' ' || c == '.' || c == '/' || c == ',' ||
      c == '\'' || c == ';') {
    return true;
  }
  if (read_escaped_single_quotes && str_end - *current_pos > 1 &&
      strncmp(*current_pos, "\\'", 2) == 0) {
    ++(*current_pos);
    return true;
  }
  return false;
}

void IsoSqlFormatTokenizer::ProcessFXModifier(const char** current_pos) {
  DCHECK(current_pos != nullptr && *current_pos != nullptr);
  DCHECK(*current_pos == dt_ctx_->fmt);
  if (strncasecmp(*current_pos, "FX", 2) == 0) {
    dt_ctx_->fx_modifier = true;
    *current_pos += 2;
  }
}

bool IsoSqlFormatTokenizer::IsStartOfTextToken(const char* current_pos) {
  DCHECK(current_pos != nullptr);
  if (*current_pos == '"' || strncmp(current_pos, "\\\"", 2) == 0) return true;
  return false;
}

FormatTokenizationResult IsoSqlFormatTokenizer::ProcessTextToken(
    const char** current_pos, const char* str_begin, const char* str_end) {
  DCHECK(dt_ctx_ != nullptr);
  DCHECK(current_pos != nullptr);
  DCHECK(*current_pos != nullptr);
  DCHECK(str_begin != nullptr);
  DCHECK(str_end != nullptr);
  DCHECK(str_begin <= *current_pos && *current_pos < str_end);
  bool is_escaped = (**current_pos == '\\');
  // Exclude opening quotation marks from the stored token.
  *current_pos += is_escaped ? 2 : 1;
  const char* start_of_content = *current_pos;
  *current_pos = FindEndOfTextToken(*current_pos, str_end, is_escaped);
  if (*current_pos == nullptr) return TEXT_TOKEN_NOT_CLOSED;
  DateTimeFormatToken token(TEXT, start_of_content - str_begin,
      *current_pos - start_of_content - 1 - is_escaped, start_of_content);
  token.is_double_escaped = is_escaped;
  dt_ctx_->toks.push_back(token);
  return SUCCESS;
}

const char* IsoSqlFormatTokenizer::FindEndOfTextToken(const char* str_start,
    const char* str_end, bool is_escaped) {
  DCHECK(str_start != nullptr);
  const char* current_pos = str_start;
  while (current_pos < str_end) {
    // Skip escaped double quotes.
    if (!is_escaped && strncmp(current_pos, "\\\"", 2) == 0) {
      current_pos += 2;
      continue;
    }
    if (is_escaped && strncmp(current_pos, "\\\\\\\"", 4) == 0) {
      current_pos += 4;
      continue;
    }
    // Skip escaped backslash.
    if (strncmp(current_pos, "\\\\", 2) == 0) {
      current_pos += 2;
      continue;
    }
    if (!is_escaped && *current_pos == '"') {
      ++current_pos;
      return current_pos;
    }
    if (is_escaped && strncmp(current_pos, "\\\"", 2) == 0) {
      current_pos += 2;
      return current_pos;
    }
    ++current_pos;
  }
  return nullptr;
}

}
}
