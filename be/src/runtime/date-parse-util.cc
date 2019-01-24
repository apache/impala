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

#include "runtime/date-parse-util.h"

#include <boost/date_time/gregorian/gregorian.hpp>

#include "cctz/civil_time.h"
#include "exprs/timestamp-functions.h"
#include "runtime/datetime-iso-sql-format-parser.h"
#include "runtime/datetime-simple-date-format-parser.h"
#include "runtime/string-value.inline.h"
#include "runtime/timestamp-value.h"
#include "util/string-parser.h"

#include "common/names.h"

using boost::gregorian::date;
using boost::posix_time::ptime;
using boost::posix_time::time_duration;

namespace impala {

using namespace datetime_parse_util;

bool DateParser::ParseSimpleDateFormat(const char* str, int len,
    const DateTimeFormatContext& dt_ctx, DateValue* date) {
  DCHECK(dt_ctx.toks.size() > 0);
  // 'dt_ctx' must have date tokens. Time tokens are accepted but ignored.
  DCHECK(dt_ctx.has_date_toks);
  DCHECK(date != nullptr);

  DateTimeParseResult dt_result;
  if (UNLIKELY(str == nullptr || len <= 0
      || !SimpleDateFormatParser::ParseDateTime(str, len, dt_ctx, &dt_result))) {
    *date = DateValue();
    return false;
  }

  int year = dt_result.realign_year ?
      RealignYear(dt_result, dt_ctx.century_break_ptime) :
      dt_result.year;

  *date = DateValue(year, dt_result.month, dt_result.day);
  return date->IsValid();
}

int DateParser::RealignYear(const DateTimeParseResult& dt_result,
    const ptime& century_break) {
  DCHECK(dt_result.realign_year);
  DCHECK(dt_result.year < 100);
  DCHECK(!century_break.is_special());

  const date& cb_date = century_break.date();
  const time_duration& cb_time = century_break.time_of_day();
  DCHECK(!cb_time.is_negative());

  // Let the century start at AABB and the year parsed be YY, this gives us AAYY.
  int year = dt_result.year + (cb_date.year() / 100) * 100;

  cctz::civil_day aligned_day(year, dt_result.month, dt_result.day);
  cctz::civil_day century_break_day(cb_date.year(), cb_date.month(), cb_date.day());

  // Advance 100 years if parsed time is before the century break.
  // For example if the century breaks at 1937 but year = 1936, the correct year would
  // be 2036.
  if (aligned_day < century_break_day
      || (aligned_day == century_break_day && (cb_time.ticks() > 0))) {
    year += 100;
  }

  return year;
}

bool DateParser::ParseSimpleDateFormat(const char* str, int len, bool accept_time_toks,
    DateValue* date) {
  DCHECK(date != nullptr);
  if (UNLIKELY(str == nullptr)) return IndicateDateParseFailure(date);

  int trimmed_len = len;
  // Remove leading white space.
  while (trimmed_len > 0 && isspace(*str)) {
    ++str;
    --trimmed_len;
  }
  // Strip the trailing blanks.
  while (trimmed_len > 0 && isspace(str[trimmed_len - 1])) --trimmed_len;
  if (UNLIKELY(trimmed_len <= 0)) return IndicateDateParseFailure(date);

  const DateTimeFormatContext* dt_ctx =
      SimpleDateFormatTokenizer::GetDefaultFormatContext(str, trimmed_len,
          accept_time_toks, false);
  if (dt_ctx != nullptr) return ParseSimpleDateFormat(str, trimmed_len, *dt_ctx, date);

  // Generating context lazily as a fall back if default formats fail.
  // ParseFormatTokenByStr() does not require a template format string.
  DateTimeFormatContext lazy_ctx(str, trimmed_len);
  if (!SimpleDateFormatTokenizer::TokenizeByStr(&lazy_ctx, accept_time_toks, false)) {
    return IndicateDateParseFailure(date);
  }
  return ParseSimpleDateFormat(str, trimmed_len, lazy_ctx, date);
}

bool DateParser::ParseIsoSqlFormat(const char* str, int len,
      const DateTimeFormatContext& dt_ctx, DateValue* date) {
  DCHECK(dt_ctx.toks.size() > 0);
  DCHECK(date != nullptr);
  DCHECK(dt_ctx.has_date_toks);

  if (UNLIKELY(str == nullptr || len <= 0)) return IndicateDateParseFailure(date);

  DateTimeParseResult dt_result;
  bool result = IsoSqlFormatParser::ParseDateTime(str, len, dt_ctx, &dt_result);
  if (!result) return IndicateDateParseFailure(date);

  *date = DateValue(dt_result.year, dt_result.month, dt_result.day);
  return date->IsValid();
}

string DateParser::Format(const DateTimeFormatContext& dt_ctx, const DateValue& date) {
  DCHECK(dt_ctx.toks.size() > 0);
  DCHECK(dt_ctx.has_date_toks && !dt_ctx.has_time_toks);

  int year, month, day;
  if (!date.ToYearMonthDay(&year, &month, &day)) return "";

  string result;
  result.reserve(dt_ctx.fmt_out_len);
  for (const DateTimeFormatToken& tok: dt_ctx.toks) {
    int32_t num_val = -1;
    const char* str_val = nullptr;
    int str_val_len = 0;
    switch (tok.type) {
      case YEAR:
      case ROUND_YEAR: {
        num_val = year;
        if (tok.len < 4) {
          int adjust_factor = std::pow(10, tok.len);
          num_val %= adjust_factor;
        }
        break;
      }
      case MONTH_IN_YEAR: num_val = month; break;
      case MONTH_IN_YEAR_SLT: {
        str_val = TimestampFunctions::MONTH_ARRAY[month - 1].c_str();
        str_val_len = 3;
        break;
      }
      case DAY_IN_MONTH: num_val = day; break;
      case DAY_IN_YEAR: {
        num_val = GetDayInYear(year, month, day);
        break;
      }
      case SEPARATOR: {
        str_val = tok.val;
        str_val_len = tok.len;
        break;
      }
      default: DCHECK(false) << "Unknown date format token";
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

bool DateParser::IndicateDateParseFailure(DateValue* date) {
  *date = DateValue();
  return false;
}

}
