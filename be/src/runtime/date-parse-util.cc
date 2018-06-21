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
#include "runtime/string-value.inline.h"
#include "runtime/timestamp-value.h"
#include "util/string-parser.h"

#include "common/names.h"

using boost::gregorian::date;
using boost::posix_time::ptime;
using boost::posix_time::time_duration;

namespace impala {

using datetime_parse_util::DEFAULT_DATE_CTX;
using datetime_parse_util::DEFAULT_DATE_FMT_LEN;

using datetime_parse_util::DateTimeFormatContext;
using datetime_parse_util::DateTimeFormatToken;
using datetime_parse_util::DateTimeParseResult;

using datetime_parse_util::IsParseCtxInitialized;
using datetime_parse_util::ParseDateTime;
using datetime_parse_util::ParseFormatTokensByStr;
using datetime_parse_util::ParseDefaultFormatTokensByStr;

using datetime_parse_util::YEAR;
using datetime_parse_util::MONTH_IN_YEAR;
using datetime_parse_util::MONTH_IN_YEAR_SLT;
using datetime_parse_util::DAY_IN_MONTH;
using datetime_parse_util::SEPARATOR;

bool DateParser::Parse(const char* str, int len, const DateTimeFormatContext& dt_ctx,
    DateValue* date) {
  DCHECK(IsParseCtxInitialized());
  DCHECK(dt_ctx.toks.size() > 0);
  // 'dt_ctx' must have date tokens. Time tokens are accepted but ignored.
  DCHECK(dt_ctx.has_date_toks);
  DCHECK(date != nullptr);

  DateTimeParseResult dt_result;
  if (UNLIKELY(str == nullptr || len <= 0
      || !ParseDateTime(str, len, dt_ctx, &dt_result))) {
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

bool DateParser::Parse(const char* str, int len, bool accept_time_toks, DateValue* date) {
  DCHECK(IsParseCtxInitialized());
  DCHECK(date != nullptr);
  if (UNLIKELY(str == nullptr)) {
    *date = DateValue();
    return false;
  }

  int trimmed_len = len;
  // Remove leading white space.
  while (trimmed_len > 0 && isspace(*str)) {
    ++str;
    --trimmed_len;
  }
  // Strip the trailing blanks.
  while (trimmed_len > 0 && isspace(str[trimmed_len - 1])) --trimmed_len;

  if (UNLIKELY(trimmed_len <= 0)) {
    *date = DateValue();
    return false;
  }

  // Determine the default formatting context that's required for parsing.
  const DateTimeFormatContext* dt_ctx = ParseDefaultFormatTokensByStr(str, trimmed_len,
      accept_time_toks, false);

  if (dt_ctx != nullptr) return Parse(str, trimmed_len, *dt_ctx, date);

  // Generating context lazily as a fall back if default formats fail.
  // ParseFormatTokenByStr() does not require a template format string.
  DateTimeFormatContext lazy_ctx(str, trimmed_len);
  if (!ParseFormatTokensByStr(&lazy_ctx, accept_time_toks, false)) {
    *date = DateValue();
    return false;
  }
  return Parse(str, trimmed_len, lazy_ctx, date);
}

int DateParser::Format(const DateTimeFormatContext& dt_ctx, const DateValue& date,
    int len, char* buff) {
  DCHECK(IsParseCtxInitialized());
  DCHECK(dt_ctx.toks.size() > 0);
  DCHECK(dt_ctx.has_date_toks && !dt_ctx.has_time_toks);
  DCHECK(len > dt_ctx.fmt_out_len);
  DCHECK(buff != nullptr);

  int year, month, day;
  if (!date.ToYearMonthDay(&year, &month, &day)) return -1;

  char* str = buff;
  for (const DateTimeFormatToken& tok: dt_ctx.toks) {
    int32_t num_val = -1;
    const char* str_val = nullptr;
    int str_val_len = 0;
    switch (tok.type) {
      case YEAR: {
        num_val = year;
        if (tok.len <= 3) num_val %= 100;
        break;
      }
      case MONTH_IN_YEAR: num_val = month; break;
      case MONTH_IN_YEAR_SLT: {
        str_val = TimestampFunctions::MONTH_ARRAY[month - 1].c_str();
        str_val_len = 3;
        break;
      }
      case DAY_IN_MONTH: num_val = day; break;
      case SEPARATOR: {
        str_val = tok.val;
        str_val_len = tok.len;
        break;
      }
      default: DCHECK(false) << "Unknown date format token";
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

}
