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

#include "runtime/timestamp-parse-util.h"
#include <boost/assign/list_of.hpp>

namespace assign = boost::assign;
using boost::unordered_map;
using boost::gregorian::date;
using boost::gregorian::date_duration;
using boost::posix_time::hours;
using boost::posix_time::not_a_date_time;
using boost::posix_time::time_duration;

namespace impala {

bool TimestampParser::initialized_ = false;
unordered_map<StringValue, int> TimestampParser::REV_MONTH_INDEX;
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
    bool is_valid_date = true;
    try {
      DCHECK(-1 <= day_offset && day_offset <= 1);
      if ((dt_result.year == 1400 && dt_result.month == 1 && dt_result.day == 1 &&
           day_offset == -1) ||
          (dt_result.year == 9999 && dt_result.month == 12 && dt_result.day == 31 &&
           day_offset == 1)) {
        // Have to check lower/upper bound explicitly.
        // Tried date::is_not_a_date_time() but it doesn't complain value is out of range
        // for "'1400-01-01' - 1 day" and "'9999-12-31' + 1 day".
        is_valid_date = false;
      } else {
        *d = date(dt_result.year, dt_result.month, dt_result.day);
        *d += date_duration(day_offset);
      }
    } catch (boost::exception& e) {
      is_valid_date = false;
    }
    if (!is_valid_date) {
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

}
