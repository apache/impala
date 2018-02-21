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

#include "exprs/timestamp-functions.h"

#include <boost/algorithm/string.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <ctime>

#include "exprs/anyval-util.h"
#include "exprs/timezone_db.h"
#include "gutil/strings/substitute.h"
#include "runtime/string-value.inline.h"
#include "runtime/timestamp-parse-util.h"
#include "runtime/timestamp-value.h"
#include "udf/udf-internal.h"
#include "udf/udf.h"

#include "common/names.h"

using boost::algorithm::iequals;
using boost::local_time::local_date_time;
using boost::local_time::time_zone_ptr;
using boost::posix_time::ptime;
using boost::posix_time::to_iso_extended_string;

typedef boost::gregorian::date Date;

namespace impala {

const string TimestampFunctions::DAY_ARRAY[7] = {"Sun", "Mon", "Tue", "Wed", "Thu",
    "Fri", "Sat"};
const string TimestampFunctions::DAYNAME_ARRAY[7] = {"Sunday", "Monday", "Tuesday",
    "Wednesday", "Thursday", "Friday", "Saturday"};
const string TimestampFunctions::MONTH_ARRAY[12] = {"Jan", "Feb", "Mar", "Apr", "May",
    "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
const string TimestampFunctions::MONTHNAME_ARRAY[12] = {"January", "February", "March",
    "April", "May", "June", "July", "August", "September", "October", "November",
    "December"};

namespace {
/// Uses Boost's internal checking to throw an exception if 'date' is out of the
/// supported range of boost::gregorian.
void ThrowIfDateOutOfRange(const boost::gregorian::date& date) {
  // Boost checks the ranges when instantiating the year/month/day representations.
  boost::gregorian::greg_year year = date.year();
  boost::gregorian::greg_month month = date.month();
  boost::gregorian::greg_day day = date.day();
  // Ensure Boost's validation is effective.
  DCHECK_GE(year, boost::gregorian::greg_year::min());
  DCHECK_LE(year, boost::gregorian::greg_year::max());
  DCHECK_GE(month, boost::gregorian::greg_month::min());
  DCHECK_LE(month, boost::gregorian::greg_month::max());
  DCHECK_GE(day, boost::gregorian::greg_day::min());
  DCHECK_LE(day, boost::gregorian::greg_day::max());
}
}

// This function uses inline asm functions, which we believe to be from the boost library.
// Inline asm is not currently supported by JIT, so this function should always be run in
// the interpreted mode. This is handled in LlvmCodeGen::LoadFunction().
TimestampVal TimestampFunctions::FromUtc(FunctionContext* context,
    const TimestampVal& ts_val, const StringVal& tz_string_val) {
  if (ts_val.is_null || tz_string_val.is_null) return TimestampVal::null();
  const TimestampValue& ts_value = TimestampValue::FromTimestampVal(ts_val);
  if (!ts_value.HasDateAndTime()) return TimestampVal::null();

  const StringValue& tz_string_value = StringValue::FromStringVal(tz_string_val);
  time_zone_ptr timezone = TimezoneDatabase::FindTimezone(
      string(tz_string_value.ptr, tz_string_value.len), ts_value, true);
  if (timezone == NULL) {
    // This should return null. Hive just ignores it.
    stringstream ss;
    ss << "Unknown timezone '" << tz_string_value << "'" << endl;
    context->AddWarning(ss.str().c_str());
    return ts_val;
  }

  try {
    ptime temp;
    ts_value.ToPtime(&temp);
    local_date_time lt(temp, timezone);
    ptime local_time = lt.local_time();
    ThrowIfDateOutOfRange(local_time.date());
    TimestampVal return_val;
    TimestampValue(local_time).ToTimestampVal(&return_val);
    return return_val;
  } catch (boost::exception&) {
    const string& msg = Substitute(
        "Timestamp '$0' did not convert to a valid local time in timezone '$1'",
        ts_value.ToString(), tz_string_value.DebugString());
    context->AddWarning(msg.c_str());
    return TimestampVal::null();
  }
}

// This function uses inline asm functions, which we believe to be from the boost library.
// Inline asm is not currently supported by JIT, so this function should always be run in
// the interpreted mode. This is handled in LlvmCodeGen::LoadFunction().
TimestampVal TimestampFunctions::ToUtc(FunctionContext* context,
    const TimestampVal& ts_val, const StringVal& tz_string_val) {
  if (ts_val.is_null || tz_string_val.is_null) return TimestampVal::null();
  const TimestampValue& ts_value = TimestampValue::FromTimestampVal(ts_val);
  if (!ts_value.HasDateAndTime()) return TimestampVal::null();

  const StringValue& tz_string_value = StringValue::FromStringVal(tz_string_val);
  time_zone_ptr timezone = TimezoneDatabase::FindTimezone(
      string(tz_string_value.ptr, tz_string_value.len), ts_value, false);
  // This should raise some sort of error or at least null. Hive Just ignores it.
  if (UNLIKELY(timezone == NULL)) {
    stringstream ss;
    ss << "Unknown timezone '" << tz_string_value << "'" << endl;
    context->AddWarning(ss.str().c_str());
    return ts_val;
  }

  if (UNLIKELY(timezone == TimezoneDatabase::TIMEZONE_MSK_PRE_2014 &&
      ts_value.date().day_number() == 2456957 &&
      ts_value.time().hours() >= 1)) {
    // On October 27, 2014 at 1:00 am MSC, Moscow time transitions from UTC+4 with no DST
    // to UTC+3 with no DST. Because of this, 1am to 1:59:59.999...am MSC happens twice.
    // We want to be consistent with the existing rule of "in case of ambiguity return
    // NULL".
    DCHECK_LT(ts_value.time().hours(), 2);
    return TimestampVal::null();
  }
  try {
    local_date_time lt(ts_value.date(), ts_value.time(), timezone,
        local_date_time::NOT_DATE_TIME_ON_ERROR);
    ptime utc_time = lt.utc_time();
    // The utc_time() conversion does not check ranges - need to explicitly check.
    ThrowIfDateOutOfRange(utc_time.date());
    TimestampVal return_val;
    TimestampValue(utc_time).ToTimestampVal(&return_val);
    return return_val;
  } catch (boost::exception&) {
    const string& msg =
        Substitute("Timestamp '$0' in timezone '$1' could not be converted to UTC",
            ts_value.ToString(), tz_string_value.DebugString());
    context->AddWarning(msg.c_str());
    return TimestampVal::null();
  }
}

void TimestampFunctions::UnixAndFromUnixPrepare(
    FunctionContext* context, FunctionContext::FunctionStateScope scope) {
  if (scope != FunctionContext::THREAD_LOCAL) return;
  DateTimeFormatContext* dt_ctx = NULL;
  if (context->IsArgConstant(1)) {
    StringVal fmt_val = *reinterpret_cast<StringVal*>(context->GetConstantArg(1));
    const StringValue& fmt_ref = StringValue::FromStringVal(fmt_val);
    if (fmt_val.is_null || fmt_ref.len == 0) {
      TimestampFunctions::ReportBadFormat(context, fmt_val, true);
      return;
    }
    dt_ctx = new DateTimeFormatContext(fmt_ref.ptr, fmt_ref.len);
    bool parse_result = TimestampParser::ParseFormatTokens(dt_ctx);
    if (!parse_result) {
      delete dt_ctx;
      TimestampFunctions::ReportBadFormat(context, fmt_val, true);
      return;
    }
  } else {
    // If our format string is constant, then we benefit from it only being parsed once in
    // the code above. If it's not constant, then we can reuse a context by resetting it.
    // This is much cheaper vs alloc/dealloc'ing a context for each evaluation.
    dt_ctx = new DateTimeFormatContext();
  }
  dt_ctx->SetCenturyBreak(*context->impl()->state()->now());
  context->SetFunctionState(scope, dt_ctx);
}

void TimestampFunctions::UnixAndFromUnixClose(FunctionContext* context,
    FunctionContext::FunctionStateScope scope) {
  if (scope == FunctionContext::THREAD_LOCAL) {
    DateTimeFormatContext* dt_ctx =
        reinterpret_cast<DateTimeFormatContext*>(context->GetFunctionState(scope));
    delete dt_ctx;
    context->SetFunctionState(scope, nullptr);
  }
}

time_zone_ptr TimezoneDatabase::FindTimezone(
    const string& tz, const TimestampValue& tv, bool tv_in_utc) {
  // The backing database does not handle timezone rule changes.
  if (iequals("Europe/Moscow", tz) || iequals("Moscow", tz) || iequals("MSK", tz)) {
    if (tv.date().year() < 2011 || (tv.date().year() == 2011 && tv.date().month() < 4)) {
      // Between January 19, 1992 and March 27, 2011 Moscow time was UTC+3 with DST. On
      // March 27, 2011 Moscow time transitioned to UTC+4 with no DST. NOTE: We currently
      // do not handle Moscow time conversions for dates before January 19, 1992
      // correctly (Impala incorrectly thinks the Moscow timezone is UTC+3 with DST
      // instead of UTC+2 with DST for those dates).
      return TIMEZONE_MSK_PRE_2011_DST;
    }
    // On October 26, 2014 at 22:00:00 UTC, Moscow time transitioned to UTC+3 with no
    // DST. We have to make a precise time check here, unlike in the case above, because
    // we can't rely on the timezone database to handle the moment of transition because
    // the rule change does not coincide with a DST change.
    const int MSK_TRANSITION_DAY = 2456956;
    const int MSK_TRANSITION_HOUR_UTC = 22;
    const int MSK_UTC_OFFSET = 4;
    if (tv.date().day_number() < MSK_TRANSITION_DAY) return TIMEZONE_MSK_PRE_2014;
    if (tv_in_utc) {
      if (tv.date().day_number() == MSK_TRANSITION_DAY &&
          tv.time().hours() < MSK_TRANSITION_HOUR_UTC) {
        return TIMEZONE_MSK_PRE_2014;
      }
    } else if (tv.date().day_number() < MSK_TRANSITION_DAY + 1 || (
        tv.date().day_number() == MSK_TRANSITION_DAY + 1 &&
        tv.time().hours() < (MSK_TRANSITION_HOUR_UTC + MSK_UTC_OFFSET) % 24)) {
      return TIMEZONE_MSK_PRE_2014;
    }
  }

  // See if they specified a zone id
  time_zone_ptr tzp = tz_database_.time_zone_from_region(tz);
  if (tzp != NULL) return tzp;

  for (vector<string>::const_iterator iter = tz_region_list_.begin();
       iter != tz_region_list_.end(); ++iter) {
    time_zone_ptr tzp = tz_database_.time_zone_from_region(*iter);
    DCHECK(tzp != NULL);
    if (tzp->dst_zone_abbrev() == tz) return tzp;
    if (tzp->std_zone_abbrev() == tz) return tzp;
    if (tzp->dst_zone_name() == tz) return tzp;
    if (tzp->std_zone_name() == tz) return tzp;
  }
  return time_zone_ptr();
}

}
