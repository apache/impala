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
#include "runtime/string-value.inline.h"
#include "runtime/timestamp-parse-util.h"
#include "runtime/timestamp-value.h"
#include "udf/udf.h"
#include "udf/udf-internal.h"

#include "common/names.h"

using boost::algorithm::iequals;
using boost::local_time::local_date_time;
using boost::local_time::time_zone_ptr;
using boost::posix_time::ptime;
using boost::posix_time::to_iso_extended_string;

namespace impala {

// This function is not cross-compiled to avoid including unnecessary boost library's
// header files which bring in a bunch of unused code and global variables and increase
// the codegen time. boost::posix_time::to_iso_extended_string() is large enough that
// it won't benefit much from inlining.
string TimestampFunctions::ToIsoExtendedString(const TimestampValue& ts_value) {
  return to_iso_extended_string(ts_value.date());
}

// This function uses inline asm functions, which we believe to be from the boost library.
// Inline asm is not currently supported by JIT, so this function should always be run in
// the interpreted mode. This is handled in ScalarFnCall::GetUdf().
TimestampVal TimestampFunctions::FromUtc(FunctionContext* context,
    const TimestampVal& ts_val, const StringVal& tz_string_val) {
  if (ts_val.is_null || tz_string_val.is_null) return TimestampVal::null();
  const TimestampValue& ts_value = TimestampValue::FromTimestampVal(ts_val);
  if (!ts_value.HasDateOrTime()) return TimestampVal::null();

  const StringValue& tz_string_value = StringValue::FromStringVal(tz_string_val);
  time_zone_ptr timezone = TimezoneDatabase::FindTimezone(
      string(tz_string_value.ptr, tz_string_value.len), ts_value);
  if (timezone == NULL) {
    // This should return null. Hive just ignores it.
    stringstream ss;
    ss << "Unknown timezone '" << tz_string_value << "'" << endl;
    context->AddWarning(ss.str().c_str());
    return ts_val;
  }

  ptime temp;
  ts_value.ToPtime(&temp);
  local_date_time lt(temp, timezone);
  TimestampValue return_value = lt.local_time();
  TimestampVal return_val;
  return_value.ToTimestampVal(&return_val);
  return return_val;
}

// This function uses inline asm functions, which we believe to be from the boost library.
// Inline asm is not currently supported by JIT, so this function should always be run in
// the interpreted mode. This is handled in ScalarFnCall::GetUdf().
TimestampVal TimestampFunctions::ToUtc(FunctionContext* context,
    const TimestampVal& ts_val, const StringVal& tz_string_val) {
  if (ts_val.is_null || tz_string_val.is_null) return TimestampVal::null();
  const TimestampValue& ts_value = TimestampValue::FromTimestampVal(ts_val);
  if (!ts_value.HasDateOrTime()) return TimestampVal::null();

  const StringValue& tz_string_value = StringValue::FromStringVal(tz_string_val);
  time_zone_ptr timezone = TimezoneDatabase::FindTimezone(
      string(tz_string_value.ptr, tz_string_value.len), ts_value);
  // This should raise some sort of error or at least null. Hive Just ignores it.
  if (timezone == NULL) {
    stringstream ss;
    ss << "Unknown timezone '" << tz_string_value << "'" << endl;
    context->AddWarning(ss.str().c_str());
    return ts_val;
  }

  local_date_time lt(ts_value.date(), ts_value.time(),
      timezone, local_date_time::NOT_DATE_TIME_ON_ERROR);
  TimestampValue return_value(lt.utc_time());
  TimestampVal return_val;
  return_value.ToTimestampVal(&return_val);
  return return_val;
}

void TimestampFunctions::UnixAndFromUnixPrepare(FunctionContext* context,
    FunctionContext::FunctionStateScope scope) {
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
  context->SetFunctionState(scope, dt_ctx);
}

void TimestampFunctions::UnixAndFromUnixClose(FunctionContext* context,
    FunctionContext::FunctionStateScope scope) {
  if (scope == FunctionContext::THREAD_LOCAL) {
    DateTimeFormatContext* dt_ctx =
        reinterpret_cast<DateTimeFormatContext*>(context->GetFunctionState(scope));
    delete dt_ctx;
  }
}

time_zone_ptr TimezoneDatabase::FindTimezone(const string& tz, const TimestampValue& tv) {
  // The backing database does not capture some subtleties, there are special cases
  if ((tv.date().year() < 2011 || (tv.date().year() == 2011 && tv.date().month() < 4)) &&
      (iequals("Europe/Moscow", tz) || iequals("Moscow", tz) || iequals("MSK", tz))) {
    // We transition in pre April 2011 from using the tz_database_ to a custom rule
    // Russia stopped using daylight savings in 2011, the tz_database_ is
    // set up assuming Russia uses daylight saving every year.
    // Sun, Mar 27, 2:00AM Moscow clocks moved forward +1 hour (a total of GMT +4)
    // Specifically,
    // UTC Time 26 Mar 2011 22:59:59 +0000 ===> Sun Mar 27 01:59:59 MSK 2011
    // UTC Time 26 Mar 2011 23:00:00 +0000 ===> Sun Mar 27 03:00:00 MSK 2011
    // This means in 2011, The database rule will apply DST starting March 26 2011.
    // This will be a correct +4 offset, and the database rule can apply until
    // Oct 31 when tz_database_ will incorrectly attempt to turn clocks backwards 1 hour.
    return TIMEZONE_MSK_PRE_2011_DST;
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
