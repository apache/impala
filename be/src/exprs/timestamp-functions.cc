// Copyright 2012 Cloudera Inc.
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

#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/date_time/time_zone_base.hpp>
#include <boost/date_time/local_time/local_time.hpp>
#include <boost/algorithm/string.hpp>
#include <gutil/strings/substitute.h>

#include "exprs/timestamp-functions.h"
#include "exprs/expr.h"
#include "exprs/anyval-util.h"

#include "runtime/tuple-row.h"
#include "runtime/timestamp-value.h"
#include "util/path-builder.h"
#include "runtime/string-value.inline.h"
#include "udf/udf.h"
#include "udf/udf-internal.h"
#include "runtime/runtime-state.h"

#define TIMEZONE_DATABASE "be/files/date_time_zonespec.csv"

#include "common/names.h"

using boost::algorithm::iequals;
using boost::gregorian::days;
using boost::gregorian::months;
using boost::gregorian::weeks;
using boost::gregorian::years;
using boost::local_time::local_date_time;
using boost::local_time::time_zone_ptr;
using boost::posix_time::hours;
using boost::posix_time::microseconds;
using boost::posix_time::milliseconds;
using boost::posix_time::minutes;
using boost::posix_time::nanoseconds;
using boost::posix_time::ptime;
using boost::posix_time::seconds;
using namespace impala_udf;
using namespace strings;

namespace impala {

// Constant strings used for DayName function.
const char* TimestampFunctions::SUNDAY = "Sunday";
const char* TimestampFunctions::MONDAY = "Monday";
const char* TimestampFunctions::TUESDAY = "Tuesday";
const char* TimestampFunctions::WEDNESDAY = "Wednesday";
const char* TimestampFunctions::THURSDAY = "Thursday";
const char* TimestampFunctions::FRIDAY = "Friday";
const char* TimestampFunctions::SATURDAY = "Saturday";

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

template <class TIME>
StringVal TimestampFunctions::FromUnix(FunctionContext* context, const TIME& intp) {
  if (intp.is_null) return StringVal::null();
  TimestampValue t(intp.val);
  return AnyValUtil::FromString(context, lexical_cast<string>(t));
}

template <class TIME>
StringVal TimestampFunctions::FromUnix(FunctionContext* context, const TIME& intp,
    const StringVal& fmt) {
  if (fmt.is_null || fmt.len == 0) {
    TimestampFunctions::ReportBadFormat(context, fmt, false);
    return StringVal::null();
  }
  if (intp.is_null) return StringVal::null();

  TimestampValue t(intp.val);
  void* state = context->GetFunctionState(FunctionContext::THREAD_LOCAL);
  DateTimeFormatContext* dt_ctx = reinterpret_cast<DateTimeFormatContext*>(state);
  if (!context->IsArgConstant(1)) {
    dt_ctx->Reset(reinterpret_cast<const char*>(fmt.ptr), fmt.len);
    if (!TimestampParser::ParseFormatTokens(dt_ctx)){
      TimestampFunctions::ReportBadFormat(context, fmt, false);
      return StringVal::null();
    }
  }

  int buff_len = dt_ctx->fmt_out_len + 1;
  StringVal result(context, buff_len);
  result.len = t.Format(*dt_ctx, buff_len, reinterpret_cast<char*>(result.ptr));
  if (result.len <= 0) return StringVal::null();
  return result;
}

BigIntVal TimestampFunctions::Unix(FunctionContext* context, const StringVal& string_val,
    const StringVal& fmt) {
  if (fmt.is_null || fmt.len == 0) {
    TimestampFunctions::ReportBadFormat(context, fmt, false);
    return BigIntVal::null();
  }
  if (string_val.is_null || string_val.len == 0) return BigIntVal::null();

  void* state = context->GetFunctionState(FunctionContext::THREAD_LOCAL);
  DateTimeFormatContext* dt_ctx = reinterpret_cast<DateTimeFormatContext*>(state);
  if (!context->IsArgConstant(1)) {
     dt_ctx->Reset(reinterpret_cast<const char*>(fmt.ptr), fmt.len);
     if (!TimestampParser::ParseFormatTokens(dt_ctx)) {
       ReportBadFormat(context, fmt, false);
       return BigIntVal::null();
     }
  }

  TimestampValue tv = TimestampValue(
      reinterpret_cast<const char*>(string_val.ptr), string_val.len, *dt_ctx);
  if (!tv.HasDate()) return BigIntVal::null();
  return BigIntVal(tv.ToUnixTime());
}

BigIntVal TimestampFunctions::Unix(FunctionContext* context, const TimestampVal& ts_val) {
  if (ts_val.is_null) return BigIntVal::null();
  const TimestampValue& tv = TimestampValue::FromTimestampVal(ts_val);
  if (!tv.HasDate()) return BigIntVal::null();
  return BigIntVal(tv.ToUnixTime());
}

BigIntVal TimestampFunctions::Unix(FunctionContext* context) {
  if (!context->impl()->state()->now()->HasDate()) return BigIntVal::null();
  return BigIntVal(context->impl()->state()->now()->ToUnixTime());
}

BigIntVal TimestampFunctions::UnixFromString(FunctionContext* context,
    const StringVal& sv) {
  if (sv.is_null) return BigIntVal::null();
  TimestampValue tv(reinterpret_cast<const char *>(sv.ptr), sv.len);
  if (!tv.HasDate()) return BigIntVal::null();
  return BigIntVal(tv.ToUnixTime());
}

void TimestampFunctions::ReportBadFormat(FunctionContext* context,
    const StringVal& format, bool is_error) {
  stringstream ss;
  const StringValue& fmt = StringValue::FromStringVal(format);
  if (format.is_null || format.len == 0) {
    ss << "Bad date/time conversion format: format string is NULL or has 0 length";
  } else {
    ss << "Bad date/time conversion format: " << fmt.DebugString();
  }
  if (is_error) {
    context->SetError(ss.str().c_str());
  } else {
    context->AddWarning(ss.str().c_str());
  }
}

StringVal TimestampFunctions::DayName(FunctionContext* context, const TimestampVal& ts) {
  if (ts.is_null) return StringVal::null();
  IntVal dow = DayOfWeek(context, ts);
  switch(dow.val) {
    case 1: return StringVal(SUNDAY);
    case 2: return StringVal(MONDAY);
    case 3: return StringVal(TUESDAY);
    case 4: return StringVal(WEDNESDAY);
    case 5: return StringVal(THURSDAY);
    case 6: return StringVal(FRIDAY);
    case 7: return StringVal(SATURDAY);
    default: return StringVal::null();
   }
}

IntVal TimestampFunctions::Year(FunctionContext* context, const TimestampVal& ts_val) {
  if (ts_val.is_null) return IntVal::null();
  const TimestampValue& ts_value = TimestampValue::FromTimestampVal(ts_val);
  if (!ts_value.HasDate()) return IntVal::null();
  return IntVal(ts_value.date().year());
}


IntVal TimestampFunctions::Month(FunctionContext* context, const TimestampVal& ts_val) {
  if (ts_val.is_null) return IntVal::null();
  const TimestampValue& ts_value = TimestampValue::FromTimestampVal(ts_val);
  if (!ts_value.HasDate()) return IntVal::null();
  return IntVal(ts_value.date().month());
}


IntVal TimestampFunctions::DayOfWeek(FunctionContext* context,
    const TimestampVal& ts_val) {
  if (ts_val.is_null) return IntVal::null();
  const TimestampValue& ts_value = TimestampValue::FromTimestampVal(ts_val);
  if (!ts_value.HasDate()) return IntVal::null();
  // Sql has the result in [1,7] where 1 = Sunday. Boost has 0 = Sunday.
  return IntVal(ts_value.date().day_of_week() + 1);
}

IntVal TimestampFunctions::DayOfMonth(FunctionContext* context,
    const TimestampVal& ts_val) {
  if (ts_val.is_null) return IntVal::null();
  const TimestampValue& ts_value = TimestampValue::FromTimestampVal(ts_val);
  if (!ts_value.HasDate()) return IntVal::null();
  return IntVal(ts_value.date().day());
}

IntVal TimestampFunctions::DayOfYear(FunctionContext* context,
    const TimestampVal& ts_val) {
  if (ts_val.is_null) return IntVal::null();
  const TimestampValue& ts_value = TimestampValue::FromTimestampVal(ts_val);
  if (!ts_value.HasDate()) return IntVal::null();
  return IntVal(ts_value.date().day_of_year());
}

IntVal TimestampFunctions::WeekOfYear(FunctionContext* context,
    const TimestampVal& ts_val) {
  if (ts_val.is_null) return IntVal::null();
  const TimestampValue& ts_value = TimestampValue::FromTimestampVal(ts_val);
  if (!ts_value.HasDate()) return IntVal::null();
  return IntVal(ts_value.date().week_number());
}

IntVal TimestampFunctions::Hour(FunctionContext* context, const TimestampVal& ts_val) {
  if (ts_val.is_null) return IntVal::null();
  const TimestampValue& ts_value = TimestampValue::FromTimestampVal(ts_val);
  if (!ts_value.HasTime()) return IntVal::null();
  return IntVal(ts_value.time().hours());
}

IntVal TimestampFunctions::Minute(FunctionContext* context, const TimestampVal& ts_val) {
  if (ts_val.is_null) return IntVal::null();
  const TimestampValue& ts_value = TimestampValue::FromTimestampVal(ts_val);
  if (!ts_value.HasTime()) return IntVal::null();
  return IntVal(ts_value.time().minutes());
}

IntVal TimestampFunctions::Second(FunctionContext* context, const TimestampVal& ts_val) {
  if (ts_val.is_null) return IntVal::null();
  const TimestampValue& ts_value = TimestampValue::FromTimestampVal(ts_val);
  if (!ts_value.HasTime()) return IntVal::null();
  return IntVal(ts_value.time().seconds());
}

TimestampVal TimestampFunctions::Now(FunctionContext* context) {
  const TimestampValue* now = context->impl()->state()->now();
  if (!now->HasDateOrTime()) return TimestampVal::null();
  TimestampVal return_val;
  now->ToTimestampVal(&return_val);
  return return_val;
}

StringVal TimestampFunctions::ToDate(FunctionContext* context,
    const TimestampVal& ts_val) {
  if (ts_val.is_null) return StringVal::null();
  const TimestampValue ts_value = TimestampValue::FromTimestampVal(ts_val);
  string result = to_iso_extended_string(ts_value.date());
  return AnyValUtil::FromString(context, result);
}

template <bool ISADD, class VALTYPE, class UNIT>
TimestampVal TimestampFunctions::DateAddSub(FunctionContext* context,
    const TimestampVal& ts_val, const VALTYPE& count) {
  if (ts_val.is_null || count.is_null) return TimestampVal::null();
  const TimestampValue& ts_value = TimestampValue::FromTimestampVal(ts_val);

  if (!ts_value.HasDate()) return TimestampVal::null();
  UNIT unit(count.val);
  TimestampValue value;
  try {
    // Adding/subtracting boost::gregorian::dates can throw (via constructing a new date)
    value = TimestampValue(
        (ISADD ? ts_value.date() + unit : ts_value.date() - unit), ts_value.time());
  } catch (const std::exception& e) {
    context->AddWarning(Substitute("Cannot $0 date interval $1: $2",
        ISADD ? "add" : "subtract", count.val, e.what()).c_str());
    return TimestampVal::null();
  }
  TimestampVal return_val;
  value.ToTimestampVal(&return_val);
  return return_val;
}

template <bool ISADD, class VALTYPE, class UNIT>
TimestampVal TimestampFunctions::TimeAddSub(FunctionContext * context,
    const TimestampVal& ts_val, const VALTYPE& count) {
  if (ts_val.is_null || count.is_null) return TimestampVal::null();
  const TimestampValue& ts_value = TimestampValue::FromTimestampVal(ts_val);
  if (!ts_value.HasDate()) return TimestampVal::null();
  UNIT unit(count.val);
  ptime p(ts_value.date(), ts_value.time());
  TimestampValue value(ISADD ? p + unit : p - unit);
  TimestampVal return_val;
  value.ToTimestampVal(&return_val);
  return return_val;
}

IntVal TimestampFunctions::DateDiff(FunctionContext* context,
    const TimestampVal& ts_val1,
    const TimestampVal& ts_val2) {
  if (ts_val1.is_null || ts_val2.is_null) return IntVal::null();
  const TimestampValue& ts_value1 = TimestampValue::FromTimestampVal(ts_val1);
  const TimestampValue& ts_value2 = TimestampValue::FromTimestampVal(ts_val2);
  if (!ts_value1.HasDate() || !ts_value2.HasDate()) {
    return IntVal::null();
  }
  return IntVal((ts_value1.date() - ts_value2.date()).days());
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
  time_zone_ptr timezone =
      TimezoneDatabase::FindTimezone(tz_string_value.DebugString(), ts_value);
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
  time_zone_ptr timezone =
      TimezoneDatabase::FindTimezone(tz_string_value.DebugString(), ts_value);
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

TimezoneDatabase::TimezoneDatabase() {
  // Create a temporary file and write the timezone information.  The boost
  // interface only loads this format from a file.  We don't want to raise
  // an error here since this is done when the backend is created and this
  // information might not actually get used by any queries.
  char filestr[] = "/tmp/impala.tzdb.XXXXXXX";
  FILE* file;
  int fd;
  if ((fd = mkstemp(filestr)) == -1) {
    LOG(ERROR) << "Could not create temporary timezone file: " << filestr;
    return;
  }
  if ((file = fopen(filestr, "w")) == NULL) {
    unlink(filestr);
    close(fd);
    LOG(ERROR) << "Could not open temporary timezone file: " << filestr;
    return;
  }
  if (fputs(TIMEZONE_DATABASE_STR, file) == EOF) {
    unlink(filestr);
    close(fd);
    fclose(file);
    LOG(ERROR) << "Could not load temporary timezone file: " << filestr;
    return;
  }
  fclose(file);
  tz_database_.load_from_file(string(filestr));
  tz_region_list_ = tz_database_.region_list();
  unlink(filestr);
  close(fd);
}

TimezoneDatabase::~TimezoneDatabase() { }

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
    if (tzp->dst_zone_abbrev() == tz)
      return tzp;
    if (tzp->std_zone_abbrev() == tz)
      return tzp;
    if (tzp->dst_zone_name() == tz)
      return tzp;
    if (tzp->std_zone_name() == tz)
      return tzp;
  }
  return time_zone_ptr();
}

// Explicit template instantiation is required for proper linking. These functions
// are only indirectly called via a function pointer provided by the opcode registry
// which does not trigger implicit template instantiation.
// Must be kept in sync with common/function-registry/impala_functions.py.
template StringVal
TimestampFunctions::FromUnix<IntVal>(FunctionContext* context, const IntVal& intp, const
  StringVal& fmt);
template StringVal
TimestampFunctions::FromUnix<BigIntVal>(FunctionContext* context, const BigIntVal& intp,
    const StringVal& fmt);
template StringVal
TimestampFunctions::FromUnix<IntVal>(FunctionContext* context , const IntVal& intp);
template StringVal
TimestampFunctions::FromUnix<BigIntVal>(FunctionContext* context, const BigIntVal& intp);

template TimestampVal
TimestampFunctions::DateAddSub<true, IntVal, years>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::DateAddSub<true, BigIntVal, years>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::DateAddSub<false, IntVal, years>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::DateAddSub<false, BigIntVal, years>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::DateAddSub<true, IntVal, months>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::DateAddSub<true, BigIntVal, months>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::DateAddSub<false, IntVal, months>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::DateAddSub<false, BigIntVal, months>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::DateAddSub<true, IntVal, weeks>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::DateAddSub<true, BigIntVal, weeks>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::DateAddSub<false, IntVal, weeks>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::DateAddSub<false, BigIntVal, weeks>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::DateAddSub<true, IntVal, days>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::DateAddSub<true, BigIntVal, days>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::DateAddSub<false, IntVal, days>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::DateAddSub<false, BigIntVal, days>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);

template TimestampVal
TimestampFunctions::TimeAddSub<true, IntVal, hours>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::TimeAddSub<true, BigIntVal, hours>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::TimeAddSub<false, IntVal, hours>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::TimeAddSub<false, BigIntVal, hours>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::TimeAddSub<true, IntVal, minutes>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::TimeAddSub<true, BigIntVal, minutes>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::TimeAddSub<false, IntVal, minutes>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::TimeAddSub<false, BigIntVal, minutes>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::TimeAddSub<true, IntVal, seconds>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::TimeAddSub<true, BigIntVal, seconds>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::TimeAddSub<false, IntVal, seconds>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::TimeAddSub<false, BigIntVal, seconds>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::TimeAddSub<true, IntVal, milliseconds>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::TimeAddSub<true, BigIntVal, milliseconds>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::TimeAddSub<false, IntVal, milliseconds>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::TimeAddSub<false, BigIntVal, milliseconds>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::TimeAddSub<true, IntVal, microseconds>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::TimeAddSub<true, BigIntVal, microseconds>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::TimeAddSub<false, IntVal, microseconds>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::TimeAddSub<false, BigIntVal, microseconds>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::TimeAddSub<true, IntVal, nanoseconds>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::TimeAddSub<true, BigIntVal, nanoseconds>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
template TimestampVal
TimestampFunctions::TimeAddSub<false, IntVal, nanoseconds>(FunctionContext* context,
    const TimestampVal& ts_val, const IntVal& count);
template TimestampVal
TimestampFunctions::TimeAddSub<false, BigIntVal, nanoseconds>(FunctionContext* context,
    const TimestampVal& ts_val, const BigIntVal& count);
}
