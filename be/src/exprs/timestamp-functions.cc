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

#include "cctz/time_zone.h"
#include "exprs/anyval-util.h"
#include "exprs/timezone_db.h"
#include "gutil/strings/substitute.h"
#include "runtime/datetime-parse-util.h"
#include "runtime/string-value.inline.h"
#include "runtime/timestamp-value.h"
#include "runtime/timestamp-value.inline.h"
#include "udf/udf-internal.h"
#include "udf/udf.h"

#include "common/names.h"

namespace impala {

using datetime_parse_util::DateTimeFormatContext;
using datetime_parse_util::ParseFormatTokens;

const string TimestampFunctions::DAY_ARRAY[7] = {"Sun", "Mon", "Tue", "Wed", "Thu",
    "Fri", "Sat"};
const string TimestampFunctions::DAYNAME_ARRAY[7] = {"Sunday", "Monday", "Tuesday",
    "Wednesday", "Thursday", "Friday", "Saturday"};
const string TimestampFunctions::MONTH_ARRAY[12] = {"Jan", "Feb", "Mar", "Apr", "May",
    "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
const string TimestampFunctions::MONTHNAME_ARRAY[12] = {"January", "February", "March",
    "April", "May", "June", "July", "August", "September", "October", "November",
    "December"};

// Sunday is mapped to 0 and Saturday is mapped to 6.
const map<string, int> TimestampFunctions::DAYNAME_MAP = {
    {"sun", 0}, {"sunday", 0},
    {"mon", 1}, {"monday", 1},
    {"tue", 2}, {"tuesday", 2},
    {"wed", 3}, {"wednesday", 3},
    {"thu", 4}, {"thursday", 4},
    {"fri", 5}, {"friday", 5},
    {"sat", 6}, {"saturday", 6},
};

TimestampVal TimestampFunctions::FromUtc(FunctionContext* context,
    const TimestampVal& ts_val, const StringVal& tz_string_val) {
  if (ts_val.is_null || tz_string_val.is_null) return TimestampVal::null();
  const TimestampValue ts_value = TimestampValue::FromTimestampVal(ts_val);
  if (UNLIKELY(!ts_value.HasDateAndTime())) return TimestampVal::null();

  const StringValue& tz_string_value = StringValue::FromStringVal(tz_string_val);
  const Timezone* timezone = TimezoneDatabase::FindTimezone(
      string(tz_string_value.ptr, tz_string_value.len));
  if (UNLIKELY(timezone == nullptr)) {
    // Although this is an error, Hive ignores it. We will issue a warning but otherwise
    // ignore the error too.
    stringstream ss;
    ss << "Unknown timezone '" << tz_string_value << "'" << endl;
    context->AddWarning(ss.str().c_str());
    return ts_val;
  }

  TimestampValue ts_value_ret = ts_value;
  ts_value_ret.UtcToLocal(*timezone);
  if (UNLIKELY(!ts_value_ret.HasDateAndTime())) {
    const string msg = Substitute(
        "Timestamp '$0' did not convert to a valid local time in timezone '$1'",
        ts_value.ToString(), tz_string_value.DebugString());
    context->AddWarning(msg.c_str());
    return TimestampVal::null();
  }

  TimestampVal ts_val_ret;
  ts_value_ret.ToTimestampVal(&ts_val_ret);
  return ts_val_ret;
}

TimestampVal TimestampFunctions::ToUtc(FunctionContext* context,
    const TimestampVal& ts_val, const StringVal& tz_string_val) {
  if (ts_val.is_null || tz_string_val.is_null) return TimestampVal::null();
  const TimestampValue& ts_value = TimestampValue::FromTimestampVal(ts_val);
  if (!ts_value.HasDateAndTime()) return TimestampVal::null();

  const StringValue& tz_string_value = StringValue::FromStringVal(tz_string_val);
  const Timezone* timezone = TimezoneDatabase::FindTimezone(
      string(tz_string_value.ptr, tz_string_value.len));
  if (UNLIKELY(timezone == nullptr)) {
    // Although this is an error, Hive ignores it. We will issue a warning but otherwise
    // ignore the error too.
    stringstream ss;
    ss << "Unknown timezone '" << tz_string_value << "'" << endl;
    context->AddWarning(ss.str().c_str());
    return ts_val;
  }

  TimestampValue ts_value_ret = ts_value;
  ts_value_ret.LocalToUtc(*timezone);
  if (UNLIKELY(!ts_value_ret.HasDateAndTime())) {
    const string& msg =
        Substitute("Timestamp '$0' in timezone '$1' could not be converted to UTC",
            ts_value.ToString(), tz_string_value.DebugString());
    context->AddWarning(msg.c_str());
    return TimestampVal::null();
  }

  TimestampVal ts_val_ret;
  ts_value_ret.ToTimestampVal(&ts_val_ret);
  return ts_val_ret;
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
    bool parse_result = ParseFormatTokens(dt_ctx);
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

}
