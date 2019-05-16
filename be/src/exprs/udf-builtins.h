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

#ifndef IMPALA_EXPRS_UDF_BUILTINS_H
#define IMPALA_EXPRS_UDF_BUILTINS_H

#include "udf/udf.h"

namespace impala {

using impala_udf::FunctionContext;
using impala_udf::BooleanVal;
using impala_udf::TinyIntVal;
using impala_udf::SmallIntVal;
using impala_udf::IntVal;
using impala_udf::BigIntVal;
using impala_udf::FloatVal;
using impala_udf::DoubleVal;
using impala_udf::TimestampVal;
using impala_udf::StringVal;
using impala_udf::DecimalVal;
using impala_udf::DateVal;

/// Builtins written against the UDF interface. The builtins in the other files
/// should be replaced to the UDF interface as well.
/// This is just to illustrate how builtins against the UDF interface will be
/// implemented.
class UdfBuiltins {
 public:
  static DoubleVal Abs(FunctionContext* context, const DoubleVal& val);
  static DoubleVal Pi(FunctionContext* context);
  static StringVal Lower(FunctionContext* context, const StringVal& str);

  static IntVal MaxInt(FunctionContext* context);
  static TinyIntVal MaxTinyInt(FunctionContext* context);
  static SmallIntVal MaxSmallInt(FunctionContext* context);
  static BigIntVal MaxBigInt(FunctionContext* context);
  static IntVal MinInt(FunctionContext* context);
  static TinyIntVal MinTinyInt(FunctionContext* context);
  static SmallIntVal MinSmallInt(FunctionContext* context);
  static BigIntVal MinBigInt(FunctionContext* context);

  static BooleanVal IsNan(FunctionContext* context, const DoubleVal& val);
  static BooleanVal IsInf(FunctionContext* context, const DoubleVal& val);

  /// This is for TRUNC(TIMESTAMP, STRING) function.
  /// Rounds (truncating down) a Timestamp to the specified unit.
  ///    Units:
  ///    CC, SCC : One greater than the first two digits of
  ///               a four-digit year
  ///    SYYYY, YYYY, YEAR, SYEAR, YYY, YY, Y : Current Year
  ///    Q : Quarter
  ///    MONTH, MON, MM, RM : Month
  ///    DDD, DD, J : Day
  ///    DAY, DY, D : Starting day of the week
  ///    WW : Truncates to the most recent date, no later than 'tv', which is on the same
  ///         day of the week as the first day of year.
  ///    W : Truncates to the most recent date, no later than 'tv', which is on the same
  ///        day of the week as the first day of month.
  ///    HH, HH12, HH24 : Hour
  ///    MI : Minute
  ///
  ///    Reference:
  ///    http://docs.oracle.com/cd/B19306_01/server.102/b14200/functions201.htm
  static TimestampVal TruncForTimestamp(FunctionContext* ctx, const TimestampVal& tv,
      const StringVal &unit_str);
  static void TruncForTimestampPrepare(FunctionContext* ctx,
      FunctionContext::FunctionStateScope scope);
  static void TruncForTimestampClose(FunctionContext* ctx,
      FunctionContext::FunctionStateScope scope);

  /// This for TRUNC(DATE, STRING) function.
  /// Rounds (truncating down) a Date to the specified unit.
  /// Works as 'TruncForTimestamp' but doesn't accept time of day units: HH, HH12, HH24,
  /// MI.
  static DateVal TruncForDate(FunctionContext* ctx, const DateVal& dv,
      const StringVal &unit_str);
  static void TruncForDatePrepare(FunctionContext* ctx,
      FunctionContext::FunctionStateScope scope);
  static void TruncForDateClose(FunctionContext* ctx,
      FunctionContext::FunctionStateScope scope);

  /// This for DATE_TRUNC(STRING, TIMESTAMP) function.
  /// Rounds (truncating down) a Timestamp to the specified unit.
  ///    Units:
  ///    MILLENNIUM: The millennium number.
  ///    CENTURY: The century number.
  ///    DECADE: The year field divided by 10.
  ///    YEAR: The year field (1400 - 9999).
  ///    MONTH: The number of the month within the year (1–12)
  ///    WEEK: The number of the week of the year that the day is in.
  ///    DAY: The day (of the month) field (1–31).
  ///    HOUR: The hour field (0–23).
  ///    MINUTE: The minutes field (0–59).
  ///    SECOND: The seconds field (0–59).
  ///    MILLISECONDS: The milliseconds fraction in the seconds.
  ///    MICROSECONDS: The microseconds fraction in the seconds.
  ///
  ///    Reference:
  ///    https://my.vertica.com/docs/8.1.x/HTML/index.htm#Authoring/
  ///       SQLReferenceManual/Functions/Date-Time/DATE_TRUNC.htm
  static TimestampVal DateTruncForTimestamp(FunctionContext* ctx,
      const StringVal &unit_str, const TimestampVal& tv);
  static void DateTruncForTimestampPrepare(FunctionContext* ctx,
      FunctionContext::FunctionStateScope scope);
  static void DateTruncForTimestampClose(FunctionContext* ctx,
      FunctionContext::FunctionStateScope scope);

  /// This is for DATE_TRUNC(STRING, DATE) function.
  /// Rounds (truncating down) a Date to the specified unit.
  /// Works as 'DateTruncForTimestamp' but doesn't accept time of day units: HOUR, MINUTE,
  /// SECOND, MILLISECONDS, MICROSECONDS.
  static DateVal DateTruncForDate(FunctionContext* ctx, const StringVal &unit_str,
      const DateVal& dv);
  static void DateTruncForDatePrepare(FunctionContext* ctx,
      FunctionContext::FunctionStateScope scope);
  static void DateTruncForDateClose(FunctionContext* ctx,
      FunctionContext::FunctionStateScope scope);

  /// This is for the EXTRACT(TIMESTAMP, STRING) and EXTRACT(TIMEUNIT FROM TIMESTAMP)
  /// functions.
  /// Returns a single field from a timestamp
  ///    Fields:
  ///      YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, MILLISECOND, EPOCH
  /// Reference:
  /// http://docs.oracle.com/cd/B19306_01/server.102/b14200/functions050.htm
  static void ExtractForTimestampPrepare(FunctionContext* ctx,
      FunctionContext::FunctionStateScope scope);
  static BigIntVal ExtractForTimestamp(FunctionContext* ctx, const TimestampVal& tv,
      const StringVal& unit_str);
  static void ExtractForTimestampClose(FunctionContext* ctx,
      FunctionContext::FunctionStateScope scope);

  /// This is for the EXTRACT(DATE, STRING) and EXTRACT(TIMEUNIT FROM DATE)
  /// functions.
  /// Works as 'ExtractForTimestamp' but doesn't accept time of day fields: HOUR, MINUTE,
  /// SECOND, MILLISECOND, EPOCH.
  static void ExtractForDatePrepare(FunctionContext* ctx,
      FunctionContext::FunctionStateScope scope);
  static BigIntVal ExtractForDate(FunctionContext* ctx, const DateVal& dv,
      const StringVal& unit_str);
  static void ExtractForDateClose(FunctionContext* ctx,
      FunctionContext::FunctionStateScope scope);

  /// This is for DATE_PART(STRING, TIMESTAMP) function.
  /// Similar to 'ExtractForTimestamp' with the argument order reversed.
  static void DatePartForTimestampPrepare(FunctionContext* ctx,
      FunctionContext::FunctionStateScope scope);
  static BigIntVal DatePartForTimestamp(FunctionContext* ctx, const StringVal& unit_str,
      const TimestampVal& tv);
  static void DatePartForTimestampClose(FunctionContext* ctx,
      FunctionContext::FunctionStateScope scope);

  /// This is for DATE_PART(STRING, DATE) function.
  /// Similar to 'ExtractForDate' with the argument order reversed.
  static void DatePartForDatePrepare(FunctionContext* ctx,
      FunctionContext::FunctionStateScope scope);
  static BigIntVal DatePartForDate(FunctionContext* ctx, const StringVal& unit_str,
      const DateVal& dv);
  static void DatePartForDateClose(FunctionContext* ctx,
      FunctionContext::FunctionStateScope scope);

  /// Converts a set of doubles to double[] stored as a StringVal
  /// Stored as a StringVal with each double value encoded in IEEE back to back
  /// The returned StringVal ptr can be casted to a double*
  static StringVal ToVector(FunctionContext* context, int n, const DoubleVal* values);

  /// Converts a double[] stored as a StringVal into a human readable string
  static StringVal PrintVector(FunctionContext* context, const StringVal& arr);

  /// Returns the n-th (0-indexed) element of a double[] stored as a StringVal
  static DoubleVal VectorGet(FunctionContext* context, const BigIntVal& n,
      const StringVal& arr);

  /// Converts a double[] stored as a StringVal to an printable ascii encoding
  /// MADlib operates on binary strings but the Impala shell is not friendly to
  /// binary data. We offer these functions to the user to allow them to
  /// copy-paste vectors to/from the impala shell.
  /// Note: this loses precision (converts from double to float)
  static StringVal EncodeVector(FunctionContext* context, const StringVal& arr);

  /// Converts a printable ascii encoding of a vector to a double[] stored as a StringVal
  static StringVal DecodeVector(FunctionContext* context, const StringVal& arr);

 private:
  /// Implementation of TruncForTimestamp, not cross-compiled.
  static void TruncForTimestampPrepareImpl(FunctionContext* ctx,
      FunctionContext::FunctionStateScope scope);
  static TimestampVal TruncForTimestampImpl(FunctionContext* context,
      const TimestampVal& tv, const StringVal &unit_str);
  /// Implementation of TruncForDate, not cross-compiled.
  static void TruncForDatePrepareImpl(FunctionContext* ctx,
      FunctionContext::FunctionStateScope scope);
  static DateVal TruncForDateImpl(FunctionContext* context, const DateVal& dv,
      const StringVal &unit_str);

  /// Implementation of DateTruncForTimestamp, not cross-compiled.
  static void DateTruncForTimestampPrepareImpl(FunctionContext* ctx,
      FunctionContext::FunctionStateScope scope);
  static TimestampVal DateTruncForTimestampImpl(FunctionContext* context,
      const StringVal &unit_str, const TimestampVal& tv);
  /// Implementation of DateTruncForDate, not cross-compiled.
  static void DateTruncForDatePrepareImpl(FunctionContext* ctx,
      FunctionContext::FunctionStateScope scope);
  static DateVal DateTruncForDateImpl(FunctionContext* context, const StringVal &unit_str,
      const DateVal& dv);

  /// Implementation of ExtractForTimestamp, not cross-compiled.
  static void ExtractForTimestampPrepareImpl(FunctionContext* ctx,
      FunctionContext::FunctionStateScope scope);
  static BigIntVal ExtractForTimestampImpl(FunctionContext* context,
      const TimestampVal& tv, const StringVal& unit_str);
  /// Implementation of ExtractForDate, not cross-compiled.
  static void ExtractForDatePrepareImpl(FunctionContext* ctx,
      FunctionContext::FunctionStateScope scope);
  static BigIntVal ExtractForDateImpl(FunctionContext* context, const DateVal& dv,
      const StringVal& unit_st);

  /// Implementation of DatePartForTimestamp, not cross-compiled.
  static void DatePartForTimestampPrepareImpl(FunctionContext* ctx,
      FunctionContext::FunctionStateScope scope);
  static BigIntVal DatePartForTimestampImpl(FunctionContext* context,
      const StringVal& unit_str, const TimestampVal& tv);
  /// Implementation of DatePartForDate, not cross-compiled.
  static void DatePartForDatePrepareImpl(FunctionContext* ctx,
      FunctionContext::FunctionStateScope scope);
  static BigIntVal DatePartForDateImpl(FunctionContext* context,
      const StringVal& unit_str, const DateVal& dv);
};

} // namespace impala
#endif
