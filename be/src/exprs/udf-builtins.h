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

  /// Rounds (truncating down) a Timestamp to the specified unit.
  ///    Units:
  ///    CC, SCC : One greater than the first two digits of
  ///               a four-digit year
  ///    SYYYY, YYYY, YEAR, SYEAR, YYY, YY, Y : Current Year
  ///    Q : Quarter
  ///    MONTH, MON, MM, RM : Month
  ///    WW : Same day of the week as the first day of the year
  ///    W : Same day of the week as the first day of the month
  ///    DDD, DD, J : Day
  ///    DAY, DY, D : Starting day of the week
  ///    HH, HH12, HH24 : Hour
  ///    MI : Minute
  ///
  ///    Reference:
  ///    http://docs.oracle.com/cd/B19306_01/server.102/b14200/functions201.htm
  static TimestampVal Trunc(FunctionContext* context, const TimestampVal& date,
      const StringVal& unit_str);
  /// Implementation of Trunc, not cross-compiled.
  static TimestampVal TruncImpl(FunctionContext* context, const TimestampVal& date,
      const StringVal& unit_str);
  static void TruncPrepare(FunctionContext* context,
      FunctionContext::FunctionStateScope scope);
  static void TruncClose(
      FunctionContext* context, FunctionContext::FunctionStateScope scope);

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

  ///    Reference:
  ///    https://my.vertica.com/docs/8.1.x/HTML/index.htm#Authoring/
  ///       SQLReferenceManual/Functions/Date-Time/DATE_TRUNC.htm
  static TimestampVal DateTrunc(
      FunctionContext* context, const StringVal& unit_str, const TimestampVal& date);
  /// Implementation of DateTrunc, not cross-compiled.
  static TimestampVal DateTruncImpl(
      FunctionContext* context, const TimestampVal& date, const StringVal& unit_str);
  static void DateTruncPrepare(
      FunctionContext* context, FunctionContext::FunctionStateScope scope);
  static void DateTruncClose(
      FunctionContext* context, FunctionContext::FunctionStateScope scope);

  /// Returns a single field from a timestamp
  ///    Fields:
  ///      YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, MILLISECOND, EPOCH
  ///    Reference:
  ///    http://docs.oracle.com/cd/B19306_01/server.102/b14200/functions050.htm
  ///
  /// This is used by the DATE_PART function.
  static BigIntVal Extract(FunctionContext* context, const StringVal& field_str,
      const TimestampVal& date);

  /// This is for the EXTRACT(Timestamp, String) and EXTRACT(Timeunit FROM
  /// Timestamp) functions.
  static BigIntVal Extract(FunctionContext* context, const TimestampVal& date,
      const StringVal& field_str);
  /// This is used by the DATE_PART function.
  static void ExtractPrepare(FunctionContext* context,
      FunctionContext::FunctionStateScope scope);

  /// This is for the EXTRACT(Timestamp, String) and EXTRACT(Timeunit FROM
  /// Timestamp) functions.
  static void SwappedExtractPrepare(FunctionContext* context,
      FunctionContext::FunctionStateScope scope);
  /// This is used by both EXTRACT and DATE_PART
  static void ExtractClose(FunctionContext* context,
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
  /// Does the preparation for EXTRACT. The unit_idx parameter should indicate which
  /// parameter of the EXTRACT call is the time unit param. DATE_PART will also use this
  /// with a different unit_idx than EXTRACT.
  static void ExtractPrepare(FunctionContext* context,
      FunctionContext::FunctionStateScope scope, int unit_idx);
};

} // namespace impala
#endif
