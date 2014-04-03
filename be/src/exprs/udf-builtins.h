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

#ifndef IMPALA_EXPRS_UDF_BUILTINS_H
#define IMPALA_EXPRS_UDF_BUILTINS_H

#include "udf/udf.h"

using namespace impala_udf;

namespace impala {

// Builtins written against the UDF interface. The builtins in the other files
// should be replaced to the UDF interface as well.
// This is just to illustrate how builtins against the UDF interface will be
// implemented.
class UdfBuiltins {
 public:
  static DoubleVal Abs(FunctionContext* context, const DoubleVal& val);
  static DoubleVal Pi(FunctionContext* context);
  static StringVal Lower(FunctionContext* context, const StringVal& str);

  // Rounds (truncating down) a Timestamp to the specified unit.
  //    Units:
  //    CC, SCC : One greater than the first two digits of
  //               a four-digit year
  //    SYYYY, YYYY, YEAR, SYEAR, YYY, YY, Y : Current Year
  //    Q : Quarter
  //    MONTH, MON, MM, RM : Month
  //    WW : Same day of the week as the first day of the year
  //    W : Same day of the week as the first day of the month
  //    DDD, DD, J : Day
  //    DAY, DY, D : Starting day of the week
  //    HH, HH12, HH24 : Hour
  //    MI : Minute
  //
  //    Reference:
  //    http://docs.oracle.com/cd/B19306_01/server.102/b14200/functions201.htm
  static TimestampVal Trunc(FunctionContext* context, const TimestampVal& date,
                            const StringVal& unit_str);
  static void TruncPrepare(FunctionContext* context,
                           FunctionContext::FunctionStateScope scope);
  static void TruncClose(FunctionContext* context,
                         FunctionContext::FunctionStateScope scope);
};

} // namespace impala
#endif
