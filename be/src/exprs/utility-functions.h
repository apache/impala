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


#ifndef IMPALA_EXPRS_UTILITY_FUNCTIONS_H
#define IMPALA_EXPRS_UTILITY_FUNCTIONS_H

#include "udf/udf.h"

using namespace impala_udf;

namespace impala {

class Expr;
class OpcodeRegistry;
class TupleRow;

class UtilityFunctions {
 public:
  /// Implementations of the FnvHash function. Returns the Fowler-Noll-Vo hash of the
  /// input as an int64_t.
  template <typename T> static BigIntVal FnvHash(FunctionContext* ctx,
      const T& input_val);
  static BigIntVal FnvHashString(FunctionContext* ctx, const StringVal& input_val);
  static BigIntVal FnvHashTimestamp(FunctionContext* ctx, const TimestampVal& input_val);
  static BigIntVal FnvHashDecimal(FunctionContext* ctx, const DecimalVal& input_val);

  /// Implementation of the user() function. Returns the username of the user who executed
  /// this function.
  static StringVal User(FunctionContext* ctx);

  /// Implementation of the effective_user() builtin. Returns the username of the
  /// effective user for authorization purposes.
  static StringVal EffectiveUser(FunctionContext* ctx);

  /// Implementation of the version() function. Returns the version string.
  static StringVal Version(FunctionContext* ctx);

  /// Implementation of the pid() function. Returns the pid of the impalad that initiated
  /// this query.
  static IntVal Pid(FunctionContext* ctx);

  /// Testing function that sleeps for the specified number of milliseconds. Returns true.
  static BooleanVal Sleep(FunctionContext* ctx, const IntVal& milliseconds);

  /// Implementation of the current_database() function. Returns the current default
  /// database from the parent session of this query.
  static StringVal CurrentDatabase(FunctionContext* ctx);

  /// Implementation of the Uuid() function.
  static StringVal Uuid(FunctionContext* ctx);
  static void UuidPrepare(FunctionContext* ctx,
      FunctionContext::FunctionStateScope scope);
  static void UuidClose(FunctionContext* ctx,
      FunctionContext::FunctionStateScope scope);

  /// Implementation of the typeOf() function. Returns the type of the input
  /// expression. input_val is not used and it is kept here in order to let
  /// the compiler generate the corresponding fully-qualified function name.
  template <typename T> static StringVal TypeOf(FunctionContext* ctx, const T& input_val);
};

}

#endif
