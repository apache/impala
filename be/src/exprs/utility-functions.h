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


#ifndef IMPALA_EXPRS_UTILITY_FUNCTIONS_H
#define IMPALA_EXPRS_UTILITY_FUNCTIONS_H

#include "udf/udf.h"

namespace impala {

using impala_udf::FunctionContext;
using impala_udf::AnyVal;
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

  /// Implementations of the MurmurHash function. Returns the Murmur hash of the
  /// input as an int64_t.
  template <typename T> static BigIntVal MurmurHash(FunctionContext* ctx,
      const T& input_val);
  static BigIntVal MurmurHashString(FunctionContext* ctx, const StringVal& input_val);
  static BigIntVal MurmurHashTimestamp(FunctionContext* ctx, const TimestampVal& input_val);
  static BigIntVal MurmurHashDecimal(FunctionContext* ctx, const DecimalVal& input_val);

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

  /// Implementation of the current_session() function. Returns the ID of the
  /// parent session of this query.
  static StringVal CurrentSession(FunctionContext* ctx);

  /// Implementation of the Uuid() function.
  static StringVal Uuid(FunctionContext* ctx);
  static void UuidPrepare(FunctionContext* ctx,
      FunctionContext::FunctionStateScope scope);
  static void UuidClose(FunctionContext* ctx,
      FunctionContext::FunctionStateScope scope);

  /// Implementation of the coordinator() function.
  /// Returns the name of the host where the coordinator is running.
  static StringVal Coordinator(FunctionContext* ctx);

  /// Implementation of the typeOf() function. Returns the type of the input
  /// expression. input_val is not used and it is kept here in order to let
  /// the compiler generate the corresponding fully-qualified function name.
  template <typename T> static StringVal TypeOf(FunctionContext* ctx, const T& input_val);

  /// Implementation of sha1 function. Returns the SHA-1 digest for a given string.
  static StringVal Sha1(FunctionContext* ctx, const StringVal& input_str);

  /// Implementation of sha2 function. Returns the SHA-2 family of hash
  /// functions (SHA-224, SHA-256, SHA-384, and SHA-512) for a given string.
  /// 'bit_len' specifies the length of bit. Allowed values are: 224, 256, 384, 512.
  static StringVal Sha2(FunctionContext* ctx, const StringVal& input_str,
      const IntVal& bit_len);

  /// Implementation of MD5 function. Returns the 128-bit MD5 checksum for a given
  /// string.
  static StringVal Md5(FunctionContext* ctx, const StringVal& input_str);

  /// Implementation of the typeOf() for BINARY type, which is generally not
  /// differentiated from STRING by the backend.
  static StringVal TypeOfBinary(FunctionContext* ctx, const StringVal& input_val);

 private:
  static StringVal GenUuid(FunctionContext* ctx);
};

}

#endif
