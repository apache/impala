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

#include <sstream>

#include "exprs/in-predicate.h"

#include "common/names.h"

using namespace impala_udf;

namespace impala {

#define IN_FUNCTIONS(AnyValType, SetType, type_name) \
  BooleanVal InPredicate::InSetLookup( \
      FunctionContext* context, const AnyValType& val, int num_args, \
      const AnyValType* args) { \
    return TemplatedIn<AnyValType, SetType, false, SET_LOOKUP>( \
        context, val, num_args, args); \
  } \
\
  BooleanVal InPredicate::NotInSetLookup( \
      FunctionContext* context, const AnyValType& val, int num_args, \
      const AnyValType* args) { \
    return TemplatedIn<AnyValType, SetType, true, SET_LOOKUP>( \
        context, val, num_args, args); \
  } \
\
  BooleanVal InPredicate::InIterate( \
      FunctionContext* context, const AnyValType& val, int num_args, \
      const AnyValType* args) { \
    return TemplatedIn<AnyValType, SetType, false, ITERATE>( \
        context, val, num_args, args); \
  } \
\
  BooleanVal InPredicate::NotInIterate( \
      FunctionContext* context, const AnyValType& val, int num_args, \
      const AnyValType* args) { \
    return TemplatedIn<AnyValType, SetType, true, ITERATE>( \
        context, val, num_args, args); \
  } \
\
  void InPredicate::SetLookupPrepare_##type_name( \
      FunctionContext* ctx, FunctionContext::FunctionStateScope scope) { \
    SetLookupPrepare<AnyValType, SetType>(ctx, scope); \
  } \
\
  void InPredicate::SetLookupClose_##type_name( \
      FunctionContext* ctx, FunctionContext::FunctionStateScope scope) { \
    SetLookupClose<SetType>(ctx, scope); \
  }

IN_FUNCTIONS(BooleanVal, bool, boolean)
IN_FUNCTIONS(TinyIntVal, int8_t, tinyint)
IN_FUNCTIONS(SmallIntVal, int16_t, smallint)
IN_FUNCTIONS(IntVal, int32_t, int)
IN_FUNCTIONS(DateVal, int32_t, date)
IN_FUNCTIONS(BigIntVal, int64_t, bigint)
IN_FUNCTIONS(FloatVal, float, float)
IN_FUNCTIONS(DoubleVal, double, double)
IN_FUNCTIONS(StringVal, StringValue, string)
IN_FUNCTIONS(TimestampVal, TimestampValue, timestamp)
IN_FUNCTIONS(DecimalVal, Decimal16Value, decimal)

}
