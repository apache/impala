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

#include "exprs/is-null-predicate.h"
#include "udf/udf.h"

using namespace impala_udf;

namespace impala {

template<typename T>
BooleanVal IsNullPredicate::IsNull(FunctionContext* ctx, const T& val) {
  return val.is_null;
}

template<typename T>
BooleanVal IsNullPredicate::IsNotNull(FunctionContext* ctx, const T& val) {
  return !val.is_null;
}

template BooleanVal IsNullPredicate::IsNull(FunctionContext*, const BooleanVal&);
template BooleanVal IsNullPredicate::IsNull(FunctionContext*, const TinyIntVal&);
template BooleanVal IsNullPredicate::IsNull(FunctionContext*, const SmallIntVal&);
template BooleanVal IsNullPredicate::IsNull(FunctionContext*, const IntVal&);
template BooleanVal IsNullPredicate::IsNull(FunctionContext*, const BigIntVal&);
template BooleanVal IsNullPredicate::IsNull(FunctionContext*, const DateVal&);
template BooleanVal IsNullPredicate::IsNull(FunctionContext*, const FloatVal&);
template BooleanVal IsNullPredicate::IsNull(FunctionContext*, const DoubleVal&);
template BooleanVal IsNullPredicate::IsNull(FunctionContext*, const StringVal&);
template BooleanVal IsNullPredicate::IsNull(FunctionContext*, const TimestampVal&);
template BooleanVal IsNullPredicate::IsNull(FunctionContext*, const DecimalVal&);

template BooleanVal IsNullPredicate::IsNotNull(FunctionContext*, const BooleanVal&);
template BooleanVal IsNullPredicate::IsNotNull(FunctionContext*, const TinyIntVal&);
template BooleanVal IsNullPredicate::IsNotNull(FunctionContext*, const SmallIntVal&);
template BooleanVal IsNullPredicate::IsNotNull(FunctionContext*, const IntVal&);
template BooleanVal IsNullPredicate::IsNotNull(FunctionContext*, const BigIntVal&);
template BooleanVal IsNullPredicate::IsNotNull(FunctionContext*, const DateVal&);
template BooleanVal IsNullPredicate::IsNotNull(FunctionContext*, const FloatVal&);
template BooleanVal IsNullPredicate::IsNotNull(FunctionContext*, const DoubleVal&);
template BooleanVal IsNullPredicate::IsNotNull(FunctionContext*, const StringVal&);
template BooleanVal IsNullPredicate::IsNotNull(FunctionContext*, const TimestampVal&);
template BooleanVal IsNullPredicate::IsNotNull(FunctionContext*, const DecimalVal&);

}
