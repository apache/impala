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

#ifdef IR_COMPILE
#include "runtime/string-value.inline.h"
#include "runtime/timestamp-value.h"
#include "udf/udf.h"

using namespace impala;
using namespace impala_udf;

// Note: we explicitly pass by reference because passing by value has special ABI rules

// Used by CodegenAnyVal::Eq()

bool StringValEq(const StringVal& x, const StringVal& y) {
  return x == y;
}

bool TimestampValEq(const TimestampVal& x, const TimestampVal& y) {
  return x == y;
}

// Used by CodegenAnyVal::EqToNativePtr()

bool StringValueEq(const StringVal& x, const StringValue& y) {
  StringValue sv = StringValue::FromStringVal(x);
  return sv.Eq(y);
}

bool TimestampValueEq(const TimestampVal& x, const TimestampValue& y) {
  TimestampValue tv = TimestampValue::FromTimestampVal(x);
  return tv == y;
}

#else
#error "This file should only be used for cross compiling to IR."
#endif

