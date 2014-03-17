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

#include "testutil/test-udas.h"

#include <udf/udf.h>

using namespace impala_udf;

void TwoArgInit(FunctionContext*, IntVal*) {}
void TwoArgUpdate(FunctionContext*, const IntVal&, const StringVal&, IntVal*) {}
void TwoArgMerge(FunctionContext*, const IntVal&, IntVal*) {}

void VarArgInit(FunctionContext*, IntVal*) {}
void VarArgUpdate(FunctionContext*, const DoubleVal&, int, const StringVal*, IntVal*) {}
void VarArgMerge(FunctionContext*, const IntVal&, IntVal*) {}

// Defines Agg(<some args>) returns int
void AggUpdate(FunctionContext*, const IntVal&, IntVal*) {}
void AggUpdate(FunctionContext*, const IntVal&, const IntVal&, IntVal*) {}
// Update function intentionally not called *Update for FE testing.
void AggFn(FunctionContext*, const IntVal&, IntVal*) {}
void AggInit(FunctionContext*, IntVal*) {}
void AggMerge(FunctionContext*, const IntVal&, IntVal*) {}
IntVal AggSerialize(FunctionContext*, const IntVal& i) { return i; }
IntVal AggFinalize(FunctionContext*, const IntVal&i) { return i; }


// Defines Agg(<some args>) returns string intermediate string
void AggUpdate(FunctionContext*, const StringVal&, const DoubleVal&, StringVal*) {}
void Agg2Update(FunctionContext*, const StringVal&, const DoubleVal&, StringVal*) {}
void Agg(FunctionContext*, const StringVal&, const DoubleVal&, StringVal*) {}

void AggInit(FunctionContext*, StringVal*){}
void AggMerge(FunctionContext*, const StringVal&, StringVal*) {}
StringVal AggSerialize(FunctionContext*, const StringVal& v) { return v;}
StringVal AggFinalize(FunctionContext*, const StringVal& v) { return v;}


// Defines Agg(int) returns BIGINT intermediate CHAR(10)
void Agg(FunctionContext*, const IntVal&, BufferVal*) {}
void AggUpdate(FunctionContext*, const IntVal&, BufferVal*) {}
void AggInit(FunctionContext*, BufferVal*) {}
void AggMerge(FunctionContext*, const BufferVal&, BufferVal*) {}
BigIntVal AggFinalize(FunctionContext*, const BufferVal&) {
  return BigIntVal::null();
}

// Defines MemTest(bigint) return bigint
// "Allocates" the specified number of bytes in the update function and frees them in the
// serialize function. Useful for testing mem limits.
void MemTestInit(FunctionContext*, BigIntVal* total) {
  *total = BigIntVal(0);
}

void MemTestUpdate(FunctionContext* context, const BigIntVal& bytes, BigIntVal* total) {
  if (bytes.is_null) return;
  context->TrackAllocation(bytes.val); // freed by serialize()
  total->val += bytes.val;
}

void MemTestMerge(FunctionContext* context, const BigIntVal& src, BigIntVal* dst) {
  if (src.is_null) return;
  context->TrackAllocation(src.val); // freed by finalize()
  if (dst->is_null) {
    *dst = src;
    return;
  }
  dst->val += src.val;
}

const BigIntVal MemTestSerialize(FunctionContext* context, const BigIntVal& total) {
  if (total.is_null) return BigIntVal(0);
  context->Free(total.val);
  return total;
}

BigIntVal MemTestFinalize(FunctionContext* context, const BigIntVal& total) {
  if (total.is_null) return BigIntVal(0);
  context->Free(total.val);
  return total;
}
