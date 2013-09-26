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

#include "exprs/udf-builtins.h"

#include <ctype.h>
#include <math.h>

using namespace std;

namespace impala {

DoubleVal UdfBuiltins::Abs(FunctionContext* context, const DoubleVal& v) {
  if (v.is_null) return v;
  return DoubleVal(fabs(v.val));
}

DoubleVal UdfBuiltins::Pi(FunctionContext* context) {
  return DoubleVal(M_PI);
}

StringVal UdfBuiltins::Lower(FunctionContext* context, const StringVal& v) {
  if (v.is_null) return v;
  StringVal result(context, v.len);
  for (int i = 0; i < v.len; ++i) {
    result.ptr[i] = tolower(v.ptr[i]);
  }
  return result;
}

}


