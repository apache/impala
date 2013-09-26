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
  static DoubleVal Abs(FunctionContext* context, const DoubleVal&);
  static DoubleVal Pi(FunctionContext* context);

  static StringVal Lower(FunctionContext* context, const StringVal&);
};

}

#endif
