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

#include "exprs/expr.h"
#include "udf/udf.h"

#ifdef IR_COMPILE

// Generate a llvm loadable function for calling GetValue on an Expr.  This is
// used as an adapter for Expr's that do not have an IR implementation.
extern "C"
void* IrExprGetValue(Expr* expr, TupleRow* row) {
  return expr->GetValue(row);
}

// Dummy function to force compilation of UDF types.
// The arguments are pointers to prevent Clang from lowering the struct types
// (e.g. IntVal={bool, i32} can be coerced to i64).
void dummy(impala_udf::FunctionContext*, impala_udf::BooleanVal*, impala_udf::TinyIntVal*,
           impala_udf::SmallIntVal*, impala_udf::IntVal*, impala_udf::BigIntVal*,
           impala_udf::FloatVal*, impala_udf::DoubleVal*, impala_udf::StringVal*,
           impala_udf::TimestampVal*) { }

#else
#error "This file should only be compiled by clang."
#endif

