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

// Compile ExprContext declaration to IR so we can use it in codegen'd functions
#include "exprs/expr-context.h"

// Dummy function to force compilation of UDF types.
// The arguments are pointers to prevent Clang from lowering the struct types
// (e.g. IntVal={bool, i32} can be coerced to i64).
void dummy(impala_udf::FunctionContext*, impala_udf::BooleanVal*, impala_udf::TinyIntVal*,
           impala_udf::SmallIntVal*, impala_udf::IntVal*, impala_udf::BigIntVal*,
           impala_udf::FloatVal*, impala_udf::DoubleVal*, impala_udf::StringVal*,
           impala_udf::TimestampVal*, impala_udf::DecimalVal*, ExprContext*) { }
#endif


// The following are compute functions that are cross-compiled to both native and IR
// libraries. In the interpreted path, these functions are executed as-is from the native
// code. In the codegen'd path, we load the IR functions and replace the Get*Val() calls
// with the appropriate child's codegen'd compute function.

using namespace impala;
using namespace impala_udf;
// Static wrappers around Get*Val() functions. We'd like to be able to call these from
// directly from native code as well as from generated IR functions.

BooleanVal Expr::GetBooleanVal(Expr* expr, ExprContext* context, TupleRow* row) {
  return expr->GetBooleanVal(context, row);
}
TinyIntVal Expr::GetTinyIntVal(Expr* expr, ExprContext* context, TupleRow* row) {
  return expr->GetTinyIntVal(context, row);
}
SmallIntVal Expr::GetSmallIntVal(Expr* expr, ExprContext* context, TupleRow* row) {
  return expr->GetSmallIntVal(context, row);
}
IntVal Expr::GetIntVal(Expr* expr, ExprContext* context, TupleRow* row) {
  return expr->GetIntVal(context, row);
}
BigIntVal Expr::GetBigIntVal(Expr* expr, ExprContext* context, TupleRow* row) {
  return expr->GetBigIntVal(context, row);
}
FloatVal Expr::GetFloatVal(Expr* expr, ExprContext* context, TupleRow* row) {
  return expr->GetFloatVal(context, row);
}
DoubleVal Expr::GetDoubleVal(Expr* expr, ExprContext* context, TupleRow* row) {
  return expr->GetDoubleVal(context, row);
}
StringVal Expr::GetStringVal(Expr* expr, ExprContext* context, TupleRow* row) {
  return expr->GetStringVal(context, row);
}
TimestampVal Expr::GetTimestampVal(Expr* expr, ExprContext* context, TupleRow* row) {
  return expr->GetTimestampVal(context, row);
}
DecimalVal Expr::GetDecimalVal(Expr* expr, ExprContext* context, TupleRow* row) {
  return expr->GetDecimalVal(context, row);
}
