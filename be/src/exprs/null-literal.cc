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

#include "null-literal.h"

#include "codegen/codegen-anyval.h"
#include "codegen/llvm-codegen.h"
#include "runtime/runtime-state.h"
#include "udf/udf.h"
#include "gen-cpp/Exprs_types.h"

#include "common/names.h"

using namespace impala_udf;
using namespace llvm;

namespace impala {

BooleanVal NullLiteral::GetBooleanVal(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK_EQ(type_.type, TYPE_BOOLEAN) << type_;
  return BooleanVal::null();
}

TinyIntVal NullLiteral::GetTinyIntVal(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK_EQ(type_.type, TYPE_TINYINT) << type_;
  return TinyIntVal::null();
}

SmallIntVal NullLiteral::GetSmallIntVal(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK_EQ(type_.type, TYPE_SMALLINT) << type_;
  return SmallIntVal::null();
}

IntVal NullLiteral::GetIntVal(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK_EQ(type_.type, TYPE_INT) << type_;
  return IntVal::null();
}

BigIntVal NullLiteral::GetBigIntVal(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK_EQ(type_.type, TYPE_BIGINT) << type_;
  return BigIntVal::null();
}

FloatVal NullLiteral::GetFloatVal(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK_EQ(type_.type, TYPE_FLOAT) << type_;
  return FloatVal::null();
}

DoubleVal NullLiteral::GetDoubleVal(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK_EQ(type_.type, TYPE_DOUBLE) << type_;
  return DoubleVal::null();
}

StringVal NullLiteral::GetStringVal(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK(type_.IsStringType()) << type_;
  return StringVal::null();
}

TimestampVal NullLiteral::GetTimestampVal(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK_EQ(type_.type, TYPE_TIMESTAMP) << type_;
  return TimestampVal::null();
}

DecimalVal NullLiteral::GetDecimalVal(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK_EQ(type_.type, TYPE_DECIMAL) << type_;
  return DecimalVal::null();
}

CollectionVal NullLiteral::GetCollectionVal(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK(type_.IsCollectionType());
  return CollectionVal::null();
}

// Generated IR for a bigint NULL literal:
//
// define { i8, i64 } @NullLiteral(
//     %"class.impala::ScalarExprEvaluator"* %eval, %"class.impala::TupleRow"* %row) {
// entry:
//   ret { i8, i64 } { i8 1, i64 0 }
// }
Status NullLiteral::GetCodegendComputeFn(LlvmCodeGen* codegen, llvm::Function** fn) {
  if (ir_compute_fn_ != nullptr) {
    *fn = ir_compute_fn_;
    return Status::OK();
  }

  DCHECK_EQ(GetNumChildren(), 0);
  Value* args[2];
  *fn = CreateIrFunctionPrototype("NullLiteral", codegen, &args);
  BasicBlock* entry_block = BasicBlock::Create(codegen->context(), "entry", *fn);
  LlvmBuilder builder(entry_block);

  Value* v = CodegenAnyVal::GetNullVal(codegen, type());
  builder.CreateRet(v);
  *fn = codegen->FinalizeFunction(*fn);
  if (UNLIKELY(*fn == nullptr)) {
    return Status(TErrorCode::IR_VERIFY_FAILED, "NullLiteral");
  }
  ir_compute_fn_ = *fn;
  return Status::OK();
}

string NullLiteral::DebugString() const {
  stringstream out;
  out << "NullLiteral(" << ScalarExpr::DebugString() << ")";
  return out.str();
}

}
