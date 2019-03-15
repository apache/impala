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

namespace impala {

const char* NullLiteral::LLVM_CLASS_NAME = "class.impala::NullLiteral";

BooleanVal NullLiteral::GetBooleanValInterpreted(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK_EQ(type_.type, TYPE_BOOLEAN) << type_;
  return BooleanVal::null();
}

TinyIntVal NullLiteral::GetTinyIntValInterpreted(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK_EQ(type_.type, TYPE_TINYINT) << type_;
  return TinyIntVal::null();
}

SmallIntVal NullLiteral::GetSmallIntValInterpreted(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK_EQ(type_.type, TYPE_SMALLINT) << type_;
  return SmallIntVal::null();
}

IntVal NullLiteral::GetIntValInterpreted(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK_EQ(type_.type, TYPE_INT) << type_;
  return IntVal::null();
}

BigIntVal NullLiteral::GetBigIntValInterpreted(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK_EQ(type_.type, TYPE_BIGINT) << type_;
  return BigIntVal::null();
}

FloatVal NullLiteral::GetFloatValInterpreted(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK_EQ(type_.type, TYPE_FLOAT) << type_;
  return FloatVal::null();
}

DoubleVal NullLiteral::GetDoubleValInterpreted(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK_EQ(type_.type, TYPE_DOUBLE) << type_;
  return DoubleVal::null();
}

StringVal NullLiteral::GetStringValInterpreted(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK(type_.IsStringType()) << type_;
  return StringVal::null();
}

TimestampVal NullLiteral::GetTimestampValInterpreted(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK_EQ(type_.type, TYPE_TIMESTAMP) << type_;
  return TimestampVal::null();
}

DecimalVal NullLiteral::GetDecimalValInterpreted(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK_EQ(type_.type, TYPE_DECIMAL) << type_;
  return DecimalVal::null();
}

DateVal NullLiteral::GetDateValInterpreted(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK_EQ(type_.type, TYPE_DATE) << type_;
  return DateVal::null();
}

CollectionVal NullLiteral::GetCollectionValInterpreted(
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
Status NullLiteral::GetCodegendComputeFnImpl(LlvmCodeGen* codegen, llvm::Function** fn) {
  DCHECK_EQ(GetNumChildren(), 0);
  llvm::Value* args[2];
  *fn = CreateIrFunctionPrototype("NullLiteral", codegen, &args);
  llvm::BasicBlock* entry_block =
      llvm::BasicBlock::Create(codegen->context(), "entry", *fn);
  LlvmBuilder builder(entry_block);

  llvm::Value* v = CodegenAnyVal::GetNullVal(codegen, type());
  builder.CreateRet(v);
  *fn = codegen->FinalizeFunction(*fn);
  if (UNLIKELY(*fn == nullptr)) {
    return Status(TErrorCode::IR_VERIFY_FAILED, "NullLiteral");
  }
  return Status::OK();
}

string NullLiteral::DebugString() const {
  stringstream out;
  out << "NullLiteral(" << ScalarExpr::DebugString() << ")";
  return out.str();
}

}
