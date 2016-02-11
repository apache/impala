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

BooleanVal NullLiteral::GetBooleanVal(ExprContext* context, TupleRow* row) {
  DCHECK_EQ(type_.type, TYPE_BOOLEAN) << type_;
  return BooleanVal::null();
}

TinyIntVal NullLiteral::GetTinyIntVal(ExprContext* context, TupleRow* row) {
  DCHECK_EQ(type_.type, TYPE_TINYINT) << type_;
  return TinyIntVal::null();
}

SmallIntVal NullLiteral::GetSmallIntVal(ExprContext* context, TupleRow* row) {
  DCHECK_EQ(type_.type, TYPE_SMALLINT) << type_;
  return SmallIntVal::null();
}

IntVal NullLiteral::GetIntVal(ExprContext* context, TupleRow* row) {
  DCHECK_EQ(type_.type, TYPE_INT) << type_;
  return IntVal::null();
}

BigIntVal NullLiteral::GetBigIntVal(ExprContext* context, TupleRow* row) {
  DCHECK_EQ(type_.type, TYPE_BIGINT) << type_;
  return BigIntVal::null();
}

FloatVal NullLiteral::GetFloatVal(ExprContext* context, TupleRow* row) {
  DCHECK_EQ(type_.type, TYPE_FLOAT) << type_;
  return FloatVal::null();
}

DoubleVal NullLiteral::GetDoubleVal(ExprContext* context, TupleRow* row) {
  DCHECK_EQ(type_.type, TYPE_DOUBLE) << type_;
  return DoubleVal::null();
}

StringVal NullLiteral::GetStringVal(ExprContext* context, TupleRow* row) {
  DCHECK(type_.IsStringType()) << type_;
  return StringVal::null();
}

TimestampVal NullLiteral::GetTimestampVal(ExprContext* context, TupleRow* row) {
  DCHECK_EQ(type_.type, TYPE_TIMESTAMP) << type_;
  return TimestampVal::null();
}

DecimalVal NullLiteral::GetDecimalVal(ExprContext* context, TupleRow* row) {
  DCHECK_EQ(type_.type, TYPE_DECIMAL) << type_;
  return DecimalVal::null();
}

CollectionVal NullLiteral::GetCollectionVal(ExprContext* context, TupleRow* row) {
  DCHECK(type_.IsCollectionType());
  return CollectionVal::null();
}

// Generated IR for a bigint NULL literal:
//
// define { i8, i64 } @NullLiteral(i8* %context, %"class.impala::TupleRow"* %row) {
// entry:
//   ret { i8, i64 } { i8 1, i64 0 }
// }
Status NullLiteral::GetCodegendComputeFn(RuntimeState* state, llvm::Function** fn) {
  if (ir_compute_fn_ != NULL) {
    *fn = ir_compute_fn_;
    return Status::OK();
  }

  DCHECK_EQ(GetNumChildren(), 0);
  LlvmCodeGen* codegen;
  RETURN_IF_ERROR(state->GetCodegen(&codegen));
  Value* args[2];
  *fn = CreateIrFunctionPrototype(codegen, "NullLiteral", &args);
  BasicBlock* entry_block = BasicBlock::Create(codegen->context(), "entry", *fn);
  LlvmCodeGen::LlvmBuilder builder(entry_block);

  Value* v = CodegenAnyVal::GetNullVal(codegen, type());
  builder.CreateRet(v);
  *fn = codegen->FinalizeFunction(*fn);
  ir_compute_fn_ = *fn;
  return Status::OK();
}

string NullLiteral::DebugString() const {
  stringstream out;
  out << "NullLiteral(" << Expr::DebugString() << ")";
  return out.str();
}

}
