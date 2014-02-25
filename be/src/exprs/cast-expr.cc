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

#include <sstream>

#include "codegen/llvm-codegen.h"
#include "exprs/cast-expr.h"
#include "gen-cpp/Exprs_types.h"

using namespace llvm;
using namespace std;

namespace impala {

CastExpr::CastExpr(const TExprNode& node)
  : Expr(node) {
}

Status CastExpr::Prepare(RuntimeState* state, const RowDescriptor& desc) {
  DCHECK_EQ(children_.size(), 1);
  return Expr::Prepare(state, desc);
}

string CastExpr::DebugString() const {
  stringstream out;
  out << "CastExpr(" << Expr::DebugString() << ")";
  return out.str();
}

bool CastExpr::IsJittable(LlvmCodeGen* codegen) const {
  // TODO: casts to and from StringValue are not yet done.
  if (type().type == TYPE_STRING || children()[0]->type().type == TYPE_STRING) {
    return false;
  }
  return Expr::IsJittable(codegen);
}

// IR Generation for Cast Exprs.  For a cast from long to double, the IR
// looks like:
//
// define double @CastExpr(i8** %row, i8* %state_data, i1* %is_null) {
// entry:
//   %child_result = call i64 @ArithmeticExpr(i8** %row, i8* %state_data, i1* %is_null)
//   %child_null = load i1* %is_null
//   br i1 %child_null, label %ret_block, label %child_not_null
//
// child_not_null:                                   ; preds = %entry
//   %tmp_cast = sitofp i64 %child_result to double
//   br label %ret_block
//
// ret_block:                                        ; preds = %child_not_null, %entry
//   %tmp_phi = phi double [ 0.000000e+00, %entry ], [ %tmp_cast, %child_not_null ]
//   ret double %tmp_phi
// }
Function* CastExpr::Codegen(LlvmCodeGen* codegen) {
  DCHECK_EQ(GetNumChildren(), 1);
  DCHECK_EQ(fn_.arg_types.size(), 2);
  Function* child_function = children()[0]->Codegen(codegen);
  if (child_function == NULL) return NULL;

  const ColumnType& ct1 = ThriftToType(fn_.arg_types[0].type);
  const ColumnType& ct2 = ThriftToType(fn_.arg_types[1].type);

  PrimitiveType t1 = ct1.type;
  PrimitiveType t2 = ct2.type;

  // No op cast.  Just return the child function.
  if (t1 == t2) {
    codegen_fn_ = child_function;
    scratch_buffer_size_ = children()[0]->scratch_buffer_size();
    return codegen_fn_;
  }

  LLVMContext& context = codegen->context();
  LlvmCodeGen::LlvmBuilder builder(context);
  Type* return_type = codegen->GetType(type());
  Function* function = CreateComputeFnPrototype(codegen, "CastExpr");

  BasicBlock* entry_block = BasicBlock::Create(context, "entry", function);
  BasicBlock* child_not_null_block =
    BasicBlock::Create(context, "child_not_null", function);
  BasicBlock* ret_block = BasicBlock::Create(context, "ret_block", function);

  builder.SetInsertPoint(entry_block);
  // Call child function
  Value* child_value = children()[0]->CodegenGetValue(codegen, entry_block,
      ret_block, child_not_null_block);

  // Do the cast
  builder.SetInsertPoint(child_not_null_block);
  Value* result = NULL;
  switch (t1) {
    case TYPE_BOOLEAN:
      switch (t2) {
        case TYPE_TINYINT:
        case TYPE_SMALLINT:
        case TYPE_INT:
        case TYPE_BIGINT:
          result = builder.CreateZExt(child_value, return_type, "tmp_cast");
          break;
        case TYPE_FLOAT:
        case TYPE_DOUBLE:
          result = builder.CreateUIToFP(child_value, return_type, "tmp_cast");
          break;
        default:
          DCHECK(false);
      }
      break;
    case TYPE_TINYINT:
      switch (t2) {
        case TYPE_BOOLEAN:
          result = builder.CreateICmpNE(child_value,
              codegen->GetIntConstant(children()[0]->type(), 0));
          break;
        case TYPE_SMALLINT:
        case TYPE_INT:
        case TYPE_BIGINT:
          result = builder.CreateSExt(child_value, return_type, "tmp_cast");
          break;
        case TYPE_FLOAT:
        case TYPE_DOUBLE:
          result = builder.CreateSIToFP(child_value, return_type, "tmp_cast");
          break;
        default: DCHECK(false);
      }
      break;
    case TYPE_SMALLINT:
      switch (t2) {
        case TYPE_BOOLEAN:
          result = builder.CreateICmpNE(child_value,
              codegen->GetIntConstant(children()[0]->type(), 0));
          break;
        case TYPE_TINYINT:
          result = builder.CreateTrunc(child_value, return_type, "tmp_cast");
          break;
        case TYPE_INT:
        case TYPE_BIGINT:
          result = builder.CreateSExt(child_value, return_type, "tmp_cast");
          break;
        case TYPE_FLOAT:
        case TYPE_DOUBLE:
          result = builder.CreateSIToFP(child_value, return_type, "tmp_cast");
          break;
        default: DCHECK(false);
      }
      break;
    case TYPE_INT:
      switch (t2) {
        case TYPE_BOOLEAN:
          result = builder.CreateICmpNE(child_value,
              codegen->GetIntConstant(children()[0]->type(), 0));
          break;
        case TYPE_TINYINT:
        case TYPE_SMALLINT:
          result = builder.CreateTrunc(child_value, return_type, "tmp_cast");
          break;
        case TYPE_BIGINT:
          result = builder.CreateSExt(child_value, return_type, "tmp_cast");
          break;
        case TYPE_FLOAT:
        case TYPE_DOUBLE:
          result = builder.CreateSIToFP(child_value, return_type, "tmp_cast");
          break;
        default: DCHECK(false);
      }
      break;
    case TYPE_BIGINT:
      switch (t2) {
        case TYPE_BOOLEAN:
          result = builder.CreateICmpNE(child_value,
              codegen->GetIntConstant(children()[0]->type(), 0));
          break;
        case TYPE_TINYINT:
        case TYPE_SMALLINT:
        case TYPE_INT:
          result = builder.CreateTrunc(child_value, return_type, "tmp_cast");
          break;
        case TYPE_FLOAT:
        case TYPE_DOUBLE:
          result = builder.CreateSIToFP(child_value, return_type, "tmp_cast");
          break;
        default: DCHECK(false);
      }
      break;
    case TYPE_FLOAT:
      switch (t2) {
        case TYPE_BOOLEAN:
          result = builder.CreateFPToSI(
              child_value, codegen->GetType(TYPE_INT), "tmp_cast");
          result = builder.CreateICmpNE(result, codegen->GetIntConstant(TYPE_INT, 0));
          break;
        case TYPE_TINYINT:
        case TYPE_SMALLINT:
        case TYPE_INT:
        case TYPE_BIGINT:
          result = builder.CreateFPToSI(child_value, return_type, "tmp_cast");
          break;
        case TYPE_DOUBLE:
          result = builder.CreateFPExt(child_value, return_type, "tmp_cast");
          break;
        default: DCHECK(false);
      }
      break;
    case TYPE_DOUBLE:
      switch (t2) {
        case TYPE_BOOLEAN:
          result = builder.CreateFPToSI(
              child_value, codegen->GetType(TYPE_INT), "tmp_cast");
          result = builder.CreateICmpNE(result, codegen->GetIntConstant(TYPE_INT, 0));
          break;
        case TYPE_TINYINT:
        case TYPE_SMALLINT:
        case TYPE_INT:
        case TYPE_BIGINT:
          result = builder.CreateFPToSI(child_value, return_type, "tmp_cast");
          break;
        case TYPE_FLOAT:
          result = builder.CreateFPTrunc(child_value, return_type, "tmp_cast");
          break;
        default: DCHECK(false);
      }
      break;
    default:
      DCHECK(false);
  }

  builder.CreateBr(ret_block);

  // Return block.  is_null return does not have to set explicitly, propagated from child
  // Use a phi node to coalesce results.
  builder.SetInsertPoint(ret_block);
  PHINode* phi_node = builder.CreatePHI(return_type, 2, "tmp_phi");
  phi_node->addIncoming(GetNullReturnValue(codegen), entry_block);
  phi_node->addIncoming(result, child_not_null_block);
  builder.CreateRet(phi_node);

  return codegen->FinalizeFunction(function);
}

}
