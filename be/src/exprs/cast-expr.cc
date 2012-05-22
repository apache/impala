// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include <sstream>
#include <glog/logging.h>

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
  if (type() == TYPE_STRING || children()[0]->type() == TYPE_STRING) {
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
  Function* child_function = children()[0]->Codegen(codegen);
  if (child_function == NULL) return NULL;

  // No op cast.  Just return the child function.
  switch (op()) {
    case TExprOpcode::CAST_BOOL_BOOL:
    case TExprOpcode::CAST_CHAR_CHAR:
    case TExprOpcode::CAST_SHORT_SHORT:
    case TExprOpcode::CAST_INT_INT:
    case TExprOpcode::CAST_LONG_LONG:
    case TExprOpcode::CAST_FLOAT_FLOAT:
    case TExprOpcode::CAST_DOUBLE_DOUBLE:
      codegen_fn_ = child_function;
      scratch_buffer_size_ = children()[0]->scratch_buffer_size();
      return codegen_fn_;
    default:
      break;
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
  switch (op()) {
    case TExprOpcode::CAST_BOOL_CHAR:
    case TExprOpcode::CAST_BOOL_SHORT:
    case TExprOpcode::CAST_BOOL_INT:
    case TExprOpcode::CAST_BOOL_LONG:
      result = builder.CreateZExt(child_value, return_type, "tmp_cast");
      break;

    case TExprOpcode::CAST_CHAR_SHORT:
    case TExprOpcode::CAST_CHAR_INT:
    case TExprOpcode::CAST_CHAR_LONG:
    case TExprOpcode::CAST_SHORT_INT:
    case TExprOpcode::CAST_SHORT_LONG:
    case TExprOpcode::CAST_INT_LONG:
      result = builder.CreateSExt(child_value, return_type, "tmp_cast");
      break;
 
    case TExprOpcode::CAST_SHORT_CHAR:
    case TExprOpcode::CAST_INT_CHAR:
    case TExprOpcode::CAST_LONG_CHAR:
    case TExprOpcode::CAST_INT_SHORT:
    case TExprOpcode::CAST_LONG_SHORT:
    case TExprOpcode::CAST_LONG_INT:
      result = builder.CreateTrunc(child_value, return_type, "tmp_cast");
      break;

    case TExprOpcode::CAST_BOOL_FLOAT:
    case TExprOpcode::CAST_BOOL_DOUBLE:
      result = builder.CreateUIToFP(child_value, return_type, "tmp_cast");
      break;

    case TExprOpcode::CAST_CHAR_FLOAT:
    case TExprOpcode::CAST_SHORT_FLOAT:
    case TExprOpcode::CAST_INT_FLOAT:
    case TExprOpcode::CAST_LONG_FLOAT:
    case TExprOpcode::CAST_CHAR_DOUBLE:
    case TExprOpcode::CAST_SHORT_DOUBLE:
    case TExprOpcode::CAST_INT_DOUBLE:
    case TExprOpcode::CAST_LONG_DOUBLE:
      result = builder.CreateSIToFP(child_value, return_type, "tmp_cast");
      break;

    // llvm will treat bool types like tinyint types.  true is any non-zero value,
    // not 'one'.  We'll fix this by explicitly checking against 0.
    // TODO: is this a problem? How should we deal with this?
    case TExprOpcode::CAST_CHAR_BOOL:
    case TExprOpcode::CAST_SHORT_BOOL:
    case TExprOpcode::CAST_INT_BOOL:
    case TExprOpcode::CAST_LONG_BOOL:
      result = builder.CreateICmpNE(child_value, 
          codegen->GetIntConstant(children()[0]->type(), 0));
      break;
    case TExprOpcode::CAST_DOUBLE_BOOL:
    case TExprOpcode::CAST_FLOAT_BOOL:
      result = builder.CreateFPToSI(child_value, codegen->GetType(TYPE_INT), "tmp_cast");
      result = builder.CreateICmpNE(result, codegen->GetIntConstant(TYPE_INT, 0));
      break;

    case TExprOpcode::CAST_FLOAT_CHAR:
    case TExprOpcode::CAST_FLOAT_SHORT:
    case TExprOpcode::CAST_FLOAT_INT:
    case TExprOpcode::CAST_FLOAT_LONG:
    case TExprOpcode::CAST_DOUBLE_CHAR:
    case TExprOpcode::CAST_DOUBLE_SHORT:
    case TExprOpcode::CAST_DOUBLE_INT:
    case TExprOpcode::CAST_DOUBLE_LONG:
      result = builder.CreateFPToSI(child_value, return_type, "tmp_cast");
      break;

    case TExprOpcode::CAST_FLOAT_DOUBLE:
      result = builder.CreateFPExt(child_value, return_type, "tmp_cast");
      break;

    case TExprOpcode::CAST_DOUBLE_FLOAT:
      result = builder.CreateFPTrunc(child_value, return_type, "tmp_cast");
      break;

    default:
      DCHECK(false) << "Unknown op: " << op();
      return NULL;
  }
  builder.CreateBr(ret_block);

  // Return block.  is_null return does not have to set explicitly, propagated from child
  // Use a phi node to coalesce results.
  builder.SetInsertPoint(ret_block);
  PHINode* phi_node = builder.CreatePHI(return_type, 2, "tmp_phi");
  phi_node->addIncoming(GetNullReturnValue(codegen), entry_block);
  phi_node->addIncoming(result, child_not_null_block);
  builder.CreateRet(phi_node);

  if (!codegen->VerifyFunction(function)) return NULL;
  return function;
}

}
