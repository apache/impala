// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include <sstream>
#include <glog/logging.h>

#include "codegen/llvm-codegen.h"
#include "exprs/binary-predicate.h"
#include "util/debug-util.h"
#include "gen-cpp/Exprs_types.h"

using namespace llvm;
using namespace std;

namespace impala {

BinaryPredicate::BinaryPredicate(const TExprNode& node)
  : Predicate(node) {
}

Status BinaryPredicate::Prepare(RuntimeState* state, const RowDescriptor& desc) {
  DCHECK_EQ(children_.size(), 2);
  return Expr::Prepare(state, desc);
}

string BinaryPredicate::DebugString() const {
  stringstream out;
  out << "BinaryPredicate(" << Expr::DebugString() << ")";
  return out.str();
}

// IR codegen for binary predicates.  For integer less than, the IR looks like:
//
// define i1 @BinaryPredicate(i8** %row, i8* %state_data, i1* %is_null) {
// entry:
//   %child_result = call i8 @IntLiteral(i8** %row, i8* %state_data, i1* %is_null)
//   %child_null = load i1* %is_null
//   br i1 %child_null, label %ret_block, label %lhs_not_null
// 
// lhs_not_null:                                     ; preds = %entry
//   %child_result1 = call i8 @IntLiteral1(i8** %row, i8* %state_data, i1* %is_null)
//   %child_null2 = load i1* %is_null
//   br i1 %child_null2, label %ret_block, label %rhs_not_null
// 
// rhs_not_null:                                     ; preds = %lhs_not_null
//   %tmp_lt = icmp slt i8 %child_result, %child_result1
//   br label %ret_block
// 
// ret_block:  
//   %tmp_phi = phi i1 [ false, %entry ], 
//                     [ false, %lhs_not_null ], 
//                     [ %tmp_lt, %rhs_not_null ]
//   ret i1 %tmp_phi
// }
Function* BinaryPredicate::Codegen(LlvmCodeGen* codegen) {
  DCHECK_EQ(GetNumChildren(), 2);
  Function* lhs_function = children()[0]->Codegen(codegen);
  Function* rhs_function = children()[1]->Codegen(codegen);
  if (lhs_function == NULL || rhs_function == NULL) return NULL;
  
  LLVMContext& context = codegen->context();
  LlvmCodeGen::LlvmBuilder builder(context);
  Type* return_type = codegen->GetType(type());
  Function* function = CreateComputeFnPrototype(codegen, "BinaryPredicate");

  BasicBlock* entry_block = BasicBlock::Create(context, "entry", function);
  BasicBlock* lhs_not_null_block = BasicBlock::Create(context, "lhs_not_null", function);
  BasicBlock* rhs_not_null_block = BasicBlock::Create(context, "rhs_not_null", function);
  BasicBlock* ret_block = BasicBlock::Create(context, "ret_block", function);

  builder.SetInsertPoint(entry_block);
  // Call lhs function
  Value* lhs_value = children()[0]->CodegenGetValue(codegen, entry_block, 
      ret_block, lhs_not_null_block);

  // Lhs not null, eval rhs
  builder.SetInsertPoint(lhs_not_null_block);
  Value* rhs_value = children()[1]->CodegenGetValue(codegen, lhs_not_null_block, 
      ret_block, rhs_not_null_block);

  // rhs not null, do arithmetic op
  builder.SetInsertPoint(rhs_not_null_block);

  Value* result = NULL;
  switch (op()) {
    case TExprOpcode::EQ_BOOL_BOOL:
    case TExprOpcode::EQ_CHAR_CHAR:
    case TExprOpcode::EQ_SHORT_SHORT:
    case TExprOpcode::EQ_INT_INT:
    case TExprOpcode::EQ_LONG_LONG:
    case TExprOpcode::ADD_LONG_LONG:
      result = builder.CreateICmpEQ(lhs_value, rhs_value, "tmp_eq");
      break;
    case TExprOpcode::EQ_FLOAT_FLOAT:
    case TExprOpcode::EQ_DOUBLE_DOUBLE:
      result = builder.CreateFCmpUEQ(lhs_value, rhs_value, "tmp_eq");
      break;

    case TExprOpcode::NE_BOOL_BOOL:
    case TExprOpcode::NE_CHAR_CHAR:
    case TExprOpcode::NE_SHORT_SHORT:
    case TExprOpcode::NE_INT_INT:
    case TExprOpcode::NE_LONG_LONG:
      result = builder.CreateICmpNE(lhs_value, rhs_value, "tmp_neq");
      break;
    case TExprOpcode::NE_FLOAT_FLOAT:
    case TExprOpcode::NE_DOUBLE_DOUBLE:
      result = builder.CreateFCmpUNE(lhs_value, rhs_value, "tmp_neq");
      break;

    case TExprOpcode::LE_BOOL_BOOL:  // LLVM defines false > true
    case TExprOpcode::GE_CHAR_CHAR:
    case TExprOpcode::GE_SHORT_SHORT:
    case TExprOpcode::GE_INT_INT:
    case TExprOpcode::GE_LONG_LONG:
      result = builder.CreateICmpSGE(lhs_value, rhs_value, "tmp_ge");
      break;
    case TExprOpcode::GE_FLOAT_FLOAT:
    case TExprOpcode::GE_DOUBLE_DOUBLE:
      result = builder.CreateFCmpUGE(lhs_value, rhs_value, "tmp_ge");
      break;

    case TExprOpcode::LT_BOOL_BOOL:  // LLVM defines false > true
    case TExprOpcode::GT_CHAR_CHAR:
    case TExprOpcode::GT_SHORT_SHORT:
    case TExprOpcode::GT_INT_INT:
    case TExprOpcode::GT_LONG_LONG:
      result = builder.CreateICmpSGT(lhs_value, rhs_value, "tmp_gt");
      break;
    case TExprOpcode::GT_FLOAT_FLOAT:
    case TExprOpcode::GT_DOUBLE_DOUBLE:
      result = builder.CreateFCmpUGT(lhs_value, rhs_value, "tmp_gt");
      break;

    case TExprOpcode::GE_BOOL_BOOL:  // LLVM defines false > true
    case TExprOpcode::LE_CHAR_CHAR:
    case TExprOpcode::LE_SHORT_SHORT:
    case TExprOpcode::LE_INT_INT:
    case TExprOpcode::LE_LONG_LONG:
      result = builder.CreateICmpSLE(lhs_value, rhs_value, "tmp_le");
      break;
    case TExprOpcode::LE_FLOAT_FLOAT:
    case TExprOpcode::LE_DOUBLE_DOUBLE:
      result = builder.CreateFCmpULE(lhs_value, rhs_value, "tmp_le");
      break;
 
    case TExprOpcode::GT_BOOL_BOOL:  // LLVM defines false > true
    case TExprOpcode::LT_CHAR_CHAR:
    case TExprOpcode::LT_SHORT_SHORT:
    case TExprOpcode::LT_INT_INT:
    case TExprOpcode::LT_LONG_LONG:
      result = builder.CreateICmpSLT(lhs_value, rhs_value, "tmp_lt");
      break;
    case TExprOpcode::LT_FLOAT_FLOAT:
    case TExprOpcode::LT_DOUBLE_DOUBLE:
      result = builder.CreateFCmpULT(lhs_value, rhs_value, "tmp_lt");
      break;

    default:
      DCHECK(false) << "Unknown op: " << op();
      return NULL;
  }
  builder.CreateBr(ret_block);

  // Return block.  is_null return does not have to be set, propagated from child
  // Use a phi node to coalesce results.
  builder.SetInsertPoint(ret_block);
  PHINode* phi_node = builder.CreatePHI(return_type, 3, "tmp_phi");
  phi_node->addIncoming(GetNullReturnValue(codegen), entry_block);
  phi_node->addIncoming(GetNullReturnValue(codegen), lhs_not_null_block);
  phi_node->addIncoming(result, rhs_not_null_block);
  builder.CreateRet(phi_node);

  if (!codegen->VerifyFunction(function)) return NULL;
  return function;
}

}
