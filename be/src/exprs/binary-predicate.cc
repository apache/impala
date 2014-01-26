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

  PrimitiveType t = children_[0]->type();
  DCHECK_EQ(t, children_[1]->type());

  Value* result = NULL;
  if (fn_.name.function_name == "eq") {
    result = codegen->CodegenEquals(&builder, lhs_value, rhs_value,
        children()[0]->type());
  } else if (fn_.name.function_name == "ne") {
    switch (t) {
      case TYPE_BOOLEAN:
      case TYPE_TINYINT:
      case TYPE_SMALLINT:
      case TYPE_INT:
      case TYPE_BIGINT:
        result = builder.CreateICmpNE(lhs_value, rhs_value, "tmp_ne");
        break;
      case TYPE_FLOAT:
      case TYPE_DOUBLE:
        result = builder.CreateFCmpUNE(lhs_value, rhs_value, "tmp_ne");
        break;
      case TYPE_STRING: {
        Function* call_fn = codegen->GetFunction(IRFunction::STRING_VALUE_NE);
        result = builder.CreateCall2(call_fn, lhs_value, rhs_value, "tmp_ne");
        break;
      }
      default:
        DCHECK(false) << "Shouldn't get here.";
    }
  } else if (fn_.name.function_name == "ge") {
    switch (t) {
      case TYPE_BOOLEAN:
        // LLVM defines false > true
        result = builder.CreateICmpSLE(lhs_value, rhs_value, "tmp_ge");
        break;
      case TYPE_TINYINT:
      case TYPE_SMALLINT:
      case TYPE_INT:
      case TYPE_BIGINT:
        result = builder.CreateICmpSGE(lhs_value, rhs_value, "tmp_ge");
        break;
      case TYPE_FLOAT:
      case TYPE_DOUBLE:
        result = builder.CreateFCmpUGE(lhs_value, rhs_value, "tmp_ge");
        break;
      case TYPE_STRING: {
        Function* call_fn = codegen->GetFunction(IRFunction::STRING_VALUE_GE);
        result = builder.CreateCall2(call_fn, lhs_value, rhs_value, "tmp_ge");
        break;
      }
      default:
        DCHECK(false) << "Shouldn't get here.";
    }
  } else if (fn_.name.function_name == "gt") {
    switch (t) {
      case TYPE_BOOLEAN:
        // LLVM defines false > true
        result = builder.CreateICmpSLT(lhs_value, rhs_value, "tmp_gt");
        break;
      case TYPE_TINYINT:
      case TYPE_SMALLINT:
      case TYPE_INT:
      case TYPE_BIGINT:
        result = builder.CreateICmpSGT(lhs_value, rhs_value, "tmp_gt");
        break;
      case TYPE_FLOAT:
      case TYPE_DOUBLE:
        result = builder.CreateFCmpUGT(lhs_value, rhs_value, "tmp_gt");
        break;
      case TYPE_STRING: {
        Function* call_fn = codegen->GetFunction(IRFunction::STRING_VALUE_GT);
        result = builder.CreateCall2(call_fn, lhs_value, rhs_value, "tmp_gt");
        break;
      }
      default:
        DCHECK(false) << "Shouldn't get here.";
    }
  } else if (fn_.name.function_name == "le") {
    switch (t) {
      case TYPE_BOOLEAN:
        // LLVM defines false > true
        result = builder.CreateICmpSGE(lhs_value, rhs_value, "tmp_le");
        break;
      case TYPE_TINYINT:
      case TYPE_SMALLINT:
      case TYPE_INT:
      case TYPE_BIGINT:
        result = builder.CreateICmpSLE(lhs_value, rhs_value, "tmp_le");
        break;
      case TYPE_FLOAT:
      case TYPE_DOUBLE:
        result = builder.CreateFCmpULE(lhs_value, rhs_value, "tmp_le");
        break;
      case TYPE_STRING: {
        Function* call_fn = codegen->GetFunction(IRFunction::STRING_VALUE_LE);
        result = builder.CreateCall2(call_fn, lhs_value, rhs_value, "tmp_le");
        break;
      }
      default:
        DCHECK(false) << "Shouldn't get here.";
    }
  } else if (fn_.name.function_name == "lt") {
    switch (t) {
      case TYPE_BOOLEAN:
        // LLVM defines false > true
        result = builder.CreateICmpSGT(lhs_value, rhs_value, "tmp_lt");
        break;
      case TYPE_TINYINT:
      case TYPE_SMALLINT:
      case TYPE_INT:
      case TYPE_BIGINT:
        result = builder.CreateICmpSLT(lhs_value, rhs_value, "tmp_lt");
        break;
      case TYPE_FLOAT:
      case TYPE_DOUBLE:
        result = builder.CreateFCmpULT(lhs_value, rhs_value, "tmp_lt");
        break;
      case TYPE_STRING: {
        Function* call_fn = codegen->GetFunction(IRFunction::STRING_VALUE_LT);
        result = builder.CreateCall2(call_fn, lhs_value, rhs_value, "tmp_lt");
        break;
      }
      default:
        DCHECK(false) << "Shouldn't get here.";
    }
  } else {
    DCHECK(false) << "Unknown binary predicate function: " << fn_.name.function_name;
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

  return codegen->FinalizeFunction(function);
}

}
