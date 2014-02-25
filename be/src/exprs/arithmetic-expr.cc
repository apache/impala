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
#include "exprs/arithmetic-expr.h"
#include "util/debug-util.h"
#include "gen-cpp/Exprs_types.h"

using namespace llvm;
using namespace std;

namespace impala {

ArithmeticExpr::ArithmeticExpr(const TExprNode& node)
  : Expr(node) {
}

Status ArithmeticExpr::Prepare(RuntimeState* state, const RowDescriptor& desc) {
  DCHECK_LE(children_.size(), 2);
  return Expr::Prepare(state, desc);
}

string ArithmeticExpr::DebugString() const {
  stringstream out;
  out << "ArithmeticExpr(" << Expr::DebugString() << ")";
  return out.str();
}

// IR generator for ArithmeticExpr.  For ADD_LONG_LONG, the IR looks like:
//
// define i64 @ArithmeticExpr(i8** %row, i8* %state_data, i1* %is_null) {
// entry:
//   %child_result = call i64 @IntLiteral(i8** %row, i8* %state_data, i1* %is_null)
//   %child_null = load i1* %is_null
//   br i1 %child_null, label %ret, label %compute_rhs
//
// compute_rhs:                                      ; preds = %entry
//   %child_result1 = call i64 @IntLiteral1(i8** %row, i8* %state_data, i1* %is_null)
//   %child_null2 = load i1* %is_null
//   br i1 %child_null2, label %ret, label %arith
//
// arith:                                            ; preds = %compute_rhs
//   %tmp_add = add i64 %child_result, %child_result1
//   br label %ret
//
// ret:                                           ; preds = %arith, %compute_rhs, %entry
//   %tmp_phi = phi i64 [ 0, %entry ], [ 0, %compute_rhs ], [ %tmp_add, %arith ]
//   ret i64 %tmp_phi
// }
Function* ArithmeticExpr::Codegen(LlvmCodeGen* codegen) {
  DCHECK_LE(GetNumChildren(), 2);
  DCHECK_GE(GetNumChildren(), 1);
  Function* lhs_function = children()[0]->Codegen(codegen);
  if (lhs_function == NULL) return NULL;
  Function* rhs_function = NULL;
  if (GetNumChildren() == 2) {
    rhs_function = children()[1]->Codegen(codegen);
    if (rhs_function == NULL) return NULL;
  }

  LLVMContext& context = codegen->context();
  LlvmCodeGen::LlvmBuilder builder(context);
  Type* return_type = codegen->GetType(type());
  Function* function = CreateComputeFnPrototype(codegen, "ArithmeticExpr");

  BasicBlock* entry_block = BasicBlock::Create(context, "entry", function);
  builder.SetInsertPoint(entry_block);

  BasicBlock* compute_rhs_block = NULL;
  BasicBlock* compute_arith_block = NULL;
  Value* lhs_value = NULL;
  Value* rhs_value = NULL;

  if (GetNumChildren() == 2) {
    compute_rhs_block = BasicBlock::Create(context, "compute_rhs", function);
  }
  compute_arith_block = BasicBlock::Create(context, "arith", function);
  BasicBlock* ret_block = BasicBlock::Create(context, "ret", function);

  // Call lhs function
  lhs_value = children()[0]->CodegenGetValue(codegen, entry_block, ret_block,
      GetNumChildren() == 2 ? compute_rhs_block : compute_arith_block);

  // Lhs not null, eval rhs
  if (GetNumChildren() == 2) {
    builder.SetInsertPoint(compute_rhs_block);
    rhs_value = children()[1]->CodegenGetValue(codegen, compute_rhs_block,
        ret_block, compute_arith_block);
  }

  // rhs not null, do arithmetic op
  builder.SetInsertPoint(compute_arith_block);

  Value* result = NULL;
  // TODO: this is a temporary hack until the expr refactoring goes in and
  // all this code is removed.
  if (fn_.name.function_name == "bitnot") {
    result = builder.CreateNot(lhs_value, "tmp_not");
  } else if (fn_.name.function_name == "bitand") {
    result = builder.CreateAnd(lhs_value, rhs_value, "tmp_and");
  } else if (fn_.name.function_name == "bitor") {
    result = builder.CreateOr(lhs_value, rhs_value, "tmp_or");
  } else if (fn_.name.function_name == "bitxor") {
    result = builder.CreateXor(lhs_value, rhs_value, "tmp_xor");
  } else if (fn_.name.function_name == "add") {
    switch(type().type) {
      case TYPE_TINYINT:
      case TYPE_SMALLINT:
      case TYPE_INT:
      case TYPE_BIGINT:
        result = builder.CreateAdd(lhs_value, rhs_value, "tmp_add");
        break;
      case TYPE_FLOAT:
      case TYPE_DOUBLE:
        result = builder.CreateFAdd(lhs_value, rhs_value, "tmp_add");
        break;
      default:
        DCHECK(false) << "Shouldn't get here.";
    }
  } else if (fn_.name.function_name == "subtract") {
    switch(type().type) {
      case TYPE_TINYINT:
      case TYPE_SMALLINT:
      case TYPE_INT:
      case TYPE_BIGINT:
        result = builder.CreateSub(lhs_value, rhs_value, "tmp_sub");
        break;
      case TYPE_FLOAT:
      case TYPE_DOUBLE:
        result = builder.CreateFSub(lhs_value, rhs_value, "tmp_sub");
        break;
      default:
        DCHECK(false) << "Shouldn't get here.";
    }
  } else if (fn_.name.function_name == "multiply") {
    switch(type().type) {
      case TYPE_TINYINT:
      case TYPE_SMALLINT:
      case TYPE_INT:
      case TYPE_BIGINT:
        result = builder.CreateMul(lhs_value, rhs_value, "tmp_mul");
        break;
      case TYPE_FLOAT:
      case TYPE_DOUBLE:
        result = builder.CreateFMul(lhs_value, rhs_value, "tmp_div");
        break;
      default:
        DCHECK(false) << "Shouldn't get here.";
    }
  } else if (fn_.name.function_name == "divide") {
    result = builder.CreateFDiv(lhs_value, rhs_value, "tmp_div");
  } else if (fn_.name.function_name == "int_divide") {
      result = builder.CreateSDiv(lhs_value, rhs_value, "tmp_div");
  } else if (fn_.name.function_name == "mod") {
    result = builder.CreateSRem(lhs_value, rhs_value, "tmp_mul");
  } else if (fn_.name.function_name == "fmod") {
    result = builder.CreateFRem(lhs_value, rhs_value, "tmp_fmod");
  } else {
    DCHECK(false) << "Unknown arithmetic function: " << fn_.name.function_name;
  }
  builder.CreateBr(ret_block);

  // Return block.  is_null return does not have to set explicitly, propagated from child
  // Use a phi node to coalesce results.
  builder.SetInsertPoint(ret_block);
  int num_paths = 1 + GetNumChildren();
  PHINode* phi_node = builder.CreatePHI(return_type, num_paths, "tmp_phi");
  phi_node->addIncoming(GetNullReturnValue(codegen), entry_block);
  if (GetNumChildren() == 2) {
    phi_node->addIncoming(GetNullReturnValue(codegen), compute_rhs_block);
  }
  phi_node->addIncoming(result, compute_arith_block);
  builder.CreateRet(phi_node);

  return codegen->FinalizeFunction(function);
}

}
