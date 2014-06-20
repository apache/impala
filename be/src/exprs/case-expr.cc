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

#include "exprs/case-expr.h"
#include "exprs/conditional-functions.h"
#include "codegen/llvm-codegen.h"

#include "gen-cpp/Exprs_types.h"

using namespace llvm;
using namespace std;

namespace impala {

CaseExpr::CaseExpr(const TExprNode& node)
  : Expr(node),
    has_case_expr_(node.case_expr.has_case_expr),
    has_else_expr_(node.case_expr.has_else_expr) {
}

Status CaseExpr::Prepare(RuntimeState* state, const RowDescriptor& row_desc) {
  RETURN_IF_ERROR(Expr::Prepare(state, row_desc));
  // Override compute function for this special case.
  // Otherwise keep the one provided by the OpCodeRegistry set in the parent's c'tor.
  if (!has_case_expr_) {
    compute_fn_ = ConditionalFunctions::NoCaseComputeFn;
  }
  return Status::OK;
}

string CaseExpr::DebugString() const {
  stringstream out;
  out << "CaseExpr(has_case_expr=" << has_case_expr_
      << " has_else_expr=" << has_else_expr_
      << " " << Expr::DebugString() << ")";
  return out.str();
}


// Sample IR output when there is a case expression and else expression
// define i1 @CaseExpr(i8** %row, i8* %state_data, i1* %is_null) {
// eval_case_expr:
//   %case_val = call i1 @SlotRef(i8** %row, i8* %state_data, i1* %is_null)
//   %child_null = load i1* %is_null
//   br i1 %child_null, label %return_else_expr, label %eval_first_when_expr
//
// eval_first_when_expr:                             ; preds = %eval_case_expr
//   %when_val = call i1 @BoolLiteral(i8** %row, i8* %state_data, i1* %is_null)
//   %child_null1 = load i1* %is_null
//   br i1 %child_null1, label %return_else_expr, label %check_when_expr_block
//
// check_when_expr_block:                            ; preds = %eval_first_when_expr
//   %tmp_eq = icmp eq i1 %case_val, %when_val
//   br i1 %tmp_eq, label %return_then_expr, label %return_else_expr
//
// return_then_expr:                                 ; preds = %check_when_expr_block
//   %0 = call i1 @BoolLiteral1(i8** %row, i8* %state_data, i1* %is_null)
//   ret i1 %0
//
// return_else_expr:;preds= %check_when_expr_block, %eval_first_when_expr, %eval_case_expr
//   %1 = call i1 @BoolLiteral2(i8** %row, i8* %state_data, i1* %is_null)
//   ret i1 %1
// }
//
// Sample IR output when there is case expression and no else expression
// define i1 @CaseExpr4(i8** %row, i8* %state_data, i1* %is_null) {
// eval_case_expr:
//   %case_val = call i1 @SlotRef(i8** %row, i8* %state_data, i1* %is_null)
//   %child_null = load i1* %is_null
//   br i1 %child_null, label %return_null, label %eval_first_when_expr
//
// eval_first_when_expr:                             ; preds = %eval_case_expr
//   %when_val = call i1 @BoolLiteral2(i8** %row, i8* %state_data, i1* %is_null)
//   %child_null1 = load i1* %is_null
//   br i1 %child_null1, label %return_null, label %check_when_expr_block
//
// check_when_expr_block:                            ; preds = %eval_first_when_expr
//   %tmp_eq = icmp eq i1 %case_val, %when_val
//   br i1 %tmp_eq, label %return_then_expr, label %return_null
//
// return_then_expr:                                 ; preds = %check_when_expr_block
//   %0 = call i1 @BoolLiteral3(i8** %row, i8* %state_data, i1* %is_null)
//   ret i1 %0
//
// return_null:  ; preds = %check_when_expr_block, %eval_first_when_expr, %eval_case_expr
//   store i1 true, i1* %is_null
//   ret i1 false
// }
//
// Sample IR output when there is no case expr and else expression
// define i1 @CaseExpr4(i8** %row, i8* %state_data, i1* %is_null) {
// eval_first_when_expr:
//   %when_val = call i1 @SlotRef(i8** %row, i8* %state_data, i1* %is_null)
//   %child_null = load i1* %is_null
//   br i1 %child_null, label %return_else_expr, label %check_when_expr_block
//
// check_when_expr_block:                            ; preds = %eval_first_when_expr
//   br i1 %when_val, label %return_then_expr, label %return_else_expr
//
// return_then_expr:                                 ; preds = %check_when_expr_block
//   %0 = call i1 @BoolLiteral2(i8** %row, i8* %state_data, i1* %is_null)
//   ret i1 %0
//
// return_else_expr:              ; preds = %check_when_expr_block, %eval_first_when_expr
//   %1 = call i1 @BoolLiteral3(i8** %row, i8* %state_data, i1* %is_null)
//   ret i1 %1
// }
Function* CaseExpr::Codegen(LlvmCodeGen* codegen) {
  const int num_children = GetNumChildren();
  for (int i = 0; i < num_children; ++i) {
    // Codegen the child exprs
    if (children()[i]->Codegen(codegen) == NULL) return NULL;
  }

  LLVMContext& context = codegen->context();
  LlvmCodeGen::LlvmBuilder builder(context);

  Function* function = CreateComputeFnPrototype(codegen, "CaseExpr");
  BasicBlock* eval_case_expr_block = NULL;

  // This is the block immediately after the when/then exprs. It will either point to a
  // block which returns the else expr, or returns NULL if no else expr is specified.
  BasicBlock* default_value_block = BasicBlock::Create(
      context, has_else_expr() ? "return_else_expr" : "return_null", function);

  // If there is a case expression, create a block to evaluate it.
  Value* case_val;
  BasicBlock* eval_first_when_expr_block = BasicBlock::Create(
      context, "eval_first_when_expr", function, default_value_block);
  BasicBlock* current_when_expr_block = eval_first_when_expr_block;
  if (has_case_expr()) {
    // Need at least case, when and then expr, and optionally an else expr
    DCHECK_GE(num_children, (has_else_expr()) ? 4 : 3);
    // If there is a case expr, create block eval_case_expr to evaluate the
    // case expr. Place this block before eval_first_when_expr_block
    eval_case_expr_block = BasicBlock::Create(context, "eval_case_expr",
        function, eval_first_when_expr_block);
    case_val = children()[0]->CodegenGetValue(codegen, eval_case_expr_block,
        default_value_block, eval_first_when_expr_block, "case_val");
  } else {
    DCHECK_GE(num_children, (has_else_expr()) ? 3 : 2);
  }

  const int loop_end = (has_else_expr()) ? num_children - 1 : num_children;
  const int last_loop_iter = loop_end - 2;
  // The loop increments by two each time, because each iteration handles one when/then
  // pair. Both when and then subexpressions are single children. If there is a case expr
  // start loop at index 1. (case expr is children()[0] and has already be evaluated.
  for (int i = (has_case_expr()) ? 1 : 0; i < loop_end; i += 2) {
    BasicBlock* check_when_expr_block = BasicBlock::Create(
        context, "check_when_expr_block", function, default_value_block);
    BasicBlock* return_then_expr_block =
        BasicBlock::Create(context, "return_then_expr", function, default_value_block);

    // continue_or_exit_block either points to the next eval_next_when_expr block,
    // or points to the defaut_value_block if there are no more when/then expressions.
    BasicBlock* continue_or_exit_block = NULL;
    if (i == last_loop_iter) {
      continue_or_exit_block = default_value_block;
    } else {
      continue_or_exit_block = BasicBlock::Create(
          context, "eval_next_when_expr", function, default_value_block);
    }

    // Get the child value of the when statement. If NULL simply continue to next when
    // statement
    Value* when_val = children()[i]->CodegenGetValue(codegen, current_when_expr_block,
        continue_or_exit_block, check_when_expr_block, "when_val");

    builder.SetInsertPoint(check_when_expr_block);
    if (has_case_expr()) {
      // Compare for equality
      Value * is_equal =
          codegen->CodegenEquals(&builder, case_val, when_val, children()[0]->type());
      builder.CreateCondBr(is_equal, return_then_expr_block, continue_or_exit_block);
    } else {
      builder.CreateCondBr(when_val, return_then_expr_block, continue_or_exit_block);
    }

    builder.SetInsertPoint(return_then_expr_block);

    // Eval and return then value
    Function::arg_iterator args_iter = function->arg_begin();
    Value* args[3] = { args_iter++, args_iter++, args_iter };
    Value* then_val = builder.CreateCall(children()[i+1]->codegen_fn(), args);
    builder.CreateRet(then_val);

    current_when_expr_block = continue_or_exit_block;
  }

  builder.SetInsertPoint(default_value_block);
  if (has_else_expr()) {
    Function::arg_iterator args_iter = function->arg_begin();
    Value* args[3] = { args_iter++, args_iter++, args_iter };
    Value* return_val =
        builder.CreateCall(children()[num_children - 1]->codegen_fn(), args);
    builder.CreateRet(return_val);
  } else {
    CodegenSetIsNullArg(codegen, default_value_block, true);
    builder.CreateRet(GetNullReturnValue(codegen));
  }

  return codegen->FinalizeFunction(function);
}
}
