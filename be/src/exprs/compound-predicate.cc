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
#include "exprs/compound-predicate.h"
#include "util/debug-util.h"

using namespace std;
using namespace llvm;

namespace impala {

CompoundPredicate::CompoundPredicate(const TExprNode& node)
  : Predicate(node) {
}

Status CompoundPredicate::Prepare(RuntimeState* state, const RowDescriptor& desc) {
  DCHECK_LE(children_.size(), 2);
  return Expr::Prepare(state, desc);
}

void* CompoundPredicate::AndComputeFn(Expr* e, TupleRow* row) {
  CompoundPredicate* p = static_cast<CompoundPredicate*>(e);
  DCHECK_EQ(p->children_.size(), 2);
  DCHECK_EQ(p->opcode_, TExprOpcode::COMPOUND_AND);
  Expr* op1 = e->children()[0];
  bool* val1 = reinterpret_cast<bool*>(op1->GetValue(row));
  Expr* op2 = e->children()[1];
  bool* val2 = reinterpret_cast<bool*>(op2->GetValue(row));
  // <> && false is false
  if ((val1 != NULL && !*val1) || (val2 != NULL && !*val2)) {
    p->result_.bool_val = false;
    return &p->result_.bool_val;
  }
  // true && NULL is NULL
  if (val1 == NULL || val2 == NULL) {
    return NULL;
  }
  p->result_.bool_val = true;
  return &p->result_.bool_val;
}

void* CompoundPredicate::OrComputeFn(Expr* e, TupleRow* row) {
  CompoundPredicate* p = static_cast<CompoundPredicate*>(e);
  DCHECK_EQ(p->children_.size(), 2);
  DCHECK_EQ(p->opcode_, TExprOpcode::COMPOUND_OR);
  Expr* op1 = e->children()[0];
  bool* val1 = reinterpret_cast<bool*>(op1->GetValue(row));
  Expr* op2 = e->children()[1];
  bool* val2 = reinterpret_cast<bool*>(op2->GetValue(row));
  // <> || true is true
  if ((val1 != NULL && *val1) || (val2 != NULL && *val2)) {
    p->result_.bool_val = true;
    return &p->result_.bool_val;
  }
  // false || NULL is NULL
  if (val1 == NULL || val2 == NULL) {
    return NULL;
  }
  p->result_.bool_val = false;
  return &p->result_.bool_val;
}

void* CompoundPredicate::NotComputeFn(Expr* e, TupleRow* row) {
  CompoundPredicate* p = static_cast<CompoundPredicate*>(e);
  DCHECK_EQ(p->children_.size(), 1);
  DCHECK_EQ(p->opcode_, TExprOpcode::COMPOUND_NOT);
  Expr* op = e->children()[0];
  bool* val = reinterpret_cast<bool*>(op->GetValue(row));
  if (val == NULL) return NULL;
  p->result_.bool_val = !*val;
  return &p->result_.bool_val;
}

string CompoundPredicate::DebugString() const {
  stringstream out;
  out << "CompoundPredicate(" << Expr::DebugString() << ")";
  return out.str();
}

// IR Codegen for the NOT compound predicate.
// define i1 @CompoundPredicate(i8** %row, i8* %state_data, i1* %is_null) {
// entry:
//   %child_result = call i1 @BinaryPredicate(i8** %row, i8* %state_data, i1* %is_null)
//   %child_null = load i1* %is_null
//   br i1 %child_null, label %ret, label %child_not_null
// 
// child_not_null:                                   ; preds = %entry
//   %0 = xor i1 %child_result, true
//   br label %ret
// 
// ret:                                              ; preds = %child_not_null, %entry
//   %tmp_phi = phi i1 [ false, %entry ], [ %0, %child_not_null ]
//   ret i1 %tmp_phi
// }
Function* CompoundPredicate::CodegenNot(LlvmCodeGen* codegen) {
  DCHECK_EQ(GetNumChildren(), 1);
  DCHECK_EQ(op(), TExprOpcode::COMPOUND_NOT);
  
  Function* child_function = children()[0]->Codegen(codegen);
  if (child_function == NULL) return NULL;

  LLVMContext& context = codegen->context();
  LlvmCodeGen::LlvmBuilder builder(context);
  Type* return_type = codegen->GetType(type());
  Function* function = CreateComputeFnPrototype(codegen, "CompoundPredicate");

  BasicBlock* entry_block = BasicBlock::Create(context, "entry", function);
  builder.SetInsertPoint(entry_block);

  BasicBlock* child_not_null_block = 
      BasicBlock::Create(context, "child_not_null", function);
  BasicBlock* ret_block = BasicBlock::Create(context, "ret", function);

  // Get child value
  Value* child_value = children()[0]->CodegenGetValue(codegen, entry_block,
      ret_block, child_not_null_block);

  // Child not NULL
  builder.SetInsertPoint(child_not_null_block);
  child_value = builder.CreateTrunc(child_value, codegen->boolean_type());
  Value* result = builder.CreateNot(child_value);
  builder.CreateBr(ret_block);

  // Ret/merge block
  builder.SetInsertPoint(ret_block);
  PHINode* phi_node = builder.CreatePHI(return_type, 2, "tmp_phi");
  phi_node->addIncoming(GetNullReturnValue(codegen), entry_block);
  phi_node->addIncoming(result, child_not_null_block);
  builder.CreateRet(phi_node);

  return function;
}

// IR codegen for compound and/or predicates.  Compound predicate has non trivial 
// null handling as well as many branches so this is pretty complicated.  The IR 
// for x && y is:
//
// define i1 @CompoundPredicate(i8** %row, i8* %state_data, i1* %is_null) {
// entry:
//   %rhs_null = alloca i1
//   %lhs_null = alloca i1
//   %lhs_call = call i1 @LiteralPredicate(i8** %row, i8* %state_data, i1* %lhs_null)
//   %rhs_call = call i1 @LiteralPredicate1(i8** %row, i8* %state_data, i1* %rhs_null)
//   %lhs_null2 = load i1* %lhs_null
//   %rhs_null3 = load i1* %rhs_null
//   %tmp_and = and i1 %lhs_call, %rhs_call
//   br i1 %lhs_null2, label %lhs_null1, label %lhs_not_null
// 
// lhs_null1:                                        ; preds = %entry
//   br i1 %rhs_null3, label %null_block, label %lhs_null_rhs_not_null
// 
// lhs_not_null:                                     ; preds = %entry
//   br i1 %rhs_null3, label %lhs_not_null_rhs_null, label %not_null_block
// 
// lhs_null_rhs_not_null:                            ; preds = %lhs_null1
//   br i1 %rhs_call, label %null_block, label %not_null_block
// 
// lhs_not_null_rhs_null:                            ; preds = %lhs_not_null
//   br i1 %lhs_call, label %null_block, label %not_null_block
// 
// null_block:                                       
//   store i1 true, i1* %is_null
//   br label %ret
// 
// not_null_block:                                   
//   %0 = phi i1 [ false, %lhs_null_rhs_not_null ], 
//               [ false, %lhs_not_null_rhs_null ], 
//               [ %tmp_and, %lhs_not_null ]
//   store i1 false, i1* %is_null
//   br label %ret
// 
// ret:                                           ; preds = %not_null_block, %null_block
//   %tmp_phi = phi i1 [ false, %null_block ], [ %0, %not_null_block ]
//   ret i1 %tmp_phi
// }
Function* CompoundPredicate::CodegenBinary(LlvmCodeGen* codegen) {
  DCHECK_EQ(GetNumChildren(), 2);

  Function* lhs_function = children()[0]->Codegen(codegen);
  if (lhs_function == NULL) return NULL;
  Function* rhs_function = children()[1]->Codegen(codegen);
  if (rhs_function == NULL) return NULL;
  
  LLVMContext& context = codegen->context();
  LlvmCodeGen::LlvmBuilder builder(context);
  Type* return_type = codegen->GetType(type());
  Function* function = CreateComputeFnPrototype(codegen, "CompoundPredicate");

  BasicBlock* entry_block = BasicBlock::Create(context, "entry", function);
  builder.SetInsertPoint(entry_block);

  LlvmCodeGen::NamedVariable lhs_null_var("lhs_null_ptr", codegen->boolean_type());
  LlvmCodeGen::NamedVariable rhs_null_var("rhs_null_ptr", codegen->boolean_type());
  
  // Create stack variables for lhs is_null result and rhs is_null result
  Value* lhs_is_null = codegen->CreateEntryBlockAlloca(function, lhs_null_var);
  Value* rhs_is_null = codegen->CreateEntryBlockAlloca(function, rhs_null_var);

  // Control blocks for aggregating results
  BasicBlock* lhs_null_block = BasicBlock::Create(context, "lhs_null", function);
  BasicBlock* lhs_not_null_block = 
      BasicBlock::Create(context, "lhs_not_null", function);
  BasicBlock* lhs_null_rhs_not_null_block = 
      BasicBlock::Create(context, "lhs_null_rhs_not_null", function);
  BasicBlock* lhs_not_null_rhs_null_block = 
      BasicBlock::Create(context, "lhs_not_null_rhs_null", function);
  BasicBlock* null_block = BasicBlock::Create(context, "null_block", function);
  BasicBlock* not_null_block = BasicBlock::Create(context, "not_null_block", function);
  BasicBlock* ret_block = BasicBlock::Create(context, "ret", function);

  // Initialize the function arguments to the child functions
  Function::arg_iterator func_args = function->arg_begin();
  Value* row_ptr = func_args++;
  Value* state_data_ptr = func_args++;
  Value* is_null_ptr = func_args;
  Value* args[3] = { row_ptr, state_data_ptr, lhs_is_null };

  // Call lhs
  Value* lhs_value = builder.CreateCall(lhs_function, args, "lhs_call");
  // Call rhs
  args[2] = rhs_is_null;
  Value* rhs_value = builder.CreateCall(rhs_function, args, "rhs_call");
  
  Value* lhs_is_null_val = builder.CreateLoad(lhs_is_null, "lhs_null");
  Value* rhs_is_null_val = builder.CreateLoad(rhs_is_null, "rhs_null");
  Value* compare = NULL;
  if (op() == TExprOpcode::COMPOUND_AND) {
    compare = builder.CreateAnd(lhs_value, rhs_value, "tmp_and");
  } else {
    compare = builder.CreateOr(lhs_value, rhs_value, "tmp_or");
  }

  // Branch if lhs is null
  builder.CreateCondBr(lhs_is_null_val, lhs_null_block, lhs_not_null_block);

  // lhs_is_null block
  builder.SetInsertPoint(lhs_null_block);
  builder.CreateCondBr(rhs_is_null_val, null_block, lhs_null_rhs_not_null_block);

  // lhs_is_not_null block
  builder.SetInsertPoint(lhs_not_null_block);
  builder.CreateCondBr(rhs_is_null_val, lhs_not_null_rhs_null_block, not_null_block);

  // lhs_not_null rhs_null block
  builder.SetInsertPoint(lhs_not_null_rhs_null_block);
  if (op() == TExprOpcode::COMPOUND_AND) {
    // false && null -> false; true && null -> null
    builder.CreateCondBr(lhs_value, null_block, not_null_block);
  } else {
    // true || null -> true; false || null -> null
    builder.CreateCondBr(lhs_value, not_null_block, null_block);
  }

  // lhs_null rhs_not_null block
  builder.SetInsertPoint(lhs_null_rhs_not_null_block);
  if (op() == TExprOpcode::COMPOUND_AND) {
    // null && false -> false; null && true -> null
    builder.CreateCondBr(rhs_value, null_block, not_null_block);
  } else {
    // null || true -> true; null || false -> null
    builder.CreateCondBr(rhs_value, not_null_block, null_block);
  }

  // NULL block
  builder.SetInsertPoint(null_block);
  builder.CreateStore(codegen->true_value(), is_null_ptr);
  builder.CreateBr(ret_block);

  // not-NULL block
  builder.SetInsertPoint(not_null_block);
  PHINode* not_null_phi = builder.CreatePHI(codegen->boolean_type(), 3);
  if (op() == TExprOpcode::COMPOUND_AND) {
    not_null_phi->addIncoming(codegen->false_value(), lhs_null_rhs_not_null_block);
    not_null_phi->addIncoming(codegen->false_value(), lhs_not_null_rhs_null_block);
    not_null_phi->addIncoming(compare, lhs_not_null_block);
  } else {
    not_null_phi->addIncoming(codegen->true_value(), lhs_null_rhs_not_null_block);
    not_null_phi->addIncoming(codegen->true_value(), lhs_not_null_rhs_null_block);
    not_null_phi->addIncoming(compare, lhs_not_null_block);
  }
  builder.CreateStore(codegen->false_value(), is_null_ptr);
  builder.CreateBr(ret_block);

  // Ret/merge block
  builder.SetInsertPoint(ret_block);
  PHINode* phi_node = builder.CreatePHI(return_type, 2, "tmp_phi");
  phi_node->addIncoming(codegen->false_value(), null_block);
  phi_node->addIncoming(not_null_phi, not_null_block);
  builder.CreateRet(phi_node);

  return function;
}

Function* CompoundPredicate::Codegen(LlvmCodeGen* codegen) {  
  Function* function = NULL;
  switch (op()) {
    case TExprOpcode::COMPOUND_AND:
    case TExprOpcode::COMPOUND_OR:
      function = CodegenBinary(codegen);
      break;
    case TExprOpcode::COMPOUND_NOT:
      function = CodegenNot(codegen);
      break;
    default:
      DCHECK(false);
      break;
  }
  if (function == NULL) return NULL;
  return codegen->FinalizeFunction(function);
}

}
