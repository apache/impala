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

#include "exprs/in-predicate.h"
#include "codegen/llvm-codegen.h"
#include "runtime/raw-value.h"
#include "runtime/string-value.inline.h"

using namespace llvm;
using namespace std;

namespace impala {

InPredicate::InPredicate(const TExprNode& node)
  : Predicate(node),
    is_not_in_(node.in_predicate.is_not_in) {
}

Status InPredicate::Prepare(RuntimeState* state, const RowDescriptor& desc) {
  DCHECK_GE(children_.size(), 2);
  Expr::PrepareChildren(state, desc);
  compute_fn_ = ComputeFn;
  return Status::OK;
}

string InPredicate::DebugString() const {
  stringstream out;
  out << "InPredicate(" << GetChild(0)->DebugString() << " " << is_not_in_ << ",[";
  int num_children = GetNumChildren();
  for (int i = 1; i < num_children; ++i) {
    out << (i == 1 ? "" : " ") << GetChild(i)->DebugString();
  }
  out << "])";
  return out.str();
}

void* InPredicate::ComputeFn(Expr* e, TupleRow* row) {
  void* cmp_val = e->children()[0]->GetValue(row);
  if (cmp_val == NULL) return NULL;
  PrimitiveType type = e->children()[0]->type();
  InPredicate* in_pred = static_cast<InPredicate*>(e);
  int32_t num_children = e->GetNumChildren();
  bool found_null = false;
  for (int32_t i = 1; i < num_children; ++i) {
    DCHECK(type == e->children()[i]->type() || type == TYPE_NULL
        || e->children()[i]->type() == TYPE_NULL);
    void* in_list_val = e->children()[i]->GetValue(row);
    if (in_list_val == NULL) {
      found_null = true;
      continue;
    }
    if (RawValue::Eq(cmp_val, in_list_val, type)) {
      e->result_.bool_val = !in_pred->is_not_in_;
      return &e->result_.bool_val;
    }
  }
  if (found_null) return NULL;
  e->result_.bool_val = in_pred->is_not_in_;
  return &e->result_.bool_val;
}

// LLVM IR generation for InPredicate. Resulting IR looks like:
//
// define i1 @InPredicate(i8** %row, i8* %state_data, i1* %is_null) {
// entry:
//   %found_null = alloca i1
//   store i1 false, i1* %found_null
//   %cmp_value = call i32 @SlotRef(i8** %row, i8* %state_data, i1* %is_null)
//   %child_null = load i1* %is_null
//   br i1 %child_null, label %null_not_found, label %in_case
// 
// in_case:                                          ; preds = %entry
//   %in_val = call i32 @IntLiteral(i8** %row, i8* %state_data, i1* %is_null)
//   %child_null1 = load i1* %is_null
//   br i1 %child_null1, label %null_block, label %compare
// 
// compare:                                          ; preds = %in_case
//   %tmp_eq = icmp eq i32 %cmp_value, %in_val
//   br i1 %tmp_eq, label %is_equal, label %continue
// 
// is_equal:                                         ; preds = %compare
//   store i1 false, i1* %is_null
//   ret i1 true
// 
// null_block:                                       ; preds = %in_case
//   store i1 true, i1* %found_null
//   br label %continue
// 
// continue:                                         ; preds = %null_block, %compare
//   %0 = load i1* %found_null
//   br i1 %0, label %null_found, label %null_not_found
// 
// null_found:                                       ; preds = %continue
//   store i1 true, i1* %is_null
//   ret i1 false
// 
// null_not_found:                                   ; preds = %continue, %entry
//   store i1 false, i1* %is_null
//   ret i1 false
// }
// TODO: for int types, this can be made more efficient by generating the code
// as a switch statement.  For example, if the query is int_col in (1,3,5),
// we could generate:
//  int cmp_val = children(0)->Eval();
//  switch (cmp_val) {
//    case 1: case 3: case 5: return true;
//    default: return false;
//  }
// We'll want to investigate how the resulting asm differs from this implementation
// i.e. if (cmp_val == 1 || cmp_val == 3 || cmp_val == 5) return true
Function* InPredicate::Codegen(LlvmCodeGen* codegen) {
  DCHECK_GE(GetNumChildren(), 1);
  for (int i = 0; i < GetNumChildren(); ++i) {
    // Codegen the child exprs
    if (children()[i]->Codegen(codegen) == NULL) return NULL;
  }

  LLVMContext& context = codegen->context();
  LlvmCodeGen::LlvmBuilder builder(context);

  Function* function = CreateComputeFnPrototype(codegen, "InPredicate");
  BasicBlock* entry_block = BasicBlock::Create(context, "entry", function);
  
  builder.SetInsertPoint(entry_block);
  Value* found_null_ptr = codegen->CreateEntryBlockAlloca(
      function, LlvmCodeGen::NamedVariable("found_null", codegen->GetType(TYPE_BOOLEAN)));
  builder.CreateStore(codegen->false_value(), found_null_ptr);

  // Block for when none of the in cases matched.
  BasicBlock* in_case_block = BasicBlock::Create(context, "in_case", function);
  BasicBlock* null_found_block, *null_not_found_block;
  codegen->CreateIfElseBlocks(function, "null_found", "null_not_found",
      &null_found_block, &null_not_found_block);
  
  // Get child value
  Value* cmp_value = children()[0]->CodegenGetValue(codegen, entry_block,
      null_not_found_block, in_case_block, "cmp_value");

  BasicBlock* current_block = in_case_block;
  builder.SetInsertPoint(current_block);
  for (int i = 1; i  < GetNumChildren(); ++i) {
    BasicBlock* compare_block = 
        BasicBlock::Create(context, "compare", function, null_found_block);
    BasicBlock* is_equal_block = 
        BasicBlock::Create(context, "is_equal", function, null_found_block);
    BasicBlock* null_block = 
        BasicBlock::Create(context, "null_block", function, null_found_block);
    BasicBlock* continue_block = 
        BasicBlock::Create(context, "continue", function, null_found_block);

    // Get the child value
    Value* in_list_val = children()[i]->CodegenGetValue(codegen, current_block, 
        null_block, compare_block, "in_val");

    builder.SetInsertPoint(compare_block);
    // Compare for equality
    Value* is_equal = 
        codegen->CodegenEquals(&builder, cmp_value, in_list_val, children()[0]->type());
    builder.CreateCondBr(is_equal, is_equal_block, continue_block);

    builder.SetInsertPoint(is_equal_block);
    CodegenSetIsNullArg(codegen, is_equal_block, false);
    builder.CreateRet(is_not_in_ ? codegen->false_value() : codegen->true_value());

    builder.SetInsertPoint(null_block);
    builder.CreateStore(codegen->true_value(), found_null_ptr);
    builder.CreateBr(continue_block);

    builder.SetInsertPoint(continue_block);
    current_block = continue_block;
  }

  // Branch on found_null
  Value* found_null_value = builder.CreateLoad(found_null_ptr);
  builder.CreateCondBr(found_null_value, null_found_block, null_not_found_block);

  builder.SetInsertPoint(null_found_block);
  CodegenSetIsNullArg(codegen, null_found_block, true);
  builder.CreateRet(GetNullReturnValue(codegen));

  builder.SetInsertPoint(null_not_found_block);
  CodegenSetIsNullArg(codegen, null_not_found_block, false);
  builder.CreateRet(is_not_in_ ? codegen->true_value() : codegen->false_value());
  
  return codegen->FinalizeFunction(function);
}

}
