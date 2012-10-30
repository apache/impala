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

#include "exprs/is-null-predicate.h"

#include <sstream>

#include "codegen/llvm-codegen.h"
#include "gen-cpp/Exprs_types.h"

using namespace std;
using namespace llvm;

namespace impala {

void* IsNullPredicate::ComputeFn(Expr* e, TupleRow* row) {
  IsNullPredicate* p = static_cast<IsNullPredicate*>(e);
  // assert(p->children_.size() == 1);
  Expr* op = e->children()[0];
  void* val = op->GetValue(row);
  p->result_.bool_val = (val == NULL) == !p->is_not_null_;
  return &p->result_.bool_val;
}

IsNullPredicate::IsNullPredicate(const TExprNode& node)
  : Predicate(node),
    is_not_null_(node.is_null_pred.is_not_null) {
}

Status IsNullPredicate::Prepare(RuntimeState* state, const RowDescriptor& row_desc) {
  RETURN_IF_ERROR(Expr::PrepareChildren(state, row_desc));
  compute_fn_ = ComputeFn;
  return Status::OK;
}

string IsNullPredicate::DebugString() const {
  stringstream out;
  out << "IsNullPredicate(not_null=" << is_not_null_ << Expr::DebugString() << ")";
  return out.str();
}

// IR Generation for IsNullPredicate.  The generated IR looks like:
// define i1 @IsNullPredicate(i8** %row, i8* %state_data, i1* %is_null) {
// entry:
//   %child_result = call i8 @IntLiteral(i8** %row, i8* %state_data, i1* %is_null)
//   %child_null = load i1* %is_null
//   br i1 %child_null, label %ret, label %not_null
// 
// not_null:                                         ; preds = %entry
//   br label %ret
// 
// ret:                                              ; preds = %not_null, %entry
//   %tmp_phi = phi i1 [ true, %entry ], [ false, %not_null ]
//   store i1 false, i1* %is_null
//   ret i1 %tmp_phi
// }
Function* IsNullPredicate::Codegen(LlvmCodeGen* codegen) {
  DCHECK_EQ(GetNumChildren(), 1);
  
  Function* child_function = children()[0]->Codegen(codegen);
  if (child_function == NULL) return NULL;

  LLVMContext& context = codegen->context();
  LlvmCodeGen::LlvmBuilder builder(context);
  Type* return_type = codegen->GetType(type());
  Function* function = CreateComputeFnPrototype(codegen, "IsNullPredicate");

  BasicBlock* entry_block = BasicBlock::Create(context, "entry", function);
  BasicBlock* not_null_block = BasicBlock::Create(context, "not_null", function);
  BasicBlock* ret_block = BasicBlock::Create(context, "ret", function);
  builder.SetInsertPoint(entry_block);
  
  // Call child function
  children()[0]->CodegenGetValue(codegen, entry_block, ret_block, not_null_block);

  builder.SetInsertPoint(not_null_block);
  builder.CreateBr(ret_block);

  // Return block.  
  builder.SetInsertPoint(ret_block);
  PHINode* phi_node = builder.CreatePHI(return_type, 2, "tmp_phi");
  if (is_not_null_) {
    phi_node->addIncoming(codegen->false_value(), entry_block);
    phi_node->addIncoming(codegen->true_value(), not_null_block);
  } else {
    phi_node->addIncoming(codegen->true_value(), entry_block);
    phi_node->addIncoming(codegen->false_value(), not_null_block);
  }
  CodegenSetIsNullArg(codegen, ret_block, false);
  builder.CreateRet(phi_node);

  return codegen->FinalizeFunction(function);
}


}
