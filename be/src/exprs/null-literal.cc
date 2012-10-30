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

#include "codegen/llvm-codegen.h"
#include "gen-cpp/Exprs_types.h"

using namespace llvm;

namespace impala {

NullLiteral::NullLiteral(const TExprNode& node)
  : Expr(node) {
}

NullLiteral::NullLiteral(PrimitiveType type) : Expr(type) {
}

void* NullLiteral::ReturnValue(Expr* e, TupleRow* row) {
  return NULL;
}

Status NullLiteral::Prepare(RuntimeState* state, const RowDescriptor& row_desc) {
  DCHECK_EQ(children_.size(), 0);
  compute_fn_ = ReturnValue;
  return Status::OK;
}

// Code generation for NULL literal.  IR looks like:
// define i1 @NullLiteral1(i8** %row, i8* %state_data, i1* %is_null) {
// entry:
//   store i1 true, i1* %is_null
//   ret i1 false
// }
Function* NullLiteral::Codegen(LlvmCodeGen* codegen) {
  DCHECK_EQ(GetNumChildren(), 0);
  LLVMContext& context = codegen->context();
  LlvmCodeGen::LlvmBuilder builder(context);
  
  Function* function = CreateComputeFnPrototype(codegen, "NullLiteral");
  BasicBlock* entry_block = BasicBlock::Create(context, "entry", function);

  builder.SetInsertPoint(entry_block);
  CodegenSetIsNullArg(codegen, entry_block, true);
  builder.CreateRet(GetNullReturnValue(codegen));
  
  return codegen->FinalizeFunction(function);
}

}
