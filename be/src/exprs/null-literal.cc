// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "null-literal.h"

#include "codegen/llvm-codegen.h"
#include "gen-cpp/Exprs_types.h"

using namespace llvm;

namespace impala {

NullLiteral::NullLiteral(const TExprNode& node)
  : Expr(node) {
}

void* NullLiteral::ReturnValue(Expr* e, TupleRow* row) {
  return NULL;
}

Status NullLiteral::Prepare(RuntimeState* state, const RowDescriptor& row_desc) {
  DCHECK_EQ(children_.size(), 0);
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
  
  Function* function = CreateComputeFnPrototype(codegen, "NullLiteral");
  BasicBlock* entry_block = BasicBlock::Create(context, "entry", function);

  codegen->builder()->SetInsertPoint(entry_block);
  SetIsNullReturnArg(codegen, function, true);
  codegen->builder()->CreateRet(GetNullReturnValue(codegen));
  
  if (!codegen->VerifyFunction(function)) return NULL;
  return function;
}

}

