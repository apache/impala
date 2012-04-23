// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "literal-predicate.h"

#include <sstream>

#include "codegen/llvm-codegen.h"
#include "gen-cpp/Exprs_types.h"

using namespace std;
using namespace llvm;

namespace impala {

void* LiteralPredicate::ComputeFn(Expr* e, TupleRow* row) {
  LiteralPredicate* p = static_cast<LiteralPredicate*>(e);
  return (p->is_null_) ? NULL : &p->result_.bool_val;
}

LiteralPredicate::LiteralPredicate(const TExprNode& node)
  : Predicate(node), is_null_(node.literal_pred.is_null) {
  result_.bool_val = node.literal_pred.value;
}

Status LiteralPredicate::Prepare(RuntimeState* state, const RowDescriptor& row_desc) {
  RETURN_IF_ERROR(Expr::PrepareChildren(state, row_desc));
  compute_fn_ = ComputeFn;
  return Status::OK;
}

string LiteralPredicate::DebugString() const {
  stringstream out;
  out << "LiteralPredicate(value=" << result_.bool_val << ")";
  return out.str();
}

// Llvm codegen for literal predicate.  The resulting IR looks like:
//
// define i1 @LiteralPredicate(i8** %row, i8* %state_data, i1* %is_null) {
// entry:
//   store i1 false, i1* %is_null
//   ret i1 true
// }
Function* LiteralPredicate::Codegen(LlvmCodeGen* codegen) {
  DCHECK_EQ(GetNumChildren(), 0);
  
  LLVMContext& context = codegen->context();
  LlvmCodeGen::LlvmBuilder builder(context);

  Function* function = CreateComputeFnPrototype(codegen, "LiteralPredicate");

  BasicBlock* entry_block = BasicBlock::Create(context, "entry", function);
  builder.SetInsertPoint(entry_block);
  
  CodegenSetIsNullArg(codegen, entry_block, is_null_);
  if (is_null_) {
    builder.CreateRet(GetNullReturnValue(codegen));
  } else {
    builder.CreateRet(ConstantInt::get(context, APInt(1, result_.bool_val, true)));
  }
  
  if (!codegen->VerifyFunction(function)) return NULL;
  return function;
}

}
