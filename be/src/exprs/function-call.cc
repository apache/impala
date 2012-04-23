// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include <sstream>
#include <string>
#include <glog/logging.h>

#include "codegen/llvm-codegen.h"
#include "exprs/function-call.h"

using namespace llvm;

using namespace std;
using namespace boost;

namespace impala {

FunctionCall::FunctionCall(const TExprNode& node)
  : Expr(node), regex_(NULL) {
}

string FunctionCall::DebugString() const {
  stringstream out;
  out << "FunctionCall(" << Expr::DebugString() << ")";
  return out.str();
}

bool FunctionCall::SetRegex(const string& pattern) {
  try {
    regex_.reset(new regex(pattern, regex_constants::extended));
  } catch(bad_expression& e) {
    return false;
  }
  return true;
}

void FunctionCall::SetReplaceStr(const StringValue* str_val) {
  replace_str_.reset(new string(str_val->ptr, str_val->len));
}

// IR generation for generic function calls.
// for sqrt, the IR looks like:
//
// define double @FunctionCall(i8** %row, i8* %state_data, i1* %is_null) {
// entry:
//   %child_result = call double @FloatLiteral(i8** %row, i8* %state_data, i1* %is_null)
//   %child_null = load i1* %is_null
//   br i1 %child_null, label %ret_block, label %not_null_block
// 
// not_null_block:                                   ; preds = %entry
//   %tmp_sqrt = call double @sqrt(double %child_result)
//   br label %ret_block
// 
// ret_block:                                        ; preds = %not_null_block, %entry
//   %tmp_phi = phi double [ 0.000000e+00, %entry ], [ %tmp_sqrt, %not_null_block ]
//   ret double %tmp_phi
// }
//
// TODO: this is still prototypey and needs to be made more generic to handle
// other functions with different signatures.
Function* FunctionCall::Codegen(LlvmCodeGen* codegen) {
  LLVMContext& context = codegen->context();
  LlvmCodeGen::LlvmBuilder builder(context);

  // Generate child functions
  for (int i = 0; i < GetNumChildren(); ++i) {
    Function* child = children()[i]->Codegen(codegen);
    if (child == NULL) return NULL;
  }

  Type* return_type = codegen->GetType(type());
  Function* function = CreateComputeFnPrototype(codegen, "FunctionCall");
  BasicBlock* entry_block = BasicBlock::Create(context, "entry", function);

  BasicBlock* not_null_block = BasicBlock::Create(context, "not_null_block", function);
  BasicBlock* ret_block = BasicBlock::Create(context, "ret_block", function);

  builder.SetInsertPoint(entry_block);
  scoped_ptr<LlvmCodeGen::FnPrototype> prototype;
  Type* ret_type = NULL;
  Value* result = NULL;
  switch (op()) {
    case TExprOpcode::MATH_SQRT: {
      ret_type = codegen->double_type();
      prototype.reset(new LlvmCodeGen::FnPrototype(codegen, "sqrt", ret_type));
      prototype->AddArgument(LlvmCodeGen::NamedVariable("x", codegen->double_type()));
      break;
    }
    default:
      LOG(ERROR) << "Unsupported function: " << op();
      return NULL;
  }

  Function* external_function = codegen->GetLibCFunction(prototype.get());

  // Call child functions.  TODO: this needs to be an IR loop over all children
  DCHECK_EQ(GetNumChildren(), 1);
  vector<Value*> args;
  args.resize(GetNumChildren());
  for (int i = 0; i < GetNumChildren(); ++i) {
    args[i] = children()[i]->CodegenGetValue(
        codegen, entry_block, ret_block, not_null_block);
  }

  builder.SetInsertPoint(not_null_block);
  result = builder.CreateCall(external_function, args, "tmp_" + prototype->name());
  builder.CreateBr(ret_block);

  builder.SetInsertPoint(ret_block);
  PHINode* phi_node = builder.CreatePHI(return_type, 2, "tmp_phi");
  phi_node->addIncoming(GetNullReturnValue(codegen), entry_block);
  phi_node->addIncoming(result, not_null_block);
  builder.CreateRet(phi_node);

  if (!codegen->VerifyFunction(function)) return NULL;
  return function;
}

}
