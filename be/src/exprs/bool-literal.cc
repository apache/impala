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

#include "bool-literal.h"

#include <sstream>

#include "codegen/llvm-codegen.h"
#include "gen-cpp/Exprs_types.h"

using namespace std;
using namespace llvm;

namespace impala {

BoolLiteral::BoolLiteral(bool b) 
  : Expr(TYPE_BOOLEAN) {
    result_.bool_val = b;
}

BoolLiteral::BoolLiteral(const TExprNode& node)
  : Expr(node) {
  result_.bool_val = node.bool_literal.value;
}


void* BoolLiteral::ReturnValue(Expr* e, TupleRow* row) {
  BoolLiteral* l = static_cast<BoolLiteral*>(e);
  return &l->result_.bool_val;
}

Status BoolLiteral::Prepare(RuntimeState* state, const RowDescriptor& row_desc) {
  DCHECK_EQ(children_.size(), 0);
  compute_fn_ = ReturnValue;
  return Status::OK;
}

string BoolLiteral::DebugString() const {
  stringstream out;
  out << "BoolLiteral(value=" << result_.bool_val << ")";
  return out.str();
}

// IR generation for BoolLiteral.  Resulting IR looks like:
// define i1 @BoolLiteral(i8** %row, i8* %state_data, i1* %is_null) {
// entry:
//   store i1 false, i1* %is_null
//   ret i1 true
// }
Function* BoolLiteral::Codegen(LlvmCodeGen* codegen) {
  DCHECK_EQ(GetNumChildren(), 0);
  LLVMContext& context = codegen->context();
  LlvmCodeGen::LlvmBuilder builder(context);

  Function* function = CreateComputeFnPrototype(codegen, "BoolLiteral");
  BasicBlock* entry_block = BasicBlock::Create(context, "entry", function);

  builder.SetInsertPoint(entry_block);
  CodegenSetIsNullArg(codegen, entry_block, false);
  builder.CreateRet(ConstantInt::get(context, APInt(1, result_.bool_val, true)));
  
  return codegen->FinalizeFunction(function);
}

}

