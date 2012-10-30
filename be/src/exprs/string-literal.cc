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

#include "string-literal.h"

#include <sstream>

#include "codegen/llvm-codegen.h"
#include "gen-cpp/Exprs_types.h"

using namespace llvm;
using namespace std;

namespace impala {

StringLiteral::StringLiteral(const StringValue& str) 
  : Expr(TYPE_STRING) {
  result_.SetStringVal(str);
}

StringLiteral::StringLiteral(const string& str) 
  : Expr(TYPE_STRING) {
  result_.SetStringVal(str);
}

StringLiteral::StringLiteral(const TExprNode& node)
  : Expr(node) {
  result_.SetStringVal(node.string_literal.value);
}

void* StringLiteral::ComputeFn(Expr* e, TupleRow* row) {
  StringLiteral* l = static_cast<StringLiteral*>(e);
  return &l->result_.string_val;
}

Status StringLiteral::Prepare(RuntimeState* state, const RowDescriptor& row_desc) {
  DCHECK_EQ(children_.size(), 0);
  compute_fn_ = ComputeFn;
  return Status::OK;
}

string StringLiteral::DebugString() const {
  stringstream out;
  out << "StringLiteral(value=" << result_.string_data << ")";
  return out.str();
}

// LLVM IR generation for string literals.  Resulting IR looks like:
// define %StringValue* @StringLiteral(i8** %row, i8* %state_data, i1* %is_null) {
// entry:
//   store i1 false, i1* %is_null
//   ; constant that is the address of the stringvalue.
//   ret %StringValue* inttoptr (i64 37529792 to %StringValue*)
// }
Function* StringLiteral::Codegen(LlvmCodeGen* codegen) {
  DCHECK_EQ(GetNumChildren(), 0);
  LLVMContext& context = codegen->context();
  LlvmCodeGen::LlvmBuilder builder(context);

  Function* function = CreateComputeFnPrototype(codegen, "StringLiteral");
  BasicBlock* entry_block = BasicBlock::Create(context, "entry", function);
  builder.SetInsertPoint(entry_block);
  
  Type* ptr_type = codegen->GetPtrType(TYPE_STRING);
  Value* str_val_ptr = codegen->CastPtrToLlvmPtr(ptr_type, &result_.string_val);
  CodegenSetIsNullArg(codegen, entry_block, false);
  builder.CreateRet(str_val_ptr);

  return codegen->FinalizeFunction(function);
}

}

