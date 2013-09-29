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

#include "float-literal.h"

#include <sstream>

#include "codegen/llvm-codegen.h"
#include "gen-cpp/Exprs_types.h"

using namespace llvm;
using namespace std;

namespace impala {

FloatLiteral::FloatLiteral(const ColumnType& type, void* value)
  : Expr(type) {
  DCHECK(value != NULL);
  switch (type_.type) {
    case TYPE_FLOAT:
      result_.float_val = *reinterpret_cast<float*>(value);
      break;
    case TYPE_DOUBLE:
      result_.double_val = *reinterpret_cast<double*>(value);
      break;
    default:
      DCHECK(false) << "FloatLiteral ctor: bad type: " << type_.DebugString();
  }
}

FloatLiteral::FloatLiteral(const TExprNode& node)
  : Expr(node) {
  switch (type_.type) {
    case TYPE_FLOAT:
      result_.float_val = node.float_literal.value;
      break;
    case TYPE_DOUBLE:
      result_.double_val = node.float_literal.value;
      break;
    default:
      DCHECK(false) << "FloatLiteral ctor: bad type: " << type_.DebugString();
  }
}

void* FloatLiteral::ReturnFloatValue(Expr* e, TupleRow* row) {
  FloatLiteral* l = static_cast<FloatLiteral*>(e);
  return &l->result_.float_val;
}

void* FloatLiteral::ReturnDoubleValue(Expr* e, TupleRow* row) {
  FloatLiteral* l = static_cast<FloatLiteral*>(e);
  return &l->result_.double_val;
}

Status FloatLiteral::Prepare(RuntimeState* state, const RowDescriptor& row_desc) {
  DCHECK_EQ(children_.size(), 0);
  switch (type()) {
    case TYPE_FLOAT:
      compute_fn_ = ReturnFloatValue;
      break;
    case TYPE_DOUBLE:
      compute_fn_ = ReturnDoubleValue;
      break;
    default:
      DCHECK(false) << "FloatLiteral::Prepare(): bad type: " << type_.DebugString();
  }
  return Status::OK;
}

string FloatLiteral::DebugString() const {
  stringstream out;
  out << "FloatLiteral(value=";
  switch (type()) {
    case TYPE_FLOAT:
      out << result_.float_val;
      break;
    case TYPE_DOUBLE:
      out << result_.double_val;
      break;
    default:
      DCHECK(false) << "FloatLiteral::DebugString(): bad type: " << type_.DebugString();
  }
  out << ")";
  return out.str();
}

// LLVM IR generation for float literals.  Resulting IR looks like:
//
// define double @FloatLiteral(i8** %row, i8* %state_data, i1* %is_null) {
// entry:
//   store i1 false, i1* %is_null
//   ret double 1.100000e+00
// }
Function* FloatLiteral::Codegen(LlvmCodeGen* codegen) {
  DCHECK_EQ(GetNumChildren(), 0);
  LLVMContext& context = codegen->context();
  LlvmCodeGen::LlvmBuilder builder(context);

  Function* function = CreateComputeFnPrototype(codegen, "FloatLiteral");
  BasicBlock* entry_block = BasicBlock::Create(context, "entry", function);

  Value* result = NULL;
  switch (type()) {
    case TYPE_FLOAT:
      result = ConstantFP::get(context, APFloat(result_.float_val));
      break;
    case TYPE_DOUBLE:
      result = ConstantFP::get(context, APFloat(result_.double_val));
      break;
    default:
      DCHECK(false) << "Invalid type.";
      return NULL;
  }

  builder.SetInsertPoint(entry_block);
  CodegenSetIsNullArg(codegen, entry_block, false);
  builder.CreateRet(result);

  return codegen->FinalizeFunction(function);
}

}
