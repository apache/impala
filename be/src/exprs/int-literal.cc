// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "int-literal.h"

#include <glog/logging.h>
#include <sstream>

#include "codegen/llvm-codegen.h"
#include "gen-cpp/Exprs_types.h"

using namespace std;
using namespace llvm;

namespace impala {

IntLiteral::IntLiteral(PrimitiveType type, void* value)
  : Expr(type) {
  DCHECK(value != NULL);
  switch (type_) {
    case TYPE_TINYINT:
      result_.tinyint_val = *reinterpret_cast<int8_t*>(value);
      break;
    case TYPE_SMALLINT:
      result_.smallint_val = *reinterpret_cast<int16_t*>(value);
      break;
    case TYPE_INT:
      result_.int_val = *reinterpret_cast<int32_t*>(value);
      break;
    case TYPE_BIGINT:
      result_.bigint_val = *reinterpret_cast<int64_t*>(value);
      break;
    default:
      DCHECK(false) << "IntLiteral ctor: bad type: " << TypeToString(type_);
  }
}

IntLiteral::IntLiteral(const TExprNode& node)
  : Expr(node) {
  switch (type_) {
    case TYPE_TINYINT:
      result_.tinyint_val = node.int_literal.value;
      break;
    case TYPE_SMALLINT:
      result_.smallint_val = node.int_literal.value;
      break;
    case TYPE_INT:
      result_.int_val = node.int_literal.value;
      break;
    case TYPE_BIGINT:
      result_.bigint_val = node.int_literal.value;
      break;
    default:
      DCHECK(false) << "IntLiteral ctor: bad type: " << TypeToString(type_);
  }
}

void* IntLiteral::ReturnTinyintValue(Expr* e, TupleRow* row) {
  IntLiteral* l = static_cast<IntLiteral*>(e);
  return &l->result_.tinyint_val;
}

void* IntLiteral::ReturnSmallintValue(Expr* e, TupleRow* row) {
  IntLiteral* l = static_cast<IntLiteral*>(e);
  return &l->result_.smallint_val;
}

void* IntLiteral::ReturnIntValue(Expr* e, TupleRow* row) {
  IntLiteral* l = static_cast<IntLiteral*>(e);
  return &l->result_.int_val;
}

void* IntLiteral::ReturnBigintValue(Expr* e, TupleRow* row) {
  IntLiteral* l = static_cast<IntLiteral*>(e);
  return &l->result_.bigint_val;
}

Status IntLiteral::Prepare(RuntimeState* state, const RowDescriptor& row_desc) {
  DCHECK_EQ(children_.size(), 0);
  switch (type_) {
    case TYPE_TINYINT:
      compute_fn_ = ReturnTinyintValue;
      break;
    case TYPE_SMALLINT:
      compute_fn_ = ReturnSmallintValue;
      break;
    case TYPE_INT:
      compute_fn_ = ReturnIntValue;
      break;
    case TYPE_BIGINT:
      compute_fn_ = ReturnBigintValue;
      break;
    default:
      DCHECK(false) << "IntLiteral::Prepare(): bad type: " << TypeToString(type_);
  }
  return Status::OK;
}

string IntLiteral::DebugString() const {
  stringstream out;
  out << "IntLiteral(value=";
  switch (type_) {
    case TYPE_TINYINT:
      out << static_cast<int>(result_.tinyint_val); // don't print it out as a char
      break;
    case TYPE_SMALLINT:
      out << result_.smallint_val;
      break;
    case TYPE_INT:
      out << result_.int_val;
      break;
    case TYPE_BIGINT:
      out << result_.bigint_val;
      break;
    default:
      DCHECK(false) << "IntLiteral::Prepare(): bad type: " << TypeToString(type_);
  }
  out << ")";
  return out.str();
}

// LLVM IR generation for int literals.  Resulting IR looks like:
//
// define i8 @IntLiteral1(i8** %row, i8* %state_data, i1* %is_null) {
// entry:
//   store i1 false, i1* %is_null
//   ret i8 2
// }
Function* IntLiteral::Codegen(LlvmCodeGen* codegen) {
  DCHECK_EQ(GetNumChildren(), 0);
  LLVMContext& context = codegen->context();
  LlvmCodeGen::LlvmBuilder builder(context);

  Function* function = CreateComputeFnPrototype(codegen, "IntLiteral");
  BasicBlock* entry_block = BasicBlock::Create(context, "entry", function);
  
  builder.SetInsertPoint(entry_block);
  Value* result = NULL;
  switch (type()) {
    case TYPE_TINYINT:
      result = ConstantInt::get(context, APInt(8, result_.tinyint_val, true));
      break;
    case TYPE_SMALLINT:
      result = ConstantInt::get(context, APInt(16, result_.smallint_val, true));
      break;
    case TYPE_INT:
      result = ConstantInt::get(context, APInt(32, result_.int_val, true));
      break;
    case TYPE_BIGINT:
      result = ConstantInt::get(context, APInt(64, result_.bigint_val, true));
      break;
    default:
      DCHECK(false) << "Invalid type.";
      return NULL;
  }
  
  CodegenSetIsNullArg(codegen, entry_block, false);
  builder.CreateRet(result);

  return codegen->FinalizeFunction(function);
}

}

