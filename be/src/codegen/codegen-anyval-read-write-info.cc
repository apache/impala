// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "codegen/codegen-anyval-read-write-info.h"

#include "codegen/codegen-anyval.h"
#include "codegen/llvm-codegen.h"
#include "llvm/IR/LLVMContext.h"

namespace impala {

void NonWritableBasicBlock::BranchTo(LlvmBuilder* builder) const {
  DCHECK(builder != nullptr);
  builder->CreateBr(basic_block_);
}

void NonWritableBasicBlock::BranchToIf(LlvmBuilder* builder,
    llvm::Value* condition, const NonWritableBasicBlock& else_block) const {
  DCHECK(builder != nullptr);
  builder->CreateCondBr(condition, basic_block_, else_block.basic_block_);
}

void NonWritableBasicBlock::BranchToIfNot(LlvmBuilder* builder,
    llvm::Value* condition, const NonWritableBasicBlock& then_block) const {
  DCHECK(builder != nullptr);
  builder->CreateCondBr(condition, then_block.basic_block_, basic_block_);
}

llvm::BasicBlock* NonWritableBasicBlock::CreateBasicBlockBefore(
    llvm::LLVMContext& context, const std::string& name, llvm::Function* fn) const {
  return llvm::BasicBlock::Create(context, name, fn, basic_block_);
}

llvm::Value* CodegenAnyValReadWriteInfo::GetSimpleVal() const {
  llvm::Value* const * val = std::get_if<llvm::Value*>(&data_);
  DCHECK(val != nullptr);
  return *val;
}

const CodegenAnyValReadWriteInfo::PtrLenStruct& CodegenAnyValReadWriteInfo::GetPtrAndLen()
    const {
  const PtrLenStruct* ptr_len_struct = std::get_if<PtrLenStruct>(&data_);
  DCHECK(ptr_len_struct != nullptr);
  return *ptr_len_struct;
}

const CodegenAnyValReadWriteInfo::TimestampStruct&
    CodegenAnyValReadWriteInfo::GetTimeAndDate() const {
  const TimestampStruct* timestamp_struct = std::get_if<TimestampStruct>(&data_);
  DCHECK(timestamp_struct != nullptr);
  return *timestamp_struct;
}

void CodegenAnyValReadWriteInfo::SetSimpleVal(llvm::Value* val) {
  DCHECK(val != nullptr);
  DCHECK(!is_data_initialized() || holds_simple_val());

  data_ = val;
}

void CodegenAnyValReadWriteInfo::SetPtrAndLen(llvm::Value* ptr, llvm::Value* len) {
  DCHECK(ptr != nullptr);
  DCHECK(len != nullptr);
  DCHECK(!is_data_initialized() || holds_ptr_and_len());

  PtrLenStruct val;
  val.ptr = ptr;
  val.len = len;
  data_ = val;
}

void CodegenAnyValReadWriteInfo::SetTimeAndDate(llvm::Value* time_of_day,
    llvm::Value* date) {
  DCHECK(time_of_day != nullptr);
  DCHECK(date != nullptr);
  DCHECK(!is_data_initialized() || holds_timestamp());

  TimestampStruct val;
  val.time_of_day = time_of_day;
  val.date = date;
  data_ = val;
}

void CodegenAnyValReadWriteInfo::SetEval(llvm::Value* eval) {
  DCHECK(eval != nullptr);
  eval_ = eval;
}

void CodegenAnyValReadWriteInfo::SetFnCtxIdx(int fn_ctx_idx) {
  DCHECK_GE(fn_ctx_idx, -1);
  fn_ctx_idx_ = fn_ctx_idx;
}

void CodegenAnyValReadWriteInfo::SetBlocks(llvm::BasicBlock* entry_block,
    llvm::BasicBlock* null_block, llvm::BasicBlock* non_null_block) {
  DCHECK(entry_block != nullptr);
  DCHECK(null_block != nullptr);
  DCHECK(non_null_block != nullptr);

  entry_block_ = entry_block;
  null_block_ = null_block;
  non_null_block_ = non_null_block;
}

llvm::ConstantStruct* CodegenAnyValReadWriteInfo::GetIrType() const {
  // Delete the vectors in 'type_copy' because they are not used here and because they
  // cannot be converted to IR.
  // TODO IMPALA-11643: Revisit this.
  ColumnType type_copy = type_;
  type_copy.children.clear();
  type_copy.field_names.clear();
  type_copy.field_ids.clear();
  return type_copy.ToIR(codegen_);
}

void CodegenAnyValReadWriteInfo::CodegenConvertToCanonicalForm() {
  switch(type_.type) {
    case TYPE_FLOAT:
    case TYPE_DOUBLE: {
      llvm::Value* new_val = CodegenAnyVal::ConvertToCanonicalForm(codegen_, builder_,
          type_, GetSimpleVal());
      SetSimpleVal(new_val);
      break;
    }
    default:
      ;
  }
}

llvm::Value* CodegenAnyValReadWriteInfo::CodegenGetFnCtx() const {
  llvm::Function* const get_func_ctx_fn =
      codegen_->GetFunction(IRFunction::GET_FUNCTION_CTX, false);
  return builder_->CreateCall(get_func_ctx_fn,
      {eval_, codegen_->GetI32Constant(fn_ctx_idx_)}, "fn_ctx");
}

llvm::PHINode* CodegenAnyValReadWriteInfo::CodegenNullPhiNode(llvm::Value* non_null_value,
    llvm::Value* null_value, std::string name) {
  return LlvmCodeGen::CreateBinaryPhiNode(builder_, non_null_value, null_value,
      non_null_block_, null_block_);
}

llvm::PHINode* CodegenAnyValReadWriteInfo::CodegenIsNullPhiNode(std::string name) {
  return CodegenNullPhiNode(codegen_->false_value(), codegen_->true_value());
}

bool CodegenAnyValReadWriteInfo::is_data_initialized() const {
  return std::get_if<std::monostate>(&data_) == nullptr;
}

bool CodegenAnyValReadWriteInfo::holds_simple_val() const {
  return std::get_if<llvm::Value*>(&data_) != nullptr;
}

bool CodegenAnyValReadWriteInfo::holds_ptr_and_len() const {
  return std::get_if<PtrLenStruct>(&data_) != nullptr;
}

bool CodegenAnyValReadWriteInfo::holds_timestamp() const {
  return std::get_if<TimestampStruct>(&data_) != nullptr;
}

} // namespace impala
