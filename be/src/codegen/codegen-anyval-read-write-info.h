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

#pragma once

#include <string>
#include <vector>
#include <variant>

#include "common/logging.h"

namespace llvm {
class ConstantStruct;
class BasicBlock;
class Function;
class LLVMContext;
class PHINode;
class Value;
}

namespace impala {

struct ColumnType;
class LlvmBuilder;
class LlvmCodeGen;

/// This class wraps an 'llvm::BasicBlock*' and provides a const interface to it extended
/// with the possibility of branching to it (either conditionally or not) and creating
/// blocks before it.
/// This is useful for example for the entry blocks of 'CodegenAnyValReadWriteInfo'
/// objects as we do not want these blocks to be writable but we want to be able to branch
/// to them.
///
/// We cannot use a simple const pointer because the branching functions
/// 'LlvmBuilder::Create[Cond]Br()' and 'llvm::BasicBlock::Create()' take a non-const
/// pointer.
class NonWritableBasicBlock {
 public:
  explicit NonWritableBasicBlock(llvm::BasicBlock* basic_block)
    : basic_block_(basic_block)
  {}

  const llvm::BasicBlock* get() { return basic_block_; }

  void BranchTo(LlvmBuilder* builder) const;

  /// Branch to this basic block if 'condition' is true, otherwise branch to 'else_block'.
  void BranchToIf(LlvmBuilder* builder, llvm::Value* condition,
      const NonWritableBasicBlock& else_block) const;

  /// Branch to this basic block if 'condition' if false, otherwise branch to
  /// 'then_block'.
  void BranchToIfNot(LlvmBuilder* builder, llvm::Value* condition,
      const NonWritableBasicBlock& then_block) const;

  /// Create a basic block that is inserted before this basic block.
  llvm::BasicBlock* CreateBasicBlockBefore(llvm::LLVMContext& context,
      const std::string& name = "", llvm::Function* fn = nullptr) const;
 private:
  llvm::BasicBlock* basic_block_;
};


/// This class is used in conversions to and from 'CodegenAnyVal', i.e. 'AnyVal' objects
/// in codegen code. This class is an interface between sources and destinations: sources
/// generate an instance of this class and destinations take that instance and use it to
/// write the value.
///
/// The other side can for example be tuples from which we read (in the
/// case of 'SlotRef'), tuples we write into (in case of materialisation, see
/// Tuple::CodegenMaterializeExprs()) but other cases exist, too. The main advantage is
/// that sources do not have to know how to write their destinations, only how to read the
/// values (and vice versa). This also makes it possible, should there be need for it, to
/// leave out 'CodegenAnyVal' and convert directly between a source and a destination that
/// know how to read and write 'CodegenAnyValReadWriteInfo's.
///
/// An instance of 'CodegenAnyValReadWriteInfo' represents a value but also contains
/// information about how it is read and written in LLVM IR.
///
/// A source (for example 'SlotRef') should generate IR that starts in 'entry_block' (so
/// that other IR code can branch to it), perform NULL checking and branch to 'null_block'
/// and 'non_null_block' accordingly. The source is responsible for creating these blocks.
/// It is allowed to create more blocks, but these blocks should not be missing.
///
/// A destination should be able to rely on this structure, i.e. it should be able to
/// branch to 'entry_block' and to generate code in 'null_block' and 'non_null_block' to
/// write the value. It is also allowed to generate additional blocks but it should not
/// write into 'entry_block' or assume that the source only used the above mentioned
/// blocks.
///
/// Structs are represented recursively. The fields 'codegen', 'builder' and 'type' should
/// be filled by the source so that the destination can use them to generate IR code.
/// Other fields, such as 'fn_ctx_idx' and 'eval' may be needed in some cases but not in
/// others.
class CodegenAnyValReadWriteInfo {
 public:
  // Used for String and collection types.
  struct PtrLenStruct {
    llvm::Value* ptr = nullptr;
    llvm::Value* len = nullptr;
  };

  // Used for Timestamp.
  struct TimestampStruct {
    llvm::Value* time_of_day = nullptr;
    llvm::Value* date = nullptr;
  };

  CodegenAnyValReadWriteInfo(LlvmCodeGen* codegen, LlvmBuilder* builder,
      const ColumnType& type)
    : codegen_(codegen),
      builder_(builder),
      type_(type)
  {
    DCHECK(codegen != nullptr);
    DCHECK(builder != nullptr);
  }

  LlvmCodeGen* codegen() const { return codegen_; }
  LlvmBuilder* builder() const { return builder_; }
  const ColumnType& type() const {return type_; }

  llvm::Value* GetSimpleVal() const;
  const PtrLenStruct& GetPtrAndLen() const;
  const TimestampStruct& GetTimeAndDate() const;

  llvm::Value* GetEval() const { return eval_; }
  int GetFnCtxIdx() const { return fn_ctx_idx_; }

  NonWritableBasicBlock entry_block() const {
    return NonWritableBasicBlock(entry_block_);
  }

  llvm::BasicBlock* null_block() const { return null_block_; }
  llvm::BasicBlock* non_null_block() const { return non_null_block_; }

  // Only one setter should only be called in the lifetime of this object as changing the
  // type is not supported. The same setter can be called multiple times.
  void SetSimpleVal(llvm::Value* val);
  void SetPtrAndLen(llvm::Value* ptr, llvm::Value* len);
  void SetTimeAndDate(llvm::Value* time_of_day, llvm::Value* date);

  void SetEval(llvm::Value* eval);
  void SetFnCtxIdx(int fn_ctx_idx);

  void SetBlocks(llvm::BasicBlock* entry_block, llvm::BasicBlock* null_block,
      llvm::BasicBlock* non_null_block);

  const std::vector<CodegenAnyValReadWriteInfo>& children() const { return children_; }
  std::vector<CodegenAnyValReadWriteInfo>& children() { return children_; }

  llvm::ConstantStruct* GetIrType() const;

  llvm::Value* CodegenGetFnCtx() const;

  // See CodegenAnyVal::ConvertToCanonicalForm.
  void CodegenConvertToCanonicalForm();

  // Creates a PHI node that will have the value 'non_null_value' if the incoming block is
  // 'non_null_block_' and the value 'null_value' if it is 'null_block_'.
  llvm::PHINode* CodegenNullPhiNode(llvm::Value* non_null_value, llvm::Value* null_value,
      std::string name = "");

  // Creates a PHI node the value of which tells whether it was reached from the non-null
  // or the null path, i.e. whether this CodegenAnyValReadWriteInfo is null.
  llvm::PHINode* CodegenIsNullPhiNode(std::string name = "");

 private:
  LlvmCodeGen* const codegen_;
  LlvmBuilder* const builder_;
  const ColumnType& type_;

  // The stored data is one of the variants below.
  std::variant<
      std::monostate, // Initial state - no value has been set
      llvm::Value*,   // Simple native types
      PtrLenStruct,   // String and collection types
      TimestampStruct // Timestamp
      > data_;

  // Pointer to the ScalarExprEvaluator in LLVM code.
  llvm::Value* eval_ = nullptr;

  // Index of the FunctionContext belonging to this value, in the ScalarExprEvaluator.
  int fn_ctx_idx_ = -1;

  // The block where codegen'd code for this object begins.
  llvm::BasicBlock* entry_block_ = nullptr;

  // The block we branch to if the read value is null.
  llvm::BasicBlock* null_block_ = nullptr;

  // The block we branch to if the read value is not null.
  llvm::BasicBlock* non_null_block_ = nullptr;

  // Vector of 'CodegenAnyValReadWriteInfo's for children in case this one refers to a
  // struct.
  std::vector<CodegenAnyValReadWriteInfo> children_;

  bool is_data_initialized() const;
  bool holds_simple_val() const;
  bool holds_ptr_and_len() const;
  bool holds_timestamp() const;
};

} // namespace impala
