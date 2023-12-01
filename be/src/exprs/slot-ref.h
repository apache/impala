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

#ifndef IMPALA_EXPRS_SLOTREF_H
#define IMPALA_EXPRS_SLOTREF_H

#include "codegen/codegen-anyval.h"
#include "exprs/scalar-expr.h"
#include "runtime/descriptors.h"

namespace impala {

using impala_udf::BooleanVal;
using impala_udf::TinyIntVal;
using impala_udf::SmallIntVal;
using impala_udf::IntVal;
using impala_udf::BigIntVal;
using impala_udf::FloatVal;
using impala_udf::DoubleVal;
using impala_udf::TimestampVal;
using impala_udf::StringVal;
using impala_udf::DecimalVal;
using impala_udf::DateVal;

/// Reference to a single slot of a tuple.
class SlotRef : public ScalarExpr {
 public:
  SlotRef(const TExprNode& node);
  SlotRef(const SlotDescriptor* desc);

  /// Instantiate a SlotRef for internal use in the backend (in cases where the frontend
  /// does not generate the appropriate exprs).
  SlotRef(const SlotDescriptor* desc, const ColumnType& type);

  /// Create a SlotRef based on the given SlotDescriptor 'desc' and make sure the type is
  /// not TYPE_NULL (if so, replaced it with TYPE_BOOLEAN).
  static SlotRef* TypeSafeCreate(const SlotDescriptor* desc);

  /// Used for testing.  GetValue will return tuple + offset interpreted as 'type'
  SlotRef(const ColumnType& type, int offset, const bool nullable = false);

  /// Initialize a SlotRef that was directly constructed by backend.
  virtual Status Init(const RowDescriptor& row_desc, bool is_entry_point,
      FragmentState* state) override WARN_UNUSED_RESULT;
  virtual std::string DebugString() const override;
  virtual Status GetCodegendComputeFnImpl(
      LlvmCodeGen* codegen, llvm::Function** fn) override WARN_UNUSED_RESULT;
  virtual bool IsSlotRef() const override { return true; }
  virtual int GetSlotIds(std::vector<SlotId>* slot_ids) const override;
  const SlotId& slot_id() const { return slot_id_; }
  static const char* LLVM_CLASS_NAME;
  NullIndicatorOffset GetNullIndicatorOffset() const { return null_indicator_offset_; }
  const SlotDescriptor* GetSlotDescriptor() const { return slot_desc_; }
  int GetTupleIdx() const { return tuple_idx_; }
  int GetSlotOffset() const { return slot_offset_; }
  virtual const TupleDescriptor* GetCollectionTupleDesc() const override;

 protected:
  friend class ScalarExpr;
  friend class ScalarExprEvaluator;

  /// For struct SlotRefs we need a FunctionContext so that we can use it later for
  /// allocating memory to StructVals.
  /// If this SlotRef is not a struct then the same function in ScalarExpr is called.
  virtual void AssignFnCtxIdx(int* next_fn_ctx_idx) override;

  GENERATE_GET_VAL_INTERPRETED_OVERRIDES_FOR_ALL_SCALAR_TYPES
  virtual CollectionVal GetCollectionValInterpreted(
      ScalarExprEvaluator*, const TupleRow*) const override;
  virtual StructVal GetStructValInterpreted(
      ScalarExprEvaluator*, const TupleRow*) const override;

 private:
  CodegenAnyVal CodegenValue(LlvmCodeGen* codegen, LlvmBuilder* builder,
      llvm::Function* fn, llvm::Value* eval_ptr, llvm::Value* row_ptr,
      llvm::BasicBlock* entry_block = nullptr);
  void CodegenNullChecking(LlvmCodeGen* codegen, LlvmBuilder* builder, llvm::Function* fn,
      llvm::BasicBlock* next_block_if_null, llvm::BasicBlock* next_block_if_not_null,
      llvm::Value* tuple_ptr);

  int tuple_idx_;  // within row
  int slot_offset_;  // within tuple
  NullIndicatorOffset null_indicator_offset_;  // within tuple
  const SlotId slot_id_;
  bool tuple_is_nullable_; // true if the tuple is nullable.
  const SlotDescriptor* slot_desc_ = nullptr;

  // After the function returns, the instruction point of the LlvmBuilder will be reset to
  // where it was before the call.
  CodegenAnyValReadWriteInfo CreateCodegenAnyValReadWriteInfo(LlvmCodeGen* codegen,
      LlvmBuilder* builder,
      llvm::Function* fn,
      llvm::Value* eval_ptr,
      llvm::Value* row_ptr,
      llvm::BasicBlock* entry_block = nullptr);
  CodegenAnyValReadWriteInfo CodegenReadSlot(LlvmCodeGen* codegen, LlvmBuilder* builder,
      llvm::Value* eval_ptr, llvm::Value* row_ptr, llvm::BasicBlock* entry_block,
      llvm::BasicBlock* null_block, llvm::BasicBlock* read_slot_block,
      llvm::Value* tuple_ptr, llvm::Value* slot_offset);
};

}

#endif
