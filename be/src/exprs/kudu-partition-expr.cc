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

#include "exprs/kudu-partition-expr.h"

#include <kudu/common/partial_row.h>

#include "codegen/codegen-anyval.h"
#include "codegen/llvm-codegen.h"
#include "exec/kudu/kudu-util.h"
#include "exprs/scalar-expr-evaluator.h"
#include "gutil/strings/substitute.h"
#include "runtime/exec-env.h"
#include "runtime/fragment-state.h"
#include "runtime/runtime-state.h"
#include "runtime/tuple-row.h"
#include "runtime/tuple.h"
#include "runtime/types.h"

#include "common/names.h"

namespace impala {

KuduPartitionExpr::KuduPartitionExpr(const TExprNode& node)
  : ScalarExpr(node), tkudu_partition_expr_(node.kudu_partition_expr) {}

Status KuduPartitionExpr::Init(
    const RowDescriptor& row_desc, bool is_entry_point, FragmentState* state) {
  RETURN_IF_ERROR(ScalarExpr::Init(row_desc, is_entry_point, state));
  DCHECK_EQ(tkudu_partition_expr_.referenced_columns.size(), children_.size());

  // Create the KuduPartitioner we'll use to get the partition index for each row.
  TableDescriptor* table_desc =
      state->desc_tbl().GetTableDescriptor(tkudu_partition_expr_.target_table_id);
  DCHECK(table_desc != nullptr);
  DCHECK(dynamic_cast<KuduTableDescriptor*>(table_desc))
      << "Target table for KuduPartitioner must be a Kudu table.";
  table_desc_ = static_cast<KuduTableDescriptor*>(table_desc);
  RETURN_IF_ERROR(ExecEnv::GetInstance()->GetKuduClient(
      table_desc_->kudu_master_addresses(), &client_));
  KUDU_RETURN_IF_ERROR(client_->OpenTable(table_desc_->table_name(), &table_),
      "Failed to open Kudu table.");
  return Status::OK();
}

Status KuduPartitionExpr::OpenEvaluator(FunctionContext::FunctionStateScope scope,
    RuntimeState* state, ScalarExprEvaluator* eval) const {
  RETURN_IF_ERROR(ScalarExpr::OpenEvaluator(scope, state, eval));

  DCHECK_GE(fn_ctx_idx_, 0);
  FunctionContext* fn_ctx = eval->fn_context(fn_ctx_idx_);
  kudu::client::KuduPartitionerBuilder b(table_);
  kudu::client::KuduPartitioner* partitioner;
  KUDU_RETURN_IF_ERROR(b.Build(&partitioner), "Failed to build Kudu partitioner.");

  // Ownership of the KuduPartitioner and PartialRow transfer to the function state.
  // They are destroyed in CloseEvaluator().
  fn_ctx->SetFunctionState(FunctionContext::THREAD_LOCAL,
      new KuduPartitionExprCtx{unique_ptr<kudu::client::KuduPartitioner>(partitioner),
          unique_ptr<kudu::KuduPartialRow>(table_->schema().NewRow())});
  return Status::OK();
}

void KuduPartitionExpr::CloseEvaluator(FunctionContext::FunctionStateScope scope,
    RuntimeState* state, ScalarExprEvaluator* eval) const {
  FunctionContext* fn_ctx = eval->fn_context(fn_ctx_idx_);
  KuduPartitionExprCtx* ctx = reinterpret_cast<KuduPartitionExprCtx*>(
      fn_ctx->GetFunctionState(FunctionContext::THREAD_LOCAL));
  // The destructor of KuduPartitionExprCtx handles all cleanup. Note that 'ctx' may be
  // NULL, in which case this is a no-op.
  delete ctx;
  ScalarExpr::CloseEvaluator(scope, state, eval);
}

IntVal KuduPartitionExpr::GetIntValInterpreted(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  FunctionContext* fn_ctx = eval->fn_context(fn_ctx_idx_);
  KuduPartitionExprCtx* ctx = reinterpret_cast<KuduPartitionExprCtx*>(
      fn_ctx->GetFunctionState(FunctionContext::THREAD_LOCAL));
  DCHECK(ctx != nullptr);
  kudu::KuduPartialRow* kudu_row = ctx->row.get();
  for (int i = 0; i < children_.size(); ++i) {
    void* val = eval->GetValue(*GetChild(i), row);
    if (val == NULL) {
      // We don't currently support nullable partition columns, but pass it along and let
      // the KuduTableSink generate the error message.
      return IntVal(-1);
    }
    int col = tkudu_partition_expr_.referenced_columns[i];
    const ColumnDescriptor& col_desc = table_desc_->col_descs()[col];
    const ColumnType& type = col_desc.type();
    DCHECK_EQ(GetChild(i)->type().type, type.type);
    Status s = WriteKuduValue(col, type, val, false, kudu_row);
    // This can only fail if we set a col to an incorect type, which would be a bug in
    // planning, so we can DCHECK.
    DCHECK(s.ok()) << "WriteKuduValue failed for col = " << col_desc.name()
                   << " and type = " << col_desc.type() << ": " << s.GetDetail();
  }

  int32_t kudu_partition = -1;
  kudu::Status s = ctx->partitioner->PartitionRow(*kudu_row, &kudu_partition);
  // This can only fail if we fail to supply some of the partition cols, which would be a
  // bug in planning, so we can DCHECK.
  DCHECK(s.ok()) << "KuduPartitioner::PartitionRow failed on row = '"
                 << kudu_row->ToString() << "': " << s.ToString();
  return IntVal(kudu_partition);
}

/// Codegens code that retrieves the KuduPartialRow and KuduPartitioner pointers stored in
/// the function context. Sets *kudu_row_ptr' and '*kudu_partitioner_ptr' to point to the
/// 'llvm::Value's that represent these pointers.
void CodegenGetKuduPartialRowAndPartitioner(LlvmCodeGen* codegen, LlvmBuilder* builder,
    llvm::Value* eval, int fn_ctx_idx,
    llvm::Value** kudu_row_ptr, llvm::Value** kudu_partitioner_ptr) {
  llvm::Type* const kudu_row_ptr_type =
      codegen->GetNamedPtrType("class.kudu::KuduPartialRow");
  llvm::Value* const kudu_row_ptr_ptr = codegen->CreateEntryBlockAlloca(
      *builder, kudu_row_ptr_type, "kudu_row_ptr_ptr");

  llvm::Type* const kudu_partitioner_ptr_type =
      codegen->GetNamedPtrType("class.kudu::client::KuduPartitioner");
  llvm::Value* const kudu_partitioner_ptr_ptr = codegen->CreateEntryBlockAlloca(*builder,
      kudu_partitioner_ptr_type, "kudu_partitioner_ptr_ptr");

  llvm::Function* const set_kudu_partial_row_and_partitioner_fn =
      codegen->GetFunction(IRFunction::SET_KUDU_PARTIAL_ROW_AND_PARTITIONER, false);
  builder->CreateCall(set_kudu_partial_row_and_partitioner_fn,
      {eval, codegen->GetI32Constant(fn_ctx_idx),
      kudu_row_ptr_ptr, kudu_partitioner_ptr_ptr});

  *kudu_row_ptr = builder->CreateLoad(kudu_row_ptr_ptr, "kudu_row_ptr");
  *kudu_partitioner_ptr = builder->CreateLoad(
      kudu_partitioner_ptr_ptr, "kudu_partitioner_ptr");
}

void CodegenCallWriteKuduValue(LlvmCodeGen* codegen, LlvmBuilder* builder, int col,
    const ColumnType& type, llvm::Value* kudu_row_ptr, llvm::Value* child_native_val) {
  llvm::Function* const write_kudu_fn =
      codegen->GetFunction(IRFunction::WRITE_KUDU_VALUE, false);

  llvm::Type* const status_type = codegen->GetNamedType(Status::LLVM_CLASS_NAME);
  llvm::Value* const status_ptr = codegen->CreateEntryBlockAlloca(*builder,
      status_type, "status_ptr");

  llvm::Value* const col_type_ptr = codegen->GetPtrTo(builder, type.ToIR(codegen));
  llvm::Value* const child_i8 = builder->CreateBitCast(
      child_native_val, codegen->i8_type()->getPointerTo());

  // This can only fail if we set a col to an incorrect type, which would be a bug in
  // planning, so we could DCHECK but in codegen code we can't so we do not check it.
  builder->CreateCall(write_kudu_fn,
      {status_ptr, codegen->GetI32Constant(col), col_type_ptr,
      child_i8, codegen->GetBoolConstant(false), kudu_row_ptr});
}

/// Sample IR:
///
/// To reproduce, run
///
/// bin/impala-py.test tests/query_test/test_kudu.py::
/// TestKuduPartitioning::test_partitions_evenly_distributed
///
/// define i64 @KuduPartitionExpr(%"class.impala::ScalarExprEvaluator"* %eval,
///                               %"class.impala::TupleRow"* %row) #47 {
/// entry:
///   %0 = alloca %"struct.impala::ColumnType"
///   %status_ptr = alloca %"class.impala::Status"
///   %1 = alloca i32
///   %kudu_partitioner_ptr_ptr = alloca %"class.kudu::client::KuduPartitioner"*
///   %kudu_row_ptr_ptr = alloca %"class.kudu::KuduPartialRow"*
///   ; The next two lines should be one line but the name of the identifier is too long.
///   call void @_ZN6impala17KuduPartitionExpr31SetKuduPartialRowAndPartitionerEPNS_19
///ScalarExprEvaluatorEiPPN4kudu14KuduPartialRowEPPNS3_6client15KuduPartitionerE(
///       %"class.impala::ScalarExprEvaluator"* %eval,
///       i32 0,
///       %"class.kudu::KuduPartialRow"** %kudu_row_ptr_ptr,
///       %"class.kudu::client::KuduPartitioner"** %kudu_partitioner_ptr_ptr)
///   %kudu_row_ptr = load %"class.kudu::KuduPartialRow"*,
///                        %"class.kudu::KuduPartialRow"** %kudu_row_ptr_ptr
///   %kudu_partitioner_ptr = load %"class.kudu::client::KuduPartitioner"*,
///                                %"class.kudu::client::KuduPartitioner"**
///                                    %kudu_partitioner_ptr_ptr
///   br label %eval_child
///
/// eval_child:                                       ; preds = %entry
///   %child = call i64 @GetSlotRef.4(%"class.impala::ScalarExprEvaluator"* %eval,
///                                   %"class.impala::TupleRow"* %row)
///   br label %entry1
///
/// entry1:                                           ; preds = %eval_child
///   %is_null = trunc i64 %child to i1
///   br i1 %is_null, label %null, label %non_null
///
/// non_null:                                         ; preds = %entry1
///   %2 = ashr i64 %child, 32
///   %3 = trunc i64 %2 to i32
///   store i32 %3, i32* %1
///   store %"struct.impala::ColumnType" {
///       i32 5, i32 -1, i32 -1, i32 -1,
///       %"class.std::vector.13" zeroinitializer,
///       %"class.std::vector.18" zeroinitializer,
///       %"class.std::vector.23" zeroinitializer },
///       %"struct.impala::ColumnType"* %0
///   %4 = bitcast i32* %1 to i8*
///   call void
///       @_ZN6impala14WriteKuduValueEiRKNS_10ColumnTypeEPKvbPN4kudu14KuduPartialRowE(
///           %"class.impala::Status"* %status_ptr,
///           i32 0,
///           %"struct.impala::ColumnType"* %0,
///           i8* %4,
///           i1 false,
///           %"class.kudu::KuduPartialRow"* %kudu_row_ptr)
///   br label %partition_block
///
/// null:                                             ; preds = %entry1
///   ret i64 -4294967296
///
/// partition_block:                                  ; preds = %non_null
///   ; The next two lines should be one line but the name of the identifier is too long.
///   %ret_val = call i64 @_ZN6impala19GetKuduPartitionRowEPN4kudu6client15
///KuduPartitionerEPNS0_14KuduPartialRowE(
///       %"class.kudu::client::KuduPartitioner"* %kudu_partitioner_ptr,
///       %"class.kudu::KuduPartialRow"* %kudu_row_ptr)
///   ret i64 %ret_val
/// }
Status KuduPartitionExpr::GetCodegendComputeFnImpl(
    LlvmCodeGen* codegen, llvm::Function** fn) {
  llvm::Function* const kudu_partition_row_fn =
      codegen->GetFunction(IRFunction::GET_KUDU_PARTITION_ROW, false);

  // Function prototype.
  llvm::LLVMContext& context = codegen->context();
  LlvmBuilder builder(context);

  // Parameters of the generated function (ScalarExprEvaluator*, TupleRow*).
  llvm::Value* args[2];
  llvm::Function* const function =
      CreateIrFunctionPrototype("KuduPartitionExpr", codegen, &args);

  // Codegen the initialisation of function context etc.
  llvm::BasicBlock* const entry_block =
      llvm::BasicBlock::Create(context, "entry", function);
  builder.SetInsertPoint(entry_block);

  llvm::Value* kudu_row_ptr = nullptr;
  llvm::Value* kudu_partitioner_ptr = nullptr;
  CodegenGetKuduPartialRowAndPartitioner(codegen, &builder, args[0], fn_ctx_idx_,
      &kudu_row_ptr, &kudu_partitioner_ptr);

  llvm::BasicBlock* current_eval_child_block = llvm::BasicBlock::Create(
      context, "eval_child", function);
  builder.CreateBr(current_eval_child_block);

  const int num_children = GetNumChildren();
  for (int i = 0; i < num_children; ++i) {
    ScalarExpr* const child_expr = GetChild(i);
    llvm::Function* child_fn = nullptr;
    RETURN_IF_ERROR(child_expr->GetCodegendComputeFn(codegen, false, &child_fn));

    builder.SetInsertPoint(current_eval_child_block);
    const ColumnType& child_type = child_expr->type();
    CodegenAnyVal child_wrapped = CodegenAnyVal::CreateCallWrapped(
        codegen, &builder, child_type, child_fn, {args[0], args[1]}, "child");

    CodegenAnyValReadWriteInfo rwi = child_wrapped.ToReadWriteInfo();
    rwi.entry_block().BranchTo(&builder);

    // Child is null.
    builder.SetInsertPoint(rwi.null_block());
    CodegenAnyVal error_ret_val =
        CodegenAnyVal::GetNonNullVal(codegen, &builder, type(), "error_ret_val");
    error_ret_val.SetVal(-1);
    builder.CreateRet(error_ret_val.GetLoweredValue());

    // Child is not null.
    builder.SetInsertPoint(rwi.non_null_block());
    const int col = tkudu_partition_expr_.referenced_columns[i];
    const ColumnDescriptor& col_desc = table_desc_->col_descs()[col];
    const ColumnType& type = col_desc.type();
    DCHECK_EQ(child_expr->type().type, type.type);

    llvm::Value* const child_native_val =
        SlotDescriptor::CodegenStoreNonNullAnyValToNewAlloca(rwi);

    CodegenCallWriteKuduValue(codegen, &builder, col, type,
        kudu_row_ptr, child_native_val);

    llvm::BasicBlock* next_eval_child_block =
        llvm::BasicBlock::Create(context, "eval_child", function);
    builder.CreateBr(next_eval_child_block);
    current_eval_child_block = next_eval_child_block;
  }

  llvm::BasicBlock* const partition_block = current_eval_child_block;
  partition_block->setName("partition_block");
  builder.SetInsertPoint(partition_block);
  llvm::Value* const ret_val = builder.CreateCall(kudu_partition_row_fn,
      {kudu_partitioner_ptr, kudu_row_ptr}, "ret_val");
  builder.CreateRet(ret_val);

  *fn = codegen->FinalizeFunction(function);
  if (UNLIKELY(*fn == nullptr)) {
    return Status(TErrorCode::IR_VERIFY_FAILED, "KuduPartitionExpr");
  }

  return Status::OK();
}

} // namespace impala
