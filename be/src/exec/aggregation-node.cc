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

#include "exec/aggregation-node.h"

#include <math.h>
#include <sstream>
#include <boost/functional/hash.hpp>
#include <thrift/protocol/TDebugProtocol.h>

#include <x86intrin.h>

#include "codegen/codegen-anyval.h"
#include "codegen/llvm-codegen.h"
#include "exec/old-hash-table.inline.h"
#include "exprs/agg-fn-evaluator.h"
#include "exprs/expr.h"
#include "exprs/expr-context.h"
#include "exprs/slot-ref.h"
#include "runtime/descriptors.h"
#include "runtime/mem-pool.h"
#include "runtime/raw-value.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/string-value.inline.h"
#include "runtime/tuple.h"
#include "runtime/tuple-row.h"
#include "udf/udf-internal.h"
#include "util/debug-util.h"
#include "util/runtime-profile-counters.h"

#include "gen-cpp/Exprs_types.h"
#include "gen-cpp/PlanNodes_types.h"

#include "common/names.h"

using namespace impala;
using namespace llvm;

namespace impala {

const char* AggregationNode::LLVM_CLASS_NAME = "class.impala::AggregationNode";

// TODO: pass in maximum size; enforce by setting limit in mempool
AggregationNode::AggregationNode(ObjectPool* pool, const TPlanNode& tnode,
                                 const DescriptorTbl& descs)
  : ExecNode(pool, tnode, descs),
    intermediate_tuple_id_(tnode.agg_node.intermediate_tuple_id),
    intermediate_tuple_desc_(NULL),
    output_tuple_id_(tnode.agg_node.output_tuple_id),
    output_tuple_desc_(NULL),
    singleton_intermediate_tuple_(NULL),
    codegen_process_row_batch_fn_(NULL),
    process_row_batch_fn_(NULL),
    needs_finalize_(tnode.agg_node.need_finalize),
    build_timer_(NULL),
    get_results_timer_(NULL),
    hash_table_buckets_counter_(NULL) {
}

Status AggregationNode::Init(const TPlanNode& tnode, RuntimeState* state) {
  RETURN_IF_ERROR(ExecNode::Init(tnode, state));
  RETURN_IF_ERROR(
      Expr::CreateExprTrees(pool_, tnode.agg_node.grouping_exprs, &probe_expr_ctxs_));
  for (int i = 0; i < tnode.agg_node.aggregate_functions.size(); ++i) {
    AggFnEvaluator* evaluator;
    RETURN_IF_ERROR(AggFnEvaluator::Create(
        pool_, tnode.agg_node.aggregate_functions[i], &evaluator));
    aggregate_evaluators_.push_back(evaluator);
    ExprContext* agg_expr_ctx;
    if (evaluator->input_expr_ctxs().size() == 1) {
      agg_expr_ctx = evaluator->input_expr_ctxs()[0];
    } else {
      // CodegenUpdateSlot() can only support aggregate operator with only one ExprContext
      // so it doesn't support operator such as group_concat. There are also aggregate
      // operators with no ExprContext (e.g. count(*)). In cases above, 'agg_expr_ctxs_'
      // will contain NULL for that entry.
      DCHECK(evaluator->agg_op() == AggFnEvaluator::OTHER || evaluator->is_count_star());
      agg_expr_ctx = NULL;
    }
    agg_expr_ctxs_.push_back(agg_expr_ctx);
  }
  return Status::OK();
}

Status AggregationNode::Prepare(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecNode::Prepare(state));

  tuple_pool_.reset(new MemPool(mem_tracker()));
  agg_fn_pool_.reset(new MemPool(expr_mem_tracker()));
  build_timer_ = ADD_TIMER(runtime_profile(), "BuildTime");
  get_results_timer_ = ADD_TIMER(runtime_profile(), "GetResultsTime");
  hash_table_buckets_counter_ =
      ADD_COUNTER(runtime_profile(), "BuildBuckets", TUnit::UNIT);
  hash_table_load_factor_counter_ =
      ADD_COUNTER(runtime_profile(), "LoadFactor", TUnit::DOUBLE_VALUE);

  intermediate_tuple_desc_ =
      state->desc_tbl().GetTupleDescriptor(intermediate_tuple_id_);
  output_tuple_desc_ = state->desc_tbl().GetTupleDescriptor(output_tuple_id_);
  DCHECK_EQ(intermediate_tuple_desc_->slots().size(),
      output_tuple_desc_->slots().size());
  RETURN_IF_ERROR(
      Expr::Prepare(probe_expr_ctxs_, state, child(0)->row_desc(), expr_mem_tracker()));
  AddExprCtxsToFree(probe_expr_ctxs_);

  // Construct build exprs from intermediate_agg_tuple_desc_
  for (int i = 0; i < probe_expr_ctxs_.size(); ++i) {
    SlotDescriptor* desc = intermediate_tuple_desc_->slots()[i];
    DCHECK(desc->type().type == TYPE_NULL ||
           desc->type() == probe_expr_ctxs_[i]->root()->type());
    // TODO: Generate the build exprs in the FE such that the existing logic
    // for handling NULL_TYPE works.
    // Hack to avoid TYPE_NULL SlotRefs.
    Expr* expr = desc->type().type != TYPE_NULL ?
        new SlotRef(desc) : new SlotRef(desc, TYPE_BOOLEAN);
    state->obj_pool()->Add(expr);
    build_expr_ctxs_.push_back(new ExprContext(expr));
    state->obj_pool()->Add(build_expr_ctxs_.back());
  }
  // Construct a new row desc for preparing the build exprs because neither the child's
  // nor this node's output row desc may contain the intermediate tuple, e.g.,
  // in a single-node plan with an intermediate tuple different from the output tuple.
  RowDescriptor build_row_desc(intermediate_tuple_desc_, false);
  RETURN_IF_ERROR(
      Expr::Prepare(build_expr_ctxs_, state, build_row_desc, expr_mem_tracker()));
  AddExprCtxsToFree(build_expr_ctxs_);

  int j = probe_expr_ctxs_.size();
  for (int i = 0; i < aggregate_evaluators_.size(); ++i, ++j) {
    SlotDescriptor* intermediate_slot_desc = intermediate_tuple_desc_->slots()[j];
    SlotDescriptor* output_slot_desc = output_tuple_desc_->slots()[j];
    FunctionContext* agg_fn_ctx;
    RETURN_IF_ERROR(aggregate_evaluators_[i]->Prepare(state, child(0)->row_desc(),
        intermediate_slot_desc, output_slot_desc, agg_fn_pool_.get(), &agg_fn_ctx));
    agg_fn_ctxs_.push_back(agg_fn_ctx);
    state->obj_pool()->Add(agg_fn_ctx);
  }

  DCHECK_EQ(agg_fn_ctxs_.size(), aggregate_evaluators_.size());
  // TODO: how many buckets?
  hash_tbl_.reset(new OldHashTable(state, build_expr_ctxs_, probe_expr_ctxs_,
      vector<ExprContext*>(), 1, true,  vector<bool>(build_expr_ctxs_.size(), true), id(),
      mem_tracker(), vector<RuntimeFilter*>(), true));

  AddCodegenDisabledMessage(state);
  return Status::OK();
}

void AggregationNode::Codegen(RuntimeState* state) {
  DCHECK(state->ShouldCodegen());
  ExecNode::Codegen(state);
  if (IsNodeCodegenDisabled()) return;

  bool codegen_enabled = false;
  LlvmCodeGen* codegen = state->codegen();
  DCHECK(codegen != NULL);
  Function* update_tuple_fn = CodegenUpdateTuple(codegen);
  if (update_tuple_fn != NULL) {
    codegen_process_row_batch_fn_ = CodegenProcessRowBatch(codegen, update_tuple_fn);
    if (codegen_process_row_batch_fn_ != NULL) {
      // Update to using codegen'd process row batch.
      codegen->AddFunctionToJit(codegen_process_row_batch_fn_,
          reinterpret_cast<void**>(&process_row_batch_fn_));
      codegen_enabled = true;
    }
  }
  runtime_profile()->AddCodegenMsg(codegen_enabled);
}

Status AggregationNode::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecNode::Open(state));

  RETURN_IF_ERROR(Expr::Open(probe_expr_ctxs_, state));
  RETURN_IF_ERROR(Expr::Open(build_expr_ctxs_, state));

  DCHECK_EQ(aggregate_evaluators_.size(), agg_fn_ctxs_.size());
  for (int i = 0; i < aggregate_evaluators_.size(); ++i) {
    RETURN_IF_ERROR(aggregate_evaluators_[i]->Open(state, agg_fn_ctxs_[i]));
  }

  if (probe_expr_ctxs_.empty()) {
    // Create single intermediate tuple. This must happen after
    // opening the aggregate evaluators.
    singleton_intermediate_tuple_ = ConstructIntermediateTuple();
    // Check for failures during AggFnEvaluator::Init().
    RETURN_IF_ERROR(state->GetQueryStatus());
    hash_tbl_->Insert(singleton_intermediate_tuple_);
  }

  RETURN_IF_ERROR(children_[0]->Open(state));

  RowBatch batch(children_[0]->row_desc(), state->batch_size(), mem_tracker());
  int64_t num_input_rows = 0;
  while (true) {
    bool eos;
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(QueryMaintenance(state));
    RETURN_IF_ERROR(children_[0]->GetNext(state, &batch, &eos));
    SCOPED_TIMER(build_timer_);

    if (VLOG_ROW_IS_ON) {
      for (int i = 0; i < batch.num_rows(); ++i) {
        TupleRow* row = batch.GetRow(i);
        VLOG_ROW << "input row: " << PrintRow(row, children_[0]->row_desc());
      }
    }
    if (process_row_batch_fn_ != NULL) {
      process_row_batch_fn_(this, &batch);
    } else if (probe_expr_ctxs_.empty()) {
      ProcessRowBatchNoGrouping(&batch);
    } else {
      ProcessRowBatchWithGrouping(&batch);
    }
    COUNTER_SET(hash_table_buckets_counter_, hash_tbl_->num_buckets());
    COUNTER_SET(hash_table_load_factor_counter_, hash_tbl_->load_factor());
    num_input_rows += batch.num_rows();
    // We must set output_iterator_ here, rather than outside the loop, because
    // output_iterator_ must be set if the function returns within the loop
    output_iterator_ = hash_tbl_->Begin();

    batch.Reset();
    RETURN_IF_ERROR(QueryMaintenance(state));
    if (eos) break;
  }

  // We have consumed all of the input from the child and transfered ownership of the
  // resources we need, so the child can be closed safely to release its resources.
  child(0)->Close(state);
  VLOG_FILE << "aggregated " << num_input_rows << " input rows into "
            << hash_tbl_->size() << " output rows";
  return Status::OK();
}

Status AggregationNode::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));
  SCOPED_TIMER(get_results_timer_);

  if (ReachedLimit()) {
    *eos = true;
    return Status::OK();
  }
  *eos = false;
  ExprContext** ctxs = &conjunct_ctxs_[0];
  int num_ctxs = conjunct_ctxs_.size();

  int count = 0;
  const int N = state->batch_size();
  while (!output_iterator_.AtEnd() && !row_batch->AtCapacity()) {
    // This loop can go on for a long time if the conjuncts are very selective. Do query
    // maintenance every N iterations.
    if (count++ % N == 0) {
      RETURN_IF_CANCELLED(state);
      RETURN_IF_ERROR(QueryMaintenance(state));
    }
    int row_idx = row_batch->AddRow();
    TupleRow* row = row_batch->GetRow(row_idx);
    Tuple* intermediate_tuple = output_iterator_.GetTuple();
    Tuple* output_tuple =
        FinalizeTuple(intermediate_tuple, row_batch->tuple_data_pool());
    output_iterator_.Next<false>();
    row->SetTuple(0, output_tuple);
    if (ExecNode::EvalConjuncts(ctxs, num_ctxs, row)) {
      VLOG_ROW << "output row: " << PrintRow(row, row_desc());
      row_batch->CommitLastRow();
      ++num_rows_returned_;
      if (ReachedLimit()) break;
    }
  }
  *eos = output_iterator_.AtEnd() || ReachedLimit();
  COUNTER_SET(rows_returned_counter_, num_rows_returned_);
  return Status::OK();
}

Status AggregationNode::Reset(RuntimeState* state) {
  DCHECK(false) << "NYI";
  return Status("NYI");
}

void AggregationNode::Close(RuntimeState* state) {
  if (is_closed()) return;

  // Iterate through the remaining rows in the hash table and call Serialize/Finalize on
  // them in order to free any memory allocated by UDAs. Finalize() requires a dst tuple
  // but we don't actually need the result, so allocate a single dummy tuple to avoid
  // accumulating memory.
  Tuple* dummy_dst = NULL;
  if (needs_finalize_ && output_tuple_desc_ != NULL) {
    dummy_dst = Tuple::Create(output_tuple_desc_->byte_size(), tuple_pool_.get());
  }
  while (!output_iterator_.AtEnd()) {
    Tuple* tuple = output_iterator_.GetTuple();
    if (needs_finalize_) {
      AggFnEvaluator::Finalize(aggregate_evaluators_, agg_fn_ctxs_, tuple, dummy_dst);
    } else {
      AggFnEvaluator::Serialize(aggregate_evaluators_, agg_fn_ctxs_, tuple);
    }
    output_iterator_.Next<false>();
  }

  if (tuple_pool_.get() != NULL) tuple_pool_->FreeAll();
  if (hash_tbl_.get() != NULL) hash_tbl_->Close();

  agg_expr_ctxs_.clear();
  for (AggFnEvaluator* aggregate_evaluator : aggregate_evaluators_) {
    aggregate_evaluator->Close(state);
  }
  for (FunctionContext* agg_fn_ctx : agg_fn_ctxs_) {
    agg_fn_ctx->impl()->Close();
  }
  if (agg_fn_pool_.get() != NULL) agg_fn_pool_->FreeAll();

  Expr::Close(probe_expr_ctxs_, state);
  Expr::Close(build_expr_ctxs_, state);
  ExecNode::Close(state);
}

Tuple* AggregationNode::ConstructIntermediateTuple() {
  Tuple* intermediate_tuple = Tuple::Create(
      intermediate_tuple_desc_->byte_size(), tuple_pool_.get());
  vector<SlotDescriptor*>::const_iterator slot_desc =
      intermediate_tuple_desc_->slots().begin();

  // copy grouping values
  for (int i = 0; i < probe_expr_ctxs_.size(); ++i, ++slot_desc) {
    if (hash_tbl_->last_expr_value_null(i)) {
      intermediate_tuple->SetNull((*slot_desc)->null_indicator_offset());
    } else {
      void* src = hash_tbl_->last_expr_value(i);
      void* dst = intermediate_tuple->GetSlot((*slot_desc)->tuple_offset());
      RawValue::Write(src, dst, (*slot_desc)->type(), tuple_pool_.get());
    }
  }

  // Initialize aggregate output.
  for (int i = 0; i < aggregate_evaluators_.size(); ++i, ++slot_desc) {
    AggFnEvaluator* evaluator = aggregate_evaluators_[i];
    evaluator->Init(agg_fn_ctxs_[i], intermediate_tuple);
    // Codegen specific path.
    // To minimize branching on the UpdateTuple path, initialize the result value
    // so that UpdateTuple doesn't have to check if the aggregation
    // dst slot is null.
    //  - sum/count: 0
    //  - min: max_value
    //  - max: min_value
    // TODO: remove when we don't use the irbuilder for codegen here.
    // This optimization no longer applies with AnyVal
    if ((*slot_desc)->type().type != TYPE_STRING &&
        (*slot_desc)->type().type != TYPE_VARCHAR &&
        (*slot_desc)->type().type != TYPE_TIMESTAMP &&
        (*slot_desc)->type().type != TYPE_CHAR &&
        (*slot_desc)->type().type != TYPE_DECIMAL) {
      ExprValue default_value;
      void* default_value_ptr = NULL;
      switch (evaluator->agg_op()) {
        case AggFnEvaluator::MIN:
          default_value_ptr = default_value.SetToMax((*slot_desc)->type());
          RawValue::Write(default_value_ptr, intermediate_tuple, *slot_desc, NULL);
          break;
        case AggFnEvaluator::MAX:
          default_value_ptr = default_value.SetToMin((*slot_desc)->type());
          RawValue::Write(default_value_ptr, intermediate_tuple, *slot_desc, NULL);
          break;
        default:
          break;
      }
    }
  }
  return intermediate_tuple;
}

void AggregationNode::UpdateTuple(Tuple* tuple, TupleRow* row) {
  DCHECK(tuple != NULL || aggregate_evaluators_.empty());
  AggFnEvaluator::Add(aggregate_evaluators_, agg_fn_ctxs_, row, tuple);
}

Tuple* AggregationNode::FinalizeTuple(Tuple* tuple, MemPool* pool) {
  DCHECK(tuple != NULL || aggregate_evaluators_.empty());
  DCHECK(output_tuple_desc_ != NULL);

  Tuple* dst = tuple;
  if (needs_finalize_ && intermediate_tuple_id_ != output_tuple_id_) {
    dst = Tuple::Create(output_tuple_desc_->byte_size(), pool);
  }
  if (needs_finalize_) {
    AggFnEvaluator::Finalize(aggregate_evaluators_, agg_fn_ctxs_, tuple, dst);
  } else {
    AggFnEvaluator::Serialize(aggregate_evaluators_, agg_fn_ctxs_, tuple);
  }
  // Copy grouping values from tuple to dst.
  // TODO: Codegen this.
  if (dst != tuple) {
    int num_grouping_slots = probe_expr_ctxs_.size();
    for (int i = 0; i < num_grouping_slots; ++i) {
      SlotDescriptor* src_slot_desc = intermediate_tuple_desc_->slots()[i];
      SlotDescriptor* dst_slot_desc = output_tuple_desc_->slots()[i];
      bool src_slot_null = tuple->IsNull(src_slot_desc->null_indicator_offset());
      void* src_slot = NULL;
      if (!src_slot_null) src_slot = tuple->GetSlot(src_slot_desc->tuple_offset());
      RawValue::Write(src_slot, dst, dst_slot_desc, NULL);
    }
  }
  return dst;
}

void AggregationNode::DebugString(int indentation_level, stringstream* out) const {
  *out << string(indentation_level * 2, ' ');
  *out << "AggregationNode("
       << "intermediate_tuple_id=" << intermediate_tuple_id_
       << " output_tuple_id=" << output_tuple_id_
       << " needs_finalize=" << needs_finalize_
       << " probe_exprs=" << Expr::DebugString(probe_expr_ctxs_)
       << " agg_exprs=" << AggFnEvaluator::DebugString(aggregate_evaluators_);
  ExecNode::DebugString(indentation_level, out);
  *out << ")";
}

IRFunction::Type GetHllUpdateFunction2(const ColumnType& type) {
  switch (type.type) {
    case TYPE_BOOLEAN: return IRFunction::HLL_UPDATE_BOOLEAN;
    case TYPE_TINYINT: return IRFunction::HLL_UPDATE_TINYINT;
    case TYPE_SMALLINT: return IRFunction::HLL_UPDATE_SMALLINT;
    case TYPE_INT: return IRFunction::HLL_UPDATE_INT;
    case TYPE_BIGINT: return IRFunction::HLL_UPDATE_BIGINT;
    case TYPE_FLOAT: return IRFunction::HLL_UPDATE_FLOAT;
    case TYPE_DOUBLE: return IRFunction::HLL_UPDATE_DOUBLE;
    case TYPE_STRING: return IRFunction::HLL_UPDATE_STRING;
    case TYPE_DECIMAL: return IRFunction::HLL_UPDATE_DECIMAL;
    default:
      DCHECK(false) << "Unsupported type: " << type;
      return IRFunction::FN_END;
  }
}

// IR Generation for updating a single aggregation slot. Signature is:
// void UpdateSlot(FunctionContext* fn_ctx, ExprContext* expr_ctx,
//     AggTuple* agg_tuple, char** row)
//
// The IR for sum(double_col) is:
// define void @UpdateSlot(%"class.impala_udf::FunctionContext"* %fn_ctx,
//                         %"class.impala::ExprContext"* %expr_ctx,
//                         { i8, [7 x i8], double }* %agg_tuple,
//                         %"class.impala::TupleRow"* %row) #34 {
// entry:
//   %src = call { i8, double } @GetSlotRef(%"class.impala::ExprContext"* %expr_ctx,
//                                          %"class.impala::TupleRow"* %row)
//   %0 = extractvalue { i8, double } %src, 0
//   %is_null = trunc i8 %0 to i1
//   br i1 %is_null, label %ret, label %src_not_null
//
// src_not_null:                                     ; preds = %entry
//   %dst_slot_ptr = getelementptr inbounds { i8, [7 x i8], double },
//       { i8, [7 x i8], double }* %agg_tuple, i32 0, i32 2
//   call void @SetNotNull({ i8, [7 x i8], double }* %agg_tuple)
//   %dst_val = load double, double* %dst_slot_ptr
//   %val = extractvalue { i8, double } %src, 1
//   %1 = fadd double %dst_val, %val
//   store double %1, double* %dst_slot_ptr
//   br label %ret
//
// ret:                                              ; preds = %src_not_null, %entry
//   ret void
// }
//
// The IR for ndv(double_col) is:
// define void @UpdateSlot(%"class.impala_udf::FunctionContext"* %fn_ctx,
//                         %"class.impala::ExprContext"* %expr_ctx,
//                         { i8, [7 x i8], %"struct.impala::StringValue" }* %agg_tuple,
//                         %"class.impala::TupleRow"* %row) #34 {
// entry:
//   %dst_lowered_ptr = alloca { i64, i8* }
//   %src_lowered_ptr = alloca { i64, i8* }
//   %src = call { i64, i8* } @GetSlotRef(%"class.impala::ExprContext"* %expr_ctx,
//                                        %"class.impala::TupleRow"* %row)
//   %0 = extractvalue { i64, i8* } %src, 0
//   %is_null = trunc i64 %0 to i1
//   br i1 %is_null, label %ret, label %src_not_null
//
// src_not_null:                                     ; preds = %entry
//   %dst_slot_ptr = getelementptr inbounds { i8, [7 x i8], %"struct.impala::StringValue" },
//       { i8, [7 x i8], %"struct.impala::StringValue" }* %agg_tuple, i32 0, i32 2
//   call void @SetNotNull({ i8, [7 x i8], %"struct.impala::StringValue" }* %agg_tuple)
//   %dst_val =
//       load %"struct.impala::StringValue", %"struct.impala::StringValue"* %dst_slot_ptr
//   store { i64, i8* } %src, { i64, i8* }* %src_lowered_ptr
//   %src_unlowered_ptr =
//       bitcast { i64, i8* }* %src_lowered_ptr to %"struct.impala_udf::StringVal"*
//   %ptr = extractvalue %"struct.impala::StringValue" %dst_val, 0
//   %dst_stringval = insertvalue { i64, i8* } zeroinitializer, i8* %ptr, 1
//   %len = extractvalue %"struct.impala::StringValue" %dst_val, 1
//   %1 = extractvalue { i64, i8* } %dst_stringval, 0
//   %2 = zext i32 %len to i64
//   %3 = shl i64 %2, 32
//   %4 = and i64 %1, 4294967295
//   %5 = or i64 %4, %3
//   %dst_stringval1 = insertvalue { i64, i8* } %dst_stringval, i64 %5, 0
//   store { i64, i8* } %dst_stringval1, { i64, i8* }* %dst_lowered_ptr
//   %dst_unlowered_ptr =
//       bitcast { i64, i8* }* %dst_lowered_ptr to %"struct.impala_udf::StringVal"*
//   call void @HllMerge(%"class.impala_udf::FunctionContext"* %fn_ctx,
//                       %"struct.impala_udf::StringVal"* %src_unlowered_ptr,
//                       %"struct.impala_udf::StringVal"* %dst_unlowered_ptr)
//   %anyval_result = load { i64, i8* }, { i64, i8* }* %dst_lowered_ptr
//   %6 = extractvalue { i64, i8* } %anyval_result, 0
//   %7 = ashr i64 %6, 32
//   %8 = trunc i64 %7 to i32
//   %9 = insertvalue %"struct.impala::StringValue" zeroinitializer, i32 %8, 1
//   %10 = extractvalue { i64, i8* } %anyval_result, 1
//   %11 = insertvalue %"struct.impala::StringValue" %9, i8* %10, 0
//   store %"struct.impala::StringValue" %11, %"struct.impala::StringValue"* %dst_slot_ptr
//   br label %ret
//
// ret:                                              ; preds = %src_not_null, %entry
//   ret void
// }
llvm::Function* AggregationNode::CodegenUpdateSlot(LlvmCodeGen* codegen,
    AggFnEvaluator* evaluator, SlotDescriptor* slot_desc) {
  // TODO: Fix this DCHECK and Init() once CodegenUpdateSlot() can handle AggFnEvaluator
  // with multiple input expressions (e.g. group_concat).
  DCHECK_EQ(evaluator->input_expr_ctxs().size(), 1);
  ExprContext* input_expr_ctx = evaluator->input_expr_ctxs()[0];
  Expr* input_expr = input_expr_ctx->root();
  // TODO: implement timestamp
  if (input_expr->type().type == TYPE_TIMESTAMP) return NULL;
  Function* agg_expr_fn;
  Status status = input_expr->GetCodegendComputeFn(codegen, &agg_expr_fn);
  if (!status.ok()) {
    VLOG_QUERY << "Could not codegen UpdateSlot(): " << status.GetDetail();
    return NULL;
  }
  DCHECK(agg_expr_fn != NULL);

  PointerType* fn_ctx_ptr_type =
      codegen->GetPtrType(FunctionContextImpl::LLVM_FUNCTIONCONTEXT_NAME);
  PointerType* expr_ctx_ptr_type = codegen->GetPtrType(ExprContext::LLVM_CLASS_NAME);
  StructType* tuple_struct = intermediate_tuple_desc_->GetLlvmStruct(codegen);
  if (tuple_struct == NULL) {
    VLOG_QUERY << "Could not codegen UpdateSlot(): could not generate tuple struct.";
    return NULL;
  }
  PointerType* tuple_ptr_type = codegen->GetPtrType(tuple_struct);
  PointerType* tuple_row_ptr_type = codegen->GetPtrType(TupleRow::LLVM_CLASS_NAME);

  // Create UpdateSlot prototype
  LlvmCodeGen::FnPrototype prototype(codegen, "UpdateSlot", codegen->void_type());
  prototype.AddArgument(LlvmCodeGen::NamedVariable("fn_ctx", fn_ctx_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("expr_ctx", expr_ctx_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("agg_tuple", tuple_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("row", tuple_row_ptr_type));

  LlvmBuilder builder(codegen->context());
  Value* args[4];
  Function* fn = prototype.GeneratePrototype(&builder, &args[0]);
  Value* fn_ctx_arg = args[0];
  Value* expr_ctx_arg = args[1];
  Value* agg_tuple_arg = args[2];
  Value* row_arg = args[3];

  BasicBlock* src_not_null_block =
      BasicBlock::Create(codegen->context(), "src_not_null", fn);
  BasicBlock* ret_block = BasicBlock::Create(codegen->context(), "ret", fn);

  // Call expr function to get src slot value
  Value* agg_expr_fn_args[] = { expr_ctx_arg, row_arg };
  CodegenAnyVal src = CodegenAnyVal::CreateCallWrapped(
      codegen, &builder, input_expr->type(), agg_expr_fn, agg_expr_fn_args, "src");

  Value* src_is_null = src.GetIsNull();
  builder.CreateCondBr(src_is_null, ret_block, src_not_null_block);

  // Src slot is not null, update dst_slot
  builder.SetInsertPoint(src_not_null_block);
  Value* dst_ptr = builder.CreateStructGEP(NULL, agg_tuple_arg,
      slot_desc->llvm_field_idx(), "dst_slot_ptr");
  Value* result = NULL;

  if (slot_desc->is_nullable()) {
    // Dst is NULL, just update dst slot to src slot and clear null bit
    slot_desc->CodegenSetNullIndicator(
        codegen, &builder, agg_tuple_arg, codegen->false_value());
  }

  // Update the slot
  Value* dst_value = builder.CreateLoad(dst_ptr, "dst_val");
  switch (evaluator->agg_op()) {
    case AggFnEvaluator::COUNT:
      if (evaluator->is_merge()) {
        result = builder.CreateAdd(dst_value, src.GetVal(), "count_sum");
      } else {
        result = builder.CreateAdd(dst_value,
            codegen->GetIntConstant(TYPE_BIGINT, 1), "count_inc");
      }
      break;
    case AggFnEvaluator::MIN: {
      Function* min_fn = codegen->CodegenMinMax(slot_desc->type(), true);
      Value* min_args[] = { dst_value, src.GetVal() };
      result = builder.CreateCall(min_fn, min_args, "min_value");
      break;
    }
    case AggFnEvaluator::MAX: {
      Function* max_fn = codegen->CodegenMinMax(slot_desc->type(), false);
      Value* max_args[] = { dst_value, src.GetVal() };
      result = builder.CreateCall(max_fn, max_args, "max_value");
      break;
    }
    case AggFnEvaluator::SUM:
      if (slot_desc->type().type == TYPE_FLOAT || slot_desc->type().type == TYPE_DOUBLE) {
        result = builder.CreateFAdd(dst_value, src.GetVal());
      } else {
        result = builder.CreateAdd(dst_value, src.GetVal());
      }
      break;
    case AggFnEvaluator::NDV: {
      DCHECK_EQ(slot_desc->type().type, TYPE_STRING);
      IRFunction::Type ir_function_type = evaluator->is_merge() ? IRFunction::HLL_MERGE
                                          : GetHllUpdateFunction2(input_expr->type());
      Function* hll_fn = codegen->GetFunction(ir_function_type, false);

      // Create pointer to src_anyval to pass to HllUpdate() function. We must use the
      // unlowered type.
      Value* src_unlowered_ptr = src.GetUnloweredPtr("src_unlowered_ptr");

      // Create StringVal* intermediate argument from dst_value
      CodegenAnyVal dst_stringval =
          CodegenAnyVal::GetNonNullVal(codegen, &builder, TYPE_STRING, "dst_stringval");
      dst_stringval.SetFromRawValue(dst_value);
      // Create pointer to dst_stringval to pass to HllUpdate() function. We must use
      // the unlowered type.
      Value* dst_lowered_ptr = dst_stringval.GetLoweredPtr("dst_lowered_ptr");
      Type* dst_unlowered_ptr_type =
          codegen->GetPtrType(CodegenAnyVal::GetUnloweredType(codegen, TYPE_STRING));
      Value* dst_unlowered_ptr = builder.CreateBitCast(
          dst_lowered_ptr, dst_unlowered_ptr_type, "dst_unlowered_ptr");

      // Call 'hll_fn'
      builder.CreateCall(
          hll_fn, ArrayRef<Value*>({fn_ctx_arg, src_unlowered_ptr, dst_unlowered_ptr}));

      // Convert StringVal intermediate 'dst_arg' back to StringValue
      Value* anyval_result = builder.CreateLoad(dst_lowered_ptr, "anyval_result");
      result = CodegenAnyVal(codegen, &builder, TYPE_STRING, anyval_result)
               .ToNativeValue();
      break;
    }
    default:
      DCHECK(false) << "bad aggregate operator: " << evaluator->agg_op();
  }

  builder.CreateStore(result, dst_ptr);
  builder.CreateBr(ret_block);

  builder.SetInsertPoint(ret_block);
  builder.CreateRetVoid();

  return codegen->FinalizeFunction(fn);
}

// IR codegen for the UpdateTuple loop.  This loop is query specific and
// based on the aggregate functions.  The function signature must match the non-
// codegen'd UpdateTuple exactly.
// For the query:
// select count(*), count(int_col), sum(double_col) the IR looks like:
//
// ; Function Attrs: alwaysinline
// define void @UpdateTuple(%"class.impala::AggregationNode"* %this_ptr,
//                          %"class.impala::Tuple"* %agg_tuple,
//                          %"class.impala::TupleRow"* %tuple_row) #34 {
// entry:
//   %tuple =
//       bitcast %"class.impala::Tuple"* %agg_tuple to { i8, [7 x i8], i64, i64, double }*
//   %src_slot = getelementptr inbounds { i8, [7 x i8], i64, i64, double },
//       { i8, [7 x i8], i64, i64, double }* %tuple, i32 0, i32 2
//   %count_star_val = load i64, i64* %src_slot
//   %count_star_inc = add i64 %count_star_val, 1
//   store i64 %count_star_inc, i64* %src_slot
//   %0 = call %"class.impala_udf::FunctionContext"*
//       @_ZNK6impala15AggregationNode11GetAggFnCtxEi(
//           %"class.impala::AggregationNode"* %this_ptr, i32 1)
//   %1 = call %"class.impala::ExprContext"*
//       @_ZNK6impala15AggregationNode13GetAggExprCtxEi(
//           %"class.impala::AggregationNode"* %this_ptr, i32 1)
//   call void @UpdateSlot(%"class.impala_udf::FunctionContext"* %0,
//                         %"class.impala::ExprContext"* %1,
//                         { i8, [7 x i8], i64, i64, double }* %tuple,
//                         %"class.impala::TupleRow"* %tuple_row)
//   %2 = call %"class.impala_udf::FunctionContext"*
//       @_ZNK6impala15AggregationNode11GetAggFnCtxEi(
//           %"class.impala::AggregationNode"* %this_ptr, i32 2)
//   %3 = call %"class.impala::ExprContext"*
//       @_ZNK6impala15AggregationNode13GetAggExprCtxEi(
//           %"class.impala::AggregationNode"* %this_ptr, i32 2)
//   call void @UpdateSlot.3(%"class.impala_udf::FunctionContext"* %2,
//                           %"class.impala::ExprContext"* %3,
//                           { i8, [7 x i8], i64, i64, double }* %tuple,
//                           %"class.impala::TupleRow"* %tuple_row)
//   ret void
// }
Function* AggregationNode::CodegenUpdateTuple(LlvmCodeGen* codegen) {
  SCOPED_TIMER(codegen->codegen_timer());

  int j = probe_expr_ctxs_.size();
  for (int i = 0; i < aggregate_evaluators_.size(); ++i, ++j) {
    SlotDescriptor* slot_desc = intermediate_tuple_desc_->slots()[j];
    AggFnEvaluator* evaluator = aggregate_evaluators_[i];

    // Timestamp and char are never supported. NDV supports decimal and string but no
    // other functions.
    // TODO: the other aggregate functions might work with decimal as-is
    if (slot_desc->type().type == TYPE_TIMESTAMP || slot_desc->type().type == TYPE_CHAR ||
        (evaluator->agg_op() != AggFnEvaluator::NDV &&
         (slot_desc->type().type == TYPE_DECIMAL ||
          slot_desc->type().type == TYPE_STRING ||
          slot_desc->type().type == TYPE_VARCHAR))) {
      VLOG_QUERY << "Could not codegen UpdateIntermediateTuple because "
                 << "string, char, timestamp and decimal are not yet supported.";
      return NULL;
    }

    // Don't codegen things that aren't builtins (for now)
    if (!evaluator->is_builtin()) return NULL;
  }

  if (intermediate_tuple_desc_->GetLlvmStruct(codegen) == NULL) {
    VLOG_QUERY << "Could not codegen UpdateTuple because we could"
               << "not generate a matching llvm struct for the intermediate tuple.";
    return NULL;
  }

  // Get the types to match the UpdateTuple signature
  Type* agg_node_type = codegen->GetType(AggregationNode::LLVM_CLASS_NAME);
  Type* agg_tuple_type = codegen->GetType(Tuple::LLVM_CLASS_NAME);
  Type* tuple_row_type = codegen->GetType(TupleRow::LLVM_CLASS_NAME);

  DCHECK(agg_node_type != NULL);
  DCHECK(agg_tuple_type != NULL);
  DCHECK(tuple_row_type != NULL);

  PointerType* agg_node_ptr_type = codegen->GetPtrType(agg_node_type);
  PointerType* agg_tuple_ptr_type = codegen->GetPtrType(agg_tuple_type);
  PointerType* tuple_row_ptr_type = codegen->GetPtrType(tuple_row_type);

  // Signature for UpdateTuple is
  // void UpdateTuple(AggregationNode* this, FunctionContext** fn_ctx,
  //     ExprContext** expr_ctx, Tuple* tuple, TupleRow* row)
  // This signature needs to match the non-codegen'd signature exactly.
  StructType* tuple_struct = intermediate_tuple_desc_->GetLlvmStruct(codegen);
  if (tuple_struct == NULL) {
    VLOG_QUERY << "Could not codegen UpdateSlot(): could not generate tuple struct.";
    return NULL;
  }
  PointerType* tuple_ptr = PointerType::get(tuple_struct, 0);
  LlvmCodeGen::FnPrototype prototype(codegen, "UpdateTuple", codegen->void_type());
  prototype.AddArgument(LlvmCodeGen::NamedVariable("this_ptr", agg_node_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("agg_tuple", agg_tuple_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("tuple_row", tuple_row_ptr_type));

  LlvmBuilder builder(codegen->context());
  Value* args[3];
  Function* fn = prototype.GeneratePrototype(&builder, &args[0]);

  // Cast the parameter types to the internal llvm runtime types.
  // TODO: get rid of this by using right type in function signature
  Value* this_arg = args[0];
  Value* agg_tuple_arg = builder.CreateBitCast(args[1], tuple_ptr, "tuple");
  Value* row_arg = args[2];

  Function* get_fn_ctx_fn = codegen->GetFunction(IRFunction::AGG_NODE_GET_FN_CTX, false);
  DCHECK(get_fn_ctx_fn != NULL);
  Function* get_expr_ctx_fn =
      codegen->GetFunction(IRFunction::AGG_NODE_GET_EXPR_CTX, false);
  DCHECK(get_expr_ctx_fn != NULL);

  // Loop over each expr and generate the IR for that slot.  If the expr is not
  // count(*), generate a helper IR function to update the slot and call that.
  j = probe_expr_ctxs_.size();
  for (int i = 0; i < aggregate_evaluators_.size(); ++i, ++j) {
    SlotDescriptor* slot_desc = intermediate_tuple_desc_->slots()[j];
    AggFnEvaluator* evaluator = aggregate_evaluators_[i];
    if (evaluator->is_count_star()) {
      // TODO: we should be able to hoist this up to the loop over the batch and just
      // increment the slot by the number of rows in the batch.
      int field_idx = slot_desc->llvm_field_idx();
      Value* const_one = codegen->GetIntConstant(TYPE_BIGINT, 1);
      Value* slot_ptr = builder.CreateStructGEP(NULL, agg_tuple_arg, field_idx,
          "src_slot");
      Value* slot_loaded = builder.CreateLoad(slot_ptr, "count_star_val");
      Value* count_inc = builder.CreateAdd(slot_loaded, const_one, "count_star_inc");
      builder.CreateStore(count_inc, slot_ptr);
    } else {
      Function* update_slot_fn = CodegenUpdateSlot(codegen, evaluator, slot_desc);
      if (update_slot_fn == NULL) return NULL;
      // Call GetAggFnCtx() to get the function context.
      Value* get_fn_ctx_args[] = { this_arg, codegen->GetIntConstant(TYPE_INT, i) };
      Value* fn_ctx = builder.CreateCall(get_fn_ctx_fn, get_fn_ctx_args);
      // Call GetAggExprCtx() to get the expression context.
      DCHECK(agg_expr_ctxs_[i] != NULL);
      Value* get_expr_ctx_args[] = { this_arg, codegen->GetIntConstant(TYPE_INT, i) };
      Value* expr_ctx = builder.CreateCall(get_expr_ctx_fn, get_expr_ctx_args);
      Value* update_slot_args[] = { fn_ctx, expr_ctx, agg_tuple_arg, row_arg };
      builder.CreateCall(update_slot_fn, update_slot_args);
    }
  }
  builder.CreateRetVoid();

  // CodegenProcessRowBatch() does the final optimizations.
  return codegen->FinalizeFunction(fn);
}

Function* AggregationNode::CodegenProcessRowBatch(LlvmCodeGen* codegen,
    Function* update_tuple_fn) {
  SCOPED_TIMER(codegen->codegen_timer());
  DCHECK(update_tuple_fn != NULL);

  // Get the cross compiled update row batch function
  IRFunction::Type ir_fn = (!probe_expr_ctxs_.empty() ?
      IRFunction::AGG_NODE_PROCESS_ROW_BATCH_WITH_GROUPING :
      IRFunction::AGG_NODE_PROCESS_ROW_BATCH_NO_GROUPING);
  Function* process_batch_fn = codegen->GetFunction(ir_fn, true);

  if (process_batch_fn == NULL) {
    LOG(ERROR) << "Could not find AggregationNode::ProcessRowBatch in module.";
    return NULL;
  }

  int replaced;
  if (!probe_expr_ctxs_.empty()) {
    // Aggregation w/o grouping does not use a hash table.

    // Codegen for hash
    Function* hash_fn = hash_tbl_->CodegenHashCurrentRow(codegen);
    if (hash_fn == NULL) return NULL;

    // Codegen HashTable::Equals
    Function* equals_fn = hash_tbl_->CodegenEquals(codegen);
    if (equals_fn == NULL) return NULL;

    // Codegen for evaluating build rows
    Function* eval_build_row_fn = hash_tbl_->CodegenEvalTupleRow(codegen, true);
    if (eval_build_row_fn == NULL) return NULL;

    // Codegen for evaluating probe rows
    Function* eval_probe_row_fn = hash_tbl_->CodegenEvalTupleRow(codegen, false);
    if (eval_probe_row_fn == NULL) return NULL;

    // Replace call sites
    replaced =
        codegen->ReplaceCallSites(process_batch_fn, eval_build_row_fn, "EvalBuildRow");
    DCHECK_EQ(replaced, 1);

    replaced =
        codegen->ReplaceCallSites(process_batch_fn, eval_probe_row_fn, "EvalProbeRow");
    DCHECK_EQ(replaced, 1);

    replaced = codegen->ReplaceCallSites(process_batch_fn, hash_fn, "HashCurrentRow");
    DCHECK_EQ(replaced, 2);

    replaced = codegen->ReplaceCallSites(process_batch_fn, equals_fn, "Equals");
    DCHECK_EQ(replaced, 1);
  }

  replaced = codegen->ReplaceCallSites(process_batch_fn, update_tuple_fn, "UpdateTuple");
  DCHECK_EQ(replaced, 1);

  return codegen->FinalizeFunction(process_batch_fn);
}

}
