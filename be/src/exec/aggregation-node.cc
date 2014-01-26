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

#include "exec/aggregation-node.h"

#include <math.h>
#include <sstream>
#include <boost/functional/hash.hpp>
#include <thrift/protocol/TDebugProtocol.h>

#include <x86intrin.h>

#include "codegen/llvm-codegen.h"
#include "exec/hash-table.inline.h"
#include "exprs/agg-fn-evaluator.h"
#include "exprs/expr.h"
#include "runtime/descriptors.h"
#include "runtime/mem-pool.h"
#include "runtime/raw-value.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/string-value.inline.h"
#include "runtime/tuple.h"
#include "runtime/tuple-row.h"
#include "util/debug-util.h"
#include "util/runtime-profile.h"

#include "gen-cpp/Exprs_types.h"
#include "gen-cpp/PlanNodes_types.h"

using namespace impala;
using namespace std;
using namespace boost;
using namespace llvm;

namespace impala {

const char* AggregationNode::LLVM_CLASS_NAME = "class.impala::AggregationNode";

// TODO: pass in maximum size; enforce by setting limit in mempool
AggregationNode::AggregationNode(ObjectPool* pool, const TPlanNode& tnode,
                                 const DescriptorTbl& descs)
  : ExecNode(pool, tnode, descs),
    agg_tuple_id_(tnode.agg_node.agg_tuple_id),
    agg_tuple_desc_(NULL),
    singleton_output_tuple_(NULL),
    codegen_process_row_batch_fn_(NULL),
    process_row_batch_fn_(NULL),
    is_merge_(tnode.agg_node.__isset.is_merge ? tnode.agg_node.is_merge : false),
    needs_finalize_(tnode.agg_node.need_finalize),
    build_timer_(NULL),
    get_results_timer_(NULL),
    hash_table_buckets_counter_(NULL) {
}

Status AggregationNode::Init(const TPlanNode& tnode) {
  RETURN_IF_ERROR(ExecNode::Init(tnode));
  RETURN_IF_ERROR(
      Expr::CreateExprTrees(pool_, tnode.agg_node.grouping_exprs, &probe_exprs_));
  for (int i = 0; i < tnode.agg_node.aggregate_functions.size(); ++i) {
    AggFnEvaluator* evaluator;
    RETURN_IF_ERROR(AggFnEvaluator::Create(
        pool_, tnode.agg_node.aggregate_functions[i], &evaluator));
    aggregate_evaluators_.push_back(evaluator);
  }
  return Status::OK;
}

Status AggregationNode::Prepare(RuntimeState* state) {
  RETURN_IF_ERROR(ExecNode::Prepare(state));

  tuple_pool_.reset(new MemPool(mem_tracker()));
  build_timer_ = ADD_TIMER(runtime_profile(), "BuildTime");
  get_results_timer_ = ADD_TIMER(runtime_profile(), "GetResultsTime");
  hash_table_buckets_counter_ =
      ADD_COUNTER(runtime_profile(), "BuildBuckets", TCounterType::UNIT);
  hash_table_load_factor_counter_ =
      ADD_COUNTER(runtime_profile(), "LoadFactor", TCounterType::DOUBLE_VALUE);

  SCOPED_TIMER(runtime_profile_->total_time_counter());

  agg_tuple_desc_ = state->desc_tbl().GetTupleDescriptor(agg_tuple_id_);
  RETURN_IF_ERROR(Expr::Prepare(probe_exprs_, state, child(0)->row_desc(), false));

  // Construct build exprs from agg_tuple_desc_
  for (int i = 0; i < probe_exprs_.size(); ++i) {
    SlotDescriptor* desc = agg_tuple_desc_->slots()[i];
    Expr* expr = new SlotRef(desc);
    state->obj_pool()->Add(expr);
    build_exprs_.push_back(expr);
  }
  RETURN_IF_ERROR(Expr::Prepare(build_exprs_, state, row_desc(), false));

  int j = probe_exprs_.size();
  for (int i = 0; i < aggregate_evaluators_.size(); ++i, ++j) {
    // skip non-materialized slots; we don't have evaluators instantiated for those
    while (!agg_tuple_desc_->slots()[j]->is_materialized()) {
      DCHECK_LT(j, agg_tuple_desc_->slots().size() - 1)
          << "#eval= " << aggregate_evaluators_.size()
          << " #probe=" << probe_exprs_.size();
      ++j;
    }
    SlotDescriptor* desc = agg_tuple_desc_->slots()[j];
    RETURN_IF_ERROR(aggregate_evaluators_[i]->Prepare(state, child(0)->row_desc(),
        tuple_pool_.get(), desc));
  }

  // TODO: how many buckets?
  hash_tbl_.reset(new HashTable(state, build_exprs_, probe_exprs_, 1, true, true,
      id(), mem_tracker()));

  if (probe_exprs_.empty()) {
    // create single output tuple now; we need to output something
    // even if our input is empty
    singleton_output_tuple_ = ConstructAggTuple();
  }

  if (state->codegen_enabled()) {
    DCHECK(state->codegen() != NULL);
    Function* update_tuple_fn = CodegenUpdateAggTuple(state->codegen());
    if (update_tuple_fn != NULL) {
      codegen_process_row_batch_fn_ =
          CodegenProcessRowBatch(state->codegen(), update_tuple_fn);
      if (codegen_process_row_batch_fn_ != NULL) {
        // Update to using codegen'd process row batch.
        state->codegen()->AddFunctionToJit(
            codegen_process_row_batch_fn_,
            reinterpret_cast<void**>(&process_row_batch_fn_));
        AddRuntimeExecOption("Codegen Enabled");
      }
    }
  }
  return Status::OK;
}

Status AggregationNode::Open(RuntimeState* state) {
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::OPEN, state));
  SCOPED_TIMER(runtime_profile_->total_time_counter());

  RETURN_IF_ERROR(children_[0]->Open(state));

  RowBatch batch(children_[0]->row_desc(), state->batch_size(), mem_tracker());
  int64_t num_input_rows = 0;
  int64_t num_agg_rows = 0;
  while (true) {
    bool eos;
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(state->CheckQueryState());
    RETURN_IF_ERROR(children_[0]->GetNext(state, &batch, &eos));
    SCOPED_TIMER(build_timer_);

    if (VLOG_ROW_IS_ON) {
      for (int i = 0; i < batch.num_rows(); ++i) {
        TupleRow* row = batch.GetRow(i);
        VLOG_ROW << "input row: " << PrintRow(row, children_[0]->row_desc());
      }
    }
    int64_t agg_rows_before = hash_tbl_->size();
    if (process_row_batch_fn_ != NULL) {
      process_row_batch_fn_(this, &batch);
    } else if (singleton_output_tuple_ != NULL) {
      ProcessRowBatchNoGrouping(&batch);
    } else {
      ProcessRowBatchWithGrouping(&batch);
    }
    COUNTER_SET(hash_table_buckets_counter_, hash_tbl_->num_buckets());
    COUNTER_SET(hash_table_load_factor_counter_, hash_tbl_->load_factor());
    num_agg_rows += (hash_tbl_->size() - agg_rows_before);
    num_input_rows += batch.num_rows();

    batch.Reset();
    RETURN_IF_ERROR(state->CheckQueryState());
    if (eos) break;
  }

  // We have consumed all of the input from the child and transfered ownership of the
  // resources we need, so the child can be closed safely to release its resources.
  child(0)->Close(state);
  if (singleton_output_tuple_ != NULL) {
    hash_tbl_->Insert(reinterpret_cast<TupleRow*>(&singleton_output_tuple_));
    ++num_agg_rows;
  }
  VLOG_FILE << "aggregated " << num_input_rows << " input rows into "
            << num_agg_rows << " output rows";
  output_iterator_ = hash_tbl_->Begin();
  return Status::OK;
}

Status AggregationNode::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(state->CheckQueryState());
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  SCOPED_TIMER(get_results_timer_);

  if (ReachedLimit()) {
    *eos = true;
    return Status::OK;
  }
  Expr** conjuncts = &conjuncts_[0];
  int num_conjuncts = conjuncts_.size();

  while (!output_iterator_.AtEnd() && !row_batch->IsFull()) {
    int row_idx = row_batch->AddRow();
    TupleRow* row = row_batch->GetRow(row_idx);
    Tuple* agg_tuple = output_iterator_.GetRow()->GetTuple(0);
    FinalizeAggTuple(agg_tuple);
    row->SetTuple(0, agg_tuple);
    if (ExecNode::EvalConjuncts(conjuncts, num_conjuncts, row)) {
      VLOG_ROW << "output row: " << PrintRow(row, row_desc());
      row_batch->CommitLastRow();
      ++num_rows_returned_;
      if (ReachedLimit()) break;
    }
    output_iterator_.Next<false>();
  }
  *eos = output_iterator_.AtEnd() || ReachedLimit();
  COUNTER_SET(rows_returned_counter_, num_rows_returned_);
  return Status::OK;
}

void AggregationNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  if (tuple_pool_.get() != NULL) tuple_pool_->FreeAll();
  if (hash_tbl_.get() != NULL) hash_tbl_->Close();
  ExecNode::Close(state);
}

Tuple* AggregationNode::ConstructAggTuple() {
  Tuple* agg_tuple = Tuple::Create(agg_tuple_desc_->byte_size(), tuple_pool_.get());
  vector<SlotDescriptor*>::const_iterator slot_desc = agg_tuple_desc_->slots().begin();

  // copy grouping values
  for (int i = 0; i < probe_exprs_.size(); ++i, ++slot_desc) {
    if (hash_tbl_->last_expr_value_null(i)) {
      agg_tuple->SetNull((*slot_desc)->null_indicator_offset());
    } else {
      void* src = hash_tbl_->last_expr_value(i);
      void* dst = agg_tuple->GetSlot((*slot_desc)->tuple_offset());
      RawValue::Write(src, dst, (*slot_desc)->type(), tuple_pool_.get());
    }
  }

  // Initialize aggregate output.
  for (int i = 0; i < aggregate_evaluators_.size(); ++i, ++slot_desc) {
    while (!(*slot_desc)->is_materialized()) ++slot_desc;
    AggFnEvaluator* evaluator = aggregate_evaluators_[i];
    evaluator->Init(agg_tuple);
    // Codegen specific path.
    // To minimize branching on the UpdateAggTuple path, initialize the result value
    // so that UpdateAggTuple doesn't have to check if the aggregation
    // dst slot is null.
    //  - sum/count: 0
    //  - min: max_value
    //  - max: min_value
    // TODO: remove when we don't use the irbuilder for codegen here.
    // This optimization no longer applies with AnyVal
    if ((*slot_desc)->type() != TYPE_STRING && (*slot_desc)->type() != TYPE_TIMESTAMP
        && (*slot_desc)->type() != TYPE_CHAR) {
      ExprValue default_value;
      void* default_value_ptr = NULL;
      switch (evaluator->agg_op()) {
        case AggFnEvaluator::MIN:
          default_value_ptr = default_value.SetToMax((*slot_desc)->type());
          RawValue::Write(default_value_ptr, agg_tuple, *slot_desc, NULL);
          break;
        case AggFnEvaluator::MAX:
          default_value_ptr = default_value.SetToMin((*slot_desc)->type());
          RawValue::Write(default_value_ptr, agg_tuple, *slot_desc, NULL);
          break;
        default:
          break;
      }
    }
  }
  return agg_tuple;
}

void AggregationNode::UpdateAggTuple(Tuple* tuple, TupleRow* row) {
  DCHECK(tuple != NULL || aggregate_evaluators_.empty());
  for (vector<AggFnEvaluator*>::const_iterator evaluator = aggregate_evaluators_.begin();
      evaluator != aggregate_evaluators_.end(); ++evaluator) {
    if (is_merge_) {
      (*evaluator)->Merge(row, tuple);
    } else {
      (*evaluator)->Update(row, tuple);
    }
  }
}

void AggregationNode::FinalizeAggTuple(Tuple* tuple) {
  DCHECK(tuple != NULL || aggregate_evaluators_.empty());
  for (vector<AggFnEvaluator*>::const_iterator evaluator = aggregate_evaluators_.begin();
      evaluator != aggregate_evaluators_.end(); ++evaluator) {
    if (needs_finalize_) {
      (*evaluator)->Finalize(tuple);
    } else {
      (*evaluator)->Serialize(tuple);
    }
  }
}

void AggregationNode::DebugString(int indentation_level, stringstream* out) const {
  *out << string(indentation_level * 2, ' ');
  *out << "AggregationNode(tuple_id=" << agg_tuple_id_
       << " is_merge=" << is_merge_ << " needs_finalize=" << needs_finalize_
       << " probe_exprs=" << Expr::DebugString(probe_exprs_)
       << " agg_exprs=" << AggFnEvaluator::DebugString(aggregate_evaluators_);
  ExecNode::DebugString(indentation_level, out);
  *out << ")";
}

// IR Generation for updating a single aggregation slot. Signature is:
// void UpdateSlot(AggTuple* agg_tuple, char** row)
// The IR for sum(double_col) is:
//  define void @UpdateSlot({ i8, double }* %agg_tuple, i8** %row) {
//  entry:
//    %src_null_ptr = alloca i1
//    %src_value = call double @SlotRef(i8** %row, i8* null, i1* %src_null_ptr)
//    %src_is_null = load i1* %src_null_ptr
//    br i1 %src_is_null, label %ret, label %src_not_null
//
//  src_not_null:                                     ; preds = %entry
//    %dst_slot_ptr = getelementptr inbounds { i8, double }* %agg_tuple, i32 0, i32 1
//    call void @SetNotNull({ i8, double }* %agg_tuple)
//    %dst_val = load double* %dst_slot_ptr
//    %0 = fadd double %dst_val, %src_value
//    store double %0, double* %dst_slot_ptr
//    br label %ret
//
//  ret:                                    ; preds = %src_not_null, %entry
//    ret void
//  }
llvm::Function* AggregationNode::CodegenUpdateSlot(
    LlvmCodeGen* codegen, AggFnEvaluator* evaluator, SlotDescriptor* slot_desc) {
  int field_idx = slot_desc->field_idx();
  DCHECK(slot_desc->is_materialized());

  LLVMContext& context = codegen->context();

  StructType* tuple_struct = agg_tuple_desc_->GenerateLlvmStruct(codegen);
  PointerType* tuple_ptr = PointerType::get(tuple_struct, 0);
  PointerType* ptr_type = codegen->ptr_type();

  // Create UpdateSlot prototype
  LlvmCodeGen::FnPrototype prototype(codegen, "UpdateSlot", codegen->void_type());
  prototype.AddArgument(LlvmCodeGen::NamedVariable("agg_tuple", tuple_ptr));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("row", PointerType::get(ptr_type, 0)));

  LlvmCodeGen::LlvmBuilder builder(context);
  Value* args[2];
  Function* fn = prototype.GeneratePrototype(&builder, &args[0]);

  LlvmCodeGen::NamedVariable null_var("src_null_ptr", codegen->boolean_type());
  Value* src_is_null_ptr = codegen->CreateEntryBlockAlloca(fn, null_var);

  // Call expr function to get src slot value
  DCHECK_EQ(evaluator->input_exprs().size(), 1);
  Expr* input_expr = evaluator->input_exprs()[0];
  Function* agg_expr_fn = input_expr->codegen_fn();
  int scratch_buffer_size = input_expr->scratch_buffer_size();
  DCHECK_EQ(scratch_buffer_size, 0);
  DCHECK(agg_expr_fn != NULL);
  if (agg_expr_fn == NULL) return NULL;

  BasicBlock* src_not_null_block, *ret_block;
  codegen->CreateIfElseBlocks(fn, "src_not_null", "ret", &src_not_null_block, &ret_block);

  Value* expr_args[] = { args[1], ConstantPointerNull::get(ptr_type), src_is_null_ptr };
  Value* src_value = input_expr->CodegenGetValue(codegen, builder.GetInsertBlock(),
      expr_args, ret_block, src_not_null_block);

  // Src slot is not null, update dst_slot
  builder.SetInsertPoint(src_not_null_block);
  Value* dst_ptr = builder.CreateStructGEP(args[0], field_idx, "dst_slot_ptr");
  Value* result = NULL;

  if (slot_desc->is_nullable()) {
    // Dst is NULL, just update dst slot to src slot and clear null bit
    Function* clear_null_fn = slot_desc->CodegenUpdateNull(codegen, tuple_struct, false);
    builder.CreateCall(clear_null_fn, args[0]);
  }

  // Update the slot
  Value* dst_value = builder.CreateLoad(dst_ptr, "dst_val");
  switch (evaluator->agg_op()) {
    case AggFnEvaluator::COUNT:
      result = builder.CreateAdd(dst_value,
          codegen->GetIntConstant(TYPE_BIGINT, 1), "count_inc");
      break;
    case AggFnEvaluator::MIN: {
      Function* min_fn = codegen->CodegenMinMax(slot_desc->type(), true);
      Value* min_args[] = { dst_value, src_value };
      result = builder.CreateCall(min_fn, min_args, "min_value");
      break;
    }
    case AggFnEvaluator::MAX: {
      Function* max_fn = codegen->CodegenMinMax(slot_desc->type(), false);
      Value* max_args[] = { dst_value, src_value };
      result = builder.CreateCall(max_fn, max_args, "max_value");
      break;
    }
    case AggFnEvaluator::SUM:
      if (slot_desc->type() == TYPE_FLOAT || slot_desc->type() == TYPE_DOUBLE) {
        result = builder.CreateFAdd(dst_value, src_value);
      } else {
        result = builder.CreateAdd(dst_value, src_value);
      }
      break;
    default:
      DCHECK(false) << "bad aggregate operator: " << evaluator->agg_op();
  }

  builder.CreateStore(result, dst_ptr);
  builder.CreateBr(ret_block);

  builder.SetInsertPoint(ret_block);
  builder.CreateRetVoid();

  return codegen->FinalizeFunction(fn);
}

// IR codegen for the UpdateAggTuple loop.  This loop is query specific and
// based on the aggregate functions.  The function signature must match the non-
// codegen'd UpdateAggTuple exactly.
// For the query:
// select count(*), count(int_col), sum(double_col) the IR looks like:
//
// define void @UpdateAggTuple(%"class.impala::AggregationNode"* %this_ptr,
//                             %"class.impala::Tuple"* %agg_tuple,
//                             %"class.impala::TupleRow"* %tuple_row) {
// entry:
//   %tuple = bitcast %"class.impala::AggregationNode"* %agg_tuple to
//                                              { i8, i64, i64, double }*
//   %row = bitcast %"class.impala::TupleRow"* %tuple_row to i8**
//   %src_slot = getelementptr inbounds { i8, i64, i64, double }* %tuple, i32 0, i32 2
//   %count_star_val = load i64* %src_slot
//   %count_star_inc = add i64 %count_star_val, 1
//   store i64 %count_star_inc, i64* %src_slot
//   call void @UpdateSlot({ i8, i64, i64, double }* %tuple, i8** %row)
//   call void @UpdateSlot2({ i8, i64, i64, double }* %tuple, i8** %row)
//   ret void
// }
Function* AggregationNode::CodegenUpdateAggTuple(LlvmCodeGen* codegen) {
  SCOPED_TIMER(codegen->codegen_timer());

  for (int i = 0; i < probe_exprs_.size(); ++i) {
    if (probe_exprs_[i]->codegen_fn() == NULL) {
      VLOG_QUERY << "Could not codegen UpdateAggTuple because "
                 << "codegen for the grouping exprs is not yet supported.";
      return NULL;
    }
  }

  int j = probe_exprs_.size();
  for (int i = 0; i < aggregate_evaluators_.size(); ++i, ++j) {
    // skip non-materialized slots; we don't have evaluators instantiated for those
    while (!agg_tuple_desc_->slots()[j]->is_materialized()) {
      DCHECK_LT(j, agg_tuple_desc_->slots().size() - 1);
      ++j;
    }
    SlotDescriptor* slot_desc = agg_tuple_desc_->slots()[j];
    AggFnEvaluator* evaluator = aggregate_evaluators_[i];

    // string and timestamp aggregation currently not supported
    if (slot_desc->type() == TYPE_STRING || slot_desc->type() == TYPE_TIMESTAMP ||
        slot_desc->type() == TYPE_CHAR) {
      VLOG_QUERY << "Could not codegen UpdateAggTuple because "
                 << "string, char and timestamp aggregation is not yet supported.";
      return NULL;
    }

    // If the evaluator can't be generated, bail generating this function
    if (!evaluator->is_count_star() &&
        evaluator->input_exprs()[0]->codegen_fn() == NULL) {
      VLOG_QUERY << "Could not codegen UpdateAggTuple because the "
                 << "underlying exprs cannot be codegened.";
      return NULL;
    }

    // Don't codegen things that aren't builtins (for now)
    if (!evaluator->is_builtin()) return NULL;
  }

  if (agg_tuple_desc_->GenerateLlvmStruct(codegen) == NULL) {
    VLOG_QUERY << "Could not codegen UpdateAggTuple because we could"
               << "not generate a  matching llvm struct for the result agg tuple.";
    return NULL;
  }

  // Get the types to match the UpdateAggTuple signature
  Type* agg_node_type = codegen->GetType(AggregationNode::LLVM_CLASS_NAME);
  Type* agg_tuple_type = codegen->GetType(Tuple::LLVM_CLASS_NAME);
  Type* tuple_row_type = codegen->GetType(TupleRow::LLVM_CLASS_NAME);

  DCHECK(agg_node_type != NULL);
  DCHECK(agg_tuple_type != NULL);
  DCHECK(tuple_row_type != NULL);

  PointerType* agg_node_ptr_type = PointerType::get(agg_node_type, 0);
  PointerType* agg_tuple_ptr_type = PointerType::get(agg_tuple_type, 0);
  PointerType* tuple_row_ptr_type = PointerType::get(tuple_row_type, 0);

  // Signature for UpdateAggTuple is
  // void UpdateAggTuple(AggTuple* agg_tuple, char** row)
  // This signature needs to match the non-codegen'd signature exactly.
  PointerType* ptr_type = codegen->ptr_type();
  StructType* tuple_struct = agg_tuple_desc_->GenerateLlvmStruct(codegen);
  PointerType* tuple_ptr = PointerType::get(tuple_struct, 0);
  LlvmCodeGen::FnPrototype prototype(codegen, "UpdateAggTuple", codegen->void_type());
  prototype.AddArgument(LlvmCodeGen::NamedVariable("this_ptr", agg_node_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("agg_tuple", agg_tuple_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("tuple_row", tuple_row_ptr_type));

  LlvmCodeGen::LlvmBuilder builder(codegen->context());
  Value* args[3];
  Function* fn = prototype.GeneratePrototype(&builder, &args[0]);

  // Cast the parameter types to the internal llvm runtime types.
  args[1] = builder.CreateBitCast(args[1], tuple_ptr, "tuple");
  args[2] = builder.CreateBitCast(args[2], PointerType::get(ptr_type, 0), "row");

  // Loop over each expr and generate the IR for that slot.  If the expr is not
  // count(*), generate a helper IR function to update the slot and call that.
  j = probe_exprs_.size();
  for (int i = 0; i < aggregate_evaluators_.size(); ++i, ++j) {
    // skip non-materialized slots; we don't have evaluators instantiated for those
    while (!agg_tuple_desc_->slots()[j]->is_materialized()) {
      DCHECK_LT(j, agg_tuple_desc_->slots().size() - 1);
      ++j;
    }
    SlotDescriptor* slot_desc = agg_tuple_desc_->slots()[j];
    AggFnEvaluator* evaluator = aggregate_evaluators_[i];
    if (evaluator->is_count_star()) {
      // TODO: we should be able to hoist this up to the loop over the batch and just
      // increment the slot by the number of rows in the batch.
      int field_idx = slot_desc->field_idx();
      Value* const_one = codegen->GetIntConstant(TYPE_BIGINT, 1);
      Value* slot_ptr = builder.CreateStructGEP(args[1], field_idx, "src_slot");
      Value* slot_loaded = builder.CreateLoad(slot_ptr, "count_star_val");
      Value* count_inc = builder.CreateAdd(slot_loaded, const_one, "count_star_inc");
      builder.CreateStore(count_inc, slot_ptr);
    } else {
      Function* update_slot_fn = CodegenUpdateSlot(codegen, evaluator, slot_desc);
      if (update_slot_fn == NULL) return NULL;
      builder.CreateCall2(update_slot_fn, args[1], args[2]);
    }
  }
  builder.CreateRetVoid();

  return codegen->OptimizeFunctionWithExprs(fn);
}

Function* AggregationNode::CodegenProcessRowBatch(
    LlvmCodeGen* codegen, Function* update_tuple_fn) {
  SCOPED_TIMER(codegen->codegen_timer());
  DCHECK(update_tuple_fn != NULL);

  // Get the cross compiled update row batch function
  IRFunction::Type ir_fn = (singleton_output_tuple_ == NULL ?
      IRFunction::AGG_NODE_PROCESS_ROW_BATCH_WITH_GROUPING :
      IRFunction::AGG_NODE_PROCESS_ROW_BATCH_NO_GROUPING);
  Function* process_batch_fn = codegen->GetFunction(ir_fn);

  if (process_batch_fn == NULL) {
    LOG(ERROR) << "Could not find AggregationNode::ProcessRowBatch in module.";
    return NULL;
  }

  int replaced = 0;
  if (singleton_output_tuple_ == NULL) {
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
    process_batch_fn = codegen->ReplaceCallSites(process_batch_fn, false,
        eval_build_row_fn, "EvalBuildRow", &replaced);
    DCHECK_EQ(replaced, 1);

    process_batch_fn = codegen->ReplaceCallSites(process_batch_fn, false,
        eval_probe_row_fn, "EvalProbeRow", &replaced);
    DCHECK_EQ(replaced, 1);

    process_batch_fn = codegen->ReplaceCallSites(process_batch_fn, false,
        hash_fn, "HashCurrentRow", &replaced);
    DCHECK_EQ(replaced, 2);

    process_batch_fn = codegen->ReplaceCallSites(process_batch_fn, false,
        equals_fn, "Equals", &replaced);
    DCHECK_EQ(replaced, 1);
  }

  process_batch_fn = codegen->ReplaceCallSites(process_batch_fn, false,
      update_tuple_fn, "UpdateAggTuple", &replaced);
  DCHECK_EQ(replaced, 1) << "One call site should be replaced.";
  DCHECK(process_batch_fn != NULL);

  return codegen->OptimizeFunctionWithExprs(process_batch_fn);
}

}
