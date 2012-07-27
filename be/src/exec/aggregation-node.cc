// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "exec/aggregation-node.h"

#include <sstream>
#include <boost/functional/hash.hpp>

#include "codegen/llvm-codegen.h"
#include "exec/hash-table.inline.h"
#include "exprs/agg-expr.h"
#include "exprs/expr.h"
#include "runtime/descriptors.h"
#include "runtime/mem-pool.h"
#include "runtime/raw-value.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
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


// This object appends n-int32s to the end of a normal tuple object to maintain the lengths
// of the string buffers in the tuple.
namespace impala {

class AggregationTuple {
 public:
  static AggregationTuple* Create(int tuple_size, int num_string_slots, MemPool* pool) {
    int size = tuple_size + sizeof(int32_t) * num_string_slots;
    AggregationTuple* result = reinterpret_cast<AggregationTuple*>(pool->Allocate(size));
    result->Init(size);
    return result;
  }

  void Init(int size) {
    bzero(this, size);
  }
    
  Tuple* tuple() { return reinterpret_cast<Tuple*>(this); }

  int32_t* BufferLengths(int tuple_size) {
    char* data = reinterpret_cast<char*>(this) + tuple_size;
    return reinterpret_cast<int*>(data);
  }

  // For C++/IR interop, we need to be able to look up types by name.
  static const char* LLVM_CLASS_NAME;

 private:
  void* data_;
};

const char* AggregationTuple::LLVM_CLASS_NAME = "class.impala::AggregationTuple";

}

const char* AggregationNode::LLVM_CLASS_NAME = "class.impala::AggregationNode";

// TODO: pass in maximum size; enforce by setting limit in mempool
// TODO: have a Status ExecNode::Init(const TPlanNode&) member function
// that does initialization outside of c'tor, so we can indicate errors
AggregationNode::AggregationNode(ObjectPool* pool, const TPlanNode& tnode,
                                 const DescriptorTbl& descs)
  : ExecNode(pool, tnode, descs),
    agg_tuple_id_(tnode.agg_node.agg_tuple_id),
    agg_tuple_desc_(NULL),
    singleton_output_tuple_(NULL),
    num_string_slots_(0),
    tuple_pool_(new MemPool()),
    process_row_batch_fn_(NULL) {
  // ignore return status for now
  Expr::CreateExprTrees(pool, tnode.agg_node.grouping_exprs, &probe_exprs_);
  Expr::CreateExprTrees(pool, tnode.agg_node.aggregate_exprs, &aggregate_exprs_);
}

Status AggregationNode::Prepare(RuntimeState* state) {
  RETURN_IF_ERROR(ExecNode::Prepare(state));

  build_timer_ =
      ADD_COUNTER(runtime_profile(), "BuildTime", TCounterType::CPU_TICKS);
  get_results_timer_ =
      ADD_COUNTER(runtime_profile(), "GetResultsTime", TCounterType::CPU_TICKS);
  hash_table_buckets_counter_ = 
      ADD_COUNTER(runtime_profile(), "BuildBuckets", TCounterType::UNIT);

  agg_tuple_desc_ = state->desc_tbl().GetTupleDescriptor(agg_tuple_id_);
  RETURN_IF_ERROR(Expr::Prepare(probe_exprs_, state, child(0)->row_desc()));
  RETURN_IF_ERROR(Expr::Prepare(aggregate_exprs_, state, child(0)->row_desc()));

  // Construct build exprs from agg_tuple_desc_
  for (int i = 0; i < probe_exprs_.size(); ++i) {
    SlotDescriptor* desc = agg_tuple_desc_->slots()[i];
    Expr* expr = new SlotRef(desc);      
    state->obj_pool()->Add(expr);
    build_exprs_.push_back(expr);
  }
  RETURN_IF_ERROR(Expr::Prepare(build_exprs_, state, row_desc()));

  // TODO: how many buckets?
  hash_tbl_.reset(new HashTable(build_exprs_, probe_exprs_, 1, true));
  
  // Determine the number of string slots in the output
  for (vector<Expr*>::const_iterator expr = aggregate_exprs_.begin();
       expr != aggregate_exprs_.end(); ++expr) {
    AggregateExpr* agg_expr = static_cast<AggregateExpr*>(*expr);
    if (agg_expr->type() == TYPE_STRING) ++num_string_slots_;
  }
  
  if (probe_exprs_.empty()) {
    // create single output tuple now; we need to output something
    // even if our input is empty
    singleton_output_tuple_ = ConstructAggTuple();
  }

  LlvmCodeGen* codegen = state->llvm_codegen();
  if (codegen != NULL) {
    Function* update_tuple_fn = CodegenUpdateAggTuple(codegen);
    if (update_tuple_fn != NULL) {
      Function* process_row_batch = CodegenProcessRowBatch(codegen, update_tuple_fn);
      if (process_row_batch != NULL) {
        void* jitted_process_row_batch = codegen->JitFunction(process_row_batch);
        process_row_batch_fn_ = 
            reinterpret_cast<ProcessRowBatchFn>(jitted_process_row_batch);
        LOG(INFO) << "AggregationNode(node_id=" << id() 
                  << ") using llvm codegend functions.";
      }
    }
  }
  return Status::OK;
}

Status AggregationNode::Open(RuntimeState* state) {
  COUNTER_SCOPED_TIMER(runtime_profile_->total_time_counter());

  RETURN_IF_ERROR(children_[0]->Open(state));

  RowBatch batch(children_[0]->row_desc(), state->batch_size());
  int64_t num_input_rows = 0;
  int64_t num_agg_rows = 0;
  while (true) {
    bool eos;
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(children_[0]->GetNext(state, &batch, &eos));
    COUNTER_SCOPED_TIMER(build_timer_);

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
    num_agg_rows += (hash_tbl_->size() - agg_rows_before);
    num_input_rows += batch.num_rows();

    if (eos) break;
    batch.Reset();
  }
  RETURN_IF_ERROR(children_[0]->Close(state));
  if (singleton_output_tuple_ != NULL) {
    hash_tbl_->Insert(reinterpret_cast<TupleRow*>(&singleton_output_tuple_));
    ++num_agg_rows;
  }
  VLOG_QUERY << "aggregated " << num_input_rows << " input rows into "
                   << num_agg_rows << " output rows";
  output_iterator_ = hash_tbl_->Begin();
  return Status::OK;
}

Status AggregationNode::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  RETURN_IF_CANCELLED(state);
  COUNTER_SCOPED_TIMER(runtime_profile_->total_time_counter());
  COUNTER_SCOPED_TIMER(get_results_timer_);

  if (ReachedLimit()) {
    *eos = true;
    return Status::OK;
  }
  Expr** conjuncts = &conjuncts_[0];
  int num_conjuncts = conjuncts_.size();

  while (output_iterator_.HasNext() && !row_batch->IsFull()) {
    int row_idx = row_batch->AddRow();
    TupleRow* row = row_batch->GetRow(row_idx);
    row->SetTuple(0, output_iterator_.GetRow()->GetTuple(0));
    if (ExecNode::EvalConjuncts(conjuncts, num_conjuncts, row)) {
      VLOG_ROW << "output row: " << PrintRow(row, row_desc());
      row_batch->CommitLastRow();
      ++num_rows_returned_;
      if (ReachedLimit()) {
        *eos = true;
        return Status::OK;
      }
    }
    output_iterator_.Next<false>();
  }
  *eos = !output_iterator_.HasNext();
  return Status::OK;
}

Status AggregationNode::Close(RuntimeState* state) {
  COUNTER_UPDATE(memory_used_counter(), tuple_pool_->peak_allocated_bytes());
  COUNTER_UPDATE(memory_used_counter(), hash_tbl_->byte_size());
  COUNTER_UPDATE(hash_table_buckets_counter_, hash_tbl_->num_buckets());
  return ExecNode::Close(state);
}

AggregationTuple* AggregationNode::ConstructAggTuple() {
  AggregationTuple* agg_out_tuple = 
      AggregationTuple::Create(agg_tuple_desc_->byte_size(), 
          num_string_slots_, tuple_pool_.get());
  Tuple* agg_tuple = agg_out_tuple->tuple();

  vector<SlotDescriptor*>::const_iterator slot_d = agg_tuple_desc_->slots().begin();
  // copy grouping values
  for (int i = 0; i < probe_exprs_.size(); ++i, ++slot_d) {
    if (hash_tbl_->last_expr_value_null(i)) {
      agg_tuple->SetNull((*slot_d)->null_indicator_offset());
    } else {
      void* src = hash_tbl_->last_expr_value(i);
      void* dst = agg_tuple->GetSlot((*slot_d)->tuple_offset());
      RawValue::Write(src, dst, (*slot_d)->type(), tuple_pool_.get());
    }
  }

  // All aggregate values except for COUNT start out with NULL
  // (so that SUM(<col>) stays NULL if <col> only contains NULL values).
  for (int i = 0; i < aggregate_exprs_.size(); ++i, ++slot_d) {
    AggregateExpr* agg_expr = static_cast<AggregateExpr*>(aggregate_exprs_[i]);
    if ((*slot_d)->is_nullable()) {
      DCHECK_NE(agg_expr->agg_op(), TAggregationOp::COUNT);
      agg_tuple->SetNull((*slot_d)->null_indicator_offset());
    } else {
      // For distributed plans, some SUMs (distributed count(*) will be non-nullable)
      DCHECK(agg_expr->agg_op() == TAggregationOp::COUNT ||
             agg_expr->agg_op() == TAggregationOp::SUM);
      // we're only aggregating into bigint slots and never return NULL
      *reinterpret_cast<int64_t*>(agg_tuple->GetSlot((*slot_d)->tuple_offset())) = 0;
    }
  }

  return agg_out_tuple;
}

char* AggregationNode::AllocateStringBuffer(int new_size, int* allocated_size) {
  new_size = ::max(new_size, FreeList::MinSize());
  char* buffer = reinterpret_cast<char*>(
      string_buffer_free_list_.Allocate(new_size, allocated_size));
  if (buffer == NULL)  {
    buffer = reinterpret_cast<char*>(tuple_pool_->Allocate(new_size));
    *allocated_size = new_size;
  }
  return buffer;
}

inline void AggregationNode::UpdateStringSlot(AggregationTuple* tuple, int string_slot_idx,
                                       StringValue* dst, const StringValue* src) {
  int32_t* string_buffer_lengths = tuple->BufferLengths(agg_tuple_desc_->byte_size());
  int curr_size = string_buffer_lengths[string_slot_idx];
  if (curr_size < src->len) {
    string_buffer_free_list_.Add(reinterpret_cast<uint8_t*>(dst->ptr), curr_size);
    dst->ptr = AllocateStringBuffer(src->len, &(string_buffer_lengths[string_slot_idx]));
  }
  strncpy(dst->ptr, src->ptr, src->len);
  dst->len = src->len;
}

inline void AggregationNode::UpdateMinStringSlot(AggregationTuple* agg_tuple, 
                                          const NullIndicatorOffset& null_indicator_offset, 
                                          int string_slot_idx, void* slot, void* value) {
  DCHECK(value != NULL);
  Tuple* tuple = agg_tuple->tuple();
  StringValue* dst_value = static_cast<StringValue*>(slot);
  StringValue* src_value = static_cast<StringValue*>(value);

  if (tuple->IsNull(null_indicator_offset)) {
    tuple->SetNotNull(null_indicator_offset);
  } else if (src_value->Compare(*dst_value) >= 0) {
    return;
  }
  UpdateStringSlot(agg_tuple, string_slot_idx, dst_value, src_value);
}

inline void AggregationNode::UpdateMaxStringSlot(AggregationTuple* agg_tuple, 
                                          const NullIndicatorOffset& null_indicator_offset,
                                          int string_slot_idx, void* slot, void* value) {
  DCHECK(value != NULL);
  Tuple* tuple = agg_tuple->tuple();
  StringValue* dst_value = static_cast<StringValue*>(slot);
  StringValue* src_value = static_cast<StringValue*>(value);

  if (tuple->IsNull(null_indicator_offset)) {
    tuple->SetNotNull(null_indicator_offset);
  } else if (src_value->Compare(*dst_value) <= 0) {
    return;
  }
  UpdateStringSlot(agg_tuple, string_slot_idx, dst_value, src_value);
}

template <typename T>
void UpdateMinSlot(Tuple* tuple, const NullIndicatorOffset& null_indicator_offset,
                   void* slot, void* value) {
  DCHECK(value != NULL);
  T* t_slot = static_cast<T*>(slot);
  if (tuple->IsNull(null_indicator_offset)) {
    tuple->SetNotNull(null_indicator_offset);
    *t_slot = *static_cast<T*>(value);
  } else {
    *t_slot = min(*t_slot, *static_cast<T*>(value));
  }
}

template <typename T>
void UpdateMaxSlot(Tuple* tuple, const NullIndicatorOffset& null_indicator_offset,
                   void* slot, void* value) {
  DCHECK(value != NULL);
  T* t_slot = static_cast<T*>(slot);
  if (tuple->IsNull(null_indicator_offset)) {
    tuple->SetNotNull(null_indicator_offset);
    *t_slot = *static_cast<T*>(value);
  } else {
    *t_slot = max(*t_slot, *static_cast<T*>(value));
  }
}

template <typename T>
void UpdateSumSlot(Tuple* tuple, const NullIndicatorOffset& null_indicator_offset,
                   void* slot, void* value) {
  DCHECK(value != NULL);
  T* t_slot = static_cast<T*>(slot);
  if (tuple->IsNull(null_indicator_offset)) {
    tuple->SetNotNull(null_indicator_offset);
    *t_slot = *static_cast<T*>(value);
  } else {
    *t_slot += *static_cast<T*>(value);
  }
}

void AggregationNode::UpdateAggTuple(AggregationTuple* agg_out_tuple, TupleRow* row) {
  DCHECK(agg_out_tuple != NULL);
  Tuple* tuple = agg_out_tuple->tuple();
  int string_slot_idx = -1;
  vector<SlotDescriptor*>::const_iterator slot_d = 
      agg_tuple_desc_->slots().begin() + probe_exprs_.size();
  for (vector<Expr*>::iterator expr = aggregate_exprs_.begin();
        expr != aggregate_exprs_.end(); ++expr, ++slot_d) {
    void* slot = tuple->GetSlot((*slot_d)->tuple_offset());
    AggregateExpr* agg_expr = static_cast<AggregateExpr*>(*expr);

    // keep track of which string slot we are on
    if (agg_expr->type() == TYPE_STRING) {
      ++string_slot_idx;
    }

    // deal with COUNT(*) separately (no need to check the actual child expr value)
    if (agg_expr->is_star()) {
      DCHECK_EQ(agg_expr->agg_op(), TAggregationOp::COUNT);
      // we're only aggregating into bigint slots
      DCHECK_EQ((*slot_d)->type(), TYPE_BIGINT);
      ++*reinterpret_cast<int64_t*>(slot);
      continue;
    }

    // determine value of aggregate's child expr
    void* value = agg_expr->GetChild(0)->GetValue(row);
    if (value == NULL) {
      // NULLs don't get aggregated
      continue;
    }

    switch (agg_expr->agg_op()) {
      case TAggregationOp::COUNT:
        ++*reinterpret_cast<int64_t*>(slot);
        break;

      case TAggregationOp::MIN:
        switch (agg_expr->type()) {
          case TYPE_BOOLEAN:
            UpdateMinSlot<bool>(tuple,
                                (*slot_d)->null_indicator_offset(), slot, value);
            break;
          case TYPE_TINYINT:
            UpdateMinSlot<int8_t>(tuple,
                                  (*slot_d)->null_indicator_offset(), slot, value);
            break;
          case TYPE_SMALLINT:
            UpdateMinSlot<int16_t>(tuple,
                                   (*slot_d)->null_indicator_offset(), slot, value);
            break;
          case TYPE_INT:
            UpdateMinSlot<int32_t>(tuple,
                                   (*slot_d)->null_indicator_offset(), slot, value);
            break;
          case TYPE_BIGINT:
            UpdateMinSlot<int64_t>(tuple,
                                   (*slot_d)->null_indicator_offset(), slot, value);
            break;
          case TYPE_FLOAT:
            UpdateMinSlot<float>(tuple, (*slot_d)->null_indicator_offset(), slot, value);
            break;
          case TYPE_DOUBLE:
            UpdateMinSlot<double>(tuple, (*slot_d)->null_indicator_offset(), slot, value);
            break;
          case TYPE_TIMESTAMP:
            UpdateMinSlot<TimestampValue>(tuple,
                                          (*slot_d)->null_indicator_offset(),
                                          slot, value);
            break;
          case TYPE_STRING:
            UpdateMinStringSlot(agg_out_tuple, (*slot_d)->null_indicator_offset(), 
                                string_slot_idx, slot, value);
            break;
          default:
            DCHECK(false) << "invalid type: " << TypeToString(agg_expr->type());
        };
        break;

      case TAggregationOp::MAX:
        switch (agg_expr->type()) {
          case TYPE_BOOLEAN:
            UpdateMaxSlot<bool>(tuple, (*slot_d)->null_indicator_offset(), slot, value);
            break;
          case TYPE_TINYINT:
            UpdateMaxSlot<int8_t>(tuple, (*slot_d)->null_indicator_offset(), slot, value);
            break;
          case TYPE_SMALLINT:
            UpdateMaxSlot<int16_t>(tuple,
                                   (*slot_d)->null_indicator_offset(), slot, value);
            break;
          case TYPE_INT:
            UpdateMaxSlot<int32_t>(tuple,
                                   (*slot_d)->null_indicator_offset(), slot, value);
            break;
          case TYPE_BIGINT:
            UpdateMaxSlot<int64_t>(tuple,
                                   (*slot_d)->null_indicator_offset(), slot, value);
            break;
          case TYPE_FLOAT:
            UpdateMaxSlot<float>(tuple, (*slot_d)->null_indicator_offset(), slot, value);
            break;
          case TYPE_DOUBLE:
            UpdateMaxSlot<double>(tuple, (*slot_d)->null_indicator_offset(), slot, value);
            break;
          case TYPE_TIMESTAMP:
            UpdateMaxSlot<TimestampValue>(tuple,
                                          (*slot_d)->null_indicator_offset(),
                                          slot, value);
            break;
          case TYPE_STRING:
            UpdateMaxStringSlot(agg_out_tuple, (*slot_d)->null_indicator_offset(), 
                                string_slot_idx, slot, value);
            break;
          default:
            DCHECK(false) << "invalid type: " << TypeToString(agg_expr->type());
        };
        break;

      case TAggregationOp::SUM:
        switch (agg_expr->type()) {
          case TYPE_BIGINT:
            UpdateSumSlot<int64_t>(tuple,
                                   (*slot_d)->null_indicator_offset(), slot, value);
            break;
          case TYPE_DOUBLE:
            UpdateSumSlot<double>(tuple, (*slot_d)->null_indicator_offset(), slot, value);
            break;
          default:
            DCHECK(false) << "invalid type: " << TypeToString(agg_expr->type());
        };
        break;

      default:
        DCHECK(false) << "bad aggregate operator: " << agg_expr->agg_op();
    }
  }
}

void AggregationNode::DebugString(int indentation_level, stringstream* out) const {
  *out << string(indentation_level * 2, ' ');
  *out << "AggregationNode(tuple_id=" << agg_tuple_id_
       << " probe_exprs=" << Expr::DebugString(probe_exprs_)
       << " agg_exprs=" << Expr::DebugString(aggregate_exprs_);
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
//    %dst_is_null = call i1 @IsNull({ i8, double }* %agg_tuple)
//    br i1 %dst_is_null, label %dst_null, label %dst_not_null
//  
//  dst_null:                                         ; preds = %src_not_null
//    call void @SetNotNull({ i8, double }* %agg_tuple)
//    store double %src_value, double* %dst_slot_ptr
//    br label %ret
//  
//  dst_not_null:                                     ; preds = %src_not_null
//    %dst_val = load double* %dst_slot_ptr
//    %0 = fadd double %dst_val, %src_value
//    store double %0, double* %dst_slot_ptr
//    br label %ret
//  
//  ret:                                    ; preds = %dst_not_null, %dst_null, %entry
//    ret void
//  }
llvm::Function* AggregationNode::CodegenUpdateSlot(LlvmCodeGen* codegen, int slot_idx) {
  AggregateExpr* agg_expr = static_cast<AggregateExpr*>(aggregate_exprs_[slot_idx]);
  SlotDescriptor* slot_desc = agg_tuple_desc_->slots()[probe_exprs_.size() + slot_idx];
  int field_idx = slot_desc->field_idx();

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
  Function* agg_expr_fn = agg_expr->codegen_fn();
  int scratch_buffer_size = agg_expr->scratch_buffer_size();
  DCHECK_EQ(scratch_buffer_size, 0);
  DCHECK(agg_expr_fn != NULL);
  if (agg_expr_fn == NULL) return NULL;
  
  BasicBlock* src_not_null_block, *ret_block;
  codegen->CreateIfElseBlocks(fn, "src_not_null", "ret", &src_not_null_block, &ret_block);

  Value* expr_args[] = { args[1], ConstantPointerNull::get(ptr_type), src_is_null_ptr };
  Value* src_value = agg_expr->CodegenGetValue(codegen, builder.GetInsertBlock(),
      expr_args, ret_block, src_not_null_block);
  
  // Src slot is not null, update dst_slot
  builder.SetInsertPoint(src_not_null_block);
  Value* dst_ptr = builder.CreateStructGEP(args[0], field_idx, "dst_slot_ptr");
  Value* result = NULL;
    
  // If the dst slot is non-nullable, just update dst, otherwise, check if the 
  // dst slot is null.
  if (slot_desc->is_nullable()) {
    BasicBlock* dst_null_block, *dst_not_null_block;
    codegen->CreateIfElseBlocks(fn, "dst_null", "dst_not_null_block", &dst_null_block,
        &dst_not_null_block, ret_block);

    // Call and check if dst slot is null
    Function* is_null_fn = slot_desc->CodegenIsNull(codegen, tuple_struct);
    Value* dst_is_null = builder.CreateCall(is_null_fn, args[0], "dst_is_null");
    builder.CreateCondBr(dst_is_null, dst_null_block, dst_not_null_block);
    
    // Dst is NULL, just update dst slot to src slot and clear null bit
    builder.SetInsertPoint(dst_null_block);
    Function* clear_null_fn = slot_desc->CodegenUpdateNull(codegen, tuple_struct, false);
    builder.CreateCall(clear_null_fn, args[0]);
    builder.CreateStore(src_value, dst_ptr);

    builder.CreateBr(ret_block);
    builder.SetInsertPoint(dst_not_null_block);
  }
    
  // Block where both src and dst are non-null
  Value* dst_value = builder.CreateLoad(dst_ptr, "dst_val");
  switch (agg_expr->agg_op()) {
    case TAggregationOp::COUNT:
      result = builder.CreateAdd(dst_value, 
          codegen->GetIntConstant(TYPE_BIGINT, 1), "count_inc");
      break;
    case TAggregationOp::MIN: {
      Function* min_fn = codegen->CodegenMinMax(agg_expr->type(), true);
      Value* min_args[] = { dst_value, src_value };
      result = builder.CreateCall(min_fn, min_args, "min_value");
      break;
    }
    case TAggregationOp::MAX: {
      Function* max_fn = codegen->CodegenMinMax(agg_expr->type(), false);
      Value* max_args[] = { dst_value, src_value };
      result = builder.CreateCall(max_fn, max_args, "max_value");
      break;
    }
    case TAggregationOp::SUM:
      if (agg_expr->type() == TYPE_FLOAT || agg_expr->type() == TYPE_DOUBLE) {
        result = builder.CreateFAdd(dst_value, src_value);
      } else {
        result = builder.CreateAdd(dst_value, src_value);
      }
      break;
    default:
      DCHECK(false) << "bad aggregate operator: " << agg_expr->agg_op();
  }
    
  builder.CreateStore(result, dst_ptr);
  builder.CreateBr(ret_block);

  builder.SetInsertPoint(ret_block);
  builder.CreateRetVoid();

  return codegen->FinalizeFunction(fn);
}

// IR codegen for the UpdateAggTuple loop.  This loop is query specific and
// based on the aggregate exprs.  The function signature must match the non-
// codegen'd UpdateAggTuple exactly.
// For the query:
// select count(*), count(int_col), sum(double_col) the IR looks like:
//
// define void @UpdateAggTuple(%"class.impala::AggregationNode"* %this_ptr,
//                             %"class.impala::AggregationTuple"* %agg_tuple, 
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
  COUNTER_SCOPED_TIMER(codegen->codegen_timer());
  for (int i = 0; i < agg_tuple_desc_->slots().size(); ++i) {
    SlotDescriptor* slot_desc = agg_tuple_desc_->slots()[i];
    if (slot_desc->type() == TYPE_STRING || slot_desc->type() == TYPE_TIMESTAMP) {
      // TODO:
      VLOG_QUERY << "Could not codegen UpdateAggTuple because "
                 << "string and timestamp slots are not yet supported.";
      return NULL;
    }
  } 
  for (int i = 0; i < aggregate_exprs_.size(); ++i) {
    AggregateExpr* agg_expr = static_cast<AggregateExpr*>(aggregate_exprs_[i]);
    // If the agg_expr can't be generated, bail generating this function
    if (!agg_expr->is_star() && agg_expr->codegen_fn() == NULL) {
      VLOG_QUERY << "Could not codegen UpdateAggTuple because the "
                 << "underlying exprs cannot be codegened.";
      return NULL;
    }
  }
  
  if (agg_tuple_desc_->GenerateLlvmStruct(codegen) == NULL) {
    VLOG_QUERY << "Could not codegen UpdateAggTuple because we could"
               << "not generate a  matching llvm struct for the result agg tuple.";
    return NULL;
  }

  // Get the types to match the UpdateAggTuple signature
  Type* agg_node_type = codegen->GetType(AggregationNode::LLVM_CLASS_NAME);
  Type* agg_tuple_type = codegen->GetType(AggregationTuple::LLVM_CLASS_NAME);
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
  for (int i = 0; i < aggregate_exprs_.size(); ++i) {
    AggregateExpr* agg_expr = static_cast<AggregateExpr*>(aggregate_exprs_[i]);
    SlotDescriptor* slot_desc = agg_tuple_desc_->slots()[probe_exprs_.size() + i];
    if (agg_expr->is_star()) {
      // TODO: we should be able to hoist this up to the loop over the batch and just
      // increment the slot by the number of rows in the batch.
      DCHECK_EQ(agg_expr->agg_op(), TAggregationOp::COUNT);
      int field_idx = slot_desc->field_idx();
      Value* const_one = codegen->GetIntConstant(TYPE_BIGINT, 1);
      Value* slot_ptr = builder.CreateStructGEP(args[1], field_idx, "src_slot");
      Value* slot_loaded = builder.CreateLoad(slot_ptr, "count_star_val");
      Value* count_inc = builder.CreateAdd(slot_loaded, const_one, "count_star_inc");
      builder.CreateStore(count_inc, slot_ptr);
    } else {
      Function* update_slot_fn = CodegenUpdateSlot(codegen, i);
      if (update_slot_fn == NULL) return NULL;
      builder.CreateCall2(update_slot_fn, args[1], args[2]);
    }
  }
  builder.CreateRetVoid();

  return codegen->FinalizeFunction(fn);
}

Function* AggregationNode::CodegenProcessRowBatch(
    LlvmCodeGen* codegen, Function* update_tuple_fn) {
  COUNTER_SCOPED_TIMER(codegen->codegen_timer());
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
  process_batch_fn = codegen->ReplaceCallSites(process_batch_fn, false, 
      update_tuple_fn, "UpdateAggTuple", &replaced); 
  DCHECK_EQ(replaced, 1) << "One call site should be replaced."; 
  DCHECK(process_batch_fn != NULL);

  return codegen->FinalizeFunction(process_batch_fn);
}

