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

#include "exprs/agg-fn-evaluator.h"

#include <sstream>

#include "codegen/llvm-codegen.h"
#include "common/logging.h"
#include "exec/aggregation-node.h"
#include "exprs/aggregate-functions.h"
#include "exprs/anyval-util.h"
#include "runtime/lib-cache.h"
#include "runtime/runtime-state.h"
#include "udf/udf-internal.h"
#include "util/debug-util.h"

#include <thrift/protocol/TDebugProtocol.h>
using namespace impala;
using namespace impala_udf;
using namespace llvm;
using namespace std;

// typedef for builtin aggregate functions. Unfortunately, these type defs don't
// really work since the actual builtin is implemented not in terms of the base
// AnyVal* type. Due to this, there are lots of casts when we use these typedefs.
// TODO: these typedefs exists as wrappers to go from (TupleRow, Tuple) to the
// types the aggregation functions need. This needs to be done with codegen instead.
typedef void (*InitFn)(FunctionContext*, AnyVal*);
typedef void (*UpdateFn0)(FunctionContext*, AnyVal*);
typedef void (*UpdateFn1)(FunctionContext*, const AnyVal&, AnyVal*);
typedef void (*UpdateFn2)(FunctionContext*, const AnyVal&, const AnyVal&, AnyVal*);
typedef void (*UpdateFn3)(FunctionContext*, const AnyVal&, const AnyVal&,
    const AnyVal&, AnyVal*);
typedef void (*UpdateFn4)(FunctionContext*, const AnyVal&, const AnyVal&,
    const AnyVal&, const AnyVal&, AnyVal*);
typedef void (*UpdateFn5)(FunctionContext*, const AnyVal&, const AnyVal&,
    const AnyVal&, const AnyVal&, const AnyVal&, AnyVal*);
typedef void (*UpdateFn6)(FunctionContext*, const AnyVal&, const AnyVal&,
    const AnyVal&, const AnyVal&, const AnyVal&, const AnyVal&, AnyVal*);
typedef void (*UpdateFn7)(FunctionContext*, const AnyVal&, const AnyVal&,
    const AnyVal&, const AnyVal&, const AnyVal&, const AnyVal&, const AnyVal&, AnyVal*);
typedef void (*UpdateFn8)(FunctionContext*, const AnyVal&, const AnyVal&,
    const AnyVal&, const AnyVal&, const AnyVal&, const AnyVal&, const AnyVal&,
    const AnyVal&, AnyVal*);
typedef StringVal (*SerializeFn)(FunctionContext*, const StringVal&);
typedef AnyVal (*FinalizeFn)(FunctionContext*, const AnyVal&);

Status AggFnEvaluator::Create(ObjectPool* pool, const TExpr& desc,
    AggFnEvaluator** result) {
  DCHECK_GT(desc.nodes.size(), 0);
  *result = pool->Add(new AggFnEvaluator(desc.nodes[0]));
  int node_idx = 0;
  for (int i = 0; i < desc.nodes[0].num_children; ++i) {
    ++node_idx;
    Expr* expr = NULL;
    RETURN_IF_ERROR(Expr::CreateTreeFromThrift(
        pool, desc.nodes, NULL, &node_idx, &expr));
    (*result)->input_exprs_.push_back(expr);
  }
  return Status::OK;
}

AggFnEvaluator::AggFnEvaluator(const TExprNode& desc)
  : fn_(desc.fn),
    return_type_(desc.fn.ret_type),
    intermediate_type_(desc.fn.aggregate_fn.intermediate_type),
    output_slot_desc_(NULL),
    cache_entry_(NULL) {
  DCHECK(desc.fn.__isset.aggregate_fn);
  DCHECK(desc.node_type == TExprNodeType::AGGREGATE_EXPR);
  // TODO: remove. See comment with AggregationOp
  if (fn_.name.function_name == "count") {
    agg_op_ = COUNT;
  } else if (fn_.name.function_name == "min") {
    agg_op_ = MIN;
  } else if (fn_.name.function_name == "max") {
    agg_op_ = MAX;
  } else if (fn_.name.function_name == "sum") {
    agg_op_ = SUM;
  } else {
    agg_op_ = OTHER;
  }
}

AggFnEvaluator::~AggFnEvaluator() {
  DCHECK(cache_entry_ == NULL) << "Need to call Close()";
}

Status AggFnEvaluator::Prepare(RuntimeState* state, const RowDescriptor& desc,
      MemPool* pool, const SlotDescriptor* output_slot_desc) {
  DCHECK(pool != NULL);
  DCHECK(output_slot_desc != NULL);
  DCHECK(output_slot_desc_ == NULL);
  output_slot_desc_ = output_slot_desc;
  ctx_.reset(FunctionContextImpl::CreateContext(state, pool));

  RETURN_IF_ERROR(Expr::Prepare(input_exprs_, state, desc, false));

  ObjectPool* obj_pool = state->obj_pool();
  for (int i = 0; i < input_exprs().size(); ++i) {
    staging_input_vals_.push_back(CreateAnyVal(obj_pool, input_exprs()[i]->type().type));
  }
  staging_output_val_ = CreateAnyVal(obj_pool, output_slot_desc_->type().type);

  // Load the function pointers.
  if (fn_.aggregate_fn.init_fn_symbol.empty() ||
      fn_.aggregate_fn.update_fn_symbol.empty() ||
      fn_.aggregate_fn.merge_fn_symbol.empty()) {
    // This path is only for partially implemented builtins.
    DCHECK_EQ(fn_.binary_type, TFunctionBinaryType::BUILTIN);
    stringstream ss;
    ss << "Function " << fn_.name.function_name << " is not implemented.";
    return Status(ss.str());
  }

  RETURN_IF_ERROR(state->lib_cache()->GetSoFunctionPtr(
      state->fs_cache(), fn_.hdfs_location, fn_.aggregate_fn.init_fn_symbol,
      &init_fn_, &cache_entry_));
  RETURN_IF_ERROR(state->lib_cache()->GetSoFunctionPtr(
      state->fs_cache(), fn_.hdfs_location, fn_.aggregate_fn.update_fn_symbol,
      &update_fn_, NULL));
  RETURN_IF_ERROR(state->lib_cache()->GetSoFunctionPtr(
      state->fs_cache(), fn_.hdfs_location, fn_.aggregate_fn.merge_fn_symbol,
      &merge_fn_, NULL));

  // Serialize and Finalize are optional
  if (!fn_.aggregate_fn.serialize_fn_symbol.empty()) {
    RETURN_IF_ERROR(state->lib_cache()->GetSoFunctionPtr(
        state->fs_cache(), fn_.hdfs_location, fn_.aggregate_fn.serialize_fn_symbol,
        &serialize_fn_, NULL));
  } else {
    serialize_fn_ = NULL;
  }
  if (!fn_.aggregate_fn.finalize_fn_symbol.empty()) {
    RETURN_IF_ERROR(state->lib_cache()->GetSoFunctionPtr(
        state->fs_cache(), fn_.hdfs_location, fn_.aggregate_fn.finalize_fn_symbol,
        &finalize_fn_, NULL));
  } else {
    finalize_fn_ = NULL;
  }
  return Status::OK;
}

void AggFnEvaluator::Close(RuntimeState* state) {
  Expr::Close(input_exprs_, state);
  if (cache_entry_ != NULL) {
    state->lib_cache()->DecrementUseCount(cache_entry_);
    cache_entry_ = NULL;
  }
}

// Utility to put val into an AnyVal struct
inline void AggFnEvaluator::SetAnyVal(const void* slot,
    const ColumnType& type, AnyVal* dst) {
  if (slot == NULL) {
    dst->is_null = true;
    return;
  }

  dst->is_null = false;
  switch (type.type) {
    case TYPE_NULL: return;
    case TYPE_BOOLEAN:
      reinterpret_cast<BooleanVal*>(dst)->val = *reinterpret_cast<const bool*>(slot);
      return;
    case TYPE_TINYINT:
      reinterpret_cast<TinyIntVal*>(dst)->val = *reinterpret_cast<const int8_t*>(slot);
      return;
    case TYPE_SMALLINT:
      reinterpret_cast<SmallIntVal*>(dst)->val = *reinterpret_cast<const int16_t*>(slot);
      return;
    case TYPE_INT:
      reinterpret_cast<IntVal*>(dst)->val = *reinterpret_cast<const int32_t*>(slot);
      return;
    case TYPE_BIGINT:
      reinterpret_cast<BigIntVal*>(dst)->val = *reinterpret_cast<const int64_t*>(slot);
      return;
    case TYPE_FLOAT:
      reinterpret_cast<FloatVal*>(dst)->val = *reinterpret_cast<const float*>(slot);
      return;
    case TYPE_DOUBLE:
      reinterpret_cast<DoubleVal*>(dst)->val = *reinterpret_cast<const double*>(slot);
      return;
    case TYPE_STRING:
      reinterpret_cast<const StringValue*>(slot)->ToStringVal(
          reinterpret_cast<StringVal*>(dst));
      return;
    case TYPE_TIMESTAMP:
      reinterpret_cast<const TimestampValue*>(slot)->ToTimestampVal(
          reinterpret_cast<TimestampVal*>(dst));
      return;
    default:
      DCHECK(false) << "NYI";
  }
}

inline void AggFnEvaluator::SetOutputSlot(const AnyVal* src, Tuple* dst) {
  if (src->is_null) {
    dst->SetNull(output_slot_desc_->null_indicator_offset());
    return;
  }

  dst->SetNotNull(output_slot_desc_->null_indicator_offset());
  void* slot = dst->GetSlot(output_slot_desc_->tuple_offset());
  switch (output_slot_desc_->type().type) {
    case TYPE_NULL:
      return;
    case TYPE_BOOLEAN:
      *reinterpret_cast<bool*>(slot) = reinterpret_cast<const BooleanVal*>(src)->val;
      return;
    case TYPE_TINYINT:
      *reinterpret_cast<int8_t*>(slot) = reinterpret_cast<const TinyIntVal*>(src)->val;
      return;
    case TYPE_SMALLINT:
      *reinterpret_cast<int16_t*>(slot) = reinterpret_cast<const SmallIntVal*>(src)->val;
      return;
    case TYPE_INT:
      *reinterpret_cast<int32_t*>(slot) = reinterpret_cast<const IntVal*>(src)->val;
      return;
    case TYPE_BIGINT:
      *reinterpret_cast<int64_t*>(slot) = reinterpret_cast<const BigIntVal*>(src)->val;
      return;
    case TYPE_FLOAT:
      *reinterpret_cast<float*>(slot) = reinterpret_cast<const FloatVal*>(src)->val;
      return;
    case TYPE_DOUBLE:
      *reinterpret_cast<double*>(slot) = reinterpret_cast<const DoubleVal*>(src)->val;
      return;
    case TYPE_STRING:
      *reinterpret_cast<StringValue*>(slot) =
          StringValue::FromStringVal(*reinterpret_cast<const StringVal*>(src));
      return;
    case TYPE_TIMESTAMP:
      *reinterpret_cast<TimestampValue*>(slot) = TimestampValue::FromTimestampVal(
          *reinterpret_cast<const TimestampVal*>(src));
      return;
    default:
      DCHECK(false) << "NYI";
  }
}

// This function would be replaced in codegen.
void AggFnEvaluator::Init(Tuple* dst) {
  DCHECK(init_fn_ != NULL);
  reinterpret_cast<InitFn>(init_fn_)(ctx_.get(), staging_output_val_);
  SetOutputSlot(staging_output_val_, dst);
}

void AggFnEvaluator::UpdateOrMerge(TupleRow* row, Tuple* dst, void* fn) {
  if (fn == NULL) return;

  bool dst_null = dst->IsNull(output_slot_desc_->null_indicator_offset());
  void* dst_slot = NULL;
  if (!dst_null) dst_slot = dst->GetSlot(output_slot_desc_->tuple_offset());
  SetAnyVal(dst_slot, output_slot_desc_->type(), staging_output_val_);

  for (int i = 0; i < input_exprs().size(); ++i) {
    void* src_slot = input_exprs()[i]->GetValue(row);
    SetAnyVal(src_slot, input_exprs()[i]->type(), staging_input_vals_[i]);
  }

  // TODO: this part is not so good and not scalable. It can be replaced with
  // codegen but we can also consider leaving it for the first few cases for
  // debugging.
  switch (input_exprs().size()) {
    case 0:
      reinterpret_cast<UpdateFn0>(fn)(ctx_.get(), staging_output_val_);
      break;
    case 1:
      reinterpret_cast<UpdateFn1>(fn)(ctx_.get(),
          *staging_input_vals_[0], staging_output_val_);
      break;
    case 2:
      reinterpret_cast<UpdateFn2>(fn)(ctx_.get(),
          *staging_input_vals_[0], *staging_input_vals_[1], staging_output_val_);
      break;
    case 3:
      reinterpret_cast<UpdateFn3>(fn)(ctx_.get(),
          *staging_input_vals_[0], *staging_input_vals_[1],
          *staging_input_vals_[2], staging_output_val_);
      break;
    case 4:
      reinterpret_cast<UpdateFn4>(fn)(ctx_.get(),
          *staging_input_vals_[0], *staging_input_vals_[1],
          *staging_input_vals_[2], *staging_input_vals_[3], staging_output_val_);
      break;
    case 5:
      reinterpret_cast<UpdateFn5>(fn)(ctx_.get(),
          *staging_input_vals_[0], *staging_input_vals_[1],
          *staging_input_vals_[2], *staging_input_vals_[3],
          *staging_input_vals_[4], staging_output_val_);
      break;
    case 6:
      reinterpret_cast<UpdateFn6>(fn)(ctx_.get(),
          *staging_input_vals_[0], *staging_input_vals_[1],
          *staging_input_vals_[2], *staging_input_vals_[3],
          *staging_input_vals_[4], *staging_input_vals_[5], staging_output_val_);
      break;
    case 7:
      reinterpret_cast<UpdateFn7>(fn)(ctx_.get(),
          *staging_input_vals_[0], *staging_input_vals_[1],
          *staging_input_vals_[2], *staging_input_vals_[3],
          *staging_input_vals_[4], *staging_input_vals_[5],
          *staging_input_vals_[6], staging_output_val_);
      break;
    case 8:
      reinterpret_cast<UpdateFn8>(fn)(ctx_.get(),
          *staging_input_vals_[0], *staging_input_vals_[1],
          *staging_input_vals_[2], *staging_input_vals_[3],
          *staging_input_vals_[4], *staging_input_vals_[5],
          *staging_input_vals_[6], *staging_input_vals_[7],
          staging_output_val_);
      break;
    default:
      DCHECK(false) << "NYI";
  }
  SetOutputSlot(staging_output_val_, dst);
}

void AggFnEvaluator::Update(TupleRow* row, Tuple* dst) {
  return UpdateOrMerge(row, dst, update_fn_);
}

void AggFnEvaluator::Merge(TupleRow* row, Tuple* dst) {
  return UpdateOrMerge(row, dst, merge_fn_);
}

void AggFnEvaluator::SerializeOrFinalize(Tuple* tuple, void* fn) {
  DCHECK_EQ(output_slot_desc_->type().type, return_type_.type);
  if (fn == NULL) return;

  bool slot_null = tuple->IsNull(output_slot_desc_->null_indicator_offset());
  void* slot = NULL;
  if (!slot_null) slot = tuple->GetSlot(output_slot_desc_->tuple_offset());
  SetAnyVal(slot, output_slot_desc_->type(), staging_output_val_);

  switch (return_type_.type) {
    case TYPE_BOOLEAN: {
      typedef BooleanVal(*Fn)(FunctionContext*, AnyVal*);
      BooleanVal v = reinterpret_cast<Fn>(fn)(ctx_.get(), staging_output_val_);
      SetOutputSlot(&v, tuple);
      break;
    }
    case TYPE_TINYINT: {
      typedef TinyIntVal(*Fn)(FunctionContext*, AnyVal*);
      TinyIntVal v = reinterpret_cast<Fn>(fn)(ctx_.get(), staging_output_val_);
      SetOutputSlot(&v, tuple);
      break;
    }
    case TYPE_SMALLINT: {
      typedef SmallIntVal(*Fn)(FunctionContext*, AnyVal*);
      SmallIntVal v = reinterpret_cast<Fn>(fn)(ctx_.get(), staging_output_val_);
      SetOutputSlot(&v, tuple);
      break;
    }
    case TYPE_INT: {
      typedef IntVal(*Fn)(FunctionContext*, AnyVal*);
      IntVal v = reinterpret_cast<Fn>(fn)(ctx_.get(), staging_output_val_);
      SetOutputSlot(&v, tuple);
      break;
    }
    case TYPE_BIGINT: {
      typedef BigIntVal(*Fn)(FunctionContext*, AnyVal*);
      BigIntVal v = reinterpret_cast<Fn>(fn)(ctx_.get(), staging_output_val_);
      SetOutputSlot(&v, tuple);
      break;
    }
    case TYPE_FLOAT: {
      typedef FloatVal(*Fn)(FunctionContext*, AnyVal*);
      FloatVal v = reinterpret_cast<Fn>(fn)(ctx_.get(), staging_output_val_);
      SetOutputSlot(&v, tuple);
      break;
    }
    case TYPE_DOUBLE: {
      typedef DoubleVal(*Fn)(FunctionContext*, AnyVal*);
      DoubleVal v = reinterpret_cast<Fn>(fn)(ctx_.get(), staging_output_val_);
      SetOutputSlot(&v, tuple);
      break;
    }
    case TYPE_STRING: {
      typedef StringVal(*Fn)(FunctionContext*, AnyVal*);
      StringVal v = reinterpret_cast<Fn>(fn)(ctx_.get(), staging_output_val_);
      SetOutputSlot(&v, tuple);
      break;
    }
    default:
      DCHECK(false) << "NYI";
  }
}

void AggFnEvaluator::Serialize(Tuple* tuple) {
  SerializeOrFinalize(tuple, serialize_fn_);
}

void AggFnEvaluator::Finalize(Tuple* tuple) {
  SerializeOrFinalize(tuple, finalize_fn_);
}

string AggFnEvaluator::DebugString(const vector<AggFnEvaluator*>& exprs) {
  stringstream out;
  out << "[";
  for (int i = 0; i < exprs.size(); ++i) {
    out << (i == 0 ? "" : " ") << exprs[i]->DebugString();
  }
  out << "]";
  return out.str();
}

string AggFnEvaluator::DebugString() const {
  stringstream out;
  out << "AggFnEvaluator(op=" << agg_op_;
  for (int i = 0; i < input_exprs_.size(); ++i) {
    out << " " << input_exprs_[i]->DebugString() << ")";
  }
  out << ")";
  return out.str();
}

