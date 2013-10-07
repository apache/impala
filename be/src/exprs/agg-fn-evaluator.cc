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
typedef StringVal (*SerializeFn)(FunctionContext*, const StringVal&);
typedef AnyVal (*FinalizeFn)(FunctionContext*, const AnyVal&);

Status AggFnEvaluator::Create(ObjectPool* pool, const TAggregateFunction& desc,
    AggFnEvaluator** result) {
  *result = pool->Add(new AggFnEvaluator(desc));
  RETURN_IF_ERROR(
      Expr::CreateExprTrees(pool, desc.input_exprs, &(*result)->input_exprs_));
  return Status::OK;
}

AggFnEvaluator::AggFnEvaluator(const TAggregateFunction& desc)
  : return_type_(desc.return_type),
    intermediate_type_(desc.intermediate_type),
    function_type_(desc.binary_type),
    agg_op_(desc.op),
    hdfs_location_(desc.binary_location),
    init_fn_symbol_(desc.init_fn_name),
    update_fn_symbol_(desc.update_fn_name),
    merge_fn_symbol_(desc.merge_fn_name),
    serialize_fn_symbol_(desc.serialize_fn_name),
    finalize_fn_symbol_(desc.finalize_fn_name),
    output_slot_desc_(NULL) {
  if (desc.binary_type == TFunctionBinaryType::BUILTIN) {
    DCHECK_NE(agg_op_, TAggregationOp::INVALID);
  } else {
    DCHECK_EQ(desc.binary_type, TFunctionBinaryType::NATIVE);
    DCHECK(!hdfs_location_.empty());
    DCHECK(!init_fn_symbol_.empty());
    DCHECK(!update_fn_symbol_.empty());
    DCHECK(!merge_fn_symbol_.empty());
  }
}

Status AggFnEvaluator::Prepare(RuntimeState* state, const RowDescriptor& desc,
      MemPool* pool, const SlotDescriptor* output_slot_desc) {
  DCHECK(pool != NULL);
  DCHECK(output_slot_desc != NULL);
  DCHECK(output_slot_desc_ == NULL);
  output_slot_desc_ = output_slot_desc;
  ctx_.reset(FunctionContextImpl::CreateContext(pool));

  RETURN_IF_ERROR(Expr::Prepare(input_exprs_, state, desc, false));

  ObjectPool* obj_pool = state->obj_pool();
  for (int i = 0; i < input_exprs().size(); ++i) {
    staging_input_vals_.push_back(CreateAnyVal(obj_pool, input_exprs()[i]->type()));
  }
  staging_output_val_ = CreateAnyVal(obj_pool, output_slot_desc_->type());

  // TODO: this should be made identical for the builtin and UDA case by
  // putting all this logic in an improved opcode registry.
  if (function_type_ == TFunctionBinaryType::BUILTIN) {
    pair<TAggregationOp::type, PrimitiveType> key;
    if (is_count_star()) {
      key = make_pair(agg_op_, INVALID_TYPE);
    } else {
      DCHECK_GE(input_exprs().size(), 1);
      key = make_pair(agg_op_, input_exprs()[0]->type());
    }
    const OpcodeRegistry::AggFnDescriptor* fn_desc =
        OpcodeRegistry::Instance()->GetBuiltinAggFnDescriptor(key);
    DCHECK(fn_desc != NULL);
    fn_ptrs_ = *fn_desc;
  } else {
    DCHECK_EQ(function_type_, TFunctionBinaryType::NATIVE);
    // Load the function pointers.
    RETURN_IF_ERROR(state->lib_cache()->GetSoFunctionPtr(
        state->fs_cache(), hdfs_location_, init_fn_symbol_, &fn_ptrs_.init_fn));
    RETURN_IF_ERROR(state->lib_cache()->GetSoFunctionPtr(
        state->fs_cache(), hdfs_location_, update_fn_symbol_, &fn_ptrs_.update_fn));
    RETURN_IF_ERROR(state->lib_cache()->GetSoFunctionPtr(
        state->fs_cache(), hdfs_location_, merge_fn_symbol_, &fn_ptrs_.merge_fn));

    // Serialize and Finalize are optional
    if (!serialize_fn_symbol_.empty()) {
      RETURN_IF_ERROR(state->lib_cache()->GetSoFunctionPtr(
          state->fs_cache(), hdfs_location_, serialize_fn_symbol_,
          &fn_ptrs_.serialize_fn));
    }
    if (!finalize_fn_symbol_.empty()) {
      RETURN_IF_ERROR(state->lib_cache()->GetSoFunctionPtr(
          state->fs_cache(), hdfs_location_, finalize_fn_symbol_, &fn_ptrs_.finalize_fn));
    }
  }
  return Status::OK;
}

// Utility to put val into an AnyVal struct
inline void AggFnEvaluator::SetAnyVal(const void* slot,
    PrimitiveType type, AnyVal* dst) {
  if (slot == NULL) {
    dst->is_null = true;
    return;
  }

  dst->is_null = false;
  switch (type) {
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
  switch (output_slot_desc_->type()) {
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
  DCHECK(fn_ptrs_.init_fn != NULL);
  reinterpret_cast<InitFn>(fn_ptrs_.init_fn)(ctx_.get(), staging_output_val_);
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
    default:
      DCHECK(false) << "NYI";
  }
  SetOutputSlot(staging_output_val_, dst);
}

void AggFnEvaluator::Update(TupleRow* row, Tuple* dst) {
  return UpdateOrMerge(row, dst, fn_ptrs_.update_fn);
}

void AggFnEvaluator::Merge(TupleRow* row, Tuple* dst) {
  return UpdateOrMerge(row, dst, fn_ptrs_.merge_fn);
}

void AggFnEvaluator::Serialize(Tuple* tuple) {
  DCHECK_EQ(output_slot_desc_->type(), intermediate_type_.type);
  if (fn_ptrs_.serialize_fn == NULL) return;
  DCHECK_EQ(intermediate_type_.type, TYPE_STRING) << "NYI";

  bool slot_null = tuple->IsNull(output_slot_desc_->null_indicator_offset());
  void* slot = NULL;
  if (!slot_null) slot = tuple->GetSlot(output_slot_desc_->tuple_offset());
  SetAnyVal(slot, output_slot_desc_->type(), staging_output_val_);

  StringVal sv = reinterpret_cast<SerializeFn>(fn_ptrs_.serialize_fn)(
      ctx_.get(), *reinterpret_cast<StringVal*>(staging_output_val_));
  SetOutputSlot(&sv, tuple);
}

void AggFnEvaluator::Finalize(Tuple* tuple) {
  DCHECK_EQ(output_slot_desc_->type(), return_type_.type);
  if (fn_ptrs_.finalize_fn == NULL) return;

  bool slot_null = tuple->IsNull(output_slot_desc_->null_indicator_offset());
  void* slot = NULL;
  if (!slot_null) slot = tuple->GetSlot(output_slot_desc_->tuple_offset());
  SetAnyVal(slot, output_slot_desc_->type(), staging_output_val_);

  if (return_type_.type == TYPE_BIGINT) {
    typedef BigIntVal(*Fn)(FunctionContext*, AnyVal*);
    BigIntVal v = reinterpret_cast<Fn>(fn_ptrs_.finalize_fn)(
        ctx_.get(), staging_output_val_);
    SetOutputSlot(&v, tuple);
  } else if (return_type_.type == TYPE_STRING) {
    typedef StringVal(*Fn)(FunctionContext*, AnyVal*);
    StringVal v = reinterpret_cast<Fn>(fn_ptrs_.finalize_fn)(
        ctx_.get(), staging_output_val_);
    SetOutputSlot(&v, tuple);
  } else {
    DCHECK(false) << "NYI";
  }
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

