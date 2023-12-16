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

#include "exprs/agg-fn-evaluator.h"

#include <endian.h>
#include <string.h>
#include <cstdint>
#include <sstream>
#include <utility>

#include "common/compiler-util.h"
#include "common/logging.h"
#include "common/object-pool.h"
#include "exprs/anyval-util.h"
#include "exprs/scalar-expr-evaluator.h"
#include "exprs/scalar-expr.h"
#include "gutil/strings/substitute.h"
#include "runtime/date-value.h"
#include "runtime/descriptors.h"
#include "runtime/raw-value.h"
#include "runtime/string-value.h"
#include "runtime/timestamp-value.h"
#include "runtime/tuple.h"
#include "runtime/types.h"
#include "udf/udf-internal.h"

#include "common/names.h"

using namespace impala;
using namespace impala_udf;

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
typedef AnyVal (*GetValueFn)(FunctionContext*, const AnyVal&);
typedef AnyVal (*FinalizeFn)(FunctionContext*, const AnyVal&);

const char* AggFnEvaluator::LLVM_CLASS_NAME = "class.impala::AggFnEvaluator";

AggFnEvaluator::AggFnEvaluator(const AggFn& agg_fn, bool is_clone)
  : is_clone_(is_clone),
    agg_fn_(agg_fn) {
}

AggFnEvaluator::~AggFnEvaluator() {
  DCHECK(closed_);
}

const SlotDescriptor& AggFnEvaluator::intermediate_slot_desc() const {
  return agg_fn_.intermediate_slot_desc();
}

const ColumnType& AggFnEvaluator::intermediate_type() const {
  return agg_fn_.intermediate_type();
}

Status AggFnEvaluator::Create(const AggFn& agg_fn, RuntimeState* state, ObjectPool* pool,
    MemPool* expr_perm_pool, MemPool* expr_results_pool, AggFnEvaluator** result) {
  *result = nullptr;

  // Create a new AggFn evaluator.
  AggFnEvaluator* agg_fn_eval = pool->Add(new AggFnEvaluator(agg_fn, false));
  agg_fn_eval->agg_fn_ctx_.reset(FunctionContextImpl::CreateContext(state, expr_perm_pool,
      expr_results_pool, agg_fn.GetIntermediateTypeDesc(), agg_fn.GetOutputTypeDesc(),
      agg_fn.arg_type_descs()));

  Status status;
  // Create the evaluators for the input expressions.
  for (const ScalarExpr* input_expr : agg_fn.children()) {
    ScalarExprEvaluator* input_eval;
    status = ScalarExprEvaluator::Create(
        *input_expr, state, pool, expr_perm_pool, expr_results_pool, &input_eval);
    if (UNLIKELY(!status.ok())) goto cleanup;
    agg_fn_eval->input_evals_.push_back(input_eval);
    DCHECK(&input_eval->root() == input_expr);

    AnyVal* staging_input_val;
    status = AllocateAnyVal(state, expr_perm_pool, input_expr->type(),
        "Could not allocate aggregate expression input value", &staging_input_val);
    agg_fn_eval->staging_input_vals_.push_back(staging_input_val);
    if (UNLIKELY(!status.ok())) goto cleanup;
  }
  DCHECK_EQ(agg_fn.GetNumChildren(), agg_fn_eval->input_evals_.size());
  DCHECK_EQ(agg_fn_eval->staging_input_vals_.size(), agg_fn_eval->input_evals_.size());

  status = AllocateAnyVal(state, expr_perm_pool, agg_fn.intermediate_type(),
      "Could not allocate aggregate expression intermediate value",
      &(agg_fn_eval->staging_intermediate_val_));
  if (UNLIKELY(!status.ok())) goto cleanup;
  status = AllocateAnyVal(state, expr_perm_pool, agg_fn.intermediate_type(),
      "Could not allocate aggregate expression merge input value",
      &(agg_fn_eval->staging_merge_input_val_));
  if (UNLIKELY(!status.ok())) goto cleanup;

  if (agg_fn.is_merge()) {
    DCHECK_EQ(agg_fn_eval->staging_input_vals_.size(), 1)
        << "Merge should only have 1 input.";
  }
  *result = agg_fn_eval;
  return Status::OK();

cleanup:
  DCHECK(!status.ok());
  agg_fn_eval->Close(state);
  return status;
}

Status AggFnEvaluator::Create(const vector<AggFn*>& agg_fns, RuntimeState* state,
    ObjectPool* pool, MemPool* expr_perm_pool, MemPool* expr_results_pool,
    vector<AggFnEvaluator*>* evals) {
  for (const AggFn* agg_fn : agg_fns) {
    AggFnEvaluator* agg_fn_eval;
    RETURN_IF_ERROR(AggFnEvaluator::Create(*agg_fn, state, pool, expr_perm_pool,
        expr_results_pool, &agg_fn_eval));
    evals->push_back(agg_fn_eval);
  }
  return Status::OK();
}

Status AggFnEvaluator::Open(RuntimeState* state) {
  if (opened_) return Status::OK();
  opened_ = true;
  RETURN_IF_ERROR(ScalarExprEvaluator::Open(input_evals_, state));
  // Now that we have opened all our input exprs, it is safe to evaluate any constant
  // values for the UDA's FunctionContext (we cannot evaluate exprs before calling Open()
  // on them).
  vector<AnyVal*> constant_args(input_evals_.size(), nullptr);
  for (int i = 0; i < input_evals_.size(); ++i) {
    ScalarExprEvaluator* eval = input_evals_[i];
    RETURN_IF_ERROR(eval->GetConstValue(state, *(agg_fn_.GetChild(i)),
        &constant_args[i]));
  }
  agg_fn_ctx_->impl()->SetConstantArgs(move(constant_args));
  return Status::OK();
}

Status AggFnEvaluator::Open(
    const vector<AggFnEvaluator*>& evals, RuntimeState* state) {
  for (AggFnEvaluator* eval : evals) RETURN_IF_ERROR(eval->Open(state));
  return Status::OK();
}

void AggFnEvaluator::Close(RuntimeState* state) {
  if (closed_) return;
  closed_ = true;
  if (!is_clone_) ScalarExprEvaluator::Close(input_evals_, state);
  agg_fn_ctx_->impl()->Close();
  agg_fn_ctx_.reset();
  input_evals_.clear();
}

void AggFnEvaluator::Close(
    const vector<AggFnEvaluator*>& evals, RuntimeState* state) {
  for (AggFnEvaluator* eval : evals) eval->Close(state);
}

void AggFnEvaluator::SetDstSlot(const AnyVal* src, const SlotDescriptor& dst_slot_desc,
    Tuple* dst) {
  if (src->is_null) {
    dst->SetNull(dst_slot_desc.null_indicator_offset());
    return;
  }

  dst->SetNotNull(dst_slot_desc.null_indicator_offset());
  void* slot = dst->GetSlot(dst_slot_desc.tuple_offset());
  switch (dst_slot_desc.type().type) {
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
    case TYPE_VARCHAR:
      *reinterpret_cast<StringValue*>(slot) =
          StringValue::FromStringVal(*reinterpret_cast<const StringVal*>(src));
      return;
    case TYPE_CHAR:
    case TYPE_FIXED_UDA_INTERMEDIATE:
      if (UNLIKELY(slot != reinterpret_cast<const StringVal*>(src)->ptr)) {
        agg_fn_ctx_->SetError(Substitute("UDA should not set pointer of $0 intermediate",
              dst_slot_desc.type().DebugString()).c_str());
      }
      return;
    case TYPE_TIMESTAMP:
      *reinterpret_cast<TimestampValue*>(slot) = TimestampValue::FromTimestampVal(
          *reinterpret_cast<const TimestampVal*>(src));
      return;
    case TYPE_DECIMAL:
      switch (dst_slot_desc.type().GetByteSize()) {
        case 4:
          *reinterpret_cast<int32_t*>(slot) =
              reinterpret_cast<const DecimalVal*>(src)->val4;
          return;
        case 8:
          *reinterpret_cast<int64_t*>(slot) =
              reinterpret_cast<const DecimalVal*>(src)->val8;
          return;
        case 16:
#if __BYTE_ORDER == __LITTLE_ENDIAN
          // On little endian, &val4, &val8, &val16 are the same address.
          // This code seems to trip up clang causing it to generate code that crashes.
          // Be careful when modifying this. See IMPALA-959 for more details.
          // I suspect an issue with xmm registers not reading from aligned memory.
          memcpy(slot, &reinterpret_cast<const DecimalVal*>(src)->val16, 16);
#else
          DCHECK(false) << "Not implemented.";
#endif
          return;
        default:
          DCHECK(false) << "Unknown decimal byte size: "
                        << dst_slot_desc.type().GetByteSize();
          return;
      }
    case TYPE_DATE:
      *reinterpret_cast<DateValue*>(slot) =
          DateValue::FromDateVal(*reinterpret_cast<const DateVal*>(src));
      return;
    default:
      DCHECK(false) << "NYI: " << dst_slot_desc.type();
  }
}

// This function would be replaced in codegen.
void AggFnEvaluator::Init(Tuple* dst) {
  DCHECK(opened_);
  DCHECK(agg_fn_.init_fn_ != nullptr);
  for (ScalarExprEvaluator* input_eval : input_evals_) {
    DCHECK(input_eval->opened());
  }

  const ColumnType& type = intermediate_type();
  const SlotDescriptor& slot_desc = intermediate_slot_desc();
  if (type.type == TYPE_CHAR || type.type == TYPE_FIXED_UDA_INTERMEDIATE) {
    // The intermediate value is represented as a fixed-length buffer inline in the tuple.
    // The aggregate function writes to this buffer directly. staging_intermediate_val_
    // is a StringVal with a pointer to the slot and the length of the slot.
    void* slot = dst->GetSlot(slot_desc.tuple_offset());
    StringVal* sv = reinterpret_cast<StringVal*>(staging_intermediate_val_);
    sv->is_null = dst->IsNull(slot_desc.null_indicator_offset());
    sv->ptr = reinterpret_cast<uint8_t*>(slot);
    sv->len = type.len;
  }
  reinterpret_cast<InitFn>(agg_fn_.init_fn_)(
       agg_fn_ctx_.get(), staging_intermediate_val_);
  SetDstSlot(staging_intermediate_val_, slot_desc, dst);
  agg_fn_ctx_->impl()->set_num_updates(0);
  agg_fn_ctx_->impl()->set_num_removes(0);
}

static void SetAnyVal(const SlotDescriptor& desc, Tuple* tuple, AnyVal* dst) {
  bool is_null = tuple->IsNull(desc.null_indicator_offset());
  void* slot = nullptr;
  if (!is_null) slot = tuple->GetSlot(desc.tuple_offset());
  AnyValUtil::SetAnyVal(slot, desc.type(), dst);
}

void AggFnEvaluator::Update(const TupleRow* row, Tuple* dst, void* fn) {
  if (fn == nullptr) return;

  const SlotDescriptor& slot_desc = intermediate_slot_desc();
  SetAnyVal(slot_desc, dst, staging_intermediate_val_);

  for (int i = 0; i < input_evals_.size(); ++i) {
    void* src_slot = input_evals_[i]->GetValue(row);
    DCHECK(&input_evals_[i]->root() == agg_fn_.GetChild(i));
    AnyValUtil::SetAnyVal(src_slot, agg_fn_.GetChild(i)->type(), staging_input_vals_[i]);
  }

  // TODO: this part is not so good and not scalable. It can be replaced with
  // codegen but we can also consider leaving it for the first few cases for
  // debugging.
  switch (input_evals_.size()) {
    case 0:
      reinterpret_cast<UpdateFn0>(fn)(agg_fn_ctx_.get(), staging_intermediate_val_);
      break;
    case 1:
      reinterpret_cast<UpdateFn1>(fn)(agg_fn_ctx_.get(),
          *staging_input_vals_[0], staging_intermediate_val_);
      break;
    case 2:
      reinterpret_cast<UpdateFn2>(fn)(agg_fn_ctx_.get(),
          *staging_input_vals_[0], *staging_input_vals_[1], staging_intermediate_val_);
      break;
    case 3:
      reinterpret_cast<UpdateFn3>(fn)(agg_fn_ctx_.get(),
          *staging_input_vals_[0], *staging_input_vals_[1],
          *staging_input_vals_[2], staging_intermediate_val_);
      break;
    case 4:
      reinterpret_cast<UpdateFn4>(fn)(agg_fn_ctx_.get(),
          *staging_input_vals_[0], *staging_input_vals_[1],
          *staging_input_vals_[2], *staging_input_vals_[3], staging_intermediate_val_);
      break;
    case 5:
      reinterpret_cast<UpdateFn5>(fn)(agg_fn_ctx_.get(),
          *staging_input_vals_[0], *staging_input_vals_[1],
          *staging_input_vals_[2], *staging_input_vals_[3],
          *staging_input_vals_[4], staging_intermediate_val_);
      break;
    case 6:
      reinterpret_cast<UpdateFn6>(fn)(agg_fn_ctx_.get(),
          *staging_input_vals_[0], *staging_input_vals_[1],
          *staging_input_vals_[2], *staging_input_vals_[3],
          *staging_input_vals_[4], *staging_input_vals_[5], staging_intermediate_val_);
      break;
    case 7:
      reinterpret_cast<UpdateFn7>(fn)(agg_fn_ctx_.get(),
          *staging_input_vals_[0], *staging_input_vals_[1],
          *staging_input_vals_[2], *staging_input_vals_[3],
          *staging_input_vals_[4], *staging_input_vals_[5],
          *staging_input_vals_[6], staging_intermediate_val_);
      break;
    case 8:
      reinterpret_cast<UpdateFn8>(fn)(agg_fn_ctx_.get(),
          *staging_input_vals_[0], *staging_input_vals_[1],
          *staging_input_vals_[2], *staging_input_vals_[3],
          *staging_input_vals_[4], *staging_input_vals_[5],
          *staging_input_vals_[6], *staging_input_vals_[7],
          staging_intermediate_val_);
      break;
    default:
      DCHECK(false) << "NYI";
  }
  SetDstSlot(staging_intermediate_val_, slot_desc, dst);
}

void AggFnEvaluator::Merge(Tuple* src, Tuple* dst) {
  DCHECK(agg_fn_.merge_fn_ != nullptr);

  const SlotDescriptor& slot_desc = intermediate_slot_desc();
  SetAnyVal(slot_desc, dst, staging_intermediate_val_);
  SetAnyVal(slot_desc, src, staging_merge_input_val_);

  // The merge fn always takes one input argument.
  reinterpret_cast<UpdateFn1>(agg_fn_.merge_fn_)(agg_fn_ctx_.get(),
      *staging_merge_input_val_, staging_intermediate_val_);
  SetDstSlot(staging_intermediate_val_, slot_desc, dst);
}

void AggFnEvaluator::SerializeOrFinalize(Tuple* src,
    const SlotDescriptor& dst_slot_desc, Tuple* dst, void* fn) {
  // No fn was given and the src and dst are identical. Nothing to be done.
  if (fn == nullptr && src == dst) return;
  // src != dst means we are performing a Finalize(), so even if fn == null we
  // still must copy the value of the src slot into dst.

  const SlotDescriptor& slot_desc = intermediate_slot_desc();
  bool src_slot_null = src->IsNull(slot_desc.null_indicator_offset());
  void* src_slot = nullptr;
  if (!src_slot_null) src_slot = src->GetSlot(slot_desc.tuple_offset());

  // No fn was given but the src and dst tuples are different (doing a Finalize()).
  // Just copy the src slot into the dst tuple.
  if (fn == nullptr) {
    DCHECK_EQ(intermediate_type(), dst_slot_desc.type());
    RawValue::Write(src_slot, dst, &dst_slot_desc, nullptr);
    return;
  }

  AnyValUtil::SetAnyVal(src_slot, intermediate_type(), staging_intermediate_val_);
  switch (dst_slot_desc.type().type) {
    case TYPE_BOOLEAN: {
      typedef BooleanVal(*Fn)(FunctionContext*, AnyVal*);
      BooleanVal v = reinterpret_cast<Fn>(fn)(
          agg_fn_ctx_.get(), staging_intermediate_val_);
      SetDstSlot(&v, dst_slot_desc, dst);
      break;
    }
    case TYPE_TINYINT: {
      typedef TinyIntVal(*Fn)(FunctionContext*, AnyVal*);
      TinyIntVal v = reinterpret_cast<Fn>(fn)(
          agg_fn_ctx_.get(), staging_intermediate_val_);
      SetDstSlot(&v, dst_slot_desc, dst);
      break;
    }
    case TYPE_SMALLINT: {
      typedef SmallIntVal(*Fn)(FunctionContext*, AnyVal*);
      SmallIntVal v = reinterpret_cast<Fn>(fn)(
          agg_fn_ctx_.get(), staging_intermediate_val_);
      SetDstSlot(&v, dst_slot_desc, dst);
      break;
    }
    case TYPE_INT: {
      typedef IntVal(*Fn)(FunctionContext*, AnyVal*);
      IntVal v = reinterpret_cast<Fn>(fn)(
          agg_fn_ctx_.get(), staging_intermediate_val_);
      SetDstSlot(&v, dst_slot_desc, dst);
      break;
    }
    case TYPE_BIGINT: {
      typedef BigIntVal(*Fn)(FunctionContext*, AnyVal*);
      BigIntVal v = reinterpret_cast<Fn>(fn)(
          agg_fn_ctx_.get(), staging_intermediate_val_);
      SetDstSlot(&v, dst_slot_desc, dst);
      break;
    }
    case TYPE_FLOAT: {
      typedef FloatVal(*Fn)(FunctionContext*, AnyVal*);
      FloatVal v = reinterpret_cast<Fn>(fn)(
          agg_fn_ctx_.get(), staging_intermediate_val_);
      SetDstSlot(&v, dst_slot_desc, dst);
      break;
    }
    case TYPE_DOUBLE: {
      typedef DoubleVal(*Fn)(FunctionContext*, AnyVal*);
      DoubleVal v = reinterpret_cast<Fn>(fn)(
          agg_fn_ctx_.get(), staging_intermediate_val_);
      SetDstSlot(&v, dst_slot_desc, dst);
      break;
    }
    case TYPE_STRING:
    case TYPE_VARCHAR: {
      typedef StringVal(*Fn)(FunctionContext*, AnyVal*);
      StringVal v = reinterpret_cast<Fn>(fn)(
          agg_fn_ctx_.get(), staging_intermediate_val_);
      SetDstSlot(&v, dst_slot_desc, dst);
      break;
    }
    case TYPE_DECIMAL: {
      typedef DecimalVal(*Fn)(FunctionContext*, AnyVal*);
      DecimalVal v = reinterpret_cast<Fn>(fn)(
          agg_fn_ctx_.get(), staging_intermediate_val_);
      SetDstSlot(&v, dst_slot_desc, dst);
      break;
    }
    case TYPE_TIMESTAMP: {
      typedef TimestampVal(*Fn)(FunctionContext*, AnyVal*);
      TimestampVal v = reinterpret_cast<Fn>(fn)(
          agg_fn_ctx_.get(), staging_intermediate_val_);
      SetDstSlot(&v, dst_slot_desc, dst);
      break;
    }
    case TYPE_CHAR:
    case TYPE_FIXED_UDA_INTERMEDIATE: {
      // Serialize() or Finalize() may rewrite the data in place, but must return the
      // same pointer.
      typedef StringVal(*Fn)(FunctionContext*, AnyVal*);
      StringVal v = reinterpret_cast<Fn>(fn)(
          agg_fn_ctx_.get(), staging_intermediate_val_);
      if (UNLIKELY(dst->GetSlot(dst_slot_desc.tuple_offset()) != v.ptr)) {
        agg_fn_ctx_->SetError(Substitute("UDA Serialize() and Finalize() must return "
            "same pointer as input for $0 intermediate",
            dst_slot_desc.type().DebugString()).c_str());
      }
      break;
    }
    case TYPE_DATE: {
      typedef DateVal(*Fn)(FunctionContext*, AnyVal*);
      DateVal v = reinterpret_cast<Fn>(fn)(
          agg_fn_ctx_.get(), staging_intermediate_val_);
      SetDstSlot(&v, dst_slot_desc, dst);
      break;
    }
    default:
      DCHECK(false) << "NYI";
  }
}

void AggFnEvaluator::ShallowClone(ObjectPool* pool, MemPool* expr_perm_pool,
    MemPool* expr_results_pool, AggFnEvaluator** cloned_eval) const {
  DCHECK(opened_);
  *cloned_eval = pool->Add(new AggFnEvaluator(agg_fn_, true));
  (*cloned_eval)->agg_fn_ctx_.reset(
      agg_fn_ctx_->impl()->Clone(expr_perm_pool, expr_results_pool));
  DCHECK_EQ((*cloned_eval)->input_evals_.size(), 0);
  (*cloned_eval)->input_evals_ = input_evals_;
  (*cloned_eval)->staging_input_vals_ = staging_input_vals_;
  (*cloned_eval)->staging_intermediate_val_ = staging_intermediate_val_;
  (*cloned_eval)->staging_merge_input_val_ = staging_merge_input_val_;
  (*cloned_eval)->opened_ = true;
}

void AggFnEvaluator::ShallowClone(ObjectPool* pool, MemPool* expr_perm_pool,
    MemPool* expr_results_pool, const vector<AggFnEvaluator*>& evals,
    vector<AggFnEvaluator*>* cloned_evals) {
  for (const AggFnEvaluator* eval : evals) {
    AggFnEvaluator* cloned_eval;
    eval->ShallowClone(pool, expr_perm_pool, expr_results_pool, &cloned_eval);
    cloned_evals->push_back(cloned_eval);
  }
}

vector<ScopedResultsPool> ScopedResultsPool::Create(
      const vector<AggFnEvaluator*>& evals, MemPool* new_results_pool) {
  vector<ScopedResultsPool> result;
  result.reserve(evals.size());
  for (AggFnEvaluator* eval : evals) {
    result.emplace_back(eval, new_results_pool);
  }
  return result;
}
