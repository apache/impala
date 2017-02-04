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

#include <sstream>

#include "codegen/llvm-codegen.h"
#include "common/logging.h"
#include "exec/aggregation-node.h"
#include "exprs/aggregate-functions.h"
#include "exprs/anyval-util.h"
#include "exprs/expr-context.h"
#include "exprs/scalar-fn-call.h"
#include "runtime/lib-cache.h"
#include "runtime/raw-value.h"
#include "runtime/runtime-state.h"
#include "runtime/string-value.inline.h"
#include "udf/udf-internal.h"
#include "util/debug-util.h"

#include <thrift/protocol/TDebugProtocol.h>

#include "common/names.h"

using namespace impala;
using namespace impala_udf;
using namespace llvm;
using std::move;

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

Status AggFnEvaluator::Create(ObjectPool* pool, const TExpr& desc,
    AggFnEvaluator** result) {
  return Create(pool, desc, false, result);
}

Status AggFnEvaluator::Create(ObjectPool* pool, const TExpr& desc,
    bool is_analytic_fn, AggFnEvaluator** result) {
  DCHECK_GT(desc.nodes.size(), 0);
  *result = pool->Add(new AggFnEvaluator(desc.nodes[0], is_analytic_fn));
  int node_idx = 0;
  for (int i = 0; i < desc.nodes[0].num_children; ++i) {
    ++node_idx;
    Expr* expr = NULL;
    ExprContext* ctx = NULL;
    RETURN_IF_ERROR(Expr::CreateTreeFromThrift(
        pool, desc.nodes, NULL, &node_idx, &expr, &ctx));
    (*result)->input_expr_ctxs_.push_back(ctx);
  }
  return Status::OK();
}

AggFnEvaluator::AggFnEvaluator(const TExprNode& desc, bool is_analytic_fn)
  : fn_(desc.fn),
    is_merge_(desc.agg_expr.is_merge_agg),
    is_analytic_fn_(is_analytic_fn),
    intermediate_slot_desc_(NULL),
    output_slot_desc_(NULL),
    arg_type_descs_(AnyValUtil::ColumnTypesToTypeDescs(
        ColumnType::FromThrift(desc.agg_expr.arg_types))),
    cache_entry_(NULL),
    init_fn_(NULL),
    update_fn_(NULL),
    remove_fn_(NULL),
    merge_fn_(NULL),
    serialize_fn_(NULL),
    get_value_fn_(NULL),
    finalize_fn_(NULL) {
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
  } else if (fn_.name.function_name == "avg") {
    agg_op_ = AVG;
  } else if (fn_.name.function_name == "ndv" ||
      fn_.name.function_name == "ndv_no_finalize") {
    agg_op_ = NDV;
  } else {
    agg_op_ = OTHER;
  }
}

AggFnEvaluator::~AggFnEvaluator() {
  DCHECK(cache_entry_ == NULL) << "Need to call Close()";
}

Status AggFnEvaluator::Prepare(RuntimeState* state, const RowDescriptor& desc,
      const SlotDescriptor* intermediate_slot_desc,
      const SlotDescriptor* output_slot_desc,
      MemPool* agg_fn_pool, FunctionContext** agg_fn_ctx) {
  DCHECK(intermediate_slot_desc != NULL);
  DCHECK_EQ(intermediate_slot_desc->type().type,
      ColumnType::FromThrift(fn_.aggregate_fn.intermediate_type).type);
  DCHECK(intermediate_slot_desc_ == NULL);
  intermediate_slot_desc_ = intermediate_slot_desc;

  DCHECK(output_slot_desc != NULL);
  DCHECK_EQ(output_slot_desc->type().type, ColumnType::FromThrift(fn_.ret_type).type);
  DCHECK(output_slot_desc_ == NULL);
  output_slot_desc_ = output_slot_desc;

  RETURN_IF_ERROR(
      Expr::Prepare(input_expr_ctxs_, state, desc, agg_fn_pool->mem_tracker()));

  for (int i = 0; i < input_expr_ctxs_.size(); ++i) {
    AnyVal* staging_input_val;
    RETURN_IF_ERROR(
        AllocateAnyVal(state, agg_fn_pool, input_expr_ctxs_[i]->root()->type(),
            "Could not allocate aggregate expression input value", &staging_input_val));
    staging_input_vals_.push_back(staging_input_val);
  }
  RETURN_IF_ERROR(AllocateAnyVal(state, agg_fn_pool, intermediate_type(),
      "Could not allocate aggregate expression intermediate value",
      &staging_intermediate_val_));
  RETURN_IF_ERROR(AllocateAnyVal(state, agg_fn_pool, intermediate_type(),
      "Could not allocate aggregate expression merge input value",
      &staging_merge_input_val_));

  if (is_merge_) {
    DCHECK_EQ(staging_input_vals_.size(), 1) << "Merge should only have 1 input.";
  }

  // Load the function pointers. Merge is not required if this is evaluating an
  // analytic function.
  if (fn_.aggregate_fn.init_fn_symbol.empty() ||
      fn_.aggregate_fn.update_fn_symbol.empty() ||
      (!is_analytic_fn_ && fn_.aggregate_fn.merge_fn_symbol.empty())) {
    // This path is only for partially implemented builtins.
    DCHECK_EQ(fn_.binary_type, TFunctionBinaryType::BUILTIN);
    stringstream ss;
    ss << "Function " << fn_.name.function_name << " is not implemented.";
    return Status(ss.str());
  }

  RETURN_IF_ERROR(LibCache::instance()->GetSoFunctionPtr(
      fn_.hdfs_location, fn_.aggregate_fn.init_fn_symbol, &init_fn_, &cache_entry_));
  RETURN_IF_ERROR(LibCache::instance()->GetSoFunctionPtr(
      fn_.hdfs_location, fn_.aggregate_fn.update_fn_symbol, &update_fn_, &cache_entry_));

  // Merge() is not loaded if evaluating the agg fn as an analytic function.
  if (!is_analytic_fn_) {
    RETURN_IF_ERROR(LibCache::instance()->GetSoFunctionPtr(fn_.hdfs_location,
          fn_.aggregate_fn.merge_fn_symbol, &merge_fn_, &cache_entry_));
  }

  // Serialize(), GetValue(), Remove() and Finalize() are optional
  if (!fn_.aggregate_fn.serialize_fn_symbol.empty()) {
    RETURN_IF_ERROR(LibCache::instance()->GetSoFunctionPtr(
        fn_.hdfs_location, fn_.aggregate_fn.serialize_fn_symbol, &serialize_fn_,
        &cache_entry_));
  }
  if (!fn_.aggregate_fn.get_value_fn_symbol.empty()) {
    RETURN_IF_ERROR(LibCache::instance()->GetSoFunctionPtr(
        fn_.hdfs_location, fn_.aggregate_fn.get_value_fn_symbol, &get_value_fn_,
        &cache_entry_));
  }
  if (!fn_.aggregate_fn.remove_fn_symbol.empty()) {
    RETURN_IF_ERROR(LibCache::instance()->GetSoFunctionPtr(fn_.hdfs_location,
        fn_.aggregate_fn.remove_fn_symbol, &remove_fn_, &cache_entry_));
  }
  if (!fn_.aggregate_fn.finalize_fn_symbol.empty()) {
    RETURN_IF_ERROR(LibCache::instance()->GetSoFunctionPtr(fn_.hdfs_location,
        fn_.aggregate_fn.finalize_fn_symbol, &finalize_fn_, &cache_entry_));
  }
  *agg_fn_ctx = FunctionContextImpl::CreateContext(state, agg_fn_pool,
      GetIntermediateTypeDesc(), GetOutputTypeDesc(), arg_type_descs_);
  return Status::OK();
}

Status AggFnEvaluator::Open(RuntimeState* state, FunctionContext* agg_fn_ctx) {
  RETURN_IF_ERROR(Expr::Open(input_expr_ctxs_, state));
  // Now that we have opened all our input exprs, it is safe to evaluate any constant
  // values for the UDA's FunctionContext (we cannot evaluate exprs before calling Open()
  // on them).
  vector<AnyVal*> constant_args(input_expr_ctxs_.size());
  for (int i = 0; i < input_expr_ctxs_.size(); ++i) {
    ExprContext* input_ctx = input_expr_ctxs_[i];
    AnyVal* const_val;
    RETURN_IF_ERROR(input_ctx->root()->GetConstVal(state, input_ctx, &const_val));
    constant_args[i] = const_val;
  }
  agg_fn_ctx->impl()->SetConstantArgs(move(constant_args));
  return Status::OK();
}

void AggFnEvaluator::Close(RuntimeState* state) {
  Expr::Close(input_expr_ctxs_, state);

  if (cache_entry_ != NULL) {
    LibCache::instance()->DecrementUseCount(cache_entry_);
    cache_entry_ = NULL;
  }
}

inline void AggFnEvaluator::SetDstSlot(FunctionContext* ctx, const AnyVal* src,
    const SlotDescriptor* dst_slot_desc, Tuple* dst) {
  if (src->is_null) {
    dst->SetNull(dst_slot_desc->null_indicator_offset());
    return;
  }

  dst->SetNotNull(dst_slot_desc->null_indicator_offset());
  void* slot = dst->GetSlot(dst_slot_desc->tuple_offset());
  switch (dst_slot_desc->type().type) {
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
      if (slot != reinterpret_cast<const StringVal*>(src)->ptr) {
        ctx->SetError("UDA should not set pointer of CHAR(N) intermediate");
      }
      return;
    case TYPE_TIMESTAMP:
      *reinterpret_cast<TimestampValue*>(slot) = TimestampValue::FromTimestampVal(
          *reinterpret_cast<const TimestampVal*>(src));
      return;
    case TYPE_DECIMAL:
      switch (dst_slot_desc->type().GetByteSize()) {
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
          break;
      }
    default:
      DCHECK(false) << "NYI: " << dst_slot_desc->type();
  }
}

// This function would be replaced in codegen.
void AggFnEvaluator::Init(FunctionContext* agg_fn_ctx, Tuple* dst) {
  DCHECK(init_fn_ != NULL);
  for (ExprContext* ctx : input_expr_ctxs_) DCHECK(ctx->opened());

  if (intermediate_type().type == TYPE_CHAR) {
    // For type char, we want to initialize the staging_intermediate_val_ with
    // a pointer into the tuple (the UDA should not be allocating it).
    void* slot = dst->GetSlot(intermediate_slot_desc_->tuple_offset());
    StringVal* sv = reinterpret_cast<StringVal*>(staging_intermediate_val_);
    sv->is_null = dst->IsNull(intermediate_slot_desc_->null_indicator_offset());
    sv->ptr = reinterpret_cast<uint8_t*>(
        StringValue::CharSlotToPtr(slot, intermediate_type()));
    sv->len = intermediate_type().len;
  }
  reinterpret_cast<InitFn>(init_fn_)(agg_fn_ctx, staging_intermediate_val_);
  SetDstSlot(agg_fn_ctx, staging_intermediate_val_, intermediate_slot_desc_, dst);
  agg_fn_ctx->impl()->set_num_updates(0);
  agg_fn_ctx->impl()->set_num_removes(0);
}

static void SetAnyVal(const SlotDescriptor* desc, Tuple* tuple, AnyVal* dst) {
  bool is_null = tuple->IsNull(desc->null_indicator_offset());
  void* slot = NULL;
  if (!is_null) slot = tuple->GetSlot(desc->tuple_offset());
  AnyValUtil::SetAnyVal(slot, desc->type(), dst);
}

void AggFnEvaluator::Update(
    FunctionContext* agg_fn_ctx, const TupleRow* row, Tuple* dst, void* fn) {
  if (fn == NULL) return;

  SetAnyVal(intermediate_slot_desc_, dst, staging_intermediate_val_);

  for (int i = 0; i < input_expr_ctxs_.size(); ++i) {
    void* src_slot = input_expr_ctxs_[i]->GetValue(row);
    AnyValUtil::SetAnyVal(
        src_slot, input_expr_ctxs_[i]->root()->type(), staging_input_vals_[i]);
  }

  // TODO: this part is not so good and not scalable. It can be replaced with
  // codegen but we can also consider leaving it for the first few cases for
  // debugging.
  switch (input_expr_ctxs_.size()) {
    case 0:
      reinterpret_cast<UpdateFn0>(fn)(agg_fn_ctx, staging_intermediate_val_);
      break;
    case 1:
      reinterpret_cast<UpdateFn1>(fn)(agg_fn_ctx,
          *staging_input_vals_[0], staging_intermediate_val_);
      break;
    case 2:
      reinterpret_cast<UpdateFn2>(fn)(agg_fn_ctx,
          *staging_input_vals_[0], *staging_input_vals_[1], staging_intermediate_val_);
      break;
    case 3:
      reinterpret_cast<UpdateFn3>(fn)(agg_fn_ctx,
          *staging_input_vals_[0], *staging_input_vals_[1],
          *staging_input_vals_[2], staging_intermediate_val_);
      break;
    case 4:
      reinterpret_cast<UpdateFn4>(fn)(agg_fn_ctx,
          *staging_input_vals_[0], *staging_input_vals_[1],
          *staging_input_vals_[2], *staging_input_vals_[3], staging_intermediate_val_);
      break;
    case 5:
      reinterpret_cast<UpdateFn5>(fn)(agg_fn_ctx,
          *staging_input_vals_[0], *staging_input_vals_[1],
          *staging_input_vals_[2], *staging_input_vals_[3],
          *staging_input_vals_[4], staging_intermediate_val_);
      break;
    case 6:
      reinterpret_cast<UpdateFn6>(fn)(agg_fn_ctx,
          *staging_input_vals_[0], *staging_input_vals_[1],
          *staging_input_vals_[2], *staging_input_vals_[3],
          *staging_input_vals_[4], *staging_input_vals_[5], staging_intermediate_val_);
      break;
    case 7:
      reinterpret_cast<UpdateFn7>(fn)(agg_fn_ctx,
          *staging_input_vals_[0], *staging_input_vals_[1],
          *staging_input_vals_[2], *staging_input_vals_[3],
          *staging_input_vals_[4], *staging_input_vals_[5],
          *staging_input_vals_[6], staging_intermediate_val_);
      break;
    case 8:
      reinterpret_cast<UpdateFn8>(fn)(agg_fn_ctx,
          *staging_input_vals_[0], *staging_input_vals_[1],
          *staging_input_vals_[2], *staging_input_vals_[3],
          *staging_input_vals_[4], *staging_input_vals_[5],
          *staging_input_vals_[6], *staging_input_vals_[7],
          staging_intermediate_val_);
      break;
    default:
      DCHECK(false) << "NYI";
  }
  SetDstSlot(agg_fn_ctx, staging_intermediate_val_, intermediate_slot_desc_, dst);
}

void AggFnEvaluator::Merge(FunctionContext* agg_fn_ctx, Tuple* src, Tuple* dst) {
  DCHECK(merge_fn_ != NULL);

  SetAnyVal(intermediate_slot_desc_, dst, staging_intermediate_val_);
  SetAnyVal(intermediate_slot_desc_, src, staging_merge_input_val_);

  // The merge fn always takes one input argument.
  reinterpret_cast<UpdateFn1>(merge_fn_)(agg_fn_ctx,
      *staging_merge_input_val_, staging_intermediate_val_);
  SetDstSlot(agg_fn_ctx, staging_intermediate_val_, intermediate_slot_desc_, dst);
}

void AggFnEvaluator::SerializeOrFinalize(FunctionContext* agg_fn_ctx, Tuple* src,
    const SlotDescriptor* dst_slot_desc, Tuple* dst, void* fn) {
  // No fn was given and the src and dst are identical. Nothing to be done.
  if (fn == NULL && src == dst) return;
  // src != dst means we are performing a Finalize(), so even if fn == null we
  // still must copy the value of the src slot into dst.

  bool src_slot_null = src->IsNull(intermediate_slot_desc_->null_indicator_offset());
  void* src_slot = NULL;
  if (!src_slot_null) src_slot = src->GetSlot(intermediate_slot_desc_->tuple_offset());

  // No fn was given but the src and dst tuples are different (doing a Finalize()).
  // Just copy the src slot into the dst tuple.
  if (fn == NULL) {
    DCHECK_EQ(intermediate_type(), dst_slot_desc->type());
    RawValue::Write(src_slot, dst, dst_slot_desc, NULL);
    return;
  }

  AnyValUtil::SetAnyVal(src_slot, intermediate_type(), staging_intermediate_val_);
  switch (dst_slot_desc->type().type) {
    case TYPE_BOOLEAN: {
      typedef BooleanVal(*Fn)(FunctionContext*, AnyVal*);
      BooleanVal v = reinterpret_cast<Fn>(fn)(agg_fn_ctx, staging_intermediate_val_);
      SetDstSlot(agg_fn_ctx, &v, dst_slot_desc, dst);
      break;
    }
    case TYPE_TINYINT: {
      typedef TinyIntVal(*Fn)(FunctionContext*, AnyVal*);
      TinyIntVal v = reinterpret_cast<Fn>(fn)(agg_fn_ctx, staging_intermediate_val_);
      SetDstSlot(agg_fn_ctx, &v, dst_slot_desc, dst);
      break;
    }
    case TYPE_SMALLINT: {
      typedef SmallIntVal(*Fn)(FunctionContext*, AnyVal*);
      SmallIntVal v = reinterpret_cast<Fn>(fn)(agg_fn_ctx, staging_intermediate_val_);
      SetDstSlot(agg_fn_ctx, &v, dst_slot_desc, dst);
      break;
    }
    case TYPE_INT: {
      typedef IntVal(*Fn)(FunctionContext*, AnyVal*);
      IntVal v = reinterpret_cast<Fn>(fn)(agg_fn_ctx, staging_intermediate_val_);
      SetDstSlot(agg_fn_ctx, &v, dst_slot_desc, dst);
      break;
    }
    case TYPE_BIGINT: {
      typedef BigIntVal(*Fn)(FunctionContext*, AnyVal*);
      BigIntVal v = reinterpret_cast<Fn>(fn)(agg_fn_ctx, staging_intermediate_val_);
      SetDstSlot(agg_fn_ctx, &v, dst_slot_desc, dst);
      break;
    }
    case TYPE_FLOAT: {
      typedef FloatVal(*Fn)(FunctionContext*, AnyVal*);
      FloatVal v = reinterpret_cast<Fn>(fn)(agg_fn_ctx, staging_intermediate_val_);
      SetDstSlot(agg_fn_ctx, &v, dst_slot_desc, dst);
      break;
    }
    case TYPE_DOUBLE: {
      typedef DoubleVal(*Fn)(FunctionContext*, AnyVal*);
      DoubleVal v = reinterpret_cast<Fn>(fn)(agg_fn_ctx, staging_intermediate_val_);
      SetDstSlot(agg_fn_ctx, &v, dst_slot_desc, dst);
      break;
    }
    case TYPE_STRING:
    case TYPE_VARCHAR: {
      typedef StringVal(*Fn)(FunctionContext*, AnyVal*);
      StringVal v = reinterpret_cast<Fn>(fn)(agg_fn_ctx, staging_intermediate_val_);
      SetDstSlot(agg_fn_ctx, &v, dst_slot_desc, dst);
      break;
    }
    case TYPE_DECIMAL: {
      typedef DecimalVal(*Fn)(FunctionContext*, AnyVal*);
      DecimalVal v = reinterpret_cast<Fn>(fn)(agg_fn_ctx, staging_intermediate_val_);
      SetDstSlot(agg_fn_ctx, &v, dst_slot_desc, dst);
      break;
    }
    case TYPE_TIMESTAMP: {
      typedef TimestampVal(*Fn)(FunctionContext*, AnyVal*);
      TimestampVal v = reinterpret_cast<Fn>(fn)(agg_fn_ctx, staging_intermediate_val_);
      SetDstSlot(agg_fn_ctx, &v, dst_slot_desc, dst);
      break;
    }
    default:
      DCHECK(false) << "NYI";
  }
}

/// Gets the update or merge function for this UDA.
Status AggFnEvaluator::GetUpdateOrMergeFunction(LlvmCodeGen* codegen, Function** uda_fn) {
  const string& symbol =
      is_merge_ ? fn_.aggregate_fn.merge_fn_symbol : fn_.aggregate_fn.update_fn_symbol;
  vector<ColumnType> fn_arg_types;
  for (ExprContext* input_expr_ctx : input_expr_ctxs_) {
    fn_arg_types.push_back(input_expr_ctx->root()->type());
  }
  // The intermediate value is passed as the last argument.
  fn_arg_types.push_back(intermediate_type());
  RETURN_IF_ERROR(codegen->LoadFunction(fn_, symbol, NULL, fn_arg_types,
      fn_arg_types.size(), false, uda_fn, &cache_entry_));

  // Inline constants into the function body (if there is an IR body).
  if (!(*uda_fn)->isDeclaration()) {
    // TODO: IMPALA-4785: we should also replace references to GetIntermediateType()
    // with constants.
    codegen->InlineConstFnAttrs(GetOutputTypeDesc(), arg_type_descs_, *uda_fn);
    *uda_fn = codegen->FinalizeFunction(*uda_fn);
    if (*uda_fn == NULL) {
      return Status(TErrorCode::UDF_VERIFY_FAILED, symbol, fn_.hdfs_location);
    }
  }
  return Status::OK();
}

FunctionContext::TypeDesc AggFnEvaluator::GetIntermediateTypeDesc() const {
  return AnyValUtil::ColumnTypeToTypeDesc(intermediate_slot_desc_->type());
}

FunctionContext::TypeDesc AggFnEvaluator::GetOutputTypeDesc() const {
  return AnyValUtil::ColumnTypeToTypeDesc(output_slot_desc_->type());
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
  for (int i = 0; i < input_expr_ctxs_.size(); ++i) {
    out << " " << input_expr_ctxs_[i]->root()->DebugString() << ")";
  }
  out << ")";
  return out.str();
}
