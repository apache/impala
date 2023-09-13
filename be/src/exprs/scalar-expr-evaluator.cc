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

#include "exprs/scalar-expr-evaluator.h"

#include <sstream>

#include "common/object-pool.h"
#include "common/status.h"
#include "exprs/aggregate-functions.h"
#include "exprs/ai-functions.h"
#include "exprs/anyval-util.h"
#include "exprs/bit-byte-functions.h"
#include "exprs/case-expr.h"
#include "exprs/cast-functions.h"
#include "exprs/compound-predicates.h"
#include "exprs/conditional-functions.h"
#include "exprs/datasketches-functions.h"
#include "exprs/date-functions.h"
#include "exprs/decimal-functions.h"
#include "exprs/decimal-operators.h"
#include "exprs/hive-udf-call.h"
#include "exprs/iceberg-functions.h"
#include "exprs/in-predicate.h"
#include "exprs/is-not-empty-predicate.h"
#include "exprs/is-null-predicate.h"
#include "exprs/like-predicate.h"
#include "exprs/literal.h"
#include "exprs/mask-functions.h"
#include "exprs/math-functions.h"
#include "exprs/null-literal.h"
#include "exprs/operators.h"
#include "exprs/scalar-expr-evaluator.h"
#include "exprs/scalar-expr.inline.h"
#include "exprs/scalar-fn-call.h"
#include "exprs/slot-ref.h"
#include "exprs/string-functions.h"
#include "exprs/timestamp-functions.h"
#include "exprs/tuple-is-null-predicate.h"
#include "exprs/udf-builtins.h"
#include "exprs/utility-functions.h"
#include "runtime/date-value.h"
#include "runtime/decimal-value.inline.h"
#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"
#include "runtime/raw-value.inline.h"
#include "runtime/runtime-state.h"
#include "udf/udf-internal.h"

#include "common/names.h"

using namespace impala;
using namespace impala_udf;

const char* ScalarExprEvaluator::LLVM_CLASS_NAME = "class.impala::ScalarExprEvaluator";

ScalarExprEvaluator::ScalarExprEvaluator(
    const ScalarExpr& root, MemPool* expr_perm_pool, MemPool* expr_results_pool)
  : expr_perm_pool_(expr_perm_pool), root_(root) {}

ScalarExprEvaluator::~ScalarExprEvaluator() {
  DCHECK(!initialized_ || closed_);
}

Status ScalarExprEvaluator::Create(const ScalarExpr& root, RuntimeState* state,
    ObjectPool* pool, MemPool* expr_perm_pool, MemPool* expr_results_pool,
    ScalarExprEvaluator** eval) {
  *eval = pool->Add(new ScalarExprEvaluator(root, expr_perm_pool, expr_results_pool));
  if (root.fn_ctx_idx_end_ > 0) {
    (*eval)->fn_ctxs_.resize(root.fn_ctx_idx_end_, nullptr);
    (*eval)->CreateFnCtxs(state, root, expr_perm_pool, expr_results_pool);
    DCHECK_EQ((*eval)->fn_ctxs_.size(), root.fn_ctx_idx_end_);
    for (FunctionContext* fn_ctx : (*eval)->fn_ctxs_) DCHECK(fn_ctx != nullptr);
    (*eval)->fn_ctxs_ptr_ = (*eval)->fn_ctxs_.data();
  } else {
    DCHECK_EQ((*eval)->fn_ctxs_.size(), 0);
    DCHECK_EQ(root.fn_ctx_idx_end_, 0);
    DCHECK_EQ(root.fn_ctx_idx_, -1);
    DCHECK((*eval)->fn_ctxs_ptr_ == nullptr);
  }
  if (root.type().IsStructType()) {
    DCHECK(root.GetNumChildren() > 0);
    Status status = Create(root.children(), state, pool, expr_perm_pool,
        expr_results_pool, &((*eval)->childEvaluators_));
    DCHECK((*eval)->childEvaluators_.size() == root.GetNumChildren());
  }
  (*eval)->initialized_ = true;
  return Status::OK();
}

Status ScalarExprEvaluator::Create(const vector<ScalarExpr*>& exprs, RuntimeState* state,
    ObjectPool* pool, MemPool* expr_perm_pool, MemPool* expr_results_pool,
    vector<ScalarExprEvaluator*>* evals) {
  for (const ScalarExpr* expr : exprs) {
    ScalarExprEvaluator* eval;
    Status status = Create(*expr, state, pool, expr_perm_pool, expr_results_pool, &eval);
    // Always add the evaluator to the vector so it can be cleaned up.
    evals->push_back(eval);
    RETURN_IF_ERROR(status);
  }
  return Status::OK();
}

void ScalarExprEvaluator::CreateFnCtxs(RuntimeState* state, const ScalarExpr& expr,
    MemPool* expr_perm_pool, MemPool* expr_results_pool) {
  const int fn_ctx_idx = expr.fn_ctx_idx();
  const bool has_fn_ctx = fn_ctx_idx != -1;
  vector<FunctionContext::TypeDesc> arg_types;
  // It's not needed to create contexts for the children of structs here as Create() is
  // called recursively for each of their children and that will take care of the context
  // creation as well.
  if (!expr.type().IsStructType()) {
    for (const ScalarExpr* child : expr.children()) {
      CreateFnCtxs(state, *child, expr_perm_pool, expr_results_pool);
      if (has_fn_ctx) arg_types.push_back(
          AnyValUtil::ColumnTypeToTypeDesc(child->type()));
    }
  }
  if (has_fn_ctx) {
    FunctionContext::TypeDesc return_type =
        AnyValUtil::ColumnTypeToTypeDesc(expr.type());
    const int varargs_buffer_size = expr.ComputeVarArgsBufferSize();
    DCHECK_GE(fn_ctx_idx, 0);
    DCHECK_LT(fn_ctx_idx, fn_ctxs_.size());
    DCHECK(fn_ctxs_[fn_ctx_idx] == nullptr);
    fn_ctxs_[fn_ctx_idx] = FunctionContextImpl::CreateContext(state, expr_perm_pool,
        expr_results_pool, return_type, arg_types, varargs_buffer_size);
  }
}

Status ScalarExprEvaluator::Open(RuntimeState* state) {
  DCHECK(initialized_);
  if (opened_) return Status::OK();
  opened_ = true;
  // Fragment-local state is only initialized for original contexts. Clones inherit the
  // original's fragment state and only need to have thread-local state initialized.
  // TODO: Move FRAGMENT_LOCAL state to ScalarExpr. ScalarExprEvaluator should only
  // have THREAD_LOCAL state.
  FunctionContext::FunctionStateScope scope =
      is_clone_ ? FunctionContext::THREAD_LOCAL : FunctionContext::FRAGMENT_LOCAL;
  return root_.OpenEvaluator(scope, state, this);
}

Status ScalarExprEvaluator::Open(
    const vector<ScalarExprEvaluator*>& evals, RuntimeState* state) {
  for (int i = 0; i < evals.size(); ++i) RETURN_IF_ERROR(evals[i]->Open(state));
  return Status::OK();
}

void ScalarExprEvaluator::Close(RuntimeState* state) {
  if (closed_) return;
  FunctionContext::FunctionStateScope scope =
      is_clone_ ? FunctionContext::THREAD_LOCAL : FunctionContext::FRAGMENT_LOCAL;
  root_.CloseEvaluator(scope, state, this);
  for (int i = 0; i < fn_ctxs_.size(); ++i) {
    fn_ctxs_[i]->impl()->Close();
    delete fn_ctxs_[i];
  }
  fn_ctxs_.clear();
  for (ScalarExprEvaluator* child : childEvaluators_) child->Close(state);
  // Memory allocated by 'fn_ctx_' is still in the MemPools. It's the responsibility of
  // the owners of those pools to free it.
  closed_ = true;
}

void ScalarExprEvaluator::Close(
    const vector<ScalarExprEvaluator*>& evals, RuntimeState* state) {
  for (ScalarExprEvaluator* eval : evals) eval->Close(state);
}

Status ScalarExprEvaluator::Clone(ObjectPool* pool, RuntimeState* state,
    MemPool* expr_perm_pool, MemPool* expr_results_pool,
    ScalarExprEvaluator** cloned_eval) const {
  DCHECK(initialized_);
  DCHECK(opened_);
  *cloned_eval = pool->Add(
      new ScalarExprEvaluator(root_, expr_perm_pool, expr_results_pool));
  for (int i = 0; i < fn_ctxs_.size(); ++i) {
    (*cloned_eval)->fn_ctxs_.push_back(
        fn_ctxs_[i]->impl()->Clone(expr_perm_pool, expr_results_pool));
  }
  (*cloned_eval)->fn_ctxs_ptr_ = (*cloned_eval)->fn_ctxs_.data();
  (*cloned_eval)->is_clone_ = true;
  (*cloned_eval)->initialized_ = true;
  (*cloned_eval)->opened_ = true;
  (*cloned_eval)->output_scale_ = output_scale_;
  return root_.OpenEvaluator(FunctionContext::THREAD_LOCAL, state, *cloned_eval);
}

Status ScalarExprEvaluator::Clone(ObjectPool* pool, RuntimeState* state,
    MemPool* expr_perm_pool, MemPool* expr_results_pool,
    const vector<ScalarExprEvaluator*>& evals,
    vector<ScalarExprEvaluator*>* cloned_evals) {
  DCHECK(cloned_evals != nullptr);
  DCHECK(cloned_evals->empty());
  for (int i = 0; i < evals.size(); ++i) {
    ScalarExprEvaluator* cloned_eval;
    Status status =
        evals[i]->Clone(pool, state, expr_perm_pool, expr_results_pool, &cloned_eval);
    // Always add the evaluator to the vector so it can be cleaned up.
    cloned_evals->push_back(cloned_eval);
    RETURN_IF_ERROR(status);
  }
  return Status::OK();
}

Status ScalarExprEvaluator::GetError(int start_idx, int end_idx) const {
  DCHECK(opened_);
  end_idx = end_idx == -1 ? fn_ctxs_.size() : end_idx;
  DCHECK_GE(start_idx, 0);
  DCHECK_LE(end_idx, fn_ctxs_.size());
  for (int idx = start_idx; idx < end_idx; ++idx) {
    DCHECK_LT(idx, fn_ctxs_.size());
    FunctionContext* fn_ctx = fn_ctxs_[idx];
    if (fn_ctx->has_error()) return Status(fn_ctx->error_msg());
  }
  return Status::OK();
}

Status ScalarExprEvaluator::GetConstValue(RuntimeState* state, const ScalarExpr& expr,
    AnyVal** const_val) {
  DCHECK(opened_);
  if (!expr.is_constant()) {
    *const_val = nullptr;
    return Status::OK();
  }

  // A constant expression shouldn't have any SlotRefs expr in it.
  DCHECK_EQ(expr.GetSlotIds(), 0);
  DCHECK(expr_perm_pool_ != nullptr);
  const ColumnType& result_type = expr.type();
  RETURN_IF_ERROR(AllocateAnyVal(state, expr_perm_pool_, result_type,
      "Could not allocate constant expression value", const_val));

  void* result = ScalarExprEvaluator::GetValue(expr, nullptr);
  AnyValUtil::SetAnyVal(result, result_type, *const_val);
  if (result_type.IsStringType()) {
    StringVal* sv = reinterpret_cast<StringVal*>(*const_val);
    if (!sv->is_null && sv->len > 0) {
      // Make sure the memory is owned by this evaluator.
      char* ptr_copy =
          reinterpret_cast<char*>(expr_perm_pool_->TryAllocateUnaligned(sv->len));
      if (ptr_copy == nullptr) {
        return expr_perm_pool_->mem_tracker()->MemLimitExceeded(
            state, "Could not allocate constant string value", sv->len);
      }
      memcpy(ptr_copy, sv->ptr, sv->len);
      sv->ptr = reinterpret_cast<uint8_t*>(ptr_copy);
    }
  }
  return GetError(expr.fn_ctx_idx_start_, expr.fn_ctx_idx_end_);
}

void* ScalarExprEvaluator::GetValue(const TupleRow* row) {
  return GetValue(root_, row);
}

void* ScalarExprEvaluator::GetValue(const ScalarExpr& expr, const TupleRow* row) {
  switch (expr.type_.type) {
    case TYPE_BOOLEAN: {
      impala_udf::BooleanVal v = expr.GetBooleanVal(this, row);
      if (v.is_null) return nullptr;
      result_.bool_val = v.val;
      return &result_.bool_val;
    }
    case TYPE_TINYINT: {
      impala_udf::TinyIntVal v = expr.GetTinyIntVal(this, row);
      if (v.is_null) return nullptr;
      result_.tinyint_val = v.val;
      return &result_.tinyint_val;
    }
    case TYPE_SMALLINT: {
      impala_udf::SmallIntVal v = expr.GetSmallIntVal(this, row);
      if (v.is_null) return nullptr;
      result_.smallint_val = v.val;
      return &result_.smallint_val;
    }
    case TYPE_INT: {
      impala_udf::IntVal v = expr.GetIntVal(this, row);
      if (v.is_null) return nullptr;
      result_.int_val = v.val;
      return &result_.int_val;
    }
    case TYPE_BIGINT: {
      impala_udf::BigIntVal v = expr.GetBigIntVal(this, row);
      if (v.is_null) return nullptr;
      result_.bigint_val = v.val;
      return &result_.bigint_val;
    }
    case TYPE_FLOAT: {
      impala_udf::FloatVal v = expr.GetFloatVal(this, row);
      if (v.is_null) return nullptr;
      result_.float_val = v.val;
      return &result_.float_val;
    }
    case TYPE_DOUBLE: {
      impala_udf::DoubleVal v = expr.GetDoubleVal(this, row);
      if (v.is_null) return nullptr;
      result_.double_val = v.val;
      return &result_.double_val;
    }
    case TYPE_STRING:
    case TYPE_VARCHAR: {
      impala_udf::StringVal v = expr.GetStringVal(this, row);
      if (v.is_null) return nullptr;
      result_.string_val.Assign(reinterpret_cast<char*>(v.ptr), v.len);
      return &result_.string_val;
    }
    case TYPE_CHAR:
    case TYPE_FIXED_UDA_INTERMEDIATE: {
      impala_udf::StringVal v = expr.GetStringVal(this, row);
      if (v.is_null) return nullptr;
      result_.string_val.Assign(reinterpret_cast<char*>(v.ptr), v.len);
      return result_.string_val.Ptr();
    }
    case TYPE_TIMESTAMP: {
      impala_udf::TimestampVal v = expr.GetTimestampVal(this, row);
      if (v.is_null) return nullptr;
      result_.timestamp_val = TimestampValue::FromTimestampVal(v);
      return &result_.timestamp_val;
    }
    case TYPE_DECIMAL: {
      DecimalVal v = expr.GetDecimalVal(this, row);
      if (v.is_null) return nullptr;
      switch (expr.type_.GetByteSize()) {
        case 4:
          result_.decimal4_val = v.val4;
          return &result_.decimal4_val;
        case 8:
          result_.decimal8_val = v.val8;
          return &result_.decimal8_val;
        case 16:
          result_.decimal16_val = v.val16;
          return &result_.decimal16_val;
        default:
          DCHECK(false) << expr.type_.GetByteSize();
          return nullptr;
      }
    }
    case TYPE_DATE: {
      impala_udf::DateVal v = expr.GetDateVal(this, row);
      if (v.is_null) return nullptr;
      const DateValue dv = DateValue::FromDateVal(v);
      if (UNLIKELY(!dv.IsValid())) return nullptr;
      result_.date_val = dv;
      return &result_.date_val;
    }
    case TYPE_ARRAY:
    case TYPE_MAP: {
      impala_udf::CollectionVal v = expr.GetCollectionVal(this, row);
      if (v.is_null) return nullptr;
      result_.collection_val.ptr = v.ptr;
      result_.collection_val.num_tuples = v.num_tuples;
      return &result_.collection_val;
    }
    case TYPE_STRUCT: {
      StructVal v = expr.GetStructVal(this, row);
      if (v.is_null) return nullptr;
      result_.struct_val = v;
      return &result_.struct_val;
    }
    default:
      DCHECK(false) << "Type not implemented: " << expr.type_.DebugString();
      return nullptr;
  }
}

void ScalarExprEvaluator::PrintValue(const TupleRow* row, string* str) {
  RawValue::PrintValue(GetValue(row), root_.type(), output_scale_, str);
}

void ScalarExprEvaluator::PrintValue(void* value, string* str) {
  RawValue::PrintValue(value, root_.type(), output_scale_, str);
}

void ScalarExprEvaluator::PrintValue(void* value, stringstream* stream) {
  RawValue::PrintValue(value, root_.type(), output_scale_, stream);
}

void ScalarExprEvaluator::PrintValue(const TupleRow* row, stringstream* stream) {
  RawValue::PrintValue(GetValue(row), root_.type(), output_scale_, stream);
}

BooleanVal ScalarExprEvaluator::GetBooleanVal(const TupleRow* row) {
  return root_.GetBooleanVal(this, row);
}

TinyIntVal ScalarExprEvaluator::GetTinyIntVal(const TupleRow* row) {
  return root_.GetTinyIntVal(this, row);
}

SmallIntVal ScalarExprEvaluator::GetSmallIntVal(const TupleRow* row) {
  return root_.GetSmallIntVal(this, row);
}

IntVal ScalarExprEvaluator::GetIntVal(const TupleRow* row) {
  return root_.GetIntVal(this, row);
}

BigIntVal ScalarExprEvaluator::GetBigIntVal(const TupleRow* row) {
  return root_.GetBigIntVal(this, row);
}

FloatVal ScalarExprEvaluator::GetFloatVal(const TupleRow* row) {
  return root_.GetFloatVal(this, row);
}

DoubleVal ScalarExprEvaluator::GetDoubleVal(const TupleRow* row) {
  return root_.GetDoubleVal(this, row);
}

StringVal ScalarExprEvaluator::GetStringVal(const TupleRow* row) {
  return root_.GetStringVal(this, row);
}

CollectionVal ScalarExprEvaluator::GetCollectionVal(const TupleRow* row) {
  return root_.GetCollectionVal(this, row);
}

StructVal ScalarExprEvaluator::GetStructVal(const TupleRow* row) {
  return root_.GetStructVal(this, row);
}

TimestampVal ScalarExprEvaluator::GetTimestampVal(const TupleRow* row) {
  return root_.GetTimestampVal(this, row);
}

DecimalVal ScalarExprEvaluator::GetDecimalVal(const TupleRow* row) {
  return root_.GetDecimalVal(this, row);
}

DateVal ScalarExprEvaluator::GetDateVal(const TupleRow* row) {
  return root_.GetDateVal(this, row);
}

void ScalarExprEvaluator::InitBuiltinsDummy() {
  // Call one function from each of the classes to pull all the symbols
  // from that class in.
  AiFunctions::is_api_endpoint_supported("");
  AggregateFunctions::InitNull(nullptr, nullptr);
  BitByteFunctions::CountSet(nullptr, TinyIntVal::null());
  CastFunctions::CastToBooleanVal(nullptr, TinyIntVal::null());
  CompoundPredicate::Not(nullptr, BooleanVal::null());
  ConditionalFunctions::NullIfZero(nullptr, TinyIntVal::null());
  DataSketchesFunctions::DsHllEstimate(nullptr, StringVal::null());
  DecimalFunctions::Precision(nullptr, DecimalVal::null());
  DecimalOperators::CastToDecimalVal(nullptr, DecimalVal::null());
  IcebergFunctions::TruncatePartitionTransform(nullptr, IntVal::null(), IntVal::null());
  InPredicate::InIterate(nullptr, BigIntVal::null(), 0, nullptr);
  IsNullPredicate::IsNull(nullptr, BooleanVal::null());
  LikePredicate::Like(nullptr, StringVal::null(), StringVal::null());
  Operators::Add_IntVal_IntVal(nullptr, IntVal::null(), IntVal::null());
  MaskFunctions::MaskShowFirstN(nullptr, StringVal::null());
  MathFunctions::Pi(nullptr);
  StringFunctions::Length(nullptr, StringVal::null());
  TimestampFunctions::Year(nullptr, TimestampVal::null());
  TimestampFunctions::FromUnixPrepare(nullptr, FunctionContext::FRAGMENT_LOCAL);
  DateFunctions::Year(nullptr, DateVal::null());
  UdfBuiltins::Pi(nullptr);
  UtilityFunctions::Pid(nullptr);
}
