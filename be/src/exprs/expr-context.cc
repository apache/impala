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

#include "exprs/expr-context.h"

#include <sstream>

#include "exprs/expr.h"
#include "runtime/mem-pool.h"
#include "runtime/runtime-state.h"
#include "udf/udf-internal.h"

#include "common/names.h"

using namespace impala;
using namespace impala_udf;

const char* ExprContext::LLVM_CLASS_NAME = "class.impala::ExprContext";

ExprContext::ExprContext(Expr* root)
  : fn_contexts_ptr_(NULL),
    root_(root),
    is_clone_(false),
    prepared_(false),
    opened_(false),
    closed_(false) {
}

ExprContext::~ExprContext() {
  DCHECK(!prepared_ || closed_);
  for (int i = 0; i < fn_contexts_.size(); ++i) {
    delete fn_contexts_[i];
  }
}

Status ExprContext::Prepare(RuntimeState* state, const RowDescriptor& row_desc,
                            MemTracker* tracker) {
  DCHECK(tracker != NULL);
  DCHECK(pool_.get() == NULL);
  prepared_ = true;
  pool_.reset(new MemPool(tracker));
  return root_->Prepare(state, row_desc, this);
}

Status ExprContext::Open(RuntimeState* state) {
  DCHECK(prepared_);
  if (opened_) return Status::OK();
  opened_ = true;
  // Fragment-local state is only initialized for original contexts. Clones inherit the
  // original's fragment state and only need to have thread-local state initialized.
  FunctionContext::FunctionStateScope scope =
      is_clone_? FunctionContext::THREAD_LOCAL : FunctionContext::FRAGMENT_LOCAL;
  return root_->Open(state, this, scope);
}

void ExprContext::Close(RuntimeState* state) {
  DCHECK(!closed_);
  FunctionContext::FunctionStateScope scope =
      is_clone_? FunctionContext::THREAD_LOCAL : FunctionContext::FRAGMENT_LOCAL;
  root_->Close(state, this, scope);

  for (int i = 0; i < fn_contexts_.size(); ++i) {
    fn_contexts_[i]->impl()->Close();
  }
  // pool_ can be NULL if Prepare() was never called
  if (pool_ != NULL) pool_->FreeAll();
  closed_ = true;
}

int ExprContext::Register(RuntimeState* state,
    const impala_udf::FunctionContext::TypeDesc& return_type,
    const vector<impala_udf::FunctionContext::TypeDesc>& arg_types,
    int varargs_buffer_size) {
  fn_contexts_.push_back(FunctionContextImpl::CreateContext(
      state, pool_.get(), return_type, arg_types, varargs_buffer_size));
  fn_contexts_ptr_ = &fn_contexts_[0];
  return fn_contexts_.size() - 1;
}

Status ExprContext::Clone(RuntimeState* state, ExprContext** new_ctx) {
  DCHECK(prepared_);
  DCHECK(opened_);
  DCHECK(*new_ctx == NULL);

  *new_ctx = state->obj_pool()->Add(new ExprContext(root_));
  (*new_ctx)->pool_.reset(new MemPool(pool_->mem_tracker()));
  for (int i = 0; i < fn_contexts_.size(); ++i) {
    (*new_ctx)->fn_contexts_.push_back(
        fn_contexts_[i]->impl()->Clone((*new_ctx)->pool_.get()));
  }
  (*new_ctx)->fn_contexts_ptr_ = &((*new_ctx)->fn_contexts_[0]);

  (*new_ctx)->is_clone_ = true;
  (*new_ctx)->prepared_ = true;
  (*new_ctx)->opened_ = true;

  return root_->Open(state, *new_ctx, FunctionContext::THREAD_LOCAL);
}

void ExprContext::FreeLocalAllocations() {
  FreeLocalAllocations(fn_contexts_);
}

void ExprContext::FreeLocalAllocations(const vector<ExprContext*>& ctxs) {
  for (int i = 0; i < ctxs.size(); ++i) {
    ctxs[i]->FreeLocalAllocations();
  }
}

void ExprContext::FreeLocalAllocations(const vector<FunctionContext*>& fn_ctxs) {
  for (int i = 0; i < fn_ctxs.size(); ++i) {
    if (fn_ctxs[i]->impl()->closed()) continue;
    fn_ctxs[i]->impl()->FreeLocalAllocations();
  }
}

void ExprContext::GetValue(TupleRow* row, bool as_ascii, TColumnValue* col_val) {
  void* value = GetValue(row);
  if (as_ascii) {
    RawValue::PrintValue(value, root_->type_, root_->output_scale_, &col_val->string_val);
    col_val->__isset.string_val = true;
    return;
  }
  if (value == NULL) return;

  StringValue* string_val = NULL;
  string tmp;
  switch (root_->type_.type) {
    case TYPE_BOOLEAN:
      col_val->__set_bool_val(*reinterpret_cast<bool*>(value));
      break;
    case TYPE_TINYINT:
      col_val->__set_byte_val(*reinterpret_cast<int8_t*>(value));
      break;
    case TYPE_SMALLINT:
      col_val->__set_short_val(*reinterpret_cast<int16_t*>(value));
      break;
    case TYPE_INT:
      col_val->__set_int_val(*reinterpret_cast<int32_t*>(value));
      break;
    case TYPE_BIGINT:
      col_val->__set_long_val(*reinterpret_cast<int64_t*>(value));
      break;
    case TYPE_FLOAT:
      col_val->__set_double_val(*reinterpret_cast<float*>(value));
      break;
    case TYPE_DOUBLE:
      col_val->__set_double_val(*reinterpret_cast<double*>(value));
      break;
    case TYPE_DECIMAL:
      switch (root_->type_.GetByteSize()) {
        case 4:
          col_val->string_val =
              reinterpret_cast<Decimal4Value*>(value)->ToString(root_->type_);
          break;
        case 8:
          col_val->string_val =
              reinterpret_cast<Decimal8Value*>(value)->ToString(root_->type_);
          break;
        case 16:
          col_val->string_val =
              reinterpret_cast<Decimal16Value*>(value)->ToString(root_->type_);
          break;
        default:
          DCHECK(false) << "Bad Type: " << root_->type_;
      }
      col_val->__isset.string_val = true;
      break;
    case TYPE_STRING:
    case TYPE_VARCHAR:
      string_val = reinterpret_cast<StringValue*>(value);
      tmp.assign(static_cast<char*>(string_val->ptr), string_val->len);
      col_val->string_val.swap(tmp);
      col_val->__isset.string_val = true;
      break;
    case TYPE_CHAR:
      tmp.assign(StringValue::CharSlotToPtr(value, root_->type_), root_->type_.len);
      col_val->string_val.swap(tmp);
      col_val->__isset.string_val = true;
      break;
    case TYPE_TIMESTAMP:
      RawValue::PrintValue(
          value, root_->type_, root_->output_scale_, &col_val->string_val);
      col_val->__isset.string_val = true;
      break;
    default:
      DCHECK(false) << "bad GetValue() type: " << root_->type_.DebugString();
  }
}

void* ExprContext::GetValue(TupleRow* row) {
  return GetValue(root_, row);
}

void* ExprContext::GetValue(Expr* e, TupleRow* row) {
  switch (e->type_.type) {
    case TYPE_BOOLEAN: {
      impala_udf::BooleanVal v = e->GetBooleanVal(this, row);
      if (v.is_null) return NULL;
      result_.bool_val = v.val;
      return &result_.bool_val;
    }
    case TYPE_TINYINT: {
      impala_udf::TinyIntVal v = e->GetTinyIntVal(this, row);
      if (v.is_null) return NULL;
      result_.tinyint_val = v.val;
      return &result_.tinyint_val;
    }
    case TYPE_SMALLINT: {
      impala_udf::SmallIntVal v = e->GetSmallIntVal(this, row);
      if (v.is_null) return NULL;
      result_.smallint_val = v.val;
      return &result_.smallint_val;
    }
    case TYPE_INT: {
      impala_udf::IntVal v = e->GetIntVal(this, row);
      if (v.is_null) return NULL;
      result_.int_val = v.val;
      return &result_.int_val;
    }
    case TYPE_BIGINT: {
      impala_udf::BigIntVal v = e->GetBigIntVal(this, row);
      if (v.is_null) return NULL;
      result_.bigint_val = v.val;
      return &result_.bigint_val;
    }
    case TYPE_FLOAT: {
      impala_udf::FloatVal v = e->GetFloatVal(this, row);
      if (v.is_null) return NULL;
      result_.float_val = v.val;
      return &result_.float_val;
    }
    case TYPE_DOUBLE: {
      impala_udf::DoubleVal v = e->GetDoubleVal(this, row);
      if (v.is_null) return NULL;
      result_.double_val = v.val;
      return &result_.double_val;
    }
    case TYPE_STRING:
    case TYPE_VARCHAR: {
      impala_udf::StringVal v = e->GetStringVal(this, row);
      if (v.is_null) return NULL;
      result_.string_val.ptr = reinterpret_cast<char*>(v.ptr);
      result_.string_val.len = v.len;
      return &result_.string_val;
    }
    case TYPE_CHAR: {
      impala_udf::StringVal v = e->GetStringVal(this, row);
      if (v.is_null) return NULL;
      result_.string_val.ptr = reinterpret_cast<char*>(v.ptr);
      result_.string_val.len = v.len;
      if (e->type_.IsVarLenStringType()) {
        return &result_.string_val;
      } else {
        return result_.string_val.ptr;
      }
    }
    case TYPE_TIMESTAMP: {
      impala_udf::TimestampVal v = e->GetTimestampVal(this, row);
      if (v.is_null) return NULL;
      result_.timestamp_val = TimestampValue::FromTimestampVal(v);
      return &result_.timestamp_val;
    }
    case TYPE_DECIMAL: {
      DecimalVal v = e->GetDecimalVal(this, row);
      if (v.is_null) return NULL;
      switch (e->type_.GetByteSize()) {
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
          DCHECK(false) << e->type_.GetByteSize();
          return NULL;
      }
    }
    case TYPE_ARRAY:
    case TYPE_MAP: {
      impala_udf::CollectionVal v = e->GetCollectionVal(this, row);
      if (v.is_null) return NULL;
      result_.collection_val.ptr = v.ptr;
      result_.collection_val.num_tuples = v.num_tuples;
      return &result_.collection_val;
    }
    default:
      DCHECK(false) << "Type not implemented: " << e->type_.DebugString();
      return NULL;
  }
}

void ExprContext::PrintValue(TupleRow* row, string* str) {
  RawValue::PrintValue(GetValue(row), root_->type(), root_->output_scale_, str);
}
void ExprContext::PrintValue(void* value, string* str) {
  RawValue::PrintValue(value, root_->type(), root_->output_scale_, str);
}
void ExprContext::PrintValue(void* value, stringstream* stream) {
  RawValue::PrintValue(value, root_->type(), root_->output_scale_, stream);
}
void ExprContext::PrintValue(TupleRow* row, stringstream* stream) {
  RawValue::PrintValue(GetValue(row), root_->type(), root_->output_scale_, stream);
}

BooleanVal ExprContext::GetBooleanVal(TupleRow* row) {
  return root_->GetBooleanVal(this, row);
}
TinyIntVal ExprContext::GetTinyIntVal(TupleRow* row) {
  return root_->GetTinyIntVal(this, row);
}
SmallIntVal ExprContext::GetSmallIntVal(TupleRow* row) {
  return root_->GetSmallIntVal(this, row);
}
IntVal ExprContext::GetIntVal(TupleRow* row) {
  return root_->GetIntVal(this, row);
}
BigIntVal ExprContext::GetBigIntVal(TupleRow* row) {
  return root_->GetBigIntVal(this, row);
}
FloatVal ExprContext::GetFloatVal(TupleRow* row) {
  return root_->GetFloatVal(this, row);
}
DoubleVal ExprContext::GetDoubleVal(TupleRow* row) {
  return root_->GetDoubleVal(this, row);
}
StringVal ExprContext::GetStringVal(TupleRow* row) {
  return root_->GetStringVal(this, row);
}
CollectionVal ExprContext::GetCollectionVal(TupleRow* row) {
  return root_->GetCollectionVal(this, row);
}
TimestampVal ExprContext::GetTimestampVal(TupleRow* row) {
  return root_->GetTimestampVal(this, row);
}
DecimalVal ExprContext::GetDecimalVal(TupleRow* row) {
  return root_->GetDecimalVal(this, row);
}
