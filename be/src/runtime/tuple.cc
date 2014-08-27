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

#include "runtime/tuple.h"

#include <vector>

#include "exprs/expr.h"
#include "exprs/expr-context.h"
#include "runtime/descriptors.h"
#include "runtime/mem-pool.h"
#include "runtime/raw-value.h"
#include "runtime/tuple-row.h"
#include "runtime/string-value.h"
#include "util/debug-util.h"

using namespace std;

namespace impala {

  const char* Tuple::LLVM_CLASS_NAME = "class.impala::Tuple";

Tuple* Tuple::DeepCopy(const TupleDescriptor& desc, MemPool* pool, bool convert_ptrs) {
  Tuple* result = reinterpret_cast<Tuple*>(pool->Allocate(desc.byte_size()));
  DeepCopy(result, desc, pool, convert_ptrs);
  return result;
}

void Tuple::DeepCopy(Tuple* dst, const TupleDescriptor& desc, MemPool* pool,
                     bool convert_ptrs) {
  memcpy(dst, this, desc.byte_size());
  // allocate in the same pool and then copy all non-null string slots
  for (vector<SlotDescriptor*>::const_iterator i = desc.string_slots().begin();
       i != desc.string_slots().end(); ++i) {
    DCHECK((*i)->type().IsVarLen());
    if (!dst->IsNull((*i)->null_indicator_offset())) {
      StringValue* string_v = dst->GetStringSlot((*i)->tuple_offset());
      int offset = pool->GetCurrentOffset();
      char* string_copy = reinterpret_cast<char*>(pool->Allocate(string_v->len));
      memcpy(string_copy, string_v->ptr, string_v->len);
      string_v->ptr = (convert_ptrs ? reinterpret_cast<char*>(offset) : string_copy);
    }
  }
}

void Tuple::DeepCopy(const TupleDescriptor& desc, char** data, int* offset,
                     bool convert_ptrs) {
  Tuple* dst = reinterpret_cast<Tuple*>(*data);
  memcpy(dst, this, desc.byte_size());
  *data += desc.byte_size();
  *offset += desc.byte_size();
  for (vector<SlotDescriptor*>::const_iterator i = desc.string_slots().begin();
       i != desc.string_slots().end(); ++i) {
    DCHECK((*i)->type().IsVarLen());
    if (!dst->IsNull((*i)->null_indicator_offset())) {
      StringValue* string_v = dst->GetStringSlot((*i)->tuple_offset());
      memcpy(*data, string_v->ptr, string_v->len);
      string_v->ptr = (convert_ptrs ? reinterpret_cast<char*>(*offset) : *data);
      *data += string_v->len;
      *offset += string_v->len;
    }
  }
}

template <bool collect_string_vals>
void Tuple::MaterializeExprs(
    TupleRow* row, const TupleDescriptor& desc,
    const vector<ExprContext*>& materialize_expr_ctxs, MemPool* pool,
    vector<StringValue*>* non_null_var_len_values, int* total_var_len) {
  if (collect_string_vals) {
    non_null_var_len_values->clear();
    *total_var_len = 0;
  }
  memset(this, 0, desc.num_null_bytes());
  // Evaluate the output_slot_exprs and place the results in the tuples.
  int mat_expr_index = 0;
  for (int i = 0; i < desc.slots().size(); ++i) {
    SlotDescriptor* slot_desc = desc.slots()[i];
    if (!slot_desc->is_materialized()) continue;
    // The FE ensures we don't get any TYPE_NULL expressions by picking an arbitrary type
    // when necessary, but does not do this for slot descs.
    // TODO: revisit this logic in the FE
    DCHECK(slot_desc->type().type == TYPE_NULL ||
           slot_desc->type() == materialize_expr_ctxs[mat_expr_index]->root()->type());
    void* src = materialize_expr_ctxs[mat_expr_index]->GetValue(row);
    if (src != NULL) {
      void* dst = GetSlot(slot_desc->tuple_offset());
      RawValue::Write(src, dst, slot_desc->type(), pool);
      if (collect_string_vals && slot_desc->type().IsVarLen()) {
        StringValue* string_val = reinterpret_cast<StringValue*>(dst);
        non_null_var_len_values->push_back(string_val);
        *total_var_len += string_val->len;
      }
    } else {
      SetNull(slot_desc->null_indicator_offset());
    }
    ++mat_expr_index;
  }

  DCHECK_EQ(mat_expr_index, materialize_expr_ctxs.size());
}

template void Tuple::MaterializeExprs<false>(TupleRow* row, const TupleDescriptor& desc,
    const vector<ExprContext*>& materialize_expr_ctxs, MemPool* pool,
    vector<StringValue*>* non_null_var_values, int* total_var_len);

template void Tuple::MaterializeExprs<true>(TupleRow* row, const TupleDescriptor& desc,
    const vector<ExprContext*>& materialize_expr_ctxs, MemPool* pool,
    vector<StringValue*>* non_null_var_values, int* total_var_len);
}
