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

#include "exprs/agg-fn.h"

#include "codegen/llvm-codegen.h"
#include "exprs/anyval-util.h"
#include "exprs/scalar-expr.h"
#include "runtime/descriptors.h"
#include "runtime/fragment-state.h"
#include "runtime/lib-cache.h"

#include "common/names.h"

using namespace impala_udf;

namespace impala {

AggFn::AggFn(const TExprNode& tnode, const SlotDescriptor& intermediate_slot_desc,
    const SlotDescriptor& output_slot_desc)
  : Expr(tnode),
    is_merge_(tnode.agg_expr.is_merge_agg),
    intermediate_slot_desc_(intermediate_slot_desc),
    output_slot_desc_(output_slot_desc),
    arg_type_descs_(AnyValUtil::ColumnTypesToTypeDescs(
        ColumnType::FromThrift(tnode.agg_expr.arg_types))) {
  DCHECK(tnode.__isset.fn);
  DCHECK(tnode.fn.__isset.aggregate_fn);
  DCHECK_EQ(tnode.node_type, TExprNodeType::AGGREGATE_EXPR);
  DCHECK_EQ(ColumnType::FromThrift(tnode.type).type,
      ColumnType::FromThrift(fn_.ret_type).type);
  const string& fn_name = fn_.name.function_name;
  if (fn_name == "count") {
    agg_op_ = COUNT;
  } else if (fn_name == "min") {
    agg_op_ = MIN;
  } else if (fn_name == "max") {
    agg_op_ = MAX;
  } else if (fn_name == "sum" || fn_name == "sum_init_zero") {
    agg_op_ = SUM;
  } else if (fn_name == "avg") {
    agg_op_ = AVG;
  } else if (fn_name == "ndv" || fn_name == "ndv_no_finalize") {
    agg_op_ = NDV;
  } else {
    agg_op_ = OTHER;
  }
}

Status AggFn::Init(const RowDescriptor& row_desc, FragmentState* state) {
  // Initialize all children (i.e. input exprs to this aggregate expr).
  for (ScalarExpr* input_expr : children()) {
    RETURN_IF_ERROR(input_expr->Init(row_desc, /*is_entry_point*/ false, state));
  }

  // Initialize the aggregate expressions' internals.
  const TAggregateFunction& aggregate_fn = fn_.aggregate_fn;
  DCHECK_EQ(intermediate_slot_desc_.type().type,
      ColumnType::FromThrift(aggregate_fn.intermediate_type).type);
  DCHECK_EQ(output_slot_desc_.type().type, ColumnType::FromThrift(fn_.ret_type).type);

  time_t mtime = fn_.last_modified_time;
  // Load the function pointers. Must have init() and update().
  if (aggregate_fn.init_fn_symbol.empty() ||
      aggregate_fn.update_fn_symbol.empty() ||
      (aggregate_fn.merge_fn_symbol.empty() && !aggregate_fn.is_analytic_only_fn)) {
    // This path is only for partially implemented builtins.
    DCHECK_EQ(fn_.binary_type, TFunctionBinaryType::BUILTIN);
    stringstream ss;
    ss << "Function " << fn_.name.function_name << " is not implemented.";
    return Status(ss.str());
  }

  RETURN_IF_ERROR(LibCache::instance()->GetSoFunctionPtr(
      fn_.hdfs_location, aggregate_fn.init_fn_symbol, mtime, &init_fn_, &cache_entry_));
  RETURN_IF_ERROR(LibCache::instance()->GetSoFunctionPtr(fn_.hdfs_location,
      aggregate_fn.update_fn_symbol, mtime, &update_fn_, &cache_entry_));

  // Merge() is not defined for purely analytic function.
  if (!aggregate_fn.is_analytic_only_fn) {
    RETURN_IF_ERROR(LibCache::instance()->GetSoFunctionPtr(fn_.hdfs_location,
        aggregate_fn.merge_fn_symbol, mtime, &merge_fn_, &cache_entry_));
  }
  // Serialize(), GetValue(), Remove() and Finalize() are optional
  if (!aggregate_fn.serialize_fn_symbol.empty()) {
    RETURN_IF_ERROR(LibCache::instance()->GetSoFunctionPtr(fn_.hdfs_location,
        aggregate_fn.serialize_fn_symbol, mtime, &serialize_fn_, &cache_entry_));
  }
  if (!aggregate_fn.get_value_fn_symbol.empty()) {
    RETURN_IF_ERROR(LibCache::instance()->GetSoFunctionPtr(fn_.hdfs_location,
        aggregate_fn.get_value_fn_symbol, mtime, &get_value_fn_, &cache_entry_));
  }
  if (!aggregate_fn.remove_fn_symbol.empty()) {
    RETURN_IF_ERROR(LibCache::instance()->GetSoFunctionPtr(fn_.hdfs_location,
        aggregate_fn.remove_fn_symbol, mtime, &remove_fn_, &cache_entry_));
  }
  if (!aggregate_fn.finalize_fn_symbol.empty()) {
    RETURN_IF_ERROR(LibCache::instance()->GetSoFunctionPtr(fn_.hdfs_location,
        fn_.aggregate_fn.finalize_fn_symbol, mtime, &finalize_fn_, &cache_entry_));
  }
  return Status::OK();
}

Status AggFn::Create(const TExpr& texpr, const RowDescriptor& row_desc,
    const SlotDescriptor& intermediate_slot_desc, const SlotDescriptor& output_slot_desc,
    FragmentState* state, AggFn** agg_fn) {
  *agg_fn = nullptr;
  ObjectPool* pool = state->obj_pool();
  const TExprNode& texpr_node = texpr.nodes[0];
  DCHECK_EQ(texpr_node.node_type, TExprNodeType::AGGREGATE_EXPR);
  if (!texpr_node.__isset.fn) {
    return Status("Function not set in thrift AGGREGATE_EXPR node");
  }
  AggFn* new_agg_fn =
      pool->Add(new AggFn(texpr_node, intermediate_slot_desc, output_slot_desc));
  RETURN_IF_ERROR(Expr::CreateTree(texpr, pool, new_agg_fn));
  Status status = new_agg_fn->Init(row_desc, state);
  if (UNLIKELY(!status.ok())) {
    new_agg_fn->Close();
    return status;
  }
  for (ScalarExpr* input_expr : new_agg_fn->children()) {
    int fn_ctx_idx = 0;
    input_expr->AssignFnCtxIdx(&fn_ctx_idx);
  }
  *agg_fn = new_agg_fn;
  return Status::OK();
}

FunctionContext::TypeDesc AggFn::GetIntermediateTypeDesc() const {
  return AnyValUtil::ColumnTypeToTypeDesc(intermediate_slot_desc_.type());
}

FunctionContext::TypeDesc AggFn::GetOutputTypeDesc() const {
  return AnyValUtil::ColumnTypeToTypeDesc(output_slot_desc_.type());
}

Status AggFn::CodegenUpdateOrMergeFunction(
    LlvmCodeGen* codegen, llvm::Function** uda_fn) {
  const string& symbol =
      is_merge_ ? fn_.aggregate_fn.merge_fn_symbol : fn_.aggregate_fn.update_fn_symbol;
  vector<ColumnType> fn_arg_types;
  for (ScalarExpr* input_expr : children()) {
    fn_arg_types.push_back(input_expr->type());
  }
  // The intermediate value is passed as the last argument.
  fn_arg_types.push_back(intermediate_type());
  RETURN_IF_ERROR(codegen->LoadFunction(fn_, symbol, nullptr, fn_arg_types,
      fn_arg_types.size(), false, uda_fn, &cache_entry_));

  // Inline constants into the function body (if there is an IR body).
  if (!(*uda_fn)->isDeclaration()) {
    // TODO: IMPALA-4785: we should also replace references to GetIntermediateType()
    // with constants.
    codegen->InlineConstFnAttrs(GetOutputTypeDesc(), arg_type_descs_, *uda_fn);
    *uda_fn = codegen->FinalizeFunction(*uda_fn);
    if (*uda_fn == nullptr) {
      return Status(TErrorCode::UDF_VERIFY_FAILED, symbol, fn_.hdfs_location);
    }
  }
  return Status::OK();
}

void AggFn::Close() {
  // This also closes all the input expressions.
  Expr::Close();
}

void AggFn::Close(const vector<AggFn*>& exprs) {
  for (AggFn* expr : exprs) expr->Close();
}

string AggFn::DebugString() const {
  stringstream out;
  out << "AggFn(op=" << agg_op_;
  for (ScalarExpr* input_expr : children()) {
    out << " " << input_expr->DebugString() << ")";
  }
  out << ")";
  return out.str();
}

string AggFn::DebugString(const vector<AggFn*>& agg_fns) {
  stringstream out;
  out << "[";
  for (int i = 0; i < agg_fns.size(); ++i) {
    out << (i == 0 ? "" : " ") << agg_fns[i]->DebugString();
  }
  out << "]";
  return out.str();
}

}
