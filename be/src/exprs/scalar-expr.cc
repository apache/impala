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

#include "exprs/scalar-expr.inline.h"

#include <sstream>
#include <thrift/protocol/TDebugProtocol.h>

#include "codegen/codegen-anyval.h"
#include "codegen/llvm-codegen.h"
#include "common/object-pool.h"
#include "common/status.h"
#include "exprs/case-expr.h"
#include "exprs/cast-format-expr.h"
#include "exprs/compound-predicates.h"
#include "exprs/conditional-functions.h"
#include "exprs/hive-udf-call.h"
#include "exprs/in-predicate.h"
#include "exprs/is-not-empty-predicate.h"
#include "exprs/is-null-predicate.h"
#include "exprs/kudu-partition-expr.h"
#include "exprs/like-predicate.h"
#include "exprs/literal.h"
#include "exprs/null-literal.h"
#include "exprs/operators.h"
#include "exprs/scalar-expr-evaluator.h"
#include "exprs/scalar-fn-call.h"
#include "exprs/slot-ref.h"
#include "exprs/string-functions.h"
#include "exprs/timestamp-functions.h"
#include "exprs/tuple-is-null-predicate.h"
#include "exprs/udf-builtins.h"
#include "exprs/utility-functions.h"
#include "exprs/valid-tuple-id.h"
#include "runtime/fragment-state.h"
#include "runtime/runtime-state.h"
#include "runtime/tuple-row.h"
#include "runtime/tuple.h"
#include "runtime/types.h"
#include "udf/udf-internal.h"
#include "udf/udf.h"

#include "gen-cpp/Exprs_types.h"
#include "gen-cpp/ImpalaService_types.h"

#include "common/names.h"

using namespace impala_udf;

namespace impala {

const char* ScalarExpr::LLVM_CLASS_NAME = "class.impala::ScalarExpr";

ScalarExpr::ScalarExpr(const ColumnType& type, bool is_constant, bool is_codegen_disabled)
  : Expr(type),
    is_constant_(is_constant),
    is_codegen_disabled_(is_codegen_disabled) {}

ScalarExpr::ScalarExpr(const TExprNode& node)
  : Expr(node),
    is_constant_(node.is_constant),
    is_codegen_disabled_(node.is_codegen_disabled) {
  if (node.__isset.fn) fn_ = node.fn;
}

Status ScalarExpr::Create(const TExpr& texpr, const RowDescriptor& row_desc,
    FragmentState* state, ObjectPool* pool, ScalarExpr** scalar_expr) {
  *scalar_expr = nullptr;
  ScalarExpr* root;
  RETURN_IF_ERROR(CreateNode(texpr.nodes[0], pool, &root));
  RETURN_IF_ERROR(Expr::CreateTree(texpr, pool, root));
  // Assume that the root is a potential entry point for interpreted callers.
  // This is not always true but would require some work to determine for
  // each of the callsites of Create().
  // TODO: fix this - reducing the number of entry points would reduce codegen overhead
  // somewhat.
  Status status = root->Init(row_desc, /*is_entry_point*/ true, state);
  if (UNLIKELY(!status.ok())) {
    root->Close();
    return status;
  }
  int fn_ctx_idx = 0;
  root->AssignFnCtxIdx(&fn_ctx_idx);
  *scalar_expr = root;
  return Status::OK();
}

Status ScalarExpr::Create(const vector<TExpr>& texprs, const RowDescriptor& row_desc,
    FragmentState* state, ObjectPool* pool, vector<ScalarExpr*>* exprs) {
  exprs->clear();
  for (const TExpr& texpr: texprs) {
    ScalarExpr* expr;
    RETURN_IF_ERROR(Create(texpr, row_desc, state, pool, &expr));
    DCHECK(expr != nullptr);
    exprs->push_back(expr);
  }
  return Status::OK();
}

Status ScalarExpr::Create(const TExpr& texpr, const RowDescriptor& row_desc,
    FragmentState* state, ScalarExpr** scalar_expr) {
  return ScalarExpr::Create(texpr, row_desc, state, state->obj_pool(), scalar_expr);
}

Status ScalarExpr::Create(const vector<TExpr>& texprs, const RowDescriptor& row_desc,
    FragmentState* state, vector<ScalarExpr*>* exprs) {
  return ScalarExpr::Create(texprs, row_desc, state, state->obj_pool(), exprs);
}

void ScalarExpr::AssignFnCtxIdx(int* next_fn_ctx_idx) {
  fn_ctx_idx_start_ = *next_fn_ctx_idx;
  if (HasFnCtx()) {
    fn_ctx_idx_ = *next_fn_ctx_idx;
    ++(*next_fn_ctx_idx);
  }
  for (ScalarExpr* child : children()) child->AssignFnCtxIdx(next_fn_ctx_idx);
  fn_ctx_idx_end_ = *next_fn_ctx_idx;
}

Status ScalarExpr::CreateNode(
    const TExprNode& texpr_node, ObjectPool* pool, ScalarExpr** expr) {
  switch (texpr_node.node_type) {
    case TExprNodeType::BOOL_LITERAL:
    case TExprNodeType::FLOAT_LITERAL:
    case TExprNodeType::INT_LITERAL:
    case TExprNodeType::STRING_LITERAL:
    case TExprNodeType::DECIMAL_LITERAL:
    case TExprNodeType::TIMESTAMP_LITERAL:
    case TExprNodeType::DATE_LITERAL:
      *expr = pool->Add(new Literal(texpr_node));
      break;
    case TExprNodeType::CASE_EXPR:
      if (!texpr_node.__isset.case_expr) {
        return Status("Case expression not set in thrift node");
      }
      *expr = pool->Add(new CaseExpr(texpr_node));
      break;
    case TExprNodeType::COMPOUND_PRED:
      if (texpr_node.fn.name.function_name == "and") {
        *expr = pool->Add(new AndPredicate(texpr_node));
      } else if (texpr_node.fn.name.function_name == "or") {
        *expr = pool->Add(new OrPredicate(texpr_node));
      } else {
        DCHECK_EQ(texpr_node.fn.name.function_name, "not");
        *expr = pool->Add(new ScalarFnCall(texpr_node));
      }
      break;
    case TExprNodeType::NULL_LITERAL:
      *expr = pool->Add(new NullLiteral(texpr_node));
      break;
    case TExprNodeType::SLOT_REF:
      if (!texpr_node.__isset.slot_ref) {
        return Status("Slot reference not set in thrift node");
      }
      *expr = pool->Add(new SlotRef(texpr_node));
      break;
    case TExprNodeType::TUPLE_IS_NULL_PRED:
      *expr = pool->Add(new TupleIsNullPredicate(texpr_node));
      break;
    case TExprNodeType::FUNCTION_CALL:
      if (!texpr_node.__isset.fn) {
        return Status("Function not set in thrift node");
      }
      // Special-case functions that have their own Expr classes
      // TODO: is there a better way to do this?
      if (texpr_node.fn.name.function_name == "if") {
        *expr = pool->Add(new IfExpr(texpr_node));
      } else if (texpr_node.fn.name.function_name == "isnull" ||
                 texpr_node.fn.name.function_name == "ifnull" ||
                 texpr_node.fn.name.function_name == "nvl") {
        *expr = pool->Add(new IsNullExpr(texpr_node));
      } else if (texpr_node.fn.name.function_name == "coalesce") {
        *expr = pool->Add(new CoalesceExpr(texpr_node));
      } else if (texpr_node.fn.binary_type == TFunctionBinaryType::JAVA) {
        *expr = pool->Add(new HiveUdfCall(texpr_node));
      } else if (texpr_node.__isset.cast_expr &&
          !texpr_node.cast_expr.cast_format.empty()) {
        *expr = pool->Add(new CastFormatExpr(texpr_node));
      } else {
        *expr = pool->Add(new ScalarFnCall(texpr_node));
      }
      break;
    case TExprNodeType::IS_NOT_EMPTY_PRED:
      *expr = pool->Add(new IsNotEmptyPredicate(texpr_node));
      break;
    case TExprNodeType::KUDU_PARTITION_EXPR:
      *expr = pool->Add(new KuduPartitionExpr(texpr_node));
      break;
    case TExprNodeType::VALID_TUPLE_ID_EXPR:
      *expr = pool->Add(new ValidTupleIdExpr(texpr_node));
      break;
    default:
      *expr = nullptr;
      stringstream os;
      os << "Unknown expr node type: " << texpr_node.node_type;
      return Status(os.str());
  }
  DCHECK(*expr != nullptr);
  return Status::OK();
}

Status ScalarExpr::OpenEvaluator(FunctionContext::FunctionStateScope scope,
    RuntimeState* state, ScalarExprEvaluator* eval) const {
  for (int i = 0; i < children_.size(); ++i) {
    ScalarExprEvaluator* child_eval = eval;
    if (type_.IsStructType()) {
      DCHECK_EQ(children_.size(), eval->GetChildEvaluators().size());
      child_eval = eval->GetChildEvaluators()[i];
    }
    RETURN_IF_ERROR(children_[i]->OpenEvaluator(scope, state, child_eval));
  }
  return Status::OK();
}

void ScalarExpr::CloseEvaluator(FunctionContext::FunctionStateScope scope,
    RuntimeState* state, ScalarExprEvaluator* eval) const {
  for (ScalarExpr* child : children_) child->CloseEvaluator(scope, state, eval);
}

void ScalarExpr::Close() {
  Expr::Close();
}

void ScalarExpr::Close(const vector<ScalarExpr*>& exprs) {
  for (ScalarExpr* expr : exprs) expr->Close();
}

struct MemLayoutData {
  int expr_idx;
  int byte_size;
  bool variable_length;
  int alignment;

  // TODO: why put var-len at end?
  bool operator<(const MemLayoutData& rhs) const {
    // variable_len go at end
    if (this->variable_length && !rhs.variable_length) return false;
    if (!this->variable_length && rhs.variable_length) return true;
    return this->byte_size < rhs.byte_size;
  }
};

ScalarExprsResultsRowLayout::ScalarExprsResultsRowLayout(
    const vector<ScalarExpr*>& exprs) {
  if (exprs.size() == 0) {
    var_results_begin_offset = -1;
    expr_values_bytes_per_row = 0;
    return;
  }

  vector<MemLayoutData> data;
  data.resize(exprs.size());

  // Collect all the byte sizes and sort them
  for (int i = 0; i < exprs.size(); ++i) {
    DCHECK(!exprs[i]->type().IsComplexType()) << "NYI";
    data[i].expr_idx = i;
    data[i].byte_size = exprs[i]->type().GetSlotSize();
    DCHECK_GT(data[i].byte_size, 0);
    data[i].variable_length = exprs[i]->type().IsVarLenStringType();
  }

  sort(data.begin(), data.end());

  expr_values_bytes_per_row = 0;
  expr_values_offsets.resize(exprs.size());
  var_results_begin_offset = -1;

  for (int i = 0; i < data.size(); ++i) {
    expr_values_offsets[data[i].expr_idx] = expr_values_bytes_per_row;
    if (data[i].variable_length && var_results_begin_offset == -1) {
      var_results_begin_offset = expr_values_bytes_per_row;
    }
    DCHECK(!(i == 0 && expr_values_bytes_per_row > 0))
        << "first value should be at start of layout";
    expr_values_bytes_per_row += data[i].byte_size;
  }
}

Status ScalarExpr::Init(
    const RowDescriptor& row_desc, bool is_entry_point, FragmentState* state) {
  DCHECK(type_.type != INVALID_TYPE);
  for (int i = 0; i < children_.size(); ++i) {
    RETURN_IF_ERROR(children_[i]->Init(row_desc, false, state));
  }
  // Add the expression to the list of expressions to codegen in the codegen phase.
  if (ShouldCodegen(state)) {
    state->AddScalarExprToCodegen(this, is_entry_point, IsInterpretable());
  }
  return Status::OK();
}

string ScalarExpr::DebugString() const {
  // TODO: implement partial debug string for member vars
  stringstream out;
  out << " type=" << type_.DebugString();
  if (!children_.empty()) {
    out << " children=" << DebugString(children_);
  }
  return out.str();
}

string ScalarExpr::DebugString(const vector<ScalarExpr*>& exprs) {
  stringstream out;
  out << "[";
  for (int i = 0; i < exprs.size(); ++i) {
    out << (i == 0 ? "" : " ") << exprs[i]->DebugString();
  }
  out << "]";
  return out.str();
}

bool ScalarExpr::ShouldCodegen(const FragmentState* state) const {
  // Use the interpreted path and call the builtin without codegen if any of the
  // followings is true:
  // 1. The expression does not have an associated RuntimeState, e.g. is a partition
  //    key expression in a descriptor table.
  // 2. codegen is disabled by query option.
  // 3. there is an optimization hint to disable codegen and the expr can be interpreted.
  // 4. Optimizer decided to disable codegen. Example: const expressions in VALUES()
  //    which are evaluated only once.
  return state != nullptr && !state->CodegenDisabledByQueryOption()
      && !((state->CodegenHasDisableHint() || is_codegen_disabled_) && IsInterpretable());
}

int ScalarExpr::GetSlotIds(vector<SlotId>* slot_ids) const {
  int n = 0;
  for (int i = 0; i < children_.size(); ++i) {
    n += children_[i]->GetSlotIds(slot_ids);
  }
  return n;
}

llvm::Function* ScalarExpr::CreateIrFunctionPrototype(
    const string& name, LlvmCodeGen* codegen, llvm::Value* (*args)[2]) {
  llvm::Type* return_type = CodegenAnyVal::GetLoweredType(codegen, type());
  LlvmCodeGen::FnPrototype prototype(codegen, name, return_type);
  prototype.AddArgument(
      LlvmCodeGen::NamedVariable(
          "eval", codegen->GetStructPtrType<ScalarExprEvaluator>()));
  prototype.AddArgument(LlvmCodeGen::NamedVariable(
      "row", codegen->GetStructPtrType<TupleRow>()));
  llvm::Function* function = prototype.GeneratePrototype(NULL, args[0]);
  DCHECK(function != NULL);
  return function;
}

Status ScalarExpr::GetCodegendComputeFn(
    LlvmCodeGen* codegen, bool is_codegen_entry_point, llvm::Function** fn) {
  if (ir_compute_fn_ != nullptr) {
    *fn = ir_compute_fn_;
  } else {
    RETURN_IF_ERROR(GetCodegendComputeFnImpl(codegen, fn));
    ir_compute_fn_ = *fn;
  }
  if (is_codegen_entry_point && !added_to_jit_) {
    // Ensure Get*Val() is made callable if this function is called at least once
    // with is_codegen_entry_point=true.
    added_to_jit_ = true;
    codegen->AddFunctionToJit(*fn, &codegend_compute_fn_);
  }
  return Status::OK();
}

#define SCALAR_EXPR_GET_VAL_INTERPRETED(type)                 \
  type ScalarExpr::Get##type##Interpreted(                    \
      ScalarExprEvaluator* eval, const TupleRow* row) const { \
    DCHECK(false) << DebugString();                           \
    return type::null();                                      \
  }

// At least one of these should always be overridden.
SCALAR_EXPR_GET_VAL_INTERPRETED(BooleanVal);
SCALAR_EXPR_GET_VAL_INTERPRETED(TinyIntVal);
SCALAR_EXPR_GET_VAL_INTERPRETED(SmallIntVal);
SCALAR_EXPR_GET_VAL_INTERPRETED(IntVal);
SCALAR_EXPR_GET_VAL_INTERPRETED(BigIntVal);
SCALAR_EXPR_GET_VAL_INTERPRETED(FloatVal);
SCALAR_EXPR_GET_VAL_INTERPRETED(DoubleVal);
SCALAR_EXPR_GET_VAL_INTERPRETED(StringVal);
SCALAR_EXPR_GET_VAL_INTERPRETED(TimestampVal);
SCALAR_EXPR_GET_VAL_INTERPRETED(DecimalVal);
SCALAR_EXPR_GET_VAL_INTERPRETED(DateVal);
SCALAR_EXPR_GET_VAL_INTERPRETED(CollectionVal);
SCALAR_EXPR_GET_VAL_INTERPRETED(StructVal);

string ScalarExpr::DebugString(const string& expr_name) const {
  stringstream out;
  out << expr_name << "(" << ScalarExpr::DebugString() << ")";
  return out.str();
}

}
