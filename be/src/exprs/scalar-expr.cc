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

#include "exprs/scalar-expr.h"

#include <sstream>
#include <thrift/protocol/TDebugProtocol.h>

#include "codegen/codegen-anyval.h"
#include "codegen/llvm-codegen.h"
#include "common/object-pool.h"
#include "common/status.h"
#include "exprs/case-expr.h"
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
#include "runtime/runtime-state.h"
#include "runtime/tuple-row.h"
#include "runtime/tuple.h"
#include "udf/udf-internal.h"
#include "udf/udf.h"

#include "gen-cpp/Exprs_types.h"
#include "gen-cpp/ImpalaService_types.h"

#include "common/names.h"

using namespace impala_udf;

namespace impala {

const char* ScalarExpr::LLVM_CLASS_NAME = "class.impala::ScalarExpr";

ScalarExpr::ScalarExpr(const ColumnType& type, bool is_constant)
  : Expr(type),
    is_constant_(is_constant) {
}

ScalarExpr::ScalarExpr(const TExprNode& node)
  : Expr(node),
    is_constant_(node.is_constant) {
  if (node.__isset.fn) fn_ = node.fn;
}

Status ScalarExpr::Create(const TExpr& texpr, const RowDescriptor& row_desc,
    RuntimeState* state, ObjectPool* pool, ScalarExpr** scalar_expr) {
  *scalar_expr = nullptr;
  ScalarExpr* root;
  RETURN_IF_ERROR(CreateNode(texpr.nodes[0], pool, &root));
  RETURN_IF_ERROR(Expr::CreateTree(texpr, pool, root));
  Status status = root->Init(row_desc, state);
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
    RuntimeState* state, ObjectPool* pool, vector<ScalarExpr*>* exprs) {
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
    RuntimeState* state, ScalarExpr** scalar_expr) {
  return ScalarExpr::Create(texpr, row_desc, state, state->obj_pool(), scalar_expr);
}

Status ScalarExpr::Create(const vector<TExpr>& texprs, const RowDescriptor& row_desc,
    RuntimeState* state, vector<ScalarExpr*>* exprs) {
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
      *expr = pool->Add(new Literal(texpr_node));
      return Status::OK();
    case TExprNodeType::CASE_EXPR:
      if (!texpr_node.__isset.case_expr) {
        return Status("Case expression not set in thrift node");
      }
      *expr = pool->Add(new CaseExpr(texpr_node));
      return Status::OK();
    case TExprNodeType::COMPOUND_PRED:
      if (texpr_node.fn.name.function_name == "and") {
        *expr = pool->Add(new AndPredicate(texpr_node));
      } else if (texpr_node.fn.name.function_name == "or") {
        *expr = pool->Add(new OrPredicate(texpr_node));
      } else {
        DCHECK_EQ(texpr_node.fn.name.function_name, "not");
        *expr = pool->Add(new ScalarFnCall(texpr_node));
      }
      return Status::OK();
    case TExprNodeType::NULL_LITERAL:
      *expr = pool->Add(new NullLiteral(texpr_node));
      return Status::OK();
    case TExprNodeType::SLOT_REF:
      if (!texpr_node.__isset.slot_ref) {
        return Status("Slot reference not set in thrift node");
      }
      *expr = pool->Add(new SlotRef(texpr_node));
      return Status::OK();
    case TExprNodeType::TUPLE_IS_NULL_PRED:
      *expr = pool->Add(new TupleIsNullPredicate(texpr_node));
      return Status::OK();
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
      } else {
        *expr = pool->Add(new ScalarFnCall(texpr_node));
      }
      return Status::OK();
    case TExprNodeType::IS_NOT_EMPTY_PRED:
      *expr = pool->Add(new IsNotEmptyPredicate(texpr_node));
      return Status::OK();
    case TExprNodeType::KUDU_PARTITION_EXPR:
      *expr = pool->Add(new KuduPartitionExpr(texpr_node));
      return Status::OK();
    default:
      *expr = nullptr;
      stringstream os;
      os << "Unknown expr node type: " << texpr_node.node_type;
      return Status(os.str());
  }
}

Status ScalarExpr::OpenEvaluator(FunctionContext::FunctionStateScope scope,
    RuntimeState* state, ScalarExprEvaluator* eval) const {
  for (int i = 0; i < children_.size(); ++i) {
    RETURN_IF_ERROR(children_[i]->OpenEvaluator(scope, state, eval));
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

  // TODO: sort by type as well?  Any reason to do this?
  // TODO: would sorting in reverse order of size be faster due to better packing?
  // TODO: why put var-len at end?
  bool operator<(const MemLayoutData& rhs) const {
    // variable_len go at end
    if (this->variable_length && !rhs.variable_length) return false;
    if (!this->variable_length && rhs.variable_length) return true;
    return this->byte_size < rhs.byte_size;
  }
};

int ScalarExpr::ComputeResultsLayout(const vector<ScalarExpr*>& exprs,
    vector<int>* offsets, int* var_result_begin) {
  if (exprs.size() == 0) {
    *var_result_begin = -1;
    return 0;
  }

  // Don't align more than word (8-byte) size. There's no performance gain beyond 8-byte
  // alignment, and there is a performance gain to keeping the results buffer small. This
  // is consistent with what compilers do.
  int MAX_ALIGNMENT = sizeof(int64_t);

  vector<MemLayoutData> data;
  data.resize(exprs.size());

  // Collect all the byte sizes and sort them
  for (int i = 0; i < exprs.size(); ++i) {
    DCHECK(!exprs[i]->type().IsComplexType()) << "NYI";
    data[i].expr_idx = i;
    data[i].byte_size = exprs[i]->type().GetSlotSize();
    DCHECK_GT(data[i].byte_size, 0);
    data[i].variable_length = exprs[i]->type().IsVarLenStringType();

    bool fixed_len_char = exprs[i]->type().type == TYPE_CHAR && !data[i].variable_length;

    // Compute the alignment of this value. Values should be self-aligned for optimal
    // memory access speed, up to the max alignment (e.g., if this value is an int32_t,
    // its offset in the buffer should be divisible by sizeof(int32_t)).
    // TODO: is self-alignment really necessary for perf?
    if (!fixed_len_char) {
      data[i].alignment = min(data[i].byte_size, MAX_ALIGNMENT);
    } else {
      // Fixed-len chars are aligned to a one-byte boundary, as if they were char[],
      // leaving no padding between them and the previous value.
      data[i].alignment = 1;
    }
  }

  sort(data.begin(), data.end());

  // Walk the types and store in a packed aligned layout
  int byte_offset = 0;

  offsets->resize(exprs.size());
  *var_result_begin = -1;

  for (int i = 0; i < data.size(); ++i) {
    // Increase byte_offset so data[i] is at the right alignment (i.e. add padding between
    // this value and the previous).
    byte_offset = BitUtil::RoundUp(byte_offset, data[i].alignment);

    (*offsets)[data[i].expr_idx] = byte_offset;
    if (data[i].variable_length && *var_result_begin == -1) {
      *var_result_begin = byte_offset;
    }
    DCHECK(!(i == 0 && byte_offset > 0)) << "first value should be at start of layout";
    byte_offset += data[i].byte_size;
  }

  return byte_offset;
}

Status ScalarExpr::Init(const RowDescriptor& row_desc, RuntimeState* state) {
  DCHECK(type_.type != INVALID_TYPE);
  for (int i = 0; i < children_.size(); ++i) {
    RETURN_IF_ERROR(children_[i]->Init(row_desc, state));
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

int ScalarExpr::GetSlotIds(vector<SlotId>* slot_ids) const {
  int n = 0;
  for (int i = 0; i < children_.size(); ++i) {
    n += children_[i]->GetSlotIds(slot_ids);
  }
  return n;
}

llvm::Function* ScalarExpr::GetStaticGetValWrapper(
    ColumnType type, LlvmCodeGen* codegen) {
  switch (type.type) {
    case TYPE_BOOLEAN:
      return codegen->GetFunction(IRFunction::SCALAR_EXPR_GET_BOOLEAN_VAL, false);
    case TYPE_TINYINT:
      return codegen->GetFunction(IRFunction::SCALAR_EXPR_GET_TINYINT_VAL, false);
    case TYPE_SMALLINT:
      return codegen->GetFunction(IRFunction::SCALAR_EXPR_GET_SMALLINT_VAL, false);
    case TYPE_INT:
      return codegen->GetFunction(IRFunction::SCALAR_EXPR_GET_INT_VAL, false);
    case TYPE_BIGINT:
      return codegen->GetFunction(IRFunction::SCALAR_EXPR_GET_BIGINT_VAL, false);
    case TYPE_FLOAT:
      return codegen->GetFunction(IRFunction::SCALAR_EXPR_GET_FLOAT_VAL, false);
    case TYPE_DOUBLE:
      return codegen->GetFunction(IRFunction::SCALAR_EXPR_GET_DOUBLE_VAL, false);
    case TYPE_STRING:
    case TYPE_CHAR:
    case TYPE_VARCHAR:
      return codegen->GetFunction(IRFunction::SCALAR_EXPR_GET_STRING_VAL, false);
    case TYPE_TIMESTAMP:
      return codegen->GetFunction(IRFunction::SCALAR_EXPR_GET_TIMESTAMP_VAL, false);
    case TYPE_DECIMAL:
      return codegen->GetFunction(IRFunction::SCALAR_EXPR_GET_DECIMAL_VAL, false);
    default:
      DCHECK(false) << "Invalid type: " << type.DebugString();
      return NULL;
  }
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

Status ScalarExpr::GetCodegendComputeFnWrapper(
    LlvmCodeGen* codegen, llvm::Function** fn) {
  if (ir_compute_fn_ != nullptr) {
    *fn = ir_compute_fn_;
    return Status::OK();
  }
  llvm::Function* static_getval_fn = GetStaticGetValWrapper(type(), codegen);

  // Call it passing this as the additional first argument.
  llvm::Value* args[2];
  ir_compute_fn_ = CreateIrFunctionPrototype("CodegenComputeFnWrapper", codegen, &args);
  llvm::BasicBlock* entry_block =
      llvm::BasicBlock::Create(codegen->context(), "entry", ir_compute_fn_);
  LlvmBuilder builder(entry_block);
  llvm::Value* this_ptr = codegen->CastPtrToLlvmPtr(
      codegen->GetStructPtrType<ScalarExpr>(), this);
  llvm::Value* compute_fn_args[] = {this_ptr, args[0], args[1]};
  llvm::Value* ret = CodegenAnyVal::CreateCall(
      codegen, &builder, static_getval_fn, compute_fn_args, "ret");
  builder.CreateRet(ret);
  *fn = codegen->FinalizeFunction(ir_compute_fn_);
  if (UNLIKELY(*fn == nullptr)) {
    return Status(TErrorCode::IR_VERIFY_FAILED, "CodegendComputeFnWrapper");
  }
  ir_compute_fn_ = *fn;
  return Status::OK();
}

// At least one of these should always be overridden.
BooleanVal ScalarExpr::GetBooleanVal(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK(false) << DebugString();
  return BooleanVal::null();
}

TinyIntVal ScalarExpr::GetTinyIntVal(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK(false) << DebugString();
  return TinyIntVal::null();
}

SmallIntVal ScalarExpr::GetSmallIntVal(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK(false) << DebugString();
  return SmallIntVal::null();
}

IntVal ScalarExpr::GetIntVal(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK(false) << DebugString();
  return IntVal::null();
}

BigIntVal ScalarExpr::GetBigIntVal(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK(false) << DebugString();
  return BigIntVal::null();
}

FloatVal ScalarExpr::GetFloatVal(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK(false) << DebugString();
  return FloatVal::null();
}

DoubleVal ScalarExpr::GetDoubleVal(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK(false) << DebugString();
  return DoubleVal::null();
}

StringVal ScalarExpr::GetStringVal(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK(false) << DebugString();
  return StringVal::null();
}

CollectionVal ScalarExpr::GetCollectionVal(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK(false) << DebugString();
  return CollectionVal::null();
}

TimestampVal ScalarExpr::GetTimestampVal(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK(false) << DebugString();
  return TimestampVal::null();
}

DecimalVal ScalarExpr::GetDecimalVal(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK(false) << DebugString();
  return DecimalVal::null();
}

string ScalarExpr::DebugString(const string& expr_name) const {
  stringstream out;
  out << expr_name << "(" << ScalarExpr::DebugString() << ")";
  return out.str();
}

}
