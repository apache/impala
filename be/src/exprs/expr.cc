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

#include <sstream>

#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/PassManager.h>
#include <llvm/Transforms/Scalar.h>
#include <llvm/Transforms/Utils/BasicBlockUtils.h>
#include <llvm/Transforms/Utils/UnrollLoop.h>
#include <llvm/Support/InstIterator.h>
#include <thrift/protocol/TDebugProtocol.h>

#include "codegen/codegen-anyval.h"
#include "codegen/llvm-codegen.h"
#include "common/object-pool.h"
#include "common/status.h"
#include "exprs/anyval-util.h"
#include "exprs/expr.h"
#include "exprs/expr-context.h"
#include "exprs/aggregate-functions.h"
#include "exprs/bit-byte-functions.h"
#include "exprs/case-expr.h"
#include "exprs/cast-functions.h"
#include "exprs/compound-predicates.h"
#include "exprs/conditional-functions.h"
#include "exprs/decimal-functions.h"
#include "exprs/decimal-operators.h"
#include "exprs/hive-udf-call.h"
#include "exprs/in-predicate.h"
#include "exprs/is-null-predicate.h"
#include "exprs/like-predicate.h"
#include "exprs/literal.h"
#include "exprs/math-functions.h"
#include "exprs/null-literal.h"
#include "exprs/operators.h"
#include "exprs/scalar-fn-call.h"
#include "exprs/slot-ref.h"
#include "exprs/string-functions.h"
#include "exprs/timestamp-functions.h"
#include "exprs/tuple-is-null-predicate.h"
#include "exprs/udf-builtins.h"
#include "exprs/utility-functions.h"
#include "gen-cpp/Exprs_types.h"
#include "gen-cpp/Data_types.h"
#include "runtime/lib-cache.h"
#include "runtime/runtime-state.h"
#include "runtime/raw-value.h"
#include "udf/udf.h"
#include "udf/udf-internal.h"

#include "gen-cpp/Exprs_types.h"
#include "gen-cpp/ImpalaService_types.h"

#include "common/names.h"

using namespace impala_udf;
using namespace llvm;

namespace impala {

const char* Expr::LLVM_CLASS_NAME = "class.impala::Expr";

const char* Expr::GET_CONSTANT_SYMBOL_PREFIX = "_ZN6impala4Expr11GetConstant";

template<class T>
bool ParseString(const string& str, T* val) {
  istringstream stream(str);
  stream >> *val;
  return !stream.fail();
}

FunctionContext* Expr::RegisterFunctionContext(ExprContext* ctx, RuntimeState* state,
                                               int varargs_buffer_size) {
  FunctionContext::TypeDesc return_type = AnyValUtil::ColumnTypeToTypeDesc(type_);
  vector<FunctionContext::TypeDesc> arg_types;
  for (int i = 0; i < children_.size(); ++i) {
    arg_types.push_back(AnyValUtil::ColumnTypeToTypeDesc(children_[i]->type_));
  }
  fn_context_index_ = ctx->Register(state, return_type, arg_types, varargs_buffer_size);
  return ctx->fn_context(fn_context_index_);
}

Expr::Expr(const ColumnType& type, bool is_slotref)
    : cache_entry_(NULL),
      is_slotref_(is_slotref),
      type_(type),
      output_scale_(-1),
      fn_context_index_(-1),
      ir_compute_fn_(NULL) {
}

Expr::Expr(const TExprNode& node, bool is_slotref)
    : cache_entry_(NULL),
      is_slotref_(is_slotref),
      type_(ColumnType::FromThrift(node.type)),
      output_scale_(-1),
      fn_context_index_(-1),
      ir_compute_fn_(NULL) {
  if (node.__isset.fn) fn_ = node.fn;
}

Expr::~Expr() {
  DCHECK(cache_entry_ == NULL);
}

void Expr::Close(RuntimeState* state, ExprContext* context,
                 FunctionContext::FunctionStateScope scope) {
  for (int i = 0; i < children_.size(); ++i) {
    children_[i]->Close(state, context, scope);
  }

  if (scope == FunctionContext::FRAGMENT_LOCAL) {
    // This is the final, non-cloned context to close. Clean up the whole Expr.
    if (cache_entry_ != NULL) {
      LibCache::instance()->DecrementUseCount(cache_entry_);
      cache_entry_ = NULL;
    }
  }
}

Status Expr::CreateExprTree(ObjectPool* pool, const TExpr& texpr, ExprContext** ctx) {
  // input is empty
  if (texpr.nodes.size() == 0) {
    *ctx = NULL;
    return Status::OK();
  }
  int node_idx = 0;
  Expr* e;
  Status status = CreateTreeFromThrift(pool, texpr.nodes, NULL, &node_idx, &e, ctx);
  if (status.ok() && node_idx + 1 != texpr.nodes.size()) {
    status = Status(
        "Expression tree only partially reconstructed. Not all thrift nodes were used.");
  }
  if (!status.ok()) {
    LOG(ERROR) << "Could not construct expr tree.\n" << status.GetDetail() << "\n"
               << apache::thrift::ThriftDebugString(texpr);
  }
  return status;
}

Status Expr::CreateExprTrees(ObjectPool* pool, const vector<TExpr>& texprs,
                             vector<ExprContext*>* ctxs) {
  ctxs->clear();
  for (int i = 0; i < texprs.size(); ++i) {
    ExprContext* ctx;
    RETURN_IF_ERROR(CreateExprTree(pool, texprs[i], &ctx));
    ctxs->push_back(ctx);
  }
  return Status::OK();
}

Status Expr::CreateTreeFromThrift(ObjectPool* pool, const vector<TExprNode>& nodes,
    Expr* parent, int* node_idx, Expr** root_expr, ExprContext** ctx) {
  // propagate error case
  if (*node_idx >= nodes.size()) {
    return Status("Failed to reconstruct expression tree from thrift.");
  }
  int num_children = nodes[*node_idx].num_children;
  Expr* expr = NULL;
  RETURN_IF_ERROR(CreateExpr(pool, nodes[*node_idx], &expr));
  DCHECK(expr != NULL);
  if (parent != NULL) {
    parent->AddChild(expr);
  } else {
    DCHECK(root_expr != NULL);
    DCHECK(ctx != NULL);
    *root_expr = expr;
    *ctx = pool->Add(new ExprContext(expr));
  }
  for (int i = 0; i < num_children; i++) {
    *node_idx += 1;
    RETURN_IF_ERROR(CreateTreeFromThrift(pool, nodes, expr, node_idx, NULL, NULL));
    // we are expecting a child, but have used all nodes
    // this means we have been given a bad tree and must fail
    if (*node_idx >= nodes.size()) {
      return Status("Failed to reconstruct expression tree from thrift.");
    }
  }
  return Status::OK();
}

Status Expr::CreateExpr(ObjectPool* pool, const TExprNode& texpr_node, Expr** expr) {
  switch (texpr_node.node_type) {
    case TExprNodeType::BOOL_LITERAL:
    case TExprNodeType::FLOAT_LITERAL:
    case TExprNodeType::INT_LITERAL:
    case TExprNodeType::STRING_LITERAL:
    case TExprNodeType::DECIMAL_LITERAL:
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
      } else if (texpr_node.fn.name.function_name == "nullif") {
        *expr = pool->Add(new NullIfExpr(texpr_node));
      } else if (texpr_node.fn.name.function_name == "isnull" ||
                 texpr_node.fn.name.function_name == "ifnull" ||
                 texpr_node.fn.name.function_name == "nvl") {
        *expr = pool->Add(new IsNullExpr(texpr_node));
      } else if (texpr_node.fn.name.function_name == "coalesce") {
        *expr = pool->Add(new CoalesceExpr(texpr_node));

      } else if (texpr_node.fn.binary_type == TFunctionBinaryType::HIVE) {
        *expr = pool->Add(new HiveUdfCall(texpr_node));
      } else {
        *expr = pool->Add(new ScalarFnCall(texpr_node));
      }
      return Status::OK();
    default:
      stringstream os;
      os << "Unknown expr node type: " << texpr_node.node_type;
      return Status(os.str());
  }
}

struct MemLayoutData {
  int expr_idx;
  int byte_size;
  bool variable_length;

  // TODO: sort by type as well?  Any reason to do this?
  bool operator<(const MemLayoutData& rhs) const {
    // variable_len go at end
    if (this->variable_length && !rhs.variable_length) return false;
    if (!this->variable_length && rhs.variable_length) return true;
    return this->byte_size < rhs.byte_size;
  }
};

int Expr::ComputeResultsLayout(const vector<Expr*>& exprs, vector<int>* offsets,
    int* var_result_begin) {
  if (exprs.size() == 0) {
    *var_result_begin = -1;
    return 0;
  }

  vector<MemLayoutData> data;
  data.resize(exprs.size());

  // Collect all the byte sizes and sort them
  for (int i = 0; i < exprs.size(); ++i) {
    data[i].expr_idx = i;
    if (exprs[i]->type().IsVarLenStringType()) {
      data[i].byte_size = 16;
      data[i].variable_length = true;
    } else {
      data[i].byte_size = exprs[i]->type().GetByteSize();
      data[i].variable_length = false;
    }
    DCHECK_NE(data[i].byte_size, 0);
  }

  sort(data.begin(), data.end());

  // Walk the types and store in a packed aligned layout
  int max_alignment = sizeof(int64_t);
  int current_alignment = data[0].byte_size;
  int byte_offset = 0;

  offsets->resize(exprs.size());
  offsets->clear();
  *var_result_begin = -1;

  for (int i = 0; i < data.size(); ++i) {
    DCHECK_GE(data[i].byte_size, current_alignment);
    // Don't align more than word (8-byte) size.  This is consistent with what compilers
    // do.
    if (exprs[data[i].expr_idx]->type().type == TYPE_CHAR &&
        !exprs[data[i].expr_idx]->type().IsVarLenStringType()) {
      // CHARs are not padded, to be consistent with complier layouts
      // aligns the next value on an 8 byte boundary
      current_alignment = (data[i].byte_size + current_alignment) % max_alignment;
    } else if (data[i].byte_size != current_alignment &&
               current_alignment != max_alignment) {
      byte_offset += data[i].byte_size - current_alignment;
      current_alignment = min(data[i].byte_size, max_alignment);
    }
    (*offsets)[data[i].expr_idx] = byte_offset;
    if (data[i].variable_length && *var_result_begin == -1) {
      *var_result_begin = byte_offset;
    }
    byte_offset += data[i].byte_size;
  }

  return byte_offset;
}

int Expr::ComputeResultsLayout(const vector<ExprContext*>& ctxs, vector<int>* offsets,
    int* var_result_begin) {
  vector<Expr*> exprs;
  for (int i = 0; i < ctxs.size(); ++i) exprs.push_back(ctxs[i]->root());
  return ComputeResultsLayout(exprs, offsets, var_result_begin);
}

void Expr::Close(const vector<ExprContext*>& ctxs, RuntimeState* state) {
  for (int i = 0; i < ctxs.size(); ++i) {
    ctxs[i]->Close(state);
  }
}

Status Expr::Prepare(const vector<ExprContext*>& ctxs, RuntimeState* state,
                     const RowDescriptor& row_desc, MemTracker* tracker) {
  for (int i = 0; i < ctxs.size(); ++i) {
    RETURN_IF_ERROR(ctxs[i]->Prepare(state, row_desc, tracker));
  }
  return Status::OK();
}

Status Expr::Prepare(RuntimeState* state, const RowDescriptor& row_desc,
                     ExprContext* context) {
  DCHECK(type_.type != INVALID_TYPE);
  for (int i = 0; i < children_.size(); ++i) {
    RETURN_IF_ERROR(children_[i]->Prepare(state, row_desc, context));
  }
  return Status::OK();
}

Status Expr::Open(const vector<ExprContext*>& ctxs, RuntimeState* state) {
  for (int i = 0; i < ctxs.size(); ++i) {
    RETURN_IF_ERROR(ctxs[i]->Open(state));
  }
  return Status::OK();
}

Status Expr::Open(RuntimeState* state, ExprContext* context,
                  FunctionContext::FunctionStateScope scope) {
  for (int i = 0; i < children_.size(); ++i) {
    RETURN_IF_ERROR(children_[i]->Open(state, context, scope));
  }
  return Status::OK();
}

Status Expr::Clone(const vector<ExprContext*>& ctxs, RuntimeState* state,
                   vector<ExprContext*>* new_ctxs) {
  DCHECK(new_ctxs != NULL);
  DCHECK(new_ctxs->empty());
  new_ctxs->resize(ctxs.size());
  for (int i = 0; i < ctxs.size(); ++i) {
    RETURN_IF_ERROR(ctxs[i]->Clone(state, &(*new_ctxs)[i]));
  }
  return Status::OK();
}

string Expr::DebugString() const {
  // TODO: implement partial debug string for member vars
  stringstream out;
  out << " type=" << type_.DebugString();
  if (!children_.empty()) {
    out << " children=" << DebugString(children_);
  }
  return out.str();
}

string Expr::DebugString(const vector<Expr*>& exprs) {
  stringstream out;
  out << "[";
  for (int i = 0; i < exprs.size(); ++i) {
    out << (i == 0 ? "" : " ") << exprs[i]->DebugString();
  }
  out << "]";
  return out.str();
}

string Expr::DebugString(const vector<ExprContext*>& ctxs) {
  vector<Expr*> exprs;
  for (int i = 0; i < ctxs.size(); ++i) exprs.push_back(ctxs[i]->root());
  return DebugString(exprs);
}

bool Expr::IsConstant() const {
  for (int i = 0; i < children_.size(); ++i) {
    if (!children_[i]->IsConstant()) return false;
  }
  return true;
}

int Expr::GetSlotIds(vector<SlotId>* slot_ids) const {
  int n = 0;
  for (int i = 0; i < children_.size(); ++i) {
    n += children_[i]->GetSlotIds(slot_ids);
  }
  return n;
}

Function* Expr::GetStaticGetValWrapper(ColumnType type, LlvmCodeGen* codegen) {
  switch (type.type) {
    case TYPE_BOOLEAN:
      return codegen->GetFunction(IRFunction::EXPR_GET_BOOLEAN_VAL);
    case TYPE_TINYINT:
      return codegen->GetFunction(IRFunction::EXPR_GET_TINYINT_VAL);
    case TYPE_SMALLINT:
      return codegen->GetFunction(IRFunction::EXPR_GET_SMALLINT_VAL);
    case TYPE_INT:
      return codegen->GetFunction(IRFunction::EXPR_GET_INT_VAL);
    case TYPE_BIGINT:
      return codegen->GetFunction(IRFunction::EXPR_GET_BIGINT_VAL);
    case TYPE_FLOAT:
      return codegen->GetFunction(IRFunction::EXPR_GET_FLOAT_VAL);
    case TYPE_DOUBLE:
      return codegen->GetFunction(IRFunction::EXPR_GET_DOUBLE_VAL);
    case TYPE_STRING:
    case TYPE_CHAR:
    case TYPE_VARCHAR:
      return codegen->GetFunction(IRFunction::EXPR_GET_STRING_VAL);
    case TYPE_TIMESTAMP:
      return codegen->GetFunction(IRFunction::EXPR_GET_TIMESTAMP_VAL);
    case TYPE_DECIMAL:
      return codegen->GetFunction(IRFunction::EXPR_GET_DECIMAL_VAL);
    default:
      DCHECK(false) << "Invalid type: " << type.DebugString();
      return NULL;
  }
}

Function* Expr::CreateIrFunctionPrototype(LlvmCodeGen* codegen, const string& name,
                                          Value* (*args)[2]) {
  Type* return_type = CodegenAnyVal::GetLoweredType(codegen, type());
  LlvmCodeGen::FnPrototype prototype(codegen, name, return_type);
  prototype.AddArgument(
      LlvmCodeGen::NamedVariable(
          "context", codegen->GetPtrType(ExprContext::LLVM_CLASS_NAME)));
  prototype.AddArgument(
      LlvmCodeGen::NamedVariable("row", codegen->GetPtrType(TupleRow::LLVM_CLASS_NAME)));
  Function* function = prototype.GeneratePrototype(NULL, args[0]);
  DCHECK(function != NULL);
  return function;
}

void Expr::InitBuiltinsDummy() {
  // Call one function from each of the classes to pull all the symbols
  // from that class in.
  // TODO: is there a better way to do this?
  AggregateFunctions::InitNull(NULL, NULL);
  BitByteFunctions::CountSet(NULL, TinyIntVal::null());
  CastFunctions::CastToBooleanVal(NULL, TinyIntVal::null());
  CompoundPredicate::Not(NULL, BooleanVal::null());
  ConditionalFunctions::NullIfZero(NULL, TinyIntVal::null());
  DecimalFunctions::Precision(NULL, DecimalVal::null());
  DecimalOperators::CastToDecimalVal(NULL, DecimalVal::null());
  InPredicate::InIterate(NULL, BigIntVal::null(), 0, NULL);
  IsNullPredicate::IsNull(NULL, BooleanVal::null());
  LikePredicate::Like(NULL, StringVal::null(), StringVal::null());
  Operators::Add_IntVal_IntVal(NULL, IntVal::null(), IntVal::null());
  MathFunctions::Pi(NULL);
  StringFunctions::Length(NULL, StringVal::null());
  TimestampFunctions::Year(NULL, TimestampVal::null());
  UdfBuiltins::Pi(NULL);
  UtilityFunctions::Pid(NULL);
}

AnyVal* Expr::GetConstVal(ExprContext* context) {
  DCHECK(context->opened_);
  if (!IsConstant()) return NULL;
  if (constant_val_.get() != NULL) return constant_val_.get();

  switch (type_.type) {
    case TYPE_BOOLEAN: {
      constant_val_.reset(new BooleanVal(GetBooleanVal(context, NULL)));
      break;
    }
    case TYPE_TINYINT: {
      constant_val_.reset(new TinyIntVal(GetTinyIntVal(context, NULL)));
      break;
    }
    case TYPE_SMALLINT: {
      constant_val_.reset(new SmallIntVal(GetSmallIntVal(context, NULL)));
      break;
    }
    case TYPE_INT: {
      constant_val_.reset(new IntVal(GetIntVal(context, NULL)));
      break;
    }
    case TYPE_BIGINT: {
      constant_val_.reset(new BigIntVal(GetBigIntVal(context, NULL)));
      break;
    }
    case TYPE_FLOAT: {
      constant_val_.reset(new FloatVal(GetFloatVal(context, NULL)));
      break;
    }
    case TYPE_DOUBLE: {
      constant_val_.reset(new DoubleVal(GetDoubleVal(context, NULL)));
      break;
    }
    case TYPE_STRING:
    case TYPE_CHAR:
    case TYPE_VARCHAR: {
      constant_val_.reset(new StringVal(GetStringVal(context, NULL)));
      break;
    }
    case TYPE_TIMESTAMP: {
      constant_val_.reset(new TimestampVal(GetTimestampVal(context, NULL)));
      break;
    }
    case TYPE_DECIMAL: {
      constant_val_.reset(new DecimalVal(GetDecimalVal(context, NULL)));
      break;
    }
    default:
      DCHECK(false) << "Type not implemented: " << type();
  }
  DCHECK(constant_val_.get() != NULL);
  return constant_val_.get();
}


template<> int Expr::GetConstant(const FunctionContext& ctx, ExprConstant c, int i) {
  switch (c) {
    case RETURN_TYPE_SIZE:
      DCHECK_EQ(i, -1);
      return AnyValUtil::TypeDescToColumnType(ctx.GetReturnType()).GetByteSize();
    case ARG_TYPE_SIZE:
      DCHECK_GE(i, 0);
      DCHECK_LT(i, ctx.GetNumArgs());
      return AnyValUtil::TypeDescToColumnType(*ctx.GetArgType(i)).GetByteSize();
    default:
      CHECK(false) << "NYI";
      return -1;
  }
}

Value* Expr::GetIrConstant(LlvmCodeGen* codegen, ExprConstant c, int i) {
  switch (c) {
    case RETURN_TYPE_SIZE:
      DCHECK_EQ(i, -1);
      return ConstantInt::get(codegen->GetType(TYPE_INT), type_.GetByteSize());
    case ARG_TYPE_SIZE:
      DCHECK_GE(i, 0);
      DCHECK_LT(i, children_.size());
      return ConstantInt::get(
          codegen->GetType(TYPE_INT), children_[i]->type_.GetByteSize());
    default:
      CHECK(false) << "NYI";
      return NULL;
  }
}

int Expr::InlineConstants(LlvmCodeGen* codegen, Function* fn) {
  int replaced = 0;
  for (inst_iterator iter = inst_begin(fn), end = inst_end(fn); iter != end; ) {
    // Increment iter now so we don't mess it up modifying the instrunction below
    Instruction* instr = &*(iter++);

    // Look for call instructions
    if (!isa<CallInst>(instr)) continue;
    CallInst* call_instr = cast<CallInst>(instr);
    Function* called_fn = call_instr->getCalledFunction();

    // Look for call to Expr::GetConstant()
    if (called_fn == NULL ||
        called_fn->getName().find(GET_CONSTANT_SYMBOL_PREFIX) == string::npos) continue;

    // 'c' and 'i' arguments must be constant
    ConstantInt* c_arg = dyn_cast<ConstantInt>(call_instr->getArgOperand(1));
    ConstantInt* i_arg = dyn_cast<ConstantInt>(call_instr->getArgOperand(2));
    DCHECK(c_arg != NULL) << "Non-constant 'c' argument to Expr::GetConstant()";
    DCHECK(i_arg != NULL) << "Non-constant 'i' argument to Expr::GetConstant()";

    // Replace the called function with the appropriate constant
    ExprConstant c_val = static_cast<ExprConstant>(c_arg->getSExtValue());
    int i_val = static_cast<int>(i_arg->getSExtValue());
    call_instr->replaceAllUsesWith(GetIrConstant(codegen, c_val, i_val));
    call_instr->eraseFromParent();
    ++replaced;
  }
  return replaced;
}

Status Expr::GetCodegendComputeFnWrapper(RuntimeState* state, Function** fn) {
  if (ir_compute_fn_ != NULL) {
    *fn = ir_compute_fn_;
    return Status::OK();
  }
  LlvmCodeGen* codegen;
  RETURN_IF_ERROR(state->GetCodegen(&codegen));
  Function* static_getval_fn = GetStaticGetValWrapper(type(), codegen);

  // Call it passing this as the additional first argument.
  Value* args[2];
  ir_compute_fn_ = CreateIrFunctionPrototype(codegen, "CodegenComputeFnWrapper", &args);
  BasicBlock* entry_block =
      BasicBlock::Create(codegen->context(), "entry", ir_compute_fn_);
  LlvmCodeGen::LlvmBuilder builder(entry_block);
  Value* this_ptr =
      codegen->CastPtrToLlvmPtr(codegen->GetPtrType(Expr::LLVM_CLASS_NAME), this);
  Value* compute_fn_args[] = { this_ptr, args[0], args[1] };
  Value* ret = CodegenAnyVal::CreateCall(
      codegen, &builder, static_getval_fn, compute_fn_args, "ret");
  builder.CreateRet(ret);
  ir_compute_fn_ = codegen->FinalizeFunction(ir_compute_fn_);
  *fn = ir_compute_fn_;
  return Status::OK();
}

// At least one of these should always be subclassed.
BooleanVal Expr::GetBooleanVal(ExprContext* context, TupleRow* row) {
  DCHECK(false) << DebugString();
  return BooleanVal::null();
}
TinyIntVal Expr::GetTinyIntVal(ExprContext* context, TupleRow* row) {
  DCHECK(false) << DebugString();
  return TinyIntVal::null();
}
SmallIntVal Expr::GetSmallIntVal(ExprContext* context, TupleRow* row) {
  DCHECK(false) << DebugString();
  return SmallIntVal::null();
}
IntVal Expr::GetIntVal(ExprContext* context, TupleRow* row) {
  DCHECK(false) << DebugString();
  return IntVal::null();
}
BigIntVal Expr::GetBigIntVal(ExprContext* context, TupleRow* row) {
  DCHECK(false) << DebugString();
  return BigIntVal::null();
}
FloatVal Expr::GetFloatVal(ExprContext* context, TupleRow* row) {
  DCHECK(false) << DebugString();
  return FloatVal::null();
}
DoubleVal Expr::GetDoubleVal(ExprContext* context, TupleRow* row) {
  DCHECK(false) << DebugString();
  return DoubleVal::null();
}
StringVal Expr::GetStringVal(ExprContext* context, TupleRow* row) {
  DCHECK(false) << DebugString();
  return StringVal::null();
}
ArrayVal Expr::GetArrayVal(ExprContext* context, TupleRow* row) {
  DCHECK(false) << DebugString();
  return ArrayVal::null();
}
TimestampVal Expr::GetTimestampVal(ExprContext* context, TupleRow* row) {
  DCHECK(false) << DebugString();
  return TimestampVal::null();
}
DecimalVal Expr::GetDecimalVal(ExprContext* context, TupleRow* row) {
  DCHECK(false) << DebugString();
  return DecimalVal::null();
}

Status Expr::GetFnContextError(ExprContext* ctx) {
  if (fn_context_index_ != -1) {
    FunctionContext* fn_ctx = ctx->fn_context(fn_context_index_);
    if (fn_ctx->has_error()) return Status(fn_ctx->error_msg());
  }
  return Status::OK();
}

}
