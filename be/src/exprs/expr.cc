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

#include <thrift/protocol/TDebugProtocol.h>

#include "codegen/codegen-anyval.h"
#include "codegen/llvm-codegen.h"
#include "common/object-pool.h"
#include "common/status.h"
#include "exprs/expr.h"
#include "exprs/aggregate-functions.h"
#include "exprs/arithmetic-expr.h"
#include "exprs/binary-predicate.h"
#include "exprs/bool-literal.h"
#include "exprs/case-expr.h"
#include "exprs/cast-expr.h"
#include "exprs/cast-functions.h"
#include "exprs/char-literal.h"
#include "exprs/compound-predicate.h"
#include "exprs/conditional-functions.h"
#include "exprs/date-literal.h"
#include "exprs/decimal-functions.h"
#include "exprs/decimal-literal.h"
#include "exprs/decimal-operators.h"
#include "exprs/float-literal.h"
#include "exprs/function-call.h"
#include "exprs/hive-udf-call.h"
#include "exprs/in-predicate.h"
#include "exprs/int-literal.h"
#include "exprs/is-null-predicate.h"
#include "exprs/like-predicate.h"
#include "exprs/math-functions.h"
#include "exprs/native-udf-expr.h"
#include "exprs/null-literal.h"
#include "exprs/operators.h"
#include "exprs/string-functions.h"
#include "exprs/string-literal.h"
#include "exprs/timestamp-functions.h"
#include "exprs/timestamp-literal.h"
#include "exprs/tuple-is-null-predicate.h"
#include "exprs/udf-builtins.h"
#include "exprs/utility-functions.h"
#include "gen-cpp/Exprs_types.h"
#include "gen-cpp/Data_types.h"
#include "runtime/lib-cache.h"
#include "runtime/runtime-state.h"
#include "runtime/raw-value.h"

#include "gen-cpp/Exprs_types.h"
#include "gen-cpp/ImpalaService_types.h"

using namespace std;
using namespace impala;
using namespace llvm;

const char* Expr::LLVM_CLASS_NAME = "class.impala::Expr";

namespace impala {

template<class T>
bool ParseString(const string& str, T* val) {
  istringstream stream(str);
  stream >> *val;
  return !stream.fail();
}

Expr::Expr(const ColumnType& type, bool is_slotref)
    : cache_entry_(NULL),
      is_udf_call_(false),
      compute_fn_(NULL),
      is_slotref_(is_slotref),
      type_(type),
      output_scale_(-1),
      ir_compute_fn_(NULL),
      codegen_fn_(NULL),
      adapter_fn_used_(false),
      scratch_buffer_size_(0),
      opened_(false),
      jitted_compute_fn_(NULL) {
}

Expr::Expr(const TExprNode& node, bool is_slotref)
    : cache_entry_(NULL),
      is_udf_call_(false),
      compute_fn_(NULL),
      is_slotref_(is_slotref),
      type_(ColumnType(node.type)),
      output_scale_(-1),
      ir_compute_fn_(NULL),
      codegen_fn_(NULL),
      adapter_fn_used_(false),
      scratch_buffer_size_(0),
      opened_(false),
      jitted_compute_fn_(NULL) {
  if (node.__isset.fn) fn_ = node.fn;
}

Expr::~Expr() {
  DCHECK(cache_entry_ == NULL) << "Need to call Close()";
}

void Expr::Close(RuntimeState* state) {
  for (int i = 0; i < children_.size(); ++i) {
    children_[i]->Close(state);
  }
  if (cache_entry_ != NULL) {
    LibCache::instance()->DecrementUseCount(cache_entry_);
    cache_entry_ = NULL;
  }
}

Status Expr::CreateExprTree(ObjectPool* pool, const TExpr& texpr, Expr** root_expr) {
  // input is empty
  if (texpr.nodes.size() == 0) {
    *root_expr = NULL;
    return Status::OK;
  }
  int node_idx = 0;
  Status status = CreateTreeFromThrift(pool, texpr.nodes, NULL, &node_idx, root_expr);
  if (status.ok() && node_idx + 1 != texpr.nodes.size()) {
    status = Status(
        "Expression tree only partially reconstructed. Not all thrift nodes were used.");
  }
  if (!status.ok()) {
    LOG(ERROR) << "Could not construct expr tree.\n"
               << apache::thrift::ThriftDebugString(texpr);
  }
  return status;
}

Expr* Expr::CreateLiteral(ObjectPool* pool, const ColumnType& type, void* data) {
  DCHECK(type.type == TYPE_NULL || data != NULL);
  Expr* result = NULL;

  switch (type.type) {
    case TYPE_NULL:
      result = new NullLiteral(TYPE_NULL);
      break;
    case TYPE_BOOLEAN:
      result = new BoolLiteral(*reinterpret_cast<bool*>(data));
      break;
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
      result = new IntLiteral(type, data);
      break;
    case TYPE_FLOAT:
    case TYPE_DOUBLE:
      result = new FloatLiteral(type, data);
      break;
    case TYPE_STRING:
      result = new StringLiteral(*reinterpret_cast<StringValue*>(data));
      break;
    case TYPE_CHAR:
      result = new CharLiteral(reinterpret_cast<uint8_t*>(data), type.len);
      break;
    default:
      DCHECK(false) << "Invalid type.";
  }
  DCHECK(result != NULL);
  pool->Add(result);
  return result;
}

Expr* Expr::CreateLiteral(ObjectPool* pool, const ColumnType& type, const string& str) {
  ExprValue val;
  Expr* result = NULL;

  switch (type.type) {
    case TYPE_NULL:
      result = new NullLiteral(TYPE_NULL);
      break;
    case TYPE_BOOLEAN:
      if (ParseString<bool>(str, &val.bool_val))
        result = new BoolLiteral(&val.bool_val);
      break;
    case TYPE_TINYINT:
      if (ParseString<int8_t>(str, &val.tinyint_val))
        result = new IntLiteral(type, &val.tinyint_val);
      break;
    case TYPE_SMALLINT:
      if (ParseString<int16_t>(str, &val.smallint_val))
        result = new IntLiteral(type, &val.smallint_val);
      break;
    case TYPE_INT:
      if (ParseString<int32_t>(str, &val.int_val))
        result = new IntLiteral(type, &val.int_val);
      break;
    case TYPE_BIGINT:
      if (ParseString<int64_t>(str, &val.bigint_val))
        result = new IntLiteral(type, &val.bigint_val);
      break;
    case TYPE_FLOAT:
      if (ParseString<float>(str, &val.float_val))
        result = new FloatLiteral(type, &val.float_val);
      break;
    case TYPE_DOUBLE:
      if (ParseString<double>(str, &val.double_val))
        result = new FloatLiteral(type, &val.double_val);
      break;
    case TYPE_TIMESTAMP:
      if (ParseString<double>(str, &val.double_val))
        result = new TimestampLiteral(val.double_val);
      break;
    case TYPE_STRING:
      result = new StringLiteral(str);
      break;
    case TYPE_CHAR:
      result = new CharLiteral(str);
      break;
    default:
      DCHECK(false) << "Unrecognized type.";
  }
  DCHECK(result != NULL);
  pool->Add(result);
  return result;
}

Status Expr::CreateExprTrees(ObjectPool* pool, const vector<TExpr>& texprs,
                             vector<Expr*>* exprs) {
  exprs->clear();
  for (int i = 0; i < texprs.size(); ++i) {
    Expr* expr;
    RETURN_IF_ERROR(CreateExprTree(pool, texprs[i], &expr));
    exprs->push_back(expr);
  }
  return Status::OK;
}

Status Expr::CreateTreeFromThrift(ObjectPool* pool, const vector<TExprNode>& nodes,
    Expr* parent, int* node_idx, Expr** root_expr) {
  // propagate error case
  if (*node_idx >= nodes.size()) {
    return Status("Failed to reconstruct expression tree from thrift.");
  }
  int num_children = nodes[*node_idx].num_children;
  Expr* expr = NULL;
  RETURN_IF_ERROR(CreateExpr(pool, nodes[*node_idx], &expr));
  // assert(parent != NULL || (node_idx == 0 && root_expr != NULL));
  if (parent != NULL) {
    parent->AddChild(expr);
  } else {
    *root_expr = expr;
  }
  for (int i = 0; i < num_children; i++) {
    *node_idx += 1;
    RETURN_IF_ERROR(CreateTreeFromThrift(pool, nodes, expr, node_idx, NULL));
    // we are expecting a child, but have used all nodes
    // this means we have been given a bad tree and must fail
    if (*node_idx >= nodes.size()) {
      return Status("Failed to reconstruct expression tree from thrift.");
    }
  }
  return Status::OK;
}

Status Expr::CreateExpr(ObjectPool* pool, const TExprNode& texpr_node, Expr** expr) {
  switch (texpr_node.node_type) {
    case TExprNodeType::ARITHMETIC_EXPR:
      *expr = pool->Add(new ArithmeticExpr(texpr_node));
      return Status::OK;
    case TExprNodeType::BINARY_PRED:
      *expr = pool->Add(new BinaryPredicate(texpr_node));
      return Status::OK;
    case TExprNodeType::BOOL_LITERAL:
      if (!texpr_node.__isset.bool_literal) {
        return Status("Boolean literal not set in thrift node");
      }
      *expr = pool->Add(new BoolLiteral(texpr_node));
      return Status::OK;
    case TExprNodeType::CASE_EXPR:
      if (!texpr_node.__isset.case_expr) {
        return Status("Case expression not set in thrift node");
      }
      *expr = pool->Add(new CaseExpr(texpr_node));
      return Status::OK;
    case TExprNodeType::CAST_EXPR:
      *expr = pool->Add(new CastExpr(texpr_node));
      return Status::OK;
    case TExprNodeType::COMPOUND_PRED:
      *expr = pool->Add(new CompoundPredicate(texpr_node));
      return Status::OK;
    case TExprNodeType::DATE_LITERAL:
      if (!texpr_node.__isset.date_literal) {
        return Status("Date literal not set in thrift node");
      }
      *expr = pool->Add(new DateLiteral(texpr_node));
      return Status::OK;
    case TExprNodeType::DECIMAL_LITERAL:
      if (!texpr_node.__isset.decimal_literal) {
        return Status("Decimal literal not set in thrift node");
      }
      *expr = pool->Add(new DecimalLiteral(texpr_node));
      return Status::OK;
    case TExprNodeType::FLOAT_LITERAL:
      if (!texpr_node.__isset.float_literal) {
        return Status("Float literal not set in thrift node");
      }
      *expr = pool->Add(new FloatLiteral(texpr_node));
      return Status::OK;
    case TExprNodeType::COMPUTE_FUNCTION_CALL:
      *expr = pool->Add(new FunctionCall(texpr_node));
      return Status::OK;
    case TExprNodeType::INT_LITERAL:
      if (!texpr_node.__isset.int_literal) {
        return Status("Int literal not set in thrift node");
      }
      *expr = pool->Add(new IntLiteral(texpr_node));
      return Status::OK;
    case TExprNodeType::IN_PRED:
      if (!texpr_node.__isset.in_predicate) {
        return Status("In predicate not set in thrift node");
      }
      *expr = pool->Add(new InPredicate(texpr_node));
      return Status::OK;
    case TExprNodeType::IS_NULL_PRED:
      if (!texpr_node.__isset.is_null_pred) {
        return Status("Is null predicate not set in thrift node");
      }
      *expr = pool->Add(new IsNullPredicate(texpr_node));
      return Status::OK;
    case TExprNodeType::LIKE_PRED:
      *expr = pool->Add(new LikePredicate(texpr_node));
      return Status::OK;
    case TExprNodeType::NULL_LITERAL:
      *expr = pool->Add(new NullLiteral(texpr_node));
      return Status::OK;
    case TExprNodeType::SLOT_REF:
      if (!texpr_node.__isset.slot_ref) {
        return Status("Slot reference not set in thrift node");
      }
      *expr = pool->Add(new SlotRef(texpr_node));
      return Status::OK;
    case TExprNodeType::STRING_LITERAL:
      if (!texpr_node.__isset.string_literal) {
        return Status("String literal not set in thrift node");
      }
      *expr = pool->Add(new StringLiteral(texpr_node));
      return Status::OK;
    case TExprNodeType::TUPLE_IS_NULL_PRED:
      *expr = pool->Add(new TupleIsNullPredicate(texpr_node));
      return Status::OK;
    case TExprNodeType::FUNCTION_CALL:
      if (!texpr_node.__isset.fn) {
        return Status("Function not set in thrift node");
      }
      if (texpr_node.fn.binary_type == TFunctionBinaryType::HIVE) {
        *expr = pool->Add(new HiveUdfCall(texpr_node));
      } else {
        *expr = pool->Add(new NativeUdfExpr(texpr_node));
      }
      return Status::OK;
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
    if (exprs[i]->type().type == TYPE_STRING) {
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
    if (data[i].byte_size != current_alignment && current_alignment != max_alignment) {
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

void Expr::GetValue(TupleRow* row, bool as_ascii, TColumnValue* col_val) {
  void* value = GetValue(row);
  if (as_ascii) {
    RawValue::PrintValue(value, type(), output_scale_, &col_val->string_val);
    col_val->__isset.string_val = true;
    return;
  }
  if (value == NULL) return;

  StringValue* string_val = NULL;
  string tmp;
  switch (type_.type) {
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
      switch (type_.GetByteSize()) {
        case 4:
          col_val->string_val =
              reinterpret_cast<Decimal4Value*>(value)->ToString(type_);
          break;
        case 8:
          col_val->string_val =
              reinterpret_cast<Decimal8Value*>(value)->ToString(type_);
          break;
        case 16:
          col_val->string_val =
              reinterpret_cast<Decimal16Value*>(value)->ToString(type_);
          break;
        default:
          DCHECK(false) << "Bad Type: " << type_;
      }
      col_val->__isset.string_val = true;
      break;
    case TYPE_STRING:
      string_val = reinterpret_cast<StringValue*>(value);
      tmp.assign(static_cast<char*>(string_val->ptr), string_val->len);
      col_val->string_val.swap(tmp);
      col_val->__isset.string_val = true;
      break;
    case TYPE_TIMESTAMP:
      RawValue::PrintValue(value, type(), output_scale_, &col_val->string_val);
      col_val->__isset.string_val = true;
      break;
    default:
      DCHECK(false) << "bad GetValue() type: " << type_.DebugString();
  }
}

void Expr::Close(const vector<Expr*>& exprs, RuntimeState* state) {
  for (int i = 0; i < exprs.size(); ++i) {
    exprs[i]->Close(state);
  }
}

Status Expr::PrepareChildren(RuntimeState* state, const RowDescriptor& row_desc) {
  DCHECK(type_.type != INVALID_TYPE);
  for (int i = 0; i < children_.size(); ++i) {
    RETURN_IF_ERROR(children_[i]->Prepare(state, row_desc));
  }
  return Status::OK;
}

Status Expr::Prepare(Expr* root, RuntimeState* state, const RowDescriptor& row_desc,
    bool disable_codegen, bool* thread_safe) {
  RETURN_IF_ERROR(root->Prepare(state, row_desc));
  LlvmCodeGen* codegen = NULL;
  // state might be NULL when called from tests
  if (state != NULL && state->codegen_enabled() && !disable_codegen) {
    codegen = state->codegen();
    DCHECK(codegen != NULL);
  }

  if (codegen != NULL && root->IsJittable(codegen)) {
    root->CodegenExprTree(codegen);
    if (thread_safe != NULL) *thread_safe = root->codegend_fn_thread_safe();
  }
  return Status::OK;
}

bool Expr::codegend_fn_thread_safe() const {
  if (adapter_fn_used_) return false;
  for (int i = 0; i < children_.size(); ++i) {
    if (!children_[i]->codegend_fn_thread_safe()) return false;
  }
  return true;
}

Status Expr::Prepare(RuntimeState* state, const RowDescriptor& row_desc) {
  RETURN_IF_ERROR(PrepareChildren(state, row_desc));
  if (is_udf_call_) return Status::OK;

  // Not all exprs have a function set (i.e. literals)
  if (fn_.__isset.scalar_fn) {
    if (!fn_.scalar_fn.symbol.empty()) {
      DCHECK_EQ(fn_.binary_type, TFunctionBinaryType::BUILTIN);
      void* fn_ptr;
      Status status = LibCache::instance()->GetSoFunctionPtr(
          "", fn_.scalar_fn.symbol, &fn_ptr, &cache_entry_);
      if (!status.ok()) {
        // Builtins symbols should exist unless there is a version mismatch.
        stringstream ss;
        ss << "Builtin '" << fn_.name.function_name << "' with symbol '"
           << fn_.scalar_fn.symbol << "' does not exist. "
           << "Verify that all your impalads are the same version.";
        status.AddErrorMsg(ss.str());
        return status;
      }
      DCHECK(fn_ptr != NULL);
      compute_fn_ = reinterpret_cast<ComputeFn>(fn_ptr);
    } else {
      stringstream ss;
      ss << "Function " << fn_.name.function_name << " is not implemented.";
      return Status(ss.str());
    }
  }
  return Status::OK;
}

Status Expr::Prepare(const vector<Expr*>& exprs, RuntimeState* state,
                     const RowDescriptor& row_desc, bool disable_codegen,
                     bool* thread_safe) {
  for (int i = 0; i < exprs.size(); ++i) {
    RETURN_IF_ERROR(Prepare(exprs[i], state, row_desc, disable_codegen, thread_safe));
  }
  return Status::OK;
}

Status Expr::Open(const vector<Expr*>& exprs, RuntimeState* state) {
  // TODO: call prepare function with FRAGMENT_LOCAL scope
  for (int i = 0; i < exprs.size(); ++i) {
    RETURN_IF_ERROR(exprs[i]->Open(state));
  }
  return Status::OK;
}

Status Expr::Open(RuntimeState* state) {
  for (int i = 0; i < children_.size(); ++i) {
    RETURN_IF_ERROR(children_[i]->Open(state));
  }
  opened_ = true;
  return Status::OK;
}

bool Expr::IsCodegenAvailable(const vector<Expr*>& exprs) {
  for (int i = 0; i < exprs.size(); ++i) {
    if (exprs[i]->codegen_fn() == NULL) return false;
  }
  return true;
}

string Expr::DebugString() const {
  // TODO: implement partial debug string for member vars
  stringstream out;
  out << " type=" << type_.DebugString();
  out << " codegen=" << (codegen_fn_ == NULL ? "false" : "true");
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

Type* Expr::GetLlvmReturnType(LlvmCodeGen* codegen) const {
  if (type().type == TYPE_STRING) {
    return codegen->GetPtrType(TYPE_STRING);
  } else  if (type().type == TYPE_TIMESTAMP) {
    // TODO
    return NULL;
  } else {
    return codegen->GetType(type());
  }
}

Function* Expr::CreateComputeFnPrototype(LlvmCodeGen* codegen, const string& name) {
  DCHECK(codegen_fn_ == NULL);
  Type* ret_type = GetLlvmReturnType(codegen);
  Type* ptr_type = codegen->ptr_type();

  LlvmCodeGen::FnPrototype prototype(codegen, name, ret_type);
  prototype.AddArgument(LlvmCodeGen::NamedVariable("row", PointerType::get(ptr_type, 0)));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("state_data", ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("is_null",
        codegen->GetPtrType(TYPE_NULL)));

  Function* function = prototype.GeneratePrototype();
  DCHECK(function != NULL);
  codegen_fn_ = function;
  return function;
}

Value* Expr::GetNullReturnValue(LlvmCodeGen* codegen) {
  switch (type().type) {
    case TYPE_NULL:
      return ConstantInt::get(codegen->context(), APInt(1, 0, true));
    case TYPE_BOOLEAN:
      return ConstantInt::get(codegen->context(), APInt(1, 0, true));
    case TYPE_TINYINT:
      return ConstantInt::get(codegen->context(), APInt(8, 0, true));
    case TYPE_SMALLINT:
      return ConstantInt::get(codegen->context(), APInt(16, 0, true));
    case TYPE_INT:
      return ConstantInt::get(codegen->context(), APInt(32, 0, true));
    case TYPE_BIGINT:
      return ConstantInt::get(codegen->context(), APInt(64, 0, true));
    case TYPE_FLOAT:
      return ConstantFP::get(codegen->context(), APFloat((float)0));
    case TYPE_DOUBLE:
      return ConstantFP::get(codegen->context(), APFloat((double)0));
    case TYPE_STRING:
      return llvm::ConstantPointerNull::get(codegen->GetPtrType(TYPE_STRING));
    default:
      // Add timestamp pointers
      DCHECK(false) << "Not yet implemented.";
      return NULL;
  }
}

void Expr::CodegenSetIsNullArg(LlvmCodeGen* codegen, BasicBlock* block, bool val) {
  LlvmCodeGen::LlvmBuilder builder(block);
  Function::arg_iterator func_args = block->getParent()->arg_begin();
  ++func_args;
  ++func_args;
  Value* is_null_ptr = func_args;
  Value* value = val ? codegen->true_value() : codegen->false_value();
  builder.CreateStore(value, is_null_ptr);
}

Value* Expr::CodegenGetValue(LlvmCodeGen* codegen, BasicBlock* caller,
    Value* args[3], BasicBlock* null_block, BasicBlock* not_null_block,
    const char* result_var_name) {
  DCHECK(codegen_fn() != NULL);
  LlvmCodeGen::LlvmBuilder builder(caller);
  Value* result = builder.CreateCall3(
      codegen_fn(), args[0], args[1], args[2], result_var_name);
  Value* is_null_val = builder.CreateLoad(args[2], "child_null");
  builder.CreateCondBr(is_null_val, null_block, not_null_block);
  return result;
}

Value* Expr::CodegenGetValue(LlvmCodeGen* codegen, BasicBlock* parent,
    BasicBlock* null_block, BasicBlock* not_null_block, const char* result_var_name) {
  Function::arg_iterator parent_args = parent->getParent()->arg_begin();
  Value* args[3] = { parent_args++, parent_args++, parent_args };
  return CodegenGetValue(
      codegen, parent, args, null_block, not_null_block, result_var_name);
}

// typedefs for jitted compute functions
typedef bool (*BoolComputeFn)(TupleRow*, char* , bool*);
typedef int8_t (*TinyIntComputeFn)(TupleRow*, char*, bool*);
typedef int16_t (*SmallIntComputeFn)(TupleRow*, char*, bool*);
typedef int32_t (*IntComputeFn)(TupleRow*, char*, bool*);
typedef int64_t (*BigintComputeFn)(TupleRow*, char*, bool*);
typedef float (*FloatComputeFn)(TupleRow*, char*, bool*);
typedef double (*DoubleComputeFn)(TupleRow*, char*, bool*);
typedef StringValue* (*StringValueComputeFn)(TupleRow*, char*, bool*);

void* Expr::EvalCodegendComputeFn(Expr* expr, TupleRow* row) {
  DCHECK(expr->jitted_compute_fn_ != NULL);
  void* func = expr->jitted_compute_fn_;
  void* result = NULL;
  bool is_null = false;
  switch (expr->type().type) {
    case TYPE_NULL: {
      is_null = true;
      break;
    }
    case TYPE_BOOLEAN: {
      BoolComputeFn new_func = reinterpret_cast<BoolComputeFn>(func);
      expr->result_.bool_val = new_func(row, NULL, &is_null);
      result = &(expr->result_.bool_val);
      break;
    }
    case TYPE_TINYINT: {
      TinyIntComputeFn new_func = reinterpret_cast<TinyIntComputeFn>(func);
      expr->result_.tinyint_val = new_func(row, NULL, &is_null);
      result = &(expr->result_.tinyint_val);
      break;
    }
    case TYPE_SMALLINT: {
      SmallIntComputeFn new_func = reinterpret_cast<SmallIntComputeFn>(func);
      expr->result_.smallint_val = new_func(row, NULL, &is_null);
      result = &(expr->result_.smallint_val);
      break;
    }
    case TYPE_INT: {
      IntComputeFn new_func = reinterpret_cast<IntComputeFn>(func);
      expr->result_.int_val = new_func(row, NULL, &is_null);
      result = &(expr->result_.int_val);
      break;
    }
    case TYPE_BIGINT: {
      BigintComputeFn new_func = reinterpret_cast<BigintComputeFn>(func);
      expr->result_.bigint_val = new_func(row, NULL, &is_null);
      result = &(expr->result_.bigint_val);
      break;
    }
    case TYPE_FLOAT: {
      FloatComputeFn new_func = reinterpret_cast<FloatComputeFn>(func);
      expr->result_.float_val = new_func(row, NULL, &is_null);
      result = &(expr->result_.float_val);
      break;
    }
    case TYPE_DOUBLE: {
      DoubleComputeFn new_func = reinterpret_cast<DoubleComputeFn>(func);
      expr->result_.double_val = new_func(row, NULL, &is_null);
      result = &(expr->result_.double_val);
      break;
    }
    case TYPE_STRING: {
      StringValueComputeFn new_func = reinterpret_cast<StringValueComputeFn>(func);
      result = new_func(row, NULL, &is_null);
      break;
    }
    default:
      DCHECK(false) << expr->type();
      break;
  }
  if (is_null) return NULL;
  return result;
}

void Expr::SetComputeFn(void* jitted_function, int scratch_size) {
  DCHECK_EQ(scratch_size, 0);
  jitted_compute_fn_ = jitted_function;
  compute_fn_ = Expr::EvalCodegendComputeFn;
}

bool Expr::IsJittable(LlvmCodeGen* codegen) const {
  if (type().type == TYPE_TIMESTAMP || type().type == TYPE_CHAR ||
      type().type == TYPE_DECIMAL) return false;
  for (int i = 0; i < GetNumChildren(); ++i) {
    if (!children()[i]->IsJittable(codegen)) return false;
  }
  return true;
}

Function* Expr::CodegenExprTree(LlvmCodeGen* codegen) {
  SCOPED_TIMER(codegen->codegen_timer());
  return this->Codegen(codegen);
}

// Codegen an adapter IR function that just calls the underlying GetValue
// define double @ExprFn(i8** %row, i8* %state_data, i1* %is_null) {
// entry:
//   %0 = bitcast i8** %row to %"class.impala::TupleRow"*
//   %1 = call i8* @IrExprGetValue(%"class.impala::Expr"*
//      inttoptr (i64 194529872 to %"class.impala::Expr"*), %"class.impala::TupleRow"* %0)
//   %2 = icmp eq i8* %1, null
//   store i1 %2, i1* %is_null
//   br i1 %2, label %null, label %not_null
//
// null:
//   ret double 0
//
// not_null:
//   %3 = bitcast i8* %1 to double*
//   %4 = load double* %3
//   ret double %4
// }
Function* Expr::Codegen(LlvmCodeGen* codegen) {
  // This results in expr trees that are not thread safe and therefore not
  // usable by the scanner threads (causes them to crash).
  // Disable this for now.
  LLVMContext& context = codegen->context();
  LlvmCodeGen::LlvmBuilder builder(context);

  // Get llvm compatible types for Expr and TupleRow
  Type* expr_type = codegen->GetType(Expr::LLVM_CLASS_NAME);
  Type* tuple_row_type = codegen->GetType(TupleRow::LLVM_CLASS_NAME);
  DCHECK(expr_type != NULL);
  DCHECK(tuple_row_type != NULL);

  PointerType* expr_ptr_type = PointerType::get(expr_type, 0);
  PointerType* tuple_row_ptr_type = PointerType::get(tuple_row_type, 0);

  Function* interpreted_fn = codegen->GetFunction(IRFunction::EXPR_GET_VALUE);
  DCHECK(interpreted_fn != NULL);

  Function* function = CreateComputeFnPrototype(codegen, "ExprFn");
  BasicBlock* entry_block = BasicBlock::Create(context, "entry", function);

  // Get the llvm input arguments to the codegen'd function
  Function::arg_iterator args_it = function->arg_begin();
  Value* args[3] = { args_it++, args_it++, args_it };

  builder.SetInsertPoint(entry_block);

  // Convert the llvm types to ComputeFn compatible types
  Value* row = builder.CreateBitCast(args[0], tuple_row_ptr_type);
  Value* this_llvm = codegen->CastPtrToLlvmPtr(expr_ptr_type, this);

  // Call the underlying function
  Value* result = builder.CreateCall2(interpreted_fn, this_llvm, row);
  Value* is_null = builder.CreateIsNull(result);
  builder.CreateStore(is_null, args[2]);

  BasicBlock* null_block, *not_null_block;
  codegen->CreateIfElseBlocks(function, "null", "not_null",
      &null_block, &not_null_block);
  builder.CreateCondBr(is_null, null_block, not_null_block);

  builder.SetInsertPoint(null_block);
  builder.CreateRet(GetNullReturnValue(codegen));

  builder.SetInsertPoint(not_null_block);
  // Convert the void* ComputeFn return value to the typed llvm version
  switch (type().type) {
    case TYPE_NULL:
      builder.CreateRet(codegen->false_value());
      break;

    case TYPE_STRING: {
      Value* cast_ptr_value = builder.CreateBitCast(result, codegen->GetPtrType(type()));
      builder.CreateRet(cast_ptr_value);
      break;
    }

    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
    case TYPE_FLOAT:
    case TYPE_DOUBLE: {
      Value* cast_ptr_value = builder.CreateBitCast(result, codegen->GetPtrType(type()));
      Value* value = builder.CreateLoad(cast_ptr_value);
      builder.CreateRet(value);
      break;
    }

    default:
      DCHECK(false);
  }

  adapter_fn_used_ = true;
  return codegen->FinalizeFunction(function);
}

Status Expr::GetIrComputeFn(RuntimeState* state, Function** fn) {
  if (ir_compute_fn_ != NULL) {
    *fn = ir_compute_fn_;
    return Status::OK;
  }
  DCHECK(state->codegen() != NULL);
  Status status = GetWrapperIrComputeFunction(state->codegen(), fn);
  if (status.ok()) ir_compute_fn_ = *fn;
  return status;
}

// Codegens a function that calls GetValue(this, row) and wraps the returned void* value
// in the appropriate AnyVal type. Note that the AnyVal types are lowered.
// Example generated function IR for a TYPE_SMALLINT expr returning a lowered SmallIntVal
// ({i8, i16} => i32):
//
// define i32 @ExprWrapper(i8* %context, %"class.impala::TupleRow"* %row) {
// entry:
//   %raw = call i8* @IrExprGetValue(
//      %"class.impala::Expr"* inttoptr (i64 82763584 to %"class.impala::Expr"*),
//      %"class.impala::TupleRow"* %row)
//   %0 = alloca i32
//   %result = load i32* %0
//   %is_null = icmp eq i8* %raw, null
//   %masked = and i32 %result, -256
//   %is_null_ext = zext i1 %is_null to i32
//   %result1 = or i32 %masked, %is_null_ext
//   br i1 %is_null, label %null, label %non_null
//
// null:                                             ; preds = %entry
//   ret i32 %result1
//
// non_null:                                         ; preds = %entry
//   %val_ptr = bitcast i8* %raw to i16*
//   %val = load i16* %val_ptr
//   %1 = zext i16 %val to i32
//   %2 = shl i32 %1, 16
//   %3 = and i32 %result1, 65535
//   %result2 = or i32 %3, %2
//   ret i32 %result2
// }
Status Expr::GetWrapperIrComputeFunction(LlvmCodeGen* codegen, Function** fn) {
  Value* args[2];
  *fn = CreateIrFunctionPrototype(codegen, "ExprWrapper", &args);
  BasicBlock* block = BasicBlock::Create(codegen->context(), "entry", *fn);
  LlvmCodeGen::LlvmBuilder builder(block);

  // Call interpreted compute function
  Function* interpreted_fn = codegen->GetFunction(IRFunction::EXPR_GET_VALUE);
  DCHECK(interpreted_fn != NULL);
  Value* this_arg = codegen->CastPtrToLlvmPtr(
      codegen->GetPtrType(Expr::LLVM_CLASS_NAME), this);
  Value* row_arg = args[1];
  Value* raw_result = builder.CreateCall2(interpreted_fn, this_arg, row_arg, "raw");

  // Convert void* 'raw_result' to *Val 'result'
  CodegenAnyVal result(codegen, &builder, type(), NULL, "result");

  Value* is_null = builder.CreateIsNull(raw_result, "is_null");
  result.SetIsNull(is_null);
  BasicBlock* null_block;
  BasicBlock* non_null_block;
  codegen->CreateIfElseBlocks(*fn, "null", "non_null", &null_block, &non_null_block);
  builder.CreateCondBr(is_null, null_block, non_null_block);

  // If expr returns NULL, return *Val 'result' immediately
  builder.SetInsertPoint(null_block);
  builder.CreateRet(result.value());

  // else set result.val
  builder.SetInsertPoint(non_null_block);
  result.SetFromRawPtr(raw_result);
  builder.CreateRet(result.value());

  *fn = codegen->FinalizeFunction(*fn);
  DCHECK(*fn != NULL);
  return Status::OK;
}

Function* Expr::CreateIrFunctionPrototype(LlvmCodeGen* codegen, const string& name,
                                          Value* (*args)[2]) {
  Type* return_type = CodegenAnyVal::GetLoweredType(codegen, type());
  LlvmCodeGen::FnPrototype prototype(codegen, name, return_type);
  // TODO: Placeholder for ExprContext argument
  prototype.AddArgument(LlvmCodeGen::NamedVariable("context", codegen->ptr_type()));
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
  CastFunctions::CastToBooleanVal(NULL, TinyIntVal::null());
  ConditionalFunctions::IsNull(NULL, NULL);
  DecimalFunctions::Precision(NULL, DecimalVal::null());
  DecimalOperators::CastToDecimalVal(NULL, DecimalVal::null());
  MathFunctions::Pi(NULL);
  Operators::Add_IntVal_IntVal(NULL, IntVal::null(), IntVal::null());
  StringFunctions::Length(NULL, NULL);
  TimestampFunctions::Year(NULL, TimestampVal::null());
  UdfBuiltins::Pi(NULL);
  UtilityFunctions::Pid(NULL, NULL);
}

AnyVal* Expr::GetConstVal() {
  DCHECK(opened_);
  if (!IsConstant()) return NULL;
  if (constant_val_.get() != NULL) return constant_val_.get();

  void* val = GetValue(NULL);
  switch (type_.type) {
    case TYPE_BOOLEAN: {
      BooleanVal v = BooleanVal::null();
      if (val != NULL) v = BooleanVal(*reinterpret_cast<bool*>(val));
      constant_val_.reset(new BooleanVal(v));
      break;
    }
    case TYPE_TINYINT: {
      TinyIntVal v = TinyIntVal::null();
      if (val != NULL) v = TinyIntVal(*reinterpret_cast<int8_t*>(val));
      constant_val_.reset(new TinyIntVal(v));
      break;
    }
    case TYPE_SMALLINT: {
      SmallIntVal v = SmallIntVal::null();
      if (val != NULL) v = SmallIntVal(*reinterpret_cast<int16_t*>(val));
      constant_val_.reset(new SmallIntVal(v));
      break;
    }
    case TYPE_INT: {
      IntVal v = IntVal::null();
      if (val != NULL) v = IntVal(*reinterpret_cast<int32_t*>(val));
      constant_val_.reset(new IntVal(v));
      break;
    }
    case TYPE_BIGINT: {
      BigIntVal v = BigIntVal::null();
      if (val != NULL) v = BigIntVal(*reinterpret_cast<int64_t*>(val));
      constant_val_.reset(new BigIntVal(v));
      break;
    }
    case TYPE_FLOAT: {
      FloatVal v = FloatVal::null();
      if (val != NULL) v = FloatVal(*reinterpret_cast<float*>(val));
      constant_val_.reset(new FloatVal(v));
      break;
    }
    case TYPE_DOUBLE: {
      DoubleVal v = DoubleVal::null();
      if (val != NULL) v = DoubleVal(*reinterpret_cast<double*>(val));
      constant_val_.reset(new DoubleVal(v));
      break;
    }
    case TYPE_STRING: {
      StringVal v = StringVal::null();
      if (val != NULL) {
        StringValue* sv = reinterpret_cast<StringValue*>(val);
        sv->ToStringVal(&v);
      }
      constant_val_.reset(new StringVal(v));
      break;
    }
    case TYPE_TIMESTAMP: {
      TimestampVal v = TimestampVal::null();
      if (val != NULL) {
        TimestampValue* tv = reinterpret_cast<TimestampValue*>(val);
        tv->ToTimestampVal(&v);
      }
      constant_val_.reset(new TimestampVal(v));
      break;
    }
    case TYPE_DECIMAL: {
      DecimalVal v = DecimalVal::null();
      if (val != NULL) {
        switch (type_.GetByteSize()) {
          case 4:
            v = DecimalVal(reinterpret_cast<Decimal4Value*>(val)->value());
            break;
          case 8:
            v = DecimalVal(reinterpret_cast<Decimal8Value*>(val)->value());
            break;
          case 16:
            v = DecimalVal(reinterpret_cast<Decimal16Value*>(val)->value());
            break;
          default:
            DCHECK(false) << type_.GetByteSize();
        }
      }
      constant_val_.reset(new DecimalVal(v));
      break;
    }
    default:
      DCHECK(false) << "Type not implemented: " << type();
  }
  DCHECK(constant_val_.get() != NULL);
  return constant_val_.get();
}

}
