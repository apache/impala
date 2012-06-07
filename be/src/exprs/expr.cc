// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include <sstream>
#include <glog/logging.h>

#include "codegen/llvm-codegen.h"
#include "common/object-pool.h"
#include "common/status.h"
#include "exprs/expr.h"
#include "exprs/agg-expr.h"
#include "exprs/arithmetic-expr.h"
#include "exprs/binary-predicate.h"
#include "exprs/bool-literal.h"
#include "exprs/case-expr.h"
#include "exprs/cast-expr.h"
#include "exprs/compound-predicate.h"
#include "exprs/date-literal.h"
#include "exprs/float-literal.h"
#include "exprs/function-call.h"
#include "exprs/int-literal.h"
#include "exprs/is-null-predicate.h"
#include "exprs/like-predicate.h"
#include "exprs/literal-predicate.h"
#include "exprs/null-literal.h"
#include "exprs/opcode-registry.h"
#include "exprs/string-literal.h"
#include "gen-cpp/Exprs_types.h"
#include "gen-cpp/Data_types.h"
#include "runtime/runtime-state.h"
#include "runtime/raw-value.h"

using namespace std;
using namespace impala;
using namespace llvm;

template<class T>
bool ParseString(const string& str, T* val) {
  istringstream stream(str);
  stream >> *val;
  return !stream.fail();
}

void* ExprValue::TryParse(const string& str, PrimitiveType type) {
  switch(type) {
    case TYPE_BOOLEAN:
      if (ParseString<bool>(str, &bool_val)) return &bool_val;
      break;
    case TYPE_TINYINT:
      if (ParseString<int8_t>(str, &tinyint_val)) return &tinyint_val;
      break;
    case TYPE_SMALLINT:
      if (ParseString<int16_t>(str, &smallint_val)) return &smallint_val;
      break;
    case TYPE_INT:
      if (ParseString<int32_t>(str, &int_val)) return &int_val;
      break;
    case TYPE_BIGINT:
      if (ParseString<int64_t>(str, &bigint_val)) return &bigint_val;
      break;
    case TYPE_FLOAT:
      if (ParseString<float>(str, &float_val)) return &float_val;
      break;
    case TYPE_DOUBLE:
      if (ParseString<double>(str, &double_val)) return &double_val;
      break;
    case TYPE_STRING:
      SetStringVal(str);
      return &string_val;
    default:
      DCHECK(false) << "Invalid type.";
  }
  return NULL;
}

Expr::Expr(PrimitiveType type)
    : opcode_(TExprOpcode::INVALID_OPCODE),
      is_slotref_(false),
      type_(type),
      codegen_fn_(NULL),
      scratch_buffer_size_(0),
      jitted_compute_fn_(NULL) {
}

Expr::Expr(const TExprNode& node)
    : opcode_(node.__isset.opcode ? node.opcode : TExprOpcode::INVALID_OPCODE),
      is_slotref_(false),
      type_(ThriftToType(node.type)),
      codegen_fn_(NULL),
      scratch_buffer_size_(0),
      jitted_compute_fn_(NULL) {
}

Expr::Expr(const TExprNode& node, bool is_slotref)
    : opcode_(node.__isset.opcode ? node.opcode : TExprOpcode::INVALID_OPCODE),
      is_slotref_(is_slotref),
      type_(ThriftToType(node.type)),
      codegen_fn_(NULL),
      scratch_buffer_size_(0),
      jitted_compute_fn_(NULL) {
}

Status Expr::CreateExprTree(ObjectPool* pool, const TExpr& texpr, Expr** root_expr) {
  // input is empty
  if (texpr.nodes.size() == 0) {
    *root_expr = NULL;
    return Status::OK;
  }
  int node_idx = 0;
  RETURN_IF_ERROR(CreateTreeFromThrift(pool, texpr.nodes, NULL, &node_idx, root_expr));
  if (node_idx + 1 != texpr.nodes.size()) {
    // TODO: print thrift msg for diagnostic purposes.
    return Status(
        "Expression tree only partially reconstructed. Not all thrift nodes were used.");
  }
  return Status::OK;
}

Expr* Expr::CreateLiteral(ObjectPool* pool, PrimitiveType type, void* data) {
  DCHECK(data != NULL);
  Expr* result = NULL;

  switch (type) {
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
    default:
      DCHECK(false) << "Invalid type.";
  }
  DCHECK(result != NULL);
  pool->Add(result);
  return result;
}

Expr* Expr::CreateLiteral(ObjectPool* pool, PrimitiveType type, const string& str) {
  ExprValue val;
  Expr* result = NULL;

  switch (type) {
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
    case TYPE_STRING:
      result = new StringLiteral(str);
      break;
    default:
      DCHECK(false) << "Unrecognized type.";
  }
  DCHECK(result != NULL);
  pool->Add(result);
  return result;
}

Status Expr::CreateExprTrees(ObjectPool* pool, const std::vector<TExpr>& texprs,
                             std::vector<Expr*>* exprs) {
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
    // TODO: print thrift msg
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
      // TODO: print thrift msg
      return Status("Failed to reconstruct expression tree from thrift.");
    }
  }
  return Status::OK;
}

Status Expr::CreateExpr(ObjectPool* pool, const TExprNode& texpr_node, Expr** expr) {
  switch (texpr_node.node_type) {
    case TExprNodeType::AGG_EXPR: {
      if (!texpr_node.__isset.agg_expr) {
        return Status("Aggregation expression not set in thrift node");
      }
      *expr = pool->Add(new AggregateExpr(texpr_node));
      return Status::OK;
    }
    case TExprNodeType::ARITHMETIC_EXPR: {
      *expr = pool->Add(new ArithmeticExpr(texpr_node));
      return Status::OK;
    }
    case TExprNodeType::BINARY_PRED: {
      *expr = pool->Add(new BinaryPredicate(texpr_node));
      return Status::OK;
    }
    case TExprNodeType::BOOL_LITERAL: {
      if (!texpr_node.__isset.bool_literal) {
        return Status("Boolean literal not set in thrift node");
      }
      *expr = pool->Add(new BoolLiteral(texpr_node));
      return Status::OK;
    }
    case TExprNodeType::CASE_EXPR: {
      if (!texpr_node.__isset.case_expr) {
        return Status("Case expression not set in thrift node");
      }
      *expr = pool->Add(new CaseExpr(texpr_node));
      return Status::OK;
    }
    case TExprNodeType::CAST_EXPR: {
      *expr = pool->Add(new CastExpr(texpr_node));
      return Status::OK;
    }
    case TExprNodeType::COMPOUND_PRED: {
      *expr = pool->Add(new CompoundPredicate(texpr_node));
      return Status::OK;
    }
    case TExprNodeType::DATE_LITERAL: {
      if (!texpr_node.__isset.date_literal) {
        return Status("Date literal not set in thrift node");
      }
      *expr = pool->Add(new DateLiteral(texpr_node));
      return Status::OK;
    }
    case TExprNodeType::FLOAT_LITERAL: {
      if (!texpr_node.__isset.float_literal) {
        return Status("Float literal not set in thrift node");
      }
      *expr = pool->Add(new FloatLiteral(texpr_node));
      return Status::OK;
    }
    case TExprNodeType::FUNCTION_CALL: {
      *expr = pool->Add(new FunctionCall(texpr_node));
      return Status::OK;
    }
    case TExprNodeType::INT_LITERAL: {
      if (!texpr_node.__isset.int_literal) {
        return Status("Int literal not set in thrift node");
      }
      *expr = pool->Add(new IntLiteral(texpr_node));
      return Status::OK;
    }
    case TExprNodeType::IS_NULL_PRED: {
      if (!texpr_node.__isset.is_null_pred) {
        return Status("Is null predicate not set in thrift node");
      }
      *expr = pool->Add(new IsNullPredicate(texpr_node));
      return Status::OK;
    }
    case TExprNodeType::LIKE_PRED: {
      *expr = pool->Add(new LikePredicate(texpr_node));
      return Status::OK;
    }
    case TExprNodeType::LITERAL_PRED: {
      if (!texpr_node.__isset.literal_pred) {
        return Status("Literal predicate not set in thrift node");
      }
      *expr = pool->Add(new LiteralPredicate(texpr_node));
      return Status::OK;
    }
    case TExprNodeType::NULL_LITERAL: {
      *expr = pool->Add(new NullLiteral(texpr_node));
      return Status::OK;
    }
    case TExprNodeType::SLOT_REF: {
      if (!texpr_node.__isset.slot_ref) {
        return Status("Slot reference not set in thrift node");
      }
      *expr = pool->Add(new SlotRef(texpr_node));
      return Status::OK;
    }
    case TExprNodeType::STRING_LITERAL: {
      if (!texpr_node.__isset.string_literal) {
        return Status("String literal not set in thrift node");
      }
      *expr = pool->Add(new StringLiteral(texpr_node));
      return Status::OK;
    }
    default:
      stringstream os;
      os << "Unknown expr node type: " << texpr_node.node_type;
      return Status(os.str());
  }
}

// TODO: create machine-independent typedefs for 1/2/4/8-byte ints
void Expr::GetValue(TupleRow* row, bool as_ascii, TColumnValue* col_val) {
  void* value = GetValue(row);
  if (as_ascii) {
    RawValue::PrintValue(value, type(), &col_val->stringVal);
    col_val->__isset.stringVal = true;
    return;
  }
  if (value == NULL) return;

  StringValue* string_val = NULL;
  string tmp;
  switch (type_) {
    case TYPE_BOOLEAN:
      col_val->boolVal = *reinterpret_cast<bool*>(value);
      col_val->__isset.boolVal = true;
      break;
    case TYPE_TINYINT:
      col_val->intVal = *reinterpret_cast<int8_t*>(value);
      col_val->__isset.intVal = true;
      break;
    case TYPE_SMALLINT:
      col_val->intVal = *reinterpret_cast<int16_t*>(value);
      col_val->__isset.intVal = true;
      break;
    case TYPE_INT:
      col_val->intVal = *reinterpret_cast<int32_t*>(value);
      col_val->__isset.intVal = true;
      break;
    case TYPE_BIGINT:
      col_val->longVal = *reinterpret_cast<int64_t*>(value);
      col_val->__isset.longVal = true;
      break;
    case TYPE_FLOAT:
      col_val->doubleVal = *reinterpret_cast<float*>(value);
      col_val->__isset.doubleVal = true;
      break;
    case TYPE_DOUBLE:
      col_val->doubleVal = *reinterpret_cast<double*>(value);
      col_val->__isset.doubleVal = true;
      break;
    case TYPE_STRING:
      string_val = reinterpret_cast<StringValue*>(value);
      tmp.assign(static_cast<char*>(string_val->ptr), string_val->len);
      col_val->stringVal.swap(tmp);
      col_val->__isset.stringVal = true;
      break;
    default:
      DCHECK(false) << "bad GetValue() type: " << TypeToString(type_);
  }
}

void Expr::PrintValue(TupleRow* row, string* str) {
  RawValue::PrintValue(GetValue(row), type(), str);
}

void Expr::PrintValue(void* value, string* str) {
  RawValue::PrintValue(value, type(), str);
}

void Expr::PrintValue(void* value, stringstream* stream) {
  RawValue::PrintValue(value, type(), stream);
}

void Expr::PrintValue(TupleRow* row, stringstream* stream) {
  RawValue::PrintValue(GetValue(row), type(), stream);
}

Status Expr::PrepareChildren(RuntimeState* state, const RowDescriptor& row_desc) {
  DCHECK(type_ != INVALID_TYPE);
  for (int i = 0; i < children_.size(); ++i) {
    RETURN_IF_ERROR(children_[i]->Prepare(state, row_desc));
  }
  return Status::OK;
}

Status Expr::Prepare(Expr* root, RuntimeState* state, const RowDescriptor& row_desc) {
  RETURN_IF_ERROR(root->Prepare(state, row_desc));
  LlvmCodeGen* codegen = NULL;
  // state might be NULL when called from Expr-test
  if (state != NULL) codegen = state->llvm_codegen();

  // codegen == NULL means jitting is disabled.
  if (codegen != NULL && root->IsJittable(codegen)) {
    Function* fn = root->CodegenExprTree(codegen);
    if (fn != NULL) {
      void* jitted_fn = codegen->JitFunction(fn);
      DCHECK(jitted_fn != NULL);
      root->SetComputeFn(jitted_fn, 0);
    }
  }
  return Status::OK;
}

Status Expr::Prepare(RuntimeState* state, const RowDescriptor& row_desc) {
  PrepareChildren(state, row_desc);
  // Not all exprs have opcodes (i.e. literals, agg-exprs)
  DCHECK(opcode_ != TExprOpcode::INVALID_OPCODE);
  compute_fn_ = OpcodeRegistry::Instance()->GetFunction(opcode_);
  if (compute_fn_ == NULL) {
    stringstream out;
    out << "Expr::Prepare(): Opcode: " << opcode_ << " does not have a registry entry. ";
    return Status(out.str());
  }
  return Status::OK;
}

Status Expr::Prepare(const std::vector<Expr*>& exprs, RuntimeState* state,
                     const RowDescriptor& row_desc) {
  for (int i = 0; i < exprs.size(); ++i) {
    RETURN_IF_ERROR(Prepare(exprs[i], state, row_desc));
  }
  return Status::OK;
}

string Expr::DebugString() const {
  // TODO: implement partial debug string for member vars
  stringstream out;
  out << " type=" << TypeToString(type_);
  if (opcode_ != TExprOpcode::INVALID_OPCODE) {
    out << " opcode=" << opcode_;
  }
  out << " codegen=" << (codegen_fn_ == NULL ? "false" : "true");
  if (!children_.empty()) {
    out << " children=" << DebugString(children_);
  }
  return out.str();
}

string Expr::DebugString(const std::vector<Expr*>& exprs) {
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
  if (type() == TYPE_STRING) {
    return codegen->string_val_ptr_type();
  } else  if (type() == TYPE_TIMESTAMP) {
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
  prototype.AddArgument(LlvmCodeGen::NamedVariable("is_null", codegen->bool_ptr_type()));

  Function* function = prototype.GeneratePrototype();
  DCHECK(function != NULL);
  codegen_fn_ = function;
  return function;
}

Value* Expr::GetNullReturnValue(LlvmCodeGen* codegen) {
  switch (type()) {
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
      return llvm::ConstantPointerNull::get(codegen->string_val_ptr_type());
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
    Value* args[3], BasicBlock* null_block, BasicBlock* not_null_block) {
  DCHECK(codegen_fn() != NULL);
  LlvmCodeGen::LlvmBuilder builder(caller);
  Value* result = builder.CreateCall3(
      codegen_fn(), args[0], args[1], args[2], "child_result");
  Value* is_null_val = builder.CreateLoad(args[2], "child_null");
  builder.CreateCondBr(is_null_val, null_block, not_null_block);
  return result;
}

Value* Expr::CodegenGetValue(LlvmCodeGen* codegen, BasicBlock* parent, 
    BasicBlock* null_block, BasicBlock* not_null_block) {
  Function::arg_iterator parent_args = parent->getParent()->arg_begin();
  Value* args[3] = { parent_args++, parent_args++, parent_args };
  return CodegenGetValue(codegen, parent, args, null_block, not_null_block);
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

void* Expr::EvalJittedComputeFn(Expr* expr, TupleRow* row) {
  DCHECK(expr->jitted_compute_fn_ != NULL);
  void* func = expr->jitted_compute_fn_;
  void* result = NULL;
  bool is_null = false;
  switch (expr->type()) {
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
  compute_fn_ = Expr::EvalJittedComputeFn;
}

bool Expr::IsJittable(LlvmCodeGen* codegen) const {
  if (type() == TYPE_TIMESTAMP) return false;
  for (int i = 0; i < GetNumChildren(); ++i) {
    if (!children()[i]->IsJittable(codegen)) return false;
  }
  return true;
}

Function* Expr::CodegenExprTree(LlvmCodeGen* codegen) {
  COUNTER_SCOPED_TIMER(codegen->codegen_timer());
  return this->Codegen(codegen);
}
