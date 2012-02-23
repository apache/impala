// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include <sstream>
#include <glog/logging.h>

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
#include "gen-cpp/ImpalaService_types.h"
#include "runtime/raw-value.h"

using namespace std;
using namespace impala;

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
      type_(type) {
}

Expr::Expr(const TExprNode& node)
    : opcode_(node.__isset.opcode ? node.opcode : TExprOpcode::INVALID_OPCODE),
      is_slotref_(false),
      type_(ThriftToType(node.type)) {
}

Expr::Expr(const TExprNode& node, bool is_slotref)
    : opcode_(node.__isset.opcode ? node.opcode : TExprOpcode::INVALID_OPCODE),
      is_slotref_(is_slotref),
      type_(ThriftToType(node.type)) {
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
      *expr = pool->Add(new BoolLiteral(texpr_node));
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
    PrintValue(value, &col_val->stringVal);
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
  PrintValue(GetValue(row), str);
}

void Expr::PrintValue(void* value, string* str) {
  RawValue::PrintValue(value, type_, str);
}

Status Expr::PrepareChildren(RuntimeState* state, const RowDescriptor& row_desc) {
  DCHECK(type_ != INVALID_TYPE);
  for (int i = 0; i < children_.size(); ++i) {
    RETURN_IF_ERROR(children_[i]->Prepare(state, row_desc));
  }
  return Status::OK;
}

Status Expr::Prepare(RuntimeState* state, const RowDescriptor& row_desc) {
  PrepareChildren(state, row_desc);
  // Not all exprs have opcodes (i.e. literals, agg-exprs)
  DCHECK(opcode_ != TExprOpcode::INVALID_OPCODE);
  compute_function_ = OpcodeRegistry::Instance()->GetFunction(opcode_);
  if (compute_function_ == NULL) {
    stringstream out;
    out << "Expr::Prepare(): Opcode: " << opcode_ << " does not have a registry entry. ";
    return Status(out.str());
  }
  return Status::OK;
}

Status Expr::Prepare(const std::vector<Expr*>& exprs, RuntimeState* state,
                     const RowDescriptor& row_desc) {
  for (int i = 0; i < exprs.size(); ++i) {
    RETURN_IF_ERROR(exprs[i]->Prepare(state, row_desc));
  }
  return Status::OK;
}

string Expr::DebugString() const {
  // TODO: implement partial debug string for member vars
  stringstream out;
  out << "type=" << TypeToString(type_);
  if (opcode_ != TExprOpcode::INVALID_OPCODE) {
    out << " opcode=" << opcode_;
  }
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

