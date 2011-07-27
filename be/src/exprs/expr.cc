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
#include "exprs/string-literal.h"
#include "gen-cpp/Exprs_types.h"
#include "gen-cpp/ImpalaService_types.h"
#include "runtime/raw-value.h"

using namespace std;
using namespace impala;

Expr::Expr(const TExprNode& node)
    : is_slotref_(false),
      type_(ThriftToType(node.type)) {
}

Expr::Expr(const TExprNode& node, bool is_slotref)
    : is_slotref_(is_slotref),
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
      *expr = pool->Add(new AggExpr(texpr_node));
      return Status::OK;
    }
    case TExprNodeType::ARITHMETIC_EXPR: {
      if (!texpr_node.__isset.op) {
        return Status("Arithmetic expression not set in thrift node");
      }
      *expr = pool->Add(new ArithmeticExpr(texpr_node));
      return Status::OK;
    }
    case TExprNodeType::BINARY_PRED: {
      if (!texpr_node.__isset.op) {
        return Status("Binary predicate not set in thrift node");
      }
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
      if (!texpr_node.__isset.op) {
        return Status("Like predicate not set in thrift node");
      }
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
      col_val->intVal = *reinterpret_cast<char*>(value);
      col_val->__isset.intVal = true;
      break;
    case TYPE_SMALLINT:
      col_val->intVal = *reinterpret_cast<short*>(value);
      col_val->__isset.intVal = true;
      break;
    case TYPE_INT:
      col_val->intVal = *reinterpret_cast<int*>(value);
      col_val->__isset.intVal = true;
      break;
    case TYPE_BIGINT:
      col_val->longVal = *reinterpret_cast<long*>(value);
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

void Expr::Prepare(RuntimeState* state) {
  for (int i = 0; i < children_.size(); ++i) {
    children_[i]->Prepare(state);
  }
}
