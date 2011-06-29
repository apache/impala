// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "expr.h"
#include "agg-expr.h"
#include "arithmetic-expr.h"
#include "binary-predicate.h"
#include "bool-literal.h"
#include "case-expr.h"
#include "cast-expr.h"
#include "compound-predicate.h"
#include "date-literal.h"
#include "float-literal.h"
#include "function-call.h"
#include "int-literal.h"
#include "is-null-predicate.h"
#include "like-predicate.h"
#include "literal-predicate.h"
#include "string-literal.h"
#include "common/status.h"
#include "gen-cpp/Exprs_types.h"

using namespace std;
using namespace impala;

Expr::Expr(const TExprNode& node)
    : is_slotref_(false),
      type_(static_cast<PrimitiveType> (node.type)) {
}

Expr::Expr(const TExprNode& node, bool is_slotref)
    : is_slotref_(is_slotref), type_(static_cast<PrimitiveType> (node.type)) {
}

Status Expr::CreateExprTree(const TExpr& texpr, Expr** root_expr) {
  // input is empty
  if (texpr.nodes.size() == 0) {
    *root_expr = NULL;
    return Status::OK;
  }
  int node_idx = 0;
  RETURN_IF_ERROR(CreateTreeFromThrift(texpr.nodes, &node_idx, NULL, root_expr));
  if (node_idx + 1 != texpr.nodes.size()) {
    Expr::DeleteExprTree(*root_expr);
    return Status("Expression tree only partially reconstructed. Not all thrift nodes were used.");
  }
  return Status::OK;
}

Status Expr::CreateTreeFromThrift(const vector<TExprNode>& nodes,
    int* node_idx, Expr* parent, Expr** root_expr) {
  // propagate error case
  if (*node_idx >= nodes.size()) {
    return Status("Failed to reconstruct expression tree from thrift.");
  }
  int num_children = nodes[*node_idx].num_children;
  Expr* expr = NULL;
  RETURN_IF_ERROR(CreateExpr(nodes[*node_idx], &expr));
  if (parent != NULL) {
    parent->AddChild(expr);
  } else {
    *root_expr = expr;
  }
  for (int i = 0; i < num_children; i++) {
    *node_idx += 1;
    RETURN_IF_ERROR(CreateTreeFromThrift(nodes, node_idx, expr, root_expr));
    // we are expecting a child, but have used all nodes
    // this means we have been given a bad tree and must fail
    if (*node_idx >= nodes.size()) {
      return Status("Failed to reconstruct expression tree from thrift.");
    }
  }
  return Status::OK;
}

Status Expr::CreateExpr(const TExprNode& texpr_node, Expr** expr) {
  switch (texpr_node.node_type) {
    case TExprNodeType::AGG_EXPR: {
      if (!texpr_node.__isset.agg_expr) {
        return Status("Aggregation expression not set in thrift node");
      }
      *expr = new AggExpr(texpr_node);
      return Status::OK;
    }
    case TExprNodeType::ARITHMETIC_EXPR: {
      if (!texpr_node.__isset.op) {
        return Status("Arithmetic expression not set in thrift node");
      }
      *expr = new ArithmeticExpr(texpr_node);
      return Status::OK;
    }
    case TExprNodeType::BINARY_PRED: {
      if (!texpr_node.__isset.op) {
        return Status("Binary predicate not set in thrift node");
      }
      *expr = new BinaryPredicate(texpr_node);
      return Status::OK;
    }
    case TExprNodeType::BOOL_LITERAL: {
      if (!texpr_node.__isset.bool_literal) {
        return Status("Boolean literal not set in thrift node");
      }
      *expr = new BoolLiteral(texpr_node);
      return Status::OK;
    }
    case TExprNodeType::CASE_EXPR: {
      if (!texpr_node.__isset.case_expr) {
        return Status("Case expression not set in thrift node");
      }
      *expr = new BoolLiteral(texpr_node);
      return Status::OK;
    }
    case TExprNodeType::CAST_EXPR: {
      *expr = new CastExpr(texpr_node);
      return Status::OK;
    }
    case TExprNodeType::COMPOUND_PRED: {
      *expr = new CompoundPredicate(texpr_node);
      return Status::OK;
    }
    case TExprNodeType::DATE_LITERAL: {
      if (!texpr_node.__isset.date_literal) {
        return Status("Date literal not set in thrift node");
      }
      *expr = new DateLiteral(texpr_node);
      return Status::OK;
    }
    case TExprNodeType::FLOAT_LITERAL: {
      if (!texpr_node.__isset.float_literal) {
        return Status("Float literal not set in thrift node");
      }
      *expr = new FloatLiteral(texpr_node);
      return Status::OK;
    }
    case TExprNodeType::FUNCTION_CALL: {
      *expr = new FunctionCall(texpr_node);
      return Status::OK;
    }
    case TExprNodeType::INT_LITERAL: {
      if (!texpr_node.__isset.int_literal) {
        return Status("Int literal not set in thrift node");
      }
      *expr = new IntLiteral(texpr_node);
      return Status::OK;
    }
    case TExprNodeType::IS_NULL_PRED: {
      if (!texpr_node.__isset.is_null_pred) {
        return Status("Is null predicate not set in thrift node");
      }
      *expr = new IsNullPredicate(texpr_node);
      return Status::OK;
    }
    case TExprNodeType::LIKE_PRED: {
      if (!texpr_node.__isset.op) {
        return Status("Like predicate not set in thrift node");
      }
      *expr = new LikePredicate(texpr_node);
      return Status::OK;
    }
    case TExprNodeType::LITERAL_PRED: {
      if (!texpr_node.__isset.literal_pred) {
        return Status("Literal predicate not set in thrift node");
      }
      *expr = new LiteralPredicate(texpr_node);
      return Status::OK;
    }
    case TExprNodeType::SLOT_REF: {
      if (!texpr_node.__isset.slot_ref) {
        return Status("Slot reference not set in thrift node");
      }
      *expr = new SlotRef(texpr_node);
      return Status::OK;
    }
    case TExprNodeType::STRING_LITERAL: {
      if (texpr_node.__isset.string_literal) {
        return Status("String literal not set in thrift node");
      }
      *expr = new StringLiteral(texpr_node);
      return Status::OK;
    }
  }
}

void Expr::DeleteExprTree(Expr* expr) {
  int num_children = expr->children().size();
  for (int i = 0; i < num_children; i++) {
    DeleteExprTree(expr->children()[i]);
  }
  delete expr;
}
