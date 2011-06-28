// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

namespace cpp impala_thrift
namespace java com.cloudera.impala.thrift

include "Descriptors.thrift"
include "Types.thrift"

enum TExprNodeType {
  AGG_EXPR,
  ARITHMETIC_EXPR,
  BINARY_PRED,
  BOOL_LITERAL,
  CASE_EXPR,
  CAST_EXPR,
  COMPOUND_PRED,
  DATE_LITERAL,
  FLOAT_LITERAL,
  FUNCTION_CALL,
  INT_LITERAL,
  IS_NULL_PRED,
  LIKE_PRED,
  LITERAL_PRED,
  SLOT_REF,
  STRING_LITERAL,
}

// op-codes for all expr operators
enum TExprOperator {
  INVALID_OP,

  // AggregateExpr
  AGG_COUNT,
  AGG_MIN,
  AGG_MAX,
  AGG_SUM,
  // AGG_AVG is not executable

  // ArithmeticExpr
  MULTIPLY,
  DIVIDE,
  MOD,
  INT_DIVIDE,
  PLUS,
  MINUS,
  BITAND,
  BITOR,
  BITXOR,
  BITNOT,

  // BinaryPredicate
  EQ,
  NE,
  LE,
  GE,
  LT,
  GT,

  // CompoundPredicate
  AND,
  OR,
  NOT,

  // LIKE predicate
  LIKE,
  RLIKE,
  REGEXP,

  // function opcodes

}

struct TAggregateExpr {
  1: required bool is_star
  2: required bool is_distinct
}

struct TBoolLiteral {
  1: required bool value
}

struct TCaseExpr {
  1: required bool has_case_expr
  2: required bool has_else_expr
}

// do we need something for cast exprs? they don't even have opcodes

struct TDateLiteral {
  1: required Types.TTimestamp value
}

struct TFloatLiteral {
  1: required double value
}

struct TIntLiteral {
  1: required i64 value
}

struct TIsNullPredicate {
  1: required bool is_not_null
}

struct TLiteralPredicate {
  1: required bool value
}

struct TSlotRef {
  1: required Descriptors.TSlotId slot_id
}

struct TStringLiteral {
  1: required string value;
}

// This is essentially a union over the subclasses of Expr.
struct TExprNode {
  1: required TExprNodeType node_type
  2: required Types.TPrimitiveType type
  3: optional TExprOperator op
  4: required i32 num_children

  5: optional TAggregateExpr agg_expr
  6: optional TBoolLiteral bool_literal
  7: optional TCaseExpr case_expr
  8: optional TDateLiteral date_literal
  9: optional TFloatLiteral float_literal
  10: optional TIntLiteral int_literal
  11: optional TIsNullPredicate is_null_pred
  12: optional TLiteralPredicate literal_pred
  13: optional TSlotRef slot_ref
  14: optional TStringLiteral string_literal
}

// A flattened representation of a tree of Expr nodes, obtained by depth-first
// traversal.
struct TExpr {
  1: required list<TExprNode> nodes
}


