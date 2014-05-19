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

namespace cpp impala
namespace java com.cloudera.impala.thrift

include "Types.thrift"

enum TExprNodeType {
  ARITHMETIC_EXPR,
  BINARY_PRED,
  BOOL_LITERAL,
  CASE_EXPR,
  CAST_EXPR,
  COMPOUND_PRED,
  DATE_LITERAL,
  FLOAT_LITERAL,
  INT_LITERAL,
  IN_PRED,
  IS_NULL_PRED,
  LIKE_PRED,
  LITERAL_PRED,
  NULL_LITERAL,
  SLOT_REF,
  STRING_LITERAL,
  TUPLE_IS_NULL_PRED,
  FUNCTION_CALL,
  AGGREGATE_EXPR,

  // TODO: old style compute functions. this will be deprecated
  COMPUTE_FUNCTION_CALL,
}

struct TBoolLiteral {
  1: required bool value
}

struct TCaseExpr {
  1: required bool has_case_expr
  2: required bool has_else_expr
}

struct TDateLiteral {
  1: required Types.TTimestamp value
}

struct TFloatLiteral {
  1: required double value
}

struct TIntLiteral {
  1: required i64 value
}

struct TInPredicate {
  1: required bool is_not_in
}

struct TIsNullPredicate {
  1: required bool is_not_null
}

struct TLikePredicate {
  1: required string escape_char;
}

struct TLiteralPredicate {
  1: required bool value
  2: required bool is_null
}

struct TTupleIsNullPredicate {
  1: required list<Types.TTupleId> tuple_ids
}

struct TSlotRef {
  1: required Types.TSlotId slot_id
}

struct TStringLiteral {
  1: required string value;
}

// This is essentially a union over the subclasses of Expr.
struct TExprNode {
  1: required TExprNodeType node_type
  2: required Types.TColumnType type
  3: required i32 num_children

  // The function to execute. Not set for SlotRefs and Literals.
  4: optional Types.TFunction fn
  // If set, child[vararg_start_idx] is the first vararg child.
  5: optional i32 vararg_start_idx

  6: optional TBoolLiteral bool_literal
  7: optional TCaseExpr case_expr
  8: optional TDateLiteral date_literal
  9: optional TFloatLiteral float_literal
  10: optional TIntLiteral int_literal
  11: optional TInPredicate in_predicate
  12: optional TIsNullPredicate is_null_pred
  13: optional TLikePredicate like_pred
  14: optional TLiteralPredicate literal_pred
  15: optional TSlotRef slot_ref
  16: optional TStringLiteral string_literal
  17: optional TTupleIsNullPredicate tuple_is_null_pred
}

// A flattened representation of a tree of Expr nodes, obtained by depth-first
// traversal.
struct TExpr {
  1: required list<TExprNode> nodes
}

// A list of TExprs
struct TExprBatch {
  1: required list<TExpr> exprs
}

