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

namespace cpp impala
namespace java org.apache.impala.thrift

include "Types.thrift"

enum TExprNodeType {
  NULL_LITERAL,
  BOOL_LITERAL,
  INT_LITERAL,
  FLOAT_LITERAL,
  STRING_LITERAL,
  DECIMAL_LITERAL,
  TIMESTAMP_LITERAL,
  CASE_EXPR,
  COMPOUND_PRED,
  IN_PRED,
  IS_NULL_PRED,
  LIKE_PRED,
  SLOT_REF,
  TUPLE_IS_NULL_PRED,
  FUNCTION_CALL,
  AGGREGATE_EXPR,
  IS_NOT_EMPTY_PRED,
  KUDU_PARTITION_EXPR
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

struct TDecimalLiteral {
  // Value of the unscaled decimal in two's complement big endian
  // i.e. BigInteger.getBytes()
  1: required binary value
}

struct TFloatLiteral {
  1: required double value
}

struct TIntLiteral {
  1: required i64 value
}

struct TTimestampLiteral {
  // 16-byte raw representation of a TimestampValue
  1: required binary value
}

// The units which can be used when extracting a Timestamp. TExtractField is never used
// in any messages. This enum is here to provide a single definition that can be shared
// by the front and backend.
enum TExtractField {
  INVALID_FIELD,
  YEAR,
  QUARTER,
  MONTH,
  DAY,
  HOUR,
  MINUTE,
  SECOND,
  MILLISECOND,
  EPOCH
}

struct TInPredicate {
  1: required bool is_not_in
}

struct TIsNullPredicate {
  1: required bool is_not_null
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

// Additional information for aggregate functions.
struct TAggregateExpr {
  // Indicates whether this expr is the merge() of an aggregation.
  1: required bool is_merge_agg

  // The types of the input arguments to the aggregate function. May differ from the
  // input expr types if this is the merge() of an aggregation.
  2: required list<Types.TColumnType> arg_types;
}

// Expr used to call into the Kudu client to determine the partition index for rows. The
// values for the partition columns are produced by its children.
struct TKuduPartitionExpr {
  // The Kudu table to use the partitioning scheme from.
  1: required Types.TTableId target_table_id

  // Mapping from the children of this expr to their column positions in the table, i.e.
  // child(i) produces the value for column referenced_columns[i].
  // TODO: Include the partition cols in the KuduTableDesciptor and remove this.
  2: required list<i32> referenced_columns
}

// This is essentially a union over the subclasses of Expr.
struct TExprNode {
  1: required TExprNodeType node_type
  2: required Types.TColumnType type
  3: required i32 num_children

  // Whether the Expr is constant according to the frontend.
  4: required bool is_constant

  // The function to execute. Not set for SlotRefs and Literals.
  5: optional Types.TFunction fn

  // If set, child[vararg_start_idx] is the first vararg child.
  6: optional i32 vararg_start_idx

  7: optional TBoolLiteral bool_literal
  8: optional TCaseExpr case_expr
  9: optional TDateLiteral date_literal
  10: optional TFloatLiteral float_literal
  11: optional TIntLiteral int_literal
  12: optional TInPredicate in_predicate
  13: optional TIsNullPredicate is_null_pred
  14: optional TLiteralPredicate literal_pred
  15: optional TSlotRef slot_ref
  16: optional TStringLiteral string_literal
  17: optional TTupleIsNullPredicate tuple_is_null_pred
  18: optional TDecimalLiteral decimal_literal
  19: optional TAggregateExpr agg_expr
  20: optional TTimestampLiteral timestamp_literal
  21: optional TKuduPartitionExpr kudu_partition_expr
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

