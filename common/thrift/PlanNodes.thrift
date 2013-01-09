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

//
// This file contains all structs, enums, etc., that together make up
// a plan tree. All information recorded in struct TPlan and below is independent
// of the execution parameters of any one of the backends on which it is running
// (those are recorded in TPlanFragmentExecParams).

namespace cpp impala
namespace java com.cloudera.impala.thrift

include "Exprs.thrift"
include "Types.thrift"

enum TPlanNodeType {  
  HDFS_SCAN_NODE,
  HBASE_SCAN_NODE,
  HASH_JOIN_NODE,
  AGGREGATION_NODE,
  SORT_NODE,
  EXCHANGE_NODE,
  MERGE_NODE
}

// phases of an execution node
enum TExecNodePhase {
  PREPARE,
  OPEN,
  GETNEXT,
  CLOSE,
  INVALID
}

// what to do when hitting a debug point (TImpalaQueryOptions.DEBUG_ACTION)
enum TDebugAction {
  WAIT,
  FAIL
}

// The information contained in subclasses of ScanNode captured in two separate
// Thrift structs:
// - TScanRange: the data range that's covered by the scan (which varies with the
//   particular partition of the plan fragment of which the scan node is a part)
// - T<subclass>: all other operational parameters that are the same across
//   all plan fragments

// Specification of subsection of a single hdfs file.
struct THdfsFileSplit {
  // file path
  1: required string path

  // starting offset
  2: required i64 offset

  // length of split
  3: required i64 length

  // ID of partition in parent THdfsScanNode. Meaningful only
  // in the context of a single THdfsScanNode, may not be unique elsewhere.
  4: required i64 partition_id
}

// key range for single THBaseScanNode
// TODO: does 'binary' have an advantage over string? strings can
// already store binary data
struct THBaseKeyRange {
  // inclusive
  1: optional string startKey

  // exclusive
  2: optional string stopKey
}

// Specification of an individual data range which is held in its entirety
// by a storage server
struct TScanRange {
  // one of these must be set for every TScanRange2
  1: optional THdfsFileSplit hdfs_file_split
  2: optional THBaseKeyRange hbase_key_range
}

struct THdfsScanNode {
  1: required Types.TTupleId tuple_id
}

struct THBaseFilter {
  1: required string family
  2: required string qualifier
  // Ordinal number into enum HBase CompareFilter.CompareOp.
  // We don't use TExprOperator because the op is interpreted by an HBase Filter, and
  // not the c++ expr eval.
  3: required i32 op_ordinal
  4: required string filter_constant
}

struct THBaseScanNode {
  1: required Types.TTupleId tuple_id

  // TODO: remove this, we already have THBaseTable.tableName
  2: required string table_name

  3: optional list<THBaseFilter> filters
}

struct TEqJoinCondition {
  // left-hand side of "<a> = <b>"
  1: required Exprs.TExpr left;
  // right-hand side of "<a> = <b>"
  2: required Exprs.TExpr right;
}

enum TJoinOp {
  INNER_JOIN,
  LEFT_OUTER_JOIN,
  LEFT_SEMI_JOIN,
  RIGHT_OUTER_JOIN,
  FULL_OUTER_JOIN
}

struct THashJoinNode {
  1: required TJoinOp join_op

  // anything from the ON, USING or WHERE clauses that's an equi-join predicate
  2: required list<TEqJoinCondition> eq_join_conjuncts

  // anything from the ON or USING clauses (but *not* the WHERE clause) that's not an
  // equi-join predicate
  3: optional list<Exprs.TExpr> other_join_conjuncts
}

struct TAggregationNode {
  1: optional list<Exprs.TExpr> grouping_exprs
  2: required list<Exprs.TExpr> aggregate_exprs
  3: required Types.TTupleId agg_tuple_id
  
  // Set to true if this aggregation function requires finalization to complete after all
  // rows have been aggregated, and this node is not an intermediate node.
  4: required bool need_finalize
}

struct TSortNode {
  1: required list<Exprs.TExpr> ordering_exprs
  2: required list<bool> is_asc_order
  // Indicates whether the backend service should use topn vs. sorting
  3: required bool use_top_n;
}

struct TMergeNode {
  // List or expr lists materialized by this node.
  // There is one list of exprs per query stmt feeding into this merge node.
  1: required list<list<Exprs.TExpr>> result_expr_lists
  // Separate list of expr lists coming from a constant select stmts.
  2: required list<list<Exprs.TExpr>> const_expr_lists
}

// This is essentially a union of all messages corresponding to subclasses
// of PlanNode.
struct TPlanNode {
  // node id, needed to reassemble tree structure
  1: required Types.TPlanNodeId node_id
  2: required TPlanNodeType node_type
  3: required i32 num_children
  4: required i64 limit
  5: required list<Types.TTupleId> row_tuples

  // nullable_tuples[i] is true if row_tuples[i] is nullable
  6: required list<bool> nullable_tuples
  7: optional list<Exprs.TExpr> conjuncts

  // Produce data in compact format.
  8: required bool compact_data

  // one field per PlanNode subclass
  9: optional THdfsScanNode hdfs_scan_node
  10: optional THBaseScanNode hbase_scan_node
  11: optional THashJoinNode hash_join_node
  12: optional TAggregationNode agg_node
  13: optional TSortNode sort_node
  14: optional TMergeNode merge_node
}

// A flattened representation of a tree of PlanNodes, obtained by depth-first
// traversal.
struct TPlan {
  1: required list<TPlanNode> nodes
}
