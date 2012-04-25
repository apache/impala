// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

namespace cpp impala
namespace java com.cloudera.impala.thrift

include "Exprs.thrift"
include "Types.thrift"

enum TPlanNodeType {  
  HDFS_TEXT_SCAN_NODE,
  HDFS_RCFILE_SCAN_NODE,
  HBASE_SCAN_NODE,
  HASH_JOIN_NODE,
  AGGREGATION_NODE,
  SORT_NODE,
  EXCHANGE_NODE
}

// The information contained in subclasses of ScanNode captured in two separate
// Thrift structs:
// - TScanRange: the data range that's covered by the scan (which varies with the
//   particular partition of the plan fragment of which the scan node is a part)
// - T<subclass>: all other operational parameters that are the same across
//   all plan fragments

// Specification of subsection of a single hdfs file.
// TODO: also specify file format, to support multi-format table partitions
struct THdfsFileSplit {
  // file path
  1: required string path

  // starting offset
  2: required i64 offset

  // length of split
  3: required i64 length
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

// Specification of data range for a particular scan node
struct TScanRange {
  // id of scan node
  1: required Types.TPlanNodeId nodeId

  // THdfsScanNode: range consists of a collection of splits
  2: optional list<THdfsFileSplit> hdfsFileSplits

  // THBaseScanNode
  3: optional list<THBaseKeyRange> hbaseKeyRanges
}

struct THdfsScanNode {
  1: required Types.TTupleId tuple_id
  // Regex to be evaluated over filepath to extract partition key values
  2: required string partition_key_regex
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
}

struct TSortNode {
  1: required list<Exprs.TExpr> ordering_exprs
  2: required list<bool> is_asc_order
  // Indicates whether the backend service should use topn vs. sorting
  3: required bool use_top_n;
}

struct TExchangeNode {
  1: required i32 num_senders
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

  // one field per PlanNode subclass
  8: optional THdfsScanNode hdfs_scan_node
  9: optional THBaseScanNode hbase_scan_node
  10: optional THashJoinNode hash_join_node
  11: optional TAggregationNode agg_node
  12: optional TSortNode sort_node
  13: optional TExchangeNode exchange_node
}

// A flattened representation of a tree of PlanNodes, obtained by depth-first
// traversal.
struct TPlan {
  1: required list<TPlanNode> nodes
}
