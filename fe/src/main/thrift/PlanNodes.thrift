// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

namespace cpp impala
namespace java com.cloudera.impala.thrift

include "Descriptors.thrift"
include "Exprs.thrift"

enum TPlanNodeType {
  HDFS_SCAN_NODE,
  HBASE_SCAN_NODE,
  AGGREGATION_NODE,
  SORT_NODE,
}

struct THdfsScanNode {
  1: required Descriptors.TTupleId tuple_id
  2: required list<string> file_paths
  3: optional list<Exprs.TExpr> key_values
}

struct THBaseScanNode {
  1: required Descriptors.TTupleId tuple_id
  2: required string table_name
}

struct TAggregationNode {
  1: optional list<Exprs.TExpr> grouping_exprs
  2: required list<Exprs.TExpr> aggregate_exprs
  3: required Descriptors.TTupleId agg_tuple_id
}

struct TSortNode {
  1: required list<Exprs.TExpr> ordering_exprs
  2: required list<bool> is_asc_order
}

// This is essentially a union of all messages corresponding to subclasses
// of PlanNode.
struct TPlanNode {
  // node id, needed to reassemble tree structure
  1: required TPlanNodeType node_type
  2: required i32 num_children
  3: optional i64 limit = 0
  4: optional list<Exprs.TExpr> conjuncts

  // one field per PlanNode subclass
  5: optional THdfsScanNode hdfs_scan_node
  6: optional THBaseScanNode hbase_scan_node
  7: optional TAggregationNode agg_node
  8: optional TSortNode sort_node
}

// A flattened representation of a tree of PlanNodes, obtained by depth-first
// traversal.
struct TPlan {
  1: required list<TPlanNode> nodes
}
