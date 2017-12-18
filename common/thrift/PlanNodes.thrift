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

//
// This file contains all structs, enums, etc., that together make up
// a plan tree. All information recorded in struct TPlan and below is independent
// of the execution parameters of any one of the backends on which it is running
// (those are recorded in TPlanFragmentInstanceCtx).

namespace cpp impala
namespace java org.apache.impala.thrift

include "CatalogObjects.thrift"
include "ExecStats.thrift"
include "Exprs.thrift"
include "Types.thrift"
include "ExternalDataSource.thrift"

enum TPlanNodeType {
  HDFS_SCAN_NODE,
  HBASE_SCAN_NODE,
  HASH_JOIN_NODE,
  AGGREGATION_NODE,
  SORT_NODE,
  EMPTY_SET_NODE,
  EXCHANGE_NODE,
  UNION_NODE,
  SELECT_NODE,
  NESTED_LOOP_JOIN_NODE,
  DATA_SOURCE_NODE,
  ANALYTIC_EVAL_NODE,
  SINGULAR_ROW_SRC_NODE,
  UNNEST_NODE,
  SUBPLAN_NODE,
  KUDU_SCAN_NODE
}

// phases of an execution node
// must be kept in sync with tests/failure/test_failpoints.py
enum TExecNodePhase {
  PREPARE,
  PREPARE_SCANNER,
  OPEN,
  GETNEXT,
  GETNEXT_SCANNER,
  CLOSE,
  INVALID
}

// what to do when hitting a debug point (TImpalaQueryOptions.DEBUG_ACTION)
enum TDebugAction {
  WAIT,
  FAIL,
  INJECT_ERROR_LOG,
  MEM_LIMIT_EXCEEDED,
  // A floating point number in range [0.0, 1.0] that gives the probability of denying
  // each reservation increase request after the initial reservation.
  SET_DENY_RESERVATION_PROBABILITY,
}

// Preference for replica selection
enum TReplicaPreference {
  CACHE_LOCAL,
  CACHE_RACK,
  DISK_LOCAL,
  DISK_RACK,
  REMOTE
}

// Specification of a runtime filter target.
struct TRuntimeFilterTargetDesc {
  // Target node id
  1: Types.TPlanNodeId node_id

  // Expr on which the filter is applied
  2: required Exprs.TExpr target_expr

  // Indicates if 'target_expr' is bound only by partition columns
  3: required bool is_bound_by_partition_columns

  // Slot ids on which 'target_expr' is bound on
  4: required list<Types.TSlotId> target_expr_slotids

  // Indicates if this target is on the same fragment as the join that
  // produced the runtime filter
  5: required bool is_local_target

  // If the target node is a Kudu scan node, the name, in the case it appears in Kudu, and
  // type of the targeted column.
  6: optional string kudu_col_name
  7: optional Types.TColumnType kudu_col_type;
}

enum TRuntimeFilterType {
  BLOOM,
  MIN_MAX
}

// Specification of a runtime filter.
struct TRuntimeFilterDesc {
  // Filter unique id (within a query)
  1: required i32 filter_id

  // Expr on which the filter is built on a hash join.
  2: required Exprs.TExpr src_expr

  // List of targets for this runtime filter
  3: required list<TRuntimeFilterTargetDesc> targets

  // Map of target node id to the corresponding index in 'targets'
  4: required map<Types.TPlanNodeId, i32> planid_to_target_ndx

  // Indicates if the source join node of this filter is a broadcast or
  // a partitioned join.
  5: required bool is_broadcast_join

  // Indicates if there is at least one target scan node that is in the
  // same fragment as the broadcast join that produced the runtime filter
  6: required bool has_local_targets

  // Indicates if there is at least one target scan node that is not in the same
  // fragment as the broadcast join that produced the runtime filter
  7: required bool has_remote_targets

  // Indicates if this filter is applied only on partition columns
  8: required bool applied_on_partition_columns

  // The estimated number of distinct values that the planner expects the filter to hold.
  // Used to compute the size of the filter.
  9: optional i64 ndv_estimate

  // The type of runtime filter to build.
  10: required TRuntimeFilterType type

  // The size of the filter based on the ndv estimate and the min/max limit specified in
  // the query options. Should be greater than zero for bloom filters, zero otherwise.
  11: optional i64 filter_size_bytes
}

// The information contained in subclasses of ScanNode captured in two separate
// Thrift structs:
// - TScanRange: the data range that's covered by the scan (which varies with the
//   particular partition of the plan fragment of which the scan node is a part)
// - T<subclass>: all other operational parameters that are the same across
//   all plan fragments

// Specification of subsection of a single hdfs file.
struct THdfsFileSplit {
  // File name (not the full path).  The path is assumed to be the
  // 'location' of the THdfsPartition referenced by partition_id.
  1: required string file_name

  // starting offset
  2: required i64 offset

  // length of split
  3: required i64 length

  // ID of partition within the THdfsTable associated with this scan node.
  4: required i64 partition_id

  // total size of the hdfs file
  5: required i64 file_length

  // compression type of the hdfs file
  6: required CatalogObjects.THdfsCompression file_compression

  // last modified time of the file
  7: required i64 mtime
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
  // one of these must be set for every TScanRange
  1: optional THdfsFileSplit hdfs_file_split
  2: optional THBaseKeyRange hbase_key_range
  3: optional binary kudu_scan_token
}

struct THdfsScanNode {
  1: required Types.TTupleId tuple_id

  // Conjuncts that can be evaluated while materializing the items (tuples) of
  // collection-typed slots. Maps from item tuple id to the list of conjuncts
  // to be evaluated.
  2: optional map<Types.TTupleId, list<Exprs.TExpr>> collection_conjuncts

  // Option to control replica selection during scan scheduling.
  3: optional TReplicaPreference replica_preference

  // Option to control tie breaks during scan scheduling.
  4: optional bool random_replica

  // Number of header lines to skip at the beginning of each file of this table. Only set
  // for hdfs text files.
  5: optional i32 skip_header_line_count

  // Indicates whether the MT scan node implementation should be used.
  // If this is true then the MT_DOP query option must be > 0.
  // TODO: Remove this option when the MT scan node supports all file formats.
  6: optional bool use_mt_scan_node

  // Conjuncts that can be evaluated against parquet::Statistics using the tuple
  // referenced by 'min_max_tuple_id'.
  7: optional list<Exprs.TExpr> min_max_conjuncts

  // Tuple to evaluate 'min_max_conjuncts' against.
  8: optional Types.TTupleId min_max_tuple_id

  // The conjuncts that are eligible for dictionary filtering.
  9: optional map<Types.TSlotId, list<i32>> dictionary_filter_conjuncts

  // The byte offset of the slot for Parquet metadata if Parquet count star optimization
  // is enabled.
  10: optional i32 parquet_count_star_slot_offset
}

struct TDataSourceScanNode {
  1: required Types.TTupleId tuple_id

  // The external data source to scan
  2: required CatalogObjects.TDataSource data_source

  // Init string for the table passed to the data source. May be an empty string.
  3: required string init_string

  // Scan predicates in conjunctive normal form that were accepted by the data source.
  4: required list<list<ExternalDataSource.TBinaryPredicate>> accepted_predicates
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

  // Suggested max value for "hbase.client.scan.setCaching"
  4: optional i32 suggested_max_caching
}

struct TKuduScanNode {
  1: required Types.TTupleId tuple_id

  // Indicates whether the MT scan node implementation should be used.
  // If this is true, then the MT_DOP query option must be > 0.
  2: optional bool use_mt_scan_node
}

struct TEqJoinCondition {
  // left-hand side of "<a> = <b>"
  1: required Exprs.TExpr left;
  // right-hand side of "<a> = <b>"
  2: required Exprs.TExpr right;
  // true if and only if operator is "<=>", also known as "IS NOT DISTINCT FROM"
  3: required bool is_not_distinct_from;
}

enum TJoinOp {
  INNER_JOIN,
  LEFT_OUTER_JOIN,
  LEFT_SEMI_JOIN,
  LEFT_ANTI_JOIN,

  // Similar to LEFT_ANTI_JOIN with special handling for NULLs for the join conjuncts
  // on the build side. Those NULLs are considered candidate matches, and therefore could
  // be rejected (ANTI-join), based on the other join conjuncts. This is in contrast
  // to LEFT_ANTI_JOIN where NULLs are not matches and therefore always returned.
  NULL_AWARE_LEFT_ANTI_JOIN,

  RIGHT_OUTER_JOIN,
  RIGHT_SEMI_JOIN,
  RIGHT_ANTI_JOIN,
  FULL_OUTER_JOIN,
  CROSS_JOIN
}

struct THashJoinNode {
  1: required TJoinOp join_op

  // equi-join predicates
  2: required list<TEqJoinCondition> eq_join_conjuncts

  // non equi-join predicates
  3: optional list<Exprs.TExpr> other_join_conjuncts
}

struct TNestedLoopJoinNode {
  1: required TJoinOp join_op

  // Join conjuncts (both equi-join and non equi-join). All other conjuncts that are
  // evaluated at the join node are stored in TPlanNode.conjuncts.
  2: optional list<Exprs.TExpr> join_conjuncts
}

struct TAggregationNode {
  1: optional list<Exprs.TExpr> grouping_exprs
  // aggregate exprs. The root of each expr is the aggregate function. The
  // other exprs are the inputs to the aggregate function.
  2: required list<Exprs.TExpr> aggregate_functions

  // Tuple id used for intermediate aggregations (with slots of agg intermediate types)
  3: required Types.TTupleId intermediate_tuple_id

  // Tupld id used for the aggregation output (with slots of agg output types)
  // Equal to intermediate_tuple_id if intermediate type == output type for all
  // aggregate functions.
  4: required Types.TTupleId output_tuple_id

  // Set to true if this aggregation node needs to run the finalization step.
  5: required bool need_finalize

  // Set to true to use the streaming preagg algorithm. Node must be a preaggregation.
  6: required bool use_streaming_preaggregation

  // Estimate of number of input rows from the planner.
  7: required i64 estimated_input_cardinality
}

struct TSortInfo {
  1: required list<Exprs.TExpr> ordering_exprs
  2: required list<bool> is_asc_order
  // Indicates, for each expr, if nulls should be listed first or last. This is
  // independent of is_asc_order.
  3: required list<bool> nulls_first
  // Expressions evaluated over the input row that materialize the tuple to be sorted.
  // Contains one expr per slot in the materialized tuple.
  4: optional list<Exprs.TExpr> sort_tuple_slot_exprs
}

enum TSortType {
  // Sort the entire input.
  TOTAL,

  // Return the first N sorted elements.
  TOPN,

  // Divide the input into batches, each of which is sorted individually.
  PARTIAL
}

struct TSortNode {
  1: required TSortInfo sort_info
  2: required TSortType type
  // This is the number of rows to skip before returning results.
  // Not used with TSortType::PARTIAL.
  3: optional i64 offset
}

enum TAnalyticWindowType {
  // Specifies the window as a logical offset
  RANGE,

  // Specifies the window in physical units
  ROWS
}

enum TAnalyticWindowBoundaryType {
  // The window starts/ends at the current row.
  CURRENT_ROW,

  // The window starts/ends at an offset preceding current row.
  PRECEDING,

  // The window starts/ends at an offset following current row.
  FOLLOWING
}

struct TAnalyticWindowBoundary {
  1: required TAnalyticWindowBoundaryType type

  // Predicate that checks: child tuple '<=' buffered tuple + offset for the orderby expr
  2: optional Exprs.TExpr range_offset_predicate

  // Offset from the current row for ROWS windows.
  3: optional i64 rows_offset_value
}

struct TAnalyticWindow {
  // Specifies the window type for the start and end bounds.
  1: required TAnalyticWindowType type

  // Absence indicates window start is UNBOUNDED PRECEDING.
  2: optional TAnalyticWindowBoundary window_start

  // Absence indicates window end is UNBOUNDED FOLLOWING.
  3: optional TAnalyticWindowBoundary window_end
}

// Defines a group of one or more analytic functions that share the same window,
// partitioning expressions and order-by expressions and are evaluated by a single
// ExecNode.
struct TAnalyticNode {
  // Exprs on which the analytic function input is partitioned. Input is already sorted
  // on partitions and order by clauses, partition_exprs is used to identify partition
  // boundaries. Empty if no partition clause is specified.
  1: required list<Exprs.TExpr> partition_exprs

  // Exprs specified by an order-by clause for RANGE windows. Used to evaluate RANGE
  // window boundaries. Empty if no order-by clause is specified or for windows
  // specifying ROWS.
  2: required list<Exprs.TExpr> order_by_exprs

  // Functions evaluated over the window for each input row. The root of each expr is
  // the aggregate function. Child exprs are the inputs to the function.
  3: required list<Exprs.TExpr> analytic_functions

  // Window specification
  4: optional TAnalyticWindow window

  // Tuple used for intermediate results of analytic function evaluations
  // (with slots of analytic intermediate types)
  5: required Types.TTupleId intermediate_tuple_id

  // Tupld used for the analytic function output (with slots of analytic output types)
  // Equal to intermediate_tuple_id if intermediate type == output type for all
  // analytic functions.
  6: required Types.TTupleId output_tuple_id

  // id of the buffered tuple (identical to the input tuple, which is assumed
  // to come from a single SortNode); not set if both partition_exprs and
  // order_by_exprs are empty
  7: optional Types.TTupleId buffered_tuple_id

  // predicate that checks: child tuple is in the same partition as the buffered tuple,
  // i.e. each partition expr is equal or both are not null. Only set if
  // buffered_tuple_id is set; should be evaluated over a row that is composed of the
  // child tuple and the buffered tuple
  8: optional Exprs.TExpr partition_by_eq

  // predicate that checks: the order_by_exprs are equal or both NULL when evaluated
  // over the child tuple and the buffered tuple. only set if buffered_tuple_id is set;
  // should be evaluated over a row that is composed of the child tuple and the buffered
  // tuple
  9: optional Exprs.TExpr order_by_eq
}

struct TUnionNode {
  // A UnionNode materializes all const/result exprs into this tuple.
  1: required Types.TTupleId tuple_id
  // List or expr lists materialized by this node.
  // There is one list of exprs per query stmt feeding into this union node.
  2: required list<list<Exprs.TExpr>> result_expr_lists
  // Separate list of expr lists coming from a constant select stmts.
  3: required list<list<Exprs.TExpr>> const_expr_lists
  // Index of the first child that needs to be materialized.
  4: required i64 first_materialized_child_idx
}

struct TExchangeNode {
  // The ExchangeNode's input rows form a prefix of the output rows it produces;
  // this describes the composition of that prefix
  1: required list<Types.TTupleId> input_row_tuples
  // For a merging exchange, the sort information.
  2: optional TSortInfo sort_info
  // This is the number of rows to skip before returning results
  3: optional i64 offset
}

struct TUnnestNode {
  // Expr that returns the in-memory collection to be scanned.
  // Currently always a SlotRef into an array-typed slot.
  1: required Exprs.TExpr collection_expr
}

// This contains all of the information computed by the plan as part of the resource
// profile that is needed by the backend to execute.
struct TBackendResourceProfile {
  // The minimum reservation for this plan node in bytes.
  1: required i64 min_reservation

  // The maximum reservation for this plan node in bytes. MAX_INT64 means effectively
  // unlimited.
  2: required i64 max_reservation

  // The spillable buffer size in bytes to use for this node, chosen by the planner.
  // Set iff the node uses spillable buffers.
  3: optional i64 spillable_buffer_size

  // The buffer size in bytes that is large enough to fit the largest row to be processed.
  // Set if the node allocates buffers for rows from the buffer pool.
  4: optional i64 max_row_buffer_size
}

// This is essentially a union of all messages corresponding to subclasses
// of PlanNode.
struct TPlanNode {
  // node id, needed to reassemble tree structure
  1: required Types.TPlanNodeId node_id
  2: required TPlanNodeType node_type
  3: required i32 num_children
  4: required i64 limit
  // Tuples in row produced by node. Must be non-empty.
  5: required list<Types.TTupleId> row_tuples

  // nullable_tuples[i] is true if row_tuples[i] is nullable
  6: required list<bool> nullable_tuples
  7: optional list<Exprs.TExpr> conjuncts

  // Set to true if codegen should be disabled for this plan node. Otherwise the plan
  // node is codegen'd if the backend supports it.
  8: required bool disable_codegen

  // one field per PlanNode subclass
  9: optional THdfsScanNode hdfs_scan_node
  10: optional THBaseScanNode hbase_scan_node
  11: optional TKuduScanNode kudu_scan_node
  12: optional TDataSourceScanNode data_source_node
  13: optional THashJoinNode hash_join_node
  14: optional TNestedLoopJoinNode nested_loop_join_node
  15: optional TAggregationNode agg_node
  16: optional TSortNode sort_node
  17: optional TUnionNode union_node
  18: optional TExchangeNode exchange_node
  19: optional TAnalyticNode analytic_node
  20: optional TUnnestNode unnest_node

  // Label that should be used to print this node to the user.
  21: optional string label

  // Additional details that should be printed to the user. This is node specific
  // e.g. table name, join strategy, etc.
  22: optional string label_detail

  // Estimated execution stats generated by the planner.
  23: optional ExecStats.TExecStats estimated_stats

  // Runtime filters assigned to this plan node
  24: optional list<TRuntimeFilterDesc> runtime_filters

  // Resource profile for this plan node.
  25: required TBackendResourceProfile resource_profile
}

// A flattened representation of a tree of PlanNodes, obtained by depth-first
// traversal.
struct TPlan {
  1: required list<TPlanNode> nodes
}
