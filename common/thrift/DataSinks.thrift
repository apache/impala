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

include "ExecStats.thrift"
include "Exprs.thrift"
include "Types.thrift"
include "Descriptors.thrift"
include "Partitions.thrift"
include "PlanNodes.thrift"
include "ResourceProfile.thrift"

enum TDataSinkType {
  DATA_STREAM_SINK = 0
  TABLE_SINK = 1
  HASH_JOIN_BUILDER = 2
  PLAN_ROOT_SINK = 3
  NESTED_LOOP_JOIN_BUILDER = 4
  ICEBERG_DELETE_BUILDER = 5
  MULTI_DATA_SINK = 6
}

enum TSinkAction {
  INSERT = 0
  UPDATE = 1
  UPSERT = 2
  DELETE = 3
}

enum TTableSinkType {
  HDFS = 0
  HBASE = 1
  KUDU = 2
}

// Sink which forwards data to a remote plan fragment,
// according to the given output partition specification
// (ie, the m:1 part of an m:n data stream)
struct TDataStreamSink {
  // destination node id
  1: required Types.TPlanNodeId dest_node_id

  // Specification of how the output of a fragment is partitioned.
  // If the partitioning type is UNPARTITIONED, the output is broadcast
  // to each destination host.
  2: required Partitions.TDataPartition output_partition
}

// Creates a new Hdfs files according to the evaluation of the partitionKeyExprs,
// and materializes all its input RowBatches as a Hdfs file.
struct THdfsTableSink {
  1: required list<Exprs.TExpr> partition_key_exprs

  2: required bool overwrite

  // The 'skip.header.line.count' property of the target Hdfs table. We will insert this
  // many empty lines at the beginning of new text files, which will be skipped by the
  // scanners while reading from the files.
  3: optional i32 skip_header_line_count

  // This property indicates to the table sink whether the input is ordered by the
  // partition keys, meaning partitions can be opened, written, and closed one by one.
  4: required bool input_is_clustered

  // Stores the indices into the list of non-clustering columns of the target table that
  // are stored in the 'sort.columns' table property. This is used in the backend to
  // populate the RowGroup::sorting_columns list in parquet files.
  5: optional list<i32> sort_columns

  // Stores the allocated ACID write id if the target table is transactional.
  6: optional i64 write_id

  // Sorting order. If not lexical, the backend should not populate the
  // RowGroup::sorting_columns list in parquet files.
  7: required Types.TSortingOrder sorting_order

  // Indicates that this HdfsTableSink is writing query results
  8: optional bool is_result_sink = false;

  // Indicates that an external FE is expecting results here
  9: optional string external_output_dir;

  // Indicates how deep into the partition specification in which to start creating
  // partition directories
  10: optional i32 external_output_partition_depth;

  // Mapping from column names to Parquet Bloom filter bitset sizes. Columns for which no
  // Parquet Bloom filter should be written should not be listed here.
  11: optional map<string, i64> parquet_bloom_filter_col_info;
}

// Structure to encapsulate specific options that are passed down to the
// IcebergBufferedDeleteSink.
struct TIcebergDeleteSink {
  // Partition expressions of this sink. In case of Iceberg DELETEs these are the
  // partition spec id and the serialized partition data.
  1: required list<Exprs.TExpr> partition_key_exprs
}

// Structure to encapsulate specific options that are passed down to the KuduTableSink
struct TKuduTableSink {
  // The position in this vector is equal to the position in the output expressions of the
  // sink and holds the index of the corresponsding column in the Kudu schema,
  // e.g. 'exprs[i]' references 'kudu_table.column(referenced_cols[i])'
  1: optional list<i32> referenced_columns

  // Defines if duplicate or not found keys should be ignored
  2: optional bool ignore_not_found_or_duplicate

  // Serialized metadata of KuduTransaction object.
  3: optional binary kudu_txn_token
}

// Sink to create the build side of a JoinNode.
struct TJoinBuildSink {
  // destination join node id
  1: required Types.TPlanNodeId dest_node_id

  // Join operation implemented by the JoinNode
  2: required PlanNodes.TJoinOp join_op

  // Equi-join conjunctions. Only set for hash join builds.
  3: optional list<PlanNodes.TEqJoinCondition> eq_join_conjuncts

  // Runtime filters produced by this sink.
  4: optional list<PlanNodes.TRuntimeFilterDesc> runtime_filters

  // Hash seed to use. Only set for hash join builds. Must be the same as the join node's
  // hash seed. Must be positive.
  5: optional i32 hash_seed

  // If true, join build sharing is enabled and, if multiple instances of a join node are
  // scheduled on the same backend, they will share the join build on that backend.
  6: optional bool share_build
}

struct TPlanRootSink {
  1: required ResourceProfile.TBackendResourceProfile resource_profile
}

// Union type of all table sinks.
struct TTableSink {
  1: required Types.TTableId target_table_id
  2: required TTableSinkType type
  3: required TSinkAction action
  4: optional THdfsTableSink hdfs_table_sink
  5: optional TKuduTableSink kudu_table_sink
  6: optional TIcebergDeleteSink iceberg_delete_sink
}

struct TDataSink {
  // Tagged union of the different types of data sink.
  1: required TDataSinkType type
  2: optional TDataStreamSink stream_sink
  3: optional TTableSink table_sink
  4: optional TJoinBuildSink join_build_sink
  5: optional TPlanRootSink plan_root_sink

  // A human-readable label for the sink.
  6: optional string label

  // Estimated execution stats generated by the planner.
  7: optional ExecStats.TExecStats estimated_stats

  // Exprs that produce values for slots of output tuple (one expr per slot).
  // Only set by the DataSink implementations that require it.
  8: optional list<Exprs.TExpr> output_exprs

  // Resource profile for this data sink. Always set.
  9: optional ResourceProfile.TBackendResourceProfile resource_profile

  // Child data sinks if this is a MULTI_DATA_SINK.
  10: optional list<TDataSink> child_data_sinks
}
