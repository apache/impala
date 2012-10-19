// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

namespace cpp impala
namespace java com.cloudera.impala.thrift

include "Exprs.thrift"
include "Types.thrift"
include "Descriptors.thrift"
include "Partitions.thrift"

enum TDataSinkType {
  DATA_STREAM_SINK,
  TABLE_SINK
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
}

// Union type of all table sinks.
// Currently, only THdfsTableSink is supported, so we don't have a separate
// TTableSinkType yet.
struct TTableSink {
  1: required Types.TTableId target_table_id
  2: required THdfsTableSink hdfs_table_sink
}

struct TDataSink {
  1: required TDataSinkType type
  2: optional TDataStreamSink stream_sink
  3: optional TTableSink table_sink
}
