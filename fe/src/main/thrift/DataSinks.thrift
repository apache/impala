// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

namespace cpp impala
namespace java com.cloudera.impala.thrift

include "Exprs.thrift"
include "Types.thrift"
include "Descriptors.thrift"

enum TDataSinkType {
  STREAM_DATA_SINK,
  TABLE_SINK
}

// Specification of how plan fragment output is partitioned. If it is hash-partitioned,
// partitionBoundaries is empty; instead, the hash value % n is used to create n partitions,
// where n == the number of destination hosts.                                                 
struct TOutputPartitionSpec {
  // if true, partition based on hash value of partitionExpr
  1: required bool isHashPartitioned
                                                                       
  // partitionExpr computes partitioning value over output tuples
  2: optional Exprs.TExpr partitionExpr

  // n-1 boundary literal exprs for n partitions
  3: list<Exprs.TExpr> partitionBoundaries
}

// Sink which forwards data to a remote plan fragment,
// according to the given output partition specification.
struct TStreamDataSink {
  // Specification of how the output is partitioned, which in conjunction with
  // TPlanExecParams.destHosts determines where each output row is sent.
  // An empty output partition specification in combination with multiple
  // destination nodes means that the output is broadcast to those nodes.
  // If the partition specification is non-empty, TPlanExecParams.destHosts's
  // size must be the same as the number of partitions.
  1: optional TOutputPartitionSpec outputPartitionSpec

  // destination node id
  2: required Types.TPlanNodeId destNodeId
}

// Creates a new Hdfs file at given path, and materializes
// all its input RowBatches as delimited text.
struct THdfsTextTableSink {
  1: required list<Exprs.TExpr> partitionKeyExprs
  2: required bool overwrite
}

// Union type of all table sinks.
// Currently, only THdfsTextTableSink is supported, so we don't have a separate
// TTableSinkType yet.
struct TTableSink {
  1: required Types.TTableId target_table_id
  2: required THdfsTextTableSink hdfs_text_table_sink
}

// This is essentially a union of all messages corresponding to subclasses
// of DataSink.
struct TDataSink {
  1: required TDataSinkType datasink_type 
  2: optional TStreamDataSink stream_data_sink
  3: optional TTableSink table_sink
}
