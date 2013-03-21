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

include "Exprs.thrift"
include "Types.thrift"
include "Descriptors.thrift"
include "Partitions.thrift"

enum TDataSinkType {
  DATA_STREAM_SINK,
  TABLE_SINK
}

enum TTableSinkType {
  HDFS,
  HBASE
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
struct TTableSink {
  1: required Types.TTableId  target_table_id
  2: required TTableSinkType type
  3: optional THdfsTableSink  hdfs_table_sink
}

struct TDataSink {
  1: required TDataSinkType type
  2: optional TDataStreamSink stream_sink
  3: optional TTableSink table_sink
}
