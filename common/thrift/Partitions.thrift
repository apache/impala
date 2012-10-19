// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

namespace cpp impala
namespace java com.cloudera.impala.thrift

include "Exprs.thrift"

enum TPartitionType {
  UNPARTITIONED,

  // round-robin partitioning
  RANDOM,

  // unordered partitioning on a set of exprs
  HASH_PARTITIONED,

  // ordered partitioning on a list of exprs
  RANGE_PARTITIONED
}

// Specification of how a single logical data stream is partitioned.
// This leaves out the parameters that determine the physical partitioning (for hash
// partitioning, the number of partitions; for range partitioning, the partitions'
// boundaries), which need to be specified by the enclosing structure/context.
struct TDataPartition {
  1: required TPartitionType type
  2: optional list<Exprs.TExpr> partitioning_exprs
}
