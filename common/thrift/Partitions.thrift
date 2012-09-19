// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

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

// Specification of how a single logical data stream is logically partitioned.
// This leaves the parameters that determine the physical partitioning (for hash
// partitioning, the number of partitions; for range partitioning, the partitions'
// boundaries), which need to be specified by the enclosing structure/context.
struct TPartitioningSpec {
  1: required TPartitionType type
  2: optional list<Exprs.TExpr> partitioning_exprs
}
