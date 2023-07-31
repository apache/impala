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

include "Exprs.thrift"

enum TPartitionType {
  UNPARTITIONED = 0

  // round-robin partition
  RANDOM = 1

  // unordered partition on a set of exprs
  // (partition bounds overlap)
  HASH_PARTITIONED = 2

  // ordered partition on a list of exprs
  // (partition bounds don't overlap)
  RANGE_PARTITIONED = 3

  // use the partitioning scheme of a Kudu table
  // TODO: this is a special case now because Kudu supports multilevel partition
  // schemes. We should add something like lists of TDataPartitions to reflect that
  // and then this can be removed. (IMPALA-5255)
  KUDU = 4

  // Used for distributing the contents of Iceberg delete files. Each row of a delete
  // file is directly sent to the hosts that are responsible for reading the
  // corresponding data files. No broadcast or shuffle is needed in this case.
  DIRECTED = 5
}

// Specification of how a single logical data stream is partitioned.
// This leaves out the parameters that determine the physical partition (for hash
// partitions, the number of partitions; for range partitions, the partitions'
// boundaries), which need to be specified by the enclosing structure/context.
struct TDataPartition {
  1: required TPartitionType type
  2: optional list<Exprs.TExpr> partition_exprs
}
