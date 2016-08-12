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
  UNPARTITIONED,

  // round-robin partition
  RANDOM,

  // unordered partition on a set of exprs
  // (partition bounds overlap)
  HASH_PARTITIONED,

  // ordered partition on a list of exprs
  // (partition bounds don't overlap)
  RANGE_PARTITIONED
}

// Specification of how a single logical data stream is partitioned.
// This leaves out the parameters that determine the physical partition (for hash
// partitions, the number of partitions; for range partitions, the partitions'
// boundaries), which need to be specified by the enclosing structure/context.
struct TDataPartition {
  1: required TPartitionType type
  2: optional list<Exprs.TExpr> partition_exprs
}
