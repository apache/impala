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
// This file contains structures produced by the planner.

namespace cpp impala
namespace java org.apache.impala.thrift

include "Types.thrift"
include "Exprs.thrift"
include "DataSinks.thrift"
include "PlanNodes.thrift"
include "Partitions.thrift"

// TPlanFragment encapsulates info needed to execute a particular
// plan fragment, including how to produce and how to partition its output.
// It leaves out node-specific parameters needed for the actual execution.
struct TPlanFragment {
  // Ordinal number of fragment within a query; range: 0..<total # fragments>
  1: required Types.TFragmentIdx idx

  // display name to be shown in the runtime profile; unique within a query
  2: required string display_name

  // no plan or descriptor table: query without From clause
  3: optional PlanNodes.TPlan plan

  // Specifies the destination of this plan fragment's output rows.
  // For example, the destination could be a stream sink which forwards
  // the data to a remote plan fragment, or a sink which writes to a table (for
  // insert stmts).
  5: optional DataSinks.TDataSink output_sink

  // Partitioning of the data created by all instances of this plan fragment;
  // partitioning.type has the following meaning:
  // - UNPARTITIONED: there is only one instance of the plan fragment
  // - RANDOM: a particular output row is randomly assigned to any of the instances
  // - HASH_PARTITIONED: output row r is produced by
  //   hash_value(partitioning.partitioning_exprs(r)) % #partitions
  // - RANGE_PARTITIONING: currently not supported
  // This is distinct from the partitioning of each plan fragment's
  // output, which is specified by output_sink.output_partitioning.
  6: required Partitions.TDataPartition partition

  // The minimum memory reservation (in bytes) required for this plan fragment to execute
  // on a single host. Split between the per-backend resources shared across all
  // instances on a backend and the per-instance resources required for each instance.
  7: optional i64 backend_min_mem_reservation_bytes
  12: optional i64 instance_min_mem_reservation_bytes

  // Total of the initial memory reservations that we expect to be claimed by this
  // fragment. I.e. the sum of the min reservations over all operators (including the
  // sink) in a single instance of this fragment. This is used for an optimization in
  // InitialReservation. Measured in bytes. required in V1.
  // Split between the per-backend resources shared across all instances on a backend
  // and the per-instance resources required for each instance.
  8: optional i64 instance_initial_mem_reservation_total_claims
  13: optional i64 backend_initial_mem_reservation_total_claims

  // The total memory (in bytes) required for the runtime filters produced by the plan
  // nodes managed by this fragment. Is included in instance_min_reservation_bytes.
  9: optional i64 produced_runtime_filters_reservation_bytes

  // The total memory (in bytes) required for the runtime filters produced by the plan
  // nodes managed by this fragment. Is included in backend_min_reservation_bytes.
  11: optional i64 consumed_runtime_filters_reservation_bytes

  // Maximum number of required threads that will be executing concurrently for this plan
  // fragment, i.e. the number of threads that this query needs to execute successfully.
  10: optional i64 thread_reservation

  // The effective number of parallelism for this fragment that dictated by the frontend
  // planner. If the frontend planner set this to a positive number, the backend scheduler
  // must make sure that it schedules no more than this many instance fragments. Must be
  // greater than 0 if query option COMPUTE_PROCESSING_COST=true. Currently not enforced
  // when fragment need to exceed max_fs_writers query option (see IMPALA-8125).
  14: optional i32 effective_instance_count

  // If true, the fragment must be scheduled on the coordinator. In this case 'partition'
  // must be UNPARTITIONED.
  15: required bool is_coordinator_only

  // Marker on whether this is a dominant fragment or not. Only possible to be true if
  // COMPUTE_PROCESSING_COST=true. Otherwise, always false.
  // See PlanFragment.java for definition of dominant fragment.
  16: optional bool is_dominant = false
}

// location information for a single scan range
struct TScanRangeLocation {
  // Index into TQueryExecRequest.host_list.
  1: required i32 host_idx;

  // disk volume identifier of a particular scan range at 'server';
  // -1 indicates an unknown volume id;
  // only set for TScanRange.hdfs_file_split
  2: optional i32 volume_id = -1

  // If true, this block is cached on this server.
  3: optional bool is_cached = false
}

// A single scan range plus the hosts that serve it
struct TScanRangeLocationList {
  1: required PlanNodes.TScanRange scan_range
  // non-empty list
  2: list<TScanRangeLocation> locations
}

// A specification for scan ranges. Scan ranges can be
// concrete or specs, which are used to generate concrete ranges.
// Each type is stored in a separate list.
struct TScanRangeSpec {
   1: optional list<TScanRangeLocationList> concrete_ranges
   2: optional list<PlanNodes.TFileSplitGeneratorSpec> split_specs
}

// A plan: tree of plan fragments that materializes either a query result or the build
// side of a join used by another plan; it consists of a sequence of plan fragments.
// TODO: rename both this and PlanNodes.TPlan (TPlan should be something like TExecPlan
// or TExecTree)
struct TPlanFragmentTree {
  1: required i32 cohort_id

  2: required list<TPlanFragment> fragments
}
