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
// This file contains the details of the protocol between coordinators and backends.

namespace cpp impala
namespace java org.apache.impala.thrift

include "Status.thrift"
include "ErrorCodes.thrift"
include "Types.thrift"
include "Exprs.thrift"
include "CatalogObjects.thrift"
include "Descriptors.thrift"
include "PlanNodes.thrift"
include "Planner.thrift"
include "DataSinks.thrift"
include "Results.thrift"
include "RuntimeProfile.thrift"
include "ImpalaService.thrift"
include "Data.thrift"
include "Query.thrift"

// constants for TPlanNodeId
const i32 INVALID_PLAN_NODE_ID = -1

// Debug options: perform some action in a particular phase of a particular node
// TODO: find a better name
struct TDebugOptions {
  // The plan node that this action should be applied to. If -1 it is applied to all plan
  // nodes.
  1: optional Types.TPlanNodeId node_id
  2: optional PlanNodes.TExecNodePhase phase
  3: optional PlanNodes.TDebugAction action
  // Optional parameter that goes along with the action.
  4: optional string action_param
}

// Descriptor about impalad address designated as runtime filter aggregator.
struct TRuntimeFilterAggDesc {
  // Hostname of aggregator backend.
  1: required string krpc_hostname
  // Ip:port of aggregator backend.
  2: required Types.TNetworkAddress krpc_address
  // Number of impalad that report filter update to this aggregator backend,
  // including the aggregator backend itself.
  3: required i32 num_reporting_hosts
}

// Descriptor that indicates that a runtime filter is produced by a plan node.
struct TRuntimeFilterSource {
  1: required Types.TPlanNodeId src_node_id
  2: required i32 filter_id

  // The following field is only set if a filter source need to send filter update
  // to a designated backend aggregator intead of the coordinator.
  3: optional TRuntimeFilterAggDesc aggregator_desc
}

// The Thrift portion of the execution parameters of a single fragment instance. Every
// fragment instance will also have a corresponding PlanFragmentInstanceCtxPB with the
// same fragment_idx.
// TODO: convert the rest of this struct to protobuf
struct TPlanFragmentInstanceCtx {
  // TPlanFragment.idx
  1: required Types.TFragmentIdx fragment_idx

  // The globally unique fragment instance id.
  // Format: query id + query-wide fragment instance index
  // The query-wide fragment instance index enumerates all fragment instances of a
  // particular query. It starts at 0, so that the query id and the id of the first
  // fragment instance are identical.
  // If there is a coordinator instance, it is the first one, with index 0.
  // Range: [0, TExecQueryFInstancesParams.fragment_instance_ctxs.size()-1]
  2: required Types.TUniqueId fragment_instance_id

  // Index of this fragment instance across all instances of its parent fragment
  // (TPlanFragment with idx = TPlanFragmentInstanceCtx.fragment_idx).
  // Range: [0, <# of instances of parent fragment> - 1]
  3: required i32 per_fragment_instance_idx

  // Number of senders for ExchangeNodes contained in TPlanFragment.plan_tree;
  // needed to create a DataStreamRecvr
  // TODO for per-query exec rpc: move these to PlanFragmentCtxPB
  5: required map<Types.TPlanNodeId, i32> per_exch_num_senders

  // Id of this instance in its role as a sender.
  6: optional i32 sender_id

  7: optional TDebugOptions debug_options

  // List of runtime filters produced by nodes in the finstance.
  8: optional list<TRuntimeFilterSource> filters_produced

  // If this is a join build fragment, the number of fragment instances that consume the
  // join build. -1 = invalid.
  10: optional i32 num_join_build_outputs

  // Number of backends executing the same fragment plan. Can be used by executors to do
  // some estimations.
  11: optional i32 num_backends;
}


// Service Protocol Details

enum ImpalaInternalServiceVersion {
  V1 = 0
}

// The following contains the per-rpc structs for the parameters and the result.

// Contains info about plan fragment execution needed for the ExecQueryFInstances rpc.
// Rather than fully coverting this to protobuf, which would be a large change, for now we
// serialize it ourselves and send it with ExecQueryFInstances as a sidecar.
// TODO: investigate if it's worth converting this fully to protobuf
struct TExecPlanFragmentInfo {
  1: optional list<Planner.TPlanFragment> fragments

  // the order corresponds to the order of fragments in 'fragments'
  2: optional list<TPlanFragmentInstanceCtx> fragment_instance_ctxs
}

// Parameters for RequestPoolService.resolveRequestPool()
// TODO: why is this here?
struct TResolveRequestPoolParams {
  // User to resolve to a pool via the allocation placement policy and
  // authorize for pool access.
  1: required string user

  // Pool name specified by the user. The allocation placement policy may
  // return a different pool.
  2: required string requested_pool
}

// Returned by RequestPoolService.resolveRequestPool()
struct TResolveRequestPoolResult {
  // Actual pool to use, as determined by the pool allocation policy. Not set
  // if no pool was resolved.
  1: optional string resolved_pool

  // True if the user has access to submit requests to the resolved_pool. Not set
  // if no pool was resolved.
  2: optional bool has_access

  3: optional Status.TStatus status
}

// Parameters for RequestPoolService.getPoolConfig()
// TODO: why is this here?
struct TPoolConfigParams {
  // Pool name
  1: required string pool
}

// Returned by RequestPoolService.getPoolConfig()
struct TPoolConfig {
  // Maximum number of placed requests before incoming requests are queued.
  // A value of 0 effectively disables the pool. -1 indicates no limit.
  1: required i64 max_requests

  // Maximum number of queued requests before incoming requests are rejected.
  // Any non-positive number (<= 0) disables queuing, i.e. requests are rejected instead
  // of queued.
  2: required i64 max_queued

  // Maximum memory resources of the pool in bytes.
  // A value of 0 effectively disables the pool. -1 indicates no limit.
  3: required i64 max_mem_resources

  // Maximum amount of time (in milliseconds) that a request will wait to be admitted
  // before timing out. Optional, if not set then the process default (set via gflags) is
  // used.
  4: optional i64 queue_timeout_ms;

  // Default query options that are applied to requests mapped to this pool.
  5: required string default_query_options;

  // Maximum amount of memory that can be assigned to a query (in bytes).
  // 0 indicates no limit. If both max_query_mem_limit and min_query_mem_limit are zero
  // then the admission controller will fall back on old behavior, which is to not set
  // any backend mem limit if mem_limit is not set in the query options.
  6: required i64 max_query_mem_limit = 0;

  // Minimum amount of memory that can be assigned to a query (in bytes).
  // 0 indicates no limit.
  7: required i64 min_query_mem_limit = 0;

  // If false, the mem_limit query option will not be bounded by the max/min query mem
  // limits specified for the pool. Default is true.
  8: required bool clamp_mem_limit_query_option = true;

  // Maximum value for the mt_dop query option. If the mt_dop is set and exceeds this
  // maximum, the mt_dop setting is reduced to the maximum. If the max_mt_dop is
  // negative, no limit is enforced.
  9: required i64 max_mt_dop = -1;

  // Maximum CPU cores per node for processing a query.
  // Typically it should be set with the value less than or equal to the number of cores
  // of the executor nodes.
  // 0 indicates no limit. Default value is set as 0.
  10: required i64 max_query_cpu_core_per_node_limit = 0;

  // Maximum CPU cores on coordinator for processing a query.
  // Typically it should be set with the value less than or equal to the number of cores
  // of the coordinators.
  // 0 indicates no limit. Default value is set as 0.
  11: required i64 max_query_cpu_core_coordinator_limit = 0;
}

struct TParseDateStringResult {
  // True iff date string was successfully parsed
  1: required bool valid
  // Number of days since 1970-01-01. Used only if 'valid' is true.
  2: optional i32 days_since_epoch
  // Canonical date string (formed as 'yyyy-MM-dd'). Used only if 'valid' is true and the
  // parsed date string was not in a canonical form.
  3: optional string canonical_date_string
}
