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


#ifndef IMPALA_RUNTIME_FRAGMENT_INSTANCE_STATE_H
#define IMPALA_RUNTIME_FRAGMENT_INSTANCE_STATE_H

#include "common/status.h"
#include "util/promise.h"
#include "runtime/plan-fragment-executor.h"

#include "gen-cpp/ImpalaInternalService_types.h"

namespace impala {

class TPlanFragmentCtx;
class TPlanFragmentInstanceCtx;
class TBloomFilter;
class TUniqueId;
class TNetworkAddress;
class QueryState;
class PlanFragmentExecutor;
class RuntimeProfile;

/// Collection of state specific to the execution of a particular fragment instance,
/// as well as manager of the execution of that fragment instance, including
/// set-up and tear-down.
/// Tear-down happens automatically in the d'tor and frees all memory allocated for
/// this fragment instance and closes all data streams.
///
/// Aside from Cancel(), which may be called asynchronously, this class is not
/// thread-safe.
///
/// TODO:
/// - merge PlanFragmentExecutor into this class
/// - move tear-down logic out of d'tor and into ReleaseResources() function
/// - as part of the per-query exec rpc, get rid of concurrency during setup
///   (and remove prepare_promise_)
/// - move ReportStatusCb() logic into PFE::SendReport() and get rid of the callback
class FragmentInstanceState {
 public:
  FragmentInstanceState(QueryState* query_state, const TPlanFragmentCtx& fragment_ctx,
      const TPlanFragmentInstanceCtx& instance_ctx, const TDescriptorTable& desc_tbl);

  /// Frees up all resources allocated in Exec().
  /// It is an error to delete a FragmentInstanceState before Exec() returns.
  ~FragmentInstanceState() { }

  /// Main loop of plan fragment execution. Blocks until execution finishes.
  void Exec();

  /// Returns current execution status, if there was an error. Otherwise cancels
  /// the fragment and returns OK.
  Status Cancel();

  /// Publishes filter with ID 'filter_id' to this fragment instance's filter bank.
  void PublishFilter(int32_t filter_id, const TBloomFilter& thrift_bloom_filter);

  QueryState* query_state() { return query_state_; }
  PlanFragmentExecutor* executor() { return &executor_; }
  const TQueryCtx& query_ctx() const;
  const TPlanFragmentCtx& fragment_ctx() const { return fragment_ctx_; }
  const TPlanFragmentInstanceCtx& instance_ctx() const { return instance_ctx_; }
  const TUniqueId& query_id() const { return query_ctx().query_id; }
  const TUniqueId& instance_id() const { return instance_ctx_.fragment_instance_id; }
  const TNetworkAddress& coord_address() const { return query_ctx().coord_address; }

 private:
  QueryState* query_state_;
  const TPlanFragmentCtx fragment_ctx_;
  const TPlanFragmentInstanceCtx instance_ctx_;

  /// instance-specific descriptor table
  /// TODO: remove when switching to per-query exec rpc
  const TDescriptorTable desc_tbl_;

  PlanFragmentExecutor executor_;

  /// protects exec_status_
  boost::mutex status_lock_;

  /// set in ReportStatusCb();
  /// if set to anything other than OK, execution has terminated w/ an error
  Status exec_status_;

  /// Barrier for the completion of executor_.Prepare().
  Promise<Status> prepare_promise_;

  /// Update 'exec_status_' w/ 'status', if the former is not already an error.
  /// Returns the value of 'exec_status_' after this method completes.
  Status UpdateStatus(const Status& status);

  /// Callback for executor; updates exec_status_ if 'status' indicates an error
  /// or if there was a thrift error.
  /// If not NULL, `profile` is encoded as a Thrift structure and transmitted as part of
  /// the reporting RPC. `profile` may be NULL if a runtime profile has not been created
  /// for this fragment (e.g. when the fragment has failed during preparation).
  /// The executor must ensure that there is only one invocation at a time.
  void ReportStatusCb(const Status& status, RuntimeProfile* profile, bool done);
};

}

#endif
