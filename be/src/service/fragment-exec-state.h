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

#ifndef IMPALA_SERVICE_FRAGMENT_EXEC_STATE_H
#define IMPALA_SERVICE_FRAGMENT_EXEC_STATE_H

#include <boost/bind.hpp>
#include <boost/thread/mutex.hpp>

#include "common/status.h"
#include "runtime/client-cache.h"
#include "runtime/plan-fragment-executor.h"
#include "service/fragment-mgr.h"

namespace impala {

/// Execution state of a single plan fragment.
class FragmentMgr::FragmentExecState {
 public:
  FragmentExecState(const TExecPlanFragmentParams& params, ExecEnv* exec_env)
    : fragment_instance_ctx_(params.fragment_instance_ctx),
      executor_(exec_env, boost::bind<void>(
          boost::mem_fn(&FragmentMgr::FragmentExecState::ReportStatusCb),
              this, _1, _2, _3)),
      client_cache_(exec_env->impalad_client_cache()), exec_params_(params) {
  }

  /// Calling the d'tor releases all memory and closes all data streams
  /// held by executor_.
  ~FragmentExecState() { }

  /// Returns current execution status, if there was an error. Otherwise cancels
  /// the fragment and returns OK.
  Status Cancel();

  /// Call Prepare() and create and initialize data sink.
  Status Prepare();

  /// Main loop of plan fragment execution. Blocks until execution finishes.
  void Exec();

  const TUniqueId& query_id() const {
    return fragment_instance_ctx_.query_ctx.query_id;
  }

  const TUniqueId& fragment_instance_id() const {
    return fragment_instance_ctx_.fragment_instance_id;
  }

  const TNetworkAddress& coord_address() const {
    return fragment_instance_ctx_.query_ctx.coord_address;
  }

  /// Set the execution thread, taking ownership of the object.
  void set_exec_thread(Thread* exec_thread) { exec_thread_.reset(exec_thread); }

  /// Publishes filter with ID 'filter_id' to this fragment's filter bank.
  void PublishFilter(int32_t filter_id, const TBloomFilter& thrift_bloom_filter);

 private:
  TPlanFragmentInstanceCtx fragment_instance_ctx_;
  PlanFragmentExecutor executor_;
  ImpalaInternalServiceClientCache* client_cache_;
  TExecPlanFragmentParams exec_params_;

  /// the thread executing this plan fragment
  boost::scoped_ptr<Thread> exec_thread_;

  /// protects exec_status_
  boost::mutex status_lock_;

  /// set in ReportStatusCb();
  /// if set to anything other than OK, execution has terminated w/ an error
  Status exec_status_;

  /// Update 'exec_status_' w/ 'status', if the former is not already an error.
  /// Returns the value of 'exec_status_' after this method completes.
  Status UpdateStatus(const Status& status);

  /// Callback for executor; updates exec_status_ if 'status' indicates an error
  /// or if there was a thrift error.
  ///
  /// If not NULL, `profile` is encoded as a Thrift structure and transmitted as part of
  /// the reporting RPC. `profile` may be NULL if a runtime profile has not been created
  /// for this fragment (e.g. when the fragment has failed during preparation).
  void ReportStatusCb(const Status& status, RuntimeProfile* profile, bool done);
};

}

#endif
