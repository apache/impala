// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "runtime/coordinator.h"

#include <glog/logging.h>

#include "util/debug-util.h"
#include "runtime/plan-executor.h"
#include "testutil/test-env.h"
#include "gen-cpp/ImpalaBackendService.h"
#include "gen-cpp/ImpalaBackendService_types.h"

using namespace std;
using namespace boost;

namespace impala {

Coordinator::Coordinator(const string& host, int port, DataStreamMgr* stream_mgr,
                         TestEnv* test_env)
  : host_(host),
    port_(port),
    stream_mgr_(stream_mgr),
    test_env_(test_env),
    executor_(new PlanExecutor(stream_mgr, test_env_->fs_cache())) {
}

Coordinator::~Coordinator() {
  exec_thread_group_.join_all();
  test_env_->ReleaseClients(clients_);
}

Status Coordinator::Exec(const TQueryExecRequest& request) { 
  // fragment 0 is the coordinator/"local" fragment that we're executing ourselves;
  // start this before starting any more plan fragments in backend threads, otherwise
  // they start sending data before the local exchange node had a chance to register
  // with the stream mgr
  DCHECK_GT(request.nodeRequestParams.size(), 0);
  // the first nodeRequestParams list contains exactly one TPlanExecParams
  // (it's meant for the coordinator fragment)
  DCHECK_EQ(request.nodeRequestParams[0].size(), 1);
  RETURN_IF_ERROR(executor_->Prepare(
      request.fragmentRequests[0], request.nodeRequestParams[0][0]));
  
  query_profile_.reset(
      new RuntimeProfile(obj_pool(), "Query(id=" + PrintId(request.queryId) + ")"));

  COUNTER_SCOPED_TIMER(query_profile_->total_time_counter());

  // determine total number of fragments
  int num_threads = 0;
  // execNodes may contain empty list
  DCHECK_GE(request.execNodes.size(), request.fragmentRequests.size() - 1);
  for (int i = 1; i < request.fragmentRequests.size(); ++i) {
    num_threads += request.execNodes[i - 1].size();
  }
  if (num_threads > 0) {
    remote_exec_status_.resize(num_threads);
    fragment_profiles_.resize(num_threads);
  }

  // Start non-coord fragments on remote nodes;
  // fragmentRequests[i] can receive data from fragmentRequests[>i],
  // so start fragments in ascending order.
  int thread_num = 0;
  for (int i = 1; i < request.fragmentRequests.size(); ++i) {
    DCHECK(test_env_ != NULL);
    int num_nodes = request.execNodes[i - 1].size();
    // ignore actual nodes indicated by TQueryExecRequest::execNodes for now
    test_env_->GetClients(num_nodes, &clients_);
    DCHECK_EQ(num_nodes, clients_.size());

    // start individual plan exec requests
    for (int j = 0; j < num_nodes; ++j) {
      DCHECK_LT(thread_num, remote_exec_status_.size());
      // there's a race condition here for multi-phase plans (i.e., > 2 fragments):
      // phase i needs to have finished setup, including registration of data streams,
      // before starting up phase i + 1
      // TODO: fix this by breaking the ExecPlanFragment() rpc into 2 rpcs: one
      // for setup, the other one for execution; add a condvar to RemoteExecInfo
      // to capture that setup phase finished successfully
      exec_thread_group_.add_thread(new thread(
          &Coordinator::ExecRemoteFragment, this, thread_num, clients_[j], 
          request.fragmentRequests[i], request.nodeRequestParams[i][j]));
      ++thread_num;
    }
  }

  // Call Open() *after* the remote fragments have started; Open() may
  // block waiting for input from the remote fragments
  RETURN_IF_ERROR(executor_->Open());
  return Status::OK;
}

void Coordinator::ExecRemoteFragment(
    int thread_num,
    ImpalaBackendServiceClient* client,
    const TPlanExecRequest& request,
    const TPlanExecParams& params) {
  VLOG(1) << "making rpc: ExecPlanFragment";
  TExecPlanFragmentResult thrift_result;
  client->ExecPlanFragment(thrift_result, request, params);
  // TODO: abort query when we get an error status
  remote_exec_status_[thread_num] = thrift_result.status;

  // Grab the lock to gather fragment results
  lock_guard<mutex> l(fragment_complete_lock_);

  // Deserialize and set each fragment as a child of the coordinator profile.
  fragment_profiles_[thread_num] = 
      RuntimeProfile::CreateFromThrift(obj_pool(), thrift_result.profiles);
  query_profile_->AddChild(fragment_profiles_[thread_num]);
}

Status Coordinator::GetNext(RowBatch** batch) {
  COUNTER_SCOPED_TIMER(query_profile_->total_time_counter());
  Status result = executor_->GetNext(batch);
  VLOG(1) << "coord.getnext";
  if (*batch == NULL) {
    // Join the threads to collect all the perf counters
    exec_thread_group_.join_all();
    query_profile_->AddChild(executor_->query_profile());
  }
  return result;
}

void Coordinator::Cancel() {
  // TODO: implement this; will require switching to async ExecPlanFragment() rpcs
  // and a CancellationMgr that allows registration of cancellation callbacks (so
  // the scan nodes know when to stop scanning)
}

const RowDescriptor& Coordinator::row_desc() const {
  DCHECK(executor_.get() != NULL);
  return executor_->row_desc();
}

RuntimeState* Coordinator::runtime_state() {
  DCHECK(executor_.get() != NULL);
  return executor_->runtime_state();
}

ObjectPool* Coordinator::obj_pool() {
  DCHECK(executor_.get() != NULL);
  return executor_->runtime_state()->obj_pool();
}

}
