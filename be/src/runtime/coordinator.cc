// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "runtime/coordinator.h"

#include <glog/logging.h>
#include <transport/TTransportUtils.h>

#include "exec/data-sink.h"
#include "exec/exec-stats.h"
#include "exec/hdfs-text-table-sink.h"
#include "runtime/client-cache.h"
#include "runtime/data-stream-sender.h"
#include "runtime/exec-env.h"
#include "runtime/plan-executor.h"
#include "runtime/row-batch.h"
#include "scheduler/scheduler.h"
#include "util/debug-util.h"
#include "gen-cpp/ImpalaBackendService.h"
#include "gen-cpp/ImpalaBackendService_types.h"

using namespace std;
using namespace boost;
using namespace apache::thrift::transport;

namespace impala {

Coordinator::Coordinator(ExecEnv* exec_env, ExecStats* exec_stats)
  : exec_env_(exec_env),
    executor_(new PlanExecutor(exec_env)),
    sink_(NULL),
    execution_completed_(false),
    exec_stats_(exec_stats) {
}

Coordinator::~Coordinator() {
  exec_thread_group_.join_all();
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

  // Only table sinks are valid sinks for coordinator fragments
  if (request.fragmentRequests[0].dataSink.__isset.tableSink) {
    RETURN_IF_ERROR(DataSink::CreateDataSink(request.fragmentRequests[0],
        request.nodeRequestParams[0][0], executor_->row_desc(), &sink_));
    exec_stats_->query_type_ = ExecStats::INSERT;
    RETURN_IF_ERROR(sink_->Init(executor_->runtime_state()));
  } else {
    sink_.reset(NULL);
  }

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
    DCHECK(exec_env_ != NULL);
    // TODO: change this in the following way:
    // * add locations to request.nodeRequestParams.scanRanges
    // * pass in request.nodeRequestParams and let the scheduler figure out where
    // we should be doing those scans, rather than the frontend
    vector<pair<string, int> > hosts;
    RETURN_IF_ERROR(exec_env_->scheduler()->GetHosts(request.execNodes[i-1], &hosts));
    DCHECK_EQ(hosts.size(), request.nodeRequestParams[i].size());

    // start individual plan exec requests
    for (int j = 0; j < hosts.size(); ++j) {
      DCHECK_LT(thread_num, remote_exec_status_.size());
      // there's a race condition here for multi-phase plans (i.e., > 2 fragments):
      // phase i needs to have finished setup, including registration of data streams,
      // before starting up phase i + 1
      // TODO: fix this by breaking the ExecPlanFragment() rpc into 2 rpcs: one
      // for setup, the other one for execution; add a condvar to RemoteExecInfo
      // to capture that setup phase finished successfully
      ImpalaBackendServiceClient* client;
      RETURN_IF_ERROR(exec_env_->client_cache()->GetClient(hosts[j], &client));
      DCHECK(client != NULL);
      PrintClientInfo(hosts[j], request.nodeRequestParams[i][j]);
      exec_thread_group_.add_thread(new thread(
          &Coordinator::ExecRemoteFragment, this, thread_num, client,
          request.fragmentRequests[i], request.nodeRequestParams[i][j]));
      ++thread_num;
    }
  }

  // Call Open() *after* the remote fragments have started; Open() may
  // block waiting for input from the remote fragments
  RETURN_IF_ERROR(executor_->Open());
  return Status::OK;
}

void Coordinator::PrintClientInfo(
    const pair<string, int>& hostport, const TPlanExecParams& params) {
  if (params.scanRanges.empty()) return;
  int64_t total = 0;
  for (int i = 0; i < params.scanRanges[0].hdfsFileSplits.size(); ++i) {
    total += params.scanRanges[0].hdfsFileSplits[i].length;
  }
  VLOG(1) << "data volume for host " << hostport.first << ":" << hostport.second
          << ": " << PrettyPrinter::Print(total, TCounterType::BYTES);
}

void Coordinator::ExecRemoteFragment(
    int thread_num,
    ImpalaBackendServiceClient* client,
    const TPlanExecRequest& request,
    const TPlanExecParams& params) {
  VLOG(1) << "making rpc: ExecPlanFragment";
  TExecPlanFragmentResult thrift_result;
  try {
    client->ExecPlanFragment(thrift_result, request, params);
  } catch (TTransportException& e) {
    stringstream msg;
    msg << "ExecPlanRequest rpc failed: " << e.what();
    LOG(ERROR) << msg.str();
    remote_exec_status_[thread_num] = Status(msg.str());
  }
  // TODO: abort query when we get an error status
  remote_exec_status_[thread_num] = thrift_result.status;
  exec_env_->client_cache()->ReleaseClient(client);

  // Grab the lock to gather fragment results
  lock_guard<mutex> l(fragment_complete_lock_);

  // Deserialize and set each fragment as a child of the coordinator profile.
  fragment_profiles_[thread_num] =
      RuntimeProfile::CreateFromThrift(obj_pool(), thrift_result.profiles);
  query_profile_->AddChild(fragment_profiles_[thread_num]);
}

Status Coordinator::GetNext(RowBatch** batch, RuntimeState* state) {
  COUNTER_SCOPED_TIMER(query_profile_->total_time_counter());
  Status result = executor_->GetNext(batch);
  VLOG(1) << "coord.getnext";
  if (*batch == NULL) {
    execution_completed_ = true;
    // Join the threads to collect all the perf counters
    exec_thread_group_.join_all();
    if (sink_.get() != NULL) RETURN_IF_ERROR(sink_->Close(state));
    query_profile_->AddChild(executor_->query_profile());
  } else {
    if (sink_.get() != NULL) {
      RETURN_IF_ERROR(sink_->Send(state, *batch));
      // Only update stats once we've done all the work intended for a batch
      exec_stats_->num_rows_ += (*batch)->num_rows();
      // Callers of this method should not use batch == NULL to detect
      // if there is no more work to be done
      *batch = NULL;
    } else {
      exec_stats_->num_rows_ += (*batch)->num_rows();
    }
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
