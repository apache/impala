// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "runtime/coordinator.h"

#include <limits>
#include <glog/logging.h>
#include <gflags/gflags.h>
#include <transport/TTransportUtils.h>
#include <boost/algorithm/string/join.hpp>

#include "exec/data-sink.h"
#include "exec/exec-stats.h"
#include "exec/hdfs-table-sink.h"
#include "runtime/client-cache.h"
#include "runtime/data-stream-sender.h"
#include "runtime/data-stream-mgr.h"
#include "runtime/exec-env.h"
#include "runtime/plan-fragment-executor.h"
#include "runtime/row-batch.h"
#include "sparrow/scheduler.h"
#include "util/debug-util.h"
#include "gen-cpp/ImpalaInternalService.h"
#include "gen-cpp/ImpalaInternalService_types.h"

using namespace std;
using namespace boost;
using namespace apache::thrift::transport;

DECLARE_int32(be_port);
DECLARE_string(host);

namespace impala {

Coordinator::Coordinator(ExecEnv* exec_env, ExecStats* exec_stats)
  : exec_env_(exec_env),
    has_called_wait_(false),
    executor_(new PlanFragmentExecutor(exec_env)),
    sink_(NULL),
    execution_completed_(false),
    exec_stats_(exec_stats) {
}

Coordinator::~Coordinator() {
}

Status Coordinator::Exec(TQueryExecRequest* request) {
  query_id_ = request->query_id;
  VLOG_QUERY << "Coordinator::Exec() stmt=" << request->sql_stmt;

  // fragment 0 is the coordinator/"local" fragment that we're executing ourselves;
  // start this before starting any more plan fragments in backend threads, otherwise
  // they start sending data before the local exchange node had a chance to register
  // with the stream mgr
  DCHECK_GT(request->node_request_params.size(), 0);
  // the first node_request_params list contains exactly one TPlanExecParams
  // (it's meant for the coordinator fragment)
  DCHECK_EQ(request->node_request_params[0].size(), 1);

  // to keep things simple, make async Cancel() calls wait until plan fragment
  // execution has been initiated
  lock_guard<mutex> l(lock_);

  // register data streams for coord fragment
  RETURN_IF_ERROR(executor_->Prepare(
      request->fragment_requests[0], request->node_request_params[0][0]));

  // Only table sinks are valid sinks for coordinator fragments
  if (request->fragment_requests[0].data_sink.__isset.tableSink) {
    RETURN_IF_ERROR(DataSink::CreateDataSink(request->fragment_requests[0],
        request->node_request_params[0][0], executor_->row_desc(), &sink_));
    exec_stats_->query_type_ = ExecStats::INSERT;
    RETURN_IF_ERROR(sink_->Init(executor_->runtime_state()));
  } else {
    sink_.reset(NULL);
  }

  if (request->node_request_params.size() > 1) {
    // for now, set destinations of 2nd fragment to coord host/port
    // TODO: determine execution hosts first, then set destinations to those hosts
    for (int i = 0; i < request->node_request_params[1].size(); ++i) {
      DCHECK_EQ(request->node_request_params[1][i].destinations.size(), 1);
      request->node_request_params[1][i].destinations[0].host = FLAGS_host;
      request->node_request_params[1][i].destinations[0].port = FLAGS_be_port;
    }
  }

  query_profile_.reset(
      new RuntimeProfile(obj_pool(), "Query(id=" + PrintId(request->query_id) + ")"));
  COUNTER_SCOPED_TIMER(query_profile_->total_time_counter());

  // Start non-coord fragments on remote nodes;
  // fragment_requests[i] can receive data from fragment_requests[>i],
  // so start fragments in ascending order.
  int backend_num = 0;
  for (int i = 1; i < request->fragment_requests.size(); ++i) {
    DCHECK(exec_env_ != NULL);
    // TODO: change this in the following way:
    // * add locations to request->node_request_params.scan_ranges
    // * pass in request->node_request_params and let the scheduler figure out where
    // we should be doing those scans, rather than the frontend
    vector<pair<string, int> > hosts;
    RETURN_IF_ERROR(
        exec_env_->scheduler()->GetHosts(request->data_locations[i-1], &hosts));
    DCHECK_EQ(hosts.size(), request->node_request_params[i].size());

    // start individual plan exec requests
    // TODO: to start up more quickly, we need to start fragment_requests[i]
    // on all backends in parallel (ie, we need to have a server-wide pool of threads
    // that we use to start plan fragments at backends)
    TPlanExecRequest& fragment_request = request->fragment_requests[i];
    for (int j = 0; j < hosts.size(); ++j) {
      // TODO: pool of pre-formatted BackendExecStates?
      BackendExecState* exec_state =
          obj_pool()->Add(
            new BackendExecState(fragment_request.fragment_id, backend_num, hosts[j]));
      DCHECK_EQ(backend_exec_states_.size(), backend_num);
      backend_exec_states_.push_back(exec_state);
      query_profile_->AddChild(exec_state->profile);
      PrintClientInfo(hosts[j], request->node_request_params[i][j]);

      Status fragment_exec_status = ExecRemoteFragment(exec_state,
          fragment_request, request->node_request_params[i][j]);
      if (!fragment_exec_status.ok()) {
        // tear down running fragments and return
        Cancel(false);
        return fragment_exec_status;
      }
      ++backend_num;
    }
  }

  return Status::OK;
}

Status Coordinator::Wait() {
  lock_guard<mutex> l(wait_lock_);
  if (has_called_wait_) return Status::OK;
  has_called_wait_ = true;
  // Open() may block
  RETURN_IF_ERROR(executor_->Open());
  return Status::OK;
}

void Coordinator::PrintClientInfo(
    const pair<string, int>& hostport, const TPlanExecParams& params) {
  if (params.scan_ranges.empty()) return;
  int64_t total = 0;
  for (int i = 0; i < params.scan_ranges[0].hdfsFileSplits.size(); ++i) {
    total += params.scan_ranges[0].hdfsFileSplits[i].length;
  }
  VLOG_CONNECTION << "data volume for host " << hostport.first
      << ":" << hostport.second
      << ": " << PrettyPrinter::Print(total, TCounterType::BYTES);
}

Status Coordinator::ExecRemoteFragment(
    BackendExecState* exec_state,
    const TPlanExecRequest& exec_request,
    const TPlanExecParams& exec_params) {
  VLOG_QUERY << "making rpc: ExecPlanFragment";
  lock_guard<mutex> l(exec_state->lock);

  // this client needs to have been released when this function finishes
  ImpalaInternalServiceClient* backend_client;
  RETURN_IF_ERROR(
      exec_env_->client_cache()->GetClient(exec_state->hostport, &backend_client));
  DCHECK(backend_client != NULL);

  TExecPlanFragmentParams params;
  params.protocol_version = ImpalaInternalServiceVersion::V1;
  // TODO: is this yet another copy? find a way to avoid those.
  params.__set_request(exec_request);
  params.__set_params(exec_params);
  params.coord.host = FLAGS_host;
  params.coord.port = FLAGS_be_port;
  params.__isset.coord = true;
  params.__set_backend_num(exec_state->backend_num);

  TExecPlanFragmentResult thrift_result;
  try {
    backend_client->ExecPlanFragment(thrift_result, params);
  } catch (TTransportException& e) {
    stringstream msg;
    msg << "ExecPlanRequest rpc failed: " << e.what();
    LOG(ERROR) << msg.str();
    exec_state->status = Status(msg.str());
    exec_env_->client_cache()->ReleaseClient(backend_client);
    return exec_state->status;
  }
  exec_state->status = thrift_result.status;
  exec_env_->client_cache()->ReleaseClient(backend_client);
  return exec_state->status;
}

Status Coordinator::GetNext(RowBatch** batch, RuntimeState* state) {
  Status status = GetNextInternal(batch, state);
  // close the executor if we see an error status (including cancellation) or 
  // hit the end
  if (!status.ok() || execution_completed_) {
    status.AddError(executor_->Close());
  }
  return status;
}

Status Coordinator::GetNextInternal(RowBatch** batch, RuntimeState* state) {
  DCHECK(has_called_wait_);
  COUNTER_SCOPED_TIMER(query_profile_->total_time_counter());
  VLOG_ROW << "coord.getnext";
  RETURN_IF_ERROR(executor_->GetNext(batch));
  if (*batch == NULL) {
    execution_completed_ = true;
    if (sink_.get() != NULL) RETURN_IF_ERROR(sink_->Close(state));
  } else {
    // TODO: fix this: the advertised behavior is that when we're sending to
    // a sink, GetNext() doesn't return until all input has been sent; ie,
    // we need to loop here
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
  return Status::OK;
}

void Coordinator::Cancel() {
  Cancel(true);
}

void Coordinator::Cancel(bool get_lock) {
  // if requested, synchronize Cancel() with a possibly concurrently running Exec()
  mutex dummy;
  lock_guard<mutex> l(get_lock ? lock_ : dummy);

  // cancel local fragment
  if (executor_.get() != NULL) {
    executor_->runtime_state()->set_is_cancelled(true);
    // cancel all incoming data streams
    exec_env_->stream_mgr()->Cancel(runtime_state()->fragment_id());
  }

  for (int i = 0; i < backend_exec_states_.size(); ++i) {
    BackendExecState* exec_state = backend_exec_states_[i];

    // lock each exec_state individually to synchronize correctly with
    // UpdateFragmentExecStatus() (which doesn't get the global lock_)
    lock_guard<mutex> l(exec_state->lock);

    // don't cancel if it already finished
    if (exec_state->done) continue;

    // if we get an error while trying to get a connection to the backend,
    // keep going
    ImpalaInternalServiceClient* backend_client;
    Status status =
        exec_env_->client_cache()->GetClient(exec_state->hostport, &backend_client);
    if (!status.ok()) {
      continue;
    }
    DCHECK(backend_client != NULL);

    TCancelPlanFragmentParams params;
    params.protocol_version = ImpalaInternalServiceVersion::V1;
    params.__set_fragment_id(exec_state->fragment_id);
    TCancelPlanFragmentResult res;
    try {
      backend_client->CancelPlanFragment(res, params);
    } catch (TTransportException& e) {
      stringstream msg;
      msg << "CancelPlanFragment rpc failed: " << e.what();
      // make a note of the error status, but keep on cancelling the other fragments
      if (exec_state->status.ok()) {
        exec_state->status = Status(msg.str());
      } else {
        // if we already recorded a failure, keep on adding error msgs
        exec_state->status.AddErrorMsg(msg.str());
      }
      exec_env_->client_cache()->ReleaseClient(backend_client);
      continue;
    }
    if (res.status.status_code != TStatusCode::OK) {
      if (exec_state->status.ok()) {
        exec_state->status = Status(algorithm::join(res.status.error_msgs, "; "));
      } else {
        // if we already recorded a failure, keep on adding error msgs
        exec_state->status.AddErrorMsg(algorithm::join(res.status.error_msgs, "; "));
      }
    }
    exec_env_->client_cache()->ReleaseClient(backend_client);
  }
}

Status Coordinator::UpdateFragmentExecStatus(
    int backend_num, const TStatus& tstatus, bool done,
    const TRuntimeProfileTree& cumulative_profile) {
  if (backend_num >= backend_exec_states_.size()) {
    return Status(TStatusCode::INTERNAL_ERROR, "unknown backend number");
  }
  BackendExecState* exec_state = backend_exec_states_[backend_num];

  Status status(tstatus);
  {
    lock_guard<mutex> l(exec_state->lock);
    // make sure we don't go from error status to OK
    DCHECK(!status.ok() || exec_state->status.ok())
        << "fragment is transitioning from error status to OK:"
        << " query_id=" << query_id_ << " fragment#=" << backend_num
        << " status=" << exec_state->status.GetErrorMsg();
    exec_state->status = status;
    exec_state->done = done;
    exec_state->profile =
        RuntimeProfile::CreateFromThrift(obj_pool(), cumulative_profile);
  }

  // for now, abort the query if we see any error
  if (!status.ok()) Cancel();
  return Status::OK;
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
