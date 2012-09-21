// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "runtime/coordinator.h"

#include <limits>
#include <transport/TTransportUtils.h>
#include <boost/algorithm/string/join.hpp>
#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/min.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <boost/accumulators/statistics/median.hpp>
#include <boost/accumulators/statistics/max.hpp>
#include <boost/accumulators/statistics/variance.hpp>
#include <boost/bind.hpp>
#include <boost/foreach.hpp>

#include "common/logging.h"
#include "exec/data-sink.h"
#include "runtime/client-cache.h"
#include "runtime/data-stream-sender.h"
#include "runtime/data-stream-mgr.h"
#include "runtime/exec-env.h"
#include "runtime/plan-fragment-executor.h"
#include "runtime/row-batch.h"
#include "runtime/parallel-executor.h"
#include "sparrow/scheduler.h"
#include "exec/exec-stats.h"
#include "exec/data-sink.h"
#include "exec/scan-node.h"
#include "util/debug-util.h"
#include "gen-cpp/ImpalaInternalService.h"
#include "gen-cpp/ImpalaInternalService_types.h"
#include "gen-cpp/Frontend_types.h"
#include "gen-cpp/PlanNodes_types.h"

using namespace std;
using namespace boost;
using namespace boost::accumulators;
using namespace apache::thrift::transport;

DECLARE_int32(be_port);
DECLARE_string(host);

namespace impala {

Coordinator::BackendExecState::BackendExecState(
    const TUniqueId& fragment_id, int backend_num,
    const std::pair<std::string, int>& hostport,
    const TPlanExecRequest* exec_request,
    const TPlanExecParams* exec_params,
    ObjectPool* obj_pool) 
  : fragment_id(fragment_id),
    backend_num(backend_num),
    hostport(hostport),
    total_split_size(0),
    exec_request(exec_request),
    exec_params(exec_params),
    initiated(false),
    done(false),
    profile_created(false) {
  ComputeTotalSplitSize(*exec_params);
  profile = obj_pool->Add(
      new RuntimeProfile(obj_pool, "Fragment " + PrintId(fragment_id)));
}

void Coordinator::BackendExecState::ComputeTotalSplitSize(
    const TPlanExecParams& params) {
  if (params.scan_ranges.empty()) return;
  total_split_size = 0;
  for (int i = 0; i < params.scan_ranges[0].hdfsFileSplits.size(); ++i) {
    total_split_size += params.scan_ranges[0].hdfsFileSplits[i].length;
  }
}

int64_t Coordinator::BackendExecState::GetNodeThroughput(int node_id) {
  RuntimeProfile::Counter* counter = NULL;
  {
    lock_guard<mutex> l(lock);
    ThroughputCounterMap::iterator i = throughput_counters.find(node_id);
    if (i == throughput_counters.end()) return 0;
    counter = i->second;
  }
  DCHECK(counter != NULL);
  // make sure not to hold lock when calling value() to avoid potential deadlocks
  return counter->value();
}

Coordinator::Coordinator(ExecEnv* exec_env, ExecStats* exec_stats)
  : exec_env_(exec_env),
    has_called_wait_(false),
    executor_(new PlanFragmentExecutor(
        exec_env, PlanFragmentExecutor::ReportStatusCallback())),
    exec_stats_(exec_stats),
    num_remaining_backends_(0),
    obj_pool_(NULL) {
}

Coordinator::~Coordinator() {
}

Status Coordinator::Exec(TQueryExecRequest* request) {
  query_id_ = request->query_id;
  VLOG_QUERY << "Exec() query_id=" << request->query_id
             << " stmt=" << request->sql_stmt;

  // to keep things simple, make async Cancel() calls wait until plan fragment
  // execution has been initiated, otherwise we might try to cancel fragment
  // execution at backends where it hasn't even started
  lock_guard<mutex> l(lock_);

  DCHECK_GT(request->fragment_requests.size(), 0);
  DCHECK_GT(request->node_request_params.size(), 0);

  if (request->has_coordinator_fragment) {
    // If a coordinator fragment is requested (for most queries this
    // will be the case, the exception is parallel INSERT queries), start
    // this before starting any more plan fragments in backend threads,
    // otherwise they start sending data before the local exchange node
    // had a chance to register with the stream mgr
    // register data streams for coord fragment
    RETURN_IF_ERROR(executor_->Prepare(
            request->fragment_requests[0], request->node_request_params[0][0]));

    if (request->node_request_params.size() > 1) {
      // for now, set destinations of 2nd fragment to coord host/port
      // TODO: determine execution hosts first, then set destinations to those hosts
      for (int i = 0; i < request->node_request_params[1].size(); ++i) {
        DCHECK_EQ(request->node_request_params[1][i].destinations.size(), 1);
        request->node_request_params[1][i].destinations[0].host = FLAGS_host;
        request->node_request_params[1][i].destinations[0].port = FLAGS_be_port;
      }
    }
  } else {
    executor_.reset(NULL);
    obj_pool_.reset(new ObjectPool());
  }

  query_profile_.reset(new RuntimeProfile(obj_pool(), "Query " + PrintId(query_id_)));
  // register coordinator's fragment profile now, before those of the backends,
  // so it shows up at the top
  agg_throughput_profile_ = obj_pool()->Add(
      new RuntimeProfile(obj_pool(), "AggregateThroughput"));
  query_profile_->AddChild(agg_throughput_profile_);
  if (executor_.get() != NULL) {
    query_profile_->AddChild(executor_->profile());
    CreateThroughputCounters(executor_->profile(), &coordinator_throughput_counters_);
  }
  SCOPED_TIMER(query_profile_->total_time_counter());

  // Start non-coord fragments on remote nodes;
  // fragment_requests[i] can receive data from fragment_requests[>i],
  // so start fragments in ascending order.
  int backend_num = 0;
  int first_remote_fragment_idx = request->has_coordinator_fragment ? 1 : 0;
  for (int i = first_remote_fragment_idx; i < request->fragment_requests.size(); ++i) {
    DCHECK(exec_env_ != NULL);
    // TODO: change this in the following way:
    // * add locations to request->node_request_params.scan_ranges
    // * pass in request->node_request_params and let the scheduler figure out where
    // we should be doing those scans, rather than the frontend
    vector<pair<string, int> > hosts;
    RETURN_IF_ERROR(
        exec_env_->scheduler()->GetHosts(
            request->data_locations[i - first_remote_fragment_idx], &hosts));
    DCHECK_EQ(hosts.size(), request->node_request_params[i].size());

    // start individual plan exec requests
    for (int j = 0; j < hosts.size(); ++j) {
      // assign fragment id that's unique across all fragment executions;
      // backend_num + 1: backend_num starts at 0, and the coordinator fragment 
      // is already assigned the query id
      TUniqueId fragment_id;
      fragment_id.hi = request->query_id.hi;
      DCHECK_LT(request->query_id.lo, numeric_limits<int64_t>::max() - backend_num - 1);
      fragment_id.lo = request->query_id.lo + backend_num + 1;

      // TODO: pool of pre-formatted BackendExecStates?
      BackendExecState* exec_state =
          obj_pool()->Add(new BackendExecState(fragment_id, backend_num, hosts[j],
                &request->fragment_requests[i],
                &request->node_request_params[i][j], obj_pool()));
      DCHECK_EQ(backend_exec_states_.size(), backend_num);
      backend_exec_states_.push_back(exec_state);
      // add profile now; we'll get periodic updates once it starts executing
      query_profile_->AddChild(exec_state->profile);
      ++backend_num;
    }
    PrintBackendInfo();
  }
  num_remaining_backends_ = backend_exec_states_.size();

  CreateThroughputCounters(request->fragment_requests);
  
  // Issue all rpcs in parallel
  Status fragments_exec_status = ParallelExecutor::Exec(
      bind<Status>(mem_fn(&Coordinator::ExecRemoteFragment), this, _1), 
      reinterpret_cast<void**>(&backend_exec_states_[0]), backend_exec_states_.size());

  // Clear state in backend_exec_states_ that is only guaranteed to exist for the
  // duration of this function
  for (int i = 0; i < backend_exec_states_.size(); ++i) {
    backend_exec_states_[i]->exec_request = NULL;
    backend_exec_states_[i]->exec_params = NULL;
  }

  if (!fragments_exec_status.ok()) {
    DCHECK(query_status_.ok());  // nobody should have been able to cancel
    query_status_ = fragments_exec_status;
    // tear down running fragments and return
    CancelInternal();
    return fragments_exec_status;
  }

  return Status::OK;
}

Status Coordinator::GetStatus() {
  lock_guard<mutex> l(lock_);
  return query_status_;
}

Status Coordinator::UpdateStatus(const Status& status) {
  lock_guard<mutex> l(lock_);
  // nothing to update
  if (status.ok()) return query_status_;

  // don't override an error status; also, cancellation has already started
  if (!query_status_.ok()) return query_status_;

  query_status_ = status;
  CancelInternal();
  return query_status_;
}

Status Coordinator::Wait() {
  lock_guard<mutex> l(wait_lock_);
  if (has_called_wait_) return Status::OK;
  has_called_wait_ = true;
  if (executor_.get() != NULL) {
    // Open() may block
    return UpdateStatus(executor_->Open());
  } else {
    unique_lock<mutex> l(lock_);
    VLOG_QUERY << "Coordinator waiting for backends to finish, " 
               << num_remaining_backends_ << " remaining";
    while (num_remaining_backends_ > 0 && query_status_.ok()) {
      backend_completion_cv_.wait(l);
    }
    VLOG_QUERY << "All backends finished or error.";
  }
  return query_status_;
}

Status Coordinator::GetNext(RowBatch** batch, RuntimeState* state) {
  VLOG_ROW << "GetNext() query_id=" << query_id_;
  DCHECK(has_called_wait_);
  SCOPED_TIMER(query_profile_->total_time_counter());
  if (executor_.get() == NULL) {
    // If there is no local fragment, we produce no output, and execution will
    // have finished after Wait.
    *batch = NULL;
    return GetStatus();
  }

  // do not acquire lock_ here, otherwise we could block and prevent an async
  // Cancel() from proceeding
  Status status = executor_->GetNext(batch);

  // if there was an error, we need to return the query's error status rather than
  // the status we just got back from the local executor (which may well be CANCELLED
  // in that case).
  RETURN_IF_ERROR(UpdateStatus(status));

  if (*batch == NULL) {
    if (VLOG_QUERY_IS_ON) {
      stringstream s;
      query_profile_->PrettyPrint(&s);
      VLOG_QUERY << "cumulative profile for query_id=" << query_id_ << "\n"
                 << s.str();
    }
  } else {
    exec_stats_->num_rows_ += (*batch)->num_rows();
  }
  return Status::OK;
}

void Coordinator::PrintBackendInfo() {
  accumulator_set<int64_t, features<tag::min, tag::max, tag::mean, tag::variance> > acc;
  for (int i = 0; i < backend_exec_states_.size(); ++i) {
    acc(backend_exec_states_[i]->total_split_size);
  }
  double min = accumulators::min(acc);
  double max = accumulators::max(acc);
  // TODO: including the median doesn't compile, looks like some includes are missing
  //double median = accumulators::median(acc);
  double mean = accumulators::mean(acc);
  double stddev = sqrt(accumulators::variance(acc));
  VLOG_QUERY << "split sizes for " << backend_exec_states_.size() << " backends:"
             << " min: " << PrettyPrinter::Print(min, TCounterType::BYTES)
             << ", max: " << PrettyPrinter::Print(max, TCounterType::BYTES)
             //<< ", median: " << PrettyPrinter::Print(median, TCounterType::BYTES)
             << ", avg: " << PrettyPrinter::Print(mean, TCounterType::BYTES)
             << ", stddev: " << PrettyPrinter::Print(stddev, TCounterType::BYTES);
  if (VLOG_FILE_IS_ON) {
    for (int i = 0; i < backend_exec_states_.size(); ++i) {
      BackendExecState* exec_state = backend_exec_states_[i];
      VLOG_FILE << "data volume for host " << exec_state->hostport.first
                << ":" << exec_state->hostport.second << ": "
                << PrettyPrinter::Print(
                  exec_state->total_split_size, TCounterType::BYTES);
    }
  }
}

void Coordinator::CreateThroughputCounters(RuntimeProfile* profile, 
    ThroughputCounterMap* throughput_counters) {
  vector<RuntimeProfile*> children;
  profile->GetAllChildren(&children);
  for (int i = 0; i < children.size(); ++i) {
    RuntimeProfile* p = children[i];
    RuntimeProfile::Counter* c = p->GetCounter(ScanNode::THROUGHPUT_COUNTER);
    if (c == NULL) {
      // this is not a scan node
      continue;
    }
    PlanNodeId id = ExecNode::GetNodeIdFromProfile(p);
    if (id < 0) {
      VLOG_QUERY << "couldn't extract a node id from profile name: " << p->name();
      continue;  // couldn't extract an id from profile name
    }
    (*throughput_counters)[id] = c;
  }
}

void Coordinator::CreateThroughputCounters(
    const vector<TPlanExecRequest>& fragment_requests) {
  for (int i = 0; i < fragment_requests.size(); ++i) {
    if (!fragment_requests[i].__isset.plan_fragment) continue;
    const vector<TPlanNode>& nodes = fragment_requests[i].plan_fragment.nodes;
    for (int j = 0; j < nodes.size(); ++j) {
      const TPlanNode& node = nodes[j];
      if (node.node_type != TPlanNodeType::HDFS_SCAN_NODE
          && node.node_type != TPlanNodeType::HBASE_SCAN_NODE) {
        continue;
      }

      stringstream s;
      s << PrintPlanNodeType(node.node_type) << " (id=" << node.node_id << ")";
      agg_throughput_profile_->AddDerivedCounter(s.str(), TCounterType::BYTES_PER_SECOND,
          bind<int64_t>(mem_fn(&Coordinator::ComputeTotalThroughput),
                        this, node.node_id));
    }
  }
}

int64_t Coordinator::ComputeTotalThroughput(int node_id) {
  int64_t value = 0;
  for (int i = 0; i < backend_exec_states_.size(); ++i) {
    BackendExecState* exec_state = backend_exec_states_[i];
    value += exec_state->GetNodeThroughput(node_id);
  }
  // Add up the local fragment throughput counter
  ThroughputCounterMap::iterator it = coordinator_throughput_counters_.find(node_id);
  if (it != coordinator_throughput_counters_.end()) {
    value += it->second->value();
  }
  return value;
}

Status Coordinator::ExecRemoteFragment(void* exec_state_arg) {
  BackendExecState* exec_state = reinterpret_cast<BackendExecState*>(exec_state_arg);
  VLOG_FILE << "making rpc: ExecPlanFragment query_id=" << query_id_
            << " fragment_id=" << exec_state->fragment_id
            << " host=" << exec_state->hostport.first
            << " port=" << exec_state->hostport.second;
  lock_guard<mutex> l(exec_state->lock);

  // this client needs to have been released when this function finishes
  ImpalaInternalServiceClient* backend_client;
  RETURN_IF_ERROR(exec_env_->client_cache()->GetClient(
      exec_state->hostport, &backend_client));
  DCHECK(backend_client != NULL);

  TExecPlanFragmentParams params;
  params.protocol_version = ImpalaInternalServiceVersion::V1;
  // TODO: is this yet another copy? find a way to avoid those.
  params.__set_request(*exec_state->exec_request);
  params.request.fragment_id = exec_state->fragment_id;
  params.__set_params(*exec_state->exec_params);
  params.coord.host = FLAGS_host;
  params.coord.port = FLAGS_be_port;
  params.__isset.coord = true;
  params.__set_backend_num(exec_state->backend_num);

  TExecPlanFragmentResult thrift_result;
  try {
    backend_client->ExecPlanFragment(thrift_result, params);
  } catch (TTransportException& e) {
    stringstream msg;
    msg << "ExecPlanRequest rpc query_id=" << query_id_
        << " fragment_id=" << exec_state->fragment_id 
        << " failed: " << e.what();
    VLOG_QUERY << msg.str();
    exec_state->status = Status(msg.str());
    exec_env_->client_cache()->ReleaseClient(backend_client);
    return exec_state->status;
  }
  exec_state->status = thrift_result.status;
  exec_env_->client_cache()->ReleaseClient(backend_client);
  if (exec_state->status.ok()) exec_state->initiated = true;
  return exec_state->status;
}

void Coordinator::Cancel() {
  lock_guard<mutex> l(lock_);
  // if the query status indicates an error, cancellation has already been initiated
  if (!query_status_.ok()) return;
  // prevent others from cancelling a second time
  query_status_ = Status::CANCELLED;
  CancelInternal();
}

void Coordinator::CancelInternal() {
  VLOG_QUERY << "Cancel() query_id=" << query_id_;
  DCHECK(!query_status_.ok());

  // cancel local fragment
  if (executor_.get() != NULL) executor_->Cancel();

  for (int i = 0; i < backend_exec_states_.size(); ++i) {
    BackendExecState* exec_state = backend_exec_states_[i];

    // lock each exec_state individually to synchronize correctly with
    // UpdateFragmentExecStatus() (which doesn't get the global lock_
    // to set its status)
    lock_guard<mutex> l(exec_state->lock);

    // no need to cancel if we already know it terminated w/ an error status
    if (!exec_state->status.ok()) continue;

    // set an error status to make sure we only cancel this once
    exec_state->status = Status::CANCELLED;

    // Nothing to cancel if the exec rpc was not sent
    if (!exec_state->initiated) continue;

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
      VLOG_QUERY << "sending CancelPlanFragment rpc for fragment_id="
                 << exec_state->fragment_id;
      backend_client->CancelPlanFragment(res, params);
    } catch (TTransportException& e) {
      stringstream msg;
      msg << "CancelPlanFragment rpc query_id=" << query_id_
          << " fragment_id=" << exec_state->fragment_id 
          << " failed: " << e.what();
      // make a note of the error status, but keep on cancelling the other fragments
      exec_state->status.AddErrorMsg(msg.str());
      exec_env_->client_cache()->ReleaseClient(backend_client);
      continue;
    }
    if (res.status.status_code != TStatusCode::OK) {
      exec_state->status.AddErrorMsg(algorithm::join(res.status.error_msgs, "; "));
    }

    exec_env_->client_cache()->ReleaseClient(backend_client);
  }

  // notify that we completed with an error
  backend_completion_cv_.notify_all();
}

Status Coordinator::UpdateFragmentExecStatus(const TReportExecStatusParams& params) {
  VLOG_FILE << "UpdateFragmentExecStatus() query_id=" << query_id_
            << " status=" << params.status.status_code
            << " done=" << (params.done ? "true" : "false");
  if (params.backend_num >= backend_exec_states_.size()) {
    return Status(TStatusCode::INTERNAL_ERROR, "unknown backend number");
  }
  BackendExecState* exec_state = backend_exec_states_[params.backend_num];

  const TRuntimeProfileTree& cumulative_profile = params.profile;
  Status status(params.status);
  {
    lock_guard<mutex> l(exec_state->lock);
    // make sure we don't go from error status to OK
    DCHECK(!status.ok() || exec_state->status.ok())
        << "fragment is transitioning from error status to OK:"
        << " query_id=" << query_id_ << " fragment_id=" << exec_state->fragment_id
        << " status=" << exec_state->status.GetErrorMsg();
    exec_state->status = status;
    exec_state->done = params.done;
    exec_state->profile->Update(cumulative_profile);
    if (!exec_state->profile_created) {
      CreateThroughputCounters(exec_state->profile, &exec_state->throughput_counters);
    }
    exec_state->profile_created = true;

    // Gather any metastore operations to be made
    if (params.done && params.__isset.partitions_to_create) {
      exec_state->updated_hdfs_partitions.insert(
          params.partitions_to_create.begin(), params.partitions_to_create.end());
    }
  }

  if (VLOG_QUERY_IS_ON) {
    stringstream s;
    exec_state->profile->PrettyPrint(&s);
    VLOG_QUERY << "profile for query_id=" << query_id_
               << " fragment_id=" << exec_state->fragment_id << "\n" << s.str();
  }
  // also print the cumulative profile
  // TODO: fix the coordinator/PlanFragmentExecutor, so this isn't needed
  if (VLOG_FILE_IS_ON) {
    stringstream s;
    query_profile_->PrettyPrint(&s);
    VLOG_FILE << "cumulative profile for query_id=" << query_id_ 
              << "\n" << s.str();
  }

  // for now, abort the query if we see any error
  // (UpdateStatus() initiates cancellation, if it hasn't already been initiated)
  if (!status.ok()) {
    UpdateStatus(status);
    return Status::OK;
  }

  if (params.done) {
    lock_guard<mutex> l(lock_);
    DCHECK_GT(num_remaining_backends_, 0);
    VLOG_QUERY << "Backend " << params.backend_num << " completed, " 
               << num_remaining_backends_ - 1 << " remaining: query_id=" << query_id_;
    if (VLOG_QUERY_IS_ON && num_remaining_backends_ > 1) {
      // print host/port info for the first backend that's still in progress as a
      // debugging aid for backend deadlocks
      for (int i = 0; i < backend_exec_states_.size(); ++i) {
        BackendExecState* exec_state = backend_exec_states_[i];
        lock_guard<mutex> l2(exec_state->lock);
        if (!exec_state->done) {
          VLOG_QUERY << "query_id=" << query_id_ << ": first in-progress backend: "
                     << exec_state->hostport.first << ":" << exec_state->hostport.second;
          break;
        }
      }
    }
    if (--num_remaining_backends_ == 0) {
      backend_completion_cv_.notify_all();
    }
  }

  return Status::OK;
}

const RowDescriptor& Coordinator::row_desc() const {
  DCHECK(executor_.get() != NULL);
  return executor_->row_desc();
}

RuntimeState* Coordinator::runtime_state() {
  return executor_.get() == NULL ? NULL : executor_->runtime_state();
}

ObjectPool* Coordinator::obj_pool() {
  return executor_.get() == NULL ? obj_pool_.get() : 
    executor_->runtime_state()->obj_pool();
}

bool Coordinator::PrepareCatalogUpdate(TCatalogUpdate* catalog_update) {
  // Assume we are called only after all fragments have completed
  DCHECK(has_called_wait_);
  BOOST_FOREACH(BackendExecState* exec_state, backend_exec_states_) {
    catalog_update->created_partitions.insert(
        exec_state->updated_hdfs_partitions.begin(),
        exec_state->updated_hdfs_partitions.end());
  }

  // If the coordinator has a sink, could be that it creates partitions as well
  RuntimeState* state = runtime_state();
  if (state != NULL) {
    // Coordinator and slaves can't both create partitions
    DCHECK(state->updated_hdfs_partitions().size() == 0
        || catalog_update->created_partitions.size() == 0);
    catalog_update->created_partitions.insert(
        state->updated_hdfs_partitions().begin(),
        state->updated_hdfs_partitions().end());
  }

  return catalog_update->created_partitions.size() != 0;
}

}
