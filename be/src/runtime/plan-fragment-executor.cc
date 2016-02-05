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

#include "runtime/plan-fragment-executor.h"

#include <thrift/protocol/TDebugProtocol.h>
#include <boost/date_time/posix_time/posix_time_types.hpp>
#include <boost/unordered_map.hpp>
#include <boost/foreach.hpp>
#include <gutil/strings/substitute.h>

#include "codegen/llvm-codegen.h"
#include "common/logging.h"
#include "common/object-pool.h"
#include "exec/data-sink.h"
#include "exec/exec-node.h"
#include "exec/exchange-node.h"
#include "exec/scan-node.h"
#include "exec/hdfs-scan-node.h"
#include "exec/hbase-table-scanner.h"
#include "exprs/expr.h"
#include "runtime/descriptors.h"
#include "runtime/data-stream-mgr.h"
#include "runtime/row-batch.h"
#include "runtime/mem-tracker.h"
#include "util/cgroups-mgr.h"
#include "util/cpu-info.h"
#include "util/debug-util.h"
#include "util/container-util.h"
#include "util/parse-util.h"
#include "util/mem-info.h"
#include "util/periodic-counter-updater.h"
#include "util/llama-util.h"
#include "util/pretty-printer.h"

DEFINE_bool(serialize_batch, false, "serialize and deserialize each returned row batch");
DEFINE_int32(status_report_interval, 5, "interval between profile reports; in seconds");
DECLARE_bool(enable_rm);

#include "common/names.h"

namespace posix_time = boost::posix_time;
using boost::get_system_time;
using boost::system_time;
using namespace apache::thrift;
using namespace strings;

namespace impala {

const string PlanFragmentExecutor::PER_HOST_PEAK_MEM_COUNTER = "PerHostPeakMemUsage";

PlanFragmentExecutor::PlanFragmentExecutor(ExecEnv* exec_env,
    const ReportStatusCallback& report_status_cb) :
    exec_env_(exec_env), plan_(NULL), report_status_cb_(report_status_cb),
    report_thread_active_(false), done_(false), closed_(false),
    has_thread_token_(false), is_prepared_(false), is_cancelled_(false),
    average_thread_tokens_(NULL), mem_usage_sampled_counter_(NULL),
    thread_usage_sampled_counter_(NULL) {
}

PlanFragmentExecutor::~PlanFragmentExecutor() {
  Close();
  if (is_prepared_ && runtime_state_->query_resource_mgr() != NULL) {
    exec_env_->resource_broker()->UnregisterQueryResourceMgr(query_id_);
  }
  // at this point, the report thread should have been stopped
  DCHECK(!report_thread_active_);
}

Status PlanFragmentExecutor::Prepare(const TExecPlanFragmentParams& request) {
  lock_guard<mutex> l(prepare_lock_);
  DCHECK(!is_prepared_);
  if (is_cancelled_) return Status::CANCELLED;

  is_prepared_ = true;
  // TODO: Break this method up.
  fragment_sw_.Start();
  const TPlanFragmentExecParams& params = request.params;
  query_id_ = request.fragment_instance_ctx.query_ctx.query_id;

  VLOG_QUERY << "Prepare(): query_id=" << PrintId(query_id_) << " instance_id="
             << PrintId(request.fragment_instance_ctx.fragment_instance_id);
  VLOG(2) << "params:\n" << ThriftDebugString(params);

  if (request.__isset.reserved_resource) {
    VLOG_QUERY << "Executing fragment in reserved resource:\n"
               << request.reserved_resource;
  }

  string cgroup = "";
  if (FLAGS_enable_rm && request.__isset.reserved_resource) {
    cgroup = exec_env_->cgroups_mgr()->UniqueIdToCgroup(PrintId(query_id_, "_"));
  }

  // Prepare() must not return before runtime_state_ is set if is_prepared_ was
  // set. Having runtime_state_.get() != NULL is a postcondition of this method in that
  // case. Do not call RETURN_IF_ERROR or explicitly return before this line.
  runtime_state_.reset(new RuntimeState(request, cgroup, exec_env_));

  // total_time_counter() is in the runtime_state_ so start it up now.
  SCOPED_TIMER(profile()->total_time_counter());

  // Register after setting runtime_state_ to ensure proper cleanup.
  if (FLAGS_enable_rm && !cgroup.empty() && request.__isset.reserved_resource) {
    bool is_first;
    RETURN_IF_ERROR(exec_env_->cgroups_mgr()->RegisterFragment(
        request.fragment_instance_ctx.fragment_instance_id, cgroup, &is_first));
    // The first fragment using cgroup sets the cgroup's CPU shares based on the reserved
    // resource.
    if (is_first) {
      DCHECK(request.__isset.reserved_resource);
      int32_t cpu_shares = exec_env_->cgroups_mgr()->VirtualCoresToCpuShares(
          request.reserved_resource.v_cpu_cores);
      RETURN_IF_ERROR(exec_env_->cgroups_mgr()->SetCpuShares(cgroup, cpu_shares));
    }
  }

  // TODO: Find the reservation id when the resource request is not set
  if (FLAGS_enable_rm && request.__isset.reserved_resource) {
    TUniqueId reservation_id;
    reservation_id << request.reserved_resource.reservation_id;

    // TODO: Combine this with RegisterFragment() etc.
    QueryResourceMgr* res_mgr;
    bool is_first = exec_env_->resource_broker()->GetQueryResourceMgr(query_id_,
        reservation_id, request.local_resource_address, &res_mgr);
    DCHECK(res_mgr != NULL);
    runtime_state_->SetQueryResourceMgr(res_mgr);
    if (is_first) {
      runtime_state_->query_resource_mgr()->InitVcoreAcquisition(
          request.reserved_resource.v_cpu_cores);
    }
  }

  // reservation or a query option.
  int64_t bytes_limit = -1;
  if (runtime_state_->query_options().__isset.mem_limit &&
      runtime_state_->query_options().mem_limit > 0) {
    bytes_limit = runtime_state_->query_options().mem_limit;
    VLOG_QUERY << "Using query memory limit from query options: "
               << PrettyPrinter::Print(bytes_limit, TUnit::BYTES);
  }

  int64_t rm_reservation_size_bytes = -1;
  if (request.__isset.reserved_resource && request.reserved_resource.memory_mb > 0) {
    rm_reservation_size_bytes =
        static_cast<int64_t>(request.reserved_resource.memory_mb) * 1024L * 1024L;
    // Queries that use more than the hard limit will be killed, so it's not useful to
    // have a reservation larger than the hard limit. Clamp reservation bytes limit to the
    // hard limit (if it exists).
    if (rm_reservation_size_bytes > bytes_limit && bytes_limit != -1) {
      runtime_state_->LogError(ErrorMsg(TErrorCode::FRAGMENT_EXECUTOR,
          PrettyPrinter::PrintBytes(rm_reservation_size_bytes),
          PrettyPrinter::PrintBytes(bytes_limit)));
      rm_reservation_size_bytes = bytes_limit;
    }
    VLOG_QUERY << "Using RM reservation memory limit from resource reservation: "
               << PrettyPrinter::Print(rm_reservation_size_bytes, TUnit::BYTES);
  }

  DCHECK(!params.request_pool.empty());
  runtime_state_->InitMemTrackers(query_id_, &params.request_pool,
      bytes_limit, rm_reservation_size_bytes);
  RETURN_IF_ERROR(runtime_state_->CreateBlockMgr());

  // Reserve one main thread from the pool
  runtime_state_->resource_pool()->AcquireThreadToken();
  if (runtime_state_->query_resource_mgr() != NULL) {
    runtime_state_->query_resource_mgr()->NotifyThreadUsageChange(1);
  }
  has_thread_token_ = true;

  average_thread_tokens_ = profile()->AddSamplingCounter("AverageThreadTokens",
      bind<int64_t>(mem_fn(&ThreadResourceMgr::ResourcePool::num_threads),
          runtime_state_->resource_pool()));
  mem_usage_sampled_counter_ = profile()->AddTimeSeriesCounter("MemoryUsage",
      TUnit::BYTES,
      bind<int64_t>(mem_fn(&MemTracker::consumption),
          runtime_state_->instance_mem_tracker()));
  thread_usage_sampled_counter_ = profile()->AddTimeSeriesCounter("ThreadUsage",
      TUnit::UNIT,
      bind<int64_t>(mem_fn(&ThreadResourceMgr::ResourcePool::num_threads),
          runtime_state_->resource_pool()));

  // set up desc tbl
  DescriptorTbl* desc_tbl = NULL;
  DCHECK(request.__isset.desc_tbl);
  RETURN_IF_ERROR(
      DescriptorTbl::Create(obj_pool(), request.desc_tbl, &desc_tbl));
  runtime_state_->set_desc_tbl(desc_tbl);
  VLOG_QUERY << "descriptor table for fragment="
             << request.fragment_instance_ctx.fragment_instance_id
             << "\n" << desc_tbl->DebugString();

  // set up plan
  DCHECK(request.__isset.fragment);
  RETURN_IF_ERROR(
      ExecNode::CreateTree(obj_pool(), request.fragment.plan, *desc_tbl, &plan_));
  runtime_state_->set_fragment_root_id(plan_->id());

  if (request.params.__isset.debug_node_id) {
    DCHECK(request.params.__isset.debug_action);
    DCHECK(request.params.__isset.debug_phase);
    ExecNode::SetDebugOptions(request.params.debug_node_id,
        request.params.debug_phase, request.params.debug_action, plan_);
  }

  // set #senders of exchange nodes before calling Prepare()
  vector<ExecNode*> exch_nodes;
  plan_->CollectNodes(TPlanNodeType::EXCHANGE_NODE, &exch_nodes);
  BOOST_FOREACH(ExecNode* exch_node, exch_nodes)
  {
    DCHECK_EQ(exch_node->type(), TPlanNodeType::EXCHANGE_NODE);
    int num_senders = FindWithDefault(params.per_exch_num_senders,
        exch_node->id(), 0);
    DCHECK_GT(num_senders, 0);
    static_cast<ExchangeNode*>(exch_node)->set_num_senders(num_senders);
  }

  // set scan ranges
  vector<ExecNode*> scan_nodes;
  vector<TScanRangeParams> no_scan_ranges;
  plan_->CollectScanNodes(&scan_nodes);
  for (int i = 0; i < scan_nodes.size(); ++i) {
    ScanNode* scan_node = static_cast<ScanNode*>(scan_nodes[i]);
    const vector<TScanRangeParams>& scan_ranges = FindWithDefault(
        params.per_node_scan_ranges, scan_node->id(), no_scan_ranges);
    scan_node->SetScanRanges(scan_ranges);
  }

  RuntimeProfile::Counter* prepare_timer = ADD_TIMER(profile(), "PrepareTime");
  {
    SCOPED_TIMER(prepare_timer);
    RETURN_IF_ERROR(plan_->Prepare(runtime_state_.get()));
  }

  PrintVolumeIds(params.per_node_scan_ranges);

  // set up sink, if required
  if (request.fragment.__isset.output_sink) {
    RETURN_IF_ERROR(DataSink::CreateDataSink(
        obj_pool(), request.fragment.output_sink, request.fragment.output_exprs,
        params, row_desc(), &sink_));
    RETURN_IF_ERROR(sink_->Prepare(runtime_state()));

    RuntimeProfile* sink_profile = sink_->profile();
    if (sink_profile != NULL) {
      profile()->AddChild(sink_profile);
    }
  } else {
    sink_.reset(NULL);
  }

  // set up profile counters
  profile()->AddChild(plan_->runtime_profile());
  rows_produced_counter_ =
      ADD_COUNTER(profile(), "RowsProduced", TUnit::UNIT);
  per_host_mem_usage_ =
      ADD_COUNTER(profile(), PER_HOST_PEAK_MEM_COUNTER, TUnit::BYTES);

  row_batch_.reset(new RowBatch(plan_->row_desc(), runtime_state_->batch_size(),
        runtime_state_->instance_mem_tracker()));
  VLOG(2) << "plan_root=\n" << plan_->DebugString();
  return Status::OK();
}

void PlanFragmentExecutor::OptimizeLlvmModule() {
  if (!runtime_state_->codegen_created()) return;
  LlvmCodeGen* codegen;
  Status status = runtime_state_->GetCodegen(&codegen, /* initalize */ false);
  DCHECK(status.ok());
  DCHECK(codegen != NULL);
  status = codegen->FinalizeModule();
  if (!status.ok()) {
    stringstream ss;
    ss << "Error with codegen for this query: " << status.GetDetail();
    runtime_state_->LogError(ErrorMsg(TErrorCode::GENERAL, ss.str()));
  }
}

void PlanFragmentExecutor::PrintVolumeIds(
    const PerNodeScanRanges& per_node_scan_ranges) {
  if (per_node_scan_ranges.empty()) return;

  HdfsScanNode::PerVolumnStats per_volume_stats;
  BOOST_FOREACH(const PerNodeScanRanges::value_type& entry, per_node_scan_ranges) {
    HdfsScanNode::UpdateHdfsSplitStats(entry.second, &per_volume_stats);
  }

  stringstream str;

  HdfsScanNode::PrintHdfsSplitStats(per_volume_stats, &str);
  profile()->AddInfoString(HdfsScanNode::HDFS_SPLIT_STATS_DESC, str.str());
  VLOG_FILE
      << "Hdfs split stats (<volume id>:<# splits>/<split lengths>) for query="
      << query_id_ << ":\n" << str.str();
}

Status PlanFragmentExecutor::Open() {
  VLOG_QUERY << "Open(): instance_id="
      << runtime_state_->fragment_instance_id();
  // we need to start the profile-reporting thread before calling Open(), since it
  // may block
  if (!report_status_cb_.empty() && FLAGS_status_report_interval > 0) {
    unique_lock<mutex> l(report_thread_lock_);
    report_thread_.reset(
        new Thread("plan-fragment-executor", "report-profile",
            &PlanFragmentExecutor::ReportProfile, this));
    // make sure the thread started up, otherwise ReportProfile() might get into a race
    // with StopReportThread()
    report_thread_started_cv_.wait(l);
    report_thread_active_ = true;
  }

  OptimizeLlvmModule();

  Status status = OpenInternal();
  if (!status.ok() && !status.IsCancelled() && !status.IsMemLimitExceeded()) {
    // Log error message in addition to returning in Status. Queries that do not
    // fetch results (e.g. insert) may not receive the message directly and can
    // only retrieve the log.
    runtime_state_->LogError(status.msg());
  }
  UpdateStatus(status);
  return status;
}

Status PlanFragmentExecutor::OpenInternal() {
  {
    SCOPED_TIMER(profile()->total_time_counter());
    RETURN_IF_ERROR(plan_->Open(runtime_state_.get()));
  }
  if (sink_.get() == NULL) return Status::OK();

  RETURN_IF_ERROR(sink_->Open(runtime_state_.get()));
  // If there is a sink, do all the work of driving it here, so that
  // when this returns the query has actually finished
  while (!done_) {
    RowBatch* batch;
    RETURN_IF_ERROR(GetNextInternal(&batch));
    if (batch == NULL) break;

    if (VLOG_ROW_IS_ON) {
      VLOG_ROW << "OpenInternal: #rows=" << batch->num_rows();
      for (int i = 0; i < batch->num_rows(); ++i) {
        VLOG_ROW << PrintRow(batch->GetRow(i), row_desc());
      }
    }
    SCOPED_TIMER(profile()->total_time_counter());
    RETURN_IF_ERROR(sink_->Send(runtime_state(), batch, done_));
  }

  // Close the sink *before* stopping the report thread. Close may need to add some
  // important information to the last report that gets sent. (e.g. table sinks record the
  // files they have written to in this method)
  //
  // The coordinator report channel waits until all backends are either in error or have
  // returned a status report with done = true, so tearing down any data stream state (a
  // separate channel) in Close is safe.
  SCOPED_TIMER(profile()->total_time_counter());
  sink_->Close(runtime_state());
  done_ = true;

  FragmentComplete();
  return Status::OK();
}

void PlanFragmentExecutor::ReportProfile() {
  VLOG_FILE << "ReportProfile(): instance_id=" << runtime_state_->fragment_instance_id();
  DCHECK(!report_status_cb_.empty());
  unique_lock<mutex> l(report_thread_lock_);
  // tell Open() that we started
  report_thread_started_cv_.notify_one();

  // Jitter the reporting time of remote fragments by a random amount between
  // 0 and the report_interval.  This way, the coordinator doesn't get all the
  // updates at once so its better for contention as well as smoother progress
  // reporting.
  int report_fragment_offset = rand() % FLAGS_status_report_interval;
  system_time timeout = get_system_time()
      + posix_time::seconds(report_fragment_offset);
  // We don't want to wait longer than it takes to run the entire fragment.
  stop_report_thread_cv_.timed_wait(l, timeout);

  while (report_thread_active_) {
    system_time timeout = get_system_time()
        + posix_time::seconds(FLAGS_status_report_interval);

    // timed_wait can return because the timeout occurred or the condition variable
    // was signaled.  We can't rely on its return value to distinguish between the
    // two cases (e.g. there is a race here where the wait timed out but before grabbing
    // the lock, the condition variable was signaled).  Instead, we will use an external
    // flag, report_thread_active_, to coordinate this.
    stop_report_thread_cv_.timed_wait(l, timeout);

    if (VLOG_FILE_IS_ON) {
      VLOG_FILE << "Reporting " << (!report_thread_active_ ? "final " : " ")
          << "profile for instance " << runtime_state_->fragment_instance_id();
      stringstream ss;
      profile()->PrettyPrint(&ss);
      VLOG_FILE << ss.str();
    }

    if (!report_thread_active_) break;

    if (completed_report_sent_.Read() == 0) {
      // No complete fragment report has been sent.
      SendReport(false);
    }
  }

  VLOG_FILE << "exiting reporting thread: instance_id="
      << runtime_state_->fragment_instance_id();
}

void PlanFragmentExecutor::SendReport(bool done) {
  if (report_status_cb_.empty()) return;

  Status status;
  {
    lock_guard<mutex> l(status_lock_);
    status = status_;
  }

  // Update the counter for the peak per host mem usage.
  per_host_mem_usage_->Set(runtime_state()->query_mem_tracker()->peak_consumption());

  // This will send a report even if we are cancelled.  If the query completed correctly
  // but fragments still need to be cancelled (e.g. limit reached), the coordinator will
  // be waiting for a final report and profile.
  report_status_cb_(status, profile(), done || !status.ok());
}

void PlanFragmentExecutor::StopReportThread() {
  if (!report_thread_active_) return;
  {
    lock_guard<mutex> l(report_thread_lock_);
    report_thread_active_ = false;
  }
  stop_report_thread_cv_.notify_one();
  report_thread_->Join();
}

// TODO: why can't we just put the total_time_counter() at the
// beginning of Open() and GetNext(). This seems to really mess
// the timer here, presumably because the data stream sender is
// multithreaded and the timer we use gets confused.
Status PlanFragmentExecutor::GetNext(RowBatch** batch) {
  VLOG_FILE << "GetNext(): instance_id="
      << runtime_state_->fragment_instance_id();
  Status status = GetNextInternal(batch);
  UpdateStatus(status);
  if (done_) {
    VLOG_QUERY << "Finished executing fragment query_id=" << PrintId(query_id_)
        << " instance_id=" << PrintId(runtime_state_->fragment_instance_id());
    FragmentComplete();
    // GetNext() uses *batch = NULL to signal the end.
    if (*batch != NULL && (*batch)->num_rows() == 0) *batch = NULL;
  }

  return status;
}

Status PlanFragmentExecutor::GetNextInternal(RowBatch** batch) {
  if (done_) {
    *batch = NULL;
    return Status::OK();
  }

  while (!done_) {
    row_batch_->Reset();
    SCOPED_TIMER(profile()->total_time_counter());
    RETURN_IF_ERROR(
        plan_->GetNext(runtime_state_.get(), row_batch_.get(), &done_));
    *batch = row_batch_.get();
    if (row_batch_->num_rows() > 0) {
      COUNTER_ADD(rows_produced_counter_, row_batch_->num_rows());
      break;
    }
  }

  return Status::OK();
}

void PlanFragmentExecutor::FragmentComplete() {
  // Check the atomic flag. If it is set, then a fragment complete report has already
  // been sent.
  bool send_report = completed_report_sent_.CompareAndSwap(0,1);

  fragment_sw_.Stop();
  int64_t cpu_and_wait_time = fragment_sw_.ElapsedTime();
  fragment_sw_ = MonotonicStopWatch();
  int64_t cpu_time = cpu_and_wait_time
      - runtime_state_->total_storage_wait_timer()->value()
      - runtime_state_->total_network_send_timer()->value()
      - runtime_state_->total_network_receive_timer()->value();
  // Timing is not perfect.
  if (cpu_time < 0) cpu_time = 0;
  runtime_state_->total_cpu_timer()->Add(cpu_time);

  ReleaseThreadToken();
  StopReportThread();
  if (send_report) SendReport(true);
}

void PlanFragmentExecutor::UpdateStatus(const Status& status) {
  if (status.ok()) return;

  bool send_report = completed_report_sent_.CompareAndSwap(0,1);

  {
    lock_guard<mutex> l(status_lock_);
    if (status_.ok()) {
      if (status.IsMemLimitExceeded()) runtime_state_->SetMemLimitExceeded();
      status_ = status;
    }
  }

  StopReportThread();
  if (send_report) SendReport(true);
}

void PlanFragmentExecutor::Cancel() {
  VLOG_QUERY << "Cancelling plan fragment...";
  lock_guard<mutex> l(prepare_lock_);
  is_cancelled_ = true;
  if (!is_prepared_) {
    VLOG_QUERY << "Cancel() called before Prepare()";
    return;
  }
  DCHECK(runtime_state_ != NULL);
  VLOG_QUERY << "Cancel(): instance_id=" << runtime_state_->fragment_instance_id();
  runtime_state_->set_is_cancelled(true);
  runtime_state_->stream_mgr()->Cancel(runtime_state_->fragment_instance_id());
}

const RowDescriptor& PlanFragmentExecutor::row_desc() {
  return plan_->row_desc();
}

RuntimeProfile* PlanFragmentExecutor::profile() {
  return runtime_state_->runtime_profile();
}

bool PlanFragmentExecutor::ReachedLimit() {
  return plan_->ReachedLimit();
}

void PlanFragmentExecutor::ReleaseThreadToken() {
  if (has_thread_token_) {
    has_thread_token_ = false;
    runtime_state_->resource_pool()->ReleaseThreadToken(true);
    if (runtime_state_->query_resource_mgr() != NULL) {
      runtime_state_->query_resource_mgr()->NotifyThreadUsageChange(-1);
    }
    PeriodicCounterUpdater::StopSamplingCounter(average_thread_tokens_);
    PeriodicCounterUpdater::StopTimeSeriesCounter(
        thread_usage_sampled_counter_);
  }
}

void PlanFragmentExecutor::Close() {
  if (closed_) return;
  row_batch_.reset();
  // Prepare may not have been called, which sets runtime_state_
  if (runtime_state_.get() != NULL) {
    if (runtime_state_->query_resource_mgr() != NULL) {
      exec_env_->cgroups_mgr()->UnregisterFragment(
          runtime_state_->fragment_instance_id(), runtime_state_->cgroup());
    }
    if (plan_ != NULL) plan_->Close(runtime_state_.get());
    if (sink_.get() != NULL) sink_->Close(runtime_state());
    BOOST_FOREACH(DiskIoMgr::RequestContext* context,
        *runtime_state_->reader_contexts()) {
      runtime_state_->io_mgr()->UnregisterContext(context);
    }
    exec_env_->thread_mgr()->UnregisterPool(runtime_state_->resource_pool());
  }
  if (mem_usage_sampled_counter_ != NULL) {
    PeriodicCounterUpdater::StopTimeSeriesCounter(mem_usage_sampled_counter_);
    mem_usage_sampled_counter_ = NULL;
  }
  closed_ = true;
}

}
