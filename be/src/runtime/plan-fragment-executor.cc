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

#include "runtime/plan-fragment-executor.h"

#include <thrift/protocol/TDebugProtocol.h>
#include <boost/date_time/posix_time/posix_time_types.hpp>
#include <boost/unordered_map.hpp>
#include <gutil/strings/substitute.h>

#include "codegen/llvm-codegen.h"
#include "common/logging.h"
#include "common/object-pool.h"
#include "exec/data-sink.h"
#include "exec/exchange-node.h"
#include "exec/exec-node.h"
#include "exec/hbase-table-scanner.h"
#include "exec/hdfs-scan-node.h"
#include "exec/plan-root-sink.h"
#include "exec/scan-node.h"
#include "exprs/expr.h"
#include "runtime/data-stream-mgr.h"
#include "runtime/descriptors.h"
#include "runtime/mem-tracker.h"
#include "runtime/row-batch.h"
#include "runtime/query-state.h"
#include "runtime/runtime-filter-bank.h"
#include "runtime/exec-env.h"
#include "util/container-util.h"
#include "runtime/runtime-state.h"
#include "util/cpu-info.h"
#include "util/debug-util.h"
#include "util/mem-info.h"
#include "util/parse-util.h"
#include "util/periodic-counter-updater.h"
#include "util/pretty-printer.h"

DEFINE_bool(serialize_batch, false, "serialize and deserialize each returned row batch");
DEFINE_int32(status_report_interval, 5, "interval between profile reports; in seconds");

#include "common/names.h"

namespace posix_time = boost::posix_time;
using boost::get_system_time;
using boost::system_time;
using namespace apache::thrift;
using namespace strings;

namespace impala {

const string PlanFragmentExecutor::PER_HOST_PEAK_MEM_COUNTER = "PerHostPeakMemUsage";

namespace {
const string OPEN_TIMER_NAME = "OpenTime";
const string PREPARE_TIMER_NAME = "PrepareTime";
const string EXEC_TIMER_NAME = "ExecTime";
}

PlanFragmentExecutor::PlanFragmentExecutor(
    const ReportStatusCallback& report_status_cb)
  : exec_tree_(NULL),
    report_status_cb_(report_status_cb),
    report_thread_active_(false),
    closed_(false),
    has_thread_token_(false),
    timings_profile_(NULL),
    root_sink_(NULL),
    is_prepared_(false),
    is_cancelled_(false),
    per_host_mem_usage_(NULL),
    rows_produced_counter_(NULL),
    average_thread_tokens_(NULL),
    mem_usage_sampled_counter_(NULL),
    thread_usage_sampled_counter_(NULL) {}

PlanFragmentExecutor::~PlanFragmentExecutor() {
  DCHECK(!is_prepared_ || closed_);
  // at this point, the report thread should have been stopped
  DCHECK(!report_thread_active_);
}

Status PlanFragmentExecutor::Prepare(
    QueryState* query_state, const TDescriptorTable& desc_tbl,
    const TPlanFragmentCtx& fragment_ctx, const TPlanFragmentInstanceCtx& instance_ctx) {
  Status status = PrepareInternal(query_state, desc_tbl, fragment_ctx, instance_ctx);
  prepared_promise_.Set(status);
  if (!status.ok()) FragmentComplete(status);
  return status;
}

Status PlanFragmentExecutor::WaitForOpen() {
  DCHECK(prepared_promise_.IsSet()) << "Prepare() must complete before WaitForOpen()";
  RETURN_IF_ERROR(prepared_promise_.Get());
  return opened_promise_.Get();
}

Status PlanFragmentExecutor::PrepareInternal(
    QueryState* qs, const TDescriptorTable& tdesc_tbl,
    const TPlanFragmentCtx& fragment_ctx, const TPlanFragmentInstanceCtx& instance_ctx) {
  lock_guard<mutex> l(prepare_lock_);
  DCHECK(!is_prepared_);

  if (is_cancelled_) return Status::CANCELLED;
  is_prepared_ = true;

  // TODO: Break this method up.
  query_id_ = qs->query_ctx().query_id;

  VLOG_QUERY << "Prepare(): instance_id="
             << PrintId(instance_ctx.fragment_instance_id);
  VLOG(2) << "fragment_instance_ctx:\n" << ThriftDebugString(instance_ctx);

  // Prepare() must not return before runtime_state_ is set if is_prepared_ was
  // set. Having runtime_state_.get() != NULL is a postcondition of this method in that
  // case. Do not call RETURN_IF_ERROR or explicitly return before this line.
  runtime_state_.reset(
      new RuntimeState(qs, fragment_ctx, instance_ctx, ExecEnv::GetInstance()));

  // total_time_counter() is in the runtime_state_ so start it up now.
  SCOPED_TIMER(profile()->total_time_counter());
  timings_profile_ = obj_pool()->Add(
      new RuntimeProfile(obj_pool(), "Fragment Instance Lifecycle Timings"));
  profile()->AddChild(timings_profile_);
  SCOPED_TIMER(ADD_TIMER(timings_profile_, PREPARE_TIMER_NAME));

  // reservation or a query option.
  int64_t bytes_limit = -1;
  if (runtime_state_->query_options().__isset.mem_limit &&
      runtime_state_->query_options().mem_limit > 0) {
    bytes_limit = runtime_state_->query_options().mem_limit;
    VLOG_QUERY << "Using query memory limit from query options: "
               << PrettyPrinter::Print(bytes_limit, TUnit::BYTES);
  }

  DCHECK(!instance_ctx.request_pool.empty());
  RETURN_IF_ERROR(runtime_state_->CreateBlockMgr());
  runtime_state_->InitFilterBank();

  // Reserve one main thread from the pool
  runtime_state_->resource_pool()->AcquireThreadToken();
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
  RETURN_IF_ERROR(DescriptorTbl::Create(obj_pool(), tdesc_tbl, &desc_tbl));
  runtime_state_->set_desc_tbl(desc_tbl);
  VLOG_QUERY << "descriptor table for fragment=" << instance_ctx.fragment_instance_id
             << "\n" << desc_tbl->DebugString();

  // set up plan
  RETURN_IF_ERROR(ExecNode::CreateTree(
      runtime_state_.get(), fragment_ctx.fragment.plan, *desc_tbl, &exec_tree_));
  runtime_state_->set_fragment_root_id(exec_tree_->id());

  if (instance_ctx.__isset.debug_node_id) {
    DCHECK(instance_ctx.__isset.debug_action);
    DCHECK(instance_ctx.__isset.debug_phase);
    ExecNode::SetDebugOptions(instance_ctx.debug_node_id, instance_ctx.debug_phase,
        instance_ctx.debug_action, exec_tree_);
  }

  // set #senders of exchange nodes before calling Prepare()
  vector<ExecNode*> exch_nodes;
  exec_tree_->CollectNodes(TPlanNodeType::EXCHANGE_NODE, &exch_nodes);
  for (ExecNode* exch_node : exch_nodes) {
    DCHECK_EQ(exch_node->type(), TPlanNodeType::EXCHANGE_NODE);
    int num_senders =
        FindWithDefault(instance_ctx.per_exch_num_senders, exch_node->id(), 0);
    DCHECK_GT(num_senders, 0);
    static_cast<ExchangeNode*>(exch_node)->set_num_senders(num_senders);
  }

  // set scan ranges
  vector<ExecNode*> scan_nodes;
  vector<TScanRangeParams> no_scan_ranges;
  exec_tree_->CollectScanNodes(&scan_nodes);
  for (int i = 0; i < scan_nodes.size(); ++i) {
    ScanNode* scan_node = static_cast<ScanNode*>(scan_nodes[i]);
    const vector<TScanRangeParams>& scan_ranges = FindWithDefault(
        instance_ctx.per_node_scan_ranges, scan_node->id(), no_scan_ranges);
    scan_node->SetScanRanges(scan_ranges);
  }

  RuntimeState* state = runtime_state();
  RuntimeProfile::Counter* prepare_timer =
      ADD_CHILD_TIMER(timings_profile_, "ExecTreePrepareTime", PREPARE_TIMER_NAME);
  {
    SCOPED_TIMER(prepare_timer);
    RETURN_IF_ERROR(exec_tree_->Prepare(state));
  }

  PrintVolumeIds(instance_ctx.per_node_scan_ranges);

  DCHECK(fragment_ctx.fragment.__isset.output_sink);
  RETURN_IF_ERROR(
      DataSink::CreateDataSink(obj_pool(), fragment_ctx.fragment.output_sink,
          fragment_ctx.fragment.output_exprs, instance_ctx, exec_tree_->row_desc(),
          &sink_));
  RETURN_IF_ERROR(
      sink_->Prepare(runtime_state(), runtime_state_->instance_mem_tracker()));

  RuntimeProfile* sink_profile = sink_->profile();
  if (sink_profile != NULL) {
    profile()->AddChild(sink_profile);
  }

  if (fragment_ctx.fragment.output_sink.type == TDataSinkType::PLAN_ROOT_SINK) {
    root_sink_ = reinterpret_cast<PlanRootSink*>(sink_.get());
    // Release the thread token on the root fragment instance. This fragment spends most
    // of the time waiting and doing very little work. Holding on to the token causes
    // underutilization of the machine. If there are 12 queries on this node, that's 12
    // tokens reserved for no reason.
    ReleaseThreadToken();
  }

  if (state->ShouldCodegen()) {
    RETURN_IF_ERROR(state->CreateCodegen());
    exec_tree_->Codegen(state);
    // It shouldn't be fatal to fail codegen. However, until IMPALA-4233 is fixed,
    // ScalarFnCall has no fall back to interpretation when codegen fails so propagates
    // the error status for now.
    RETURN_IF_ERROR(state->CodegenScalarFns());
  }

  // set up profile counters
  profile()->AddChild(exec_tree_->runtime_profile());
  rows_produced_counter_ =
      ADD_COUNTER(profile(), "RowsProduced", TUnit::UNIT);
  per_host_mem_usage_ =
      ADD_COUNTER(profile(), PER_HOST_PEAK_MEM_COUNTER, TUnit::BYTES);

  row_batch_.reset(new RowBatch(exec_tree_->row_desc(), state->batch_size(),
      state->instance_mem_tracker()));
  VLOG(2) << "plan_root=\n" << exec_tree_->DebugString();
  return Status::OK();
}

Status PlanFragmentExecutor::OptimizeLlvmModule() {
  if (!runtime_state_->ShouldCodegen()) return Status::OK();
  LlvmCodeGen* codegen = runtime_state_->codegen();
  DCHECK(codegen != NULL);
  return codegen->FinalizeModule();
}

void PlanFragmentExecutor::PrintVolumeIds(
    const PerNodeScanRanges& per_node_scan_ranges) {
  if (per_node_scan_ranges.empty()) return;

  HdfsScanNode::PerVolumnStats per_volume_stats;
  for (const PerNodeScanRanges::value_type& entry: per_node_scan_ranges) {
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
  DCHECK(prepared_promise_.IsSet() && prepared_promise_.Get().ok());
  Status status;
  {
    SCOPED_TIMER(profile()->total_time_counter());
    SCOPED_TIMER(ADD_TIMER(timings_profile_, OPEN_TIMER_NAME));
    VLOG_QUERY << "Open(): instance_id=" << runtime_state_->fragment_instance_id();
    status = OpenInternal();
  }
  if (!status.ok()) FragmentComplete(status);
  opened_promise_.Set(status);
  return status;
}

Status PlanFragmentExecutor::OpenInternal() {
  SCOPED_THREAD_COUNTER_MEASUREMENT(runtime_state_->total_thread_statistics());
  RETURN_IF_ERROR(
      runtime_state_->desc_tbl().PrepareAndOpenPartitionExprs(runtime_state_.get()));

  // We need to start the profile-reporting thread before calling exec_tree_->Open(),
  // since it may block.
  if (!report_status_cb_.empty() && FLAGS_status_report_interval > 0) {
    unique_lock<mutex> l(report_thread_lock_);
    report_thread_.reset(
        new Thread("plan-fragment-executor", "report-profile",
            &PlanFragmentExecutor::ReportProfileThread, this));
    // Make sure the thread started up, otherwise ReportProfileThread() might get into
    // a race with StopReportThread().
    while (!report_thread_active_) report_thread_started_cv_.wait(l);
  }

  RETURN_IF_ERROR(OptimizeLlvmModule());

  {
    SCOPED_TIMER(ADD_CHILD_TIMER(timings_profile_, "ExecTreeOpenTime", OPEN_TIMER_NAME));
    RETURN_IF_ERROR(exec_tree_->Open(runtime_state_.get()));
  }
  return sink_->Open(runtime_state_.get());
}

Status PlanFragmentExecutor::Exec() {
  DCHECK(opened_promise_.IsSet() && opened_promise_.Get().ok());
  Status status;
  {
    // Must go out of scope before FragmentComplete(), otherwise counter will not be
    // updated by time final profile is sent.
    SCOPED_TIMER(profile()->total_time_counter());
    SCOPED_TIMER(ADD_TIMER(timings_profile_, EXEC_TIMER_NAME));
    status = ExecInternal();

    if (!status.ok() && !status.IsCancelled() && !status.IsMemLimitExceeded()) {
      // Log error message in addition to returning in Status. Queries that do not fetch
      // results (e.g. insert) may not receive the message directly and can only retrieve
      // the log.
      runtime_state_->LogError(status.msg());
    }
  }
  FragmentComplete(status);
  return status;
}

Status PlanFragmentExecutor::ExecInternal() {
  RuntimeProfile::Counter* plan_exec_timer =
      ADD_CHILD_TIMER(timings_profile_, "ExecTreeExecTime", EXEC_TIMER_NAME);
  SCOPED_THREAD_COUNTER_MEASUREMENT(runtime_state_->total_thread_statistics());
  bool exec_tree_complete = false;
  do {
    Status status;
    row_batch_->Reset();
    {
      SCOPED_TIMER(plan_exec_timer);
      status = exec_tree_->GetNext(
          runtime_state_.get(), row_batch_.get(), &exec_tree_complete);
    }
    if (VLOG_ROW_IS_ON) row_batch_->VLogRows("PlanFragmentExecutor::ExecInternal()");
    COUNTER_ADD(rows_produced_counter_, row_batch_->num_rows());
    RETURN_IF_ERROR(status);
    RETURN_IF_ERROR(sink_->Send(runtime_state(), row_batch_.get()));
  } while (!exec_tree_complete);

  // Flush the sink *before* stopping the report thread. Flush may need to add some
  // important information to the last report that gets sent. (e.g. table sinks record the
  // files they have written to in this method)
  RETURN_IF_ERROR(sink_->FlushFinal(runtime_state()));
  return Status::OK();
}

void PlanFragmentExecutor::ReportProfileThread() {
  VLOG_FILE << "ReportProfileThread(): instance_id="
            << runtime_state_->fragment_instance_id();
  DCHECK(!report_status_cb_.empty());
  unique_lock<mutex> l(report_thread_lock_);
  // tell Open() that we started
  report_thread_active_ = true;
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
    SendReport(false, Status::OK());
  }

  VLOG_FILE << "exiting reporting thread: instance_id="
      << runtime_state_->fragment_instance_id();
}

void PlanFragmentExecutor::SendReport(bool done, const Status& status) {
  DCHECK(status.ok() || done);
  if (report_status_cb_.empty()) return;

  // Update the counter for the peak per host mem usage.
  if (per_host_mem_usage_ != nullptr) {
    per_host_mem_usage_->Set(runtime_state()->query_mem_tracker()->peak_consumption());
  }

  // This will send a report even if we are cancelled.  If the query completed correctly
  // but fragments still need to be cancelled (e.g. limit reached), the coordinator will
  // be waiting for a final report and profile.
  RuntimeProfile* prof = is_prepared_ ? profile() : nullptr;
  report_status_cb_(status, prof, done);
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

void PlanFragmentExecutor::FragmentComplete(const Status& status) {
  ReleaseThreadToken();
  StopReportThread();
  // It's safe to send final report now that the reporting thread is stopped.
  SendReport(true, status);
}

void PlanFragmentExecutor::Cancel() {
  VLOG_QUERY << "Cancelling fragment instance...";
  lock_guard<mutex> l(prepare_lock_);
  is_cancelled_ = true;
  if (!is_prepared_) {
    VLOG_QUERY << "Cancel() called before Prepare()";
    return;
  }

  // Ensure that the sink is closed from both sides. Although in ordinary executions we
  // rely on the consumer to do this, in error cases the consumer may not be able to send
  // CloseConsumer() (see IMPALA-4348 for an example).
  if (root_sink_ != nullptr) root_sink_->CloseConsumer();

  DCHECK(runtime_state_ != NULL);
  VLOG_QUERY << "Cancel(): instance_id=" << runtime_state_->fragment_instance_id();
  runtime_state_->set_is_cancelled(true);
  runtime_state_->stream_mgr()->Cancel(runtime_state_->fragment_instance_id());
}

RuntimeProfile* PlanFragmentExecutor::profile() {
  return runtime_state_->runtime_profile();
}

void PlanFragmentExecutor::ReleaseThreadToken() {
  if (has_thread_token_) {
    has_thread_token_ = false;
    runtime_state_->resource_pool()->ReleaseThreadToken(true);
    PeriodicCounterUpdater::StopSamplingCounter(average_thread_tokens_);
    PeriodicCounterUpdater::StopTimeSeriesCounter(
        thread_usage_sampled_counter_);
  }
}

void PlanFragmentExecutor::Close() {
  DCHECK(!has_thread_token_);
  DCHECK(!report_thread_active_);

  if (closed_) return;
  if (!is_prepared_) return;
  if (sink_.get() != nullptr) sink_->Close(runtime_state());

  row_batch_.reset();

  // Prepare should always have been called, and so runtime_state_ should be set
  DCHECK(prepared_promise_.IsSet());
  if (exec_tree_ != NULL) exec_tree_->Close(runtime_state_.get());
  if (mem_usage_sampled_counter_ != NULL) {
    // This counter references runtime_state_->instance_mem_tracker() so must be
    // stopped before calling ReleaseResources().
    PeriodicCounterUpdater::StopTimeSeriesCounter(mem_usage_sampled_counter_);
    mem_usage_sampled_counter_ = NULL;
  }
  // Sanity timer checks
#ifndef NDEBUG
  int64_t total_time = profile()->total_time_counter()->value();
  int64_t other_time = 0;
  for (auto& name: {PREPARE_TIMER_NAME, OPEN_TIMER_NAME, EXEC_TIMER_NAME}) {
    RuntimeProfile::Counter* counter = timings_profile_->GetCounter(name);
    if (counter != nullptr) other_time += counter->value();
  }
  // TODO: IMPALA-4631: Occasionally we see other_time = total_time + 1 for some reason
  // we don't yet understand, so add 1 to total_time to avoid DCHECKing in that case.
  DCHECK_LE(other_time, total_time + 1);
#endif
  runtime_state_->ReleaseResources();

  closed_ = true;
}

}
