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


#include "runtime/fragment-instance-state.h"

#include <sstream>
#include <thrift/protocol/TDebugProtocol.h>
#include <boost/thread/locks.hpp>
#include <boost/thread/thread_time.hpp>
#include <boost/thread/lock_guard.hpp>
#include <boost/date_time/posix_time/posix_time_types.hpp>

#include "common/names.h"
#include "codegen/llvm-codegen.h"
#include "exec/plan-root-sink.h"
#include "exec/exec-node.h"
#include "exec/hdfs-scan-node-base.h"  // for PerVolumeStats
#include "exec/exchange-node.h"
#include "exec/scan-node.h"
#include "runtime/exec-env.h"
#include "runtime/backend-client.h"
#include "runtime/runtime-filter-bank.h"
#include "runtime/client-cache.h"
#include "runtime/data-stream-mgr.h"
#include "runtime/runtime-state.h"
#include "runtime/query-state.h"
#include "runtime/query-state.h"
#include "runtime/mem-tracker.h"
#include "runtime/row-batch.h"
#include "scheduling/query-schedule.h"
#include "util/debug-util.h"
#include "util/container-util.h"
#include "util/periodic-counter-updater.h"
#include "gen-cpp/ImpalaInternalService_types.h"

DEFINE_int32(status_report_interval, 5, "interval between profile reports; in seconds");

using namespace impala;
using namespace apache::thrift;

const string FragmentInstanceState::PER_HOST_PEAK_MEM_COUNTER = "PerHostPeakMemUsage";
const string FragmentInstanceState::FINST_THREAD_GROUP_NAME = "fragment-execution";

static const string OPEN_TIMER_NAME = "OpenTime";
static const string PREPARE_TIMER_NAME = "PrepareTime";
static const string EXEC_TIMER_NAME = "ExecTime";

FragmentInstanceState::FragmentInstanceState(
    QueryState* query_state, const TPlanFragmentCtx& fragment_ctx,
    const TPlanFragmentInstanceCtx& instance_ctx)
  : query_state_(query_state),
    fragment_ctx_(fragment_ctx),
    instance_ctx_(instance_ctx) {
}

Status FragmentInstanceState::Exec() {
  Status status = Prepare();
  DCHECK(runtime_state_ != nullptr);  // we need to guarantee at least that
  prepared_promise_.Set(status);
  if (!status.ok()) {
    opened_promise_.Set(status);
    goto done;
  }
  status = Open();
  opened_promise_.Set(status);
  if (!status.ok()) goto done;

  {
    // Must go out of scope before Finalize(), otherwise counter will not be
    // updated by time final profile is sent.
    SCOPED_TIMER(profile()->total_time_counter());
    SCOPED_TIMER(ADD_TIMER(timings_profile_, EXEC_TIMER_NAME));
    status = ExecInternal();
  }

done:
  UpdateState(StateEvent::EXEC_END);
  // call this before Close() to make sure the thread token got released
  Finalize(status);
  Close();
  return status;
}

void FragmentInstanceState::Cancel() {
  // Make sure Prepare() finished. We don't care about the status since the query is
  // being cancelled.
  discard_result(WaitForPrepare());

  // Ensure that the sink is closed from both sides. Although in ordinary executions we
  // rely on the consumer to do this, in error cases the consumer may not be able to send
  // CloseConsumer() (see IMPALA-4348 for an example).
  if (root_sink_ != nullptr) root_sink_->CloseConsumer();

  DCHECK(runtime_state_ != nullptr);
  runtime_state_->set_is_cancelled();
  runtime_state_->stream_mgr()->Cancel(runtime_state_->fragment_instance_id());
}

Status FragmentInstanceState::Prepare() {
  DCHECK(!prepared_promise_.IsSet());
  VLOG(2) << "fragment_instance_ctx:\n" << ThriftDebugString(instance_ctx_);

  // Do not call RETURN_IF_ERROR or explicitly return before this line,
  // runtime_state_ != nullptr is a postcondition of this function.
  runtime_state_ = obj_pool()->Add(new RuntimeState(
      query_state_, fragment_ctx_, instance_ctx_, ExecEnv::GetInstance()));

  // total_time_counter() is in the runtime_state_ so start it up now.
  SCOPED_TIMER(profile()->total_time_counter());
  timings_profile_ =
      RuntimeProfile::Create(obj_pool(), "Fragment Instance Lifecycle Timings");
  profile()->AddChild(timings_profile_);
  SCOPED_TIMER(ADD_TIMER(timings_profile_, PREPARE_TIMER_NAME));

  // Events that are tracked in a separate timeline for each fragment instance, relative
  // to the startup of the query state.
  event_sequence_ =
      profile()->AddEventSequence("Fragment Instance Lifecycle Event Timeline");
  event_sequence_->Start(query_state_->fragment_events_start_time());
  UpdateState(StateEvent::PREPARE_START);

  RETURN_IF_ERROR(runtime_state_->InitFilterBank(
      fragment_ctx_.fragment.runtime_filters_reservation_bytes));

  // Reserve one main thread from the pool
  runtime_state_->resource_pool()->AcquireThreadToken();
  avg_thread_tokens_ = profile()->AddSamplingCounter("AverageThreadTokens",
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

  // set up plan
  RETURN_IF_ERROR(ExecNode::CreateTree(
      runtime_state_, fragment_ctx_.fragment.plan, query_state_->desc_tbl(),
      &exec_tree_));
  runtime_state_->set_fragment_root_id(exec_tree_->id());
  if (instance_ctx_.__isset.debug_options) {
    ExecNode::SetDebugOptions(instance_ctx_.debug_options, exec_tree_);
  }

  // set #senders of exchange nodes before calling Prepare()
  vector<ExecNode*> exch_nodes;
  exec_tree_->CollectNodes(TPlanNodeType::EXCHANGE_NODE, &exch_nodes);
  for (ExecNode* exch_node : exch_nodes) {
    DCHECK_EQ(exch_node->type(), TPlanNodeType::EXCHANGE_NODE);
    int num_senders =
        FindWithDefault(instance_ctx_.per_exch_num_senders, exch_node->id(), 0);
    DCHECK_GT(num_senders, 0);
    static_cast<ExchangeNode*>(exch_node)->set_num_senders(num_senders);
  }

  // set scan ranges
  vector<ExecNode*> scan_nodes;
  vector<TScanRangeParams> no_scan_ranges;
  exec_tree_->CollectScanNodes(&scan_nodes);
  for (ExecNode* scan_node: scan_nodes) {
    const vector<TScanRangeParams>& scan_ranges = FindWithDefault(
        instance_ctx_.per_node_scan_ranges, scan_node->id(), no_scan_ranges);
    static_cast<ScanNode*>(scan_node)->SetScanRanges(scan_ranges);
  }

  RuntimeProfile::Counter* prepare_timer =
      ADD_CHILD_TIMER(timings_profile_, "ExecTreePrepareTime", PREPARE_TIMER_NAME);
  {
    SCOPED_TIMER(prepare_timer);
    RETURN_IF_ERROR(exec_tree_->Prepare(runtime_state_));
  }
  PrintVolumeIds();

  // prepare sink_
  DCHECK(fragment_ctx_.fragment.__isset.output_sink);
  RETURN_IF_ERROR(DataSink::Create(fragment_ctx_, instance_ctx_, exec_tree_->row_desc(),
      runtime_state_, &sink_));
  RETURN_IF_ERROR(sink_->Prepare(runtime_state_, runtime_state_->instance_mem_tracker()));
  RuntimeProfile* sink_profile = sink_->profile();
  if (sink_profile != nullptr) profile()->AddChild(sink_profile);

  if (fragment_ctx_.fragment.output_sink.type == TDataSinkType::PLAN_ROOT_SINK) {
    root_sink_ = reinterpret_cast<PlanRootSink*>(sink_);
    // Release the thread token on the root fragment instance. This fragment spends most
    // of the time waiting and doing very little work. Holding on to the token causes
    // underutilization of the machine. If there are 12 queries on this node, that's 12
    // tokens reserved for no reason.
    ReleaseThreadToken();
  }

  // set up profile counters
  profile()->AddChild(exec_tree_->runtime_profile());
  rows_produced_counter_ =
      ADD_COUNTER(profile(), "RowsProduced", TUnit::UNIT);
  per_host_mem_usage_ =
      ADD_COUNTER(profile(), PER_HOST_PEAK_MEM_COUNTER, TUnit::BYTES);

  row_batch_.reset(
      new RowBatch(exec_tree_->row_desc(), runtime_state_->batch_size(),
        runtime_state_->instance_mem_tracker()));
  VLOG(2) << "plan_root=\n" << exec_tree_->DebugString();

  // We need to start the profile-reporting thread before calling Open(),
  // since it may block.
  if (FLAGS_status_report_interval > 0) {
    string thread_name = Substitute("profile-report (finst:$0)", PrintId(instance_id()));
    unique_lock<mutex> l(report_thread_lock_);
    RETURN_IF_ERROR(Thread::Create(FragmentInstanceState::FINST_THREAD_GROUP_NAME,
        thread_name, [this]() { this->ReportProfileThread(); }, &report_thread_, true));
    // Make sure the thread started up, otherwise ReportProfileThread() might get into
    // a race with StopReportThread().
    while (!report_thread_active_) report_thread_started_cv_.Wait(l);
  }

  return Status::OK();
}

Status FragmentInstanceState::Open() {
  DCHECK(prepared_promise_.IsSet());
  DCHECK(!opened_promise_.IsSet());
  SCOPED_TIMER(profile()->total_time_counter());
  SCOPED_TIMER(ADD_TIMER(timings_profile_, OPEN_TIMER_NAME));
  SCOPED_THREAD_COUNTER_MEASUREMENT(runtime_state_->total_thread_statistics());

  if (runtime_state_->ShouldCodegen()) {
    UpdateState(StateEvent::CODEGEN_START);
    RETURN_IF_ERROR(runtime_state_->CreateCodegen());
    exec_tree_->Codegen(runtime_state_);
    // It shouldn't be fatal to fail codegen. However, until IMPALA-4233 is fixed,
    // ScalarFnCall has no fall back to interpretation when codegen fails so propagates
    // the error status for now.
    RETURN_IF_ERROR(runtime_state_->CodegenScalarFns());

    LlvmCodeGen* codegen = runtime_state_->codegen();
    DCHECK(codegen != nullptr);
    RETURN_IF_ERROR(codegen->FinalizeModule());
  }

  {
    UpdateState(StateEvent::OPEN_START);
    SCOPED_TIMER(ADD_CHILD_TIMER(timings_profile_, "ExecTreeOpenTime", OPEN_TIMER_NAME));
    RETURN_IF_ERROR(exec_tree_->Open(runtime_state_));
  }
  return sink_->Open(runtime_state_);
}

Status FragmentInstanceState::ExecInternal() {
  RuntimeProfile::Counter* plan_exec_timer =
      ADD_CHILD_TIMER(timings_profile_, "ExecTreeExecTime", EXEC_TIMER_NAME);
  SCOPED_THREAD_COUNTER_MEASUREMENT(runtime_state_->total_thread_statistics());
  bool exec_tree_complete = false;
  UpdateState(StateEvent::WAITING_FOR_FIRST_BATCH);
  do {
    Status status;
    row_batch_->Reset();
    {
      SCOPED_TIMER(plan_exec_timer);
      RETURN_IF_ERROR(
          exec_tree_->GetNext(runtime_state_, row_batch_.get(), &exec_tree_complete));
    }
    UpdateState(StateEvent::BATCH_PRODUCED);
    if (VLOG_ROW_IS_ON) row_batch_->VLogRows("FragmentInstanceState::ExecInternal()");
    COUNTER_ADD(rows_produced_counter_, row_batch_->num_rows());
    RETURN_IF_ERROR(sink_->Send(runtime_state_, row_batch_.get()));
    UpdateState(StateEvent::BATCH_SENT);
  } while (!exec_tree_complete);

  UpdateState(StateEvent::LAST_BATCH_SENT);
  // Flush the sink *before* stopping the report thread. Flush may need to add some
  // important information to the last report that gets sent. (e.g. table sinks record the
  // files they have written to in this method)
  RETURN_IF_ERROR(sink_->FlushFinal(runtime_state()));
  return Status::OK();
}

void FragmentInstanceState::Close() {
  DCHECK(!report_thread_active_);
  DCHECK(runtime_state_ != nullptr);

  // guard against partially-finished Prepare()
  if (sink_ != nullptr) sink_->Close(runtime_state_);

  // Stop updating profile counters in background.
  profile()->StopPeriodicCounters();

  // We need to delete row_batch_ here otherwise we can't delete the instance_mem_tracker_
  // in runtime_state_->ReleaseResources().
  // TODO: do not delete mem trackers in Close()/ReleaseResources(), they are part of
  // the control structures we need to preserve until the underlying QueryState
  // disappears.
  row_batch_.reset();
  if (exec_tree_ != nullptr) exec_tree_->Close(runtime_state_);
  runtime_state_->ReleaseResources();

  // Sanity timer checks
#ifndef NDEBUG
  if (profile() != nullptr && timings_profile_ != nullptr) {
    int64_t total_time = profile()->total_time_counter()->value();
    int64_t other_time = 0;
    for (auto& name: {PREPARE_TIMER_NAME, OPEN_TIMER_NAME, EXEC_TIMER_NAME}) {
      RuntimeProfile::Counter* counter = timings_profile_->GetCounter(name);
      if (counter != nullptr) other_time += counter->value();
    }
    // TODO: IMPALA-4631: Occasionally we see other_time = total_time + 1 for some reason
    // we don't yet understand, so add 1 to total_time to avoid DCHECKing in that case.
    DCHECK_LE(other_time, total_time + 1);
  }
#endif
}

void FragmentInstanceState::ReportProfileThread() {
  VLOG_FILE << "ReportProfileThread(): instance_id=" << PrintId(instance_id());
  unique_lock<mutex> l(report_thread_lock_);
  // tell Prepare() that we started
  report_thread_active_ = true;
  report_thread_started_cv_.NotifyOne();

  // Jitter the reporting time of remote fragments by a random amount between
  // 0 and the report_interval.  This way, the coordinator doesn't get all the
  // updates at once so its better for contention as well as smoother progress
  // reporting.
  int report_fragment_offset = rand() % FLAGS_status_report_interval;
  // We don't want to wait longer than it takes to run the entire fragment.
  stop_report_thread_cv_.WaitFor(l, report_fragment_offset * MICROS_PER_SEC);

  while (report_thread_active_) {
    // timed_wait can return because the timeout occurred or the condition variable
    // was signaled.  We can't rely on its return value to distinguish between the
    // two cases (e.g. there is a race here where the wait timed out but before grabbing
    // the lock, the condition variable was signaled).  Instead, we will use an external
    // flag, report_thread_active_, to coordinate this.
    stop_report_thread_cv_.WaitFor(l, FLAGS_status_report_interval * MICROS_PER_SEC);

    if (!report_thread_active_) break;
    SendReport(false, Status::OK());
  }

  VLOG_FILE << "exiting reporting thread: instance_id=" << instance_id();
}

void FragmentInstanceState::SendReport(bool done, const Status& status) {
  DCHECK(status.ok() || done);
  DCHECK(runtime_state_ != nullptr);

  if (VLOG_FILE_IS_ON) {
    VLOG_FILE << "Reporting " << (done ? "final " : "") << "profile for instance "
        << runtime_state_->fragment_instance_id();
    stringstream ss;
    profile()->PrettyPrint(&ss);
    VLOG_FILE << ss.str();
  }

  // Update the counter for the peak per host mem usage.
  if (per_host_mem_usage_ != nullptr) {
    per_host_mem_usage_->Set(runtime_state()->query_mem_tracker()->peak_consumption());
  }

  query_state_->ReportExecStatus(done, status, this);
}

void FragmentInstanceState::UpdateState(const StateEvent event)
{
  TFInstanceExecState::type current_state = current_state_.Load();
  TFInstanceExecState::type next_state = current_state;
  switch (event) {
    case StateEvent::PREPARE_START:
      DCHECK_EQ(current_state, TFInstanceExecState::WAITING_FOR_EXEC);
      next_state = TFInstanceExecState::WAITING_FOR_PREPARE;
      break;

    case StateEvent::CODEGEN_START:
      DCHECK_EQ(current_state, TFInstanceExecState::WAITING_FOR_PREPARE);
      event_sequence_->MarkEvent("Prepare Finished");
      next_state = TFInstanceExecState::WAITING_FOR_CODEGEN;
      break;

    case StateEvent::OPEN_START:
      if (current_state == TFInstanceExecState::WAITING_FOR_PREPARE) {
        event_sequence_->MarkEvent("Prepare Finished");
      } else {
        DCHECK_EQ(current_state, TFInstanceExecState::WAITING_FOR_CODEGEN);
      }
      next_state = TFInstanceExecState::WAITING_FOR_OPEN;
      break;

    case StateEvent::WAITING_FOR_FIRST_BATCH:
      DCHECK_EQ(current_state, TFInstanceExecState::WAITING_FOR_OPEN);
      event_sequence_->MarkEvent("Open Finished");
      next_state = TFInstanceExecState::WAITING_FOR_FIRST_BATCH;
      break;

    case StateEvent::BATCH_PRODUCED:
      if (UNLIKELY(current_state == TFInstanceExecState::WAITING_FOR_FIRST_BATCH)) {
        event_sequence_->MarkEvent("First Batch Produced");
        next_state = TFInstanceExecState::FIRST_BATCH_PRODUCED;
      } else {
        DCHECK_EQ(current_state, TFInstanceExecState::PRODUCING_DATA);
      }
      break;

    case StateEvent::BATCH_SENT:
      if (UNLIKELY(current_state == TFInstanceExecState::FIRST_BATCH_PRODUCED)) {
        event_sequence_->MarkEvent("First Batch Sent");
        next_state = TFInstanceExecState::PRODUCING_DATA;
      } else {
        DCHECK_EQ(current_state, TFInstanceExecState::PRODUCING_DATA);
      }
      break;

    case StateEvent::LAST_BATCH_SENT:
      DCHECK_EQ(current_state, TFInstanceExecState::PRODUCING_DATA);
      next_state = TFInstanceExecState::LAST_BATCH_SENT;
      break;

    case StateEvent::EXEC_END:
      // Allow abort in all states to make error handling easier.
      event_sequence_->MarkEvent("ExecInternal Finished");
      next_state = TFInstanceExecState::FINISHED;
      break;

    default:
      DCHECK(false) << "Unexpected Event: " << static_cast<int>(event);
      break;
  }
  // current_state_ is an AtomicEnum to add memory barriers for concurrent reads by the
  // profile reporting thread. This method is the only one updating it and is not
  // meant to be thread safe.
  if (next_state != current_state) current_state_.Store(next_state);
}

void FragmentInstanceState::StopReportThread() {
  if (!report_thread_active_) return;
  {
    lock_guard<mutex> l(report_thread_lock_);
    report_thread_active_ = false;
  }
  stop_report_thread_cv_.NotifyOne();
  report_thread_->Join();
}

void FragmentInstanceState::Finalize(const Status& status) {
  if (fragment_ctx_.fragment.output_sink.type != TDataSinkType::PLAN_ROOT_SINK) {
    // if we haven't already release this thread token in Prepare(), release it now
    ReleaseThreadToken();
  }
  StopReportThread();
  // It's safe to send final report now that the reporting thread is stopped.
  SendReport(true, status);
}

void FragmentInstanceState::ReleaseThreadToken() {
  DCHECK(runtime_state_ != nullptr);
  DCHECK(runtime_state_->resource_pool() != nullptr);
  runtime_state_->resource_pool()->ReleaseThreadToken(true);
  if (avg_thread_tokens_ != nullptr) {
    PeriodicCounterUpdater::StopSamplingCounter(avg_thread_tokens_);
  }
  if (thread_usage_sampled_counter_ != nullptr) {
    PeriodicCounterUpdater::StopTimeSeriesCounter(thread_usage_sampled_counter_);
  }
}

Status FragmentInstanceState::WaitForPrepare() {
  return prepared_promise_.Get();
}

bool FragmentInstanceState::IsPrepared() {
  return prepared_promise_.IsSet();
}

Status FragmentInstanceState::WaitForOpen() {
  return opened_promise_.Get();
}

void FragmentInstanceState::PublishFilter(const TPublishFilterParams& params) {
  VLOG_FILE << "PublishFilter(): instance_id=" << PrintId(instance_id())
            << " filter_id=" << params.filter_id;
  // Wait until Prepare() is done, so we know that the filter bank is set up.
  if (!WaitForPrepare().ok()) return;
  runtime_state_->filter_bank()->PublishGlobalFilter(params);
}

string FragmentInstanceState::ExecStateToString(const TFInstanceExecState::type state) {
  // Labels to send to the debug webpages to display the current state to the user.
  static const string finstance_state_labels[] = {
      "Waiting for Exec",         // WAITING_FOR_EXEC
      "Waiting for Codegen",      // WAITING_FOR_CODEGEN
      "Waiting for Prepare",      // WAITING_FOR_PREPARE
      "Waiting for First Batch",  // WAITING_FOR_OPEN
      "Waiting for First Batch",  // WAITING_FOR_FIRST_BATCH
      "First batch produced",     // FIRST_BATCH_PRODUCED
      "Producing Data",           // PRODUCING_DATA
      "Last batch sent",          // LAST_BATCH_SENT
      "Finished"                  // FINISHED
  };
  /// Make sure we have a label for every possible state.
  static_assert(
      sizeof(finstance_state_labels) / sizeof(char*) == TFInstanceExecState::FINISHED + 1,
      "");

  DCHECK_LT(state, sizeof(finstance_state_labels) / sizeof(char*))
      << "Unknown instance state";
  return finstance_state_labels[state];
}

const TQueryCtx& FragmentInstanceState::query_ctx() const {
  return query_state_->query_ctx();
}

ObjectPool* FragmentInstanceState::obj_pool() {
  return query_state_->obj_pool();
}

RuntimeProfile* FragmentInstanceState::profile() const {
  return runtime_state_->runtime_profile();
}

void FragmentInstanceState::PrintVolumeIds() {
  if (instance_ctx_.per_node_scan_ranges.empty()) return;

  HdfsScanNodeBase::PerVolumeStats per_volume_stats;
  for (const PerNodeScanRanges::value_type& entry: instance_ctx_.per_node_scan_ranges) {
    HdfsScanNodeBase::UpdateHdfsSplitStats(entry.second, &per_volume_stats);
  }

  stringstream str;
  HdfsScanNodeBase::PrintHdfsSplitStats(per_volume_stats, &str);
  profile()->AddInfoString(HdfsScanNodeBase::HDFS_SPLIT_STATS_DESC, str.str());
  VLOG_FILE
      << "Hdfs split stats (<volume id>:<# splits>/<split lengths>) for query="
      << query_id() << ":\n" << str.str();
}
