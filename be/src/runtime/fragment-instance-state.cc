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
#include "exec/hdfs-scan-node-base.h"
#include "exec/exchange-node.h"
#include "exec/scan-node.h"
#include "runtime/exec-env.h"
#include "runtime/backend-client.h"
#include "runtime/client-cache.h"
#include "runtime/krpc-data-stream-mgr.h"
#include "runtime/query-state.h"
#include "runtime/query-state.h"
#include "runtime/mem-tracker.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-filter-bank.h"
#include "runtime/runtime-state.h"
#include "runtime/thread-resource-mgr.h"
#include "scheduling/query-schedule.h"
#include "util/debug-util.h"
#include "util/container-util.h"
#include "util/periodic-counter-updater.h"
#include "gen-cpp/ImpalaInternalService_types.h"

using namespace impala;
using namespace apache::thrift;

const string FragmentInstanceState::PER_HOST_PEAK_MEM_COUNTER = "PerHostPeakMemUsage";
const string FragmentInstanceState::FINST_THREAD_GROUP_NAME = "fragment-execution";
const string FragmentInstanceState::FINST_THREAD_NAME_PREFIX = "exec-finstance";

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
  bool is_prepared = false;
  Status status = Prepare();
  DCHECK(runtime_state_ != nullptr);  // we need to guarantee at least that

  if (!status.ok()) {
    discard_result(opened_promise_.Set(status));
    goto done;
  }
  // Tell the managing 'QueryState' that we're done with Prepare().
  query_state_->DonePreparing();
  is_prepared = true;

  status = Open();
  discard_result(opened_promise_.Set(status));
  if (!status.ok()) goto done;

  {
    // Must go out of scope before Finalize(), otherwise counter will not be
    // updated by time final profile is sent.
    SCOPED_TIMER2(profile()->total_time_counter(),
        ADD_TIMER(timings_profile_, EXEC_TIMER_NAME));
    status = ExecInternal();
  }

done:
  // Don't transition to completion until Close() is called as some new errors may be
  // logged in RuntimeState:error_log_.
  Close();

  // Must update the fragment instance state first before updating the 'Query State'.
  // Otherwise, there is a race when reading the 'done' flag with GetStatusReport().
  // This may lead to the "final" profile being sent with the 'done' flag as false.
  DCHECK_EQ(is_prepared,
      current_state_.Load() > FInstanceExecStatePB::WAITING_FOR_PREPARE);
  UpdateState(StateEvent::EXEC_END);

  if (!status.ok()) {
    if (!is_prepared) {
      // Tell the managing 'QueryState' that we hit an error during Prepare().
      query_state_->ErrorDuringPrepare(status, instance_id());
    } else {
      // Tell the managing 'QueryState' that we hit an error during execution.
      query_state_->ErrorDuringExecute(status, instance_id());
    }
  } else {
    // Tell the managing 'QueryState' that we're done with executing.
    query_state_->DoneExecuting();
  }
  return status;
}

void FragmentInstanceState::Cancel() {
  DCHECK(runtime_state_ != nullptr);
  runtime_state_->set_is_cancelled();
  if (root_sink_ != nullptr) root_sink_->Cancel(runtime_state_);
  ExecEnv::GetInstance()->stream_mgr()->Cancel(runtime_state_->fragment_instance_id());
}

Status FragmentInstanceState::Prepare() {
  DCHECK_EQ(current_state_.Load(), FInstanceExecStatePB::WAITING_FOR_EXEC);
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

  // Reserve one main thread from the pool
  runtime_state_->resource_pool()->AcquireThreadToken();

  // Exercise debug actions at the first point where errors are possible in Prepare().
  RETURN_IF_ERROR(DebugAction(query_state_->query_options(), "FIS_IN_PREPARE"));

  RETURN_IF_ERROR(runtime_state_->InitFilterBank(
      fragment_ctx_.fragment.runtime_filters_reservation_bytes));

  avg_thread_tokens_ = profile()->AddSamplingCounter("AverageThreadTokens",
      bind<int64_t>(mem_fn(&ThreadResourcePool::num_threads),
          runtime_state_->resource_pool()));
  mem_usage_sampled_counter_ = profile()->AddSamplingTimeSeriesCounter("MemoryUsage",
      TUnit::BYTES,
      bind<int64_t>(mem_fn(&MemTracker::consumption),
          runtime_state_->instance_mem_tracker()));
  thread_usage_sampled_counter_ = profile()->AddSamplingTimeSeriesCounter("ThreadUsage",
      TUnit::UNIT,
      bind<int64_t>(mem_fn(&ThreadResourcePool::num_threads),
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

  profile()->AddDerivedCounter("ExchangeScanRatio", TUnit::DOUBLE_VALUE, [this](){
      int64_t counter_val = 0;
      *reinterpret_cast<double*>(&counter_val) =
          runtime_state_->ComputeExchangeScanRatio();
      return counter_val;
      });

  row_batch_.reset(
      new RowBatch(exec_tree_->row_desc(), runtime_state_->batch_size(),
        runtime_state_->instance_mem_tracker()));
  VLOG(2) << "plan_root=\n" << exec_tree_->DebugString();
  return Status::OK();
}

void FragmentInstanceState::GetStatusReport(FragmentInstanceExecStatusPB* instance_status,
    TRuntimeProfileTree* thrift_profile) {
  DFAKE_SCOPED_LOCK(report_status_lock_);
  DCHECK(!final_report_sent_);
  // Update the counter for the peak per host mem usage.
  if (per_host_mem_usage_ != nullptr) {
    per_host_mem_usage_->Set(runtime_state()->query_mem_tracker()->peak_consumption());
  }
  if (final_report_generated_) {
    // Since execution was already finished, the contents of this report will be identical
    // to the last report, so don't advance the sequence number.
    instance_status->set_report_seq_no(report_seq_no_);
  } else {
    instance_status->set_report_seq_no(AdvanceReportSeqNo());
  }
  const TUniqueId& finstance_id = instance_id();
  TUniqueIdToUniqueIdPB(finstance_id, instance_status->mutable_fragment_instance_id());
  const bool done = IsDone();
  instance_status->set_done(done);
  instance_status->set_current_state(current_state());
  DCHECK(profile() != nullptr);
  profile()->ToThrift(thrift_profile);
  // Send the DML stats if this is the final report.
  if (done) {
    runtime_state()->dml_exec_state()->ToProto(
        instance_status->mutable_dml_exec_status());
    final_report_generated_ = true;
  }
  if (prev_stateful_reports_.size() > 0) {
    // Send errors from previous reports that failed.
    *instance_status->mutable_stateful_report() =
        {prev_stateful_reports_.begin(), prev_stateful_reports_.end()};
  }
  if (runtime_state()->HasErrors()) {
    // Add any new errors.
    StatefulStatusPB* stateful_report = instance_status->add_stateful_report();
    stateful_report->set_report_seq_no(report_seq_no_);
    runtime_state()->GetUnreportedErrors(stateful_report->mutable_error_log());
  }
}

void FragmentInstanceState::ReportSuccessful(
    const FragmentInstanceExecStatusPB& instance_exec_status) {
  prev_stateful_reports_.clear();
  if (instance_exec_status.done()) final_report_sent_ = true;
}

void FragmentInstanceState::ReportFailed(
    const FragmentInstanceExecStatusPB& instance_exec_status) {
  int num_reports = instance_exec_status.stateful_report_size();
  if (num_reports > 0 && prev_stateful_reports_.size() != num_reports) {
    // If a stateful report was generated in GetStatusReport(), copy it to
    // 'prev_stateful_reports_'. It will be the last one in the list and will have a seq
    // no that matches the overall report's seq no. There can be at most 1 new stateful
    // report that has been generated since the last call to ReportSuccessful()/Failed().
    DCHECK_EQ(prev_stateful_reports_.size() + 1, num_reports);
    const StatefulStatusPB& stateful_report =
        instance_exec_status.stateful_report()[num_reports - 1];
    DCHECK_EQ(stateful_report.report_seq_no(), instance_exec_status.report_seq_no());
    prev_stateful_reports_.emplace_back(stateful_report);
  }
}

Status FragmentInstanceState::Open() {
  DCHECK(!opened_promise_.IsSet());
  DCHECK_EQ(current_state_.Load(), FInstanceExecStatePB::WAITING_FOR_PREPARE);
  SCOPED_TIMER2(profile()->total_time_counter(),
      ADD_TIMER(timings_profile_, OPEN_TIMER_NAME));
  SCOPED_THREAD_COUNTER_MEASUREMENT(runtime_state_->total_thread_statistics());

  if (runtime_state_->ShouldCodegen()) {
    UpdateState(StateEvent::CODEGEN_START);
    RETURN_IF_ERROR(runtime_state_->CreateCodegen());
    {
      SCOPED_TIMER2(runtime_state_->codegen()->ir_generation_timer(),
          runtime_state_->codegen()->runtime_profile()->total_time_counter());
      SCOPED_THREAD_COUNTER_MEASUREMENT(
          runtime_state_->codegen()->llvm_thread_counters());
      exec_tree_->Codegen(runtime_state_);
      sink_->Codegen(runtime_state_->codegen());

      // It shouldn't be fatal to fail codegen. However, until IMPALA-4233 is fixed,
      // ScalarFnCall has no fall back to interpretation when codegen fails so propagates
      // the error status for now.
      RETURN_IF_ERROR(runtime_state_->CodegenScalarExprs());
    }

    LlvmCodeGen* codegen = runtime_state_->codegen();
    DCHECK(codegen != nullptr);
    RETURN_IF_ERROR(codegen->FinalizeModule());
  }

  {
    UpdateState(StateEvent::OPEN_START);
    // Inject failure if debug actions are enabled.
    RETURN_IF_ERROR(DebugAction(query_state_->query_options(), "FIS_IN_OPEN"));

    SCOPED_TIMER(ADD_CHILD_TIMER(timings_profile_, "ExecTreeOpenTime", OPEN_TIMER_NAME));
    RETURN_IF_ERROR(exec_tree_->Open(runtime_state_));
  }
  return sink_->Open(runtime_state_);
}

Status FragmentInstanceState::ExecInternal() {
  DCHECK_EQ(current_state_.Load(), FInstanceExecStatePB::WAITING_FOR_OPEN);
  // Inject failure if debug actions are enabled.
  RETURN_IF_ERROR(DebugAction(query_state_->query_options(), "FIS_IN_EXEC_INTERNAL"));

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
  // Flush the sink as a final step.
  RETURN_IF_ERROR(sink_->FlushFinal(runtime_state()));
  return Status::OK();
}

void FragmentInstanceState::Close() {
  DCHECK(runtime_state_ != nullptr);

  // If we haven't already released this thread token in Prepare(), release
  // it before calling Close().
  if (fragment_ctx_.fragment.output_sink.type != TDataSinkType::PLAN_ROOT_SINK) {
    ReleaseThreadToken();
  }

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
    // TODO: IMPALA-4631: Occasionally we see other_time = total_time + ε where ε is 1,
    // 2, or 3. It appears to be a bug with clocks on some virtualized systems. Add 3
    // to total_time to avoid DCHECKing in that case.
    DCHECK_LE(other_time, total_time + 3);
  }
#endif
}

void FragmentInstanceState::UpdateState(const StateEvent event)
{
  FInstanceExecStatePB current_state = current_state_.Load();
  FInstanceExecStatePB next_state = current_state;
  switch (event) {
    case StateEvent::PREPARE_START:
      DCHECK_EQ(current_state, FInstanceExecStatePB::WAITING_FOR_EXEC);
      next_state = FInstanceExecStatePB::WAITING_FOR_PREPARE;
      break;

    case StateEvent::CODEGEN_START:
      DCHECK_EQ(current_state, FInstanceExecStatePB::WAITING_FOR_PREPARE);
      event_sequence_->MarkEvent("Prepare Finished");
      next_state = FInstanceExecStatePB::WAITING_FOR_CODEGEN;
      break;

    case StateEvent::OPEN_START:
      if (current_state == FInstanceExecStatePB::WAITING_FOR_PREPARE) {
        event_sequence_->MarkEvent("Prepare Finished");
      } else {
        DCHECK_EQ(current_state, FInstanceExecStatePB::WAITING_FOR_CODEGEN);
      }
      next_state = FInstanceExecStatePB::WAITING_FOR_OPEN;
      break;

    case StateEvent::WAITING_FOR_FIRST_BATCH:
      DCHECK_EQ(current_state, FInstanceExecStatePB::WAITING_FOR_OPEN);
      event_sequence_->MarkEvent("Open Finished");
      next_state = FInstanceExecStatePB::WAITING_FOR_FIRST_BATCH;
      break;

    case StateEvent::BATCH_PRODUCED:
      if (UNLIKELY(current_state == FInstanceExecStatePB::WAITING_FOR_FIRST_BATCH)) {
        event_sequence_->MarkEvent("First Batch Produced");
        next_state = FInstanceExecStatePB::FIRST_BATCH_PRODUCED;
      } else {
        DCHECK_EQ(current_state, FInstanceExecStatePB::PRODUCING_DATA);
      }
      break;

    case StateEvent::BATCH_SENT:
      if (UNLIKELY(current_state == FInstanceExecStatePB::FIRST_BATCH_PRODUCED)) {
        event_sequence_->MarkEvent("First Batch Sent");
        next_state = FInstanceExecStatePB::PRODUCING_DATA;
      } else {
        DCHECK_EQ(current_state, FInstanceExecStatePB::PRODUCING_DATA);
      }
      break;

    case StateEvent::LAST_BATCH_SENT:
      DCHECK_EQ(current_state, FInstanceExecStatePB::PRODUCING_DATA);
      next_state = FInstanceExecStatePB::LAST_BATCH_SENT;
      break;

    case StateEvent::EXEC_END:
      // Allow abort in all states to make error handling easier.
      event_sequence_->MarkEvent("ExecInternal Finished");
      next_state = FInstanceExecStatePB::FINISHED;
      break;

    default:
      DCHECK(false) << "Unexpected Event: " << static_cast<int>(event);
      break;
  }
  // This method is the only one updating 'current_state_' and is not meant to be thread
  // safe.
  if (next_state != current_state) current_state_.Store(next_state);
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

Status FragmentInstanceState::WaitForOpen() {
  return opened_promise_.Get();
}

void FragmentInstanceState::PublishFilter(const TPublishFilterParams& params) {
  VLOG_FILE << "PublishFilter(): instance_id=" << PrintId(instance_id())
            << " filter_id=" << params.filter_id;
  runtime_state_->filter_bank()->PublishGlobalFilter(params);
}

const string& FragmentInstanceState::ExecStateToString(FInstanceExecStatePB state) {
  // Labels to send to the debug webpages to display the current state to the user.
  static const string finstance_state_labels[] = {
      "Waiting for Exec",         // WAITING_FOR_EXEC
      "Waiting for Prepare",      // WAITING_FOR_PREPARE
      "Waiting for Codegen",      // WAITING_FOR_CODEGEN
      "Waiting for First Batch",  // WAITING_FOR_OPEN
      "Waiting for First Batch",  // WAITING_FOR_FIRST_BATCH
      "First batch produced",     // FIRST_BATCH_PRODUCED
      "Producing Data",           // PRODUCING_DATA
      "Last batch sent",          // LAST_BATCH_SENT
      "Finished"                  // FINISHED
  };
  /// Make sure we have a label for every possible state.
  static_assert(sizeof(finstance_state_labels) / sizeof(string) ==
      FInstanceExecStatePB::FINISHED + 1, "");

  DCHECK_LT(state, sizeof(finstance_state_labels) / sizeof(string))
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
      << PrintId(query_id()) << ":\n" << str.str();
}
