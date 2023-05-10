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
#include <boost/bind.hpp>
#include <boost/date_time/posix_time/posix_time_types.hpp>
#include <boost/thread/thread_time.hpp>
#include <thrift/protocol/TDebugProtocol.h>

#include "codegen/llvm-codegen.h"
#include "exec/exchange-node.h"
#include "exec/exec-node.h"
#include "exec/hdfs-scan-node-base.h"
#include "exec/join-builder.h"
#include "exec/nested-loop-join-builder.h"
#include "exec/partitioned-hash-join-builder.h"
#include "exec/plan-root-sink.h"
#include "exec/scan-node.h"
#include "gen-cpp/ImpalaInternalService_types.h"
#include "kudu/rpc/rpc_context.h"
#include "runtime/client-cache.h"
#include "runtime/exec-env.h"
#include "runtime/fragment-state.h"
#include "runtime/krpc-data-stream-sender.h"
#include "runtime/mem-tracker.h"
#include "runtime/query-state.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-filter-bank.h"
#include "runtime/runtime-state.h"
#include "runtime/thread-resource-mgr.h"
#include "util/container-util.h"
#include "util/debug-util.h"
#include "util/periodic-counter-updater.h"
#include "util/uid-util.h"

#include "common/names.h"

using google::protobuf::RepeatedPtrField;
using kudu::rpc::RpcContext;
using namespace apache::thrift;

namespace impala {

const string FragmentInstanceState::PER_HOST_PEAK_MEM_COUNTER = "PerHostPeakMemUsage";
const string FragmentInstanceState::FINST_THREAD_GROUP_NAME = "fragment-execution";
const string FragmentInstanceState::FINST_THREAD_NAME_PREFIX = "exec-finstance";

static const string OPEN_TIMER_NAME = "OpenTime";
static const string PREPARE_TIMER_NAME = "PrepareTime";
static const string EXEC_TIMER_NAME = "ExecTime";

PROFILE_DECLARE_COUNTER(ScanRangesComplete);
PROFILE_DECLARE_COUNTER(BytesRead);

FragmentInstanceState::FragmentInstanceState(QueryState* query_state,
    FragmentState* fragment_state, const TPlanFragmentInstanceCtx& instance_ctx,
    const PlanFragmentInstanceCtxPB& instance_ctx_pb)
  : query_state_(query_state),
    fragment_state_(fragment_state),
    fragment_(fragment_state->fragment()),
    instance_ctx_(instance_ctx),
    fragment_ctx_(fragment_state->fragment_ctx()),
    instance_ctx_pb_(instance_ctx_pb) {}

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

  DCHECK_EQ(is_prepared,
      current_state_.Load() > FInstanceExecStatePB::WAITING_FOR_PREPARE);

  if (!status.ok()) {
    exec_failed_.Store(true);
    if (!is_prepared) {
      UpdateState(StateEvent::EXEC_END);

      // Tell the managing 'QueryState' that we hit an error during Prepare().
      query_state_->ErrorDuringPrepare(status, instance_id());
    } else {
      // Must set exec_failed_ first, then update the 'Query State' before updating the
      // fragment instance state. Otherwise, there is a race when reading the 'done' flag
      // with GetStatusReport(). This may lead to report the instance as "completed"
      // without error, or the "final" profile being sent with the 'done' flag as false.

      // Tell the managing 'QueryState' that we hit an error during execution.
      query_state_->ErrorDuringExecute(status, instance_id());

      UpdateState(StateEvent::EXEC_END);
      query_state_->DoneRemainingExecuting();
    }
  } else {
    UpdateState(StateEvent::EXEC_END);

    // Tell the managing 'QueryState' that we're done with executing.
    query_state_->DoneExecuting();
  }
  return status;
}

void FragmentInstanceState::Cancel() {
  DCHECK(runtime_state_ != nullptr);
  runtime_state_->Cancel();
  PlanRootSink* root_sink = GetRootSink();
  if (root_sink != nullptr) root_sink->Cancel(runtime_state_);
}

Status FragmentInstanceState::Prepare() {
  DCHECK_EQ(current_state_.Load(), FInstanceExecStatePB::WAITING_FOR_EXEC);
  VLOG(2) << "fragment_instance_ctx:\n" << ThriftDebugString(instance_ctx_);

  // Do not call RETURN_IF_ERROR or explicitly return before this line,
  // runtime_state_ != nullptr is a postcondition of this function.
  runtime_state_ = obj_pool()->Add(new RuntimeState(query_state_, fragment_,
      instance_ctx_, fragment_ctx_, instance_ctx_pb_, ExecEnv::GetInstance()));

  // total_time_counter() is in the runtime_state_ so start it up now.
  SCOPED_TIMER(profile()->total_time_counter());
  timings_profile_ = RuntimeProfile::Create(
      obj_pool(), RuntimeProfile::FRAGMENT_INSTANCE_LIFECYCLE_TIMINGS, false);
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

  // Create the exec tree.
  const PlanNode* plan_tree = fragment_state_->plan_tree();
  DCHECK(plan_tree != nullptr);
  RETURN_IF_ERROR(ExecNode::CreateTree(
      runtime_state_, *plan_tree, query_state_->desc_tbl(), &exec_tree_));
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
    DCHECK_GT(num_senders, 0) << exch_node->id();
    static_cast<ExchangeNode*>(exch_node)->set_num_senders(num_senders);
  }

  // set scan ranges
  vector<ExecNode*> scan_nodes;
  ScanRangesPB no_scan_ranges;
  exec_tree_->CollectScanNodes(&scan_nodes);
  for (ExecNode* scan_node: scan_nodes) {
    const ScanRangesPB& scan_ranges = FindWithDefault(
        instance_ctx_pb_.per_node_scan_ranges(), scan_node->id(), no_scan_ranges);
    static_cast<ScanNode*>(scan_node)->SetScanRanges(scan_ranges.scan_ranges());
  }

  RuntimeProfile::Counter* prepare_timer =
      ADD_CHILD_TIMER(timings_profile_, "ExecTreePrepareTime", PREPARE_TIMER_NAME);
  {
    SCOPED_TIMER(prepare_timer);
    RETURN_IF_ERROR(exec_tree_->Prepare(runtime_state_));
  }
  PrintVolumeIds();

  // prepare sink_
  const DataSinkConfig* sink_config = fragment_state_->sink_config();
  DCHECK(sink_config != nullptr);
  sink_ = sink_config->CreateSink(runtime_state_);
  RETURN_IF_ERROR(sink_->Prepare(runtime_state_, runtime_state_->instance_mem_tracker()));
  RuntimeProfile* sink_profile = sink_->profile();
  if (sink_profile != nullptr) profile()->AddChild(sink_profile);

  PlanRootSink* root_sink = GetRootSink();
  if (root_sink != nullptr) {
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
    TRuntimeProfileTree* unagg_profile, AggregatedRuntimeProfile* agg_profile,
    const Status& overall_status) {
  DCHECK_NE(unagg_profile == nullptr, agg_profile == nullptr);
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
  // For failed fragment instance, report it as "done" when overall_statue is reported
  // with error. This avoid coordinator to ignore the last status report.
  const bool done = (IsDone() && !ExecFailed()) || (ExecFailed() && !overall_status.ok());
  instance_status->set_done(done);
  instance_status->set_current_state(current_state());
  DCHECK(profile() != nullptr);
  if (agg_profile != nullptr) {
    // Figure out the index of this instance relative to other instances of this fragment
    // on the backend.
    int instance_idx = instance_ctx_.per_fragment_instance_idx -
        fragment_state_->min_per_fragment_instance_idx();
    agg_profile->UpdateAggregatedFromInstance(profile(), instance_idx);
  } else {
    DCHECK(unagg_profile != nullptr);
    profile()->ToThrift(unagg_profile);
  }

  // Pull out and aggregate counters from the profile.
  RuntimeProfile::Counter* user_time = profile()->GetCounter("TotalThreadsUserTime");
  if (user_time != nullptr) cpu_user_ns_ = user_time->value();

  RuntimeProfile::Counter* system_time = profile()->GetCounter("TotalThreadsSysTime");
  if (system_time != nullptr) cpu_sys_ns_ = system_time->value();

  // Compute local_time for use below.
  profile()->ComputeTimeInProfile();
  vector<RuntimeProfileBase*> nodes;
  profile()->GetAllChildren(&nodes);
  int64_t bytes_read = 0;
  int64_t scan_ranges_complete = 0;
  int64_t total_bytes_sent = 0;
  std::map<int32_t, int64_t> per_join_rows_produced;
  for (RuntimeProfileBase* node : nodes) {
    RuntimeProfile::Counter* c = node->GetCounter(PROFILE_BytesRead.name());
    if (c != nullptr) bytes_read += c->value();
    c = node->GetCounter(PROFILE_ScanRangesComplete.name());
    if (c != nullptr) scan_ranges_complete += c->value();
    c = node->GetCounter(KrpcDataStreamSender::TOTAL_BYTES_SENT_COUNTER);
    if (c != nullptr) total_bytes_sent += c->value();

    bool is_plan_node = node->metadata().__isset.plan_node_id;
    bool is_data_sink = node->metadata().__isset.data_sink_id;
    // Plan Nodes and data sinks get an entry in the exec summary.
    if (is_plan_node || is_data_sink) {
      ExecSummaryDataPB* summary_data = instance_status->add_exec_summary_data();
      if (is_plan_node) {
        summary_data->set_plan_node_id(node->metadata().plan_node_id);
      } else {
        summary_data->set_data_sink_id(node->metadata().data_sink_id);
      }
      RuntimeProfile::Counter* rows_counter = node->GetCounter("RowsReturned");
      RuntimeProfile::Counter* mem_counter = node->GetCounter("PeakMemoryUsage");
      if (rows_counter != nullptr) {
        summary_data->set_rows_returned(rows_counter->value());
        // row count stats for a join node
        string hash_type = PrintValue(TPlanNodeType::HASH_JOIN_NODE);
        string nested_loop_type = PrintValue(TPlanNodeType::NESTED_LOOP_JOIN_NODE);
        if (node->name().rfind(hash_type, 0) == 0
            || node->name().rfind(nested_loop_type, 0) == 0) {
          per_join_rows_produced[node->metadata().plan_node_id] = rows_counter->value();
        }
      }
      if (mem_counter != nullptr) summary_data->set_peak_mem_usage(mem_counter->value());
      summary_data->set_local_time_ns(node->local_time());
    }
  }
  bytes_read_ = bytes_read;
  scan_ranges_complete_ = scan_ranges_complete;
  total_bytes_sent_  = total_bytes_sent;
  per_join_rows_produced_ = per_join_rows_produced;

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
  StatefulStatusPB* stateful_report = nullptr;
  if (runtime_state()->HasErrors()) {
    // Add any new errors.
    stateful_report = instance_status->add_stateful_report();
    stateful_report->set_report_seq_no(report_seq_no_);
    runtime_state()->GetUnreportedErrors(stateful_report->mutable_error_log());
  }
  // If set in the RuntimeState, set the AuxErrorInfoPB field.
  if (runtime_state()->HasAuxErrorInfo()) {
    if (stateful_report == nullptr) {
      stateful_report = instance_status->add_stateful_report();
      stateful_report->set_report_seq_no(report_seq_no_);
    }
    runtime_state()->GetUnreportedAuxErrorInfo(stateful_report->mutable_aux_error_info());
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

  if (fragment_state_->ShouldCodegen()) {
    UpdateState(StateEvent::CODEGEN_START);
    RETURN_IF_ERROR(fragment_state_->InvokeCodegen(event_sequence_));
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
  // Release resources from final row batch.
  row_batch_->Reset();

  UpdateState(StateEvent::LAST_BATCH_SENT);

  // Close the tree before the sink is flushed to release 'exec_tree_' resources.
  // This can significantly reduce resource consumption if 'sink_' is a join
  // build, where FlushFinal() blocks until the consuming fragment is finished.
  exec_tree_->Close(runtime_state_);

  // Flush the sink as a final step.
  RETURN_IF_ERROR(sink_->FlushFinal(runtime_state()));
  return Status::OK();
}

void FragmentInstanceState::Close() {
  DCHECK(runtime_state_ != nullptr);

  // Required to wake up any threads that might be blocked waiting for filters, e.g.
  // scanner threads.
  // TODO: we might be able to remove this with mt_dop, since we only need to worry
  // have the fragment thread (i.e. the current thread).
  Cancel();

  // If we haven't already released this thread token in Prepare(), release
  // it before calling Close().
  if (fragment_.output_sink.type != TDataSinkType::PLAN_ROOT_SINK) {
    ReleaseThreadToken();
  }

  // guard against partially-finished Prepare()
  if (sink_ != nullptr) sink_->Close(runtime_state_);

  // Stop updating profile counters in background.
  profile()->StopPeriodicCounters();

  // Delete row_batch_ to free resources associated with it.
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

PlanRootSink* FragmentInstanceState::GetRootSink() const {
  return fragment_.output_sink.type == TDataSinkType::PLAN_ROOT_SINK ?
      static_cast<PlanRootSink*>(sink_) :
      nullptr;
}

bool FragmentInstanceState::HasJoinBuildSink() const {
  return IsJoinBuildSink(fragment_.output_sink.type);
}

JoinBuilder* FragmentInstanceState::GetJoinBuildSink() const {
  return HasJoinBuildSink() ? static_cast<JoinBuilder*>(sink_) : nullptr;
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
  if (instance_ctx_pb_.per_node_scan_ranges().empty()) return;

  HdfsScanNodeBase::PerVolumeStats per_volume_stats;
  for (const auto& entry : instance_ctx_pb_.per_node_scan_ranges()) {
    HdfsScanNodeBase::UpdateHdfsSplitStats(entry.second.scan_ranges(), &per_volume_stats);
  }

  stringstream str;
  HdfsScanNodeBase::PrintHdfsSplitStats(per_volume_stats, &str);
  profile()->AddInfoString(HdfsScanNodeBase::HDFS_SPLIT_STATS_DESC, str.str());
  VLOG_FILE
      << "Hdfs split stats (<volume id>:<# splits>/<split lengths>) for query="
      << PrintId(query_id()) << ":\n" << str.str();
}
}
