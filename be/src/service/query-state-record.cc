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

// Contains implementations of the QueryStateRecord and QueryStateExpanded struct
// functions. These structs represent the state of a query for capturing the query in the
// query log and completed queries table.

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <boost/algorithm/string.hpp>

#include <gutil/strings/numbers.h>
#include <gutil/strings/strcat.h>
#include "runtime/coordinator.h"
#include "scheduling/admission-controller.h"
#include "scheduling/scheduler.h"
#include "service/client-request-state.h"
#include "service/query-state-record.h"
#include "util/debug-util.h"
#include "util/network-util.h"
#include "util/string-util.h"

using namespace std;

namespace impala {

QueryStateRecord::QueryStateRecord(
    const ClientRequestState& query_handle, vector<uint8_t>&& compressed_profile)
  : compressed_profile(compressed_profile) {
  Init(query_handle);
}

QueryStateRecord::QueryStateRecord(const ClientRequestState& query_handle)
  : compressed_profile() {
  Init(query_handle);
}

void QueryStateRecord::Init(const ClientRequestState& query_handle) {
  id = query_handle.query_id();

  const string* plan_str = query_handle.summary_profile()->GetInfoString("Plan");
  if (plan_str != nullptr) {
    plan = *plan_str;
    // Remove any trailing newlines.
    boost::algorithm::trim_if(plan, boost::algorithm::is_any_of("\n"));
  }

  stmt = query_handle.sql_stmt();
  effective_user = query_handle.effective_user();
  default_db = query_handle.default_db();
  start_time_us = query_handle.start_time_us();
  end_time_us = query_handle.end_time_us();
  wait_time_ms = query_handle.wait_time_ms();
  client_fetch_wait_time_ns = query_handle.client_fetch_wait_time_ns();
  query_handle.summary_profile()->GetTimeline(&timeline);

  Coordinator* coord = query_handle.GetCoordinator();
  if (coord != nullptr) {
    num_completed_scan_ranges = coord->scan_progress().num_complete();
    total_scan_ranges = coord->scan_progress().total();
    num_completed_fragment_instances = coord->query_progress().num_complete();
    total_fragment_instances = coord->query_progress().total();
    const auto& utilization = coord->ComputeQueryResourceUtilization();
    total_peak_mem_usage = utilization.total_peak_mem_usage;
    cluster_mem_est = query_handle.schedule()->cluster_mem_est();
    bytes_read = utilization.bytes_read;
    bytes_sent = utilization.exchange_bytes_sent + utilization.scan_bytes_sent;
    has_coord = true;
  } else {
    num_completed_scan_ranges = 0;
    total_scan_ranges = 0;
    num_completed_fragment_instances = 0;
    total_fragment_instances = 0;
    total_peak_mem_usage = 0;
    cluster_mem_est = 0;
    bytes_read = 0;
    bytes_sent = 0;
    has_coord = false;
  }
  beeswax_query_state = query_handle.BeeswaxQueryState();
  ClientRequestState::RetryState retry_state = query_handle.retry_state();
  if (retry_state == ClientRequestState::RetryState::NOT_RETRIED) {
    query_state = beeswax::_QueryState_VALUES_TO_NAMES.find(beeswax_query_state)->second;
  } else {
    query_state = query_handle.RetryStateToString(retry_state);
  }
  num_rows_fetched = query_handle.num_rows_fetched();
  query_status = query_handle.query_status();

  query_handle.query_events()->ToThrift(&event_sequence);

  const TExecRequest& request = query_handle.exec_request();
  stmt_type = request.stmt_type;
  // Save the query fragments so that the plan can be visualised.
  for (const TPlanExecInfo& plan_exec_info: request.query_exec_request.plan_exec_info) {
    fragments.insert(fragments.end(),
        plan_exec_info.fragments.begin(), plan_exec_info.fragments.end());
  }
  all_rows_returned = query_handle.eos();
  last_active_time_ms = query_handle.last_active_ms();
  // For statement types other than QUERY/DML, show an empty string for resource pool
  // to indicate that they are not subjected to admission control.
  if (stmt_type == TStmtType::QUERY || stmt_type == TStmtType::DML) {
    resource_pool = query_handle.request_pool();
  }
  user_has_profile_access = query_handle.user_has_profile_access();

  // In some cases like canceling and closing the original query or closing the session
  // we may not create the new query, we also check whether the retrided query id is set.
  was_retried = query_handle.WasRetried() && query_handle.IsSetRetriedId();
  if (was_retried) {
    retried_query_id = make_unique<TUniqueId>(query_handle.retried_id());
  }
}

bool QueryStateRecord::StartTimeComparator::operator() (
    const QueryStateRecord& lhs, const QueryStateRecord& rhs) const {
  if (lhs.start_time_us == rhs.start_time_us) return lhs.id < rhs.id;
  return lhs.start_time_us < rhs.start_time_us;
}

int64_t EstimateSize(const QueryStateRecord* record) {
  int64_t size = sizeof(QueryStateRecord); // 800
  size += sizeof(uint8_t) * record->compressed_profile.capacity();
  size += record->effective_user.capacity();
  size += record->default_db.capacity();
  size += record->stmt.capacity();
  size += record->plan.capacity();
  size += record->query_state.capacity();
  size += record->timeline.capacity();
  size += record->resource_pool.capacity();

  // The following dynamic memory of field members are estimated rather than
  // exactly sized. Some of thrift members might be nested, but the estimation
  // does not traverse deeper than the first level.

  // TExecSummary exec_summary
  if (record->exec_summary.__isset.nodes) {
    size += sizeof(TPlanNodeExecSummary) * record->exec_summary.nodes.capacity();
  }
  if (record->exec_summary.__isset.exch_to_sender_map) {
    size += sizeof(int32_t) * 2 * record->exec_summary.exch_to_sender_map.size();
  }
  if (record->exec_summary.__isset.error_logs) {
    for (const auto& log : record->exec_summary.error_logs) size += log.capacity();
  }
  if (record->exec_summary.__isset.queued_reason) {
    size += record->exec_summary.queued_reason.capacity();
  }

  // Status query_status
  if (!record->query_status.ok()) {
    size += record->query_status.msg().msg().capacity();
    for (const auto& detail : record->query_status.msg().details()) {
      size += detail.capacity();
    }
  }

  // TEventSequence event_sequence
  size += record->event_sequence.name.capacity();
  size += sizeof(int64_t) * record->event_sequence.timestamps.capacity();
  for (const auto& label : record->event_sequence.labels) size += label.capacity();

  // vector<TPlanFragment> fragments
  size += sizeof(TPlanFragment) * record->fragments.capacity();

  return size;
}

static bool find_instance(RuntimeProfileBase* prof) {
  return boost::algorithm::istarts_with(prof->name(), "Instance");
}

static bool find_averaged(RuntimeProfileBase* prof) {
  return boost::algorithm::istarts_with(prof->name(), "Averaged Fragment");
}

QueryStateExpanded::QueryStateExpanded(const ClientRequestState& exec_state,
    const std::shared_ptr<QueryStateRecord> base) :
    base_state(base ? move(base) : make_shared<QueryStateRecord>(exec_state)) {
  if (exec_state.session()->session_type == TSessionType::HIVESERVER2){
    hiveserver2_protocol_version = exec_state.session()->hs2_version;
  }
  query_options = exec_state.query_options();
  session_id = exec_state.session_id();
  session_type = exec_state.session()->session_type;
  redacted_sql = exec_state.redacted_sql();
  db_user_connection = exec_state.connected_user();
  client_address = exec_state.session()->network_address;
  impala_query_end_state = exec_state.ExecStateToString(exec_state.exec_state());
  per_host_mem_estimate = exec_state.exec_request()
      .query_exec_request.planner_per_host_mem_estimate;
  dedicated_coord_mem_estimate = exec_state.exec_request()
      .query_exec_request.dedicated_coord_mem_estimate;
  row_materialization_rate = exec_state.row_materialization_rate();
  row_materialization_time = exec_state.row_materialization_timer();
  tables = exec_state.tables();

  // Update name_rows_fetched with the final count after query close.
  base_state->num_rows_fetched = exec_state.num_rows_fetched_counter();

  // Fields from the schedule.
  if (exec_state.schedule() != nullptr) {
    // Per-Host Metrics
    for (int i =0; i < exec_state.schedule()->backend_exec_params_size(); i++) {
      const BackendExecParamsPB& b = exec_state.schedule()->backend_exec_params(i);
      TNetworkAddress host = FromNetworkAddressPB(b.address());

      PerHostState state;
      state.fragment_instance_count = b.instance_params_size();
      per_host_state.emplace(move(host), move(state));
    }
  }

  // Fields from the summary profile.
  if (exec_state.summary_profile() != nullptr) {
    const string* result_ptr = exec_state.summary_profile()
        ->GetInfoString(AdmissionController::PROFILE_INFO_KEY_ADMISSION_RESULT);
    admission_result = result_ptr == nullptr ? "" : *result_ptr;

    const string* exec_group_ptr = exec_state.summary_profile()
       ->GetInfoString(AdmissionController::PROFILE_INFO_KEY_EXECUTOR_GROUP);
    executor_group = exec_group_ptr == nullptr ? "" : *exec_group_ptr;

    const string* exec_summary_ptr = exec_state.summary_profile()->
        GetInfoString("ExecSummary");
    if (exec_summary_ptr != nullptr && !exec_summary_ptr->empty()) {
      exec_summary = *exec_summary_ptr;
      boost::algorithm::trim_if(exec_summary, boost::algorithm::is_any_of("\n"));
    }
  }

  // Fields from the coordinator
  Coordinator* coord = exec_state.GetCoordinator();
  if (coord != nullptr) {
    // Query Profile is initialized when query execution starts. Thus, it will always be
    // non-null at this point since this code runs after the query completes.
    DCHECK(coord->query_profile() != nullptr);
    map<string, int64_t> host_scratch_bytes;
    map<string, int64_t> scanner_io_wait;
    map<string, int64_t> bytes_read_cache;
    vector<RuntimeProfileBase*> prof_stack;

    // Lambda function to recursively walk through a profile.
    std::function<void(RuntimeProfileBase*)> process_exec_profile = [
        &process_exec_profile, &host_scratch_bytes, &scanner_io_wait, &prof_stack,
        &bytes_read_cache, this](RuntimeProfileBase* profile) {
      prof_stack.push_back(profile);
      if (const auto& cntr = profile->GetCounter("ScratchBytesWritten");
          cntr != nullptr) {
        host_scratch_bytes.emplace(profile->name(), cntr->value());
      }

      // Metrics from HDFS_SCAN_NODE entries.
      if (const string& scan = prof_stack.back()->name();
          boost::algorithm::istarts_with(scan, "HDFS_SCAN_NODE")) {
        // Find a parent instance. If none found, assume in Averaged Fragment.
        if (auto it = find_if(prof_stack.begin()+1, prof_stack.end()-1, find_instance);
            it != prof_stack.end()-1) {
          DCHECK(find_if(prof_stack.begin(), prof_stack.end(), find_averaged)
              == prof_stack.end());
          const string& inst = (*it)->name();
          if (const auto& cntr = profile->GetCounter("ScannerIoWaitTime");
              cntr != nullptr) {
            scanner_io_wait.emplace(StrCat(inst, "::", scan), cntr->value());
          }

          if (const auto& cntr = profile->GetCounter("DataCacheHitBytes");
              cntr != nullptr) {
            bytes_read_cache.emplace(StrCat(inst, "::", scan), cntr->value());
          }
        } else {
          DCHECK(find_if(prof_stack.begin(), prof_stack.end(), find_averaged)
              != prof_stack.end());
        }
      }

      // Total Bytes Read
      if (const auto& cntr = profile->GetCounter("TotalBytesRead"); cntr != nullptr) {
        bytes_read_total = cntr->value();
      }

      // Recursively walk down through all child nodes.
      vector<RuntimeProfileBase*> children;
      profile->GetChildren(&children);
      for (const auto& child : children) {
        process_exec_profile(child);
      }
      prof_stack.pop_back();
    };

    process_exec_profile(coord->query_profile());

    // Compressed Bytes Spilled
    for (const auto& hsb : host_scratch_bytes) {
      compressed_bytes_spilled += hsb.second;
    }

    // Read IO Wait Time Total and Average
    if (scanner_io_wait.size() > 0) {
      for (const auto& item : scanner_io_wait) {
        read_io_wait_time_total += item.second;
      }

      read_io_wait_time_mean = read_io_wait_time_total / scanner_io_wait.size();
    }

    // Bytes Read from Data Cache
    for (const auto& b : bytes_read_cache) {
      bytes_read_cache_total += b.second;
    }

    // Per-Node Peak Memory Usage
    for (const auto& be : coord->BackendResourceUtilization()) {
      TNetworkAddress addr = FromNetworkAddressPB(be.first);
      if(const auto& host = per_host_state.find(addr);
          LIKELY(host != per_host_state.end())) {
        host->second.peak_memory_usage = be.second.peak_per_host_mem_consumption;
      } else{
        PerHostState state;
        state.peak_memory_usage = be.second.peak_per_host_mem_consumption;
        per_host_state.emplace(addr, state);
      }
    }
  } // Fields from the coordinator

  // Executor Group
  const RuntimeProfile* fe_profile = exec_state.frontend_profile();
  DCHECK(fe_profile != nullptr); // Frontend profile is initialized in the constructor.
  std::vector<RuntimeProfileBase*> children;
  stringstream exec_group_str;

  fe_profile->GetChildren(&children);
  for (const auto& child : children) {
    if (boost::algorithm::istarts_with(child->name(), "executor group ")) {
      child->PrettyPrint(&exec_group_str);
    }
  }
  executor_groups = exec_group_str.str();
  boost::algorithm::trim_if(executor_groups, boost::algorithm::is_any_of("\n"));

  // Find important events in the events timeline and store them in their own map.
  for (const auto& event : EventsTimeline()) {
    if (boost::algorithm::iequals(event.first, "planning finished")) {
      events.insert_or_assign(PLANNING_FINISHED, event.second);
    } else if (boost::algorithm::iequals(event.first, "submit for admission")) {
      events.insert_or_assign(SUBMIT_FOR_ADMISSION, event.second);
    } else if (boost::algorithm::iequals(event.first, "completed admission")) {
      events.insert_or_assign(COMPLETED_ADMISSION, event.second);
    } else if (boost::algorithm::istarts_with(event.first, "all ")
               && boost::algorithm::icontains(event.first, " execution backends ")
               && boost::algorithm::iends_with(event.first, " started")) {
      events.insert_or_assign(ALL_BACKENDS_STARTED, event.second);
    } else if (boost::algorithm::iequals(event.first, "rows available")) {
      events.insert_or_assign(ROWS_AVAILABLE, event.second);
    } else if (boost::algorithm::iequals(event.first, "first row fetched")) {
      events.insert_or_assign(FIRST_ROW_FETCHED, event.second);
    } else if (boost::algorithm::iequals(event.first, "last row fetched")) {
      events.insert_or_assign(LAST_ROW_FETCHED, event.second);
    } else if (boost::algorithm::iequals(event.first, "unregister query")) {
      events.insert_or_assign(UNREGISTER_QUERY, event.second);
    }
  }
} // QueryStateExpanded constructor

bool QueryStateExpanded::events_timeline_empty() const {
  DCHECK(base_state->event_sequence.labels.size() ==
      base_state->event_sequence.timestamps.size());
  return base_state->event_sequence.labels.empty() ||
      base_state->event_sequence.timestamps.empty();
}

bool PerHostPeakMemoryComparator(const pair<TNetworkAddress, PerHostState>& a,
    const pair<TNetworkAddress, PerHostState>& b) {
  return a.second.peak_memory_usage < b.second.peak_memory_usage;
}

/// Events Timeline Iterator
EventsTimelineIterator::EventsTimelineIterator(const std::vector<std::string>* labels,
      const std::vector<std::int64_t>* timestamps) :
      EventsTimelineIterator(labels, timestamps, 0) {}

EventsTimelineIterator::EventsTimelineIterator(const std::vector<std::string>* labels,
      const std::vector<std::int64_t>* timestamps, size_t cur) : labels_(labels),
      timestamps_(timestamps), cur_(cur) {
  DCHECK(labels != nullptr);
  DCHECK(timestamps != nullptr);
  DCHECK(labels->size() == timestamps->size());
}

EventsTimelineIterator QueryStateExpanded::EventsTimeline() const {
  return EventsTimelineIterator(&base_state->event_sequence.labels,
      &base_state->event_sequence.timestamps);
}

EventsTimelineIterator::iter_t EventsTimelineIterator::operator*() const {
  return make_pair(labels_->at(cur_), timestamps_->at(cur_));
}

EventsTimelineIterator& EventsTimelineIterator::operator++() {
  ++cur_;
  return *this;
}

EventsTimelineIterator EventsTimelineIterator::operator++(int) {
  ++cur_;
  return *this;
}

bool EventsTimelineIterator::operator==(const EventsTimelineIterator& other) const {
  DCHECK(labels_ == other.labels_);
  DCHECK(timestamps_ == other.timestamps_);
  return cur_ == other.cur_;
}

bool EventsTimelineIterator::operator!=(const EventsTimelineIterator& other) const {
  return !(*this == other);
}

EventsTimelineIterator EventsTimelineIterator::begin() {
  return EventsTimelineIterator(labels_, timestamps_);
}

EventsTimelineIterator EventsTimelineIterator::end() {
  return EventsTimelineIterator(labels_, timestamps_, labels_->size());
}

} // namespace impala
