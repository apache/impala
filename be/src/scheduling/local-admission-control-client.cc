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

#include "scheduling/local-admission-control-client.h"

#include "runtime/exec-env.h"
#include "util/runtime-profile-counters.h"
#include "util/uid-util.h"

#include "common/names.h"

namespace impala {

LocalAdmissionControlClient::LocalAdmissionControlClient(const TUniqueId& query_id) {
  TUniqueIdToUniqueIdPB(query_id, &query_id_);
}

Status LocalAdmissionControlClient::SubmitForAdmission(
    const AdmissionController::AdmissionRequest& request,
    RuntimeProfile::EventSequence* query_events,
    std::unique_ptr<QuerySchedulePB>* schedule_result,
    int64_t* wait_start_time_ms, int64_t* wait_end_time_ms) {
  ScopedEvent completedEvent(
      query_events, AdmissionControlClient::QUERY_EVENT_COMPLETED_ADMISSION);
  query_events->MarkEvent(QUERY_EVENT_SUBMIT_FOR_ADMISSION);
  bool queued;
  Status status = ExecEnv::GetInstance()->admission_controller()->SubmitForAdmission(
      request, &admit_outcome_, schedule_result, queued);
  if (queued) {
    query_events->MarkEvent(QUERY_EVENT_QUEUED);
    DCHECK(status.ok());
    status = ExecEnv::GetInstance()->admission_controller()->WaitOnQueued(
        request.query_id, schedule_result, /*timeout_ms*/ 0, /*wait_timed_out*/ nullptr,
        wait_start_time_ms, wait_end_time_ms);
  }
  return status;
}

void LocalAdmissionControlClient::ReleaseQuery(int64_t peak_mem_consumption) {
  ExecEnv::GetInstance()->admission_controller()->ReleaseQuery(
      query_id_, ExecEnv::GetInstance()->backend_id(), peak_mem_consumption);
}

void LocalAdmissionControlClient::ReleaseQueryBackends(
    const vector<NetworkAddressPB>& host_addrs) {
  ExecEnv::GetInstance()->admission_controller()->ReleaseQueryBackends(
      query_id_, ExecEnv::GetInstance()->backend_id(), host_addrs);
}

void LocalAdmissionControlClient::CancelAdmission() {
  admit_outcome_.Set(AdmissionOutcome::CANCELLED);
}

} // namespace impala
