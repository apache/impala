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

#pragma once

#include <memory>
#include <vector>

#include "common/status.h"
#include "gen-cpp/Types_types.h"
#include "gen-cpp/admission_control_service.pb.h"
#include "gen-cpp/common.pb.h"
#include "scheduling/admission-control-client.h"
#include "scheduling/admission-controller.h"

namespace kudu {
class Status;
}

namespace impala {

class AdmissionControlServiceProxy;

/// Implementation of AdmissionControlClient used to submit queries for admission to an
/// AdmissionController running remotely in an admissiond.
///
/// Handles retrying of rpcs for fault tolerance:
/// - For the AdmitQuery() rpc, retries with jitter and backoff for a configurable amount
///   of time, then fails the query if unsuccessful. The default retry time was chosen as
///   a larger value (60 seconds) to minimize the number of failed queries when the
///   admissiond is restarted or temporarily unavailable.
/// - For the ReleaseQuery(), ReleaseQueryBackends(), and CancelAdmission() rpcs, retries
///   just 3 times before giving up. Failures of these rpcs are not considered to fail the
///   overall query, and there are other mechanisms in place to ensure resources are
///   eventually released regardless of failures of these rpcs, eg. AdmissionHeartbeat.
class RemoteAdmissionControlClient : public AdmissionControlClient {
 public:
  RemoteAdmissionControlClient(const TQueryCtx& query_ctx);

  virtual Status SubmitForAdmission(const AdmissionController::AdmissionRequest& request,
      RuntimeProfile::EventSequence* query_events,
      std::unique_ptr<QuerySchedulePB>* schedule_result,
      int64_t* wait_start_time_ms, int64_t* wait_end_time_ms) override;
  virtual void ReleaseQuery(int64_t peak_mem_consumption) override;
  virtual void ReleaseQueryBackends(
      const std::vector<NetworkAddressPB>& host_addr) override;
  virtual void CancelAdmission() override;

 private:
  // Owned by the ClientRequestState.
  const TQueryCtx& query_ctx_;

  // The id of the query being considered for admission.
  UniqueIdPB query_id_;

  /// Protects 'pending_admit_' and 'cancelled_'.
  std::mutex lock_;

  /// If true, the AdmitQuery rpc has been sent but a final admission decision has not yet
  /// been recieved by GetQueryStatus().
  bool pending_admit_ = false;

  /// If true, CancelAdmission() was called. If SubmitForAdmission() is called
  /// subsequently, it will not send the AdmitQuery rpc
  bool cancelled_ = false;

  /// Constants related to retrying the idempotent rpcs.
  static const int RPC_NUM_RETRIES = 3;
  static const int64_t RPC_TIMEOUT_MS = 10 * MILLIS_PER_SEC;
  static const int64_t RPC_BACKOFF_TIME_MS = 3 * MILLIS_PER_SEC;

  /// Checks if admission has already been cancelled, and if not sends the AdmitQuery rpc.
  /// Sets 'rpc_status' to the return Status from the rpc layer, and returns OK if the
  /// query was successfully submitted for admission.
  Status TryAdmitQuery(AdmissionControlServiceProxy* proxy,
      const TQueryExecRequest& request, AdmitQueryRequestPB* req,
      kudu::Status* rpc_status);
};

} // namespace impala
