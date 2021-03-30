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
#include "util/uid-util.h"

#include "common/names.h"

namespace impala {

LocalAdmissionControlClient::LocalAdmissionControlClient(const TUniqueId& query_id) {
  TUniqueIdToUniqueIdPB(query_id, &query_id_);
}

Status LocalAdmissionControlClient::SubmitForAdmission(
    const AdmissionController::AdmissionRequest& request,
    std::unique_ptr<QuerySchedulePB>* schedule_result) {
  return ExecEnv::GetInstance()->admission_controller()->SubmitForAdmission(
      request, &admit_outcome_, schedule_result);
}

void LocalAdmissionControlClient::ReleaseQuery(int64_t peak_mem_consumption) {
  ExecEnv::GetInstance()->admission_controller()->ReleaseQuery(
      query_id_, peak_mem_consumption);
}

void LocalAdmissionControlClient::ReleaseQueryBackends(
    const vector<NetworkAddressPB>& host_addrs) {
  ExecEnv::GetInstance()->admission_controller()->ReleaseQueryBackends(
      query_id_, host_addrs);
}

void LocalAdmissionControlClient::CancelAdmission() {
  admit_outcome_.Set(AdmissionOutcome::CANCELLED);
}

} // namespace impala
