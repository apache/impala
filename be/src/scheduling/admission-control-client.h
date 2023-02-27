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
#include "scheduling/admission-controller.h"

namespace impala {

// Base class used to abstract out the logic for submitting queries to an admission
// controller running locally or to one running remotely.
class AdmissionControlClient {
 public:
  static const std::string QUERY_EVENT_SUBMIT_FOR_ADMISSION;
  static const std::string QUERY_EVENT_QUEUED;
  static const std::string QUERY_EVENT_COMPLETED_ADMISSION;

  // Creates a new AdmissionControlClient and returns it in 'client'.
  static void Create(
      const TQueryCtx& query_ctx, std::unique_ptr<AdmissionControlClient>* client);

  virtual ~AdmissionControlClient() {}

  // Called to schedule and admit the query. Blocks until an admission decision is made.
  virtual Status SubmitForAdmission(const AdmissionController::AdmissionRequest& request,
      RuntimeProfile::EventSequence* query_events,
      std::unique_ptr<QuerySchedulePB>* schedule_result,
      int64_t* wait_start_time_ms, int64_t* wait_end_time_ms) = 0;

  // Called when the query has completed to release all of its resources.
  virtual void ReleaseQuery(int64_t peak_mem_consumption) = 0;

  // Called with a list of backends the query has completed on, to release the resources
  // for the query on those backends.
  virtual void ReleaseQueryBackends(const std::vector<NetworkAddressPB>& host_addr) = 0;

  // Called to cancel admission for the query.
  virtual void CancelAdmission() = 0;
};

} // namespace impala
