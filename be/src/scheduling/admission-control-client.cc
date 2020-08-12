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

#include "scheduling/admission-control-client.h"

#include "runtime/exec-env.h"
#include "scheduling/local-admission-control-client.h"
#include "scheduling/remote-admission-control-client.h"

#include "common/names.h"

namespace impala {

// Profile query events
const string AdmissionControlClient::QUERY_EVENT_SUBMIT_FOR_ADMISSION =
    "Submit for admission";
const string AdmissionControlClient::QUERY_EVENT_QUEUED = "Queued";
const string AdmissionControlClient::QUERY_EVENT_COMPLETED_ADMISSION =
    "Completed admission";

void AdmissionControlClient::Create(
    const TQueryCtx& query_ctx, unique_ptr<AdmissionControlClient>* client) {
  if (ExecEnv::GetInstance()->AdmissionServiceEnabled()) {
    client->reset(new RemoteAdmissionControlClient(query_ctx));
  } else {
    client->reset(new LocalAdmissionControlClient(query_ctx.query_id));
  }
}

} // namespace impala
