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

//
// This file contains the main() function for the admission controller process,
// which exports the KRPC service AdmissionControlService.

#include <gutil/strings/substitute.h>

#include "common/daemon-env.h"
#include "common/init.h"
#include "common/logging.h"
#include "scheduling/admission-control-service.h"
#include "scheduling/admissiond-env.h"
#include "util/network-util.h"

DECLARE_int32(webserver_port);
DECLARE_int32(state_store_subscriber_port);

using namespace impala;
using namespace strings;

int AdmissiondMain(int argc, char** argv) {
  // Override default for webserver port
  FLAGS_webserver_port = 25030;
  FLAGS_state_store_subscriber_port = 23030;
  InitCommonRuntime(argc, argv, /* init_jvm */ true);

  DaemonEnv daemon_env("admissiond");
  ABORT_IF_ERROR(daemon_env.Init(/* init_jvm */ true));

  AdmissiondEnv admissiond_env;
  ABORT_IF_ERROR(admissiond_env.Init());
  ABORT_IF_ERROR(admissiond_env.StartServices());
  admissiond_env.admission_control_service()->Join();

  return 0;
}
