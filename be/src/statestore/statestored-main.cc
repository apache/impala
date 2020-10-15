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
// This file contains the main() function for the state store process,
// which exports the Thrift service StatestoreService.

#include <iostream>
#include <string>

#include "common/daemon-env.h"
#include "common/init.h"
#include "common/logging.h"
#include "common/status.h"
#include "rpc/rpc-trace.h"
#include "statestore/statestore.h"
#include "util/common-metrics.h"
#include "util/debug-util.h"
#include "util/memory-metrics.h"
#include "util/metrics.h"
#include "util/webserver.h"

DECLARE_int32(state_store_port);
DECLARE_int32(webserver_port);

#include "common/names.h"

using namespace impala;

int StatestoredMain(int argc, char** argv) {
  // Override default for webserver port
  FLAGS_webserver_port = 25010;
  InitCommonRuntime(argc, argv, false);

  DaemonEnv daemon_env("statestore");
  ABORT_IF_ERROR(daemon_env.Init(/* init_jvm */ false));

  Statestore statestore(daemon_env.metrics());
  ABORT_IF_ERROR(statestore.Init(FLAGS_state_store_port));
  statestore.RegisterWebpages(daemon_env.webserver());
  statestore.MainLoop();

  return 0;
}
