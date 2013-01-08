// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//
// This file contains the main() function for the state store process,
// which exports the Thrift service StateStoreService.

#include <iostream>
#include <string>

#include "common/daemon.h"
#include "common/logging.h"
#include "common/status.h"
#include "statestore/state-store.h"
#include "util/metrics.h"
#include "util/webserver.h"
#include "util/default-path-handlers.h"

DECLARE_int32(state_store_port);
DECLARE_int32(webserver_port);
DECLARE_bool(enable_webserver);
DECLARE_string(principal);

using namespace std;
using namespace boost;
using namespace impala;

int main(int argc, char** argv) {
  // Override default for webserver port
  FLAGS_webserver_port = 25010;
  InitDaemon(argc, argv);

  if (!FLAGS_principal.empty()) {
    EXIT_IF_ERROR(InitKerberos("StateStore"));
  }

  scoped_ptr<Webserver> webserver(new Webserver());

  if (FLAGS_enable_webserver) {
    AddDefaultPathHandlers(webserver.get());
    EXIT_IF_ERROR(webserver->Start());
  } else {
    LOG(INFO) << "Not starting webserver";
  }

  scoped_ptr<Metrics> metrics(new Metrics());
  metrics->Init(FLAGS_enable_webserver ? webserver.get() : NULL);

  shared_ptr<StateStore> state_store(
      new StateStore(StateStore::DEFAULT_UPDATE_FREQUENCY_MS,
          metrics.get()));
  state_store->RegisterWebpages(webserver.get());
  state_store->Start(FLAGS_state_store_port);
  state_store->WaitForServerToStop();
}
