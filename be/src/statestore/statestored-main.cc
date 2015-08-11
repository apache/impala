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
// which exports the Thrift service StatestoreService.

#include <iostream>
#include <string>

#include "common/init.h"
#include "common/logging.h"
#include "common/status.h"
#include "rpc/rpc-trace.h"
#include "runtime/mem-tracker.h"
#include "statestore/statestore.h"
#include "util/debug-util.h"
#include "util/metrics.h"
#include "util/memory-metrics.h"
#include "util/webserver.h"
#include "util/default-path-handlers.h"

DECLARE_int32(state_store_port);
DECLARE_int32(webserver_port);
DECLARE_bool(enable_webserver);
DECLARE_string(principal);
DECLARE_string(ssl_server_certificate);
DECLARE_string(ssl_private_key);
DECLARE_string(ssl_private_key_password_cmd);

#include "common/names.h"

using namespace impala;
using namespace apache::thrift;

int main(int argc, char** argv) {
  // Override default for webserver port
  FLAGS_webserver_port = 25010;
  InitCommonRuntime(argc, argv, false);

  MemTracker mem_tracker;
  scoped_ptr<Webserver> webserver(new Webserver());

  if (FLAGS_enable_webserver) {
    AddDefaultUrlCallbacks(webserver.get(), &mem_tracker);
    EXIT_IF_ERROR(webserver->Start());
  } else {
    LOG(INFO) << "Not starting webserver";
  }

  scoped_ptr<MetricGroup> metrics(new MetricGroup("statestore"));
  metrics->Init(FLAGS_enable_webserver ? webserver.get() : NULL);
  EXIT_IF_ERROR(RegisterMemoryMetrics(metrics.get(), false));
  StartThreadInstrumentation(metrics.get(), webserver.get());
  InitRpcEventTracing(webserver.get());
  // TODO: Add a 'common metrics' method to add standard metrics to
  // both statestored and impalad
  metrics->AddProperty<string>("statestore.version", GetVersionString(true));

  Statestore statestore(metrics.get());
  statestore.RegisterWebpages(webserver.get());
  shared_ptr<TProcessor> processor(
      new StatestoreServiceProcessor(statestore.thrift_iface()));
  shared_ptr<TProcessorEventHandler> event_handler(
      new RpcEventHandler("statestore", metrics.get()));
  processor->setEventHandler(event_handler);

  ThriftServer* server = new ThriftServer("StatestoreService", processor,
      FLAGS_state_store_port, NULL, metrics.get(), 5);
  if (!FLAGS_ssl_server_certificate.empty()) {
    LOG(INFO) << "Enabling SSL for Statestore";
    EXIT_IF_ERROR(server->EnableSsl(FLAGS_ssl_server_certificate, FLAGS_ssl_private_key,
        FLAGS_ssl_private_key_password_cmd));
  }
  EXIT_IF_ERROR(server->Start());

  statestore.MainLoop();
}
