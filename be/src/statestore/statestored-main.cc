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

#include "common/init.h"
#include "common/logging.h"
#include "common/status.h"
#include "rpc/rpc-trace.h"
#include "runtime/mem-tracker.h"
#include "statestore/statestore.h"
#include "util/common-metrics.h"
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
DECLARE_string(ssl_cipher_list);
DECLARE_string(ssl_minimum_version);

#include "common/names.h"

using namespace impala;
using namespace apache::thrift;
using namespace apache::thrift::transport;

int StatestoredMain(int argc, char** argv) {
  // Override default for webserver port
  FLAGS_webserver_port = 25010;
  InitCommonRuntime(argc, argv, false);

  MemTracker mem_tracker;
  scoped_ptr<Webserver> webserver(new Webserver());
  scoped_ptr<MetricGroup> metrics(new MetricGroup("statestore"));

  if (FLAGS_enable_webserver) {
    AddDefaultUrlCallbacks(webserver.get(), &mem_tracker, metrics.get());
    ABORT_IF_ERROR(webserver->Start());
  } else {
    LOG(INFO) << "Not starting webserver";
  }

  ABORT_IF_ERROR(
      metrics->Init(FLAGS_enable_webserver ? webserver.get() : nullptr));
  ABORT_IF_ERROR(RegisterMemoryMetrics(metrics.get(), false, nullptr, nullptr));
  ABORT_IF_ERROR(StartMemoryMaintenanceThread());
  ABORT_IF_ERROR(
    StartThreadInstrumentation(metrics.get(), webserver.get(), false));
  InitRpcEventTracing(webserver.get());
  // TODO: Add a 'common metrics' method to add standard metrics to
  // both statestored and impalad
  metrics->AddProperty<string>("statestore.version", GetVersionString(true));

  CommonMetrics::InitCommonMetrics(metrics.get());

  Statestore statestore(metrics.get());
  ABORT_IF_ERROR(statestore.Init());
  statestore.RegisterWebpages(webserver.get());
  boost::shared_ptr<TProcessor> processor(
      new StatestoreServiceProcessor(statestore.thrift_iface()));
  boost::shared_ptr<TProcessorEventHandler> event_handler(
      new RpcEventHandler("statestore", metrics.get()));
  processor->setEventHandler(event_handler);

  ThriftServer* server;
  ThriftServerBuilder builder("StatestoreService", processor, FLAGS_state_store_port);
  if (EnableInternalSslConnections()) {
    SSLProtocol ssl_version;
    ABORT_IF_ERROR(
        SSLProtoVersions::StringToProtocol(FLAGS_ssl_minimum_version, &ssl_version));
    LOG(INFO) << "Enabling SSL for Statestore";
    builder.ssl(FLAGS_ssl_server_certificate, FLAGS_ssl_private_key)
        .pem_password_cmd(FLAGS_ssl_private_key_password_cmd)
        .ssl_version(ssl_version)
        .cipher_list(FLAGS_ssl_cipher_list);
  }
  ABORT_IF_ERROR(builder.metrics(metrics.get()).Build(&server));
  ABORT_IF_ERROR(server->Start());

  statestore.MainLoop();

  return 0;
}
