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
// This file contains the main() function for the catalog daemon process,

#include <jni.h>
#include <boost/scoped_ptr.hpp>

#include "catalog/catalog-server.h"
#include "common/init.h"
#include "common/status.h"
#include "rpc/authentication.h"
#include "rpc/rpc-trace.h"
#include "rpc/thrift-util.h"
#include "rpc/thrift-server.h"
#include "runtime/mem-tracker.h"
#include "service/fe-support.h"
#include "util/debug-util.h"
#include "util/jni-util.h"
#include "util/metrics.h"
#include "util/network-util.h"
#include "util/memory-metrics.h"
#include "util/webserver.h"
#include "util/default-path-handlers.h"

DECLARE_string(classpath);
DECLARE_string(principal);
DECLARE_int32(catalog_service_port);
DECLARE_int32(webserver_port);
DECLARE_bool(enable_webserver);
DECLARE_int32(state_store_subscriber_port);
DECLARE_string(ssl_server_certificate);
DECLARE_string(ssl_private_key);
DECLARE_string(ssl_private_key_password_cmd);

#include "common/names.h"

using namespace impala;
using namespace apache::thrift;

int CatalogdMain(int argc, char** argv) {
  FLAGS_webserver_port = 25020;
  FLAGS_state_store_subscriber_port = 23020;
  InitCommonRuntime(argc, argv, true);
  InitFeSupport();

  MemTracker process_mem_tracker;
  scoped_ptr<Webserver> webserver(new Webserver());
  if (FLAGS_enable_webserver) {
    AddDefaultUrlCallbacks(webserver.get(), &process_mem_tracker);
    EXIT_IF_ERROR(webserver->Start());
  } else {
    LOG(INFO) << "Not starting webserver";
  }

  scoped_ptr<MetricGroup> metrics(new MetricGroup("catalog"));
  metrics->Init(FLAGS_enable_webserver ? webserver.get() : NULL);
  EXIT_IF_ERROR(RegisterMemoryMetrics(metrics.get(), true));
  StartThreadInstrumentation(metrics.get(), webserver.get());

  InitRpcEventTracing(webserver.get());
  metrics->AddProperty<string>("catalog.version", GetVersionString(true));

  CatalogServer catalog_server(metrics.get());
  EXIT_IF_ERROR(catalog_server.Start());
  catalog_server.RegisterWebpages(webserver.get());
  shared_ptr<TProcessor> processor(
      new CatalogServiceProcessor(catalog_server.thrift_iface()));
  shared_ptr<TProcessorEventHandler> event_handler(
      new RpcEventHandler("catalog-server", metrics.get()));
  processor->setEventHandler(event_handler);

  ThriftServer* server = new ThriftServer("CatalogService", processor,
      FLAGS_catalog_service_port, NULL, metrics.get(), 5);
  if (EnableInternalSslConnections()) {
    LOG(INFO) << "Enabling SSL for CatalogService";
    EXIT_IF_ERROR(server->EnableSsl(FLAGS_ssl_server_certificate, FLAGS_ssl_private_key,
        FLAGS_ssl_private_key_password_cmd));
  }
  EXIT_IF_ERROR(server->Start());
  LOG(INFO) << "CatalogService started on port: " << FLAGS_catalog_service_port;
  server->Join();

  return 0;
}
