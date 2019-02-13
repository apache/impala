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
// This file contains the main() function for the catalog daemon process,

#include <jni.h>
#include <boost/scoped_ptr.hpp>

#include "catalog/catalog-server.h"
#include "common/init.h"
#include "common/status.h"
#include "rpc/authentication.h"
#include "rpc/rpc-trace.h"
#include "rpc/thrift-server.h"
#include "runtime/mem-tracker.h"
#include "service/fe-support.h"
#include "util/common-metrics.h"
#include "util/debug-util.h"
#include "util/default-path-handlers.h"
#include "util/event-metrics.h"
#include "util/jni-util.h"
#include "util/memory-metrics.h"
#include "util/metrics.h"
#include "util/network-util.h"
#include "util/openssl-util.h"
#include "util/webserver.h"

DECLARE_string(classpath);
DECLARE_string(principal);
DECLARE_int32(catalog_service_port);
DECLARE_int32(webserver_port);
DECLARE_bool(enable_webserver);
DECLARE_int32(state_store_subscriber_port);
DECLARE_string(ssl_server_certificate);
DECLARE_string(ssl_private_key);
DECLARE_string(ssl_private_key_password_cmd);
DECLARE_string(ssl_cipher_list);
DECLARE_string(ssl_minimum_version);

#include "common/names.h"

using namespace impala;
using namespace apache::thrift;
using namespace apache::thrift;

int CatalogdMain(int argc, char** argv) {
  FLAGS_webserver_port = 25020;
  FLAGS_state_store_subscriber_port = 23020;
  InitCommonRuntime(argc, argv, true);
  InitFeSupport();

  MemTracker process_mem_tracker;
  scoped_ptr<Webserver> webserver(new Webserver());
  scoped_ptr<MetricGroup> metrics(new MetricGroup("catalog"));

  if (FLAGS_enable_webserver) {
    AddDefaultUrlCallbacks(webserver.get(), &process_mem_tracker, metrics.get());
    ABORT_IF_ERROR(webserver->Start());
  } else {
    LOG(INFO) << "Not starting webserver";
  }

  ABORT_IF_ERROR(metrics->Init(FLAGS_enable_webserver ? webserver.get() : nullptr));
  ABORT_IF_ERROR(RegisterMemoryMetrics(metrics.get(), true, nullptr, nullptr));
  ABORT_IF_ERROR(StartMemoryMaintenanceThread());
  ABORT_IF_ERROR(StartThreadInstrumentation(metrics.get(), webserver.get(), true));

  InitRpcEventTracing(webserver.get());
  metrics->AddProperty<string>("catalog.version", GetVersionString(true));

  CommonMetrics::InitCommonMetrics(metrics.get());
  MetastoreEventMetrics::InitMetastoreEventMetrics(metrics.get());

  CatalogServer catalog_server(metrics.get());
  ABORT_IF_ERROR(catalog_server.Start());
  catalog_server.RegisterWebpages(webserver.get());
  boost::shared_ptr<TProcessor> processor(
      new CatalogServiceProcessor(catalog_server.thrift_iface()));
  boost::shared_ptr<TProcessorEventHandler> event_handler(
      new RpcEventHandler("catalog-server", metrics.get()));
  processor->setEventHandler(event_handler);

  ThriftServer* server;
  ThriftServerBuilder builder("CatalogService", processor, FLAGS_catalog_service_port);

  if (IsInternalTlsConfigured()) {
    SSLProtocol ssl_version;
    ABORT_IF_ERROR(
        SSLProtoVersions::StringToProtocol(FLAGS_ssl_minimum_version, &ssl_version));
    LOG(INFO) << "Enabling SSL for CatalogService";
    builder.ssl(FLAGS_ssl_server_certificate, FLAGS_ssl_private_key)
        .pem_password_cmd(FLAGS_ssl_private_key_password_cmd)
        .ssl_version(ssl_version)
        .cipher_list(FLAGS_ssl_cipher_list);
  }
  ABORT_IF_ERROR(builder.metrics(metrics.get()).Build(&server));
  ABORT_IF_ERROR(server->Start());
  LOG(INFO) << "CatalogService started on port: " << FLAGS_catalog_service_port;
  server->Join();

  return 0;
}
