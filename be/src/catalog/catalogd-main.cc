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

#include <gflags/gflags.h>
#include <jni.h>

#include "catalog/catalog-server.h"
#include "common/daemon-env.h"
#include "common/init.h"
#include "common/status.h"
#include "rpc/authentication.h"
#include "rpc/rpc-trace.h"
#include "rpc/thrift-server.h"
#include "service/fe-support.h"
#include "util/debug-util.h"
#include "util/event-metrics.h"
#include "util/jni-util.h"
#include "util/memory-metrics.h"
#include "util/metrics.h"
#include "util/network-util.h"
#include "util/openssl-util.h"

DECLARE_int32(catalog_service_port);
DECLARE_int32(metrics_webserver_port);
DECLARE_int32(webserver_port);
DECLARE_int32(state_store_subscriber_port);
DECLARE_string(ssl_server_certificate);
DECLARE_string(ssl_private_key);
DECLARE_string(ssl_private_key_password_cmd);
DECLARE_string(ssl_cipher_list);
DECLARE_string(ssl_minimum_version);
#include "common/names.h"

using namespace impala;
using namespace apache::thrift;

int CatalogdMain(int argc, char** argv) {
  // Set webserver_port as 25020 and state_store_subscriber_port as 23020 for catalogd
  // if and only if these two ports had not been set explicitly in command line.
  // An Impala cluster could be launched with more than one catalogd instances when
  // CatalogD HA is enabled. These two ports should be set explicitly in command line
  // with different values for each catalogd instance when launching a mini-cluster.
  if (google::GetCommandLineFlagInfoOrDie("webserver_port").is_default) {
    FLAGS_webserver_port = 25020;
  }
  if (google::GetCommandLineFlagInfoOrDie("state_store_subscriber_port").is_default) {
    FLAGS_state_store_subscriber_port = 23020;
  }

  InitCommonRuntime(argc, argv, true);
  InitFeSupport();

  DaemonEnv daemon_env("catalog");
  ABORT_IF_ERROR(daemon_env.Init(/* init_jvm */ true));
  MetastoreEventMetrics::InitMetastoreEventMetrics(daemon_env.metrics());

  CatalogServer catalog_server(daemon_env.metrics());
  ABORT_IF_ERROR(catalog_server.Start());
  catalog_server.RegisterWebpages(daemon_env.webserver(), false);
  if (FLAGS_metrics_webserver_port > 0) {
    catalog_server.RegisterWebpages(daemon_env.metrics_webserver(), true);
  }
  std::shared_ptr<TProcessor> processor(
      new CatalogServiceProcessor(catalog_server.thrift_iface()));
  std::shared_ptr<TProcessorEventHandler> event_handler(
      new RpcEventHandler("catalog-server", daemon_env.metrics()));
  processor->setEventHandler(event_handler);

  ThriftServer* server;
  ThriftServerBuilder builder("CatalogService", processor, FLAGS_catalog_service_port);
  // Mark this as an internal service to use a more permissive Thrift max message size
  builder.is_external_facing(false);

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
  ABORT_IF_ERROR(builder.metrics(daemon_env.metrics()).Build(&server));
  ABORT_IF_ERROR(server->Start());
  catalog_server.MarkServiceAsStarted();
  LOG(INFO) << "CatalogService started on port: " << FLAGS_catalog_service_port;
  server->Join();

  return 0;
}
