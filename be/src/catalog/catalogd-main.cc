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
#include "rpc/thrift-util.h"
#include "rpc/thrift-server.h"
#include "util/debug-util.h"
#include "util/jni-util.h"
#include "util/metrics.h"
#include "util/mem-metrics.h"
#include "util/network-util.h"
#include "util/webserver.h"
#include "util/default-path-handlers.h"

DECLARE_string(classpath);
DECLARE_string(principal);
DECLARE_int32(catalog_service_port);
DECLARE_int32(webserver_port);
DECLARE_bool(enable_webserver);
DECLARE_int32(state_store_subscriber_port);

using namespace impala;
using namespace std;
using namespace boost;

using namespace ::apache::thrift::server;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;

int main(int argc, char** argv) {
  FLAGS_webserver_port = 25020;
  FLAGS_state_store_subscriber_port = 23020;
  InitCommonRuntime(argc, argv, true);

  EXIT_IF_ERROR(JniUtil::Init());

  scoped_ptr<Webserver> webserver(new Webserver());
  if (FLAGS_enable_webserver) {
    AddDefaultPathHandlers(webserver.get());
    EXIT_IF_ERROR(webserver->Start());
  } else {
    LOG(INFO) << "Not starting webserver";
  }

  scoped_ptr<Metrics> metrics(new Metrics());
  metrics->Init(FLAGS_enable_webserver ? webserver.get() : NULL);
  RegisterTcmallocMetrics(metrics.get());
  metrics->CreateAndRegisterPrimitiveMetric<string>(
      "catalog.version", GetVersionString(true));

  CatalogServer catalog_server(metrics.get());
  catalog_server.Start();
  catalog_server.RegisterWebpages(webserver.get());
  shared_ptr<TProcessor> processor(
      new CatalogServiceProcessor(catalog_server.thrift_iface()));

  ThriftServer* server = new ThriftServer("CatalogService", processor,
      FLAGS_catalog_service_port, NULL, metrics.get(), 5);
  EXIT_IF_ERROR(server->Start());
  LOG(INFO) << "CatalogService started on port: " << FLAGS_catalog_service_port;
  server->Join();
}
