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

#include "testutil/in-process-servers.h"

#include <boost/scoped_ptr.hpp>
#include <stdlib.h>

#include "statestore/statestore.h"
#include "rpc/thrift-util.h"
#include "rpc/thrift-server.h"
#include "util/network-util.h"
#include "util/webserver.h"
#include "util/default-path-handlers.h"
#include "util/metrics.h"
#include "runtime/exec-env.h"
#include "service/impala-server.h"

#include "common/names.h"

DECLARE_string(ssl_server_certificate);
DECLARE_string(ssl_private_key);
DECLARE_int32(be_port);
DECLARE_int32(krpc_port);

using namespace apache::thrift;
using namespace impala;

InProcessImpalaServer* InProcessImpalaServer::StartWithEphemeralPorts(
    const string& statestore_host, int statestore_port) {
  for (int tries = 0; tries < 10; ++tries) {
    vector<int> used_ports;
    int backend_port = FindUnusedEphemeralPort(&used_ports);
    if (backend_port == -1) continue;
    // This flag is read directly in several places to find the address of the local
    // backend interface.
    FLAGS_be_port = backend_port;

    int krpc_port = FindUnusedEphemeralPort(&used_ports);
    if (krpc_port == -1) continue;
    FLAGS_krpc_port = krpc_port;

    int subscriber_port = FindUnusedEphemeralPort(&used_ports);
    if (subscriber_port == -1) continue;

    int webserver_port = FindUnusedEphemeralPort(&used_ports);
    if (webserver_port == -1) continue;

    int beeswax_port = FindUnusedEphemeralPort(&used_ports);
    if (beeswax_port == -1) continue;

    int hs2_port = FindUnusedEphemeralPort(&used_ports);
    if (hs2_port == -1) continue;

    InProcessImpalaServer* impala = new InProcessImpalaServer(FLAGS_hostname,
        backend_port, krpc_port, subscriber_port, webserver_port, statestore_host,
        statestore_port);
    // Start the daemon and check if it works, if not delete the current server object and
    // pick a new set of ports
    Status started = impala->StartWithClientServers(beeswax_port, hs2_port);
    if (started.ok()) {
      const Status status = impala->SetCatalogInitialized();
      if (!status.ok()) LOG(WARNING) << status.GetDetail();
      return impala;
    }
    delete impala;
  }
  DCHECK(false) << "Could not find port to start Impalad.";
  return NULL;
}

InProcessImpalaServer::InProcessImpalaServer(const string& hostname, int backend_port,
    int krpc_port, int subscriber_port, int webserver_port, const string& statestore_host,
    int statestore_port)
    : hostname_(hostname), backend_port_(backend_port),
      beeswax_port_(0),
      hs2_port_(0),
      impala_server_(NULL),
      exec_env_(new ExecEnv(hostname, backend_port, krpc_port, subscriber_port,
          webserver_port, statestore_host, statestore_port)) {
}

Status InProcessImpalaServer::SetCatalogInitialized() {
  DCHECK(impala_server_ != NULL) << "Call Start*() first.";
  return exec_env_->frontend()->SetCatalogInitialized();
}

Status InProcessImpalaServer::StartWithClientServers(int beeswax_port, int hs2_port) {
  RETURN_IF_ERROR(exec_env_->Init());
  beeswax_port_ = beeswax_port;
  hs2_port_ = hs2_port;

  impala_server_.reset(new ImpalaServer(exec_env_.get()));
  RETURN_IF_ERROR(impala_server_->Init(backend_port_, beeswax_port, hs2_port));
  RETURN_IF_ERROR(impala_server_->Start());

  // Wait for up to 1s for the backend server to start
  RETURN_IF_ERROR(WaitForServer(hostname_, backend_port_, 10, 100));
  return Status::OK();
}

Status InProcessImpalaServer::Join() {
  impala_server_->Join();
  return Status::OK();
}

InProcessStatestore* InProcessStatestore::StartWithEphemeralPorts() {
  for (int tries = 0; tries < 10; ++tries) {
    vector<int> used_ports;
    int statestore_port = FindUnusedEphemeralPort(&used_ports);
    if (statestore_port == -1) continue;

    int webserver_port = FindUnusedEphemeralPort(&used_ports);
    if (webserver_port == -1) continue;

    InProcessStatestore* ips = new InProcessStatestore(statestore_port, webserver_port);
    if (ips->Start().ok()) return ips;
    delete ips;
  }
  DCHECK(false) << "Could not find port to start Statestore.";
  return NULL;
}

InProcessStatestore::InProcessStatestore(int statestore_port, int webserver_port)
    : webserver_(new Webserver(webserver_port)),
      metrics_(new MetricGroup("statestore")),
      statestore_port_(statestore_port),
      statestore_(new Statestore(metrics_.get())) {
  AddDefaultUrlCallbacks(webserver_.get());
  statestore_->RegisterWebpages(webserver_.get());
}

Status InProcessStatestore::Start() {
  RETURN_IF_ERROR(statestore_->Init());
  RETURN_IF_ERROR(webserver_->Start());
  boost::shared_ptr<TProcessor> processor(
      new StatestoreServiceProcessor(statestore_->thrift_iface()));

  ThriftServerBuilder builder("StatestoreService", processor, statestore_port_);
  if (EnableInternalSslConnections()) {
    LOG(INFO) << "Enabling SSL for Statestore";
    builder.ssl(FLAGS_ssl_server_certificate, FLAGS_ssl_private_key);
  }
  ThriftServer* server;
  ABORT_IF_ERROR(builder.metrics(metrics_.get()).Build(&server));
  statestore_server_.reset(server);
  RETURN_IF_ERROR(Thread::Create("statestore", "main-loop",
      &Statestore::MainLoop, statestore_.get(), &statestore_main_loop_));

  RETURN_IF_ERROR(statestore_server_->Start());
  return WaitForServer("localhost", statestore_port_, 10, 100);
}
