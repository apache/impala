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
#include "util/openssl-util.h"
#include "runtime/exec-env.h"
#include "scheduling/scheduler.h"
#include "service/frontend.h"
#include "service/impala-server.h"

#include "common/names.h"

DECLARE_string(ssl_server_certificate);
DECLARE_string(ssl_private_key);
DECLARE_int32(krpc_port);
DECLARE_bool(rpc_use_unix_domain_socket);

using namespace apache::thrift;
using namespace impala;

Status InProcessImpalaServer::StartWithEphemeralPorts(const string& statestore_host,
    int statestore_port, InProcessImpalaServer** server) {
  if (FLAGS_rpc_use_unix_domain_socket) {
    // IMPALA-11129: This test utility function call WaitForServer() to wait KRPC server
    // to start. WaitForServer(), which is defined in be/src/rpc/thrift-util.c, use
    // TSocket (Thrift Socket) to connect KRPC server. Even TSocket provide a constructor
    // with Unix Domain Socket address as parameter, but TSocket don't support UDS
    // address in the form of name in "Abstract Namespace". So we have to disable KRPC
    // running over UDS for this unit-test by setting FLAGS_rpc_use_unix_domain_socket
    // as false.
    // This function is called by backend unit tests:
    //   be/src/service/session-expiry-test.cc and be/src/exprs/expr-test.cc.
    FLAGS_rpc_use_unix_domain_socket = false;
  }

  // This flag is read directly in several places to find the address of the backend
  // interface, so we must set it here.
  FLAGS_krpc_port = FindUnusedEphemeralPort();

  *server = new InProcessImpalaServer(
      FLAGS_hostname, FLAGS_krpc_port, 0, 0, statestore_host, statestore_port);
  // Start the daemon and check if it works, if not delete the current server object and
  // pick a new set of ports
  return (*server)->StartWithClientServers(0, 0, 0);
}

InProcessImpalaServer::InProcessImpalaServer(const string& hostname, int krpc_port,
    int subscriber_port, int webserver_port, const string& statestore_host,
    int statestore_port)
  : krpc_port_(krpc_port),
    beeswax_port_(0),
    hs2_port_(0),
    hs2_http_port_(0),
    impala_server_(NULL),
    exec_env_(new ExecEnv(
        krpc_port, subscriber_port, webserver_port, statestore_host, statestore_port)) {}

void InProcessImpalaServer::SetCatalogIsReady() {
  DCHECK(impala_server_ != NULL) << "Call Start*() first.";
  exec_env_->frontend()->SetCatalogIsReady();
}

Status InProcessImpalaServer::StartWithClientServers(
    int beeswax_port, int hs2_port, int hs2_http_port) {
  RETURN_IF_ERROR(exec_env_->Init());
  beeswax_port_ = beeswax_port;
  hs2_port_ = hs2_port;
  hs2_http_port_ = hs2_http_port;

  impala_server_.reset(new ImpalaServer(exec_env_.get()));
  SetCatalogIsReady();
  RETURN_IF_ERROR(impala_server_->Start(beeswax_port, hs2_port, hs2_http_port_, 0));

  // Wait for up to 1s for the backend server to start
  RETURN_IF_ERROR(WaitForServer(FLAGS_hostname, krpc_port_, 10, 100));
  return Status::OK();
}

Status InProcessImpalaServer::Join() {
  impala_server_->Join();
  return Status::OK();
}

int InProcessImpalaServer::GetBeeswaxPort() const {
  return impala_server_->GetBeeswaxPort();
}

int InProcessImpalaServer::GetHS2Port() const {
  return impala_server_->GetHS2Port();
}
