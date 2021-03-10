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

#ifndef IMPALA_TESTUTIL_IN_PROCESS_SERVERS_H
#define IMPALA_TESTUTIL_IN_PROCESS_SERVERS_H

#include <boost/scoped_ptr.hpp>

#include "common/status.h"
#include "util/thread.h"
#include "runtime/exec-env.h"

namespace impala {

class ImpalaServer;
class ThriftServer;
class Webserver;
class MetricGroup;
class Statestore;

/// A single impala service, with a backend server, two client servers,
/// a webserver and optionally a connection to a statestore.
//
/// TODO: Static StartCluster method which runs one or more
/// ImpalaServer(s) and an optional Statestore.
/// TODO: Fix occasional abort when object is destroyed.
class InProcessImpalaServer {
 public:
  /// Initialises the server, but does not start any network-attached
  /// services or run any threads.
  InProcessImpalaServer(const std::string& hostname, int krpc_port, int subscriber_port,
      int webserver_port, const std::string& statestore_host, int statestore_port);

  /// Starts an in-process Impala server with ephemeral ports that are independent of the
  /// ports used by a concurrently running normal Impala daemon. The hostname is set to
  /// "localhost" and the ports are picked from the ephemeral port range and exposed as
  /// member variables. Internally this will call StartWithClientservers() and
  /// SetCatalogInitialized(). The default values for statestore_host and statestore_port
  /// indicate that a statestore connection should not be used. These values are directly
  /// forwarded to the ExecEnv. Returns ok and sets *server on success. On failure returns
  /// an error. *server may or may not be set on error, but is always invalid to use.
  static Status StartWithEphemeralPorts(const std::string& statestore_host,
      int statestore_port, InProcessImpalaServer** server);

  /// Starts all servers, including the beeswax and hs2 client
  /// servers.
  Status StartWithClientServers(int beeswax_port, int hs2_port, int hs2_http_port);

  /// Blocks until the backend server exits. Returns Status::OK unless
  /// there was an error joining.
  Status Join();

  ImpalaServer* impala_server() { return impala_server_.get(); }

  MetricGroup* metrics() { return exec_env_->metrics(); }

  /// Sets the catalog on this impalad to be initialized. If we don't
  /// start up a catalogd, then there is no one to initialize it otherwise.
  void SetCatalogIsReady();

  /// Get the beeswax port of the running server.
  int GetBeeswaxPort() const;

  /// Get the HS2 port of the running server.
  int GetHS2Port() const;

 private:
  uint32_t krpc_port_;

  uint32_t beeswax_port_;

  uint32_t hs2_port_;

  uint32_t hs2_http_port_;

  /// The ImpalaServer that handles client and backend requests.
  std::shared_ptr<ImpalaServer> impala_server_;

  /// ExecEnv holds much of the per-service state
  boost::scoped_ptr<ExecEnv> exec_env_;
};

}

#endif
