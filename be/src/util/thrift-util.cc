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

#include "util/thrift-util.h"

#include <Thrift.h>
#include <transport/TSocket.h>

#include <boost/shared_ptr.hpp>
#include <boost/thread.hpp>
#include <server/TNonblockingServer.h>
#include <transport/TServerSocket.h>
#include <concurrency/ThreadManager.h>
#include <concurrency/PosixThreadFactory.h>

#include "util/hash-util.h"
#include "util/thrift-server.h"
#include "gen-cpp/Types_types.h"

using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::transport;
using namespace apache::thrift::server;
using namespace apache::thrift::protocol;
using namespace apache::thrift::concurrency;
using namespace boost;

namespace impala {

// Comparator for THostPorts. Thrift declares this (in gen-cpp/Types_types.h) but
// never defines it.
bool THostPort::operator<(const THostPort& that) const {
  if (this->ipaddress < that.ipaddress) {
    return true;
  } else if ((this->ipaddress == that.ipaddress) && (this->port < that.port)) {
    return true;
  }
  return false;
};

static void ThriftOutputFunction(const char* output) {
  VLOG_QUERY << output;
}

void InitThriftLogging() {
  GlobalOutput.setOutputFunction(ThriftOutputFunction);
}

Status WaitForLocalServer(const ThriftServer& server, int num_retries,
    int retry_interval_ms) {
  return WaitForServer("localhost", server.port(), num_retries, retry_interval_ms);
}

Status WaitForServer(const string& host, int port, int num_retries,
    int retry_interval_ms) {
  int retry_count = 0;
  while (retry_count < num_retries) {
    try {
      TSocket socket(host, port);
      // Timeout is in ms
      socket.setConnTimeout(500);
      socket.open();
      socket.close();
      return Status::OK;
    } catch (TTransportException& e) {
      VLOG_QUERY << "Connection failed: " << e.what();
    }
    ++retry_count;
    VLOG_QUERY << "Waiting " << retry_interval_ms << "ms for Thrift server at "
               << host << ":" << port
               << " to come up, failed attempt " << retry_count
               << " of " << num_retries;
    usleep(retry_interval_ms * 1000);
  }
  return Status("Server did not come up");
}

void THostPortToString(const THostPort& address, string* out) {
  stringstream ss;
  ss << address.ipaddress << ":" << address.port;
  *out = ss.str();
}

std::ostream& operator<<(std::ostream& out, const THostPort& hostport) {
  out << hostport.ipaddress << ":" << hostport.port;
  return out;
}

}
