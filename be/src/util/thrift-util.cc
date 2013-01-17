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

#include <boost/shared_ptr.hpp>
#include <boost/thread.hpp>
#include <thrift/Thrift.h>
#include <thrift/transport/TSocket.h>
#include <thrift/server/TNonblockingServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/concurrency/ThreadManager.h>
#include <thrift/concurrency/PosixThreadFactory.h>

#include "util/hash-util.h"
#include "util/thrift-server.h"
#include "gen-cpp/Types_types.h"
#include "gen-cpp/Data_types.h"

// TCompactProtocol requires some #defines to work right.  They also define UNLIKLEY
// so we need to undef this.
// TODO: is there a better include to use?
#ifdef UNLIKELY
#undef UNLIKELY
#endif
#define SIGNED_RIGHT_SHIFT_IS 1
#define ARITHMETIC_RIGHT_SHIFT 1
#include <thrift/protocol/TCompactProtocol.h>

using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::transport;
using namespace apache::thrift::server;
using namespace apache::thrift::protocol;
using namespace apache::thrift::concurrency;
using namespace boost;

namespace impala {

ThriftSerializer::ThriftSerializer(bool compact, int initial_buffer_size) :  
    mem_buffer_(new TMemoryBuffer(initial_buffer_size)) {
  if (compact) {
    TCompactProtocolFactoryT<TMemoryBuffer> factory;
    protocol_ = factory.getProtocol(mem_buffer_);
  } else {
    TBinaryProtocolFactoryT<TMemoryBuffer> factory;
    protocol_ = factory.getProtocol(mem_buffer_);
  }
}

shared_ptr<TProtocol> CreateDeserializeProtocol(
    shared_ptr<TMemoryBuffer> mem, bool compact) {
  if (compact) {
    TCompactProtocolFactoryT<TMemoryBuffer> tproto_factory;
    return tproto_factory.getProtocol(mem);
  } else {
    TBinaryProtocolFactoryT<TMemoryBuffer> tproto_factory;
    return tproto_factory.getProtocol(mem);
  }
}

// Comparator for THostPorts. Thrift declares this (in gen-cpp/Types_types.h) but
// never defines it.
bool TNetworkAddress::operator<(const TNetworkAddress& that) const {
  if (this->hostname < that.hostname) {
    return true;
  } else if ((this->hostname == that.hostname) && (this->port < that.port)) {
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

void TNetworkAddressToString(const TNetworkAddress& address, string* out) {
  stringstream ss;
  ss << address;
  *out = ss.str();
}

std::ostream& operator<<(std::ostream& out, const TNetworkAddress& hostport) {
  out << hostport.hostname << ":" << hostport.port;
  return out;
}

std::ostream& operator<<(std::ostream& out, const TColumnValue& colval) {
  if (colval.__isset.boolVal) {
    out << ((colval.boolVal) ? "true" : "false");
  } else if (colval.__isset.doubleVal) {
    out << colval.doubleVal;
  } else if (colval.__isset.intVal) {
    out << colval.intVal;
  } else if (colval.__isset.longVal) {
    out << colval.longVal;
  } else if (colval.__isset.stringVal) {
    out << colval.stringVal;
  } else {
    out << "NULL";
  }
  return out;
}

}
