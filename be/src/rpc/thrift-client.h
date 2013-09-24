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


#ifndef IMPALA_RPC_THRIFT_CLIENT_H
#define IMPALA_RPC_THRIFT_CLIENT_H

#include <ostream>
#include <sstream>
#include <boost/shared_ptr.hpp>
#include <common/status.h>
#include <thrift/Thrift.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <sstream>
#include <gflags/gflags.h>

#include "transport/TSaslClientTransport.h"
#include "transport/TSasl.h"
#include "rpc/thrift-server.h"
#include "util/authorization.h"
#include "gen-cpp/Types_types.h"

DECLARE_string(principal);
DECLARE_string(hostname);

namespace impala {
// Super class for templatized thrift clients.
class ThriftClientImpl {
 public:
  ~ThriftClientImpl() {
    Close();
  }
  const std::string& ipaddress() { return ipaddress_; }
  int port() { return port_; }

  // Open the connection to the remote server. May be called repeatedly, is idempotent
  // unless there is a failure to connect.
  Status Open();

  // Retry the Open num_retries time waiting wait_ms milliseconds between retries.
  // If num_retries == 0, the connection is retried indefinitely.
  Status OpenWithRetry(uint32_t num_retries, uint64_t wait_ms);

  // Close the connection with the remote server. May be called repeatedly.
  void Close();

 protected:
  ThriftClientImpl(const std::string& ipaddress, int port)
    : ipaddress_(ipaddress),
      port_(port),
      socket_(new apache::thrift::transport::TSocket(ipaddress, port)) {
  }
  std::string ipaddress_;
  int port_;

  // Sasl Client object.  Contains client kerberos identification data.
  // Will be NULL if kerberos is not being used.
  boost::shared_ptr<sasl::TSasl> sasl_client_;

  // All shared pointers, because Thrift requires them to be
  boost::shared_ptr<apache::thrift::transport::TSocket> socket_;
  boost::shared_ptr<apache::thrift::transport::TTransport> transport_;
  boost::shared_ptr<apache::thrift::protocol::TBinaryProtocol> protocol_;
};


// Utility client to a Thrift server. The parameter type is the Thrift interface type that
// the server implements.
template <class InterfaceType>
class ThriftClient : public ThriftClientImpl{
 public:
  ThriftClient(const std::string& ipaddress, int port,
      ThriftServer::ServerType server_type = ThriftServer::Threaded);

  // Returns the object used to actually make RPCs against the remote server
  InterfaceType* iface() { return iface_.get(); }

 private:
  boost::shared_ptr<InterfaceType> iface_;
};

template <class InterfaceType>
ThriftClient<InterfaceType>::ThriftClient(
    const std::string& ipaddress, int port, ThriftServer::ServerType server_type)
      : ThriftClientImpl(ipaddress, port),
        iface_(new InterfaceType(protocol_)) {

  switch (server_type) {
    case ThriftServer::Nonblocking:
      // The Nonblocking server is disabled at this time.  There are
      // issues with the framed protocol throwing negative frame size errors.
      LOG(WARNING) << "Nonblocking server usage is experimental";
      if (!FLAGS_principal.empty()) {
        LOG(ERROR) << "Nonblocking servers cannot be used with Kerberos";
      }
      transport_.reset(new apache::thrift::transport::TFramedTransport(socket_));
      break;
    case ThriftServer::ThreadPool:
    case ThriftServer::Threaded:
      transport_.reset(new apache::thrift::transport::TBufferedTransport(socket_));
      break;
    default:
      std::stringstream error_msg;
      error_msg << "Unsupported server type: " << server_type;
      LOG(ERROR) << error_msg.str();
      DCHECK(false);
      break;
  }
  // Check to enable kerberos
  if (!FLAGS_principal.empty()) {
    GetTSaslClient(ipaddress_, &sasl_client_);
    transport_.reset(new apache::thrift::transport::TSaslClientTransport(
        sasl_client_, transport_));
  }

  protocol_.reset(new apache::thrift::protocol::TBinaryProtocol(transport_));
  iface_.reset(new InterfaceType(protocol_));
}

}
#endif
