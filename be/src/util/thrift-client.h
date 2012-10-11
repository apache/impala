// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_UTIL_THRIFT_CLIENT_H
#define IMPALA_UTIL_THRIFT_CLIENT_H

#include <sstream>
#include <boost/shared_ptr.hpp>
#include <common/status.h>
#include <transport/TSocket.h>
#include <transport/TBufferTransports.h>
#include <transport/TSaslClientTransport.h>
#include <transport/TSasl.h>
#include <protocol/TBinaryProtocol.h>
#include <sstream>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "util/thrift-server.h"
#include "util/authorization.h"

DECLARE_string(principal);
DECLARE_string(hostname);
DECLARE_bool(use_nonblocking);

namespace impala {
// Super class for templatized thrift clients.
class ThriftClientImpl {
 public:
  ~ThriftClientImpl() {
    Close();
  }
  const std::string& ipaddress() { return ipaddress_; }
  int port() { return port_; }

  // Open the connection to the remote server. May be called
  // repeatedly, is idempotent unless there is a failure to connect.
  Status Open();

  // Retry the Open num_retries time waiting wait_ms milliseconds between retries.
  Status OpenWithRetry(int num_retries, int wait_ms);

  // Close the connection with the remote server. May be called
  // repeatedly.
  Status Close();
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


// Utility client to a Thrift server. The parameter type is the
// Thrift interface type that the server implements.
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
  // Switch to Nonblocking as the default.
  if (FLAGS_use_nonblocking && server_type == ThriftServer::Threaded) {
    server_type = ThriftServer::Nonblocking;
  }

  switch (server_type) {
    case ThriftServer::Nonblocking:
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
