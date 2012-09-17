// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_UTIL_THRIFT_CLIENT_H
#define IMPALA_UTIL_THRIFT_CLIENT_H

#include <sstream>
#include <boost/shared_ptr.hpp>
#include <common/status.h>
#include <transport/TSocket.h>
#include <transport/TBufferTransports.h>
#include <protocol/TBinaryProtocol.h>
#include <sstream>

#include "util/thrift-server.h"

namespace impala {

// Utility client to a Thrift server. The parameter type is the
// Thrift interface type that the server implements.
template <class InterfaceType>
class ThriftClient {
 public:
  ThriftClient(std::string host, int port,
      ThriftServer::ServerType server_type = ThriftServer::Nonblocking);
  ~ThriftClient();

  // Returns the object used to actually make RPCs against the remote server
  InterfaceType* iface() { return iface_.get(); }

  const std::string& host() { return host_; }
  int port() { return port_; }

  // Open the connection to the remote server. May be called
  // repeatedly, is idempotent unless there is a failure to connect.
  Status Open();

  Status OpenWithRetry(int num_retries, int wait_ms);

  // Close the connection with the remote server. May be called
  // repeatedly.
  Status Close();

 private:
  std::string host_;
  int port_;

  // All shared pointers, because Thrift requires them to be
  boost::shared_ptr<apache::thrift::transport::TSocket> socket_;
  boost::shared_ptr<apache::thrift::transport::TTransport> transport_;
  boost::shared_ptr<apache::thrift::protocol::TBinaryProtocol> protocol_;
  boost::shared_ptr<InterfaceType> iface_;
};

template <class InterfaceType>
ThriftClient<InterfaceType>::ThriftClient(std::string host, int port,
    ThriftServer::ServerType server_type)
    : host_(host),
      port_(port),
      socket_(new apache::thrift::transport::TSocket(host, port)),
      iface_(new InterfaceType(protocol_)) {
  switch (server_type) {
    case ThriftServer::Nonblocking:
      transport_.reset(new apache::thrift::transport::TFramedTransport(socket_));
      break;
    case ThriftServer::ThreadPool:
      transport_ = socket_;
      break;
    default:
      std::stringstream error_msg;
      error_msg << "Unsupported server type: " << server_type;
      LOG(ERROR) << error_msg.str();
      DCHECK(false);
      break;
  }
  protocol_.reset(new apache::thrift::protocol::TBinaryProtocol(transport_));
  iface_.reset(new InterfaceType(protocol_));
}

template <class InterfaceType>
Status ThriftClient<InterfaceType>::Open() {
  try {
    if (!transport_->isOpen()) {
      transport_->open();
    }
  } catch (apache::thrift::transport::TTransportException& e) {
    std::stringstream msg;
    msg << "Couldn't open transport for " << host() << ":" << port()
        << "(" << e.what() << ")";
    return impala::Status(msg.str());
  }
  return impala::Status::OK;
}

template <class InterfaceType>
Status ThriftClient<InterfaceType>::OpenWithRetry(int num_retries, int wait_ms) {
  DCHECK_GT(num_retries, 0);
  DCHECK_GE(wait_ms, 0);
  Status status;
  for (int i = 0; i < num_retries; ++i) {
    status = Open();
    if (status.ok()) return status;
    LOG(INFO) << "Unable to connect to " << host_ << ":" << port_ 
              << " (Attempt " << i + 1 << " of " << num_retries << ")";
    usleep(wait_ms * 1000L);
  }

  return status;
}


template <class InterfaceType>
Status ThriftClient<InterfaceType>::Close() {
  if (transport_->isOpen()) {
    transport_->close();
  }
  return Status::OK;
}

template <class InterfaceType>
ThriftClient<InterfaceType>::~ThriftClient() {
  Close();
}

}

#endif
