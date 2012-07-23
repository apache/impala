// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_UTIL_THRIFT_CLIENT_H
#define IMPALA_UTIL_THRIFT_CLIENT_H

#include <sstream>
#include <boost/shared_ptr.hpp>
#include <common/status.h>
#include <transport/TSocket.h>
#include <transport/TBufferTransports.h>
#include <protocol/TBinaryProtocol.h>

namespace impala {

// Utility client to a Thrift server. The parameter type is the
// Thrift interface type that the server implements.
template <class InterfaceType>
class ThriftClient {
 public:
  ThriftClient(std::string host, int port);
  ~ThriftClient();

  // Returns the object used to actually make RPCs against the remote server
  InterfaceType* iface() { return iface_.get(); }

  const std::string& host() { return host_; }
  int port() { return port_; }

  // Open the connection to the remote server. May be called
  // repeatedly, is idempotent unless there is a failure to connect.
  Status Open();

  // Close the connection with the remote server. May be called
  // repeatedly.
  Status Close();

 private:
  std::string host_;
  int port_;

  // All shared pointers, because Thrift requires them to be
  boost::shared_ptr<apache::thrift::transport::TSocket> socket_;
  boost::shared_ptr<apache::thrift::transport::TFramedTransport> transport_;
  boost::shared_ptr<apache::thrift::protocol::TBinaryProtocol> protocol_;
  boost::shared_ptr<InterfaceType> iface_;
};

template <class InterfaceType>
ThriftClient<InterfaceType>::ThriftClient(std::string host, int port)
    : host_(host),
      port_(port),
      socket_(new apache::thrift::transport::TSocket(host, port)),
      transport_(new apache::thrift::transport::TFramedTransport(socket_)),
      protocol_(new apache::thrift::protocol::TBinaryProtocol(transport_)),
      iface_(new InterfaceType(protocol_)) {
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
