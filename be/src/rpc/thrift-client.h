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

#pragma once

#include <boost/shared_ptr.hpp>
#include <common/status.h>
#include <thrift/Thrift.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TSSLSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <gflags/gflags.h>

#include "transport/TSaslClientTransport.h"
#include "transport/TSasl.h"
#include "rpc/authentication.h"
#include "rpc/thrift-server.h"
#include "rpc/thrift-util.h"
#include "gen-cpp/Types_types.h"

DECLARE_string(principal);
DECLARE_string(hostname);

namespace impala {
/// Super class for templatized thrift clients.
class ThriftClientImpl {
 public:
  virtual ~ThriftClientImpl() { Close(); }

  const TNetworkAddress& address() const { return address_; }

  /// Open the connection to the remote server. May be called repeatedly, is idempotent
  /// unless there is a failure to connect.
  /// If Open() fails, the connection remains closed.
  Status Open();

  /// Retry the Open num_retries time waiting wait_ms milliseconds between retries.
  /// If num_retries == 0, the connection is retried indefinitely.
  Status OpenWithRetry(uint32_t num_retries, uint64_t wait_ms);

  /// Close the connection with the remote server. May be called repeatedly.
  void Close();

  /// Set receive timeout on the underlying TSocket.
  void setRecvTimeout(int32_t ms) { socket_->setRecvTimeout(ms); }

  /// Set send timeout on the underlying TSocket.
  void setSendTimeout(int32_t ms) { socket_->setSendTimeout(ms); }

  Status init_status() { return init_status_; }

 protected:
  ThriftClientImpl(const std::string& ipaddress, int port, bool ssl, bool disable_tls12);

  /// Create a new socket without opening it. Returns an error if the socket could not
  /// be created.
  Status CreateSocket();

  /// Address of the server this client communicates with.
  TNetworkAddress address_;

  /// True if ssl encryption is enabled on this connection.
  bool ssl_;

  /// Whether to disable TLSv1.2. This is used to test TLSv1.3 ciphersuites.
  /// TODO: Remove this when Thrift supports ssl_minimum_version=tlsv1.3.
  bool disable_tls12_;

  Status init_status_;

  /// Sasl Client object.  Contains client kerberos identification data.
  /// Will be NULL if kerberos is not being used.
  std::shared_ptr<sasl::TSasl> sasl_client_;

  /// This factory sets up the openSSL library state and needs to be alive as long as its
  /// owner(a ThriftClientImpl instance) does. Otherwise the OpenSSL state is lost
  /// (refer IMPALA-2747).
  boost::scoped_ptr<ImpalaTlsSocketFactory> ssl_factory_;

  /// All shared pointers, because Thrift requires them to be
  std::shared_ptr<apache::thrift::transport::TSocket> socket_;
  std::shared_ptr<apache::thrift::transport::TTransport> transport_;
  std::shared_ptr<apache::thrift::protocol::TBinaryProtocol> protocol_;
};


/// Utility client to a Thrift server. The parameter type is the Thrift interface type
/// that the server implements.
/// TODO: Consider a builder class to make constructing this class easier.
template <class InterfaceType>
class ThriftClient : public ThriftClientImpl {
 public:
  /// Creates, but does not connect, a new ThriftClient for a remote server.
  ///  - ipaddress: address of remote server
  ///  - port: port on which remote service runs
  ///  - service_name: If set, the target service to connect to.
  ///  - auth_provider: Authentication scheme to use. If NULL, use the global default
  ///    client<->demon authentication scheme.
  ///  - ssl: if true, SSL is enabled on this connection
  ///  - disable_tls12: If true, disable TLS 1.2. This is used for testing TLS 1.3.
  ///    It can be removed when Thrift supports ssl_minimum_version=tlsv1.3.
  ThriftClient(const std::string& ipaddress, int port,
      const std::string& service_name = "", AuthProvider* auth_provider = NULL,
      bool ssl = false, bool disable_tls12 = false);

  /// Returns the object used to actually make RPCs against the remote server
  InterfaceType* iface() { return iface_.get(); }

 private:
  std::shared_ptr<InterfaceType> iface_;

  AuthProvider* auth_provider_;
};

template <class InterfaceType>
ThriftClient<InterfaceType>::ThriftClient(const std::string& ipaddress, int port,
    const std::string& service_name, AuthProvider* auth_provider, bool ssl,
    bool disable_tls12)
  : ThriftClientImpl(ipaddress, port, ssl, disable_tls12),
      iface_(new InterfaceType(protocol_)),
      auth_provider_(auth_provider) {

  if (auth_provider_ == NULL) {
    auth_provider_ = AuthManager::GetInstance()->GetInternalAuthProvider();
    DCHECK(auth_provider_ != NULL);
  }

  // If socket_ is NULL (because ThriftClientImpl::CreateSocket() failed in the base
  // class constructor, nothing else should be constructed. Open()/Reopen() will return
  // the error that the socket couldn't be created and the caller should be careful to
  // not use the client after that.
  // TODO: Move initialization code that can fail into a separate Init() method.
  if (socket_ == NULL) {
    DCHECK(!init_status_.ok());
    return;
  }

  // transport_ is created by wrapping the socket_ in the TTransport provided by the
  // auth_provider_ and then a TBufferedTransport (IMPALA-1928).
  transport_ = socket_;
  init_status_ = auth_provider_->WrapClientTransport(address_.hostname, transport_,
      service_name, &transport_);
  if (!init_status_.ok()) return; // The caller will decide what to do with the Status.
  ThriftServer::BufferedTransportFactory factory;
  transport_ = factory.getTransport(transport_);

  protocol_.reset(new apache::thrift::protocol::TBinaryProtocol(transport_));
  iface_.reset(new InterfaceType(protocol_));
}
}
