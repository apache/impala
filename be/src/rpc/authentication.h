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


#ifndef IMPALA_SERVICE_AUTHENTICATION_H
#define IMPALA_SERVICE_AUTHENTICATION_H

#include <string>
#include <thrift/transport/TTransport.h>
#include <thrift/transport/TSSLSocket.h>

#include "rpc/auth-provider.h"
#include "sasl/sasl.h"
#include "transport/TSaslServerTransport.h"
#include "transport/TSasl.h"
#include "common/status.h"

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wheader-hygiene"
using namespace ::apache::thrift::transport;
#pragma clang diagnostic pop

namespace impala {

/// System-wide authentication manager responsible for initialising authentication systems,
/// including SSL, Sasl and Kerberos, and for providing auth-enabled Thrift structures to
/// servers and clients.
/// There should only be one AuthManager instantiated at a time.
/// If Init() is called more than once, all the setup state will be overwritten (Currently
/// only used for testing purposes to validate different security configurations)
class AuthManager {
 public:
  static AuthManager* GetInstance() { return AuthManager::auth_manager_; }

  /// Set up internal and external AuthProvider classes. This also initializes SSL (via
  /// the creation of ssl_socket_factory_).
  Status Init();

  /// Returns the authentication provider to use for "external" communication
  /// such as the impala shell, jdbc, odbc, etc. This only applies to the server
  /// side of a connection; the client side of said connection is never an
  /// internal process.
  AuthProvider* GetExternalAuthProvider();

  /// Returns the authentication provider to use for internal daemon <-> daemon
  /// connections.  This goes for both the client and server sides.  An example
  /// connection this applies to would be backend <-> statestore.
  AuthProvider* GetInternalAuthProvider();

 private:
  /// One-time kerberos-specific environment variable setup. Called by Init() if Kerberos
  /// is enabled.
  Status InitKerberosEnv();

  static AuthManager* auth_manager_;

  /// These are provided for convenience, so that demon<->demon and client<->demon services
  /// don't have to check the auth flags to figure out which auth provider to use.
  boost::scoped_ptr<AuthProvider> internal_auth_provider_;
  boost::scoped_ptr<AuthProvider> external_auth_provider_;

  /// A thrift SSL socket factory must be created and live the lifetime of the process to
  /// ensure that the thrift OpenSSL initialization code runs at Init(), and is not
  /// unregistered (which thrift will do when the refcount of TSSLSocketFactory objects
  /// reach 0), see IMPALA-4933. For simplicity, and because Kudu will expect SSL to be
  /// initialized, this will be created regardless of whether or not SSL credentials are
  /// specified. This factory isn't otherwise used.
  boost::scoped_ptr<TSSLSocketFactory> ssl_socket_factory_;
};


}
#endif
