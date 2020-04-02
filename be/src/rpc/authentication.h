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

class ImpalaLdap;

/// System-wide authentication manager responsible for initialising authentication systems,
/// including SSL, Sasl and Kerberos, and for providing auth-enabled Thrift structures to
/// servers and clients.
/// There should only be one AuthManager instantiated at a time.
/// If Init() is called more than once, all the setup state will be overwritten (Currently
/// only used for testing purposes to validate different security configurations)
class AuthManager {
 public:
  static AuthManager* GetInstance() { return AuthManager::auth_manager_; }

  AuthManager();
  ~AuthManager();

  /// Set up internal and external AuthProvider classes. This also initializes SSL (via
  /// kudu::security::InitializeOpenSSL()).
  Status Init();

  /// Returns the authentication provider to use for "external" communication with
  /// hs2-http protocol. Generally the same as GetExternalAuthProvider(), but can
  /// be different in case of SAML SSO is enabled but LDAP/Kerberos is not, as SAML
  /// can be only used during http authentication.
  AuthProvider* GetExternalHttpAuthProvider();

  /// Returns the authentication provider to use for "external" communication
  /// such as the impala shell, jdbc, odbc, etc. This only applies to the server
  /// side of a connection; the client side of said connection is never an
  /// internal process.
  AuthProvider* GetExternalAuthProvider();

  /// Returns the authentication provider to use for "external frontend" communication.
  /// This only applies to the server side of a connection; the client side of said
  /// connection is never an internal process. Currently this is either null if
  /// external_fe_port <= 0 or NoAuthProvider.
  AuthProvider* GetExternalFrontendAuthProvider();

  /// Returns the authentication provider to use for internal daemon <-> daemon
  /// connections.  This goes for both the client and server sides.  An example
  /// connection this applies to would be backend <-> statestore.
  AuthProvider* GetInternalAuthProvider();

  ImpalaLdap* GetLdap() { return ldap_.get(); }

 private:
  /// One-time kerberos-specific environment variable setup. Sets variables like
  /// KRB5CCNAME and friends so that command-line flags take effect in the C++ and
  /// Java Kerberos implementations. Called by Init() whether or not Kerberos is enabled.
  Status InitKerberosEnv();

  static AuthManager* auth_manager_;

  /// These are provided for convenience, so that demon<->demon and client<->demon services
  /// don't have to check the auth flags to figure out which auth provider to use.
  /// external_http_auth_provider_ overrides external_auth_provider_ for http requests if
  /// the auth mechanism only supports http requests.
  boost::scoped_ptr<AuthProvider> internal_auth_provider_;
  boost::scoped_ptr<AuthProvider> external_auth_provider_;
  boost::scoped_ptr<AuthProvider> external_http_auth_provider_;
  boost::scoped_ptr<AuthProvider> external_fe_auth_provider_;

  /// Used to authenticate usernames and passwords to LDAP.
  std::unique_ptr<ImpalaLdap> ldap_;
};


}
#endif
