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


#ifndef IMPALA_SERVICE_AUTHENTICATION_H
#define IMPALA_SERVICE_AUTHENTICATION_H

#include <string>
#include <thrift/transport/TTransport.h>

#include "rpc/auth-provider.h"
#include "sasl/sasl.h"
#include "transport/TSaslServerTransport.h"
#include "transport/TSasl.h"
#include "common/status.h"

using namespace ::apache::thrift::transport;

namespace impala {

// System-wide authentication manager responsible for initialising authentication systems,
// including Sasl and Kerberos, and for providing auth-enabled Thrift structures to
// servers and clients.
class AuthManager {
 public:
  static AuthManager* GetInstance() { return AuthManager::auth_manager_; };

  // Checks the auth flags and creates appropriate auth providers for client and backend
  // use. If the authentication scheme needs any initialisation (e.g. run Kinit for
  // Kerberos), this method does that by calling Start() on both auth providers.
  Status Init();

  // Returns the provider to use if you are an Impala client-facing service such as
  // Beeswax or HS2.
  AuthProvider* GetClientFacingAuthProvider();

  // Returns the provider to use if you are an internal server<->server service such as an
  // Impala backend.
  AuthProvider* GetServerFacingAuthProvider();

 private:
  static AuthManager* auth_manager_;

  // These are provided for convenience, so that demon<->demon and client<->demon services
  // don't have to check the auth flags to figure out which auth provider to use.
  boost::scoped_ptr<AuthProvider> client_auth_provider_;
  boost::scoped_ptr<AuthProvider> server_auth_provider_;
};


}
#endif
