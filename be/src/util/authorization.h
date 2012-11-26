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


#ifndef IMPALA_SERVICE_AUTHORIZATION_H_
#define IMPALA_SERVICE_AUTHORIZATION_H_

#include <string>
#include <thrift/transport/TTransport.h>

#include "sasl/sasl.h"
#include "transport/TSaslServerTransport.h"
#include "transport/TSasl.h"
#include "common/status.h"

using namespace ::apache::thrift::transport;

namespace impala {
// Routines to support Kerberos authentication through the thrift-sasl transport

// Supported sasl mechanism
static const std::string KERBEROS_MECHANISM = "GSSAPI";

// Initialize the sasl library. Called once per process.
// appname: name of the application for error messages.
Status InitKerberos(const std::string& appname);

// Get a kerberos transport factory.
// The returned factory will wrap a transport in a transport that implements
// the sasl authorization protocol.
// principal: Two part kerberos principal name
// keyTabFile: Path to Kerberos security key file
// factory: returned factory
Status GetKerberosTransportFactory(const std::string& principal,
    const std::string& key_tab_file, boost::shared_ptr<TTransportFactory>* factory);

// Get a sasl client to implement the kerberos authorization protocol.
// The saslClient is passed to the TSaslClientTransport constructor.
// service: service to talk to, e.g. impala
// hostname: fully qualified host name that service runs on.
// saslClient: the returned sasl client.
Status GetTSaslClient(const std::string& hostname,
    boost::shared_ptr<sasl::TSasl>* saslClient);

// Returns the system defined hostname on which the process is running.
// If the name cannot be found a warning is issued and an empty string is returned.
std::string GetHostname();
}
#endif
