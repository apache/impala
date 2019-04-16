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

#ifndef IMPALA_RPC_AUTH_PROVIDER_H
#define IMPALA_RPC_AUTH_PROVIDER_H

#include <string>
#include <boost/thread/mutex.hpp>
#include <sasl/sasl.h>

#include "common/status.h"
#include "rpc/thrift-server.h"
#include "util/promise.h"

namespace sasl { class TSasl; }

namespace impala {

/// An AuthProvider creates Thrift transports that are set up to authenticate themselves
/// using a protocol such as Kerberos or PLAIN/SASL. Both server and client transports are
/// provided by this class, using slightly different mechanisms (servers use a factory,
/// clients wrap a pre-provided transport)
class AuthProvider {
 public:
  /// Initialises any state required to perform authentication using this provider.
  virtual Status Start() WARN_UNUSED_RESULT = 0;

  /// Creates a new Thrift transport factory in the out parameter that performs
  /// authorisation per this provider's protocol. The top-level transport returned by
  /// 'factory' must always be a TBufferedTransport, but depending on the AuthProvider
  /// implementation and the value of 'underlying_transport_type', that may be wrapped
  /// around another transport type, eg. a TSaslServerTransport.
  virtual Status GetServerTransportFactory(
      ThriftServer::TransportType underlying_transport_type,
      boost::shared_ptr<apache::thrift::transport::TTransportFactory>* factory)
      WARN_UNUSED_RESULT = 0;

  /// Called by Thrift clients to wrap a raw transport with any intermediate transport
  /// that an auth protocol requires.
  /// TODO: Return the correct clients for HTTP base transport. At this point, no clients
  /// for HTTP endpoints are internal to the Impala service, so it should be OK.
  virtual Status WrapClientTransport(const std::string& hostname,
      boost::shared_ptr<apache::thrift::transport::TTransport> raw_transport,
      const std::string& service_name,
      boost::shared_ptr<apache::thrift::transport::TTransport>* wrapped_transport)
      WARN_UNUSED_RESULT = 0;

  /// Setup 'connection_ptr' to get its username from 'underlying_transport'.
  virtual void SetupConnectionContext(
      const boost::shared_ptr<ThriftServer::ConnectionContext>& connection_ptr,
      ThriftServer::TransportType underlying_transport_type,
      apache::thrift::transport::TTransport* underlying_transport) = 0;

  /// Returns true if this provider uses Sasl at the transport layer.
  virtual bool is_secure() = 0;

  virtual ~AuthProvider() { }
};

/// Used if either (or both) Kerberos and LDAP auth are desired. For BINARY connections we
/// use Sasl for the communication, and for HTTP connections we use BASIC auth.  This
/// "wraps" the underlying communication, in thrift-speak. This is used for both client
/// and server contexts; there is one for internal and one for external communication.
class SecureAuthProvider : public AuthProvider {
 public:
  SecureAuthProvider(bool is_internal)
    : has_ldap_(false), is_internal_(is_internal), needs_kinit_(false) {}

  /// Performs initialization of external state. Kinit if configured to use kerberos.
  /// If we're using ldap, set up appropriate certificate usage.
  virtual Status Start();

  /// Wrap the client transport with a new TSaslClientTransport.  This is only for
  /// internal connections.  Since, as a daemon, we only do Kerberos and not LDAP,
  /// we can go straight to Kerberos.
  /// This is only applicable to Thrift connections and not KRPC connections.
  virtual Status WrapClientTransport(const std::string& hostname,
      boost::shared_ptr<apache::thrift::transport::TTransport> raw_transport,
      const std::string& service_name,
      boost::shared_ptr<apache::thrift::transport::TTransport>* wrapped_transport);

  /// This sets up a mapping between auth types (PLAIN and GSSAPI) and callbacks.
  /// When a connection comes in, thrift will see one of the above on the wire, do
  /// a table lookup, and associate the appropriate callbacks with the connection.
  /// Then presto! You've got authentication for the connection.
  /// This is only applicable to Thrift connections and not KRPC connections.
  virtual Status GetServerTransportFactory(
      ThriftServer::TransportType underlying_transport_type,
      boost::shared_ptr<apache::thrift::transport::TTransportFactory>* factory);

  /// IF sasl was used, the username will be available from the handshake, and we set it
  /// here. If HTTP BASIC auth was used, the username won't be available until the first
  /// packet is received, so w register a callback with the transport that will set the
  /// username then.
  virtual void SetupConnectionContext(
      const boost::shared_ptr<ThriftServer::ConnectionContext>& connection_ptr,
      ThriftServer::TransportType underlying_transport_type,
      apache::thrift::transport::TTransport* underlying_transport);

  virtual bool is_secure() { return true; }

  /// Initializes kerberos items and checks for sanity.  Failures can occur on a
  /// malformed principal or when setting some environment variables.  Called
  /// prior to Start().
  Status InitKerberos(const std::string& principal, const std::string& keytab_path);

  /// Initializes ldap - just record that we're going to use it.  Called prior to
  /// Start().
  void InitLdap() { has_ldap_ = true; }

  /// Used for testing
  const std::string& principal() const { return principal_; }
  const std::string& service_name() const { return service_name_; }
  const std::string& hostname() const { return hostname_; }
  const std::string& realm() const { return realm_; }
  bool has_ldap() { return has_ldap_; }

 private:
  /// Do we (the server side only) support ldap for this connnection?
  bool has_ldap_;

  /// Hostname of this machine - if kerberos, derived from principal.  If there
  /// is no kerberos, but LDAP is used, then acquired via GetHostname().
  std::string hostname_;

  /// True if internal, false if external.
  bool is_internal_;

  /// All the rest of these private items are Kerberos-specific.

  /// The Kerberos principal. If is_internal_ is true and --be_principal was
  /// supplied, this is --be_principal.  In all other cases this is --principal.
  std::string principal_;

  /// The full path to the keytab where the above principal can be found.
  std::string keytab_file_;

  /// The service name, deduced from the principal. Used by servers to indicate
  /// what service a principal must have a ticket for in order to be granted
  /// access to this service.
  std::string service_name_;

  /// Principal's realm, again derived from principal.
  std::string realm_;

  /// True if tickets for this principal should be obtained.  This is true if
  /// we're an auth provider for an "internal" connection, because we may
  /// function as a client.
  bool needs_kinit_;

  /// One-time kerberos-specific environment variable setup.  Called by InitKerberos().
  Status InitKerberosEnv() WARN_UNUSED_RESULT;
};

/// This provider implements no authentication, so any connection is immediately
/// successful.  There's no Sasl in the picture.
class NoAuthProvider : public AuthProvider {
 public:
  NoAuthProvider() { }

  virtual Status Start() { return Status::OK(); }

  virtual Status GetServerTransportFactory(
      ThriftServer::TransportType underlying_transport_type,
      boost::shared_ptr<apache::thrift::transport::TTransportFactory>* factory);

  virtual Status WrapClientTransport(const std::string& hostname,
      boost::shared_ptr<apache::thrift::transport::TTransport> raw_transport,
      const std::string& service_name,
      boost::shared_ptr<apache::thrift::transport::TTransport>* wrapped_transport);

  /// If there is no auth, then we don't have a username available.
  virtual void SetupConnectionContext(
      const boost::shared_ptr<ThriftServer::ConnectionContext>& connection_ptr,
      ThriftServer::TransportType underlying_transport_type,
      apache::thrift::transport::TTransport* underlying_transport) {
    connection_ptr->username = "";
  }

  virtual bool is_secure() { return false; }
};

/// The first entry point to the authentication subsystem.  Performs initialization
/// of Sasl, the global AuthManager, and the two authentication providers.  Appname
/// should generally be argv[0]. Normally, InitAuth() should only be called once.
/// In certain test cases, we may call it more than once. It's important that InitAuth()
/// is called with the same 'appname' if it's called more than once. Otherwise, error
/// status will be returned.
Status InitAuth(const std::string& appname);

}

#endif
