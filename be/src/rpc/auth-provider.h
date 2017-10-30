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
#include "util/promise.h"

namespace sasl { class TSasl; }

namespace impala {

class Thread;

/// An AuthProvider creates Thrift transports that are set up to authenticate themselves
/// using a protocol such as Kerberos or PLAIN/SASL. Both server and client transports are
/// provided by this class, using slightly different mechanisms (servers use a factory,
/// clients wrap a pre-provided transport)
class AuthProvider {
 public:
  /// Initialises any state required to perform authentication using this provider.
  virtual Status Start() WARN_UNUSED_RESULT = 0;

  /// Creates a new Thrift transport factory in the out parameter that performs
  /// authorisation per this provider's protocol.
  virtual Status GetServerTransportFactory(
      boost::shared_ptr<apache::thrift::transport::TTransportFactory>* factory)
      WARN_UNUSED_RESULT = 0;

  /// Called by Thrift clients to wrap a raw transport with any intermediate transport
  /// that an auth protocol requires.
  virtual Status WrapClientTransport(const std::string& hostname,
      boost::shared_ptr<apache::thrift::transport::TTransport> raw_transport,
      const std::string& service_name,
      boost::shared_ptr<apache::thrift::transport::TTransport>* wrapped_transport)
      WARN_UNUSED_RESULT = 0;

  /// Returns true if this provider uses Sasl at the transport layer.
  virtual bool is_sasl() = 0;

  virtual ~AuthProvider() { }
};

/// If either (or both) Kerberos and LDAP auth are desired, we use Sasl for the
/// communication.  This "wraps" the underlying communication, in thrift-speak.
/// This is used for both client and server contexts; there is one for internal
/// and one for external communication.
class SaslAuthProvider : public AuthProvider {
 public:
  SaslAuthProvider(bool is_internal) : has_ldap_(false), is_internal_(is_internal),
      needs_kinit_(false) {}

  /// Performs initialization of external state.  If we're using kerberos and
  /// need to kinit, start that thread.  If we're using ldap, set up appropriate
  /// certificate usage.
  virtual Status Start();

  /// Wrap the client transport with a new TSaslClientTransport.  This is only for
  /// internal connections.  Since, as a daemon, we only do Kerberos and not LDAP,
  /// we can go straight to Kerberos.
  virtual Status WrapClientTransport(const std::string& hostname,
      boost::shared_ptr<apache::thrift::transport::TTransport> raw_transport,
      const std::string& service_name,
      boost::shared_ptr<apache::thrift::transport::TTransport>* wrapped_transport);

  /// This sets up a mapping between auth types (PLAIN and GSSAPI) and callbacks.
  /// When a connection comes in, thrift will see one of the above on the wire, do
  /// a table lookup, and associate the appropriate callbacks with the connection.
  /// Then presto! You've got authentication for the connection.
  virtual Status GetServerTransportFactory(
      boost::shared_ptr<apache::thrift::transport::TTransportFactory>* factory);

  virtual bool is_sasl() { return true; }

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

  /// Runs "RunKinit" below if needs_kinit_ is true and FLAGS_use_kudu_kinit is false.
  /// Once started, this thread lives as long as the process does and periodically forks
  /// impalad and execs the 'kinit' process.
  std::unique_ptr<Thread> kinit_thread_;

  /// Periodically (roughly once every FLAGS_kerberos_reinit_interval minutes) calls kinit
  /// to get a ticket granting ticket from the kerberos server for principal_, which is
  /// kept in the kerberos cache associated with this process. This ensures that we have
  /// valid kerberos credentials when operating as a client. Once the first attempt to
  /// obtain a ticket has completed, first_kinit is Set() with the status of the operation.
  /// Additionally, if the first attempt fails, this method will return.
  void RunKinit(Promise<Status>* first_kinit);

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
      boost::shared_ptr<apache::thrift::transport::TTransportFactory>* factory);

  virtual Status WrapClientTransport(const std::string& hostname,
      boost::shared_ptr<apache::thrift::transport::TTransport> raw_transport,
      const std::string& service_name,
      boost::shared_ptr<apache::thrift::transport::TTransport>* wrapped_transport);

  virtual bool is_sasl() { return false; }
};

/// The first entry point to the authentication subsystem.  Performs initialization
/// of Sasl, the global AuthManager, and the two authentication providers.  Appname
/// should generally be argv[0].
Status InitAuth(const std::string& appname);

}

#endif
