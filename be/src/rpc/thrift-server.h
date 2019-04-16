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

#ifndef IMPALA_RPC_THRIFT_SERVER_H
#define IMPALA_RPC_THRIFT_SERVER_H

#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/unordered_map.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <thrift/TProcessor.h>
#include <thrift/server/TServer.h>
#include <thrift/transport/TSSLSocket.h>

#include "common/status.h"
#include "util/metrics.h"
#include "util/thread.h"

namespace impala {

class AuthProvider;

/// Utility class for all Thrift servers. Runs a TAcceptQueueServer server with, by
/// default, no enforced concurrent connection limit, that exposes the interface
/// described by a user-supplied TProcessor object.
///
/// Use a ThriftServerBuilder to construct a ThriftServer. ThriftServer's c'tors are
/// private.
/// TODO: shutdown is buggy (which only harms tests)
class ThriftServer {
 public:
  /// Transport factory that wraps transports in a buffered transport with a customisable
  /// buffer-size and optionally in another transport from a provided factory. A larger
  /// buffer is usually more efficient, as it allows the underlying transports to perform
  /// fewer system calls.
  class BufferedTransportFactory :
      public apache::thrift::transport::TBufferedTransportFactory {
   public:
    static const int DEFAULT_BUFFER_SIZE_BYTES = 128 * 1024;

    BufferedTransportFactory(uint32_t buffer_size = DEFAULT_BUFFER_SIZE_BYTES,
        apache::thrift::transport::TTransportFactory* wrapped_factory =
            new apache::thrift::transport::TTransportFactory())
      : buffer_size_(buffer_size), wrapped_factory_(wrapped_factory) {}

    virtual boost::shared_ptr<apache::thrift::transport::TTransport> getTransport(
        boost::shared_ptr<apache::thrift::transport::TTransport> trans) {
      boost::shared_ptr<apache::thrift::transport::TTransport> wrapped =
          wrapped_factory_->getTransport(trans);
      return boost::shared_ptr<apache::thrift::transport::TTransport>(
          new apache::thrift::transport::TBufferedTransport(wrapped, buffer_size_));
    }
   private:
    uint32_t buffer_size_;
    std::unique_ptr<apache::thrift::transport::TTransportFactory> wrapped_factory_;
  };

  /// Transport implementation used by the thrift server.
  enum TransportType {
    BINARY, // Thrift bytes over default transport.
    HTTP, // Thrift bytes over HTTP transport.
  };

  /// Username.
  typedef std::string Username;

  /// Per-connection information.
  struct ConnectionContext {
    TUniqueId connection_id;
    Username username;
    TNetworkAddress network_address;
    std::string server_name;
  };

  /// Interface class for receiving connection creation / termination events.
  class ConnectionHandlerIf {
   public:
    /// Called when a connection is established (when a client connects).
    virtual void ConnectionStart(const ConnectionContext& connection_context) = 0;

    /// Called when a connection is terminated (when a client closes the connection).
    /// After this callback returns, the memory connection_context references is no longer
    /// valid and clients must not refer to it again.
    virtual void ConnectionEnd(const ConnectionContext& connection_context) = 0;

    virtual ~ConnectionHandlerIf() = default;
  };

  int port() const { return port_; }

  bool ssl_enabled() const { return ssl_enabled_; }

  /// Blocks until the server stops and exits its main thread.
  void Join();

  /// FOR TESTING ONLY; stop the server and block until the server is stopped
  void StopForTesting();

  /// Starts the main server thread. Once this call returns, clients
  /// may connect to this server and issue RPCs. May not be called more
  /// than once.
  Status Start();

  /// Sets the connection handler which receives events when connections are created or
  /// closed.
  void SetConnectionHandler(ConnectionHandlerIf* connection) {
    connection_handler_ = connection;
  }

  /// Returns true if the current thread has a connection context set on it.
  static bool HasThreadConnectionContext();

  /// Returns a unique identifier for the current connection. A connection is
  /// identified with the lifetime of a socket connection to this server.
  /// It is only safe to call this method during a Thrift processor RPC
  /// implementation. Otherwise, the result of calling this method is undefined.
  /// It is also only safe to reference the returned value during an RPC method.
  static const TUniqueId& GetThreadConnectionId();

  /// Returns a pointer to a struct that contains information about the current
  /// connection. This includes:
  ///   - A unique identifier for the connection.
  ///   - The username provided by the underlying transport for the current connection, or
  ///     an empty string if the transport did not provide a username. Currently, only the
  ///     TSasl transport provides this information.
  ///   - The client connection network address.
  /// It is only safe to call this method during a Thrift processor RPC
  /// implementation. Otherwise, the result of calling this method is undefined.
  /// It is also only safe to reference the returned value during an RPC method.
  static const ConnectionContext* GetThreadConnectionContext();

 private:
  friend class ThriftServerBuilder;

  /// Creates, but does not start, a new server on the specified port
  /// that exports the supplied interface.
  ///  - name: human-readable name of this server. Should not contain spaces
  ///  - processor: Thrift processor to handle RPCs
  ///  - port: The port the server will listen for connections on
  ///  - auth_provider: Authentication scheme to use. If nullptr, use the global default
  ///    demon<->demon provider.
  ///  - metrics: if not nullptr, the server will register metrics on this object
  ///  - max_concurrent_connections: The maximum number of concurrent connections allowed.
  ///    If 0, there will be no enforced limit on the number of concurrent connections.
  ///  - amount of time in milliseconds an accepted client connection will be held in
  ///    the accepted queue, after which the request will be rejected if a server
  ///    thread can't be found. If 0, no timeout is enforced.
  ThriftServer(const std::string& name,
      const boost::shared_ptr<apache::thrift::TProcessor>& processor, int port,
      AuthProvider* auth_provider = nullptr, MetricGroup* metrics = nullptr,
      int max_concurrent_connections = 0, int64_t queue_timeout_ms = 0,
      TransportType server_transport = TransportType::BINARY);

  /// Enables secure access over SSL. Must be called before Start(). The first three
  /// arguments are the minimum SSL/TLS version, and paths to certificate and private key
  /// files in .PEM format, respectively. If either file does not exist, an error is
  /// returned. The fourth, optional, argument provides the command to run if a password
  /// is required to decrypt the private key. It is invoked once, and the resulting
  /// password is used only for password-protected .PEM files. The final argument is a
  /// string containing a list of cipher suites, separated by commas, to enable.
  Status EnableSsl(apache::thrift::transport::SSLProtocol version,
      const std::string& certificate, const std::string& private_key,
      const std::string& pem_password_cmd = "", const std::string& ciphers = "");

  /// Creates the server socket on which this server listens. May be SSL enabled. Returns
  /// OK unless there was a Thrift error.
  Status CreateSocket(boost::shared_ptr<apache::thrift::transport::TServerSocket>* socket);

  /// True if the server has been successfully started, for internal use only
  bool started_;

  /// The port on which the server interface is exposed. Usually the port that was
  /// passed to the constructor, but if this was the wildcard port 0, then this is
  /// replaced with whatever port number the server is listening on.
  int port_;

  /// True if the server socket only accepts SSL connections
  bool ssl_enabled_;

  /// Path to certificate file in .PEM format
  std::string certificate_path_;

  /// Path to private key file in .PEM format
  std::string private_key_path_;

  /// Password string retrieved by running command in EnableSsl().
  std::string key_password_;

  /// List of ciphers that are ok for clients to use when connecting.
  std::string cipher_list_;

  /// The SSL/TLS protocol client versions that this server will allow to connect.
  apache::thrift::transport::SSLProtocol version_;

  /// Maximum number of concurrent connections (connections will block until fewer than
  /// max_concurrent_connections_ are concurrently active). If 0, there is no enforced
  /// limit.
  int max_concurrent_connections_;

  /// Amount of time in milliseconds an accepted client connection will be kept in the
  /// accept queue before it is timed out. If 0, there is no timeout.
  /// Used in TAcceptQueueServer.
  int64_t queue_timeout_ms_;

  /// User-specified identifier that shows up in logs
  const std::string name_;

  /// Thread that runs ThriftServerEventProcessor::Supervise() in a separate loop
  std::unique_ptr<Thread> server_thread_;

  /// Thrift housekeeping
  boost::scoped_ptr<apache::thrift::server::TServer> server_;
  boost::shared_ptr<apache::thrift::TProcessor> processor_;

  /// If not nullptr, called when connection events happen. Not owned by us.
  ConnectionHandlerIf* connection_handler_;

  /// Protects connection_contexts_
  boost::mutex connection_contexts_lock_;

  /// Map of active connection context to a shared_ptr containing that context; when an
  /// item is removed from the map, it is automatically freed.
  typedef boost::unordered_map<ConnectionContext*, boost::shared_ptr<ConnectionContext>>
      ConnectionContextSet;
  ConnectionContextSet connection_contexts_;

  /// Metrics subsystem access
  MetricGroup* metrics_;

  /// True if metrics are enabled
  bool metrics_enabled_;

  /// Number of currently active connections
  IntGauge* num_current_connections_metric_;

  /// Total connections made over the lifetime of this server
  IntCounter* total_connections_metric_;

  /// Used to generate a unique connection id for every connection
  boost::uuids::random_generator uuid_generator_;

  /// Not owned by us, owned by the AuthManager
  AuthProvider* auth_provider_;

  /// Underlying transport type used by this thrift server.
  TransportType transport_type_;

  /// Helper class which monitors starting servers. Needs access to internal members, and
  /// is not used outside of this class.
  class ThriftServerEventProcessor;
  friend class ThriftServerEventProcessor;
};

/// Helper class to build new ThriftServer instances.
class ThriftServerBuilder {
 public:
  ThriftServerBuilder(const std::string& name,
      const boost::shared_ptr<apache::thrift::TProcessor>& processor, int port)
    : name_(name), processor_(processor), port_(port) {}

  /// Sets the auth provider for this server. Default is the system global auth provider.
  ThriftServerBuilder& auth_provider(AuthProvider* provider) {
    auth_provider_ = provider;
    return *this;
  }

  /// Sets the metrics instance that this server should register metrics with. Default is
  /// nullptr.
  ThriftServerBuilder& metrics(MetricGroup* metrics) {
    metrics_ = metrics;
    return *this;
  }

  /// Sets the maximum concurrent thread count for this server. Default is 0, which means
  /// there is no enforced limit.
  ThriftServerBuilder& max_concurrent_connections(int max_concurrent_connections) {
    max_concurrent_connections_ = max_concurrent_connections;
    return *this;
  }

  ThriftServerBuilder& queue_timeout(int64_t timeout_ms) {
    queue_timeout_ms_ = timeout_ms;
    return *this;
  }

  /// Enables SSL for this server.
  ThriftServerBuilder& ssl(
      const std::string& certificate, const std::string& private_key) {
    enable_ssl_ = true;
    certificate_ = certificate;
    private_key_ = private_key;
    return *this;
  }

  /// Sets the SSL/TLS client version(s) that this server will allow to connect.
  ThriftServerBuilder& ssl_version(apache::thrift::transport::SSLProtocol version) {
    version_ = version;
    return *this;
  }

  /// Sets the command used to compute the password for the SSL private key. Default is
  /// empty, i.e. no password needed.
  ThriftServerBuilder& pem_password_cmd(const std::string& pem_password_cmd) {
    pem_password_cmd_ = pem_password_cmd;
    return *this;
  }

  /// Sets the list of acceptable cipher suites for this server. Default is to use all
  /// available system cipher suites.
  ThriftServerBuilder& cipher_list(const std::string& ciphers) {
    ciphers_ = ciphers;
    return *this;
  }

  /// Sets the underlying transport type for the thrift server.
  ThriftServerBuilder& transport_type(ThriftServer::TransportType transport_type) {
    server_transport_type_ = transport_type;
    return *this;
  }

  /// Constructs a new ThriftServer and puts it in 'server', if construction was
  /// successful, returns an error otherwise. In the error case, 'server' will not have
  /// been set and will not need to be freed, otherwise the caller assumes ownership of
  /// '*server'.
  Status Build(ThriftServer** server) {
    std::unique_ptr<ThriftServer> ptr(
        new ThriftServer(name_, processor_, port_, auth_provider_, metrics_,
            max_concurrent_connections_, queue_timeout_ms_, server_transport_type_));
    if (enable_ssl_) {
      RETURN_IF_ERROR(ptr->EnableSsl(
          version_, certificate_, private_key_, pem_password_cmd_, ciphers_));
    }
    (*server) = ptr.release();
    return Status::OK();
  }

 private:
  int64_t queue_timeout_ms_ = 0;
  int max_concurrent_connections_ = 0;
  std::string name_;
  boost::shared_ptr<apache::thrift::TProcessor> processor_;
  int port_ = 0;
  ThriftServer::TransportType server_transport_type_ =
      ThriftServer::TransportType::BINARY;

  AuthProvider* auth_provider_ = nullptr;
  MetricGroup* metrics_ = nullptr;

  bool enable_ssl_ = false;
  apache::thrift::transport::SSLProtocol version_ =
      apache::thrift::transport::SSLProtocol::TLSv1_0;
  std::string certificate_;
  std::string private_key_;
  std::string pem_password_cmd_;
  std::string ciphers_;
};

/// Contains a map from string for --ssl_minimum_version to Thrift's SSLProtocol.
struct SSLProtoVersions {
  static std::map<std::string, apache::thrift::transport::SSLProtocol> PROTO_MAP;

  /// Given a string, find a corresponding SSLProtocol from PROTO_MAP. Returns an error if
  /// one cannot be found. Matching is case-insensitive.
  static Status StringToProtocol(
      const std::string& in, apache::thrift::transport::SSLProtocol* protocol);

  /// Returns true if 'protocol' is supported by the version of OpenSSL this binary is
  /// linked to.
  static bool IsSupported(const apache::thrift::transport::SSLProtocol& protocol);
};

}

#endif
