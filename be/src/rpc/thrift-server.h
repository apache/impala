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


#ifndef IMPALA_RPC_THRIFT_SERVER_H
#define IMPALA_RPC_THRIFT_SERVER_H

#include <boost/thread/mutex.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/unordered_map.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <thrift/server/TServer.h>
#include <thrift/TProcessor.h>

#include "common/status.h"
#include "rpc/auth-provider.h"
#include "util/metrics.h"
#include "util/thread.h"

namespace impala {

/// Utility class for all Thrift servers. Runs a threaded server by default, or a
/// TThreadPoolServer with, by default, 2 worker threads, that exposes the interface
/// described by a user-supplied TProcessor object.
/// If TThreadPoolServer is used, client must use TSocket as transport.
/// TODO: Need a builder to help with the unwieldy constructor
/// TODO: shutdown is buggy (which only harms tests)
class ThriftServer {
 public:
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
  };

  static const int DEFAULT_WORKER_THREADS = 2;

  /// There are 2 servers supported by Thrift with different threading models.
  /// ThreadPool  -- Allocates a fixed number of threads. A thread is used by a
  ///                connection until it closes.
  /// Threaded    -- Allocates 1 thread per connection, as needed.
  enum ServerType { ThreadPool = 0, Threaded };

  /// Creates, but does not start, a new server on the specified port
  /// that exports the supplied interface.
  ///  - name: human-readable name of this server. Should not contain spaces
  ///  - processor: Thrift processor to handle RPCs
  ///  - port: The port the server will listen for connections on
  ///  - auth_provider: Authentication scheme to use. If NULL, use the global default
  ///    demon<->demon provider.
  ///  - metrics: if not NULL, the server will register metrics on this object
  ///  - num_worker_threads: the number of worker threads to use in any thread pool
  ///  - server_type: the type of IO strategy this server should employ
  ThriftServer(const std::string& name,
      const boost::shared_ptr<apache::thrift::TProcessor>& processor, int port,
      AuthProvider* auth_provider = NULL, MetricGroup* metrics = NULL,
      int num_worker_threads = DEFAULT_WORKER_THREADS, ServerType server_type = Threaded);

  /// Enables secure access over SSL. Must be called before Start(). The first two
  /// arguments are paths to certificate and private key files in .PEM format,
  /// respectively. If either file does not exist, an error is returned. The final
  /// optional argument provides the command to run if a password is required to decrypt
  /// the private key. It is invoked once, and the resulting password is used only for
  /// password-protected .PEM files.
  Status EnableSsl(const std::string& certificate, const std::string& private_key,
      const std::string& pem_password_cmd = "");

  int port() const { return port_; }

  bool ssl_enabled() const { return ssl_enabled_; }

  /// Blocks until the server stops and exits its main thread.
  void Join();

  /// FOR TESTING ONLY; stop the server and block until the server is stopped; use it
  /// only if it is a Threaded server.
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
  /// Creates the server socket on which this server listens. May be SSL enabled. Returns
  /// OK unless there was a Thrift error.
  Status CreateSocket(
      boost::shared_ptr<apache::thrift::transport::TServerTransport>* socket);

  /// True if the server has been successfully started, for internal use only
  bool started_;

  /// The port on which the server interface is exposed
  int port_;

  /// True if the server socket only accepts SSL connections
  bool ssl_enabled_;

  /// Path to certificate file in .PEM format
  std::string certificate_path_;

  /// Path to private key file in .PEM format
  std::string private_key_path_;

  /// Password string retrieved by running command in EnableSsl().
  std::string key_password_;

  /// How many worker threads to use to serve incoming requests
  /// (requests are queued if no thread is immediately available)
  int num_worker_threads_;

  /// ThreadPool or Threaded server
  ServerType server_type_;

  /// User-specified identifier that shows up in logs
  const std::string name_;

  /// Thread that runs ThriftServerEventProcessor::Supervise() in a separate loop
  boost::scoped_ptr<Thread> server_thread_;

  /// Thrift housekeeping
  boost::scoped_ptr<apache::thrift::server::TServer> server_;
  boost::shared_ptr<apache::thrift::TProcessor> processor_;

  /// If not NULL, called when connection events happen. Not owned by us.
  ConnectionHandlerIf* connection_handler_;

  /// Protects connection_contexts_
  boost::mutex connection_contexts_lock_;

  /// Map of active connection context to a shared_ptr containing that context; when an
  /// item is removed from the map, it is automatically freed.
  typedef boost::unordered_map<ConnectionContext*, boost::shared_ptr<ConnectionContext> >
      ConnectionContextSet;
  ConnectionContextSet connection_contexts_;

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

  /// Helper class which monitors starting servers. Needs access to internal members, and
  /// is not used outside of this class.
  class ThriftServerEventProcessor;
  friend class ThriftServerEventProcessor;
};

}

#endif
