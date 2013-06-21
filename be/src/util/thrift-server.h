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


#ifndef IMPALA_UTIL_THRIFT_SERVER_H
#define IMPALA_UTIL_THRIFT_SERVER_H

#include <boost/thread/mutex.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/unordered_map.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <thrift/server/TNonblockingServer.h>
#include <thrift/TProcessor.h>

#include "common/status.h"
#include "util/metrics.h"

namespace boost { class thread; }

namespace impala {

// Utility class for all Thrift servers. Runs a TNonblockingServer(default) or a
// TThreadPoolServer with, by default, 2 worker threads, that exposes the interface
// described by a user-supplied TProcessor object.
// If TNonblockingServer is used, client must use TFramedTransport.
// If TThreadPoolServer is used, client must use TSocket as transport.
class ThriftServer {
 public:
  // An opaque identifier for the current session, which identifies a client connection.
  typedef std::string SessionId;

  // Username.
  typedef std::string Username;

  // Per-connection session information.
  struct SessionContext {
    SessionId session_id;
    Username username;
    TNetworkAddress network_address;
  };

  // Interface class for receiving session creation / termination events.
  class SessionHandlerIf {
   public:
    // Called when a session is established (when a client connects).
    virtual void SessionStart(const SessionContext& session_context) = 0;

    // Called when a session is terminated (when a client closes the connection).
    // After this callback returns, the memory session_context references is no longer
    // valid and clients must not refer to it again.
    virtual void SessionEnd(const SessionContext& session_context) = 0;
  };

  static const int DEFAULT_WORKER_THREADS = 2;

  // There are 3 servers supported by Thrift with different threading models.
  // ThreadPool  -- Allocates a fixed number of threads. A thread is used by a
  //                connection until it closes.
  // Threaded    -- Allocates 1 thread per connection, as needed.
  // Nonblocking -- Threads are allocated to a connection only when the server
  //                is working on behalf of the connection.
  enum ServerType { ThreadPool = 0, Threaded, Nonblocking };

  // Creates, but does not start, a new server on the specified port
  // that exports the supplied interface.
  //  - name: human-readable name of this server. Should not contain spaces
  //  - processor: Thrift processor to handle RPCs
  //  - port: The port the server will listen for connections on
  //  - metrics: if not NULL, the server will register metrics on this object
  //  - num_worker_threads: the number of worker threads to use in any thread pool
  //  - server_type: the type of IO strategy this server should employ
  ThriftServer(const std::string& name,
      const boost::shared_ptr<apache::thrift::TProcessor>& processor, int port,
      Metrics* metrics = NULL, int num_worker_threads = DEFAULT_WORKER_THREADS,
      ServerType server_type = Threaded);

  int port() const { return port_; }

  // Blocks until the server stops and exits its main thread.
  void Join();

  // FOR TESTING ONLY; stop the server and block until the server is stopped; use it
  // only if it is a Threaded server.
  void StopForTesting();

  // Starts the main server thread. Once this call returns, clients
  // may connect to this server and issue RPCs. May not be called more
  // than once.
  Status Start();

  // Sets the session handler which receives events when sessions are created or closed.
  void SetSessionHandler(SessionHandlerIf* session) {
    session_handler_ = session;
  }

  // Returns a unique identifier for the current session. A session is
  // identified with the lifetime of a socket connection to this server.
  // It is only safe to call this method during a Thrift processor RPC
  // implementation. Otherwise, the result of calling this method is undefined.
  // It is also only safe to reference the returned value during an RPC method.
  static const SessionId& GetThreadSessionId();

  // Returns a pointer to a struct that contains information about the current
  // session. This includes:
  //   - A unique identifier for the session.
  //   - The username provided by the underlying transport for the current session, or an
  //     empty string if the transport did not provide a username. Currently, only the
  //     TSasl transport provides this information.
  //   - The client connection network address.
  // It is only safe to call this method during a Thrift processor RPC
  // implementation. Otherwise, the result of calling this method is undefined.
  // It is also only safe to reference the returned value during an RPC method.
  static const SessionContext* GetThreadSessionContext();

 private:
  // True if the server has been successfully started, for internal use only
  bool started_;

  // The port on which the server interface is exposed
  int port_;

  // How many worker threads to use to serve incoming requests
  // (requests are queued if no thread is immediately available)
  int num_worker_threads_;

  // ThreadPool or NonBlocking server
  ServerType server_type_;

  // User-specified identifier that shows up in logs
  const std::string name_;

  // Thread that runs the TNonblockingServer::serve loop
  boost::scoped_ptr<boost::thread> server_thread_;

  // Thrift housekeeping
  boost::scoped_ptr<apache::thrift::server::TServer> server_;
  boost::shared_ptr<apache::thrift::TProcessor> processor_;

  // If not NULL, called when session events happen. Not owned by us.
  SessionHandlerIf* session_handler_;

  // Protects session_contexts_
  boost::mutex session_contexts_lock_;

  // Map of active session context to a shared_ptr containing that context; when an item
  // is removed from the map, it is automatically freed.
  typedef boost::unordered_map<SessionContext*, boost::shared_ptr<SessionContext> >
      SessionContextSet;
  SessionContextSet session_contexts_;

  // True if using a secure transport
  bool kerberos_enabled_;

  // True if metrics are enabled
  bool metrics_enabled_;

  // Number of currently active connections
  Metrics::IntMetric* num_current_connections_metric_;

  // Total connections made over the lifetime of this server
  Metrics::IntMetric* total_connections_metric_;

  // Used to generate a unique session id for every connection
  boost::uuids::random_generator uuid_generator_;

  // Helper class which monitors starting servers. Needs access to internal members, and
  // is not used outside of this class.
  class ThriftServerEventProcessor;
  friend class ThriftServerEventProcessor;
};

}

#endif
