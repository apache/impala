// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_UTIL_THRIFT_SERVER_H
#define IMPALA_UTIL_THRIFT_SERVER_H

#include <boost/thread/mutex.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/unordered_map.hpp>
#include <server/TNonblockingServer.h>
#include <TProcessor.h>

#include "common/status.h"

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
  typedef std::string SessionKey;

  // Interface class for receiving session creation / termination events.
  class SessionHandlerIf {
   public:
    // Called when a session is established (when a client connects).
    virtual void SessionStart(const SessionKey& session_key) = 0;

    // Called when a session is terminated (when a client closes the connection).
    // After this callback returns, the memory session_key references is no longer valid
    // and clients must not refer to it again. 
    virtual void SessionEnd(const SessionKey& session_key) = 0;
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
  ThriftServer(const std::string& name,
      const boost::shared_ptr<apache::thrift::TProcessor>& processor, int port,
      int num_worker_threads = DEFAULT_WORKER_THREADS,
      ServerType server_type = Threaded);

  int port() const { return port_; }

  // Blocks until the server stops and exits its main thread.
  void Join();

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
  static SessionKey* GetThreadSessionKey();

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

  // Protects session_keys_
  boost::mutex session_keys_lock_;

  // Map of active session keys to shared_ptr containing that key; when a key is
  // removed it is automatically freed.
  typedef boost::unordered_map<SessionKey*, boost::shared_ptr<SessionKey> > SessionKeySet;
  SessionKeySet session_keys_;

  // True if using a secure transport
  bool kerberos_enabled_;

  // Helper class which monitors starting servers. Needs access to internal members, and
  // is not used outside of this class.
  class ThriftServerEventProcessor;
  friend class ThriftServerEventProcessor;
};

}

#endif
