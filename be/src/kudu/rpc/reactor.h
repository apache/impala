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
#ifndef KUDU_RPC_REACTOR_H
#define KUDU_RPC_REACTOR_H

#include <stdint.h>

#include <list>
#include <map>
#include <memory>
#include <set>
#include <string>

#include <boost/function.hpp>
#include <boost/intrusive/list.hpp>
#include <boost/optional.hpp>
#include <ev++.h>

#include "kudu/gutil/ref_counted.h"
#include "kudu/rpc/connection.h"
#include "kudu/rpc/connection_id.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/transfer.h"
#include "kudu/util/thread.h"
#include "kudu/util/locks.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/status.h"

namespace kudu {

class Socket;

namespace rpc {

typedef std::list<scoped_refptr<Connection>> conn_list_t;

class DumpRunningRpcsRequestPB;
class DumpRunningRpcsResponsePB;
class Messenger;
class MessengerBuilder;
class Reactor;
enum class CredentialsPolicy;

// Simple metrics information from within a reactor.
struct ReactorMetrics {
  // Number of client RPC connections currently connected.
  int32_t num_client_connections_;
  // Number of server RPC connections currently connected.
  int32_t num_server_connections_;

  // Total number of client RPC connections opened during Reactor's lifetime.
  uint64_t total_client_connections_;
  // Total number of server RPC connections opened during Reactor's lifetime.
  uint64_t total_server_connections_;
};

// A task which can be enqueued to run on the reactor thread.
class ReactorTask : public boost::intrusive::list_base_hook<> {
 public:
  ReactorTask();

  // Run the task. 'reactor' is guaranteed to be the current thread.
  virtual void Run(ReactorThread *reactor) = 0;

  // Abort the task, in the case that the reactor shut down before the
  // task could be processed. This may or may not run on the reactor thread
  // itself.
  //
  // The Reactor guarantees that the Reactor lock is free when this
  // method is called.
  virtual void Abort(const Status &abort_status) {}

  virtual ~ReactorTask();

 private:
  DISALLOW_COPY_AND_ASSIGN(ReactorTask);
};

// A ReactorTask that is scheduled to run at some point in the future.
//
// Semantically it works like RunFunctionTask with a few key differences:
// 1. The user function is called during Abort. Put another way, the
//    user function is _always_ invoked, even during reactor shutdown.
// 2. To differentiate between Abort and non-Abort, the user function
//    receives a Status as its first argument.
class DelayedTask : public ReactorTask {
 public:
  DelayedTask(boost::function<void(const Status &)> func, MonoDelta when);

  // Schedules the task for running later but doesn't actually run it yet.
  void Run(ReactorThread* thread) override;

  // Behaves like ReactorTask::Abort.
  void Abort(const Status& abort_status) override;

 private:
  // libev callback for when the registered timer fires.
  void TimerHandler(ev::timer& watcher, int revents);

  // User function to invoke when timer fires or when task is aborted.
  const boost::function<void(const Status&)> func_;

  // Delay to apply to this task.
  const MonoDelta when_;

  // Link back to registering reactor thread.
  ReactorThread* thread_;

  // libev timer. Set when Run() is invoked.
  ev::timer timer_;
};

// A ReactorThread is a libev event handler thread which manages I/O
// on a list of sockets.
//
// All methods in this class are _only_ called from the reactor thread itself
// except where otherwise specified. New methods should DCHECK(IsCurrentThread())
// to ensure this.
class ReactorThread {
 public:
  friend class Connection;

  // Client-side connection map. Multiple connections could be open to a remote
  // server if multiple credential policies are used for individual RPCs.
  typedef std::unordered_multimap<ConnectionId, scoped_refptr<Connection>,
                                  ConnectionIdHash, ConnectionIdEqual>
      conn_multimap_t;

  ReactorThread(Reactor *reactor, const MessengerBuilder &bld);

  // This may be called from another thread.
  Status Init();

  // Add any connections on this reactor thread into the given status dump.
  Status DumpRunningRpcs(const DumpRunningRpcsRequestPB& req,
                         DumpRunningRpcsResponsePB* resp);

  // Block until the Reactor thread is shut down
  //
  // This must be called from another thread.
  void Shutdown();

  // This method is thread-safe.
  void WakeThread();

  // libev callback for handling async notifications in our epoll thread.
  void AsyncHandler(ev::async &watcher, int revents);

  // libev callback for handling timer events in our epoll thread.
  void TimerHandler(ev::timer &watcher, int revents);

  // Register an epoll timer watcher with our event loop.
  // Does not set a timeout or start it.
  void RegisterTimeout(ev::timer *watcher);

  // This may be called from another thread.
  const std::string &name() const;

  MonoTime cur_time() const;

  // This may be called from another thread.
  Reactor *reactor();

  // Return true if this reactor thread is the thread currently
  // running. Should be used in DCHECK assertions.
  bool IsCurrentThread() const;

  // Begin the process of connection negotiation.
  // Must be called from the reactor thread.
  Status StartConnectionNegotiation(const scoped_refptr<Connection>& conn);

  // Transition back from negotiating to processing requests.
  // Must be called from the reactor thread.
  void CompleteConnectionNegotiation(const scoped_refptr<Connection>& conn,
                                     const Status& status,
                                     std::unique_ptr<ErrorStatusPB> rpc_error);

  // Collect metrics.
  // Must be called from the reactor thread.
  Status GetMetrics(ReactorMetrics *metrics);

 private:
  friend class AssignOutboundCallTask;
  friend class CancellationTask;
  friend class RegisterConnectionTask;
  friend class DelayedTask;

  // Run the main event loop of the reactor.
  void RunThread();

  // Find a connection to the given remote and returns it in 'conn'.
  // Returns true if a connection is found. Returns false otherwise.
  bool FindConnection(const ConnectionId& conn_id,
                      CredentialsPolicy cred_policy,
                      scoped_refptr<Connection>* conn);

  // Find or create a new connection to the given remote.
  // If such a connection already exists, returns that, otherwise creates a new one.
  // May return a bad Status if the connect() call fails.
  // The resulting connection object is managed internally by the reactor thread.
  Status FindOrStartConnection(const ConnectionId& conn_id,
                               CredentialsPolicy cred_policy,
                               scoped_refptr<Connection>* conn);

  // Shut down the given connection, removing it from the connection tracking
  // structures of this reactor.
  //
  // The connection is not explicitly deleted -- shared_ptr reference counting
  // may hold on to the object after this, but callers should assume that it
  // _may_ be deleted by this call.
  void DestroyConnection(Connection *conn, const Status &conn_status,
                         std::unique_ptr<ErrorStatusPB> rpc_error = {});

  // Scan any open connections for idle ones that have been idle longer than
  // connection_keepalive_time_
  void ScanIdleConnections();

  // Create a new client socket (non-blocking, NODELAY)
  static Status CreateClientSocket(Socket *sock);

  // Initiate a new connection on the given socket.
  static Status StartConnect(Socket *sock, const Sockaddr &remote);

  // Assign a new outbound call to the appropriate connection object.
  // If this fails, the call is marked failed and completed.
  void AssignOutboundCall(const std::shared_ptr<OutboundCall> &call);

  // Cancel the outbound call. May update corresponding connection
  // object to remove call from the CallAwaitingResponse object.
  // Also mark the call as slated for cancellation so the callback
  // may be invoked early if the RPC hasn't yet been sent or if it's
  // waiting for a response from the remote.
  void CancelOutboundCall(const std::shared_ptr<OutboundCall> &call);

  // Register a new connection.
  void RegisterConnection(scoped_refptr<Connection> conn);

  // Actually perform shutdown of the thread, tearing down any connections,
  // etc. This is called from within the thread.
  void ShutdownInternal();

  scoped_refptr<kudu::Thread> thread_;

  // our epoll object (or kqueue, etc).
  ev::dynamic_loop loop_;

  // Used by other threads to notify the reactor thread
  ev::async async_;

  // Handles the periodic timer.
  ev::timer timer_;

  // Scheduled (but not yet run) delayed tasks.
  //
  // Each task owns its own memory and must be freed by its TaskRun and
  // Abort members, provided it was allocated on the heap.
  std::set<DelayedTask*> scheduled_tasks_;

  // The current monotonic time.  Updated every coarse_timer_granularity_secs_.
  MonoTime cur_time_;

  // last time we did TCP timeouts.
  MonoTime last_unused_tcp_scan_;

  // Map of sockaddrs to Connection objects for outbound (client) connections.
  conn_multimap_t client_conns_;

  // List of current connections coming into the server.
  conn_list_t server_conns_;

  Reactor *reactor_;

  // If a connection has been idle for this much time, it is torn down.
  const MonoDelta connection_keepalive_time_;

  // Scan for idle connections on this granularity.
  const MonoDelta coarse_timer_granularity_;

  // Total number of client connections opened during Reactor's lifetime.
  uint64_t total_client_conns_cnt_;

  // Total number of server connections opened during Reactor's lifetime.
  uint64_t total_server_conns_cnt_;
};

// A Reactor manages a ReactorThread
class Reactor {
 public:
  Reactor(std::shared_ptr<Messenger> messenger,
          int index,
          const MessengerBuilder &bld);
  Status Init();

  // Block until the Reactor is shut down
  void Shutdown();

  ~Reactor();

  const std::string &name() const;

  // Collect metrics about the reactor.
  Status GetMetrics(ReactorMetrics *metrics);

  // Add any connections on this reactor thread into the given status dump.
  Status DumpRunningRpcs(const DumpRunningRpcsRequestPB& req,
                         DumpRunningRpcsResponsePB* resp);

  // Queue a new incoming connection. Takes ownership of the underlying fd from
  // 'socket', but not the Socket object itself.
  // If the reactor is already shut down, takes care of closing the socket.
  void RegisterInboundSocket(Socket *socket, const Sockaddr &remote);

  // Queue a new call to be sent. If the reactor is already shut down, marks
  // the call as failed.
  void QueueOutboundCall(const std::shared_ptr<OutboundCall> &call);

  // Queue a new reactor task to cancel an outbound call.
  void QueueCancellation(const std::shared_ptr<OutboundCall> &call);

  // Schedule the given task's Run() method to be called on the
  // reactor thread.
  // If the reactor shuts down before it is run, the Abort method will be
  // called.
  // Does _not_ take ownership of 'task' -- the task should take care of
  // deleting itself after running if it is allocated on the heap.
  void ScheduleReactorTask(ReactorTask *task);

  Status RunOnReactorThread(const boost::function<Status()>& f);

  // If the Reactor is closing, returns false.
  // Otherwise, drains the pending_tasks_ queue into the provided list.
  bool DrainTaskQueue(boost::intrusive::list<ReactorTask> *tasks);

  Messenger *messenger() const {
    return messenger_.get();
  }

  // Indicates whether the reactor is shutting down.
  //
  // This method is thread-safe.
  bool closing() const;

  // Is this reactor's thread the current thread?
  bool IsCurrentThread() const {
    return thread_.IsCurrentThread();
  }

 private:
  friend class ReactorThread;
  typedef simple_spinlock LockType;
  mutable LockType lock_;

  // parent messenger
  std::shared_ptr<Messenger> messenger_;

  const std::string name_;

  // Whether the reactor is shutting down.
  // Guarded by lock_.
  bool closing_;

  // Tasks to be run within the reactor thread.
  // Guarded by lock_.
  boost::intrusive::list<ReactorTask> pending_tasks_; // NOLINT(build/include_what_you_use)

  ReactorThread thread_;

  DISALLOW_COPY_AND_ASSIGN(Reactor);
};

} // namespace rpc
} // namespace kudu

#endif
