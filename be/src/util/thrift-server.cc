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

#include <boost/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <thrift/concurrency/PosixThreadFactory.h>
#include <thrift/concurrency/Thread.h>
#include <thrift/concurrency/ThreadManager.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TNonblockingServer.h>
#include <thrift/server/TThreadPoolServer.h>
#include <thrift/server/TThreadedServer.h>
#include <thrift/transport/TSocket.h>
#include <thrift/server/TThreadPoolServer.h>
#include <thrift/transport/TServerSocket.h>
#include <gflags/gflags.h>

#include "gen-cpp/Types_types.h"
#include "util/thrift-server.h"
#include "util/authorization.h"
#include "util/debug-util.h"
#include "util/network-util.h"
#include "util/uid-util.h"

#include <sstream>

using namespace std;
using namespace boost;
using namespace boost::uuids;
using namespace apache::thrift;
using namespace apache::thrift::concurrency;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::server;

DEFINE_int32(rpc_cnxn_attempts, 10,
    "Advanced: The number of times to retry connecting to an RPC server. If zero or less,"
    "connections will be retried until successful");
DEFINE_int32(rpc_cnxn_retry_interval_ms, 2000,
    "Advanced: The interval, in ms, between retrying connections to an RPC server");
DECLARE_string(principal);
DECLARE_string(keytab_file);

namespace impala {

// Helper class that starts a server in a separate thread, and handles
// the inter-thread communication to monitor whether it started
// correctly.
class ThriftServer::ThriftServerEventProcessor : public TServerEventHandler {
 public:
  ThriftServerEventProcessor(ThriftServer* thrift_server)
      : thrift_server_(thrift_server),
        signal_fired_(false) { }

  // Called by TNonBlockingServer when server has acquired its resources and is ready to
  // serve, and signals to StartAndWaitForServer that start-up is finished.
  // From TServerEventHandler.
  virtual void preServe();

  // Called when a client connects; we create per-client state and call any
  // SessionHandlerIf handler.
  virtual void* createContext(shared_ptr<TProtocol> input, shared_ptr<TProtocol> output);

  // Called when a client starts an RPC; we set the thread-local session context.
  virtual void processContext(void* context, shared_ptr<TTransport> output);

  // Called when a client disconnects; we call any SessionHandlerIf handler.
  virtual void deleteContext(void* serverContext, shared_ptr<TProtocol> input,
      shared_ptr<TProtocol> output);

  // Waits for a timeout of TIMEOUT_MS for a server to signal that it has started
  // correctly.
  Status StartAndWaitForServer();

 private:
  // Lock used to ensure that there are no missed notifications between starting the
  // supervision thread and calling signal_cond_.timed_wait. Also used to ensure
  // thread-safe access to members of thrift_server_
  boost::mutex signal_lock_;

  // Condition variable that is notified by the supervision thread once either
  // a) all is well or b) an error occurred.
  boost::condition_variable signal_cond_;

  // The ThriftServer under management. This class is a friend of ThriftServer, and
  // reaches in to change member variables at will.
  ThriftServer* thrift_server_;

  // Guards against spurious condition variable wakeups
  bool signal_fired_;

  // The time, in milliseconds, to wait for a server to come up
  static const int TIMEOUT_MS = 2500;

  // Called in a separate thread; wraps TNonBlockingServer::serve in an exception handler
  void Supervise();
};

Status ThriftServer::ThriftServerEventProcessor::StartAndWaitForServer() {
  // Locking here protects against missed notifications if Supervise executes quickly
  unique_lock<mutex> lock(signal_lock_);
  thrift_server_->started_ = false;

  thrift_server_->server_thread_.reset(
      new Thread("thrift-server", "supervise",
                 &ThriftServer::ThriftServerEventProcessor::Supervise, this));

  system_time deadline = get_system_time() +
      posix_time::milliseconds(ThriftServer::ThriftServerEventProcessor::TIMEOUT_MS);

  // Loop protects against spurious wakeup. Locks provide necessary fences to ensure
  // visibility.
  while (!signal_fired_) {
    // Yields lock and allows supervision thread to continue and signal
    if (!signal_cond_.timed_wait(lock, deadline)) {
      stringstream ss;
      ss << "ThriftServer '" << thrift_server_->name_ << "' (on port: "
         << thrift_server_->port_ << ") did not start within "
         << ThriftServer::ThriftServerEventProcessor::TIMEOUT_MS << "ms";
      LOG(ERROR) << ss.str();
      return Status(ss.str());
    }
  }

  // started_ == true only if preServe was called. May be false if there was an exception
  // after preServe that was caught by Supervise, causing it to reset the error condition.
  if (thrift_server_->started_ == false) {
    stringstream ss;
    ss << "ThriftServer '" << thrift_server_->name_ << "' (on port: "
       << thrift_server_->port_ << ") did not start correctly ";
    LOG(ERROR) << ss.str();
    return Status(ss.str());
  }
  return Status::OK;
}

void ThriftServer::ThriftServerEventProcessor::Supervise() {
  DCHECK(thrift_server_->server_.get() != NULL);
  try {
    thrift_server_->server_->serve();
  } catch (TException& e) {
    LOG(ERROR) << "ThriftServer '" << thrift_server_->name_ << "' (on port: "
               << thrift_server_->port_ << ") exited due to TException: " << e.what();
  }
  {
    // signal_lock_ ensures mutual exclusion of access to thrift_server_
    lock_guard<mutex> lock(signal_lock_);
    thrift_server_->started_ = false;

    // There may not be anyone waiting on this signal (if the
    // exception occurs after startup). That's not a problem, this is
    // just to avoid waiting for the timeout in case of a bind
    // failure, for example.
    signal_fired_ = true;
  }
  signal_cond_.notify_all();
}

void ThriftServer::ThriftServerEventProcessor::preServe() {
  // Acquire the signal lock to ensure that StartAndWaitForServer is
  // waiting on signal_cond_ when we notify.
  lock_guard<mutex> lock(signal_lock_);
  signal_fired_ = true;

  // This is the (only) success path - if this is not reached within TIMEOUT_MS,
  // StartAndWaitForServer will indicate failure.
  thrift_server_->started_ = true;

  // Should only be one thread waiting on signal_cond_, but wake all just in case.
  signal_cond_.notify_all();
}

// This thread-local variable contains the current session context for whichever
// thrift server is currently serving a request on the current thread. This includes
// session state such as the session identifier and the username.
__thread ThriftServer::SessionContext* __session_context__;


const TUniqueId& ThriftServer::GetThreadSessionId() {
  return __session_context__->session_id;
}

const ThriftServer::SessionContext* ThriftServer::GetThreadSessionContext() {
  return __session_context__;
}

void* ThriftServer::ThriftServerEventProcessor::createContext(shared_ptr<TProtocol> input,
    shared_ptr<TProtocol> output) {
  TSocket* socket = NULL;
  TTransport* transport = input->getTransport().get();
  shared_ptr<SessionContext> session_ptr = shared_ptr<SessionContext>(new SessionContext);
  if (!thrift_server_->kerberos_enabled_) {
    switch (thrift_server_->server_type_) {
      case Nonblocking:
        socket = static_cast<TSocket*>(
            static_cast<TFramedTransport*>(transport)->getUnderlyingTransport().get());
        break;
      case ThreadPool:
      case Threaded:
        socket = static_cast<TSocket*>(
            static_cast<TBufferedTransport*>(transport)->getUnderlyingTransport().get());
        break;
      default:
        DCHECK(false) << "Unexpected thrift server type";
    }
  } else {
    TSaslServerTransport* sasl_transport = static_cast<TSaslServerTransport*>(transport);
    // Get the username from the transport.
    session_ptr->username = sasl_transport->getUsername();
    socket = static_cast<TSocket*>(sasl_transport->getUnderlyingTransport().get());
  }

  {
    session_ptr->network_address =
        MakeNetworkAddress(socket->getPeerAddress(), socket->getPeerPort());

    lock_guard<mutex> l(thrift_server_->session_contexts_lock_);
    uuid session_uuid = thrift_server_->uuid_generator_();
    UUIDToTUniqueId(session_uuid, &session_ptr->session_id);

    // Add the session to the session map.
    __session_context__ = session_ptr.get();
    thrift_server_->session_contexts_[session_ptr.get()] = session_ptr;
  }

  if (thrift_server_->session_handler_ != NULL) {
    thrift_server_->session_handler_->SessionStart(*__session_context__);
  }

  if (thrift_server_->metrics_enabled_) {
    thrift_server_->num_current_connections_metric_->Increment(1L);
    thrift_server_->total_connections_metric_->Increment(1L);
  }

  // Store the __session_context__ in the per-client context. If only this were
  // accessible from RPC method calls, we wouldn't have to
  // mess around with thread locals.
  return (void*)__session_context__;
}

void ThriftServer::ThriftServerEventProcessor::processContext(void* context,
    shared_ptr<TTransport> transport) {
  __session_context__ = reinterpret_cast<SessionContext*>(context);
}

void ThriftServer::ThriftServerEventProcessor::deleteContext(void* serverContext,
    shared_ptr<TProtocol> input, shared_ptr<TProtocol> output) {

  __session_context__ = (SessionContext*) serverContext;

  if (thrift_server_->session_handler_ != NULL) {
    thrift_server_->session_handler_->SessionEnd(*__session_context__);
  }

  {
    lock_guard<mutex> l(thrift_server_->session_contexts_lock_);
    thrift_server_->session_contexts_.erase(__session_context__);
  }

  if (thrift_server_->metrics_enabled_) {
    thrift_server_->num_current_connections_metric_->Increment(-1L);
  }
}

ThriftServer::ThriftServer(const string& name, const shared_ptr<TProcessor>& processor,
    int port, Metrics* metrics, int num_worker_threads, ServerType server_type)
    : started_(false),
      port_(port),
      num_worker_threads_(num_worker_threads),
      server_type_(server_type),
      name_(name),
      server_thread_(NULL),
      server_(NULL),
      processor_(processor),
      session_handler_(NULL),
      kerberos_enabled_(false) {
  if (metrics != NULL) {
    metrics_enabled_ = true;
    stringstream count_ss;
    count_ss << "impala.thrift-server." << name << ".connections-in-use";
    num_current_connections_metric_ =
        metrics->CreateAndRegisterPrimitiveMetric(count_ss.str(), 0L);
    stringstream max_ss;
    max_ss << "impala.thrift-server." << name << ".total-connections";
    total_connections_metric_ =
        metrics->CreateAndRegisterPrimitiveMetric(max_ss.str(), 0L);
  } else {
    metrics_enabled_ = false;
  }
}

Status ThriftServer::Start() {
  DCHECK(!started_);
  shared_ptr<TProtocolFactory> protocol_factory(new TBinaryProtocolFactory());
  shared_ptr<ThreadManager> thread_mgr;
  shared_ptr<ThreadFactory> thread_factory(new PosixThreadFactory());
  shared_ptr<TServerTransport> fe_server_transport;
  shared_ptr<TTransportFactory> transport_factory;

  // TODO: The thrift non-blocking server needs to be fixed.
  if (server_type_ == Nonblocking && !FLAGS_principal.empty()) {
    string mesg("Nonblocking servers cannot be used with Kerberos");
    LOG(ERROR) << mesg;
    return (Status(mesg));
  }

  if (server_type_ != Threaded) {
    thread_mgr = ThreadManager::newSimpleThreadManager(num_worker_threads_);
    thread_mgr->threadFactory(thread_factory);
    thread_mgr->start();
  }

  if (!FLAGS_principal.empty()) {
    if (FLAGS_keytab_file.empty()) {
      LOG(ERROR) << "Kerberos principal, '" << FLAGS_principal <<
          "' specified, but no keyfile";
      return Status("no keyfile");
    }
    RETURN_IF_ERROR(GetKerberosTransportFactory(FLAGS_principal,
        FLAGS_keytab_file, &transport_factory));
    kerberos_enabled_ = true;
  }

  // Note - if you change the transport types here, you must check that the
  // logic in createContext is still accurate.
  TServerSocket* server_socket;
  switch (server_type_) {
    case Nonblocking:
      if (transport_factory.get() == NULL) {
        transport_factory.reset(new TTransportFactory());
      }
      server_.reset(new TNonblockingServer(processor_,
          transport_factory, transport_factory,
          protocol_factory, protocol_factory, port_, thread_mgr));
      break;
    case ThreadPool:
      fe_server_transport.reset(new TServerSocket(port_));
      if (transport_factory.get() == NULL) {
        transport_factory.reset(new TBufferedTransportFactory());
      }
      server_.reset(new TThreadPoolServer(processor_, fe_server_transport,
          transport_factory, protocol_factory, thread_mgr));
      break;
    case Threaded:
      server_socket = new TServerSocket(port_);
      //      server_socket->setAcceptTimeout(500);
      fe_server_transport.reset(server_socket);
      if (transport_factory.get() == NULL) {
        transport_factory.reset(new TBufferedTransportFactory());
      }
      server_.reset(new TThreadedServer(processor_, fe_server_transport,
          transport_factory, protocol_factory, thread_factory));
      break;
    default:
      stringstream error_msg;
      error_msg << "Unsupported server type: " << server_type_;
      LOG(ERROR) << error_msg.str();
      return Status(error_msg.str());
  }
  shared_ptr<ThriftServer::ThriftServerEventProcessor> event_processor(
      new ThriftServer::ThriftServerEventProcessor(this));
  server_->setServerEventHandler(event_processor);

  RETURN_IF_ERROR(event_processor->StartAndWaitForServer());

  LOG(INFO) << "ThriftServer '" << name_ << "' started on port: " << port_;
  DCHECK(started_);
  return Status::OK;
}

void ThriftServer::Join() {
  DCHECK(server_thread_ != NULL);
  DCHECK(started_);
  server_thread_->Join();
}

void ThriftServer::StopForTesting() {
  DCHECK(server_thread_ != NULL);
  DCHECK(server_);
  DCHECK_EQ(server_type_, Threaded);
  server_->stop();
  if (started_) Join();
}
}
