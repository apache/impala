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

#include <boost/filesystem.hpp>
#include <boost/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <thrift/concurrency/Thread.h>
#include <thrift/concurrency/ThreadManager.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TThreadPoolServer.h>
#include <thrift/server/TThreadedServer.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TSSLServerSocket.h>
#include <thrift/transport/TSSLSocket.h>
#include <thrift/server/TThreadPoolServer.h>
#include <thrift/transport/TServerSocket.h>
#include <gflags/gflags.h>

#include "gen-cpp/Types_types.h"
#include "rpc/authentication.h"
#include "rpc/thrift-server.h"
#include "rpc/thrift-thread.h"
#include "util/debug-util.h"
#include "util/network-util.h"
#include "util/os-util.h"
#include "util/uid-util.h"
#include <sstream>

#include "common/names.h"

namespace posix_time = boost::posix_time;
using namespace boost::algorithm;
using boost::filesystem::exists;
using boost::get_system_time;
using boost::system_time;
using boost::uuids::uuid;
using namespace apache::thrift::concurrency;
using namespace apache::thrift::protocol;
using namespace apache::thrift::server;
using namespace apache::thrift::transport;
using namespace apache::thrift;

DEFINE_int32(rpc_cnxn_attempts, 10, "Deprecated");
DEFINE_int32(rpc_cnxn_retry_interval_ms, 2000, "Deprecated");
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

  // Called by the Thrift server implementation when it has acquired its resources and is
  // ready to serve, and signals to StartAndWaitForServer that start-up is finished. From
  // TServerEventHandler.
  virtual void preServe();

  // Called when a client connects; we create per-client state and call any
  // ConnectionHandlerIf handler.
  virtual void* createContext(shared_ptr<TProtocol> input, shared_ptr<TProtocol> output);

  // Called when a client starts an RPC; we set the thread-local connection context.
  virtual void processContext(void* context, shared_ptr<TTransport> output);

  // Called when a client disconnects; we call any ConnectionHandlerIf handler.
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

  // Called in a separate thread
  void Supervise();
};

Status ThriftServer::ThriftServerEventProcessor::StartAndWaitForServer() {
  // Locking here protects against missed notifications if Supervise executes quickly
  unique_lock<mutex> lock(signal_lock_);
  thrift_server_->started_ = false;

  stringstream name;
  name << "supervise-" << thrift_server_->name_;
  thrift_server_->server_thread_.reset(
      new Thread("thrift-server", name.str(),
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
  return Status::OK();
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

// This thread-local variable contains the current connection context for whichever
// thrift server is currently serving a request on the current thread. This includes
// connection state such as the connection identifier and the username.
__thread ThriftServer::ConnectionContext* __connection_context__;


const TUniqueId& ThriftServer::GetThreadConnectionId() {
  return __connection_context__->connection_id;
}

const ThriftServer::ConnectionContext* ThriftServer::GetThreadConnectionContext() {
  return __connection_context__;
}

void* ThriftServer::ThriftServerEventProcessor::createContext(shared_ptr<TProtocol> input,
    shared_ptr<TProtocol> output) {
  TSocket* socket = NULL;
  TTransport* transport = input->getTransport().get();
  shared_ptr<ConnectionContext> connection_ptr =
      shared_ptr<ConnectionContext>(new ConnectionContext);
  TTransport* underlying_transport =
      (static_cast<TBufferedTransport*>(transport))->getUnderlyingTransport().get();
  if (!thrift_server_->auth_provider_->is_sasl()) {
    socket = static_cast<TSocket*>(underlying_transport);
  } else {
    TSaslServerTransport* sasl_transport = static_cast<TSaslServerTransport*>(
        underlying_transport);

    // Get the username from the transport.
    connection_ptr->username = sasl_transport->getUsername();
    socket = static_cast<TSocket*>(sasl_transport->getUnderlyingTransport().get());
  }

  {
    connection_ptr->server_name = thrift_server_->name_;
    connection_ptr->network_address =
        MakeNetworkAddress(socket->getPeerAddress(), socket->getPeerPort());

    lock_guard<mutex> l(thrift_server_->connection_contexts_lock_);
    uuid connection_uuid = thrift_server_->uuid_generator_();
    UUIDToTUniqueId(connection_uuid, &connection_ptr->connection_id);

    // Add the connection to the connection map.
    __connection_context__ = connection_ptr.get();
    thrift_server_->connection_contexts_[connection_ptr.get()] = connection_ptr;
  }

  if (thrift_server_->connection_handler_ != NULL) {
    thrift_server_->connection_handler_->ConnectionStart(*__connection_context__);
  }

  if (thrift_server_->metrics_enabled_) {
    thrift_server_->num_current_connections_metric_->Increment(1L);
    thrift_server_->total_connections_metric_->Increment(1L);
  }

  // Store the __connection_context__ in the per-client context. If only this were
  // accessible from RPC method calls, we wouldn't have to
  // mess around with thread locals.
  return (void*)__connection_context__;
}

void ThriftServer::ThriftServerEventProcessor::processContext(void* context,
    shared_ptr<TTransport> transport) {
  __connection_context__ = reinterpret_cast<ConnectionContext*>(context);
}

void ThriftServer::ThriftServerEventProcessor::deleteContext(void* serverContext,
    shared_ptr<TProtocol> input, shared_ptr<TProtocol> output) {
  __connection_context__ = (ConnectionContext*) serverContext;

  if (thrift_server_->connection_handler_ != NULL) {
    thrift_server_->connection_handler_->ConnectionEnd(*__connection_context__);
  }

  {
    lock_guard<mutex> l(thrift_server_->connection_contexts_lock_);
    thrift_server_->connection_contexts_.erase(__connection_context__);
  }

  if (thrift_server_->metrics_enabled_) {
    thrift_server_->num_current_connections_metric_->Increment(-1L);
  }
}

ThriftServer::ThriftServer(const string& name, const shared_ptr<TProcessor>& processor,
    int port, AuthProvider* auth_provider, MetricGroup* metrics, int num_worker_threads,
    ServerType server_type)
    : started_(false),
      port_(port),
      ssl_enabled_(false),
      num_worker_threads_(num_worker_threads),
      server_type_(server_type),
      name_(name),
      server_thread_(NULL),
      server_(NULL),
      processor_(processor),
      connection_handler_(NULL),
      auth_provider_(auth_provider) {
  if (auth_provider_ == NULL) {
    auth_provider_ = AuthManager::GetInstance()->GetInternalAuthProvider();
  }
  if (metrics != NULL) {
    metrics_enabled_ = true;
    stringstream count_ss;
    count_ss << "impala.thrift-server." << name << ".connections-in-use";
    num_current_connections_metric_ = metrics->AddGauge(count_ss.str(), 0L);
    stringstream max_ss;
    max_ss << "impala.thrift-server." << name << ".total-connections";
    total_connections_metric_ = metrics->AddCounter(max_ss.str(), 0L);
  } else {
    metrics_enabled_ = false;
  }
}

/// Factory subclass to override getPassword() which provides a password string to Thrift
/// to decrypt the private key file.
class ImpalaSslSocketFactory : public TSSLSocketFactory {
 public:
  ImpalaSslSocketFactory(const string& password) : password_(password) { }

 protected:
  virtual void getPassword(string& output, int size) {
    output = password_;
    if (output.size() > size) output.resize(size);
  }

 private:
  /// The password string.
  const string password_;
};

Status ThriftServer::CreateSocket(shared_ptr<TServerTransport>* socket) {
  if (ssl_enabled()) {
    // This 'factory' is only called once, since CreateSocket() is only called from
    // Start()
    shared_ptr<TSSLSocketFactory> socket_factory(
        new ImpalaSslSocketFactory(key_password_));
    socket_factory->overrideDefaultPasswordCallback();
    try {
      socket_factory->loadCertificate(certificate_path_.c_str());
      socket_factory->loadPrivateKey(private_key_path_.c_str());
      socket->reset(new TSSLServerSocket(port_, socket_factory));
    } catch (const TException& e) {
      return Status(TErrorCode::SSL_SOCKET_CREATION_FAILED, e.what());
    }
    return Status::OK();
  } else {
    socket->reset(new TServerSocket(port_));
    return Status::OK();
  }
}

Status ThriftServer::EnableSsl(const string& certificate, const string& private_key,
    const string& pem_password_cmd) {
  DCHECK(!started_);
  if (certificate.empty()) return Status(TErrorCode::SSL_CERTIFICATE_PATH_BLANK);
  if (private_key.empty()) return Status(TErrorCode::SSL_PRIVATE_KEY_PATH_BLANK);

  if (!exists(certificate)) {
    return Status(TErrorCode::SSL_CERTIFICATE_NOT_FOUND, certificate);
  }

  // TODO: Consider warning if private key file is world-readable
  if (!exists(private_key)) {
    return Status(TErrorCode::SSL_PRIVATE_KEY_NOT_FOUND, private_key);
  }

  ssl_enabled_ = true;
  certificate_path_ = certificate;
  private_key_path_ = private_key;

  if (!pem_password_cmd.empty()) {
    if (!RunShellProcess(pem_password_cmd, &key_password_)) {
      return Status(TErrorCode::SSL_PASSWORD_CMD_FAILED, pem_password_cmd, key_password_);
    } else {
      trim_right(key_password_);
      LOG(INFO) << "Command '" << pem_password_cmd << "' executed successfully, "
                << ".PEM password retrieved";
    }
  }

  return Status::OK();
}

Status ThriftServer::Start() {
  DCHECK(!started_);
  shared_ptr<TProtocolFactory> protocol_factory(new TBinaryProtocolFactory());
  shared_ptr<ThreadFactory> thread_factory(
      new ThriftThreadFactory("thrift-server", name_));

  // Note - if you change the transport types here, you must check that the
  // logic in createContext is still accurate.
  shared_ptr<TServerTransport> server_socket;
  shared_ptr<TTransportFactory> transport_factory;
  RETURN_IF_ERROR(CreateSocket(&server_socket));
  RETURN_IF_ERROR(auth_provider_->GetServerTransportFactory(&transport_factory));
  switch (server_type_) {
    case ThreadPool:
      {
        shared_ptr<ThreadManager> thread_mgr(
            ThreadManager::newSimpleThreadManager(num_worker_threads_));
        thread_mgr->threadFactory(thread_factory);
        thread_mgr->start();
        server_.reset(new TThreadPoolServer(processor_, server_socket,
                transport_factory, protocol_factory, thread_mgr));
      }
      break;
    case Threaded:
      server_.reset(new TThreadedServer(processor_, server_socket,
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

  LOG(INFO) << "ThriftServer '" << name_ << "' started on port: " << port_
            << (ssl_enabled() ? "s" : "");
  DCHECK(started_);
  return Status::OK();
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
