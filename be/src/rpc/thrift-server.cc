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

#include <mutex>
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <openssl/err.h>
#include <thrift/concurrency/Thread.h>
#include <thrift/concurrency/ThreadManager.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TSSLServerSocket.h>
#include <thrift/transport/TSSLSocket.h>
#include <thrift/transport/TServerSocket.h>
#include <gflags/gflags.h>

#include <sstream>
#include "gen-cpp/Types_types.h"
#include "kudu/util/openssl_util.h"
#include "rpc/TAcceptQueueServer.h"
#include "rpc/auth-provider.h"
#include "rpc/authentication.h"
#include "rpc/thrift-server.h"
#include "rpc/thrift-thread.h"
#include "rpc/thrift-util.h"
#include "transport/THttpServer.h"
#include "util/condition-variable.h"
#include "util/debug-util.h"
#include "util/metrics.h"
#include "util/network-util.h"
#include "util/openssl-util.h"
#include "util/os-util.h"
#include "util/thread.h"
#include "util/uid-util.h"

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

DECLARE_string(principal);
DECLARE_string(keytab_file);

namespace impala {

// Specifies the allowed set of values for --ssl_minimum_version. To keep consistent with
// Apache Kudu, specifying a single version enables all versions including and succeeding
// that one (e.g. TLSv1.1 enables v1.1 and v1.2).
map<string, SSLProtocol> SSLProtoVersions::PROTO_MAP = {
    {"tlsv1.2", TLSv1_2},
    {"tlsv1.1", TLSv1_1},
    {"tlsv1", TLSv1_0}};

// A generic wrapper for OpenSSL structures.
template <typename T>
using c_unique_ptr = std::unique_ptr<T, std::function<void(T*)>>;

Status SSLProtoVersions::StringToProtocol(const string& in, SSLProtocol* protocol) {
  for (const auto& proto : SSLProtoVersions::PROTO_MAP) {
    if (iequals(in, proto.first)) {
      *protocol = proto.second;
      return Status::OK();
    }
  }

  return Status(Substitute("Unknown TLS version: '$0'", in));
}

bool SSLProtoVersions::IsSupported(const SSLProtocol& protocol) {
  DCHECK_LE(protocol, TLSv1_2);
  int max_supported_tls_version = MaxSupportedTlsVersion();
  DCHECK_GE(max_supported_tls_version, TLS1_VERSION);

  switch (max_supported_tls_version) {
    case TLS1_VERSION:
      return protocol == TLSv1_0;
    case TLS1_1_VERSION:
      return protocol == TLSv1_0 || protocol == TLSv1_1;
    default:
      DCHECK_GE(max_supported_tls_version, TLS1_2_VERSION);
      return true;
  }
}

Status ThriftServer::ThriftServerEventProcessor::StartAndWaitForServer() {
  // Locking here protects against missed notifications if Supervise executes quickly
  unique_lock<mutex> lock(signal_lock_);
  thrift_server_->started_ = false;

  stringstream name;
  name << "supervise-" << thrift_server_->name_;
  RETURN_IF_ERROR(Thread::Create("thrift-server", name.str(),
      &ThriftServer::ThriftServerEventProcessor::Supervise, this,
      &thrift_server_->server_thread_));

  timespec deadline;
  TimeFromNowMillis(ThriftServer::ThriftServerEventProcessor::TIMEOUT_MS, &deadline);

  // Loop protects against spurious wakeup. Locks provide necessary fences to ensure
  // visibility.
  while (!signal_fired_) {
    // Yields lock and allows supervision thread to continue and signal
    if (!signal_cond_.WaitUntil(lock, deadline)) {
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
  signal_cond_.NotifyAll();
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
  signal_cond_.NotifyAll();
}

// This thread-local variable contains the current connection context for whichever
// thrift server is currently serving a request on the current thread. This includes
// connection state such as the connection identifier and the username.
__thread ThriftServer::ConnectionContext* __connection_context__;

bool ThriftServer::HasThreadConnectionContext() {
  return __connection_context__ != nullptr;
}

const TUniqueId& ThriftServer::GetThreadConnectionId() {
  return __connection_context__->connection_id;
}

const ThriftServer::ConnectionContext* ThriftServer::GetThreadConnectionContext() {
  return __connection_context__;
}

void* ThriftServer::ThriftServerEventProcessor::createContext(
    std::shared_ptr<TProtocol> input, std::shared_ptr<TProtocol> output) {
  std::shared_ptr<ConnectionContext> connection_ptr =
      std::shared_ptr<ConnectionContext>(new ConnectionContext);
  thrift_server_->auth_provider_->SetupConnectionContext(connection_ptr,
      thrift_server_->transport_type_, input->getTransport().get(),
      output->getTransport().get());

  {
    connection_ptr->server_name = thrift_server_->name_;

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
    std::shared_ptr<TTransport> transport) {
  __connection_context__ = reinterpret_cast<ConnectionContext*>(context);
}

bool ThriftServer::ThriftServerEventProcessor::IsIdleContext(void* context) {
  __connection_context__ = reinterpret_cast<ConnectionContext*>(context);
  if (thrift_server_->connection_handler_ != nullptr) {
    return thrift_server_->connection_handler_->IsIdleConnection(*__connection_context__);
  }
  return false;
}

void ThriftServer::ThriftServerEventProcessor::deleteContext(void* context,
    std::shared_ptr<TProtocol> input, std::shared_ptr<TProtocol> output) {
  __connection_context__ = reinterpret_cast<ConnectionContext*>(context);

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

ThriftServer::ThriftServer(const string& name,
    const std::shared_ptr<TProcessor>& processor, int port, AuthProvider* auth_provider,
    MetricGroup* metrics, int max_concurrent_connections, int64_t queue_timeout_ms,
    int64_t idle_poll_period_ms, TransportType transport_type,
    bool is_external_facing)
  : started_(false),
    port_(port),
    ssl_enabled_(false),
    max_concurrent_connections_(max_concurrent_connections),
    queue_timeout_ms_(queue_timeout_ms),
    idle_poll_period_ms_(idle_poll_period_ms),
    name_(name),
    metrics_name_(Substitute("impala.thrift-server.$0", name_)),
    server_(NULL),
    processor_(processor),
    connection_handler_(NULL),
    metrics_(NULL),
    auth_provider_(auth_provider),
    transport_type_(transport_type),
    is_external_facing_(is_external_facing) {
  if (auth_provider_ == NULL) {
    auth_provider_ = AuthManager::GetInstance()->GetInternalAuthProvider();
  }
  if (metrics != NULL) {
    metrics_enabled_ = true;
    stringstream count_ss;
    count_ss << metrics_name_ << ".connections-in-use";
    num_current_connections_metric_ = metrics->AddGauge(count_ss.str(), 0);
    stringstream max_ss;
    max_ss << metrics_name_ << ".total-connections";
    total_connections_metric_ = metrics->AddCounter(max_ss.str(), 0);
    metrics_ = metrics;
  } else {
    metrics_enabled_ = false;
  }
}

namespace {

/// Factory subclass to override getPassword() which provides a password string to Thrift
/// to decrypt the private key file.
class ImpalaPasswordedTlsSocketFactory : public ImpalaTlsSocketFactory {
 public:
  ImpalaPasswordedTlsSocketFactory(SSLProtocol version, const string& password)
    : ImpalaTlsSocketFactory(version), password_(password) {}

 protected:
  virtual void getPassword(string& output, int size) override {
    output = password_;
    if (output.size() > size) output.resize(size);
  }

 private:
  /// The password string.
  const string password_;
};
}
Status ThriftServer::CreateSocket(std::shared_ptr<TServerSocket>* socket) {
  if (ssl_enabled()) {
    if (!SSLProtoVersions::IsSupported(version_)) {
      return Status(TErrorCode::SSL_SOCKET_CREATION_FAILED,
          Substitute("TLS ($0) version not supported (maximum supported version is $1)",
                        version_, MaxSupportedTlsVersion()));
    }
    try {
      // This 'factory' is only called once, since CreateSocket() is only called from
      // Start(). The c'tor may throw if there is an error initializing the SSL context.
      std::shared_ptr<ImpalaTlsSocketFactory> socket_factory(
          new ImpalaPasswordedTlsSocketFactory(version_, key_password_));
      socket_factory->overrideDefaultPasswordCallback();

      socket_factory->configureCiphers(cipher_list_, tls_ciphersuites_,
          disable_tls12_);
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

Status ThriftServer::EnableSsl(SSLProtocol version, const string& certificate,
    const string& private_key, const string& pem_password_cmd,
    const std::string& cipher_list, const std::string& tls_ciphersuites,
    bool disable_tls12) {
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
  cipher_list_ = cipher_list;
  tls_ciphersuites_ = tls_ciphersuites;
  disable_tls12_ = disable_tls12;
  version_ = version;

  if (!pem_password_cmd.empty()) {
    if (!RunShellProcess(pem_password_cmd, &key_password_, true, {"JAVA_TOOL_OPTIONS"})) {
      return Status(TErrorCode::SSL_PASSWORD_CMD_FAILED, pem_password_cmd, key_password_);
    } else {
      LOG(INFO) << "Command '" << pem_password_cmd << "' executed successfully, "
                << ".PEM password retrieved";
    }
  }

  return Status::OK();
}

Status ThriftServer::Start() {
  DCHECK(!started_);
  std::shared_ptr<TProtocolFactory> protocol_factory(new TBinaryProtocolFactory());
  std::shared_ptr<ThreadFactory> thread_factory(
      new ThriftThreadFactory("thrift-server", name_));

  // Note - if you change the transport types here, you must check that the
  // logic in createContext is still accurate.
  std::shared_ptr<TServerSocket> server_socket;
  std::shared_ptr<TTransportFactory> transport_factory;
  RETURN_IF_ERROR(CreateSocket(&server_socket));
  RETURN_IF_ERROR(auth_provider_->GetServerTransportFactory(
      transport_type_, metrics_name_, metrics_, &transport_factory));

  server_.reset(new TAcceptQueueServer(processor_, server_socket, transport_factory,
      protocol_factory, thread_factory, name_, max_concurrent_connections_,
      queue_timeout_ms_, idle_poll_period_ms_, is_external_facing_));
  if (metrics_ != NULL) {
    (static_cast<TAcceptQueueServer*>(server_.get()))
        ->InitMetrics(metrics_, metrics_name_);
  }
  std::shared_ptr<ThriftServer::ThriftServerEventProcessor> event_processor(
      new ThriftServer::ThriftServerEventProcessor(this));
  server_->setServerEventHandler(event_processor);

  RETURN_IF_ERROR(event_processor->StartAndWaitForServer());

  // If port_ was 0, figure out which port the server is listening on after starting.
  port_ = server_socket->getPort();
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
  server_->stop();
  if (started_) Join();
}

void ThriftServer::GetConnectionContextList(ConnectionContextList* connection_contexts) {
  lock_guard<mutex> l(connection_contexts_lock_);
  for (const ConnectionContextSet::value_type& pair : connection_contexts_) {
    connection_contexts->push_back(pair.second);
  }
}
}
