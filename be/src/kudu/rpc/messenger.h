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
#ifndef KUDU_RPC_MESSENGER_H
#define KUDU_RPC_MESSENGER_H

#include <stdint.h>

#include <list>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <boost/optional.hpp>
#include <gtest/gtest_prod.h>

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/rpc/connection.h"
#include "kudu/rpc/response_callback.h"
#include "kudu/security/token.pb.h"
#include "kudu/util/locks.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/status.h"

namespace kudu {

class Socket;
class ThreadPool;

namespace security {
class TlsContext;
class TokenVerifier;
}

namespace rpc {

class AcceptorPool;
class DumpRunningRpcsRequestPB;
class DumpRunningRpcsResponsePB;
class InboundCall;
class Messenger;
class OutboundCall;
class Reactor;
class ReactorThread;
class RpcService;
class RpczStore;

struct AcceptorPoolInfo {
 public:
  explicit AcceptorPoolInfo(Sockaddr bind_address)
      : bind_address_(std::move(bind_address)) {}

  Sockaddr bind_address() const {
    return bind_address_;
  }

 private:
  Sockaddr bind_address_;
};

// Authentication configuration for RPC connections.
enum class RpcAuthentication {
  DISABLED,
  OPTIONAL,
  REQUIRED,
};

// Encryption configuration for RPC connections.
enum class RpcEncryption {
  DISABLED,
  OPTIONAL,
  REQUIRED,
};

// Used to construct a Messenger.
class MessengerBuilder {
 public:
  friend class Messenger;
  friend class ReactorThread;

  explicit MessengerBuilder(std::string name);

  // Set the length of time we will keep a TCP connection will alive with no traffic.
  MessengerBuilder &set_connection_keepalive_time(const MonoDelta &keepalive);

  // Set the number of reactor threads that will be used for sending and
  // receiving.
  MessengerBuilder &set_num_reactors(int num_reactors);

  // Set the minimum number of connection-negotiation threads that will be used
  // to handle the blocking connection-negotiation step.
  MessengerBuilder &set_min_negotiation_threads(int min_negotiation_threads);

  // Set the maximum number of connection-negotiation threads that will be used
  // to handle the blocking connection-negotiation step.
  MessengerBuilder &set_max_negotiation_threads(int max_negotiation_threads);

  // Set the granularity with which connections are checked for keepalive.
  MessengerBuilder &set_coarse_timer_granularity(const MonoDelta &granularity);

  // Set metric entity for use by RPC systems.
  MessengerBuilder &set_metric_entity(const scoped_refptr<MetricEntity>& metric_entity);

  // Set the SASL protocol name that is used for the SASL negotiation.
  MessengerBuilder &set_sasl_proto_name(std::string sasl_proto_name);

  // Configure the messenger to enable TLS encryption on inbound connections.
  MessengerBuilder& enable_inbound_tls();

  Status Build(std::shared_ptr<Messenger> *msgr);

 private:
  const std::string name_;
  MonoDelta connection_keepalive_time_;
  int num_reactors_;
  int min_negotiation_threads_;
  int max_negotiation_threads_;
  MonoDelta coarse_timer_granularity_;
  scoped_refptr<MetricEntity> metric_entity_;
  std::string sasl_proto_name_;
  bool enable_inbound_tls_;
};

// A Messenger is a container for the reactor threads which run event loops
// for the RPC services. If the process is a server, a Messenger can also have
// one or more attached AcceptorPools which accept RPC connections. In this case,
// calls received over the connection are enqueued into the messenger's service_queue
// for processing by a ServicePool.
//
// Users do not typically interact with the Messenger directly except to create
// one as a singleton, and then make calls using Proxy objects.
//
// See rpc-test.cc and rpc-bench.cc for example usages.
class Messenger {
 public:
  friend class MessengerBuilder;
  friend class Proxy;
  friend class Reactor;
  typedef std::vector<std::shared_ptr<AcceptorPool> > acceptor_vec_t;
  typedef std::unordered_map<std::string, scoped_refptr<RpcService> > RpcServicesMap;

  static const uint64_t UNKNOWN_CALL_ID = 0;

  ~Messenger();

  // Stop all communication and prevent further use.
  // It's not required to call this -- dropping the shared_ptr provided
  // from MessengerBuilder::Build will automatically call this method.
  void Shutdown();

  // Add a new acceptor pool listening to the given accept address.
  // You can create any number of acceptor pools you want, including none.
  //
  // The created pool is returned in *pool. The Messenger also retains
  // a reference to the pool, so the caller may safely drop this reference
  // and the pool will remain live.
  //
  // NOTE: the returned pool is not initially started. You must call
  // pool->Start(...) to begin accepting connections.
  //
  // If Kerberos is enabled, this also runs a pre-flight check that makes
  // sure the environment is appropriately configured to authenticate
  // clients via Kerberos. If not, this returns a RuntimeError.
  Status AddAcceptorPool(const Sockaddr &accept_addr,
                         std::shared_ptr<AcceptorPool>* pool);

  // Register a new RpcService to handle inbound requests.
  Status RegisterService(const std::string& service_name,
                         const scoped_refptr<RpcService>& service);

  // Unregister currently-registered RpcService.
  Status UnregisterService(const std::string& service_name);

  Status UnregisterAllServices();

  // Queue a call for transmission. This will pick the appropriate reactor,
  // and enqueue a task on that reactor to assign and send the call.
  void QueueOutboundCall(const std::shared_ptr<OutboundCall> &call);

  // Enqueue a call for processing on the server.
  void QueueInboundCall(gscoped_ptr<InboundCall> call);

  // Queue a cancellation for the given outbound call.
  void QueueCancellation(const std::shared_ptr<OutboundCall> &call);

  // Take ownership of the socket via Socket::Release
  void RegisterInboundSocket(Socket *new_socket, const Sockaddr &remote);

  // Dump the current RPCs into the given protobuf.
  Status DumpRunningRpcs(const DumpRunningRpcsRequestPB& req,
                         DumpRunningRpcsResponsePB* resp);

  // Run 'func' on a reactor thread after 'when' time elapses.
  //
  // The status argument conveys whether 'func' was run correctly (i.e.
  // after the elapsed time) or not.
  void ScheduleOnReactor(const boost::function<void(const Status&)>& func,
                         MonoDelta when);

  const security::TlsContext& tls_context() const { return *tls_context_; }
  security::TlsContext* mutable_tls_context() { return tls_context_.get(); }

  const security::TokenVerifier& token_verifier() const { return *token_verifier_; }
  security::TokenVerifier* mutable_token_verifier() { return token_verifier_.get(); }
  std::shared_ptr<security::TokenVerifier> shared_token_verifier() const {
    return token_verifier_;
  }

  boost::optional<security::SignedTokenPB> authn_token() const {
    std::lock_guard<simple_spinlock> l(authn_token_lock_);
    return authn_token_;
  }
  void set_authn_token(const security::SignedTokenPB& token) {
    std::lock_guard<simple_spinlock> l(authn_token_lock_);
    authn_token_ = token;
  }

  RpcAuthentication authentication() const { return authentication_; }
  RpcEncryption encryption() const { return encryption_; }

  ThreadPool* negotiation_pool(Connection::Direction dir);

  RpczStore* rpcz_store() { return rpcz_store_.get(); }

  int num_reactors() const { return reactors_.size(); }

  const std::string& name() const {
    return name_;
  }

  bool closing() const {
    shared_lock<rw_spinlock> l(lock_.get_lock());
    return closing_;
  }

  scoped_refptr<MetricEntity> metric_entity() const { return metric_entity_.get(); }

  const std::string& sasl_proto_name() const {
    return sasl_proto_name_;
  }

  const scoped_refptr<RpcService> rpc_service(const std::string& service_name) const;

 private:
  FRIEND_TEST(TestRpc, TestConnectionKeepalive);
  FRIEND_TEST(TestRpc, TestCredentialsPolicy);
  FRIEND_TEST(TestRpc, TestReopenOutboundConnections);

  explicit Messenger(const MessengerBuilder &bld);

  Reactor* RemoteToReactor(const Sockaddr &remote);
  Status Init();
  void RunTimeoutThread();
  void UpdateCurTime();

  // Called by external-facing shared_ptr when the user no longer holds
  // any references. See 'retain_self_' for more info.
  void AllExternalReferencesDropped();

  const std::string name_;

  // Protects closing_, acceptor_pools_, rpc_services_.
  mutable percpu_rwlock lock_;

  bool closing_;

  // Whether to require authentication and encryption on the connections managed
  // by this messenger.
  // TODO(KUDU-1928): scope these to individual proxies, so that messengers can be
  // reused by different clients.
  RpcAuthentication authentication_;
  RpcEncryption encryption_;

  // Pools which are listening on behalf of this messenger.
  // Note that the user may have called Shutdown() on one of these
  // pools, so even though we retain the reference, it may no longer
  // be listening.
  acceptor_vec_t acceptor_pools_;

  // RPC services that handle inbound requests.
  RpcServicesMap rpc_services_;

  std::vector<Reactor*> reactors_;

  // Separate client and server negotiation pools to avoid possibility of distributed
  // deadlock. See KUDU-2041.
  gscoped_ptr<ThreadPool> client_negotiation_pool_;
  gscoped_ptr<ThreadPool> server_negotiation_pool_;

  std::unique_ptr<security::TlsContext> tls_context_;

  // A TokenVerifier, which can verify client provided authentication tokens.
  std::shared_ptr<security::TokenVerifier> token_verifier_;

  // An optional token, which can be used to authenticate to a server.
  mutable simple_spinlock authn_token_lock_;
  boost::optional<security::SignedTokenPB> authn_token_;

  std::unique_ptr<RpczStore> rpcz_store_;

  scoped_refptr<MetricEntity> metric_entity_;

  // The SASL protocol name that is used for the SASL negotiation.
  const std::string sasl_proto_name_;

  // The ownership of the Messenger object is somewhat subtle. The pointer graph
  // looks like this:
  //
  //    [User Code ]             |      [ Internal code ]
  //                             |
  //     shared_ptr[1]           |
  //         |                   |
  //         v
  //      Messenger    <------------ shared_ptr[2] --- Reactor
  //       ^    |       ----------- bare pointer --> Reactor
  //        \__/
  //     shared_ptr[2]
  //     (retain_self_)
  //
  // shared_ptr[1] instances use Messenger::AllExternalReferencesDropped()
  //   as a deleter.
  // shared_ptr[2] are "traditional" shared_ptrs which call 'delete' on the
  //   object.
  //
  // The teardown sequence is as follows:
  // Option 1): User calls "Shutdown()" explicitly:
  //  - Messenger::Shutdown tells Reactors to shut down
  //  - When each reactor thread finishes, it drops its shared_ptr[2]
  //  - the Messenger::retain_self instance remains, keeping the Messenger
  //    alive.
  //  - The user eventually drops its shared_ptr[1], which calls
  //    Messenger::AllExternalReferencesDropped. This drops retain_self_
  //    and results in object destruction.
  // Option 2): User drops all of its shared_ptr[1] references
  //  - Though the Reactors still reference the Messenger, AllExternalReferencesDropped
  //    will get called, which triggers Messenger::Shutdown.
  //  - AllExternalReferencesDropped drops retain_self_, so the only remaining
  //    references are from Reactor threads. But the reactor threads are shutting down.
  //  - When the last Reactor thread dies, there will be no more shared_ptr[1] references
  //    and the Messenger will be destroyed.
  //
  // The main goal of all of this confusion is that the reactor threads need to be able
  // to shut down asynchronously, and we need to keep the Messenger alive until they
  // do so. So, handing out a normal shared_ptr to users would force the Messenger
  // destructor to Join() the reactor threads, which causes a problem if the user
  // tries to destruct the Messenger from within a Reactor thread itself.
  std::shared_ptr<Messenger> retain_self_;

  DISALLOW_COPY_AND_ASSIGN(Messenger);
};

} // namespace rpc
} // namespace kudu

#endif
