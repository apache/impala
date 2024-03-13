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

#pragma once

#include <cstdint>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <typeinfo>

#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <boost/unordered_map.hpp>
#include <thrift/Thrift.h>
#include <thrift/transport/TTransportException.h>

#include "common/logging.h"
#include "common/status.h"
#include "gen-cpp/ErrorCodes_types.h"
#include "gen-cpp/Types_types.h"
#include "gutil/strings/substitute.h"
#include "rpc/thrift-client.h"
#include "rpc/thrift-util.h"
#include "util/container-util.h"
#include "util/metrics-fwd.h"
#include "util/network-util.h"
#include "util/time.h"

namespace impala {

class MetricGroup;
template <class T> class ClientCache;

/// Opaque pointer type which allows users of ClientCache to refer to particular client
/// instances without requiring that we parameterise ClientCacheHelper by type.
typedef void* ClientKey;

/// Helper class which implements the majority of the caching functionality without using
/// templates (i.e. pointers to the superclass of all ThriftClients and a void* for the
/// key). This class is for internal use only; the public interface is in ClientCache
/// below.
//
/// A client is either 'in-use' (the user of the cache is between GetClient() and
/// ReleaseClient() pairs) or 'cached', in which case it is available for the next
/// GetClient() call. Internally, this class maintains a map of all clients, in use or not,
/// which is indexed by their ClientKey (see below), and a map from server address to a
/// list of the keys of all clients that are not currently in use.
//
/// The user of this class only sees RPC proxy classes, but we have to track the
/// ThriftClient to manipulate the underlying transport. To do this, we use an opaque
/// ClientKey pointer type to act as the key for a particular client. We actually know the
/// type of the value at the end of pointer (it's the type parameter to ClientCache), but
/// we deliberately avoid using it so that we don't have to parameterise this class by
/// type, and thus this entire class doesn't get inlined every time it gets used.
//
/// This class is thread-safe.
//
/// TODO: shut down clients in the background if they don't get used for a period of time
/// TODO: More graceful handling of clients that have failed (maybe better
/// handled by a smart-wrapper of the interface object).
/// TODO: limits on total number of clients, and clients per-backend
/// TODO: move this to a separate header file, so that the public interface is more
/// prominent in this file
class ClientCacheHelper {
 public:
  /// Callback method which produces a client object when one cannot be found in the
  /// cache. Supplied by the ClientCache wrapper.
  typedef boost::function<ThriftClientImpl* (const TNetworkAddress& address,
                                             ClientKey* client_key)> ClientFactory;

  /// Returns a client for the given address in 'client_key'. If a previously created
  /// client is not available (i.e. there are no entries in the per-host cache), a new
  /// client is created by calling the supplied 'factory_method'. As a postcondition, the
  /// returned client will not be present in the per-host cache.
  //
  /// If there is an error creating the new client, *client_key will be NULL.
  Status GetClient(const TNetworkAddress& address, ClientFactory factory_method,
      ClientKey* client_key) WARN_UNUSED_RESULT;

  /// Returns a newly-opened client in client_key. May reopen the existing client, or may
  /// replace it with a new one (created using 'factory_method').
  //
  /// Returns an error status and sets 'client_key' to NULL if a new client cannot
  /// created.
  Status ReopenClient(
      ClientFactory factory_method, ClientKey* client_key) WARN_UNUSED_RESULT;

  /// Returns a client to the cache. Upon return, *client_key will be NULL, and the
  /// associated client will be available in the per-host cache.
  void ReleaseClient(ClientKey* client_key);

  /// Close all connections to a host (e.g., in case of failure) so that on their
  /// next use they will have to be reopened via ReopenClient().
  void CloseConnections(const TNetworkAddress& address);

  /// Close the client connection and don't put client back to per-host cache.
  /// Also remove client from client_map_.
  void DestroyClient(ClientKey* client_key);

  /// Return a debug representation of the contents of this cache.
  std::string DebugString();

  /// Closes every connection in the cache. Used only for testing.
  void TestShutdown();

  /// Creates two metrics for this cache measuring the number of clients currently used,
  /// and the total number in the cache.
  void InitMetrics(MetricGroup* metrics, const std::string& key_prefix,
      const std::string& key_appendix);

 private:
  template <class T> friend class ClientCache;
  /// Private constructor so that only ClientCache can instantiate this class.
  ClientCacheHelper(uint32_t num_tries, uint64_t wait_ms, int32_t send_timeout_ms,
      int32_t recv_timeout_ms)
      : num_tries_(num_tries),
        wait_ms_(wait_ms),
        send_timeout_ms_(send_timeout_ms),
        recv_timeout_ms_(recv_timeout_ms),
        metrics_enabled_(false) { }

  /// There are three lock categories - the cache-wide lock (cache_lock_), the locks for a
  /// specific cache (PerHostCache::lock) and the lock for the set of all clients
  /// (client_map_lock_). They do not have to be taken concurrently (and should not be, in
  /// general), but if they are they must be taken in this order:
  /// cache_lock_->PerHostCache::lock->client_map_lock_.

  /// A PerHostCache is a list of available client keys for a single host, plus a lock that
  /// protects that list. Only one PerHostCache will ever be created for a given host, so
  /// when a PerHostCache is retrieved from the PerHostCacheMap containing it there is no
  /// need to hold on to the container's lock.
  //
  /// Only clients that are not currently in use are tracked in their host's
  /// PerHostCache. When a client is returned to the cache via ReleaseClient(), it is
  /// reinserted into its corresponding PerHostCache list. Clients returned by GetClient()
  /// are considered to be immediately in use, and so don't exist in their PerHostCache
  /// until they are released for the first time.
  struct PerHostCache {
    /// Protects clients.
    std::mutex lock;

    /// List of client keys for this entry's host.
    std::list<ClientKey> clients;
  };

  /// Protects per_host_caches_
  std::mutex cache_lock_;

  /// Map from an address to a PerHostCache containing a list of keys that have entries in
  /// client_map_ for that host. The value type is wrapped in a shared_ptr so that the
  /// copy c'tor for PerHostCache is not required.
  typedef boost::unordered_map<
      TNetworkAddress, std::shared_ptr<PerHostCache>> PerHostCacheMap;
  PerHostCacheMap per_host_caches_;

  /// Protects client_map_.
  std::mutex client_map_lock_;

  /// Map from client key back to its associated ThriftClientImpl transport. This is where
  /// all the clients are actually stored, and client instances are owned by this class
  /// and persist for exactly as long as they are present in this map.
  /// We use a map (vs. unordered_map) so we get iterator consistency across operations.
  typedef std::map<ClientKey, std::shared_ptr<ThriftClientImpl>> ClientMap;
  ClientMap client_map_;

  /// Number of attempts to make to open a connection. 0 means retry indefinitely.
  const uint32_t num_tries_;

  /// Time to wait between failed connection attempts.
  const uint64_t wait_ms_;

  /// Time to wait for the underlying socket to send data, e.g., for an RPC.
  const int32_t send_timeout_ms_;

  /// Time to wait for the underlying socket to receive data, e.g., for an RPC response.
  const int32_t recv_timeout_ms_;

  /// True if metrics have been registered (i.e. InitMetrics() was called)), and *_metric_
  /// are valid pointers.
  bool metrics_enabled_;

  /// Number of clients 'checked-out' from the cache
  IntGauge* clients_in_use_metric_;

  /// Total clients in the cache, including those in use
  IntGauge* total_clients_metric_;

  /// Create a new client for specific address in 'client' and put it in client_map_
  Status CreateClient(const TNetworkAddress& address, ClientFactory factory_method,
      ClientKey* client_key) WARN_UNUSED_RESULT;
};

/// A scoped client connection to help manage clients from a client cache. Clients of this
/// class should use DoRpc() to actually make RPC calls.
template<class T>
class ClientConnection {
 public:
  ClientConnection(ClientCache<T>* client_cache, TNetworkAddress address, Status* status)
    : client_cache_(client_cache), client_(NULL), address_(address),
      client_is_unrecoverable_(false) {
    // TODO: Inject fault here to exercise IMPALA-5576.
    *status = client_cache_->GetClient(address, &client_);
    if (status->ok()) DCHECK(client_ != NULL);
  }

  ~ClientConnection() {
    if (client_ != NULL) {
      if (client_is_unrecoverable_) {
        client_cache_->DestroyClient(&client_);
      } else {
        client_cache_->ReleaseClient(&client_);
      }
    }
  }

  Status Reopen() WARN_UNUSED_RESULT { return client_cache_->ReopenClient(&client_); }

  T* operator->() const { return client_; }

  /// Perform an RPC call f(request, response), with some failure handling in case the
  /// TCP connection underpinning this client has been closed unexpectedly. Note that
  /// this can lead to f() being called twice, as this method may retry f() once,
  /// depending on the error received from the first attempt.
  ///
  /// Returns RPC_RECV_TIMEOUT if a timeout occurred while waiting for a response,
  /// RPC_CLIENT_CONNECT_FAILURE if the client failed to connect, and RPC_GENERAL_ERROR
  /// if the RPC could not be completed for any other reason (except for an unexpectedly
  /// closed cnxn).
  /// Application-level failures should be signalled through the response type.
  template <class F, class Request, class Response>
  Status DoRpc(const F& f, const Request& request, Response* response) {
    DCHECK(response != nullptr);
    client_is_unrecoverable_ = true;
    bool send_done = false;
    try {
      (client_->*f)(*response, request, &send_done);
    } catch (const apache::thrift::transport::TTransportException& e) {
      if (send_done && IsReadTimeoutTException(e)) {
        return RecvTimeoutStatus(typeid(*response).name());
      }

      // Client may have unexpectedly been closed, so re-open and retry once if we didn't
      // successfully send the payload yet or if the exception indicates the connection
      // was closed on the other end. Note that TCP can have a half-open connection so
      // send() may still succeed even after the other end already closed the socket.
      // The payload can just be buffered in the kernel.
      if (!send_done || IsConnResetTException(e)) {
        return RetryRpc(f, request, response);
      }

      // Payload was sent and failure wasn't a timeout waiting for response. Fail the RPC.
      return GeneralErrorStatus(e, typeid(*response).name(), send_done);
    } catch (const apache::thrift::TException& e) {
      return GeneralErrorStatus(e, typeid(*response).name(), send_done);
    }
    client_is_unrecoverable_ = false;
    return Status::OK();
  }

  /// Return struct for DoRpcWithRetry() that allows callers to distinguish between
  /// failures in getting a client and failures sending the RPC.
  struct RpcStatus {
    Status status;

    // Set to true if 'status' is not OK and the error occurred while getting the client.
    bool client_error;

    static RpcStatus OK() { return {Status::OK(), false}; }
  };

  /// Helper that retries constructing a client and calling DoRpc() up to 'retries' times
  /// with 'delay_ms' delay between retries. This handles both RPC failures and failures
  /// to get a client from 'client_cache'.  'debug_fn' is a Status-returning function that
  /// can be used to inject errors into the RPC.
  template <class F, class DebugF, class Request, class Response>
  static RpcStatus DoRpcWithRetry(ClientCache<T>* client_cache, TNetworkAddress address,
      const F& f, const Request& request, int retries, int64_t delay_ms,
      const DebugF& debug_fn, Response* response) {
    Status rpc_status;
    Status client_status;

    // Try to send the RPC as many times as requested before failing.
    for (int i = 0; i < retries; ++i) {
      if (i > 0) SleepForMs(delay_ms); // Delay before retrying.
      ClientConnection<T> client(client_cache, address, &client_status);
      if (!client_status.ok()) continue;

      rpc_status = debug_fn();
      if (!rpc_status.ok()) {
        LOG(INFO) << "Injected RPC error to " << TNetworkAddressToString(address) << ": "
                  << rpc_status.GetDetail();
        continue;
      }

      rpc_status = client.DoRpc(f, request, response);
      if (rpc_status.ok()) break;
      LOG(INFO) << "RPC to " << TNetworkAddressToString(address) << " failed "
                << rpc_status.GetDetail();
    }
    if (!client_status.ok()) return {client_status, true};
    if (!rpc_status.ok()) return {rpc_status, false};
    return RpcStatus::OK();
  }

  /// In certain cases, the server may take longer to provide an RPC response than
  /// the configured socket timeout. Callers may wish to retry receiving the response.
  /// This is safe if and only if DoRpc() returned RPC_RECV_TIMEOUT.
  template <class F, class Response>
  Status RetryRpcRecv(const F& recv_func, Response* response) {
    DCHECK(response != NULL);
    DCHECK(client_is_unrecoverable_);
    try {
      (client_->*recv_func)(*response);
    } catch (const apache::thrift::transport::TTransportException& e) {
      if (IsReadTimeoutTException(e)) {
        return RecvTimeoutStatus(typeid(*response).name());
      }
      // If it's not timeout exception, then the connection is broken, stop retrying.
      return GeneralErrorStatus(e, typeid(*response).name(), true);
    } catch (const apache::thrift::TException& e) {
      return GeneralErrorStatus(e, typeid(*response).name(), true);
    }
    client_is_unrecoverable_ = false;
    return Status::OK();
  }

 private:
  ClientCache<T>* client_cache_;
  T* client_;
  TNetworkAddress address_;

  /// Indicate the last rpc call sent by this connection succeeds or not. If the rpc call
  /// fails for any reason, the connection could be left in a bad state and cannot be
  /// recovered.
  bool client_is_unrecoverable_;

  /// Returns an error Status for general RPC errors not due to recv timeout or
  /// stale connections. Will also log some details about the error.
  Status GeneralErrorStatus(
      const apache::thrift::TException& e, const std::string& rpc_name, bool send_done) {
    ErrorMsg msg(TErrorCode::RPC_GENERAL_ERROR, strings::Substitute("Client for $0 hit "
        "an unexpected exception: $1, type: $2, rpc: $3, send: $4",
        TNetworkAddressToString(address_), e.what(), typeid(e).name(), rpc_name,
        send_done ? "done" : "not done"));
    VLOG_QUERY << msg.msg();
    return Status::Expected(msg);
  }

  /// Returns an error Status for RPC errors due to recv timeout.
  /// Will also log some details about the error.
  Status RecvTimeoutStatus(const std::string& rpc_name) {
    ErrorMsg msg(TErrorCode::RPC_RECV_TIMEOUT, TNetworkAddressToString(address_),
        rpc_name);
    VLOG_QUERY << msg.msg();
    return Status::Expected(msg);
  }

  /// Retry the RPC if TCP connection underpinning this client has been closed
  /// unexpectedly. Called only when we didn't succeed in sending all the payload
  /// in the first invocation of RPC call. Returns RPC_CLIENT_CONNECT_FAILURE
  /// on connection failure or RPC_GENERAL_ERROR for all other RPC failures.
  template <class F, class Request, class Response>
  Status RetryRpc(const F& f, const Request& request, Response* response) {
    DCHECK(client_is_unrecoverable_);
    // Client may have unexpectedly been closed, so re-open and retry.
    // TODO: ThriftClient should return proper error codes.
    Status status = Reopen();
    if (!status.ok()) {
      ErrorMsg msg(TErrorCode::RPC_CLIENT_CONNECT_FAILURE, status.GetDetail());
      VLOG_QUERY << msg.msg() << " rpc: " << typeid(*response).name();
      return Status::Expected(msg);
    }
    bool send_done = false;
    try {
      (client_->*f)(*response, request, &send_done);
    } catch (const apache::thrift::TException& e) {
      // By this point the RPC really has failed.
      // TODO: Revisit this logic later. It's possible that the new connection
      // works but we hit timeout here.
      return GeneralErrorStatus(e, typeid(*response).name(), send_done);
    }
    client_is_unrecoverable_ = false;
    return Status::OK();
  }
};

/// Generic cache of Thrift clients for a given service type.
/// This class is thread-safe.
template<class T>
class ClientCache {
 public:
  typedef ThriftClient<T> Client;

  ClientCache(const std::string& service_name = "", bool enable_ssl = false)
      : client_cache_helper_(1, 0, 0, 0) {
    client_factory_ = boost::bind<ThriftClientImpl*>(
        boost::mem_fn(&ClientCache::MakeClient), this, _1, _2, service_name, enable_ssl);
  }

  /// Create a ClientCache where connections are tried num_tries times, with a pause of
  /// wait_ms between attempts. The underlying TSocket's send and receive timeouts of
  /// each connection can also be set. If num_tries == 0, retry connections indefinitely.
  /// A send/receive timeout of 0 means there is no timeout.
  ClientCache(uint32_t num_tries, uint64_t wait_ms, int32_t send_timeout_ms = 0,
      int32_t recv_timeout_ms = 0, const std::string& service_name = "",
      bool enable_ssl = false)
      : client_cache_helper_(num_tries, wait_ms, send_timeout_ms, recv_timeout_ms) {
    client_factory_ = boost::bind<ThriftClientImpl*>(
        boost::mem_fn(&ClientCache::MakeClient), this, _1, _2, service_name, enable_ssl);
  }

  /// Close all clients connected to the supplied address, (e.g., in
  /// case of failure) so that on their next use they will have to be
  /// Reopen'ed.
  void CloseConnections(const TNetworkAddress& address) {
    return client_cache_helper_.CloseConnections(address);
  }

  /// Helper method which returns a debug string
  std::string DebugString() {
    return client_cache_helper_.DebugString();
  }

  /// For testing only: shutdown all clients
  void TestShutdown() {
    return client_cache_helper_.TestShutdown();
  }

  /// Adds metrics for this cache to the supplied Metrics instance. The
  /// metrics have keys that are prefixed by the key_prefix argument
  /// (which should not end in a period).
  /// Must be called before the cache is used, otherwise the metrics might be wrong
  void InitMetrics(MetricGroup* metrics, const std::string& key_prefix,
      const std::string& key_appendix = "") {
    client_cache_helper_.InitMetrics(metrics, key_prefix, key_appendix);
  }

 private:
  friend class ClientConnection<T>;
  friend class ClientCacheTest;

  /// Most operations in this class are thin wrappers around the
  /// equivalent in ClientCacheHelper, which is a non-templated cache
  /// to avoid inlining lots of code wherever this cache is used.
  ClientCacheHelper client_cache_helper_;

  /// Function pointer, bound to MakeClient, which produces clients when the cache is empty
  ClientCacheHelper::ClientFactory client_factory_;

  /// Obtains a pointer to a Thrift interface object (of type T),
  /// backed by a live transport which is already open. Returns
  /// Status::OK unless there was an error opening the transport.
  Status GetClient(const TNetworkAddress& address, T** iface) WARN_UNUSED_RESULT {
    return client_cache_helper_.GetClient(
        address, client_factory_, reinterpret_cast<ClientKey*>(iface));
  }

  /// Close and delete the underlying transport. Return a new client connecting to the
  /// same host/port.
  /// Returns an error status if a new connection cannot be established and *client will
  /// be unaffected in that case.
  Status ReopenClient(T** client) WARN_UNUSED_RESULT {
    return client_cache_helper_.ReopenClient(
        client_factory_, reinterpret_cast<ClientKey*>(client));
  }

  /// Return the client to the cache and set *client to NULL.
  void ReleaseClient(T** client) {
    return client_cache_helper_.ReleaseClient(reinterpret_cast<ClientKey*>(client));
  }

  /// Destroy the client because it's left in an unrecoverable state after errors
  /// in DoRpc() to avoid more rpc failure.
  void DestroyClient(T** client) {
    return client_cache_helper_.DestroyClient(reinterpret_cast<ClientKey*>(client));
  }

  /// Factory method to produce a new ThriftClient<T> for the wrapped cache
  ThriftClientImpl* MakeClient(const TNetworkAddress& address, ClientKey* client_key,
      const std::string service_name, bool enable_ssl) {
    Client* client = new Client(address.hostname, address.port, service_name, NULL,
        enable_ssl);
    *client_key = reinterpret_cast<ClientKey>(client->iface());
    return client;
  }

};

}
