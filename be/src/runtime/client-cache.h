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


#ifndef IMPALA_RUNTIME_CLIENT_CACHE_H
#define IMPALA_RUNTIME_CLIENT_CACHE_H

#include <vector>
#include <list>
#include <string>
#include <boost/unordered_map.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/bind.hpp>

#include "util/metrics.h"
#include "rpc/thrift-client.h"

#include "common/status.h"

namespace impala {

// Helper class which implements the majority of the caching
// functionality without using templates (i.e. pointers to the
// superclass of all ThriftClients and a void* for the key).
//
// The user of this class only sees RPC proxy classes, but we have
// to track the ThriftClient to manipulate the underlying
// transport. To do this, we maintain a map from an opaque 'key'
// pointer type to the client implementation. We actually know the
// type of the pointer (it's the type parameter to ClientCache), but
// we deliberately avoid using it so that this entire class doesn't
// get inlined every time it gets used.
//
// This class is thread-safe.
//
// TODO: shut down clients in the background if they don't get used for a period of time
// TODO: in order to reduce locking overhead when getting/releasing clients,
// add call to hand back pointer to list stored in ClientCache and add separate lock
// to list (or change to lock-free list)
// TODO: reduce locking overhead and by adding per-address client caches, each with its
// own lock.
// TODO: More graceful handling of clients that have failed (maybe better
// handled by a smart-wrapper of the interface object).
// TODO: limits on total number of clients, and clients per-backend
class ClientCacheHelper {
 public:
  // Callback method which produces a client object when one cannot be
  // found in the cache. Supplied by the ClientCache wrapper.
  typedef boost::function<ThriftClientImpl* (const TNetworkAddress& hostport,
                                             void** client_key)> ClientFactory;

  // Return client for specific host/port in 'client'. If a client
  // is not available, the client parameter is set to NULL.
  Status GetClient(const TNetworkAddress& hostport,
      ClientFactory factory_method, void** client_key);

  // Close and delete the underlying transport and remove the client from client_map_.
  // Return a new client connecting to the same host/port.
  // Return an error status and set client_key to NULL if a new client cannot
  // created.
  Status ReopenClient(ClientFactory factory_method, void** client_key);

  // Return a client to the cache, without closing it, and set *client_key to NULL.
  void ReleaseClient(void** client_key);

  // Close all connections to a host (e.g., in case of failure) so that on their
  // next use they will have to be Reopen'ed.
  void CloseConnections(const TNetworkAddress& address);

  std::string DebugString();

  void TestShutdown();

  void InitMetrics(Metrics* metrics, const std::string& key_prefix);

 private:
  template <class T> friend class ClientCache;
  // Private constructor so that only ClientCache can instantiate this class.
  ClientCacheHelper(uint32_t num_tries, uint64_t wait_ms, int32_t send_timeout_ms,
      int32_t recv_timeout_ms)
      : num_tries_(num_tries),
        wait_ms_(wait_ms),
        send_timeout_ms_(send_timeout_ms),
        recv_timeout_ms_(recv_timeout_ms),
        metrics_enabled_(false) { }

  // Protects all member variables
  // TODO: have more fine-grained locks or use lock-free data structures,
  // this isn't going to scale for a high request rate
  boost::mutex lock_;

  // map from (host, port) to list of client keys for that address
  typedef boost::unordered_map<
      TNetworkAddress, std::list<void*> > ClientCacheMap;
  ClientCacheMap client_cache_;

  // Map from client key back to its associated ThriftClientImpl transport
  typedef boost::unordered_map<void*, ThriftClientImpl*> ClientMap;
  ClientMap client_map_;

  // Number of attempts to make to open a connection. 0 means retry indefinitely.
  uint32_t num_tries_;

  // Time to wait between failed connection attempts.
  uint64_t wait_ms_;

  // Time to wait for the underlying socket to send data, e.g., for an RPC.
  int32_t send_timeout_ms_;

  // Time to wait for the underlying socket to receive data, e.g., for an RPC response.
  int32_t recv_timeout_ms_;

  // Metrics
  bool metrics_enabled_;

  // Number of clients 'checked-out' from the cache
  Metrics::IntMetric* clients_in_use_metric_;

  // Total clients in the cache, including those in use
  Metrics::IntMetric* total_clients_metric_;

  // Create a new client for specific host/port in 'client' and put it in client_map_
  Status CreateClient(const TNetworkAddress& hostport, ClientFactory factory_method,
      void** client_key);
};

template<class T>
class ClientCache;

// A scoped client connection to help manage clients from a client cache.
//
// Example:
//   {
//     ImpalaInternalServiceConnection client(cache, address, &status);
//     try {
//       client->TransmitData(...);
//     } catch (TTransportException& e) {
//       // Retry
//       RETURN_IF_ERROR(client.Reopen());
//       client->TransmitData(...);
//     }
//   }
// ('client' is released back to cache upon destruction.)
template<class T>
class ClientConnection {
 public:
  ClientConnection(ClientCache<T>* client_cache, TNetworkAddress address, Status* status)
    : client_cache_(client_cache),
      client_(NULL) {
    *status = client_cache_->GetClient(address, &client_);
    if (status->ok()) DCHECK(client_ != NULL);
  }

  ~ClientConnection() {
    if (client_ != NULL) {
      client_cache_->ReleaseClient(&client_);
    }
  }

  Status Reopen() {
    return client_cache_->ReopenClient(&client_);
  }

  T* operator->() const {
    return client_;
  }

 private:
  ClientCache<T>* client_cache_;
  T* client_;
};

// Generic cache of Thrift clients for a given service type.
// This class is thread-safe.
template<class T>
class ClientCache {
 public:
  typedef ThriftClient<T> Client;

  ClientCache() : client_cache_helper_(1, 0, 0, 0) {
    client_factory_ =
        boost::bind<ThriftClientImpl*>(
            boost::mem_fn(&ClientCache::MakeClient), this, _1, _2);
  }

  // Create a ClientCache where connections are tried num_tries times, with a pause of
  // wait_ms between attempts. The underlying TSocket's send and receive timeouts of
  // each connection can also be set. If num_tries == 0, retry connections indefinitely.
  // A send/receive timeout of 0 means there is no timeout.
  ClientCache(uint32_t num_tries, uint64_t wait_ms, int32_t send_timeout_ms = 0,
      int32_t recv_timeout_ms = 0)
      : client_cache_helper_(num_tries, wait_ms, send_timeout_ms, recv_timeout_ms) {
    client_factory_ =
        boost::bind<ThriftClientImpl*>(
            boost::mem_fn(&ClientCache::MakeClient), this, _1, _2);
  }

  // Close all clients connected to the supplied address, (e.g., in
  // case of failure) so that on their next use they will have to be
  // Reopen'ed.
  void CloseConnections(const TNetworkAddress& hostport) {
    return client_cache_helper_.CloseConnections(hostport);
  }

  // Helper method which returns a debug string
  std::string DebugString() {
    return client_cache_helper_.DebugString();
  }

  // For testing only: shutdown all clients
  void TestShutdown() {
    return client_cache_helper_.TestShutdown();
  }

  // Adds metrics for this cache to the supplied Metrics instance. The
  // metrics have keys that are prefixed by the key_prefix argument
  // (which should not end in a period).
  // Must be called before the cache is used, otherwise the metrics might be wrong
  void InitMetrics(Metrics* metrics, const std::string& key_prefix) {
    client_cache_helper_.InitMetrics(metrics, key_prefix);
  }

 private:
  friend class ClientConnection<T>;

  // Most operations in this class are thin wrappers around the
  // equivalent in ClientCacheHelper, which is a non-templated cache
  // to avoid inlining lots of code wherever this cache is used.
  ClientCacheHelper client_cache_helper_;

  // Function pointer, bound to MakeClient, which produces clients when the cache is empty
  ClientCacheHelper::ClientFactory client_factory_;

  // Obtains a pointer to a Thrift interface object (of type T),
  // backed by a live transport which is already open. Returns
  // Status::OK unless there was an error opening the transport.
  Status GetClient(const TNetworkAddress& hostport, T** iface) {
    return client_cache_helper_.GetClient(hostport, client_factory_,
        reinterpret_cast<void**>(iface));
  }

  // Close and delete the underlying transport. Return a new client connecting to the
  // same host/port.
  // Return an error status if a new connection cannot be established and *client will be
  // NULL in that case.
  Status ReopenClient(T** client) {
    return client_cache_helper_.ReopenClient(client_factory_,
        reinterpret_cast<void**>(client));
  }

  // Return the client to the cache and set *client to NULL.
  void ReleaseClient(T** client) {
    return client_cache_helper_.ReleaseClient(reinterpret_cast<void**>(client));
  }

  // Factory method to produce a new ThriftClient<T> for the wrapped cache
  ThriftClientImpl* MakeClient(const TNetworkAddress& hostport, void** client_key) {
    Client* client = new Client(hostport.hostname, hostport.port);
    *client_key = reinterpret_cast<void*>(client->iface());
    return client;
  }

};

// Impala backend client cache, used by a backend to send requests
// to any other backend.
class ImpalaInternalServiceClient;
typedef ClientCache<ImpalaInternalServiceClient> ImpalaInternalServiceClientCache;
typedef ClientConnection<ImpalaInternalServiceClient> ImpalaInternalServiceConnection;

}

#endif
