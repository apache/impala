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
#include "util/thrift-client.h"

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
// TODO: More graceful handling of clients that have failed (maybe better
// handled by a smart-wrapper of the interface object).
// TODO: limits on total number of clients, and clients per-backend
class ClientCacheHelper {
 public:
  // Callback method which produces a client object when one cannot be
  // found in the cache. Supplied by the ClientCache wrapper.
  typedef boost::function<ThriftClientImpl* (const std::pair<std::string, int>& hostport,
                                             void** client_key)> ClientFactory;

  // Return client for specific host/port in 'client'. If a client
  // is not available, the client parameter is set to NULL.
  Status GetClient(const std::pair<std::string, int>& hostport,
      ClientFactory factory_method, ThriftClientImpl** client);

  // Reopens the underlying transport in case of error.
  Status ReopenClient(void* client_key);

  // Return a client to the cache, without closing it
  void ReleaseClient(void* client_key);

  // Close all connections to a host (e.g., in case of failure) so that on their
  // next use they will have to be Reopen'ed.
  void CloseConnections(const std::pair<std::string, int>& hostport);

  std::string DebugString();

  void TestShutdown();

  void InitMetrics(Metrics* metrics, const std::string& key_prefix);

 private:
  template <class T> friend class ClientCache;
  // Private constructor so that only ClientCache can instantiate this class.
  ClientCacheHelper() : metrics_enabled_(false) { }

  // Protects all member variables
  // TODO: have more fine-grained locks or use lock-free data structures,
  // this isn't going to scale for a high request rate
  boost::mutex lock_;

  // map from (host, port) to list of clients for that address
  typedef boost::unordered_map<
      std::pair<std::string, int>, std::list<ThriftClientImpl*> > ClientCacheMap;
  ClientCacheMap client_cache_;

  // Map from client key back to its associated ThriftClientImpl transport
  typedef boost::unordered_map<void*, ThriftClientImpl*> ClientMap;
  ClientMap client_map_;

  // Metrics
  bool metrics_enabled_;

  // Number of clients 'checked-out' from the cache
  Metrics::IntMetric* clients_in_use_metric_;

  // Total clients in the cache, including those in use
  Metrics::IntMetric* total_clients_metric_;
};

// Generic cache of Thrift clients for a given service type.
// This class is thread-safe.
template<class T>
class ClientCache {
 public:
  typedef ThriftClient<T> Client;

  ClientCache() : client_cache_helper_() {
    client_factory_ =
        boost::bind<ThriftClientImpl*>(mem_fn(&ClientCache::MakeClient), this, _1, _2);
  }

  // Obtains a pointer to a Thrift interface object (of type T),
  // backed by a live transport which is already open. Returns
  // Status::OK unless there was an error opening the transport.
  Status GetClient(const std::pair<std::string, int>& hostport, T** iface) {
    ThriftClientImpl* raw_client;
    RETURN_IF_ERROR(
        client_cache_helper_.GetClient(hostport, client_factory_, &raw_client));
    *iface = reinterpret_cast<Client*>(raw_client)->iface();

    return Status::OK;
  }

  // Closes and reopens the client's transport
  Status ReopenClient(T* client) {
    return client_cache_helper_.ReopenClient(reinterpret_cast<void*>(client));
  }

  // Return the client to the cache
  void ReleaseClient(T* client) {
    return client_cache_helper_.ReleaseClient(reinterpret_cast<void*>(client));
  }

  // Close all clients connected to the supplied address, (e.g., in
  // case of failure) so that on their next use they will have to be
  // Reopen'ed.
  void CloseConnections(const std::pair<std::string, int>& hostport) {
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
  // Most operations in this class are thin wrappers around the
  // equivalent in ClientCacheHelper, which is a non-templated cache
  // to avoid inlining lots of code wherever this cache is used.
  ClientCacheHelper client_cache_helper_;

  // Function pointer, bound to MakeClient, which produces clients when the cache is empty
  ClientCacheHelper::ClientFactory client_factory_;

  // Factory method to produce a new ThriftClient<T> for the wrapped cache
  ThriftClientImpl* MakeClient(const std::pair<std::string, int>& hostport,
      void** client_key) {
    Client* client = new Client(hostport.first, hostport.second);
    *client_key = reinterpret_cast<void*>(client->iface());
    return client;
  }

};

// Impala backend client cache, used by a backend to send requests
// to any other backend.
class ImpalaInternalServiceClient;
typedef ClientCache<ImpalaInternalServiceClient> ImpalaInternalServiceClientCache;

}

#endif
