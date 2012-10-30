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

#include "util/thrift-client.h"

#include "common/status.h"

namespace impala {

class ImpalaInternalServiceClient;

// Cache of Thrift clients for ImpalaInternalServices.
// This class is thread-safe.
// TODO: shut down clients in the background if they don't get used for a period of time
// TODO: in order to reduce locking overhead when getting/releasing clients,
// add call to hand back pointer to list stored in ClientCache and add separate lock
// to list (or change to lock-free list)
// TODO: More graceful handling of clients that have failed (maybe better
// handled by a wrapper of ImpalaInternalServiceClient).
class BackendClientCache {
 public:
  // Create cache with given upper limits for the total number of cached
  // clients and the total number of clients per single host/port.
  // 0 means no limit.
  // Limits are ignored for now.
  BackendClientCache(int max_clients, int max_clients_per_backend);

  // Return client for specific host/port in 'client'.
  Status GetClient(
      const std::pair<std::string, int>& hostport,
      ImpalaInternalServiceClient** client);

  // Reopens the underlying transport in case of error.
  Status ReopenClient(ImpalaInternalServiceClient* client);

  // Hand client back.
  void ReleaseClient(ImpalaInternalServiceClient* client);

  // Close all connections to a host (e.g., in case of failure) so that on their
  // next use they will have to be Reopen'ed.
  void CloseConnections(const std::pair<std::string, int>& hostport);

  std::string DebugString();

 private:
  int max_clients_;
  int max_clients_per_backend_;

  // protects all fields below
  // TODO: have more fine-grained locks or use lock-free data structures,
  // this isn't going to scale for a high request rate
  boost::mutex lock_;

  typedef ThriftClient<ImpalaInternalServiceClient> BackendClient;

  // map from (host, port) to list of clients;
  // we own BackendClient*
  typedef boost::unordered_map<std::pair<std::string, int>, std::list<BackendClient*> >
      ClientCache;
  ClientCache client_cache_;

  // map from client back to its containing struct
  typedef boost::unordered_map<ImpalaInternalServiceClient*, BackendClient*> ClientMap;
  ClientMap client_map_;
};

}

#endif
