// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_RUNTIME_CLIENT_CACHE_H
#define IMPALA_RUNTIME_CLIENT_CACHE_H

#include <vector>
#include <list>
#include <string>
#include <boost/unordered_map.hpp>

#include "common/status.h"

namespace impala {

class ImpalaBackendServiceClient;

// Cache of Thrift clients for ImpalaBackendServices.
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
      ImpalaBackendServiceClient** client);

  // Hand client back.
  void ReleaseClient(ImpalaBackendServiceClient* client);

  std::string DebugString();

 private:
  int max_clients_;
  int max_clients_per_backend_;

  struct ClientInfo;
  // map from (host, port) to list of clients;
  // we own ClientInfo*
  typedef boost::unordered_map<std::pair<std::string, int>, std::list<ClientInfo*> >
      ClientCache;
  ClientCache client_cache_;

  // map from client back to its containing struct
  typedef boost::unordered_map<ImpalaBackendServiceClient*, ClientInfo*> ClientMap;
  ClientMap client_map_;
};

}

#endif
