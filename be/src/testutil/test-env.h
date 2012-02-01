// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_TESTUTIL_TEST_ENV_H
#define IMPALA_TESTUTIL_TEST_ENV_H

#include <string>
#include <vector>
#include <list>
#include <boost/thread/thread.hpp>
#include <boost/unordered_map.hpp>
#include <boost/scoped_ptr.hpp>

#include "common/status.h"

namespace apache { namespace thrift { namespace server { class TServer; } } }

namespace impala {

class HdfsFsCache;
class ImpalaBackendServiceClient;

// Create environment for single-process distributed query execution.
class TestEnv {
 public:
  TestEnv(int start_port);

  // Stop backend threads.
  ~TestEnv();

  // Start backends in separate threads; each one exports ImpalaBackendService
  // Thrift service.
  Status StartBackends(int num_backends);

  // Return "num_backends" clients.
  // Returned clients are owned by TestEnv and must not be deallocated.
  void GetClients(
      int num_backends, std::vector<ImpalaBackendServiceClient*>* clients);

  // Hand clients back to TestEnv.
  void ReleaseClients(const std::vector<ImpalaBackendServiceClient*>& clients);

  HdfsFsCache* fs_cache() { return fs_cache_.get(); }

  std::string DebugString();

 private:
  boost::scoped_ptr<HdfsFsCache> fs_cache_;
  int start_port_;

  struct BackendInfo;
  // owned by us
  std::vector<BackendInfo*> backend_info_;

  // TODO: move all of this into a separate BackendClientCache class
  // that manages connections and shuts them down if they don't get used
  // for a while, etc.
  struct ClientInfo;

  // map from (host, port) to list of clients;
  // we own ClientInfo*
  typedef boost::unordered_map<std::pair<std::string, int>, std::list<ClientInfo*> >
      ClientCache;
  ClientCache client_cache_;

  // map from client back to its containing struct
  typedef boost::unordered_map<ImpalaBackendServiceClient*, ClientInfo*> ClientMap;
  ClientMap client_map_;

  void RunBackendServer(apache::thrift::server::TServer* server);
};

}

#endif
