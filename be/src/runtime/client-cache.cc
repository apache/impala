// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include "runtime/client-cache.h"

#include <sstream>
#include <server/TServer.h>
#include <protocol/TBinaryProtocol.h>
#include <transport/TSocket.h>
#include <transport/TTransportUtils.h>
#include <glog/logging.h>
#include <gflags/gflags.h>
#include <memory>

#include "gen-cpp/ImpalaInternalService.h"

using namespace std;
using namespace boost;
using namespace apache::thrift;
using namespace apache::thrift::server;
using namespace apache::thrift::transport;
using namespace apache::thrift::protocol;

namespace impala {

struct BackendClientCache::ClientInfo {
  string host;
  int port;
  shared_ptr<TTransport> socket;
  shared_ptr<TTransport> transport;
  shared_ptr<TProtocol> protocol;
  scoped_ptr<ImpalaInternalServiceClient> client;

  ClientInfo(const string& host, int port);
  ~ClientInfo();
  Status Init();
};

BackendClientCache::ClientInfo::ClientInfo(const string& host, int port)
  : host(host),
    port(port),
    socket(new TSocket(host, port)),
    transport(new TBufferedTransport(socket)),
    protocol(new TBinaryProtocol(transport)),
    client(new ImpalaInternalServiceClient(protocol)) {
}

BackendClientCache::ClientInfo::~ClientInfo() {
  transport->close();
}

Status BackendClientCache::ClientInfo::Init() {
  try {
    transport->open();
  } catch (TTransportException& e) {
    stringstream msg;
    msg << "couldn't open transport for " << host << ":" << port;
    LOG(ERROR) << msg.str();
    return Status(msg.str());
  }
  return Status::OK;
}

BackendClientCache::BackendClientCache(int max_clients, int max_clients_per_backend)
  : max_clients_(max_clients),
    max_clients_per_backend_(max_clients_per_backend) {
}

Status BackendClientCache::GetClient(
    const pair<string, int>& hostport, ImpalaInternalServiceClient** client) {
  VLOG_CONNECTION << "GetClient("
      << hostport.first << ":" << hostport.second << ")";
  lock_guard<mutex> l(lock_);
  ClientCache::iterator cache_entry = client_cache_.find(hostport);
  if (cache_entry == client_cache_.end()) {
    cache_entry =
        client_cache_.insert(make_pair(hostport, list<ClientInfo*>())).first;
    DCHECK(cache_entry != client_cache_.end());
  }

  list<ClientInfo*>& info_list = cache_entry->second;
  if (!info_list.empty()) {
    *client = info_list.front()->client.get();
    VLOG_CONNECTION << "GetClient(): adding client for " << info_list.front()->host
       << ":" << info_list.front()->port;
    info_list.pop_front();
  } else {
    auto_ptr<ClientInfo> info(
        new ClientInfo(cache_entry->first.first, cache_entry->first.second));
    RETURN_IF_ERROR(info->Init());
    client_map_[info->client.get()] = info.get();
    VLOG_CONNECTION << "GetClient(): creating client for "
         << info->host << ":" << info->port;
    *client = info.release()->client.get();
  }

  return Status::OK;
}

void BackendClientCache::ReleaseClient(ImpalaInternalServiceClient* client) {
  lock_guard<mutex> l(lock_);
  ClientMap::iterator i = client_map_.find(client);
  DCHECK(i != client_map_.end());
  ClientInfo* info = i->second;
  VLOG_CONNECTION << "releasing client for " << info->host << ":" << info->port;
  ClientCache::iterator j = client_cache_.find(make_pair(info->host, info->port));
  DCHECK(j != client_cache_.end());
  j->second.push_back(info);
}

string BackendClientCache::DebugString() {
  lock_guard<mutex> l(lock_);
  stringstream out;
  out << "BackendClientCache(#hosts=" << client_cache_.size()
      << " [";
  for (ClientCache::iterator i = client_cache_.begin(); i != client_cache_.end(); ++i) {
    if (i != client_cache_.begin()) out << " ";
    out << i->first.first << ":" << i->first.second << ":" << i->second.size();
  }
  out << "])";
  return out.str();
}

}
