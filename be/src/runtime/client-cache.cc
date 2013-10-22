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

#include "runtime/client-cache.h"

#include <sstream>
#include <thrift/server/TServer.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>
#include <memory>

#include <boost/foreach.hpp>

#include "common/logging.h"
#include "util/container-util.h"
#include "util/network-util.h"
#include "rpc/thrift-util.h"
#include "gen-cpp/ImpalaInternalService.h"

using namespace std;
using namespace boost;
using namespace apache::thrift;
using namespace apache::thrift::server;
using namespace apache::thrift::transport;
using namespace apache::thrift::protocol;

namespace impala {

Status ClientCacheHelper::GetClient(const TNetworkAddress& hostport,
    ClientFactory factory_method, void** client_key) {
  lock_guard<mutex> lock(lock_);
  VLOG_RPC << "GetClient(" << hostport << ")";
  ClientCacheMap::iterator cache_entry = client_cache_.find(hostport);
  if (cache_entry == client_cache_.end()) {
    cache_entry =
        client_cache_.insert(make_pair(hostport, list<void*>())).first;
    DCHECK(cache_entry != client_cache_.end());
  }

  list<void*>& info_list = cache_entry->second;
  if (!info_list.empty()) {
    *client_key = info_list.front();
    VLOG_RPC << "GetClient(): cached client for " << hostport;
    info_list.pop_front();
  } else {
    RETURN_IF_ERROR(CreateClient(hostport, factory_method, client_key));
  }

  if (metrics_enabled_) {
    clients_in_use_metric_->Increment(1);
  }

  return Status::OK;
}

Status ClientCacheHelper::ReopenClient(ClientFactory factory_method, void** client_key) {
  lock_guard<mutex> lock(lock_);
  ClientMap::iterator i = client_map_.find(*client_key);
  DCHECK(i != client_map_.end());
  ThriftClientImpl* info = i->second;
  const string ipaddress = info->ipaddress();
  int port = info->port();

  info->Close();

  // TODO: Thrift TBufferedTransport cannot be re-opened after Close() because it does
  // not clean up internal buffers it reopens. To work around this issue, create a new
  // client instead.
  client_map_.erase(*client_key);
  delete info;
  *client_key = NULL;
  if (metrics_enabled_) {
    total_clients_metric_->Increment(-1);
  }
  RETURN_IF_ERROR(
      CreateClient(MakeNetworkAddress(ipaddress, port), factory_method, client_key));
  return Status::OK;
}

Status ClientCacheHelper::CreateClient(const TNetworkAddress& hostport,
    ClientFactory factory_method, void** client_key) {
  auto_ptr<ThriftClientImpl> client_impl(factory_method(hostport, client_key));
  VLOG_CONNECTION << "CreateClient(): adding new client for "
                  << client_impl->ipaddress() << ":" << client_impl->port();
  Status status = client_impl->OpenWithRetry(num_tries_, wait_ms_);
  if (!status.ok()) {
    *client_key = NULL;
    return status;
  }
  // Set the TSocket's send and receive timeouts.
  client_impl->setRecvTimeout(recv_timeout_ms_);
  client_impl->setSendTimeout(send_timeout_ms_);

  // Because the client starts life 'checked out', we don't add it to the cache map
  client_map_[*client_key] = client_impl.get();
  client_impl.release();
  if (metrics_enabled_) {
    total_clients_metric_->Increment(1);
  }
  return Status::OK;
}

void ClientCacheHelper::ReleaseClient(void** client_key) {
  DCHECK(*client_key != NULL) << "Trying to release NULL client";
  lock_guard<mutex> lock(lock_);
  ClientMap::iterator i = client_map_.find(*client_key);
  DCHECK(i != client_map_.end());
  ThriftClientImpl* info = i->second;
  VLOG_RPC << "releasing client for "
           << info->ipaddress() << ":" << info->port();
  ClientCacheMap::iterator j =
      client_cache_.find(MakeNetworkAddress(info->ipaddress(), info->port()));
  DCHECK(j != client_cache_.end());
  j->second.push_back(*client_key);
  if (metrics_enabled_) {
    clients_in_use_metric_->Increment(-1);
  }
  *client_key = NULL;
}

void ClientCacheHelper::CloseConnections(const TNetworkAddress& hostport) {
  lock_guard<mutex> lock(lock_);
  ClientCacheMap::iterator cache_entry = client_cache_.find(hostport);
  if (cache_entry == client_cache_.end()) return;
  VLOG_RPC << "Invalidating all " << cache_entry->second.size() << " clients for: "
           << hostport;
  BOOST_FOREACH(void* client_key, cache_entry->second) {
    ClientMap::iterator client_map_entry = client_map_.find(client_key);
    DCHECK(client_map_entry != client_map_.end());
    client_map_entry->second->Close();
  }
}

string ClientCacheHelper::DebugString() {
  stringstream out;
  out << "ClientCacheHelper(#hosts=" << client_cache_.size()
      << " [";
  for (ClientCacheMap::iterator i = client_cache_.begin(); i != client_cache_.end(); ++i) {
    if (i != client_cache_.begin()) out << " ";
    out << i->first << ":" << i->second.size();
  }
  out << "])";
  return out.str();
}

void ClientCacheHelper::TestShutdown() {
  vector<TNetworkAddress> hostports;
  {
    lock_guard<mutex> lock(lock_);
    BOOST_FOREACH(const ClientCacheMap::value_type& i, client_cache_) {
      hostports.push_back(i.first);
    }
  }
  for (vector<TNetworkAddress>::iterator it = hostports.begin(); it != hostports.end();
      ++it) {
    CloseConnections(*it);
  }
}

void ClientCacheHelper::InitMetrics(Metrics* metrics, const string& key_prefix) {
  DCHECK(metrics != NULL);
  // Not strictly needed if InitMetrics is called before any cache
  // usage, but ensures that metrics_enabled_ is published.
  lock_guard<mutex> lock(lock_);
  stringstream count_ss;
  count_ss << key_prefix << ".client-cache.clients-in-use";
  clients_in_use_metric_ =
      metrics->CreateAndRegisterPrimitiveMetric(count_ss.str(), 0L);

  stringstream max_ss;
  max_ss << key_prefix << ".client-cache.total-clients";
  total_clients_metric_ = metrics->CreateAndRegisterPrimitiveMetric(max_ss.str(), 0L);
  metrics_enabled_ = true;
}


}
