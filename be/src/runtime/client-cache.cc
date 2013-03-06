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
#include "util/thrift-util.h"
#include "gen-cpp/ImpalaInternalService.h"
using namespace std;
using namespace boost;
using namespace apache::thrift;
using namespace apache::thrift::server;
using namespace apache::thrift::transport;
using namespace apache::thrift::protocol;

namespace impala {

Status ClientCacheHelper::GetClient(const pair<string, int>& hostport,
    ClientFactory factory_method, ThriftClientImpl** client) {
  lock_guard<mutex> lock(lock_);
  VLOG_RPC << "GetClient("
           << hostport.first << ":" << hostport.second << ")";
  ClientCacheMap::iterator cache_entry = client_cache_.find(hostport);
  if (cache_entry == client_cache_.end()) {
    cache_entry =
        client_cache_.insert(make_pair(hostport, list<ThriftClientImpl*>())).first;
    DCHECK(cache_entry != client_cache_.end());
  }

  list<ThriftClientImpl*>& info_list = cache_entry->second;
  if (!info_list.empty()) {
    *client = info_list.front();
    VLOG_RPC << "GetClient(): cached client for "
             << info_list.front()->ipaddress()
             << ":" << info_list.front()->port();
    info_list.pop_front();
  } else {
    void* client_key;
    auto_ptr<ThriftClientImpl> client_impl(factory_method(hostport, &client_key));
    VLOG_CONNECTION << "GetClient(): adding new client for "
                    << client_impl->ipaddress() << ":" << client_impl->port();
    RETURN_IF_ERROR(client_impl->Open());
    // Because the client starts life 'checked out', we don't add it to the cache map
    client_map_[client_key] = client_impl.get();
    *client = client_impl.release();
    if (metrics_enabled_) {
      total_clients_metric_->Increment(1);
    }
  }

  if (metrics_enabled_) {
    clients_in_use_metric_->Increment(1);
  }

  return Status::OK;
}

Status ClientCacheHelper::ReopenClient(void* client_key) {
  lock_guard<mutex> lock(lock_);
  ClientMap::iterator i = client_map_.find(client_key);
  DCHECK(i != client_map_.end());
  ThriftClientImpl* info = i->second;
  RETURN_IF_ERROR(info->Close());
  RETURN_IF_ERROR(info->Open());
  return Status::OK;
}

void ClientCacheHelper::ReleaseClient(void* client_key) {
  lock_guard<mutex> lock(lock_);
  ClientMap::iterator i = client_map_.find(client_key);
  DCHECK(i != client_map_.end());
  ThriftClientImpl* info = i->second;
  VLOG_RPC << "releasing client for "
           << info->ipaddress() << ":" << info->port();
  ClientCacheMap::iterator j =
      client_cache_.find(make_pair(info->ipaddress(), info->port()));
  DCHECK(j != client_cache_.end());
  j->second.push_back(info);
  if (metrics_enabled_) {
    clients_in_use_metric_->Increment(-1);
  }
}

void ClientCacheHelper::CloseConnections(const pair<string, int>& hostport) {
  lock_guard<mutex> lock(lock_);
  ClientCacheMap::iterator cache_entry = client_cache_.find(hostport);
  if (cache_entry == client_cache_.end()) return;
  VLOG_RPC << "Invalidating all " << cache_entry->second.size() << " clients for: "
           << hostport.first << ":" << hostport.second;
  BOOST_FOREACH(ThriftClientImpl* client, cache_entry->second) {
    client->Close();
  }
}

string ClientCacheHelper::DebugString() {
  stringstream out;
  out << "ClientCacheHelper(#hosts=" << client_cache_.size()
      << " [";
  for (ClientCacheMap::iterator i = client_cache_.begin(); i != client_cache_.end(); ++i) {
    if (i != client_cache_.begin()) out << " ";
    out << i->first.first << ":" << i->first.second << ":" << i->second.size();
  }
  out << "])";
  return out.str();
}

void ClientCacheHelper::TestShutdown() {
  vector<pair<string, int> > hostports;
  {
    lock_guard<mutex> lock(lock_);
    BOOST_FOREACH(const ClientCacheMap::value_type& i, client_cache_) {
      hostports.push_back(i.first);
    }
  }
  for (vector<pair<string, int> >::iterator it = hostports.begin(); it != hostports.end();
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
