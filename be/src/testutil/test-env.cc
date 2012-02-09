// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "testutil/test-env.h"

#include <boost/algorithm/string.hpp>
#include <server/TServer.h>
#include <protocol/TBinaryProtocol.h>
#include <transport/TSocket.h>
#include <transport/TTransportUtils.h>
#include <glog/logging.h>

#include "common/status.h"
#include "service/backend-service.h"
#include "runtime/data-stream-mgr.h"
#include "runtime/hdfs-fs-cache.h"
#include "gen-cpp/ImpalaBackendService.h"

using namespace boost;
using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::server;
using namespace apache::thrift::transport;
using namespace apache::thrift::protocol;

DEFINE_string(backends, "", "comma-separated list of <host:port> pairs");

namespace impala {

struct TestEnv::ClientInfo {
  string host;
  int port;
  shared_ptr<TTransport> socket;
  shared_ptr<TTransport> transport;
  shared_ptr<TProtocol> protocol;
  scoped_ptr<ImpalaBackendServiceClient> client;

  ClientInfo(const string& host, int port);
  ~ClientInfo();
  Status Init();
};

TestEnv::ClientInfo::ClientInfo(const string& host, int port)
  : host(host),
    port(port),
    socket(new TSocket(host, port)),
    transport(new TBufferedTransport(socket)),
    protocol(new TBinaryProtocol(transport)),
    client(new ImpalaBackendServiceClient(protocol)) {
}

TestEnv::ClientInfo::~ClientInfo() {
  transport->close();
}

Status TestEnv::ClientInfo::Init() {
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

struct TestEnv::BackendInfo {
  thread backend_thread;
  TServer* server;
  DataStreamMgr stream_mgr;

  BackendInfo(): server(NULL) {}
};

TestEnv::TestEnv(int num_backends, int start_port)
  : fs_cache_(new HdfsFsCache()),
    num_backends_(num_backends),
    start_port_(start_port) {
  DCHECK_GT(num_backends_, 0);
  InitClientCache();
}

TestEnv::~TestEnv() {
  for (int i = 0; i < backend_info_.size(); ++i) {
    BackendInfo* info = backend_info_[i];
    info->server->stop();
    info->backend_thread.join();
    // TODO: auto_ptr?
    delete info;
  }
  for (ClientCache::iterator i = client_cache_.begin(); i != client_cache_.end(); ++i) {
    list<ClientInfo*>& clients = i->second;
    while (!clients.empty()) {
      delete clients.front();
      clients.pop_front();
    }
  }
}

void TestEnv::InitClientCache() {
  if (!FLAGS_backends.empty()) {
    vector<string> hostports;
    split(hostports, FLAGS_backends, is_any_of(","));
    for (int i = 0; i < hostports.size(); ++i) {
      int pos = hostports[i].find(':');
      if (pos == string::npos) {
        LOG(ERROR) << "ignore backend " << hostports[i] << ": missing ':'";
        continue;
      }
      string host = hostports[i].substr(0, pos);
      int port = atoi(hostports[i].substr(pos + 1).c_str());
      client_cache_[make_pair(host, port)] = list<ClientInfo*>();
    }
  } else {
    for (int i = 0; i < num_backends_; ++i) {
      client_cache_[make_pair("localhost", start_port_ + i)] = list<ClientInfo*>();
    }
  }
}

Status TestEnv::StartBackends() {
  if (!FLAGS_backends.empty()) return Status::OK;
  LOG(INFO) << "starting " << num_backends_ << " backends";
  int port = start_port_;
  for (int i = 0; i < num_backends_; ++i) {
    BackendInfo* info = new BackendInfo();
    info->server = StartImpalaBackendService(&info->stream_mgr, fs_cache_.get(), port++);
    DCHECK(info->server != NULL);
    info->backend_thread = thread(&TestEnv::RunBackendServer, this, info->server);
    backend_info_.push_back(info);
  }
  return Status::OK;
}

void TestEnv::RunBackendServer(TServer* server) {
  VLOG(1) << "serve()";
  server->serve();
}

void TestEnv::GetClients(int num_backends, vector<ImpalaBackendServiceClient*>* clients) {
  VLOG(1) << "GetClients(" << num_backends << ")";
  clients->clear();
  DCHECK_GT(num_backends, 0);
  while (true) {
    for (ClientCache::iterator i = client_cache_.begin(); i != client_cache_.end(); ++i) {
      list<ClientInfo*>& info_list = i->second;
      // if there's a client in the list, use that, otherwise create a new one
      if (!info_list.empty()) {
        clients->push_back(info_list.front()->client.get());
        VLOG(1) << "GetClients: adding client for host=" << info_list.front()->host
                << " port=" << info_list.front()->port;
        info_list.pop_front();
      } else {
        ClientInfo* info = new ClientInfo(i->first.first, i->first.second);
        DCHECK(info->Init().ok());
        client_map_[info->client.get()] = info;
        clients->push_back(info->client.get());
        VLOG(1) << "GetClients: creating client for host=" << info->host
                << " port=" << info->port;
      }
      if (clients->size() == num_backends) return;
    }
  }
}

void TestEnv::ReleaseClients(const vector<ImpalaBackendServiceClient*>& clients) {
  VLOG(1) << "releasing " << clients.size() << " clients";
  for (int i = 0; i < clients.size(); ++i) {
    ClientMap::iterator j = client_map_.find(clients[i]);
    DCHECK(j != client_map_.end());
    ClientInfo* info = j->second;
    ClientCache::iterator k = client_cache_.find(make_pair(info->host, info->port));
    DCHECK(k != client_cache_.end());
    k->second.push_back(info);
  }
}

std::string TestEnv::DebugString() {
  stringstream out;
  out << "#hosts=" << client_cache_.size()
      << " (";
  for (ClientCache::iterator i = client_cache_.begin(); i != client_cache_.end(); ++i) {
    if (i != client_cache_.begin()) out << " ";
    out << i->first.first << ":" << i->first.second << ":" << i->second.size();
  }
  out << ")";
  return out.str();
}

}
