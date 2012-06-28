// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include <vector>

#include <glog/logging.h>

#include <boost/algorithm/string.hpp>

#include "sparrow/simple-scheduler.h"
#include "gen-cpp/ImpalaBackendService.h"

using namespace std;
using namespace boost;
using impala::Status;
using impala::THostPort;

namespace sparrow {

SimpleScheduler::SimpleScheduler(const vector<THostPort>& backends) {
  DCHECK(backends.size() > 0);
  for (int i = 0; i < backends.size(); ++i) {
    string host = backends[i].host;
    int port = backends[i].port;

    HostMap::iterator i = host_map_.find(host);
    if (i == host_map_.end()) {
      i = host_map_.insert(make_pair(host, list<int>())).first;
    }
    i->second.push_back(port);
  }
  next_nonlocal_host_entry_ = host_map_.begin();
}

Status SimpleScheduler::GetHosts(
    const vector<THostPort>& data_locations, vector<pair<string, int> >* hostports) {
  hostports->clear();
  for (int i = 0; i < data_locations.size(); ++i) {
    HostMap::iterator entry = host_map_.find(data_locations[i].host);
    if (entry == host_map_.end()) {
      // round robin the host
      entry = next_nonlocal_host_entry_;
      ++next_nonlocal_host_entry_;
      if (next_nonlocal_host_entry_ == host_map_.end()) {
        next_nonlocal_host_entry_ = host_map_.begin();
      }
    }
    DCHECK(!entry->second.empty());
    // Round-robin between impalads on the same host. Pick the first one, then move it
    // to the back of the queue
    int port = entry->second.front();
    hostports->push_back(make_pair(entry->first, port));
    entry->second.pop_front();
    entry->second.push_back(port);
    VLOG(1) << "SimpleScheduler: selecting "
            << entry->first << ":" << entry->second.front();
  }
  DCHECK_EQ(data_locations.size(), hostports->size());
  return Status::OK;
}

}
