// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include <glog/logging.h>
#include <gflags/gflags.h>

#include <boost/algorithm/string.hpp>

#include "scheduler/simple-scheduler.h"

using namespace std;
using namespace boost;

DEFINE_string(backends, "", "comma-separated list of <host:port> pairs");

namespace impala {

SimpleScheduler::SimpleScheduler() {
  if (FLAGS_backends.empty()) return;
  vector<string> backends;
  split(backends, FLAGS_backends, is_any_of(","));
  for (int i = 0; i < backends.size(); ++i) {
    int pos = backends[i].find(':');
    if (pos == string::npos) {
      LOG(ERROR) << "ignoring backend " << backends[i] << ": missing ':'";
      continue;
    }
    string host = backends[i].substr(0, pos);
    int port = atoi(backends[i].substr(pos + 1).c_str());

    HostMap::iterator i = host_map_.find(host);
    if (i == host_map_.end()) {
      i = host_map_.insert(make_pair(host, list<int>())).first;
    }
    i->second.push_back(port);
  }
}

Status SimpleScheduler::GetHosts(
    const vector<string>& data_locations, vector<pair<string, int> >* hostports) {
  hostports->clear();
  for (int i = 0; i < data_locations.size(); ++i) {
    HostMap::iterator entry = host_map_.find(data_locations[i]);
    if (entry == host_map_.end()) {
      // TODO: should we make an effort to pick a random host?
      entry = host_map_.begin();
    }
    DCHECK(!entry->second.empty());
    // TODO: return a randomly selected backend for this host?
    // Will we ever have multiple backends on the same host, other
    // than in a test setup?
    hostports->push_back(make_pair(entry->first, entry->second.front()));
    VLOG(1) << "SimpleScheduler: selecting "
            << entry->first << ":" << entry->second.front();
  }
  DCHECK_EQ(data_locations.size(), hostports->size());
  return Status::OK;
}

}
