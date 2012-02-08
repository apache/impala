// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include "runtime/hdfs-fs-cache.h"

#include <boost/thread/locks.hpp>
#include <glog/logging.h>
#include <gflags/gflags.h>

using namespace boost;

DEFINE_string(nn, "localhost", "namenode host");
DEFINE_int32(nn_port, 20500, "namenode port");

namespace impala {

HdfsFsCache::~HdfsFsCache() {
  for (HdfsFsMap::iterator i = fs_map_.begin(); i != fs_map_.end(); ++i) {
    int status = hdfsDisconnect(i->second);
    if (status != 0) { 
      // TODO: add error details
      LOG(ERROR) << "hdfsDisconnect(\"" << i->first.first << "\", " << i->first.second
                 << ") failed: " << " Error(" << errno << "): " << strerror(errno);
    }
  }
}

hdfsFS HdfsFsCache::GetConnection(const std::string& host, int port) {
  lock_guard<mutex> l(lock_);
  HdfsFsMap::iterator i = fs_map_.find(make_pair(host, port));
  if (i == fs_map_.end()) {
    hdfsFS conn = hdfsConnect(host.c_str(), port);
    DCHECK(conn != NULL);
    fs_map_.insert(make_pair(make_pair(host, port), conn));
    return conn;
  } else {
    return i->second;
  }
}

hdfsFS HdfsFsCache::GetDefaultConnection() {
  return GetConnection(FLAGS_nn, FLAGS_nn_port);
}

}
