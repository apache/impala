// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include "util/thrift-util.h"

// TODO: remove this!
#include <glog/logging.h>

#include "util/hash-util.h"
#include "gen-cpp/Types_types.h"

namespace impala {

std::size_t hash_value(const THostPort& host_port) {
  uint32_t hash = HashUtil::Hash(host_port.host.c_str(), host_port.host.length(), 0);
  return HashUtil::Hash(&host_port.port, sizeof(host_port.port), hash);
}

// Comparator for THostPorts. Thrift declares this (in gen-cpp/Types_types.h) but
// never defines it.
bool THostPort::operator<(const THostPort& that) const {
  if (this->host < that.host) {
    return true;
  } else if ((this->host == that.host) && (this->port < that.port)) {
    return true;
  }
  return false;
};

}
