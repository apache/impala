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

#include "util/llama-util.h"

#include <sstream>
#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string.hpp>

using namespace std;
using namespace boost;
using namespace llama;

namespace llama {

ostream& operator<<(ostream& os, const TUniqueId& id) {
  os << hex << id.hi << ":" << id.lo;
  return os;
}

ostream& operator<<(ostream& os, const TNetworkAddress& address) {
  os << address.hostname << ":" << dec << address.port;
  return os;
}

ostream& operator<<(ostream& os, const TResource& resource) {
  os << "Resource("
     << "client_resource_id=" << resource.client_resource_id << " "
     << "v_cpu_cores=" << dec << resource.v_cpu_cores << " "
     << "memory_mb=" << dec << resource.memory_mb << " "
     << "asked_location=" << resource.askedLocation << " "
     << "enforcement=" << resource.enforcement << ")";
  return os;
}

ostream& operator<<(ostream& os, const TAllocatedResource& resource) {
  os << "Allocated Resource("
     << "reservation_id=" << resource.reservation_id << " "
     << "client_resource_id=" << resource.client_resource_id << " "
     << "rm_resource_id=" << resource.rm_resource_id << " "
     << "v_cpu_cores=" << dec << resource.v_cpu_cores << " "
     << "memory_mb=" << dec << resource.memory_mb << " "
     << "location=" << resource.location << ")";
  return os;
}

ostream& operator<<(ostream& os, const llama::TLlamaAMGetNodesRequest& request) {
  os << "GetNodes Request(llama handle=" << request.am_handle << ")";
  return os;
}

ostream& operator<<(ostream& os, const llama::TLlamaAMReservationRequest& request) {
  os << "Reservation Request("
     << "llama handle=" << request.am_handle << " "
     << "queue=" << request.queue << " "
     << "user=" << request.user << " "
     << "gang=" << request.gang << " "
     << "resources=[";
  for (int i = 0; i < request.resources.size(); ++i) {
    os << request.resources[i];
    if (i + 1 != request.resources.size()) os << ",";
  }
  os << "])";
  return os;
}

ostream& operator<<(ostream& os,
    const llama::TLlamaAMReservationExpansionRequest& request) {
  os << "Expansion Request("
     << "llama handle=" << request.am_handle << " "
     << "reservation id=" << request.expansion_of << " "
     << "resource=" << request.resource << ")";
  return os;
}

ostream& operator<<(ostream& os, const llama::TLlamaAMReleaseRequest& request) {
  os << "Release Request("
     << "llama handle=" << request.am_handle << " "
     << "reservation id=" << request.reservation_id << ")";
  return os;
}

llama::TUniqueId& operator<<(llama::TUniqueId& dest, const impala::TUniqueId& src) {
  dest.lo = src.lo;
  dest.hi = src.hi;
  return dest;
}

impala::TUniqueId& operator<<(impala::TUniqueId& dest, const llama::TUniqueId& src) {
  dest.lo = src.lo;
  dest.hi = src.hi;
  return dest;
}

bool operator==(const impala::TUniqueId& impala_id, const llama::TUniqueId& llama_id) {
  return impala_id.lo == llama_id.lo && impala_id.hi == llama_id.hi;
}

llama::TNetworkAddress& operator<<(llama::TNetworkAddress& dest,
    const impala::TNetworkAddress& src) {
  dest.hostname = src.hostname;
  dest.port = src.port;
  return dest;
}

impala::TNetworkAddress& operator<<(impala::TNetworkAddress& dest,
    const llama::TNetworkAddress& src) {
  dest.hostname = src.hostname;
  dest.port = src.port;
  return dest;
}

impala::Status LlamaStatusToImpalaStatus(const TStatus& status,
    const string& err_prefix) {
  if (status.status_code == TStatusCode::OK) return impala::Status::OK;
  stringstream ss;
  ss << err_prefix << " " << join(status.error_msgs, ", ");
  return impala::Status(ss.str());
}

string GetShortName(const string& user) {
  if (user.empty() || user[0] == '/' || user[0] == '@') return user;

  vector<string> components;
  split(components, user, is_any_of("/@"));
  return components[0];
}

}
