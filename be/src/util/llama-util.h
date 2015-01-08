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

#ifndef IMPALA_UTIL_LLAMA_UTIL_H
#define IMPALA_UTIL_LLAMA_UTIL_H

#include <ostream>
#include <string>
#include <boost/functional/hash.hpp>

#include "gen-cpp/Types_types.h"  // for TUniqueId
#include "gen-cpp/Llama_types.h"  // for TUniqueId
#include "common/status.h"

namespace llama {

std::ostream& operator<<(std::ostream& os, const llama::TUniqueId& id);
std::ostream& operator<<(std::ostream& os, const llama::TNetworkAddress& address);
std::ostream& operator<<(std::ostream& os, const llama::TResource& resource);
std::ostream& operator<<(std::ostream& os, const llama::TAllocatedResource& resource);

std::ostream& operator<<(std::ostream& os,
    const llama::TLlamaAMGetNodesRequest& request);
std::ostream& operator<<(std::ostream& os,
    const llama::TLlamaAMReservationRequest& request);
std::ostream& operator<<(std::ostream& os,
    const llama::TLlamaAMReservationExpansionRequest& request);
std::ostream& operator<<(std::ostream& os,
    const llama::TLlamaAMReleaseRequest& request);

// 'Assignment' operators to convert types between the llama and impala namespaces.
llama::TUniqueId& operator<<(llama::TUniqueId& dest, const impala::TUniqueId& src);
impala::TUniqueId& operator<<(impala::TUniqueId& dest, const llama::TUniqueId& src);

bool operator==(const impala::TUniqueId& impala_id, const llama::TUniqueId& llama_id);

llama::TNetworkAddress& operator<<(llama::TNetworkAddress& dest,
    const impala::TNetworkAddress& src);
impala::TNetworkAddress& operator<<(impala::TNetworkAddress& dest,
    const llama::TNetworkAddress& src);

impala::Status LlamaStatusToImpalaStatus(const llama::TStatus& status,
    const std::string& err_prefix = "");

// This function must be called 'hash_value' to be picked up by boost.
inline std::size_t hash_value(const llama::TUniqueId& id) {
  std::size_t seed = 0;
  boost::hash_combine(seed, id.lo);
  boost::hash_combine(seed, id.hi);
  return seed;
}

// Get the short version of the user name (the user's name up to the first '/' or '@')
// If neither are found (or are found at the beginning of the user name) return username.
std::string GetShortName(const std::string& user);

}

#endif
