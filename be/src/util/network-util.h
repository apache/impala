// Copyright 2013 Cloudera Inc.
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

#include "common/status.h"
#include "gen-cpp/Types_types.h"
#include <vector>

namespace impala {

// Looks up all IP addresses associated with a given hostname. Returns
// an error status if any system call failed, otherwise OK. Even if OK
// is returned, addresses may still be of zero length.
Status HostnameToIpAddrs(const std::string& name, std::vector<std::string>* addresses);

// Finds the first non-localhost IP address in the given list. Returns
// true if such an address was found, false otherwise.
bool FindFirstNonLocalhost(const std::vector<std::string>& addresses, std::string* addr);

// Sets the output argument to the system defined hostname.
// Returns OK if a hostname can be found, false otherwise.
Status GetHostname(std::string* hostname);

// Utility method because Thrift does not supply useful constructors
TNetworkAddress MakeNetworkAddress(const std::string& hostname, int port);

}
