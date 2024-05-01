// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <string>
#include <vector>

#include "common/status.h"
#include "gen-cpp/common.pb.h"
#include "gen-cpp/Types_types.h"

namespace kudu {
class Sockaddr;
} // namespace kudu

namespace impala {

/// Type to store hostnames, which can be rfc1123 hostnames or IPv4 addresses.
typedef std::string Hostname;

/// Type to store IPv4 addresses.
typedef std::string IpAddr;

/// Returns true if 'addr' is a fully resolved IP address, rather than a fqdn + port.
bool IsResolvedAddress(const TNetworkAddress& addr);
bool IsResolvedAddress(const NetworkAddressPB& addr);

/// Looks up all IP addresses associated with a given hostname and returns one of them via
/// 'address'. If the IP addresses of a host don't change, then subsequent calls will
/// always return the same address. Returns an error status if any system call failed,
/// otherwise OK. Even if OK is returned, addresses may still be of zero length.
Status HostnameToIpAddr(const Hostname& hostname, IpAddr* ip) WARN_UNUSED_RESULT;

/// Finds the first non-localhost IP address in the given list. Returns
/// true if such an address was found, false otherwise.
bool FindFirstNonLocalhost(const std::vector<std::string>& addresses, std::string* addr);

/// Sets the output argument to the system defined hostname.
/// Returns OK if a hostname can be found, false otherwise.
Status GetHostname(std::string* hostname) WARN_UNUSED_RESULT;

/// Generate UDS address.
string GetUDSAddress(const std::string& hostname, int port, const UniqueIdPB& backend_id,
    const UdsAddressUniqueIdPB& uds_addr_unique_id);

/// Utility methods because Thrift/protobuf do not supply useful constructors
TNetworkAddress MakeNetworkAddress(const std::string& hostname, int port);
NetworkAddressPB MakeNetworkAddressPB(const std::string& hostname, int port);
NetworkAddressPB MakeNetworkAddressPB(const std::string& hostname, int port,
    const UniqueIdPB& backend_id, const UdsAddressUniqueIdPB& uds_addr_unique_id);
/// This function generate unique ID if needed.
/// It's only used if backend ID is not available, like unit-test or Admissiond.
NetworkAddressPB MakeNetworkAddressPB(const std::string& hostname, int port,
    const UdsAddressUniqueIdPB& uds_addr_unique_id);

/// Utility method to parse the given string into a network address.
/// Accepted format: "host:port" or "host". For the latter format the port is set to zero.
/// If the given string address is malformed, returns a network address with an empty
/// hostname and a port of 0.
TNetworkAddress MakeNetworkAddress(const std::string& address);

/// Returns true if the ip address parameter is the wildcard interface (0.0.0.0)
bool IsWildcardAddress(const std::string& ipaddress);

/// Utility method to print address as address:port
std::string TNetworkAddressToString(const TNetworkAddress& address);

/// Utility method to print a NetworkAddressPB as address:port.
std::string NetworkAddressPBToString(const NetworkAddressPB& address);

inline ostream& operator<<(ostream& os, const NetworkAddressPB& address) {
  return os << NetworkAddressPBToString(address);
}

/// Utility method to convert a NetworkAddressPB to a TNetworkAddress.
TNetworkAddress FromNetworkAddressPB(const NetworkAddressPB& address);

/// Utility method to convert a TNetworkAddress to a NetworkAddressPB.
NetworkAddressPB FromTNetworkAddress(const TNetworkAddress& address);

/// Utility method to convert NetworkAddressPB to Kudu Sockaddr.
/// If use_uds is true, set Kudu Sockaddr as UDS address.
/// Note that 'address' has to contain a resolved IP address.
Status NetworkAddressPBToSockaddr(
    const NetworkAddressPB& address, bool use_uds, kudu::Sockaddr* sockaddr);

/// Custom comparator to sort network addresses first by host (alphabetically) and then by
/// by port (numerically) and finally by uds address (alphabetically).
struct TNetworkAddressComparator {
  bool operator()(const TNetworkAddress& a, const TNetworkAddress& b) const;
};

/// Returns a ephemeral port that is currently unused. Returns -1 on an error or if
/// a free ephemeral port can't be found after 100 tries.
int FindUnusedEphemeralPort();

/// Compare function for two NetworkAddressPB.
/// The order is decided first by hostname, then by port, then by uds address.
inline int CompareNetworkAddressPB(
    const NetworkAddressPB& lhs, const NetworkAddressPB& rhs) {
  int comp = lhs.hostname().compare(rhs.hostname());
  if (comp == 0) comp = lhs.port() - rhs.port();
  if (comp == 0) {
    if (lhs.has_uds_address()) {
      if (rhs.has_uds_address()) {
        comp = lhs.uds_address().compare(rhs.uds_address());
      } else {
        comp = 1; // lhs preceed rhs
      }
    } else if (rhs.has_uds_address()) {
      comp = -1; // rhs preceed lhs
    }
  }
  return comp;
}

/// Return true if two NetworkAddressPB are match.
inline bool KrpcAddressEqual(const NetworkAddressPB& lhs, const NetworkAddressPB& rhs) {
  return CompareNetworkAddressPB(lhs, rhs) == 0;
}

extern const std::string LOCALHOST_IP_STR;

} // namespace impala
