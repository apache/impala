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

#include "util/network-util.h"

#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <limits.h>
#include <sstream>
#include <vector>
#include <boost/algorithm/string.hpp>
#include <boost/foreach.hpp>

#include "util/debug-util.h"
#include "util/error-util.h"
#include <util/string-parser.h>

#include "common/names.h"

using boost::algorithm::is_any_of;
using boost::algorithm::split;

#ifdef __APPLE__
// OS X does not seem to have a similar limitation as Linux and thus the
// macro is not defined.
#define HOST_NAME_MAX 64
#endif

namespace impala {

static const string LOCALHOST("127.0.0.1");

Status GetHostname(string* hostname) {
  char name[HOST_NAME_MAX];
  int ret = gethostname(name, HOST_NAME_MAX);
  if (ret != 0) {
    string error_msg = GetStrErrMsg();
    stringstream ss;
    ss << "Could not get hostname: " << error_msg;
    return Status(ss.str());
  }
  *hostname = string(name);
  return Status::OK();
}

Status HostnameToIpAddrs(const string& name, vector<string>* addresses) {
  addrinfo hints;
  memset(&hints, 0, sizeof(struct addrinfo));
  hints.ai_family = AF_INET; // IPv4 addresses only
  hints.ai_socktype = SOCK_STREAM;

  struct addrinfo* addr_info;
  if (getaddrinfo(name.c_str(), NULL, &hints, &addr_info) != 0) {
    stringstream ss;
    ss << "Could not find IPv4 address for: " << name;
    return Status(ss.str());
  }

  addrinfo* it = addr_info;
  while (it != NULL) {
    char addr_buf[64];
    const char* result =
        inet_ntop(AF_INET, &((sockaddr_in*)it->ai_addr)->sin_addr, addr_buf, 64);
    if (result == NULL) {
      stringstream ss;
      ss << "Could not convert IPv4 address for: " << name;
      freeaddrinfo(addr_info);
      return Status(ss.str());
    }
    addresses->push_back(string(addr_buf));
    it = it->ai_next;
  }

  freeaddrinfo(addr_info);
  return Status::OK();
}

bool FindFirstNonLocalhost(const vector<string>& addresses, string* addr) {
  BOOST_FOREACH(const string& candidate, addresses) {
    if (candidate != LOCALHOST) {
      *addr = candidate;
      return true;
    }
  }

  return false;
}

TNetworkAddress MakeNetworkAddress(const string& hostname, int port) {
  TNetworkAddress ret;
  ret.__set_hostname(hostname);
  ret.__set_port(port);
  return ret;
}

TNetworkAddress MakeNetworkAddress(const string& address) {
  vector<string> tokens;
  split(tokens, address, is_any_of(":"));
  TNetworkAddress ret;
  if (tokens.size() == 1) {
    ret.__set_hostname(tokens[0]);
    ret.port = 0;
    return ret;
  }
  if (tokens.size() != 2) return ret;
  ret.__set_hostname(tokens[0]);
  StringParser::ParseResult parse_result;
  int32_t port = StringParser::StringToInt<int32_t>(
      tokens[1].data(), tokens[1].length(), &parse_result);
  if (parse_result != StringParser::PARSE_SUCCESS) return ret;
  ret.__set_port(port);
  return ret;
}

bool IsWildcardAddress(const string& ipaddress) {
  return ipaddress == "0.0.0.0";
}

string TNetworkAddressToString(const TNetworkAddress& address) {
  stringstream ss;
  ss << address;
  return ss.str();
}

ostream& operator<<(ostream& out, const TNetworkAddress& hostport) {
  out << hostport.hostname << ":" << dec << hostport.port;
  return out;
}

}
