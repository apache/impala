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

#include "scheduling/cluster-membership-test-util.h"
#include "common/logging.h"
#include "common/names.h"

static const int BACKEND_PORT = 1000;
static const int KRPC_PORT = 2000;
static const string HOSTNAME_PREFIX = "host_";
static const string IP_PREFIX = "10";

namespace impala {
namespace test {

string HostIdxToHostname(int host_idx) {
  return HOSTNAME_PREFIX + std::to_string(host_idx);
}

string HostIdxToIpAddr(int host_idx) {
  DCHECK_LT(host_idx, (1 << 24));
  string suffix;
  for (int i = 0; i < 3; ++i) {
    suffix = "." + std::to_string(host_idx % 256) + suffix; // prepend
    host_idx /= 256;
  }
  DCHECK_EQ(0, host_idx);
  return IP_PREFIX + suffix;
}

TBackendDescriptor MakeBackendDescriptor(int idx, int port_offset) {
  TBackendDescriptor be_desc;
  be_desc.address.hostname = HostIdxToHostname(idx);
  be_desc.address.port = BACKEND_PORT + port_offset;
  be_desc.ip_address = HostIdxToIpAddr(idx);
  // krpc_address is always resolved
  be_desc.krpc_address.hostname = be_desc.ip_address;
  be_desc.krpc_address.port = KRPC_PORT + port_offset;
  be_desc.__set_is_coordinator(true);
  be_desc.__set_is_executor(true);
  be_desc.is_quiescing = false;
  return be_desc;
}

}  // end namespace test
}  // end namespace impala
