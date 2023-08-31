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
#include "scheduling/executor-group.h"
#include "service/impala-server.h"
#include "util/uid-util.h"
#include "gutil/strings/strcat.h"

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
    suffix = StrCat(".", std::to_string(host_idx % 256), suffix); // prepend
    host_idx /= 256;
  }
  DCHECK_EQ(0, host_idx);
  return IP_PREFIX + suffix;
}

BackendDescriptorPB MakeBackendDescriptor(
    int idx, const ExecutorGroupDescPB& group_desc, int port_offset,
    int64_t admit_mem_limit) {
  BackendDescriptorPB be_desc;
  UUIDToUniqueIdPB(boost::uuids::random_generator()(), be_desc.mutable_backend_id());
  be_desc.mutable_address()->set_hostname(HostIdxToHostname(idx));
  be_desc.mutable_address()->set_port(BACKEND_PORT + port_offset);
  be_desc.set_ip_address(HostIdxToIpAddr(idx));
  // krpc_address is always resolved
  be_desc.mutable_krpc_address()->set_hostname(be_desc.ip_address());
  be_desc.mutable_krpc_address()->set_port(KRPC_PORT + port_offset);
  be_desc.set_is_coordinator(true);
  be_desc.set_is_executor(true);
  be_desc.set_is_quiescing(false);
  be_desc.set_admit_mem_limit(admit_mem_limit);
  *be_desc.add_executor_groups() = group_desc;
  return be_desc;
}

BackendDescriptorPB MakeBackendDescriptor(
    int idx, const ExecutorGroup& group, int port_offset, int64_t admit_mem_limit) {
  ExecutorGroupDescPB group_desc;
  group_desc.set_name(group.name());
  group_desc.set_min_size(group.min_size());
  return MakeBackendDescriptor(idx, group_desc, port_offset, admit_mem_limit);
}

BackendDescriptorPB MakeBackendDescriptor(int idx, int port_offset,
    int64_t admit_mem_limit) {
  ExecutorGroupDescPB group_desc;
  group_desc.set_name(ImpalaServer::DEFAULT_EXECUTOR_GROUP_NAME);
  group_desc.set_min_size(1);
  return MakeBackendDescriptor(idx, group_desc, port_offset, admit_mem_limit);
}

}  // end namespace test
}  // end namespace impala
