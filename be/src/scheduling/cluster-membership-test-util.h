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

namespace impala {

class BackendDescriptorPB;
class ExecutorGroup;
class ExecutorGroupDescPB;

namespace test {

static const int64_t MEGABYTE = 1024L * 1024L;
static const int64_t GIGABYTE = 1024L * MEGABYTE;

/// Convert a host index to a hostname.
std::string HostIdxToHostname(int host_idx);

/// Convert a host index to an IP address. The host index must be smaller than 2^24 and
/// will specify the lower 24 bits of the IPv4 address (the lower 3 octets).
std::string HostIdxToIpAddr(int host_idx);

/// Various methods to build a new backend descriptor. 'idx' is used to determine its name
/// and IP address and the caller must make sure that it is unique across sets of hosts.
/// To create backends on the same host, an optional port offset can be specified.
///
/// Make a backend descriptor for group 'group_desc'.
BackendDescriptorPB MakeBackendDescriptor(
    int idx, const ExecutorGroupDescPB& group_desc, int port_offset = 0,
    int64_t admit_mem_limit = 4L * MEGABYTE);

/// Make a backend descriptor for 'group'.
BackendDescriptorPB MakeBackendDescriptor(
    int idx, const ExecutorGroup& group, int port_offset = 0,
    int64_t admit_mem_limit = 4L * MEGABYTE);

/// Make a backend descriptor for the default executor group.
BackendDescriptorPB MakeBackendDescriptor(int idx, int port_offset = 0,
    int64_t admit_mem_limit = 4L * MEGABYTE);

}  // end namespace test
}  // end namespace impala
