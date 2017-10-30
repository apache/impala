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

#ifndef KUDU_RPC_RPC_CONSTANTS_H
#define KUDU_RPC_RPC_CONSTANTS_H

#include <cstdint>
#include <set>

#include "kudu/rpc/rpc_header.pb.h"

namespace kudu {
namespace rpc {

// Magic number bytes sent at connection setup time.
extern const char* const kMagicNumber;

// App name for SASL library init
extern const char* const kSaslAppName;

// Current version of the RPC protocol.
static const uint32_t kCurrentRpcVersion = 9;

// From Hadoop.
static const int32_t kInvalidCallId = -2;
static const int32_t kConnectionContextCallId = -3;
static const int32_t kNegotiateCallId = -33;

static const uint8_t kMagicNumberLength = 4;
static const uint8_t kHeaderFlagsLength = 3;

// There is a 4-byte length prefix before any packet.
static const uint8_t kMsgLengthPrefixLength = 4;

// The set of RPC features that this server build supports.
// Non-const for testing.
extern std::set<RpcFeatureFlag> kSupportedServerRpcFeatureFlags;

// The set of RPC features that this client build supports.
// Non-const for testing.
extern std::set<RpcFeatureFlag> kSupportedClientRpcFeatureFlags;

} // namespace rpc
} // namespace kudu

#endif // KUDU_RPC_RPC_CONSTANTS_H
