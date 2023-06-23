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

#ifndef STATESTORE_STATESTORE_SERVICE_CLIENT_WRAPPER_H
#define STATESTORE_STATESTORE_SERVICE_CLIENT_WRAPPER_H

#include "gen-cpp/StatestoreHaService.h"
#include "gen-cpp/StatestoreService.h"

namespace impala {

class StatestoreServiceClientWrapper : public StatestoreServiceClient {
 public:
  StatestoreServiceClientWrapper(
      std::shared_ptr<::apache::thrift::protocol::TProtocol> prot)
    : StatestoreServiceClient(prot) {
  }

  StatestoreServiceClientWrapper(
      std::shared_ptr<::apache::thrift::protocol::TProtocol> iprot,
      std::shared_ptr<::apache::thrift::protocol::TProtocol> oprot)
    : StatestoreServiceClient(iprot, oprot) {
  }

/// We intentionally disable this clang warning as we intend to hide the
/// the same-named functions defined in the base class.
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Woverloaded-virtual"

  void RegisterSubscriber(TRegisterSubscriberResponse& _return,
      const TRegisterSubscriberRequest& params, bool* send_done) {
    DCHECK(!*send_done);
    send_RegisterSubscriber(params);
    *send_done = true;
    recv_RegisterSubscriber(_return);
  }

  void GetProtocolVersion(TGetProtocolVersionResponse& _return,
      const TGetProtocolVersionRequest& params, bool* send_done) {
    DCHECK(!*send_done);
    send_GetProtocolVersion(params);
    *send_done = true;
    recv_GetProtocolVersion(_return);
  }

  void SetStatestoreDebugAction(TSetStatestoreDebugActionResponse& _return,
      const TSetStatestoreDebugActionRequest& params, bool* send_done) {
    DCHECK(!*send_done);
    send_SetStatestoreDebugAction(params);
    *send_done = true;
    recv_SetStatestoreDebugAction(_return);
  }
#pragma clang diagnostic pop
};

class StatestoreHaServiceClientWrapper : public StatestoreHaServiceClient {
 public:
  StatestoreHaServiceClientWrapper(
      std::shared_ptr<::apache::thrift::protocol::TProtocol> prot)
    : StatestoreHaServiceClient(prot) {
  }

  StatestoreHaServiceClientWrapper(
      std::shared_ptr<::apache::thrift::protocol::TProtocol> iprot,
      std::shared_ptr<::apache::thrift::protocol::TProtocol> oprot)
    : StatestoreHaServiceClient(iprot, oprot) {
  }

/// We intentionally disable this clang warning as we intend to hide the
/// the same-named functions defined in the base class.
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Woverloaded-virtual"

  void StatestoreHaHandshake(TStatestoreHaHandshakeResponse& _return,
      const TStatestoreHaHandshakeRequest& params, bool* send_done) {
    DCHECK(!*send_done);
    send_StatestoreHaHandshake(params);
    *send_done = true;
    recv_StatestoreHaHandshake(_return);
  }

  void StatestoreHaHeartbeat(TStatestoreHaHeartbeatResponse& _return,
      const TStatestoreHaHeartbeatRequest& params, bool* send_done) {
    DCHECK(!*send_done);
    send_StatestoreHaHeartbeat(params);
    *send_done = true;
    recv_StatestoreHaHeartbeat(_return);
  }

#pragma clang diagnostic pop
};

}
#endif
