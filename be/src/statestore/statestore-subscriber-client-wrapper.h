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

#ifndef STATESTORE_STATESTORE_SUBSCRIBER_CLIENT_WRAPPER_H
#define STATESTORE_STATESTORE_SUBSCRIBER_CLIENT_WRAPPER_H

#include "gen-cpp/StatestoreSubscriber.h"

namespace impala {

class StatestoreSubscriberClientWrapper : public StatestoreSubscriberClient {
  public:
   StatestoreSubscriberClientWrapper(
       std::shared_ptr<::apache::thrift::protocol::TProtocol> prot)
     : StatestoreSubscriberClient(prot) {
   }

   StatestoreSubscriberClientWrapper(
       std::shared_ptr<::apache::thrift::protocol::TProtocol> iprot,
       std::shared_ptr<::apache::thrift::protocol::TProtocol> oprot)
     : StatestoreSubscriberClient(iprot, oprot) {
   }

/// We intentionally disable this clang warning as we intend to hide the
/// the same-named functions defined in the base class.
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Woverloaded-virtual"

   void Heartbeat(THeartbeatResponse& _return, const THeartbeatRequest& params,
       bool* send_done) {
     DCHECK(!*send_done);
     send_Heartbeat(params);
     *send_done = true;
     recv_Heartbeat(_return);
   }

   void UpdateState(TUpdateStateResponse& _return, const TUpdateStateRequest& params,
       bool* send_done) {
     DCHECK(!*send_done);
     send_UpdateState(params);
     *send_done = true;
     recv_UpdateState(_return);
   }

   void UpdateCatalogd(TUpdateCatalogdResponse& _return,
       const TUpdateCatalogdRequest& params, bool* send_done) {
     DCHECK(!*send_done);
     send_UpdateCatalogd(params);
     *send_done = true;
     recv_UpdateCatalogd(_return);
   }

   void UpdateStatestoredRole(TUpdateStatestoredRoleResponse& _return,
       const TUpdateStatestoredRoleRequest& params, bool* send_done) {
     DCHECK(!*send_done);
     send_UpdateStatestoredRole(params);
     *send_done = true;
     recv_UpdateStatestoredRole(_return);
   }

#pragma clang diagnostic pop
};

}

#endif
