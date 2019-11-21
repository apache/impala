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

#ifndef IMPALA_BACKEND_CLIENT_H
#define IMPALA_BACKEND_CLIENT_H

#include "runtime/client-cache.h"
#include "testutil/fault-injection-util.h"
#include "util/runtime-profile-counters.h"

#include "gen-cpp/ImpalaInternalService.h"

namespace impala {

/// Proxy class that extends ImpalaInternalServiceClient to allow callers to time
/// the wall-clock time taken in TransmitData(), so that the time spent sending data
/// between backends in a query can be measured.
class ImpalaBackendClient : public ImpalaInternalServiceClient {
 public:
  ImpalaBackendClient(boost::shared_ptr< ::apache::thrift::protocol::TProtocol> prot)
    : ImpalaInternalServiceClient(prot) {
  }

  ImpalaBackendClient(boost::shared_ptr< ::apache::thrift::protocol::TProtocol> iprot,
      boost::shared_ptr< ::apache::thrift::protocol::TProtocol> oprot)
    : ImpalaInternalServiceClient(iprot, oprot) {
  }

/// We intentionally disable this clang warning as we intend to hide the
/// the same-named functions defined in the base class.
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Woverloaded-virtual"

  void UpdateFilter(TUpdateFilterResult& _return, const TUpdateFilterParams& params,
      bool* send_done) {
    DCHECK(!*send_done);
    ImpalaInternalServiceClient::send_UpdateFilter(params);
    *send_done = true;
    ImpalaInternalServiceClient::recv_UpdateFilter(_return);
  }

  void PublishFilter(TPublishFilterResult& _return, const TPublishFilterParams& params,
      bool* send_done) {
    DCHECK(!*send_done);
    ImpalaInternalServiceClient::send_PublishFilter(params);
    *send_done = true;
    ImpalaInternalServiceClient::recv_PublishFilter(_return);
  }

#pragma clang diagnostic pop

};

}

#endif // IMPALA_BACKEND_CLIENT_H
