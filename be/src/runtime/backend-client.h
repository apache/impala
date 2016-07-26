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
#include "util/runtime-profile-counters.h"

#include "gen-cpp/ImpalaInternalService.h"

namespace impala {

/// Proxy class that extends ImpalaInternalServiceClient to allow callers to time
/// the wall-clock time taken in TransmitData(), so that the time spent sending data
/// between backends in a query can be measured.
class ImpalaBackendClient : public ImpalaInternalServiceClient {
 public:
  ImpalaBackendClient(boost::shared_ptr< ::apache::thrift::protocol::TProtocol> prot)
    : ImpalaInternalServiceClient(prot), transmit_csw_(NULL) {
  }

  ImpalaBackendClient(boost::shared_ptr< ::apache::thrift::protocol::TProtocol> iprot,
      boost::shared_ptr< ::apache::thrift::protocol::TProtocol> oprot)
    : ImpalaInternalServiceClient(iprot, oprot), transmit_csw_(NULL) {
  }

  void TransmitData(TTransmitDataResult& _return, const TTransmitDataParams& params) {
    if (transmit_csw_ != NULL) {
      SCOPED_CONCURRENT_COUNTER(transmit_csw_);
      ImpalaInternalServiceClient::send_TransmitData(params);
    } else {
      ImpalaInternalServiceClient::send_TransmitData(params);
    }
    ImpalaInternalServiceClient::recv_TransmitData(_return);
  }

  /// Callers of TransmitData() should provide their own counter to measure the data
  /// transmission time.
  void SetTransmitDataCounter(RuntimeProfile::ConcurrentTimerCounter* csw) {
    DCHECK(transmit_csw_ == NULL);
    transmit_csw_ = csw;
  }

  /// ImpalaBackendClient is shared by multiple queries. It's the caller's responsibility
  /// to reset the counter after data transmission.
  void ResetTransmitDataCounter() {
    transmit_csw_ = NULL;
  }

 private:
  RuntimeProfile::ConcurrentTimerCounter* transmit_csw_;
};

}

#endif // IMPALA_BACKEND_CLIENT_H
