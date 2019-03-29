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

#ifndef IMPALA_TESTUTIL_FAULT_INJECTION_UTIL_H
#define IMPALA_TESTUTIL_FAULT_INJECTION_UTIL_H

#include "util/time.h"

namespace impala {

#ifndef NDEBUG

class FaultInjectionUtil {
 public:

  enum RpcExceptionType {
    RPC_EXCEPTION_NONE = 0,
    RPC_EXCEPTION_SEND_CLOSED_CONNECTION,
    RPC_EXCEPTION_SEND_STALE_CONNECTION,
    RPC_EXCEPTION_SEND_TIMEDOUT,
    RPC_EXCEPTION_RECV_CLOSED_CONNECTION,
    RPC_EXCEPTION_RECV_TIMEDOUT,
    RPC_EXCEPTION_SSL_SEND_CLOSED_CONNECTION,
    RPC_EXCEPTION_SSL_SEND_STALE_CONNECTION,
    RPC_EXCEPTION_SSL_SEND_TIMEDOUT,
    RPC_EXCEPTION_SSL_RECV_CLOSED_CONNECTION,
    RPC_EXCEPTION_SSL_RECV_TIMEDOUT,
  };

  /// Test util function that injects exceptions to RPC client functions.
  /// 'is_send' indicates whether injected fault is at the send() or recv() of an RPC.
  /// The exception specified in 'FLAGS_fault_injection_rpc_exception_type' is injected
  /// on every 'freq' invocations of this function.
  static void InjectRpcException(bool is_send, int freq);

};

#define FAULT_INJECTION_SEND_RPC_EXCEPTION(freq)                 \
    FaultInjectionUtil::InjectRpcException(true, freq)
#define FAULT_INJECTION_RECV_RPC_EXCEPTION(freq)                 \
    FaultInjectionUtil::InjectRpcException(false, freq)

#else // NDEBUG

#define FAULT_INJECTION_SEND_RPC_EXCEPTION(freq)
#define FAULT_INJECTION_RECV_RPC_EXCEPTION(freq)

#endif

}
#endif // IMPALA_TESTUTIL_FAULT_INJECTION_UTIL_H
