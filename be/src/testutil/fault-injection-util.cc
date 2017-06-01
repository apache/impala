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

#ifndef NDEBUG

#include "testutil/fault-injection-util.h"

#include <thrift/transport/TSSLSocket.h>
#include <thrift/transport/TTransportException.h>

#include "common/atomic.h"

#include "common/names.h"

DECLARE_int32(fault_injection_rpc_delay_ms);
DECLARE_int32(fault_injection_rpc_type);
DECLARE_int32(fault_injection_rpc_exception_type);

namespace impala {

using apache::thrift::transport::TTransportException;
using apache::thrift::transport::TSSLException;

int32_t FaultInjectionUtil::GetTargetRPCType() {
  int32_t target_rpc_type = FLAGS_fault_injection_rpc_type;
  if (target_rpc_type == RPC_RANDOM) target_rpc_type = rand() % RPC_RANDOM;
  DCHECK_LT(target_rpc_type, RPC_RANDOM);
  return target_rpc_type;
}

void FaultInjectionUtil::InjectRpcDelay(RpcCallType my_type) {
  std::random_device rd;
  srand(rd());
  int32_t delay_ms = FLAGS_fault_injection_rpc_delay_ms;
  if (delay_ms == 0) return;
  int32_t target_rpc_type = GetTargetRPCType();
  if (target_rpc_type == my_type) SleepForMs(delay_ms);
}

void FaultInjectionUtil::InjectRpcException(RpcCallType my_type, bool is_send) {
  static AtomicInt32 send_count(-1);
  static AtomicInt32 recv_count(-1);
  int32_t xcp_type = FLAGS_fault_injection_rpc_exception_type;
  if (xcp_type == RPC_EXCEPTION_NONE) return;

  // We currently support injecting exception at TransmitData() RPC only.
  int32_t target_rpc_type = GetTargetRPCType();
  DCHECK_EQ(target_rpc_type, RPC_TRANSMITDATA);

  if (is_send) {
    int32_t count = send_count.Add(1);
    if (count % 1024 == 0) {
      if (xcp_type == RPC_EXCEPTION_LOST_CONNECTION_SEND) {
        // Test both exception types which are considered recoverable
        // by IsSendFailTException().
        if ((count / 1024) % 2 == 0) {
          throw TTransportException(TTransportException::NOT_OPEN,
              "Called write on non-open socket");
        } else {
          throw TTransportException(TTransportException::TIMED_OUT,
              "send timeout expired");
        }
      } else if (xcp_type == RPC_EXCEPTION_SSL_ERROR_SEND) {
        throw TSSLException("SSL_write: SSL resource temporarily unavailable");
      }
    }
  } else {
    if (recv_count.Add(1) % 1024 == 0) {
      if (xcp_type == RPC_EXCEPTION_LOST_CONNECTION_RECV) {
        throw TTransportException(TTransportException::NOT_OPEN,
            "Called read on non-open socket");
      } else if (xcp_type == RPC_EXCEPTION_SSL_ERROR_RECV) {
        throw TSSLException("SSL_read: SSL resource temporarily unavailable");
      }
    }
  }
}

}

#endif
