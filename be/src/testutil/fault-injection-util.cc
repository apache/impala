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

#include <random>

#include <thrift/transport/TSSLSocket.h>
#include <thrift/transport/TTransportException.h>

#include "common/atomic.h"

#include "common/names.h"

DECLARE_int32(fault_injection_rpc_exception_type);

namespace impala {

using apache::thrift::transport::TTransportException;
using apache::thrift::transport::TSSLException;

void FaultInjectionUtil::InjectRpcException(bool is_send, int freq) {
  static AtomicInt32 send_count(-1);
  static AtomicInt32 recv_count(-1);
  int32_t xcp_type = FLAGS_fault_injection_rpc_exception_type;
  if (xcp_type == RPC_EXCEPTION_NONE) return;

  // We currently support injecting exception at some RPCs only.
  if (is_send) {
    if (send_count.Add(1) % freq == 0) {
      switch (xcp_type) {
        case RPC_EXCEPTION_SEND_CLOSED_CONNECTION:
          throw TTransportException(TTransportException::NOT_OPEN,
              "Called write on non-open socket");
        case RPC_EXCEPTION_SEND_TIMEDOUT:
          throw TTransportException(TTransportException::TIMED_OUT,
              "send timeout expired");
        case RPC_EXCEPTION_SSL_SEND_CLOSED_CONNECTION:
          throw TTransportException(TTransportException::NOT_OPEN);
        case RPC_EXCEPTION_SSL_SEND_TIMEDOUT:
          throw TSSLException("SSL_write: Resource temporarily unavailable");
        // Simulate half-opened connections.
        case RPC_EXCEPTION_SEND_STALE_CONNECTION:
          throw TTransportException(TTransportException::END_OF_FILE,
             "No more data to read.");
        case RPC_EXCEPTION_SSL_SEND_STALE_CONNECTION:
          throw TSSLException("SSL_read: Connection reset by peer");
        // fall through for the default case.
      }
    }
  } else {
    if (recv_count.Add(1) % freq == 0) {
      switch (xcp_type) {
        case RPC_EXCEPTION_RECV_CLOSED_CONNECTION:
          throw TTransportException(TTransportException::NOT_OPEN,
              "Called read on non-open socket");
        case RPC_EXCEPTION_RECV_TIMEDOUT:
          throw TTransportException(TTransportException::TIMED_OUT,
              "EAGAIN (timed out)");
        case RPC_EXCEPTION_SSL_RECV_CLOSED_CONNECTION:
          throw TTransportException(TTransportException::NOT_OPEN);
        case RPC_EXCEPTION_SSL_RECV_TIMEDOUT:
          throw TSSLException("SSL_read: Resource temporarily unavailable");
        // fall through for the default case.
      }
    }
  }
}

}

#endif
