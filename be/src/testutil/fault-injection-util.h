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

#ifndef NDEBUG
  DECLARE_int32(fault_injection_rpc_delay_ms);
  DECLARE_int32(fault_injection_rpc_type);
#endif

namespace impala {

#ifndef NDEBUG
  enum RpcCallType {
    RPC_NULL = 0,
    RPC_EXECPLANFRAGMENT,
    RPC_CANCELPLANFRAGMENT,
    RPC_PUBLISHFILTER,
    RPC_UPDATEFILTER,
    RPC_TRANSMITDATA,
    RPC_REPORTEXECSTATUS,
    RPC_RANDOM    // This must be last.
  };

  /// Test util function that can inject delay to specified RPC server handling
  /// function so that RPC caller could hit the RPC recv timeout condition.
  /// my_type specifies which RPC type the current function is.
  /// rpc_type specifies which RPC function the delay should be enabled.
  /// delay_ms specifies how long the delay should be.
  static void InjectRpcDelay(RpcCallType my_type, int32_t rpc_type, int32_t delay_ms) {
    std::random_device rd;
    srand(rd());
    if (delay_ms == 0) return;
    if (rpc_type == RPC_RANDOM) rpc_type = rand() % RPC_RANDOM;
    if (rpc_type == my_type) SleepForMs(delay_ms);
  }

  #define FAULT_INJECTION_RPC_DELAY(type) InjectRpcDelay(type, \
      FLAGS_fault_injection_rpc_type, FLAGS_fault_injection_rpc_delay_ms)
#else
  #define FAULT_INJECTION_RPC_DELAY(type)
#endif

}
#endif // IMPALA_TESTUTIL_FAULT_INJECTION_UTIL_H
