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

#ifndef IMPALA_RPC_RPC_MGR_INLINE_H
#define IMPALA_RPC_RPC_MGR_INLINE_H

#include "rpc/rpc-mgr.h"

#include <gflags/gflags.h>

#include "exec/kudu/kudu-util.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/rpc/service_pool.h"
#include "kudu/rpc/user_credentials.h"
#include "runtime/exec-env.h"
#include "util/network-util.h"

DECLARE_bool(rpc_use_loopback);
namespace impala {

/// Always inline to avoid having to provide a definition for each use type P.
template <typename P>
Status RpcMgr::GetProxy(const NetworkAddressPB& address, const std::string& hostname,
    std::unique_ptr<P>* proxy) {
  DCHECK(proxy != nullptr);
  DCHECK(is_inited()) << "Must call Init() before GetProxy()";
  DCHECK(IsResolvedAddress(address));
  NetworkAddressPB address_to_use = address;
  // Talk to self via loopback.
  if (FLAGS_rpc_use_loopback
      && address_to_use.hostname() == ExecEnv::GetInstance()->krpc_address().hostname()) {
    address_to_use.set_hostname(LOCALHOST_IP_STR);
  }
  kudu::Sockaddr sockaddr = kudu::Sockaddr::Wildcard();
  // Connect to KRPC server via Unix domain socket if krpc_use_uds_ is true.
  RETURN_IF_ERROR(NetworkAddressPBToSockaddr(address_to_use, krpc_use_uds_, &sockaddr));
  proxy->reset(new P(messenger_, sockaddr, hostname));

  // Always set the user credentials as Proxy ctor may fail in GetLoggedInUser().
  // An empty user name will result in SASL failure. See IMPALA-7585.
  kudu::rpc::UserCredentials creds;
  creds.set_real_user("impala");
  (*proxy)->set_user_credentials(creds);

  return Status::OK();
}

template <typename Proxy, typename ProxyMethod, typename Request, typename Response>
Status RpcMgr::DoRpcWithRetry(const std::unique_ptr<Proxy>& proxy,
    const ProxyMethod& rpc_call, const Request& request, Response* response,
    const TQueryCtx& query_ctx, const char* error_msg, const int times_to_try,
    const int64_t timeout_ms, const int64_t server_busy_backoff_ms,
    const char* debug_action) {
  DCHECK_GT(times_to_try, 0);
  Status rpc_status;

  for (int i = 0; i < times_to_try; ++i) {
    kudu::rpc::RpcController rpc_controller;
    rpc_controller.set_timeout(kudu::MonoDelta::FromMilliseconds(timeout_ms));

    if (debug_action != nullptr) {
      // Check for injected failures.
      rpc_status = DebugAction(query_ctx.client_request.query_options, debug_action);
      if (!rpc_status.ok()) continue;
    }

    const kudu::Status& k_status =
        (proxy.get()->*rpc_call)(request, response, &rpc_controller);
    rpc_status = FromKuduStatus(k_status, error_msg);

    // If the call succeeded, or the server is not busy, then return result to caller.
    if (rpc_status.ok() || !RpcMgr::IsServerTooBusy(rpc_controller)) {
      return rpc_status;
    }

    // Server is busy, sleep if caller requested it and this is not the last time to try.
    if (server_busy_backoff_ms != 0 && i != times_to_try - 1) {
      SleepForMs(server_busy_backoff_ms);
    }
  }
  return rpc_status;
}

} // namespace impala

#endif
