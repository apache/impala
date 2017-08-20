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

#include "exec/kudu-util.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/rpc/service_pool.h"
#include "util/network-util.h"

namespace impala {

/// Always inline to avoid having to provide a definition for each use type P.
template <typename P>
Status RpcMgr::GetProxy(const TNetworkAddress& address, std::unique_ptr<P>* proxy) {
  DCHECK(proxy != nullptr);
  DCHECK(is_inited()) << "Must call Init() before GetProxy()";
  DCHECK(IsResolvedAddress(address));
  kudu::Sockaddr sockaddr;
  RETURN_IF_ERROR(TNetworkAddressToSockaddr(address, &sockaddr));
  proxy->reset(new P(messenger_, sockaddr, address.hostname));
  return Status::OK();
}

} // namespace impala

#endif
