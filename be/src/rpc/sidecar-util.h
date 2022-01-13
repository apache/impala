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

#pragma once

#include "common/status.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_sidecar.h"
#include "kudu/util/faststring.h"
#include "rpc/thrift-util.h"
#include "util/kudu-status-util.h"

DECLARE_int64(rpc_max_message_size);

namespace impala {

class KrpcSerializer {
 public:
  KrpcSerializer() : serializer_(/* compact */ true) {}

  /// Serialize obj and set it as a sidecar on 'rpc_controller', returning the idx in
  /// 'sidecar_idx'. The memory for the sidecar is owned by this object and must remain
  /// valid until the rpc has completed.
  template <class T>
  Status SerializeToSidecar(
      const T* obj, kudu::rpc::RpcController* rpc_controller, int* sidecar_idx) {
    uint8_t* serialized_buf = nullptr;
    uint32_t serialized_len = 0;
    RETURN_IF_ERROR(serializer_.SerializeToBuffer(obj, &serialized_len, &serialized_buf));
    std::unique_ptr<kudu::rpc::RpcSidecar> rpc_sidecar =
        kudu::rpc::RpcSidecar::FromSlice(kudu::Slice(serialized_buf, serialized_len));
    KUDU_RETURN_IF_ERROR(
        rpc_controller->AddOutboundSidecar(move(rpc_sidecar), sidecar_idx),
        "Failed to add sidecar");
    return Status::OK();
  }

 private:
  ThriftSerializer serializer_;
};

// Retrieves the sidecar at 'sidecar_idx' from 'rpc_context' and deserializes it into
// 'thrift_obj'. 'rpc' can be either an RpcContext or an RpcController.
template <typename RPC, typename T>
Status GetSidecar(int sidecar_idx, RPC* rpc, T* thrift_obj) {
  kudu::Slice sidecar_slice;
  KUDU_RETURN_IF_ERROR(
      rpc->GetInboundSidecar(sidecar_idx, &sidecar_slice), "Failed to get sidecar");
  uint32_t len = sidecar_slice.size();
  RETURN_IF_ERROR(DeserializeThriftMsg(sidecar_slice.data(), &len, true, thrift_obj));
  return Status::OK();
}

// Serializes 'obj' and sets is as a sidecar on 'rpc' using a faststring, which transfers
// ownership of the sidecar memory to the RPC layer. This introduces another copy that
// wouldn't be necessary if using a Slice sidecar, but it's convenient for situations
// where it would be tricky to manually ensure that the sidecar's memory is released after
// the RPC has completed. 'rpc' may be either an RpcContext or an RpcController.
//
// If successful, the sidecar idx is returned in 'sidecar_idx'.
template <typename RPC, typename T>
Status SetFaststringSidecar(const T& obj, RPC* rpc, int* sidecar_idx) {
  ThriftSerializer serializer(/* compact */ true);
  uint8_t* serialized_buf = nullptr;
  uint32_t serialized_len = 0;
  RETURN_IF_ERROR(serializer.SerializeToBuffer(&obj, &serialized_len, &serialized_buf));
  if (serialized_len > FLAGS_rpc_max_message_size) {
    return Status("Serialized sidecar exceeds --rpc_max_message_size.");
  }
  kudu::faststring sidecar_str;
  sidecar_str.assign_copy(serialized_buf, serialized_len);
  std::unique_ptr<kudu::rpc::RpcSidecar> rpc_sidecar =
      kudu::rpc::RpcSidecar::FromFaststring(std::move(sidecar_str));
  RETURN_IF_ERROR(FromKuduStatus(
      rpc->AddOutboundSidecar(move(rpc_sidecar), sidecar_idx), "Failed to add sidecar"));
  return Status::OK();
}

} // namespace impala
