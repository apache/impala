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

#ifndef IMPALA_SERVICE_CONTROL_SERVICE_H
#define IMPALA_SERVICE_CONTROL_SERVICE_H

#include "gen-cpp/common.pb.h"
#include "gen-cpp/control_service.service.h"

#include "kudu/rpc/rpc_context.h"
#include "kudu/rpc/rpc_controller.h"

#include "common/status.h"

using kudu::MonoDelta;
using kudu::rpc::RpcContext;
using kudu::rpc::RpcController;

namespace kudu {
namespace rpc {
class RpcContext;
} // namespace rpc
} // namespace kudu

namespace impala {

class ClientRequestState;
class ControlServiceProxy;
class MemTracker;
class MetricGroup;
class QueryExecMgr;
class TRuntimeProfileTree;

/// This singleton class implements service for managing execution of queries in Impala.
class ControlService : public ControlServiceIf {
 public:
  ControlService(MetricGroup* metric_group);

  /// Initializes the service by registering it with the singleton RPC manager.
  /// This mustn't be called until RPC manager has been initialized.
  Status Init();

  /// Returns true iff the 'remote_user' in 'context' is authorized to access
  /// ControlService. On denied access, the RPC is replied to with an error message.
  /// Authorization is enforced only when Kerberos is enabled.
  virtual bool Authorize(const google::protobuf::Message* req,
      google::protobuf::Message* resp, kudu::rpc::RpcContext* rpc_context) override;

  /// Starts execution of a query's fragment instances on a backend.
  virtual void ExecQueryFInstances(const ExecQueryFInstancesRequestPB* req,
      ExecQueryFInstancesResponsePB* resp, kudu::rpc::RpcContext* context) override;

  /// Updates the coordinator with the query status of the backend encoded in 'req'.
  virtual void ReportExecStatus(const ReportExecStatusRequestPB* req,
      ReportExecStatusResponsePB* resp, kudu::rpc::RpcContext* rpc_context) override;

  /// Cancel any executing fragment instances for the query id specified in 'req'.
  virtual void CancelQueryFInstances(const CancelQueryFInstancesRequestPB* req,
      CancelQueryFInstancesResponsePB* resp, ::kudu::rpc::RpcContext* context) override;

  /// Initiate shutdown.
  virtual void RemoteShutdown(const RemoteShutdownParamsPB* req,
      RemoteShutdownResultPB* response, ::kudu::rpc::RpcContext* context) override;

  /// Gets a ControlService proxy to a server with 'address' and 'hostname'.
  /// The newly created proxy is returned in 'proxy'. Returns error status on failure.
  static Status GetProxy(const NetworkAddressPB& address, const std::string& hostname,
      std::unique_ptr<ControlServiceProxy>* proxy);

 private:
  /// Tracks the memory usage of payload in the service queue.
  std::unique_ptr<MemTracker> mem_tracker_;

  /// Helper for deserializing runtime profile from the sidecar attached in the inbound
  /// call within 'rpc_context'. On success, returns the deserialized profile in
  /// 'thrift_profiles'. On failure, returns the error status;
  static Status GetProfile(const ReportExecStatusRequestPB& request,
      const ClientRequestState& request_state, kudu::rpc::RpcContext* rpc_context,
      TRuntimeProfileForest* thrift_profiles);

  /// Helper for serializing 'status' as part of 'response'. Also releases memory
  /// of the RPC payload previously accounted towards the internal memory tracker.
  template <typename ResponsePBType>
  void RespondAndReleaseRpc(
      const Status& status, ResponsePBType* response, kudu::rpc::RpcContext* rpc_context);
};

} // namespace impala

#endif // IMPALA_SERVICE_CONTROL_SERVICE_H
