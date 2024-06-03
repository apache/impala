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

#ifndef IMPALA_SERVICE_DATA_STREAM_SERVICE_H
#define IMPALA_SERVICE_DATA_STREAM_SERVICE_H

#include "gen-cpp/common.pb.h"
#include "gen-cpp/data_stream_service.service.h"

#include "common/status.h"

namespace kudu {
namespace rpc {
class RpcContext;
} // namespace rpc
} // namespace kudu

namespace impala {

class DataStreamServiceProxy;
class MemTracker;
class MetricGroup;

/// This is singleton class which provides data transmission services between fragment
/// instances. The client for this service is implemented in KrpcDataStreamSender.
/// The processing of incoming requests is implemented in KrpcDataStreamRecvr.
/// KrpcDataStreamMgr is responsible for routing the incoming requests to the
/// appropriate receivers. Metrics exposed by the service will be added to 'metric_group'.
class DataStreamService : public DataStreamServiceIf {
 public:
  static constexpr std::string_view END_DATA_STREAM = "EndDataStream";

  DataStreamService(MetricGroup* metric_group);

  /// Initializes the service by registering it with the singleton RPC manager.
  /// This mustn't be called until RPC manager has been initialized.
  Status Init();

  /// Returns true iff the 'remote_user' in 'context' is authorized to access
  /// DataStreamService. On denied access, the RPC is replied to with an error message.
  /// Authorization is enforced only when Kerberos is enabled.
  virtual bool Authorize(const google::protobuf::Message* req,
      google::protobuf::Message* resp, kudu::rpc::RpcContext* context);

  /// Notifies the receiver to close the data stream specified in 'request'.
  /// The receiver replies to the client with a status serialized in 'response'.
  virtual void EndDataStream(const EndDataStreamRequestPB* request,
      EndDataStreamResponsePB* response, kudu::rpc::RpcContext* context);

  /// Sends a row batch to the receiver specified in 'request'.
  /// The receiver replies to the client with a status serialized in 'response'.
  virtual void TransmitData(const TransmitDataRequestPB* request,
      TransmitDataResponsePB* response, kudu::rpc::RpcContext* context);

  /// Called by fragment instances that produce local runtime filters to deliver them to
  /// the coordinator for aggregation and broadcast.
  virtual void UpdateFilter(const UpdateFilterParamsPB* req, UpdateFilterResultPB* resp,
      kudu::rpc::RpcContext* context);

  /// Called by fragment instances that produce local runtime filters to deliver them to
  /// the aggregator backend for intermediate aggregation.
  virtual void UpdateFilterFromRemote(const UpdateFilterParamsPB* req,
      UpdateFilterResultPB* resp, kudu::rpc::RpcContext* context);

  /// Called by the coordinator to deliver global runtime filters to fragments for
  /// application at plan nodes.
  virtual void PublishFilter(const PublishFilterParamsPB* req,
      PublishFilterResultPB* resp, kudu::rpc::RpcContext* context);

  /// Respond to a RPC passed in 'response'/'ctx' with 'status' and release
  /// the payload memory from 'mem_tracker'. Takes ownership of 'ctx'.
  template<typename ResponsePBType>
  static void RespondAndReleaseRpc(const Status& status, ResponsePBType* response,
      kudu::rpc::RpcContext* ctx, MemTracker* mem_tracker);

  /// Respond to a RPC passed in 'response'/'ctx' with 'status'. Takes ownership of 'ctx'.
  template<typename ResponsePBType>
  static void RespondRpc(const Status& status, ResponsePBType* response,
      kudu::rpc::RpcContext* ctx);

  MemTracker* mem_tracker() { return mem_tracker_.get(); }

  /// Gets a DataStreamService proxy to a server with 'address' and 'hostname'.
  /// The newly created proxy is returned in 'proxy'. Returns error status on failure.
  static Status GetProxy(const NetworkAddressPB& address, const std::string& hostname,
      std::unique_ptr<DataStreamServiceProxy>* proxy);

 private:
  /// Tracks the memory usage of the payloads in the service queue.
  std::unique_ptr<MemTracker> mem_tracker_;
};

} // namespace impala
#endif // IMPALA_SERVICE_DATA_STREAM_SERVICE_H
