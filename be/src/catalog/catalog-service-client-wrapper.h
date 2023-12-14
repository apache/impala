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

#ifndef CATALOG_CATALOG_SERVICE_CLIENT_WRAPPER_H
#define CATALOG_CATALOG_SERVICE_CLIENT_WRAPPER_H

#include "gen-cpp/CatalogService.h"

namespace impala {

class CatalogServiceClientWrapper : public CatalogServiceClient {
 public:
  CatalogServiceClientWrapper(
      std::shared_ptr<::apache::thrift::protocol::TProtocol> prot)
    : CatalogServiceClient(prot) {
  }

  CatalogServiceClientWrapper(
      std::shared_ptr<::apache::thrift::protocol::TProtocol> iprot,
      std::shared_ptr<::apache::thrift::protocol::TProtocol> oprot)
    : CatalogServiceClient(iprot, oprot) {
  }

/// We intentionally disable this clang warning as we intend to hide the
/// the same-named functions defined in the base class.
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Woverloaded-virtual"

  void ExecDdl(TDdlExecResponse& _return, const TDdlExecRequest& req, bool* send_done) {
    DCHECK(!*send_done);
    send_ExecDdl(req);
    *send_done = true;
    recv_ExecDdl(_return);
  }

  void GetCatalogObject(TGetCatalogObjectResponse& _return,
      const TGetCatalogObjectRequest& req, bool* send_done) {
    DCHECK(!*send_done);
    send_GetCatalogObject(req);
    *send_done = true;
    recv_GetCatalogObject(_return);
  }

  void GetPartialCatalogObject(TGetPartialCatalogObjectResponse& _return,
      const TGetPartialCatalogObjectRequest& req, bool* send_done) {
    DCHECK(!*send_done);
    send_GetPartialCatalogObject(req);
    *send_done = true;
    recv_GetPartialCatalogObject(_return);
  }

  void ResetMetadata(TResetMetadataResponse& _return, const TResetMetadataRequest& req,
      bool* send_done) {
    DCHECK(!*send_done);
    send_ResetMetadata(req);
    *send_done = true;
    recv_ResetMetadata(_return);
  }

  void UpdateCatalog(TUpdateCatalogResponse& _return, const TUpdateCatalogRequest& req,
      bool* send_done) {
    DCHECK(!*send_done);
    send_UpdateCatalog(req);
    *send_done = true;
    recv_UpdateCatalog(_return);
  }

  void GetFunctions(TGetFunctionsResponse& _return, const TGetFunctionsRequest& req,
      bool* send_done) {
    DCHECK(!*send_done);
    send_GetFunctions(req);
    *send_done = true;
    recv_GetFunctions(_return);
  }

  void PrioritizeLoad(TPrioritizeLoadResponse& _return, const TPrioritizeLoadRequest& req,
      bool* send_done) {
    DCHECK(!*send_done);
    send_PrioritizeLoad(req);
    *send_done = true;
    recv_PrioritizeLoad(_return);
  }

  void GetPartitionStats(TGetPartitionStatsResponse& _return,
      const TGetPartitionStatsRequest& req, bool* send_done) {
    DCHECK(!*send_done);
    send_GetPartitionStats(req);
    *send_done = true;
    recv_GetPartitionStats(_return);
  }

  void UpdateTableUsage(TUpdateTableUsageResponse& _return,
      const TUpdateTableUsageRequest& req, bool* send_done) {
    DCHECK(!*send_done);
    send_UpdateTableUsage(req);
    *send_done = true;
    recv_UpdateTableUsage(_return);
  }

  void GetNullPartitionName(TGetNullPartitionNameResponse& _return,
      const TGetNullPartitionNameRequest& req, bool* send_done) {
    DCHECK(!*send_done);
    send_GetNullPartitionName(req);
    *send_done = true;
    recv_GetNullPartitionName(_return);
  }

  void GetLatestCompactions(TGetLatestCompactionsResponse& _return,
      const TGetLatestCompactionsRequest& req, bool* send_done) {
    DCHECK(!*send_done);
    send_GetLatestCompactions(req);
    *send_done = true;
    recv_GetLatestCompactions(_return);
  }
#pragma clang diagnostic pop
};

}

#endif
