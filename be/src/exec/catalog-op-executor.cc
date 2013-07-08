// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <sstream>

#include "exec/catalog-op-executor.h"
#include "common/status.h"
#include "service/impala-server.h"

#include "gen-cpp/CatalogService.h"
#include "gen-cpp/CatalogService_types.h"

using namespace std;
using namespace impala;

DECLARE_int32(catalog_service_port);
DECLARE_string(catalog_service_host);

Status CatalogOpExecutor::Exec(const TCatalogOpRequest& request) {
  ThriftClient<CatalogServiceClient> client(FLAGS_catalog_service_host,
      FLAGS_catalog_service_port, ThriftServer::ThreadPool);
  switch (request.op_type) {
    case TCatalogOpType::DDL: {
      RETURN_IF_ERROR(client.Open());
      catalog_update_result_.reset(new TCatalogUpdateResult());
      exec_response_.reset(new TDdlExecResponse());
      client.iface()->ExecDdl(*exec_response_.get(), request.ddl_params);
      catalog_update_result_.reset(
          new TCatalogUpdateResult(exec_response_.get()->result));
      return Status(exec_response_->result.status);
    }
    case TCatalogOpType::RESET_METADATA: {
      ThriftClient<CatalogServiceClient> client(FLAGS_catalog_service_host,
          FLAGS_catalog_service_port, ThriftServer::ThreadPool);
      TResetMetadataResponse response;
      catalog_update_result_.reset(new TCatalogUpdateResult());
      RETURN_IF_ERROR(client.Open());
      client.iface()->ResetMetadata(response, request.reset_metadata_params);
      catalog_update_result_.reset(new TCatalogUpdateResult(response.result));
      return Status(response.result.status);
    }
    default: {
      stringstream ss;
      ss << "TCatalogOpType: " << request.op_type << " does not support execution "
         << "against the CatalogService.";
      return Status(ss.str());
    }
  }
}
