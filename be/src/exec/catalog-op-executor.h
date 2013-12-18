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


#ifndef IMPALA_EXEC_CATALOG_OP_EXECUTOR_H
#define IMPALA_EXEC_CATALOG_OP_EXECUTOR_H

#include <boost/scoped_ptr.hpp>
#include "gen-cpp/cli_service_types.h"
#include "gen-cpp/Frontend_types.h"
#include "runtime/client-cache.h"

namespace impala {

class Status;

// The CatalogOpExecutor is responsible for executing catalog operations.
// This includes DDL statements such as CREATE and ALTER as well as statements such
// as INVALIDATE METADATA. One CatalogOpExecutor is typically created per catalog
// operation.
class CatalogOpExecutor {
 public:
  CatalogOpExecutor(CatalogServiceClientCache* client_cache)
      : client_cache_(client_cache) {}

  // Executes the given catalog operation against the catalog server.
  Status Exec(const TCatalogOpRequest& catalog_op);

  // Fetches the metadata for the specific TCatalogObject descriptor from the catalog
  // server. If the catalog server does not have the object cached, its metadata will
  // be loaded.
  Status GetCatalogObject(const TCatalogObject& object_desc, TCatalogObject* result);

  // Translates the given compute stats params and its child-query results into
  // a new table alteration request for updating the stats metadata, and executes
  // the alteration via Exec();
  Status ExecComputeStats(const TComputeStatsParams& compute_stats_params,
      const apache::hive::service::cli::thrift::TTableSchema& tbl_stats_schema,
      const apache::hive::service::cli::thrift::TRowSet& tbl_stats_data,
      const apache::hive::service::cli::thrift::TTableSchema& col_stats_schema,
      const apache::hive::service::cli::thrift::TRowSet& col_stats_data);

  // Set in Exec(), returns a pointer to the TDdlExecResponse of the DDL execution.
  // If called before Exec(), this will return NULL. Only set if the
  // TCatalogOpType is DDL.
  const TDdlExecResponse* ddl_exec_response() const { return exec_response_.get(); }

  // Set in Exec(), for operations that execute using the CatalogServer. Returns
  // a pointer to the TCatalogUpdateResult of the operation. This includes details on
  // the Status of the operation, the CatalogService ID that processed the request,
  // and the minimum catalog version that will reflect this change.
  // If called before Exec(), this will return NULL.
  const TCatalogUpdateResult* update_catalog_result() const {
    return catalog_update_result_.get();
  }

 private:
  // Helper functions used in ExecComputeStats() for setting the thrift structs in params
  // for the table/column stats based on the results of the corresponding child query.
  static void SetTableStats(
      const apache::hive::service::cli::thrift::TTableSchema& tbl_stats_schema,
      const apache::hive::service::cli::thrift::TRowSet& tbl_stats_data,
      TAlterTableUpdateStatsParams* params);
  static void SetColumnStats(
      const apache::hive::service::cli::thrift::TTableSchema& col_stats_schema,
      const apache::hive::service::cli::thrift::TRowSet& col_stats_data,
      TAlterTableUpdateStatsParams* params);

  // Response from executing the DDL request, see ddl_exec_response().
  boost::scoped_ptr<TDdlExecResponse> exec_response_;

  // Result of executing a DDL request using the CatalogService
  boost::scoped_ptr<TCatalogUpdateResult> catalog_update_result_;

  // Client cache to use when making connections to the catalog service. Not owned by this
  // class.
  CatalogServiceClientCache* client_cache_;
};

}

#endif
