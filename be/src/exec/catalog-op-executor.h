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


#ifndef IMPALA_EXEC_CATALOG_OP_EXECUTOR_H
#define IMPALA_EXEC_CATALOG_OP_EXECUTOR_H

#include <boost/scoped_ptr.hpp>
#include "gen-cpp/TCLIService_types.h"
#include "gen-cpp/Frontend_types.h"

namespace impala {

class ExecEnv;
class Frontend;
class Status;
class RuntimeProfile;

class TGetPartialCatalogObjectRequest;
class TGetPartialCatalogObjectResponse;

/// The CatalogOpExecutor is responsible for executing catalog operations.
/// This includes DDL statements such as CREATE and ALTER as well as statements such
/// as INVALIDATE METADATA. One CatalogOpExecutor is typically created per catalog
/// operation.
class CatalogOpExecutor {
 public:
  CatalogOpExecutor(ExecEnv* env, Frontend* fe, RuntimeProfile* profile)
      : env_(env), fe_(fe), profile_(profile) {}

  /// Executes the given catalog operation against the catalog server.
  Status Exec(const TCatalogOpRequest& catalog_op);

  /// Fetches the metadata for the specific TCatalogObject descriptor from the catalog
  /// server. If the catalog server does not have the object cached, its metadata will
  /// be loaded.
  Status GetCatalogObject(const TCatalogObject& object_desc, TCatalogObject* result);

  /// Fetch partial information about a specific TCatalogObject from the catalog server.
  Status GetPartialCatalogObject(const TGetPartialCatalogObjectRequest& req,
      TGetPartialCatalogObjectResponse* resp);

  /// Translates the given compute stats request and its child-query results into
  /// a new table alteration request for updating the stats metadata, and executes
  /// the alteration via Exec();
  Status ExecComputeStats(const TCatalogServiceRequestHeader& header,
      const TCatalogOpRequest& compute_stats_request,
      const apache::hive::service::cli::thrift::TTableSchema& tbl_stats_schema,
      const apache::hive::service::cli::thrift::TRowSet& tbl_stats_data,
      const apache::hive::service::cli::thrift::TTableSchema& col_stats_schema,
      const apache::hive::service::cli::thrift::TRowSet& col_stats_data);

  /// Makes an RPC to the CatalogServer to prioritize the loading of the catalog objects
  /// specified in the TPrioritizeLoadRequest. Returns OK if the RPC was successful,
  /// otherwise a bad status will be returned.
  Status PrioritizeLoad(const TPrioritizeLoadRequest& req,
      TPrioritizeLoadResponse* result);

  /// Makes an RPC to the CatalogServer to fetch the partition statistics of the
  /// partitions that are specified in TGetPartitionStatsRequest.
  Status GetPartitionStats(
      const TGetPartitionStatsRequest& req, TGetPartitionStatsResponse* result);

  /// Makes an RPC to the catalog server to report recently used tables and their use
  /// counts in this impalad since the last report.
  Status UpdateTableUsage(const TUpdateTableUsageRequest& req,
      TUpdateTableUsageResponse* resp);

  /// Makes an RPC to the catalog server to get the null partition name.
  Status GetNullPartitionName(
      const TGetNullPartitionNameRequest& req, TGetNullPartitionNameResponse* result);

  /// Makes an RPC to the catalog server to get the latest compactions.
  Status GetLatestCompactions(
      const TGetLatestCompactionsRequest& req, TGetLatestCompactionsResponse* result);

  /// Set in Exec(), returns a pointer to the TDdlExecResponse of the DDL execution.
  /// If called before Exec(), this will return NULL. Only set if the
  /// TCatalogOpType is DDL.
  const TDdlExecResponse* ddl_exec_response() const { return exec_response_.get(); }

  /// Set in Exec(), for operations that execute using the CatalogServer. Returns
  /// a pointer to the TCatalogUpdateResult of the operation. This includes details on
  /// the Status of the operation, the CatalogService ID that processed the request,
  /// and the minimum catalog version that will reflect this change.
  /// If called before Exec(), this will return NULL.
  const TCatalogUpdateResult* update_catalog_result() const {
    return catalog_update_result_.get();
  }

  /// Set in Exec(), for operations that are executed using the CatalogServer. Returns
  /// a pointer to the profile of the execution in catalogd.
  const TRuntimeProfileNode* catalog_profile() const { return catalog_profile_.get(); }

 private:
  /// Helper functions used in ExecComputeStats() for setting the thrift structs in params
  /// for the table/column stats based on the results of the corresponding child query.
  static void SetTableStats(
      const apache::hive::service::cli::thrift::TTableSchema& tbl_stats_schema,
      const apache::hive::service::cli::thrift::TRowSet& tbl_stats_data,
      const std::vector<TPartitionStats>& existing_part_stats,
      TAlterTableUpdateStatsParams* params);
  static void SetColumnStats(
      const apache::hive::service::cli::thrift::TTableSchema& col_stats_schema,
      const apache::hive::service::cli::thrift::TRowSet& col_stats_data,
      TAlterTableUpdateStatsParams* params);

  /// Response from executing the DDL request, see ddl_exec_response().
  boost::scoped_ptr<TDdlExecResponse> exec_response_;

  /// Result of executing a DDL request using the CatalogService
  boost::scoped_ptr<TCatalogUpdateResult> catalog_update_result_;

  /// Profile of the execution on Catalog Server side
  std::unique_ptr<TRuntimeProfileNode> catalog_profile_;

  ExecEnv* env_;
  Frontend* fe_;
  RuntimeProfile* profile_;

  /// Handles additional BE work that needs to be done for drop function and data source,
  /// in particular, clearing the local library cache for this function.
  void HandleDropFunction(const TDropFunctionParams&);
  void HandleDropDataSource(const TDropDataSourceParams&);
};

}

#endif
