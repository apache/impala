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

#include "exec/catalog-op-executor.h"

#include <sstream>

#include "common/status.h"
#include "catalog/catalog-service-client-wrapper.h"
#include "exec/incr-stats-util.h"
#include "runtime/lib-cache.h"
#include "runtime/client-cache-types.h"
#include "runtime/exec-env.h"
#include "service/frontend.h"
#include "service/impala-server.h"
#include "service/hs2-util.h"
#include "util/runtime-profile-counters.h"
#include "util/string-parser.h"
#include "util/test-info.h"
#include "util/time.h"
#include "gen-cpp/CatalogService.h"
#include "gen-cpp/CatalogService_types.h"
#include "gen-cpp/CatalogObjects_types.h"

#include <thrift/protocol/TDebugProtocol.h>
#include <thrift/Thrift.h>
#include <gutil/strings/substitute.h>

#include "common/names.h"
using namespace impala;
using namespace apache::hive::service::cli::thrift;
using namespace apache::thrift;

DECLARE_bool(use_local_catalog);
DECLARE_int32(catalog_service_port);
DECLARE_string(catalog_service_host);

DEFINE_int32_hidden(inject_latency_after_catalog_fetch_ms, 0,
    "Latency (ms) to be injected after fetching catalog data from the catalogd");

Status CatalogOpExecutor::Exec(const TCatalogOpRequest& request) {
  Status status;
  DCHECK(profile_ != NULL);
  RuntimeProfile::Counter* exec_timer = ADD_TIMER(profile_, "CatalogOpExecTimer");
  SCOPED_TIMER(exec_timer);
  const TNetworkAddress& address =
      MakeNetworkAddress(FLAGS_catalog_service_host, FLAGS_catalog_service_port);
  CatalogServiceConnection client(env_->catalogd_client_cache(), address, &status);
  RETURN_IF_ERROR(status);
  switch (request.op_type) {
    case TCatalogOpType::DDL: {
      // Compute stats stmts must be executed via ExecComputeStats().
      DCHECK(request.ddl_params.ddl_type != TDdlType::COMPUTE_STATS);

      exec_response_.reset(new TDdlExecResponse());
      RETURN_IF_ERROR(client.DoRpc(&CatalogServiceClientWrapper::ExecDdl,
          request.ddl_params, exec_response_.get()));
      catalog_update_result_.reset(
          new TCatalogUpdateResult(exec_response_.get()->result));
      Status status(exec_response_->result.status);
      if (status.ok()) {
        if (request.ddl_params.ddl_type == TDdlType::DROP_FUNCTION) {
          HandleDropFunction(request.ddl_params.drop_fn_params);
        } else if (request.ddl_params.ddl_type == TDdlType::DROP_DATA_SOURCE) {
          HandleDropDataSource(request.ddl_params.drop_data_source_params);
        }
      }
      return status;
    }
    case TCatalogOpType::RESET_METADATA: {
      TResetMetadataResponse response;
      RETURN_IF_ERROR(client.DoRpc(&CatalogServiceClientWrapper::ResetMetadata,
          request.reset_metadata_params, &response));
      catalog_update_result_.reset(new TCatalogUpdateResult(response.result));
      return Status(response.result.status);
    }
    default: {
      return Status(Substitute("TCatalogOpType: $0 does not support execution against the"
          " CatalogService", request.op_type));
    }
  }
}

Status CatalogOpExecutor::ExecComputeStats(
    const TCatalogOpRequest& compute_stats_request,
    const TTableSchema& tbl_stats_schema, const TRowSet& tbl_stats_data,
    const TTableSchema& col_stats_schema, const TRowSet& col_stats_data) {
  // Create a new DDL request to alter the table's statistics.
  TCatalogOpRequest catalog_op_req;
  catalog_op_req.__isset.ddl_params = true;
  catalog_op_req.__set_op_type(TCatalogOpType::DDL);
  catalog_op_req.__set_sync_ddl(compute_stats_request.sync_ddl);
  TDdlExecRequest& update_stats_req = catalog_op_req.ddl_params;
  update_stats_req.__set_ddl_type(TDdlType::ALTER_TABLE);
  update_stats_req.__set_sync_ddl(compute_stats_request.sync_ddl);

  const TComputeStatsParams& compute_stats_params =
      compute_stats_request.ddl_params.compute_stats_params;
  TAlterTableUpdateStatsParams& update_stats_params =
      update_stats_req.alter_table_params.update_stats_params;
  update_stats_req.__isset.alter_table_params = true;
  update_stats_req.alter_table_params.__set_alter_type(TAlterTableType::UPDATE_STATS);
  update_stats_req.alter_table_params.__set_table_name(compute_stats_params.table_name);
  update_stats_req.alter_table_params.__isset.update_stats_params = true;
  update_stats_params.__set_table_name(compute_stats_params.table_name);
  update_stats_params.__set_expect_all_partitions(
      compute_stats_params.expect_all_partitions);
  update_stats_params.__set_is_incremental(compute_stats_params.is_incremental);

  // Fill the alteration request based on the child-query results.
  SetTableStats(tbl_stats_schema, tbl_stats_data,
      compute_stats_params.existing_part_stats, &update_stats_params);
  if (compute_stats_params.__isset.total_file_bytes) {
    update_stats_params.table_stats.__set_total_file_bytes(
        compute_stats_params.total_file_bytes);
  }
  // col_stats_schema and col_stats_data will be empty if there was no column stats query.
  if (!col_stats_schema.columns.empty()) {
    if (compute_stats_params.is_incremental) {
      RuntimeProfile::Counter* incremental_finalize_timer =
          ADD_TIMER(profile_, "FinalizeIncrementalStatsTimer");
      SCOPED_TIMER(incremental_finalize_timer);
      FinalizePartitionedColumnStats(col_stats_schema,
          compute_stats_params.existing_part_stats,
          compute_stats_params.expected_partitions,
          col_stats_data, compute_stats_params.num_partition_cols, &update_stats_params);
    } else {
      SetColumnStats(col_stats_schema, col_stats_data, &update_stats_params);
    }
  }

  // Execute the 'alter table update stats' request.
  RETURN_IF_ERROR(Exec(catalog_op_req));
  return Status::OK();
}

void CatalogOpExecutor::HandleDropFunction(const TDropFunctionParams& request) {
  DCHECK(fe_ != NULL) << "FE tests should not be calling this";
  // Can only be called after successfully processing a catalog update.
  DCHECK(catalog_update_result_ != NULL);

  // Lookup in the local catalog the metadata for the function.
  TCatalogObject obj;
  obj.type = TCatalogObjectType::FUNCTION;
  obj.fn.name = request.fn_name;
  obj.fn.arg_types = request.arg_types;
  obj.fn.signature = request.signature;
  obj.__isset.fn = true;
  obj.fn.__isset.signature = true;

  TCatalogObject fn;
  Status status = fe_->GetCatalogObject(obj, &fn);
  if (!status.ok()) {
    // This can happen if the function was dropped by another impalad.
    VLOG_QUERY << "Could not lookup function catalog object: "
               << apache::thrift::ThriftDebugString(request);
    return;
  }
  // This function may have been dropped and re-created. To avoid removing the re-created
  // function's entry from the cache verify the existing function has a catalog
  // version <= the dropped version. This may happen if the update from the statestore
  // gets applied *before* the result of a direct-DDL drop function command.
  if (fn.catalog_version <= catalog_update_result_->version) {
    LibCache::instance()->RemoveEntry(fn.fn.hdfs_location);
  }
}

void CatalogOpExecutor::HandleDropDataSource(const TDropDataSourceParams& request) {
  DCHECK(fe_ != NULL) << "FE tests should not be calling this";
  // Can only be called after successfully processing a catalog update.
  DCHECK(catalog_update_result_ != NULL);

  // Lookup in the local catalog the metadata for the data source.
  TCatalogObject obj;
  obj.type = TCatalogObjectType::DATA_SOURCE;
  obj.data_source.name = request.data_source;
  obj.__isset.data_source = true;

  TCatalogObject ds;
  Status status = fe_->GetCatalogObject(obj, &ds);
  if (!status.ok()) {
    // This can happen if the data source was dropped by another impalad.
    VLOG_QUERY << "Could not lookup data source catalog object: "
               << apache::thrift::ThriftDebugString(request);
    return;
  }
  // This data source may have been dropped and re-created. To avoid removing the
  // re-created data source's entry from the cache verify the existing data source has a
  // catalog version <= the dropped version. This may happen if the update from the
  // statestore gets applied *before* the result of a direct-DDL drop data source
  // command.
  if (ds.catalog_version <= catalog_update_result_->version) {
    LibCache::instance()->RemoveEntry(ds.data_source.hdfs_location);
  }
}

void CatalogOpExecutor::SetTableStats(const TTableSchema& tbl_stats_schema,
    const TRowSet& tbl_stats_data, const vector<TPartitionStats>& existing_part_stats,
    TAlterTableUpdateStatsParams* params) {
  if (tbl_stats_data.rows.size() == 1 && tbl_stats_data.rows[0].colVals.size() == 1) {
    // Unpartitioned table. Only set table stats, but no partition stats.
    // The first column is the COUNT(*) expr of the original query.
    params->table_stats.__set_num_rows(tbl_stats_data.rows[0].colVals[0].i64Val.value);
    params->__isset.table_stats = true;
    return;
  }

  // Accumulate total number of rows in the partitioned table.
  long total_num_rows = 0;
  // Set per-partition stats.
  for (const TRow& row: tbl_stats_data.rows) {
    DCHECK_GT(row.colVals.size(), 0);
    // The first column is the COUNT(*) expr of the original query.
    DCHECK(row.colVals[0].__isset.i64Val);
    int64_t num_rows = row.colVals[0].i64Val.value;
    // The remaining columns are partition columns that the results are grouped by.
    vector<string> partition_key_vals;
    partition_key_vals.reserve(row.colVals.size());
    for (int j = 1; j < row.colVals.size(); ++j) {
      stringstream ss;
      PrintTColumnValue(row.colVals[j], &ss);
      partition_key_vals.push_back(ss.str());
    }
    params->partition_stats[partition_key_vals].stats.__set_num_rows(num_rows);
    total_num_rows += num_rows;
  }
  params->__isset.partition_stats = true;

  // Add row counts of existing partitions that are not going to be modified.
  for (const TPartitionStats& existing_stats: existing_part_stats) {
    total_num_rows += existing_stats.stats.num_rows;
  }

  // Set per-table stats.
  params->table_stats.__set_num_rows(total_num_rows);
  params->__isset.table_stats = true;
}

void CatalogOpExecutor::SetColumnStats(const TTableSchema& col_stats_schema,
    const TRowSet& col_stats_data, TAlterTableUpdateStatsParams* params) {
  // Expect exactly one result row.
  DCHECK_EQ(1, col_stats_data.rows.size());
  const TRow& col_stats_row = col_stats_data.rows[0];

  // Set per-column stats. For a column at position i in its source table,
  // the NDVs and the number of NULLs are at position i and i + 1 of the
  // col_stats_row, respectively. Positions i + 2 and i + 3 contain the max/avg
  // length for string columns, and -1 for non-string columns.
  for (int i = 0; i < col_stats_row.colVals.size(); i += 4) {
    TColumnStats col_stats;
    col_stats.__set_num_distinct_values(col_stats_row.colVals[i].i64Val.value);
    col_stats.__set_num_nulls(col_stats_row.colVals[i + 1].i64Val.value);
    col_stats.__set_max_size(col_stats_row.colVals[i + 2].i32Val.value);
    col_stats.__set_avg_size(col_stats_row.colVals[i + 3].doubleVal.value);
    params->column_stats[col_stats_schema.columns[i].columnName] = col_stats;
  }
  params->__isset.column_stats = true;
}

Status CatalogOpExecutor::GetCatalogObject(const TCatalogObject& object_desc,
    TCatalogObject* result) {
  const TNetworkAddress& address =
      MakeNetworkAddress(FLAGS_catalog_service_host, FLAGS_catalog_service_port);
  Status status;
  CatalogServiceConnection client(env_->catalogd_client_cache(), address, &status);
  RETURN_IF_ERROR(status);

  TGetCatalogObjectRequest request;
  request.__set_object_desc(object_desc);

  TGetCatalogObjectResponse response;
  RETURN_IF_ERROR(
      client.DoRpc(&CatalogServiceClientWrapper::GetCatalogObject, request, &response));
  *result = response.catalog_object;
  return Status::OK();
}

Status CatalogOpExecutor::GetPartialCatalogObject(
    const TGetPartialCatalogObjectRequest& req,
    TGetPartialCatalogObjectResponse* resp) {
  DCHECK(FLAGS_use_local_catalog || TestInfo::is_test());
  const TNetworkAddress& address =
      MakeNetworkAddress(FLAGS_catalog_service_host, FLAGS_catalog_service_port);
  Status status;
  CatalogServiceConnection client(env_->catalogd_client_cache(), address, &status);
  RETURN_IF_ERROR(status);
  RETURN_IF_ERROR(
      client.DoRpc(&CatalogServiceClientWrapper::GetPartialCatalogObject, req, resp));
  if (FLAGS_inject_latency_after_catalog_fetch_ms > 0) {
    SleepForMs(FLAGS_inject_latency_after_catalog_fetch_ms);
  }
  return Status::OK();
}


Status CatalogOpExecutor::PrioritizeLoad(const TPrioritizeLoadRequest& req,
    TPrioritizeLoadResponse* result) {
  const TNetworkAddress& address =
      MakeNetworkAddress(FLAGS_catalog_service_host, FLAGS_catalog_service_port);
  Status status;
  CatalogServiceConnection client(env_->catalogd_client_cache(), address, &status);
  RETURN_IF_ERROR(status);
  RETURN_IF_ERROR(
      client.DoRpc(&CatalogServiceClientWrapper::PrioritizeLoad, req, result));
  return Status::OK();
}

Status CatalogOpExecutor::GetPartitionStats(
    const TGetPartitionStatsRequest& req, TGetPartitionStatsResponse* result) {
  const TNetworkAddress& address =
      MakeNetworkAddress(FLAGS_catalog_service_host, FLAGS_catalog_service_port);
  Status status;
  CatalogServiceConnection client(env_->catalogd_client_cache(), address, &status);
  RETURN_IF_ERROR(status);
  RETURN_IF_ERROR(
      client.DoRpc(&CatalogServiceClientWrapper::GetPartitionStats, req, result));
  return Status::OK();
}

Status CatalogOpExecutor::SentryAdminCheck(const TSentryAdminCheckRequest& req,
    TSentryAdminCheckResponse* result) {
  const TNetworkAddress& address =
      MakeNetworkAddress(FLAGS_catalog_service_host, FLAGS_catalog_service_port);
  Status cnxn_status;
  CatalogServiceConnection client(env_->catalogd_client_cache(), address, &cnxn_status);
  RETURN_IF_ERROR(cnxn_status);
  RETURN_IF_ERROR(
      client.DoRpc(&CatalogServiceClientWrapper::SentryAdminCheck, req, result));
  return Status::OK();
}

Status CatalogOpExecutor::UpdateTableUsage(const TUpdateTableUsageRequest& req,
  TUpdateTableUsageResponse* resp) {
  const TNetworkAddress& address =
      MakeNetworkAddress(FLAGS_catalog_service_host, FLAGS_catalog_service_port);
  Status cnxn_status;
  CatalogServiceConnection client(env_->catalogd_client_cache(), address, &cnxn_status);
  RETURN_IF_ERROR(cnxn_status);
  RETURN_IF_ERROR(
      client.DoRpc(&CatalogServiceClientWrapper::UpdateTableUsage, req, resp));
  return Status::OK();
}
