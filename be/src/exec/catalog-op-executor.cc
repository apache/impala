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
#include "util/debug-util.h"
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
DECLARE_string(debug_actions);

DECLARE_int32(catalog_client_connection_num_retries);
DECLARE_int32(catalog_client_rpc_timeout_ms);
DECLARE_int32(catalog_client_rpc_retry_interval_ms);

DEFINE_int32_hidden(inject_latency_before_catalog_fetch_ms, 0,
    "Latency (ms) to be injected before fetching catalog data from the catalogd");
DEFINE_int32_hidden(inject_latency_after_catalog_fetch_ms, 0,
    "Latency (ms) to be injected after fetching catalog data from the catalogd");
DEFINE_double_hidden(inject_failure_ratio_in_catalog_fetch, -1,
    "Ratio to randomly fail the GetPartialCatalogObject RPC with TABLE_NOT_LOADED "
    "status. 0.1 means fail it in a possibility of 10%. Negative values disable this. "
    "values >= 1 will always make the RPC fail");

/// Used purely for debug actions. The DEBUG_ACTION is only executed on the first RPC
/// attempt.
static Status CatalogRpcDebugFn(int* attempt) {
  return (*attempt)++ == 0 ?
      DebugAction(FLAGS_debug_actions, "CATALOG_RPC_FIRST_ATTEMPT") :
      Status::OK();
}

/// Used in LocalCatalog mode to verify the catalog RPC reponse only contains minimal
/// catalog objects, i.e. database objects have only db names and table objects have only
/// db and table names.
static void VerifyMinimalResponse(const TCatalogUpdateResult& result) {
  for (const TCatalogObject& obj : result.updated_catalog_objects) {
    if (obj.type == impala::TCatalogObjectType::TABLE) {
      // Make sure the table object only contains db_name and tbl_name and nothing else.
      DCHECK(!obj.table.__isset.metastore_table);
      DCHECK(!obj.table.__isset.columns);
      DCHECK(!obj.table.__isset.clustering_columns);
      DCHECK(!obj.table.__isset.table_stats);
      DCHECK(!obj.table.__isset.hdfs_table);
      DCHECK(!obj.table.__isset.kudu_table);
      DCHECK(!obj.table.__isset.hbase_table);
      DCHECK(!obj.table.__isset.iceberg_table);
      DCHECK(!obj.table.__isset.data_source_table);
    } else if (obj.type == impala::TCatalogObjectType::DATABASE) {
      DCHECK(!obj.db.__isset.metastore_db)
          << "Minimal database TCatalogObject should have empty metastore_db";
    }
  }
}

Status CatalogOpExecutor::Exec(const TCatalogOpRequest& request) {
  Status status;
  DCHECK(profile_ != NULL);
  RuntimeProfile::Counter* exec_timer = ADD_TIMER(profile_, "CatalogOpExecTimer");
  SCOPED_TIMER(exec_timer);
  RETURN_IF_ERROR(status);
  switch (request.op_type) {
    case TCatalogOpType::DDL: {
      // Compute stats stmts must be executed via ExecComputeStats().
      DCHECK(request.ddl_params.ddl_type != TDdlType::COMPUTE_STATS);

      exec_response_.reset(new TDdlExecResponse());
      int attempt = 0; // Used for debug action only.
      CatalogServiceConnection::RpcStatus rpc_status =
          CatalogServiceConnection::DoRpcWithRetry(env_->catalogd_client_cache(),
              *ExecEnv::GetInstance()->GetCatalogdAddress().get(),
              &CatalogServiceClientWrapper::ExecDdl, request.ddl_params,
              FLAGS_catalog_client_connection_num_retries,
              FLAGS_catalog_client_rpc_retry_interval_ms,
              [&attempt]() { return CatalogRpcDebugFn(&attempt); }, exec_response_.get());
      RETURN_IF_ERROR(rpc_status.status);
      if (FLAGS_use_local_catalog) VerifyMinimalResponse(exec_response_->result);
      catalog_update_result_.reset(
          new TCatalogUpdateResult(exec_response_->result));
      Status status(exec_response_->result.status);
      if (status.ok()) {
        if (request.ddl_params.ddl_type == TDdlType::DROP_FUNCTION) {
          HandleDropFunction(request.ddl_params.drop_fn_params);
        } else if (request.ddl_params.ddl_type == TDdlType::DROP_DATA_SOURCE) {
          HandleDropDataSource(request.ddl_params.drop_data_source_params);
        }
      }
      if (exec_response_->__isset.profile) {
        catalog_profile_ = make_unique<TRuntimeProfileNode>(exec_response_->profile);
      }
      return status;
    }
    case TCatalogOpType::RESET_METADATA: {
      TResetMetadataResponse response;
      int attempt = 0; // Used for debug action only.
      CatalogServiceConnection::RpcStatus rpc_status =
          CatalogServiceConnection::DoRpcWithRetry(env_->catalogd_client_cache(),
              *ExecEnv::GetInstance()->GetCatalogdAddress().get(),
              &CatalogServiceClientWrapper::ResetMetadata, request.reset_metadata_params,
              FLAGS_catalog_client_connection_num_retries,
              FLAGS_catalog_client_rpc_retry_interval_ms,
              [&attempt]() { return CatalogRpcDebugFn(&attempt); }, &response);
      RETURN_IF_ERROR(rpc_status.status);
      if (FLAGS_use_local_catalog) VerifyMinimalResponse(response.result);
      if (response.__isset.profile) {
        catalog_profile_ = make_unique<TRuntimeProfileNode>(response.profile);
      }
      catalog_update_result_.reset(new TCatalogUpdateResult(response.result));
      return Status(response.result.status);
    }
    default: {
      return Status(Substitute("TCatalogOpType: $0 does not support execution against the"
          " CatalogService", request.op_type));
    }
  }
}

Status CatalogOpExecutor::ExecComputeStats(const TCatalogServiceRequestHeader& header,
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
  update_stats_req.query_options.__set_sync_ddl(compute_stats_request.sync_ddl);
  update_stats_req.query_options.__set_debug_action(
      compute_stats_request.ddl_params.query_options.debug_action);
  update_stats_req.__set_header(header);

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
  // length for string columns. Positions i+4 and i+5 contain the numTrues/numFalses
  // (-1 for non-string columns). Positions i+6 and i+7 contain the min and the max.
  for (int i = 0; i < col_stats_row.colVals.size(); i += 8) {
    TColumnStats col_stats;
    col_stats.__set_num_distinct_values(col_stats_row.colVals[i].i64Val.value);
    col_stats.__set_num_nulls(col_stats_row.colVals[i + 1].i64Val.value);
    col_stats.__set_max_size(col_stats_row.colVals[i + 2].i32Val.value);
    col_stats.__set_avg_size(col_stats_row.colVals[i + 3].doubleVal.value);
    col_stats.__set_num_trues(col_stats_row.colVals[i + 4].i64Val.value);
    col_stats.__set_num_falses(col_stats_row.colVals[i + 5].i64Val.value);
    // By default, the low and high value in TColumnStats are unset. Set both
    // conditionally.
    TColumnValue low_value = ConvertToTColumnValue(
        col_stats_schema.columns[i + 6], col_stats_row.colVals[i + 6]);
    if (isOneFieldSet(low_value)) {
      col_stats.__set_low_value(low_value);
    }
    TColumnValue high_value = ConvertToTColumnValue(
        col_stats_schema.columns[i + 7], col_stats_row.colVals[i + 7]);
    if (isOneFieldSet(high_value)) {
      col_stats.__set_high_value(high_value);
    }
    params->column_stats[col_stats_schema.columns[i].columnName] = col_stats;
  }
  params->__isset.column_stats = true;
}

Status CatalogOpExecutor::GetCatalogObject(const TCatalogObject& object_desc,
    TCatalogObject* result) {
  TGetCatalogObjectRequest request;
  request.__set_object_desc(object_desc);

  TGetCatalogObjectResponse response;
  int attempt = 0; // Used for debug action only.
  CatalogServiceConnection::RpcStatus rpc_status =
      CatalogServiceConnection::DoRpcWithRetry(env_->catalogd_client_cache(),
          *ExecEnv::GetInstance()->GetCatalogdAddress().get(),
          &CatalogServiceClientWrapper::GetCatalogObject, request,
          FLAGS_catalog_client_connection_num_retries,
          FLAGS_catalog_client_rpc_retry_interval_ms,
          [&attempt]() { return CatalogRpcDebugFn(&attempt); }, &response);
  RETURN_IF_ERROR(rpc_status.status);
  *result = response.catalog_object;
  return Status::OK();
}

Status CatalogOpExecutor::GetPartialCatalogObject(
    const TGetPartialCatalogObjectRequest& req,
    TGetPartialCatalogObjectResponse* resp) {
  DCHECK(FLAGS_use_local_catalog || TestInfo::is_test());
  if (FLAGS_inject_latency_before_catalog_fetch_ms > 0) {
    SleepForMs(FLAGS_inject_latency_before_catalog_fetch_ms);
  }
  // Non-table requests are lightweight requests that won't be blocked by table loading
  // or table locks. Note that when loading table list of a db, the type is DB.
  auto client_cache_ptr = (req.object_desc.type == TCatalogObjectType::TABLE) ?
      env_->catalogd_client_cache() : env_->catalogd_lightweight_req_client_cache();
  int attempt = 0; // Used for debug action only.
  CatalogServiceConnection::RpcStatus rpc_status =
      CatalogServiceConnection::DoRpcWithRetry(client_cache_ptr,
          *ExecEnv::GetInstance()->GetCatalogdAddress().get(),
          &CatalogServiceClientWrapper::GetPartialCatalogObject, req,
          FLAGS_catalog_client_connection_num_retries,
          FLAGS_catalog_client_rpc_retry_interval_ms,
          [&attempt]() { return CatalogRpcDebugFn(&attempt); }, resp);
  RETURN_IF_ERROR(rpc_status.status);
  if (FLAGS_inject_latency_after_catalog_fetch_ms > 0) {
    SleepForMs(FLAGS_inject_latency_after_catalog_fetch_ms);
  }
  if (FLAGS_inject_failure_ratio_in_catalog_fetch > 0
      && rand() < FLAGS_inject_failure_ratio_in_catalog_fetch * (RAND_MAX + 1L)) {
    resp->lookup_status = CatalogLookupStatus::TABLE_NOT_LOADED;
  }
  return Status::OK();
}


Status CatalogOpExecutor::PrioritizeLoad(const TPrioritizeLoadRequest& req,
    TPrioritizeLoadResponse* result) {
  int attempt = 0; // Used for debug action only.
  CatalogServiceConnection::RpcStatus rpc_status =
      CatalogServiceConnection::DoRpcWithRetry(
          env_->catalogd_lightweight_req_client_cache(),
          *ExecEnv::GetInstance()->GetCatalogdAddress().get(),
          &CatalogServiceClientWrapper::PrioritizeLoad, req,
          FLAGS_catalog_client_connection_num_retries,
          FLAGS_catalog_client_rpc_retry_interval_ms,
          [&attempt]() { return CatalogRpcDebugFn(&attempt); }, result);
  RETURN_IF_ERROR(rpc_status.status);
  return Status::OK();
}

Status CatalogOpExecutor::GetPartitionStats(
    const TGetPartitionStatsRequest& req, TGetPartitionStatsResponse* result) {
  int attempt = 0; // Used for debug action only.
  CatalogServiceConnection::RpcStatus rpc_status =
      CatalogServiceConnection::DoRpcWithRetry(env_->catalogd_client_cache(),
          *ExecEnv::GetInstance()->GetCatalogdAddress().get(),
          &CatalogServiceClientWrapper::GetPartitionStats, req,
          FLAGS_catalog_client_connection_num_retries,
          FLAGS_catalog_client_rpc_retry_interval_ms,
          [&attempt]() { return CatalogRpcDebugFn(&attempt); }, result);
  RETURN_IF_ERROR(rpc_status.status);
  return Status::OK();
}

Status CatalogOpExecutor::UpdateTableUsage(const TUpdateTableUsageRequest& req,
  TUpdateTableUsageResponse* resp) {
  int attempt = 0; // Used for debug action only.
  CatalogServiceConnection::RpcStatus rpc_status =
      CatalogServiceConnection::DoRpcWithRetry(
          // The operation doesn't require table lock in catalogd. It doesn't require
          // the table being loaded neither. So we can use clients for lightweight
          // requests.
          env_->catalogd_lightweight_req_client_cache(),
          *ExecEnv::GetInstance()->GetCatalogdAddress().get(),
          &CatalogServiceClientWrapper::UpdateTableUsage, req,
          FLAGS_catalog_client_connection_num_retries,
          FLAGS_catalog_client_rpc_retry_interval_ms,
          [&attempt]() { return CatalogRpcDebugFn(&attempt); }, resp);
  RETURN_IF_ERROR(rpc_status.status);
  return Status::OK();
}

Status CatalogOpExecutor::GetNullPartitionName(
    const TGetNullPartitionNameRequest& req, TGetNullPartitionNameResponse* result) {
  int attempt = 0; // Used for debug action only.
  CatalogServiceConnection::RpcStatus rpc_status =
      CatalogServiceConnection::DoRpcWithRetry(
          env_->catalogd_lightweight_req_client_cache(),
          *ExecEnv::GetInstance()->GetCatalogdAddress().get(),
          &CatalogServiceClientWrapper::GetNullPartitionName, req,
          FLAGS_catalog_client_connection_num_retries,
          FLAGS_catalog_client_rpc_retry_interval_ms,
          [&attempt]() { return CatalogRpcDebugFn(&attempt); }, result);
  RETURN_IF_ERROR(rpc_status.status);
  return Status::OK();
}

Status CatalogOpExecutor::GetLatestCompactions(
    const TGetLatestCompactionsRequest& req, TGetLatestCompactionsResponse* result) {
  int attempt = 0; // Used for debug action only.
  CatalogServiceConnection::RpcStatus rpc_status =
      CatalogServiceConnection::DoRpcWithRetry(
          env_->catalogd_lightweight_req_client_cache(),
          *ExecEnv::GetInstance()->GetCatalogdAddress().get(),
          &CatalogServiceClientWrapper::GetLatestCompactions, req,
          FLAGS_catalog_client_connection_num_retries,
          FLAGS_catalog_client_rpc_retry_interval_ms,
          [&attempt]() { return CatalogRpcDebugFn(&attempt); }, result);
  RETURN_IF_ERROR(rpc_status.status);
  return Status::OK();
}
