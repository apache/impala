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

#include "exec/catalog-op-executor.h"

#include <sstream>

#include "common/status.h"
#include "runtime/lib-cache.h"
#include "service/impala-server.h"
#include "util/string-parser.h"

#include "gen-cpp/CatalogService.h"
#include "gen-cpp/CatalogService_types.h"

#include <thrift/protocol/TDebugProtocol.h>

using namespace std;
using namespace impala;
using namespace apache::hive::service::cli::thrift;

DECLARE_int32(catalog_service_port);
DECLARE_string(catalog_service_host);

Status CatalogOpExecutor::Exec(const TCatalogOpRequest& request) {
  Status status;
  const TNetworkAddress& address =
      MakeNetworkAddress(FLAGS_catalog_service_host, FLAGS_catalog_service_port);
  CatalogServiceConnection client(env_->catalogd_client_cache(), address, &status);
  RETURN_IF_ERROR(status);
  switch (request.op_type) {
    case TCatalogOpType::DDL: {
      // Compute stats stmts must be executed via ExecComputeStats().
      DCHECK(request.ddl_params.ddl_type != TDdlType::COMPUTE_STATS);
      if (request.ddl_params.ddl_type == TDdlType::DROP_FUNCTION) {
        HandleDropFunction(request.ddl_params.drop_fn_params);
      }
      catalog_update_result_.reset(new TCatalogUpdateResult());
      exec_response_.reset(new TDdlExecResponse());
      client->ExecDdl(*exec_response_.get(), request.ddl_params);
      catalog_update_result_.reset(
          new TCatalogUpdateResult(exec_response_.get()->result));
      return Status(exec_response_->result.status);
    }
    case TCatalogOpType::RESET_METADATA: {
      TResetMetadataResponse response;
      catalog_update_result_.reset(new TCatalogUpdateResult());
      client->ResetMetadata(response, request.reset_metadata_params);
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

Status CatalogOpExecutor::ExecComputeStats(
    const TComputeStatsParams& compute_stats_params,
    const TTableSchema& tbl_stats_schema, const TRowSet& tbl_stats_data,
    const TTableSchema& col_stats_schema, const TRowSet& col_stats_data) {
  // Create a new DDL request to alter the table's statistics.
  TCatalogOpRequest catalog_op_req;
  catalog_op_req.__isset.ddl_params = true;
  catalog_op_req.__set_op_type(TCatalogOpType::DDL);
  TDdlExecRequest& update_stats_req = catalog_op_req.ddl_params;
  update_stats_req.__set_ddl_type(TDdlType::ALTER_TABLE);

  TAlterTableUpdateStatsParams& update_stats_params =
      update_stats_req.alter_table_params.update_stats_params;
  update_stats_req.__isset.alter_table_params = true;
  update_stats_req.alter_table_params.__set_alter_type(TAlterTableType::UPDATE_STATS);
  update_stats_req.alter_table_params.__set_table_name(compute_stats_params.table_name);
  update_stats_req.alter_table_params.__isset.update_stats_params = true;
  update_stats_params.__set_table_name(compute_stats_params.table_name);

  // Fill the alteration request based on the child-query results.
  SetTableStats(tbl_stats_schema, tbl_stats_data, &update_stats_params);
  SetColumnStats(col_stats_schema, col_stats_data, &update_stats_params);

  // Execute the 'alter table update stats' request.
  RETURN_IF_ERROR(Exec(catalog_op_req));
  return Status::OK;
}

void CatalogOpExecutor::HandleDropFunction(const TDropFunctionParams& request) {
  DCHECK(fe_ != NULL) << "FE tests should not be calling this";

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
    VLOG_QUERY << "Could not lookup catalog object: "
               << apache::thrift::ThriftDebugString(request);
    return;
  }
  env_->lib_cache()->RemoveEntry(fn.fn.hdfs_location);
}

void CatalogOpExecutor::SetTableStats(const TTableSchema& tbl_stats_schema,
    const TRowSet& tbl_stats_data, TAlterTableUpdateStatsParams* params) {
  // Accumulate total number of rows in the table.
  long total_num_rows = 0;
  // Set per-partition stats.
  BOOST_FOREACH(const TRow& row, tbl_stats_data.rows) {
    DCHECK_GT(row.colVals.size(), 0);
    // The first column is the COUNT(*) expr of the original query.
    DCHECK(row.colVals[0].__isset.i64Val);
    int64_t num_rows = row.colVals[0].i64Val.value;
    // The remaining columns are partition columns that the results are grouped by.
    vector<string> partition_key_vals;
    partition_key_vals.reserve(row.colVals.size());
    for (int j = 1; j < row.colVals.size(); ++j) {
      // The partition-key values have been explicitly cast to string in the select list.
      DCHECK(row.colVals[j].__isset.stringVal);
      partition_key_vals.push_back(row.colVals[j].stringVal.value);
    }
    params->partition_stats[partition_key_vals].__set_num_rows(num_rows);
    total_num_rows += num_rows;
  }
  params->__isset.partition_stats = true;

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
    // The NDVs are written as a string column by the estimation function.
    StringParser::ParseResult parse_result;
    const string& ndvs_str = col_stats_row.colVals[i].stringVal.value;
    int64_t ndvs = StringParser::StringToInt<int64_t>(ndvs_str.data(),
        ndvs_str.size(), &parse_result);
    DCHECK_EQ(StringParser::PARSE_SUCCESS, parse_result);

    TColumnStats col_stats;
    col_stats.__set_num_distinct_values(ndvs);
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
  client->GetCatalogObject(response, request);
  *result = response.catalog_object;
  return Status::OK;
}
