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
#include "util/string-parser.h"

#include "gen-cpp/CatalogService.h"
#include "gen-cpp/CatalogService_types.h"

using namespace std;
using namespace impala;
using namespace apache::hive::service::cli::thrift;

DECLARE_int32(catalog_service_port);
DECLARE_string(catalog_service_host);

Status CatalogOpExecutor::Exec(const TCatalogOpRequest& request) {
  ThriftClient<CatalogServiceClient> client(FLAGS_catalog_service_host,
      FLAGS_catalog_service_port, NULL, ThriftServer::ThreadPool);
  switch (request.op_type) {
    case TCatalogOpType::DDL: {
      // Compute stats stmts must be executed via ExecComputeStats().
      DCHECK(request.ddl_params.ddl_type != TDdlType::COMPUTE_STATS);
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
          FLAGS_catalog_service_port, NULL, ThriftServer::ThreadPool);
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
  // col_stats_row, respectively.
  for (int i = 0; i < col_stats_row.colVals.size(); i += 2) {
    // The NDVs are written as a string column by the estimation function.
    StringParser::ParseResult parse_result;
    const string& ndvs_str = col_stats_row.colVals[i].stringVal.value;
    int64_t ndvs = StringParser::StringToInt<int64_t>(ndvs_str.data(),
        ndvs_str.size(), &parse_result);
    DCHECK_EQ(StringParser::PARSE_SUCCESS, parse_result);

    TColumnStats col_stats;
    col_stats.__set_num_distinct_values(ndvs);
    col_stats.__set_num_nulls(col_stats_row.colVals[i + 1].i64Val.value);
    // TODO: Gather and set the maxColLen/avgColLen stats as well. The planner
    // currently does not rely on them significantly.
    col_stats.__set_avg_size(-1);
    col_stats.__set_max_size(-1);
    params->column_stats[col_stats_schema.columns[i].columnName] = col_stats;
  }
  params->__isset.column_stats = true;
}
