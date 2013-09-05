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
#include <boost/foreach.hpp>

#include "exec/ddl-executor.h"
#include "common/status.h"
#include "runtime/row-batch.h"
#include "service/impala-server.h"

using namespace std;
using namespace impala;

DdlExecutor::DdlExecutor(Frontend* frontend)
    : frontend_(frontend) {
  DCHECK(frontend != NULL);
}

void DdlExecutor::SetResultSet(const vector<string>& results) {
  result_set_.resize(results.size());
  for (int i = 0; i < results.size(); ++i) {
    result_set_[i].__isset.colVals = true;
    result_set_[i].colVals.resize(1);
    result_set_[i].colVals[0].__set_stringVal(results[i]);
  }
}

Status DdlExecutor::Exec(const TDdlExecRequest& exec_request,
    const TSessionState& session) {
  exec_response_.reset(new TDdlExecResponse());
  switch (exec_request.ddl_type) {
    case TDdlType::SHOW_TABLES: {
      const TShowTablesParams* params = &exec_request.show_tables_params;
      // A NULL pattern means match all tables. However, Thrift string types can't
      // be NULL in C++, so we have to test if it's set rather than just blindly
      // using the value.
      const string* table_name =
          params->__isset.show_pattern ? &(params->show_pattern) : NULL;
      TGetTablesResult table_names;
      RETURN_IF_ERROR(frontend_->GetTableNames(params->db, table_name,
          &session, &table_names));
      SetResultSet(table_names.tables);
      return Status::OK;
    }
    case TDdlType::SHOW_DBS: {
      const TShowDbsParams* params = &exec_request.show_dbs_params;
      TGetDbsResult db_names;
      const string* db_pattern =
          params->__isset.show_pattern ? (&params->show_pattern) : NULL;
      RETURN_IF_ERROR(
          frontend_->GetDbNames(db_pattern, &session, &db_names));
      SetResultSet(db_names.dbs);
      return Status::OK;
    }
    case TDdlType::SHOW_FUNCTIONS: {
      const TShowFunctionsParams* params = &exec_request.show_fns_params;
      TGetFunctionsResult functions;
      const string* fn_pattern =
          params->__isset.show_pattern ? (&params->show_pattern) : NULL;
      RETURN_IF_ERROR(
          frontend_->GetFunctions(params->db, fn_pattern, &session, &functions));
      SetResultSet(functions.fn_signatures);
      return Status::OK;
    }
    case TDdlType::DESCRIBE: {
      TDescribeTableResult response;
      RETURN_IF_ERROR(frontend_->DescribeTable(exec_request.describe_table_params,
          &response));
      // Set the result set
      result_set_ = response.results;
      return Status::OK;
    }
    case TDdlType::ALTER_TABLE:
    case TDdlType::ALTER_VIEW:
    case TDdlType::CREATE_DATABASE:
    case TDdlType::CREATE_TABLE_LIKE:
    case TDdlType::CREATE_TABLE:
    case TDdlType::CREATE_TABLE_AS_SELECT:
    case TDdlType::CREATE_VIEW:
    case TDdlType::CREATE_FUNCTION:
    case TDdlType::DROP_DATABASE:
    case TDdlType::DROP_FUNCTION:
    case TDdlType::DROP_TABLE:
    case TDdlType::DROP_VIEW:
      return frontend_->ExecDdlRequest(exec_request, exec_response_.get());
    case TDdlType::RESET_METADATA:
      return frontend_->ResetMetadata(exec_request.reset_metadata_params);
    default: {
      stringstream ss;
      ss << "Unknown DDL exec request type: " << exec_request.ddl_type;
      return Status(ss.str());
    }
  }
}

// TODO: This is likely a superset of GetTableNames/GetDbNames. Coalesce these different
// code paths.
Status DdlExecutor::Exec(const TMetadataOpRequest& exec_request) {
  TMetadataOpResponse metadata_op_result_;
  RETURN_IF_ERROR(frontend_->ExecHiveServer2MetadataOp(exec_request,
      &metadata_op_result_));
  result_set_metadata_ = metadata_op_result_.result_set_metadata;
  result_set_ = metadata_op_result_.results;
  return Status::OK;
}
