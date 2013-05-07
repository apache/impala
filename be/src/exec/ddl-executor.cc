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

Status DdlExecutor::Exec(const TDdlExecRequest& exec_request,
    const TSessionState& session) {
  switch (exec_request.ddl_type) {
    case TDdlType::SHOW_TABLES: {
      const TShowTablesParams* params = &exec_request.show_tables_params;
      // A NULL pattern means match all tables. However, Thrift string types can't
      // be NULL in C++, so we have to test if it's set rather than just blindly
      // using the value.
      const string* table_name =
          params->__isset.show_pattern ? &(params->show_pattern) : NULL;
      // TODO: refactor ImpalaServer->GetXXX outside of impala-server.
      TGetTablesResult table_names;
      RETURN_IF_ERROR(frontend_->GetTableNames(params->db, table_name,
          &session, &table_names));

      // Set the result set
      result_set_.resize(table_names.tables.size());
      for (int i = 0; i < table_names.tables.size(); ++i) {
        result_set_[i].__isset.colVals = true;
        result_set_[i].colVals.resize(1);
        result_set_[i].colVals[0].__set_stringVal(table_names.tables[i]);
      }
      return Status::OK;
    }
    case TDdlType::SHOW_DBS: {
      const TShowDbsParams* params = &exec_request.show_dbs_params;
      TGetDbsResult db_names;
      const string* db_pattern =
          params->__isset.show_pattern ? (&params->show_pattern) : NULL;
      RETURN_IF_ERROR(
          frontend_->GetDbNames(db_pattern, &session, &db_names));

      // Set the result set
      result_set_.resize(db_names.dbs.size());
      for (int i = 0; i < db_names.dbs.size(); ++i) {
        result_set_[i].__isset.colVals = true;
        result_set_[i].colVals.resize(1);
        result_set_[i].colVals[0].__set_stringVal(db_names.dbs[i]);
      }
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
      return frontend_->AlterTable(exec_request.alter_table_params);
    case TDdlType::CREATE_DATABASE:
      return frontend_->CreateDatabase(exec_request.create_db_params);
    case TDdlType::CREATE_TABLE_LIKE:
      return frontend_->CreateTableLike(
          exec_request.create_table_like_params);
    case TDdlType::CREATE_TABLE:
      return frontend_->CreateTable(exec_request.create_table_params);
    case TDdlType::DROP_DATABASE:
      return frontend_->DropDatabase(exec_request.drop_db_params);
    case TDdlType::DROP_TABLE:
      return frontend_->DropTable(exec_request.drop_table_params);
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
