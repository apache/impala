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

DdlExecutor::DdlExecutor(ImpalaServer* impala_server)
    : impala_server_(impala_server) {
  DCHECK(impala_server != NULL);
}

Status DdlExecutor::Exec(TDdlExecRequest* exec_request) {
  if (exec_request->ddl_type == TDdlType::SHOW_TABLES) {
    // A NULL pattern means match all tables. However, Thrift string types can't
    // be NULL in C++, so we have to test if it's set rather than just blindly
    // using the value.
    string* table_name = 
        exec_request->__isset.show_pattern ? &(exec_request->show_pattern) : NULL;
    // NULL DB means search all dbs; required until we support USE, and not set
    // in the Thrift request
    // TODO: refactor ImpalaServer->GetXXX outside of impala-server.
    TGetTablesResult table_names;
    RETURN_IF_ERROR(impala_server_->GetTableNames(&exec_request->database, table_name, 
        &table_names));

    // Set the result set
    result_set_.resize(table_names.tables.size());
    for (int i = 0; i < table_names.tables.size(); ++i) {
      result_set_[i].__isset.colVals = true;
      result_set_[i].colVals.resize(1);
      result_set_[i].colVals[0].__set_stringVal(table_names.tables[i]);
    }
    return Status::OK;
  }

  if (exec_request->ddl_type == TDdlType::SHOW_DBS) {
    string* db_pattern = 
      exec_request->__isset.show_pattern ? (&exec_request->show_pattern) : NULL;
    TGetDbsResult db_names;
    RETURN_IF_ERROR(impala_server_->GetDbNames(db_pattern, &db_names));

    // Set the result set
    result_set_.resize(db_names.dbs.size());
    for (int i = 0; i < db_names.dbs.size(); ++i) {
      result_set_[i].__isset.colVals = true;
      result_set_[i].colVals.resize(1);
      result_set_[i].colVals[0].__set_stringVal(db_names.dbs[i]);
    }
    return Status::OK;
  }

  if (exec_request->ddl_type == TDdlType::DESCRIBE) {
    TDescribeTableResult table_columns;
    RETURN_IF_ERROR(impala_server_->DescribeTable(exec_request->database, 
        exec_request->describe_table, &table_columns));

    // Set the result set
    result_set_.resize(table_columns.columns.size());
    for (int i = 0; i < table_columns.columns.size(); ++i) {
      result_set_[i].__isset.colVals = true;
      result_set_[i].colVals.resize(2);
      result_set_[i].colVals[0].__set_stringVal(table_columns.columns[i].columnName);
      result_set_[i].colVals[1].__set_stringVal(
          TypeToOdbcString(ThriftToType(table_columns.columns[i].columnType)));
    }
    return Status::OK;
  }

  stringstream ss;
  ss << "Unknown DDL exec request type: " << exec_request->ddl_type;
  return Status(ss.str());
}

// TODO: This is likely a superset of GetTableNames/GetDbNames. Coalesce these different
// code paths.
Status DdlExecutor::Exec(const TMetadataOpRequest& exec_request) {
  TMetadataOpResponse metdata_op_result_;
  RETURN_IF_ERROR(impala_server_->ExecHiveServer2MetadataOp(exec_request,
      &metdata_op_result_));
  request_id_ = metdata_op_result_.request_id;
  result_set_metadata_ = metdata_op_result_.result_set_metadata;
  result_set_ = metdata_op_result_.results;
  return Status::OK;
}

