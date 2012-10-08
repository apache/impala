// Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#include <sstream>
#include <boost/foreach.hpp>

#include "exec/ddl-executor.h"
#include "common/status.h"
#include "runtime/row-batch.h"
#include "service/impala-server.h"

using namespace std;
using namespace impala;

DdlExecutor::DdlExecutor(ImpalaServer* impala_server, const string& delimiter) 
    : delimiter_(delimiter),
      impala_server_(impala_server) {
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
    RETURN_IF_ERROR(impala_server_->GetTableNames(&exec_request->database, table_name, 
        &ascii_rows_));
    return Status::OK;
  }

  if (exec_request->ddl_type == TDdlType::SHOW_DBS) {
    string* db_pattern = 
      exec_request->__isset.show_pattern ? (&exec_request->show_pattern) : NULL;
    RETURN_IF_ERROR(impala_server_->GetDbNames(db_pattern, &ascii_rows_));
    return Status::OK;
  }

  if (exec_request->ddl_type == TDdlType::DESCRIBE) {
    vector<TColumnDesc> columns;
    RETURN_IF_ERROR(impala_server_->DescribeTable(exec_request->database, 
        exec_request->describe_table, &columns));
    BOOST_FOREACH(const TColumnDesc& column, columns) {
      stringstream row;
      row << column.columnName << delimiter_ 
          << TypeToOdbcString(ThriftToType(column.columnType));
      ascii_rows_.push_back(row.str());
    }
    return Status::OK;
  }

  stringstream ss;
  ss << "Unknown DDL exec request type: " << exec_request->ddl_type;
  return Status(ss.str());
}

