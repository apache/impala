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

/// Contains declarations that only pertain to the worker thread that persists completed
/// queries into the database table.

#pragma once

#include "workload_mgmt/workload-management.h"

#include <array>
#include <string>
#include <utility>

#include "gen-cpp/SystemTables_types.h"
#include "service/query-options.h"
#include "service/query-state-record.h"
#include "util/string-util.h"

DECLARE_int32(query_log_max_sql_length);
DECLARE_int32(query_log_max_plan_length);

namespace impala {
namespace workloadmgmt {

/// Struct defining the context for generating the sql DML that inserts records into the
/// completed queries table.
struct FieldParserContext {
  const QueryStateExpanded* record;
  const std::string cluster_id;
  StringStreamPop& sql;

  FieldParserContext(const QueryStateExpanded* rec, const std::string& cluster_id,
      StringStreamPop& s) : record(rec), cluster_id(cluster_id), sql(s) {}
}; // struct FieldParserContext

/// Type of a function that retrieves one piece of information from the context and
/// adds it to the SQL statement that inserts rows into the completed queries table.
using FieldParser = void (*)(FieldParserContext&);

/// Number of query table columns. Used to initialize a std::array.
constexpr size_t NumQueryTableColumns = TQueryTableColumn::EXECUTOR_SLOTS + 1;

/// Array containing parser functions for each query column. These parsers are used to
/// generate the value for each query column from a completed query represented by a
/// `QueryStateExpanded` object.
extern const std::array<FieldParser, NumQueryTableColumns> FIELD_PARSERS;

/// Track the state of the thread that processes the completed queries queue.
enum class WorkloadManagementState {
  // Workload management has not started.
  NOT_STARTED,

  // Running initial startup checks.
  STARTING,

  // Intial startup checks completed.
  STARTED,

  // Initial setup of the workload management db tables is done, completed queries queue
  // is now being processed.
  RUNNING,

  // Coordinator graceful shutdown initiated, and all running queries have finished or
  // been cancelled. The completed queries queue can now be drained.
  SHUTTING_DOWN,

  // In-memory completed queries queue drained, coordinator shutdown can finish.
  SHUTDOWN
};

/// Represents one query that has completed.
struct CompletedQuery {
  // Contains information about the completed query.
  std::shared_ptr<QueryStateExpanded> query;

  // Count of the number of times the completed query has attempted to be inserted into
  // the completed queries table. The count is tracked so that the number of attempts can
  // be limited and failing inserts do not retry indefinitely.
  uint8_t insert_attempts_count;

  CompletedQuery(std::shared_ptr<QueryStateExpanded> query) : query(std::move(query)) {
    insert_attempts_count = 0;
  }
};

} // namespace workloadmgmt
} // namespace impala
