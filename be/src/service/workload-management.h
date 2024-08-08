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

#pragma once

#include <array>
#include <memory>
#include <string>
#include <utility>

#include "gen-cpp/SystemTables_types.h"
#include "kudu/util/version_util.h"
#include "service/query-state-record.h"
#include "util/string-util.h"
#include "util/version-util.h"

namespace impala {

/// Struct defining the context for generating the sql DML that inserts records into the
/// completed queries table.
struct FieldParserContext {
  const QueryStateExpanded* record;
  const std::string cluster_id;
  StringStreamPop& sql;

  FieldParserContext(const QueryStateExpanded* rec, const std::string& cluster_id,
      StringStreamPop& s) : record(rec), cluster_id(cluster_id), sql(s) {}
}; // struct FieldParserContext

/// Constants for all possible schema versions.
const kudu::Version NO_TABLE_EXISTS = constructVersion(0, 0, 0);
const kudu::Version VERSION_1_0_0 = constructVersion(1, 0, 0);

/// Type of a function that retrieves one piece of information from the context and writes
/// it to the SQL statement that inserts rows into the completed queries table.
using FieldParser = void (*)(FieldParserContext&);

/// Contains all necessary information for the definition and parsing of a single field
/// in workload management.
struct FieldDefinition {
  public:
    // Name of the database column.
    const TQueryTableColumn::type db_column;

    // Type of the database column.
    const TPrimitiveType::type db_column_type;

    // Function that will extract the column value from the provided FieldParseContext and
    // will write that value into a sql statement,
    const FieldParser parser;

    // Specifies the first schema version where the column appears.
    const kudu::Version schema_version;

    // When column type is decimal, specifies the precision and scale for the column.
    const int16_t precision;
    const int16_t scale;

    FieldDefinition(const TQueryTableColumn::type db_col,
        const TPrimitiveType::type db_col_type, const FieldParser fp,
        const kudu::Version schema_ver, const int16_t precision = 0,
        const int16_t scale = 0) :
        db_column(std::move(db_col)), db_column_type(std::move(db_col_type)),
        parser(std::move(fp)), schema_version(std::move(schema_ver)),
        precision(precision), scale(scale) { }
}; // struct FieldDefinition

/// Number of query table columns
constexpr size_t NumQueryTableColumns = TQueryTableColumn::TABLES_QUERIED + 1;

/// This list is the main data structure for workload management. Each list entry
/// contains the name of a column in the completed queries table, the type of that column,
/// and an implementation of a FieldParser for generating the value of that column from a
/// `QueryStateExpanded` object.
extern const std::array<FieldDefinition, NumQueryTableColumns> FIELD_DEFINITIONS;

/// Track the state of the thread that processes the completed queries queue. Access to
/// the ThreadState variable must only happen after taking a lock on the associated mutex.
/// Can be used to track the lifecycle of a thread.
enum ThreadState {
  NOT_STARTED,
  INITIALIZING,
  RUNNING,
  SHUTTING_DOWN,
  SHUTDOWN
};

// Represents one query that has completed.
struct CompletedQuery {
  // Contains information about the completed query.
  const std::shared_ptr<QueryStateExpanded> query;

  // Count of the number of times the completed query has attempted to be inserted into
  // the completed queries table. he count is tracked so that the number of attempts can
  // be limited and failing inserts do not retry indefinitely.
  uint8_t insert_attempts_count;

  CompletedQuery(const std::shared_ptr<QueryStateExpanded> query) :
      query(std::move(query)) {
    insert_attempts_count = 0;
  }
};

} // namespace impala
