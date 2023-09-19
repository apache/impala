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

/// Contains declarations that are used across the entire workload management lifecycle
/// (both workload management initialization that sets up the necessary database tables
/// and the worker thread that persists completed queries into the database table).

#pragma once

#include <map>
#include <string>
#include <utility>

#include "common/status.h"
#include "gen-cpp/SystemTables_types.h"
#include "gen-cpp/Types_types.h"
#include "kudu/util/version_util.h"
#include "util/version-util.h"

namespace impala {
namespace workloadmgmt {

/// Parses the value of the --workload_mgmt_schema_version startup flag into a
/// kudu::Version object and passes that object back in the `target_schema_version`
/// function output parameter. If the --workload_mgmt_schema_version startup flag is
/// empty, the latest workload management schema version is used.
impala::Status ParseSchemaVersionFlag(kudu::Version* target_schema_version);

/// Runs common startup checks for any daemon that participates in workload management.
/// These checks ensure the provided `kudu::Version` object is a known schema version.
/// Also caches the `target_schema_version` for use in the IncludeField function.
impala::Status StartupChecks(const kudu::Version& target_schema_version);

/// Retrieves the query log table name with or without the db prepended.
std::string QueryLogTableName(bool with_db);

/// Retrieves the live queries table name with or without the db prepended.
std::string QueryLiveTableName(bool with_db);

/// Name of the database where all workload management tables will be stored.
const std::string WM_DB = "sys";

/// Constants for all possible schema versions.
const kudu::Version NO_TABLE_EXISTS = kudu::Version();
const kudu::Version VERSION_1_0_0 = impala::ConstructVersion(1, 0, 0);
const kudu::Version VERSION_1_1_0 = impala::ConstructVersion(1, 1, 0);

/// Set of all possible valid schema versions.  Must be sorted in order from earliest to
/// latest version.
const std::set<kudu::Version> KNOWN_VERSIONS = {VERSION_1_0_0, VERSION_1_1_0};

/// Constants declaring how durations measured in milliseconds will be stored in the
/// table. Must match the constants with the same name declared in SystemTable.java.
static constexpr int8_t DURATION_DECIMAL_PRECISION = 18;
static constexpr int8_t DURATION_DECIMAL_SCALE = 3;

/// Contains all necessary information for the definition and parsing of a single field
/// in workload management.
class FieldDefinition {
 public:
  // Type of the database column.
  const TPrimitiveType::type db_column_type;

  // Specifies the first schema version where the column appears.
  const kudu::Version schema_version;

  // When column type is decimal, specifies the precision and scale for the column.
  const int16_t precision;
  const int16_t scale;

  FieldDefinition(TPrimitiveType::type db_col_type, kudu::Version schema_ver,
      const int16_t precision = 0, const int16_t scale = 0)
    : db_column_type(std::move(db_col_type)),
      schema_version(std::move(schema_ver)),
      precision(precision),
      scale(scale) {}

  // Indicates if the schema version where this field was introduced is less than or
  // equal to the specified target schema version. If returning is true, this field
  // should be included in the workload management table DMLs. If returning false, this
  // field is too new and must be ignored.
  bool Include(const kudu::Version& target_schema_version) const {
    return schema_version <= target_schema_version;
  }
}; // class FieldDefinition

/// List of query table columns. Must be kept in-sync with SystemTables.thrift
extern const std::map<TQueryTableColumn::type, FieldDefinition> FIELD_DEFINITIONS;

/// Determines if the specified column is part of the configured workload management
/// target schema version and thus should be included when creating/updating the workload
/// management tables or building DML insert statements. The StartupChecks function must
/// have already been called or else a DCHECK will fail.
bool IncludeField(const TQueryTableColumn::type& col);

} // namespace workloadmgmt
} // namespace impala
