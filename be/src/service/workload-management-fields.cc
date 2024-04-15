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

// This file contains the definition of the FIELD_DEFINITIONS list from the associated
// header file. Each field definition consists of the database column name for the field,
// the sql type of the database column, and a function that extracts the actual value from
// a `QueryStateExpanded` instance and writes it to the stream that is collecting all the
// values for the insert dml.

#include "service/workload-management.h"

#include <algorithm>
#include <string>
#include <utility>

#include <boost/algorithm/string.hpp>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gutil/strings/substitute.h>

#include "common/compiler-util.h"
#include "gen-cpp/Types_types.h"
#include "runtime/exec-env.h"
#include "service/query-options.h"
#include "service/query-state-record.h"
#include "util/debug-util.h"
#include "util/network-util.h"
#include "util/sql-util.h"

DECLARE_int32(query_log_max_sql_length);
DECLARE_int32(query_log_max_plan_length);

using namespace std;
using strings::Substitute;

namespace impala {

namespace workload_management {

/// Helper type for event timeline timestamp functions.
using _event_compare_pred = function<bool(const string& comp)>;

// Constant declaring how to convert from micro and nano seconds to milliseconds.
static constexpr double MICROS_TO_MILLIS = 1000;
static constexpr double NANOS_TO_MILLIS = 1000000;

// Constants declaring how durations measured in milliseconds will be stored in the table.
// Must match the constants with the same name declared in SystemTable.java.
static constexpr int8_t DURATION_DECIMAL_PRECISION = 18;
static constexpr int8_t DURATION_DECIMAL_SCALE = 3;

// SQL column type for duration columns.
static const string MILLIS_DECIMAL_TYPE = Substitute("DECIMAL($0,$1)",
    DURATION_DECIMAL_PRECISION, DURATION_DECIMAL_SCALE);

/// Helper function to write a decimal value to the completed queries sql stream.
///
/// Parameters:
///   `ctx`     The field parse context object.
///   `val`     A value to write in the completed queries sql stream.
///   `factor`  The provided val input will be divided by this value.
static void _write_decimal(FieldParserContext& ctx, int64_t val, double factor) {
  ctx.sql << "CAST(" << val / factor << " AS " << MILLIS_DECIMAL_TYPE << ")";
}

/// Helper function to write the timestamp for a single event from the events timeline
/// into the completed queries sql statement stream.
///
/// Parameters:
///   `ctx`  The field parse context object.
///   `target_event` The element from the QueryEvent enum that represents the event being
///                  inserted into the sql statement. The corresponding event value will
///                  be retrieved from the map of query events.
static void _write_event(FieldParserContext& ctx, QueryEvent target_event) {
  const auto& event = ctx.record->events.find(target_event);
  DCHECK(event != ctx.record->events.end());
  _write_decimal(ctx, event->second, NANOS_TO_MILLIS);
}

/// List of query table columns. Must be kept in-sync with SystemTables.thrift
const array<FieldDefinition, NumQueryTableColumns> FIELD_DEFINITIONS{{
    // Cluster Id
    // Required
    FieldDefinition(TQueryTableColumn::CLUSTER_ID, TPrimitiveType::STRING,
        [](FieldParserContext& ctx){
          ctx.sql << "'" << ctx.cluster_id << "'";
        }),

    // Query Id
    FieldDefinition(TQueryTableColumn::QUERY_ID, TPrimitiveType::STRING,
        [](FieldParserContext& ctx){
          ctx.sql << "'" << PrintId(ctx.record->base_state->id) << "'";
        }),

    // Session Id
    FieldDefinition(TQueryTableColumn::SESSION_ID, TPrimitiveType::STRING,
        [](FieldParserContext& ctx){
          ctx.sql << "'" << PrintId(ctx.record->session_id) << "'";
        }),

    // Session Type
    FieldDefinition(TQueryTableColumn::SESSION_TYPE, TPrimitiveType::STRING,
        [](FieldParserContext& ctx){
          ctx.sql << "'" << ctx.record->session_type << "'";
        }),

    // Hiveserver2 Protocol Version
    FieldDefinition(TQueryTableColumn::HIVESERVER2_PROTOCOL_VERSION,
        TPrimitiveType::STRING, [](FieldParserContext& ctx){
          ctx.sql << "'";
          if (ctx.record->session_type == TSessionType::HIVESERVER2) {
            ctx.sql << ctx.record->hiveserver2_protocol_version;
          }
          ctx.sql << "'";
        }),

    // Effective User
    FieldDefinition(TQueryTableColumn::DB_USER, TPrimitiveType::STRING,
        [](FieldParserContext& ctx){
          ctx.sql << "'" << ctx.record->base_state->effective_user << "'";
        }),

    // DB User
    FieldDefinition(TQueryTableColumn::DB_USER_CONNECTION, TPrimitiveType::STRING,
        [](FieldParserContext& ctx){
          ctx.sql << "'" << ctx.record->db_user_connection << "'";
        }),

    // Default DB
    FieldDefinition(TQueryTableColumn::DB_NAME, TPrimitiveType::STRING,
        [](FieldParserContext& ctx){
          ctx.sql << "'" << ctx.record->base_state->default_db << "'";
        }),

    // Impala Coordinator
    FieldDefinition(TQueryTableColumn::IMPALA_COORDINATOR, TPrimitiveType::STRING,
        [](FieldParserContext& ctx){
          ctx.sql << "'" <<TNetworkAddressToString(
              ExecEnv::GetInstance()->configured_backend_address()) << "'";
        }),

    // Query Status
    FieldDefinition(TQueryTableColumn::QUERY_STATUS, TPrimitiveType::STRING,
        [](FieldParserContext& ctx){
          ctx.sql << "'";
          if (ctx.record->base_state->query_status.ok()) {
            ctx.sql << "OK";
          } else {
            ctx.sql << EscapeSql(ctx.record->base_state->query_status.msg().msg());
          }
          ctx.sql << "'";
        }),

    // Query State
    FieldDefinition(TQueryTableColumn::QUERY_STATE, TPrimitiveType::STRING,
        [](FieldParserContext& ctx){
          ctx.sql << "'" << ctx.record->base_state->query_state << "'";
        }),

    // Impala Query End State
    FieldDefinition(TQueryTableColumn::IMPALA_QUERY_END_STATE, TPrimitiveType::STRING,
        [](FieldParserContext& ctx){
          ctx.sql << "'" << ctx.record->impala_query_end_state << "'";
        }),

    // Query Type
    FieldDefinition(TQueryTableColumn::QUERY_TYPE, TPrimitiveType::STRING,
        [](FieldParserContext& ctx){
          ctx.sql << "'" << ctx.record->base_state->stmt_type << "'";
        }),

    // Client Network Address
    FieldDefinition(TQueryTableColumn::NETWORK_ADDRESS, TPrimitiveType::STRING,
        [](FieldParserContext& ctx){
          ctx.sql << "'" << TNetworkAddressToString(ctx.record->client_address) << "'";
        }),

    // Query Start Time in UTC
    // Required
    FieldDefinition(TQueryTableColumn::START_TIME_UTC, TPrimitiveType::TIMESTAMP,
        [](FieldParserContext& ctx){
          ctx.sql << "UNIX_MICROS_TO_UTC_TIMESTAMP(" <<
              ctx.record->base_state->start_time_us << ")";
        }),

    // Query Duration
    FieldDefinition(TQueryTableColumn::TOTAL_TIME_MS, TPrimitiveType::DECIMAL,
        [](FieldParserContext& ctx){
          _write_decimal(ctx, (ctx.record->base_state->end_time_us -
              ctx.record->base_state->start_time_us), MICROS_TO_MILLIS);
        }, DURATION_DECIMAL_PRECISION, DURATION_DECIMAL_SCALE),

    // Query Options set by Configuration
    FieldDefinition(TQueryTableColumn::QUERY_OPTS_CONFIG, TPrimitiveType::STRING,
        [](FieldParserContext& ctx){
          const string opts_str = DebugQueryOptions(ctx.record->query_options);
          ctx.sql << "'" << EscapeSql(opts_str) << "'";
        }),

    // Resource Pool
    FieldDefinition(TQueryTableColumn::RESOURCE_POOL, TPrimitiveType::STRING,
        [](FieldParserContext& ctx){
          ctx.sql << "'" << EscapeSql(ctx.record->base_state->resource_pool) << "'";
        }),

    // Per-host Memory Estimate
    FieldDefinition(TQueryTableColumn::PER_HOST_MEM_ESTIMATE, TPrimitiveType::BIGINT,
        [](FieldParserContext& ctx){
          ctx.sql << ctx.record->per_host_mem_estimate;
        }),

    // Dedicated Coordinator Memory Estimate
    FieldDefinition(TQueryTableColumn::DEDICATED_COORD_MEM_ESTIMATE,
        TPrimitiveType::BIGINT, [](FieldParserContext& ctx){
          ctx.sql << ctx.record->dedicated_coord_mem_estimate;
        }),

    // Per-Host Fragment Instances
    FieldDefinition(TQueryTableColumn::PER_HOST_FRAGMENT_INSTANCES,
        TPrimitiveType::STRING, [](FieldParserContext& ctx){
          ctx.sql << "'";

          if (!ctx.record->per_host_state.empty()) {
            for (const auto& iter : ctx.record->per_host_state) {
              ctx.sql << TNetworkAddressToString(iter.first) << "=" <<
                  iter.second.fragment_instance_count << ',';
            }
            ctx.sql.move_back();
          }

          ctx.sql << "'";
        }),

    // Backends Count
    FieldDefinition(TQueryTableColumn::BACKENDS_COUNT, TPrimitiveType::INT,
        [](FieldParserContext& ctx){
          if (ctx.record->per_host_state.empty()) {
            ctx.sql << 0;
          } else {
            ctx.sql << ctx.record->per_host_state.size();
          }
        }),

    // Admission Result
    FieldDefinition(TQueryTableColumn::ADMISSION_RESULT, TPrimitiveType::STRING,
        [](FieldParserContext& ctx){
          ctx.sql << "'" << ctx.record->admission_result << "'";
        }),

    // Cluster Memory Admitted
    FieldDefinition(TQueryTableColumn::CLUSTER_MEMORY_ADMITTED, TPrimitiveType::BIGINT,
        [](FieldParserContext& ctx){
          ctx.sql << ctx.record->base_state->cluster_mem_est;
        }),

    // Executor Group
    FieldDefinition(TQueryTableColumn::EXECUTOR_GROUP, TPrimitiveType::STRING,
        [](FieldParserContext& ctx){
          ctx.sql << "'" << ctx.record->executor_group << "'";
        }),

    // Executor Groups
    FieldDefinition(TQueryTableColumn::EXECUTOR_GROUPS, TPrimitiveType::STRING,
        [](FieldParserContext& ctx){
          ctx.sql << "'" << EscapeSql(ctx.record->executor_groups) << "'";
        }),

    // Exec Summary (also known as the operator summary)
    FieldDefinition(TQueryTableColumn::EXEC_SUMMARY, TPrimitiveType::STRING,
        [](FieldParserContext& ctx){
          ctx.sql << "'" << EscapeSql(ctx.record->exec_summary) << "'";
        }),

    // Number of rows fetched
    FieldDefinition(TQueryTableColumn::NUM_ROWS_FETCHED, TPrimitiveType::BIGINT,
        [](FieldParserContext& ctx){
          ctx.sql << ctx.record->base_state->num_rows_fetched;
        }),

    // Row Materialization Rate
    FieldDefinition(TQueryTableColumn::ROW_MATERIALIZATION_ROWS_PER_SEC,
        TPrimitiveType::BIGINT, [](FieldParserContext& ctx){
          ctx.sql << ctx.record->row_materialization_rate;
        }),

    // Row Materialization Time
    FieldDefinition(TQueryTableColumn::ROW_MATERIALIZATION_TIME_MS,
        TPrimitiveType::DECIMAL, [](FieldParserContext& ctx){
          _write_decimal(ctx, ctx.record->row_materialization_time, NANOS_TO_MILLIS);
        }, DURATION_DECIMAL_PRECISION, DURATION_DECIMAL_SCALE),

    // Compressed Bytes Spilled to Disk
    FieldDefinition(TQueryTableColumn::COMPRESSED_BYTES_SPILLED, TPrimitiveType::BIGINT,
        [](FieldParserContext& ctx){
          ctx.sql << ctx.record->compressed_bytes_spilled;
        }),

    // Events Timeline Planning Finished
    FieldDefinition(TQueryTableColumn::EVENT_PLANNING_FINISHED, TPrimitiveType::DECIMAL,
        [](FieldParserContext& ctx){
          _write_event(ctx, PLANNING_FINISHED);
        }, DURATION_DECIMAL_PRECISION, DURATION_DECIMAL_SCALE),

    // Events Timeline Submit for Admission
    FieldDefinition(TQueryTableColumn::EVENT_SUBMIT_FOR_ADMISSION,
        TPrimitiveType::DECIMAL, [](FieldParserContext& ctx){
          _write_event(ctx, SUBMIT_FOR_ADMISSION);
        }, DURATION_DECIMAL_PRECISION, DURATION_DECIMAL_SCALE),

    // Events Timeline Completed Admission
    FieldDefinition(TQueryTableColumn::EVENT_COMPLETED_ADMISSION,
        TPrimitiveType::DECIMAL, [](FieldParserContext& ctx){
          _write_event(ctx, COMPLETED_ADMISSION);
        }, DURATION_DECIMAL_PRECISION, DURATION_DECIMAL_SCALE),

    // Events Timeline All Execution Backends Started
    FieldDefinition(TQueryTableColumn::EVENT_ALL_BACKENDS_STARTED,
        TPrimitiveType::DECIMAL, [](FieldParserContext& ctx){
          _write_event(ctx, ALL_BACKENDS_STARTED);
        }, DURATION_DECIMAL_PRECISION, DURATION_DECIMAL_SCALE),

    // Events Timeline Rows Available
    FieldDefinition(TQueryTableColumn::EVENT_ROWS_AVAILABLE, TPrimitiveType::DECIMAL,
        [](FieldParserContext& ctx){
          _write_event(ctx, ROWS_AVAILABLE);
        }, DURATION_DECIMAL_PRECISION, DURATION_DECIMAL_SCALE),

    // Events Timeline First Row Fetched
    FieldDefinition(TQueryTableColumn::EVENT_FIRST_ROW_FETCHED, TPrimitiveType::DECIMAL,
        [](FieldParserContext& ctx){
          _write_event(ctx, FIRST_ROW_FETCHED);
        }, DURATION_DECIMAL_PRECISION, DURATION_DECIMAL_SCALE),

    // Events Timeline Last Row Fetched
    FieldDefinition(TQueryTableColumn::EVENT_LAST_ROW_FETCHED, TPrimitiveType::DECIMAL,
        [](FieldParserContext& ctx){
          _write_event(ctx, LAST_ROW_FETCHED);
        }, DURATION_DECIMAL_PRECISION, DURATION_DECIMAL_SCALE),

    // Events Timeline Unregister Query
    FieldDefinition(TQueryTableColumn::EVENT_UNREGISTER_QUERY, TPrimitiveType::DECIMAL,
        [](FieldParserContext& ctx){
          _write_event(ctx, UNREGISTER_QUERY);
        }, DURATION_DECIMAL_PRECISION, DURATION_DECIMAL_SCALE),

    // Read IO Wait Time Total
    FieldDefinition(TQueryTableColumn::READ_IO_WAIT_TOTAL_MS, TPrimitiveType::DECIMAL,
        [](FieldParserContext& ctx){
          _write_decimal(ctx, ctx.record->read_io_wait_time_total, NANOS_TO_MILLIS);
        }, DURATION_DECIMAL_PRECISION, DURATION_DECIMAL_SCALE),

    // Read IO Wait Time Mean
    FieldDefinition(TQueryTableColumn::READ_IO_WAIT_MEAN_MS, TPrimitiveType::DECIMAL,
        [](FieldParserContext& ctx){
          _write_decimal(ctx, ctx.record->read_io_wait_time_mean, NANOS_TO_MILLIS);
        }, DURATION_DECIMAL_PRECISION, DURATION_DECIMAL_SCALE),

    // Bytes Read from the Data Cache Total
    FieldDefinition(TQueryTableColumn::BYTES_READ_CACHE_TOTAL, TPrimitiveType::BIGINT,
        [](FieldParserContext& ctx){
          ctx.sql << ctx.record->bytes_read_cache_total;
        }),

    // Bytes Read Total
    FieldDefinition(TQueryTableColumn::BYTES_READ_TOTAL, TPrimitiveType::BIGINT,
        [](FieldParserContext& ctx){
          ctx.sql << ctx.record->bytes_read_total;
        }),

    // Per-Node Peak Memory Usage Min
    FieldDefinition(TQueryTableColumn::PERNODE_PEAK_MEM_MIN, TPrimitiveType::BIGINT,
        [](FieldParserContext& ctx){
          auto min_elem = min_element(ctx.record->per_host_state.cbegin(),
              ctx.record->per_host_state.cend(), PerHostPeakMemoryComparator);

          if (LIKELY(min_elem != ctx.record->per_host_state.cend())) {
            ctx.sql << min_elem->second.peak_memory_usage;
          } else {
            ctx.sql << 0;
          }
        }),

    // Per-Node Peak Memory Usage Max
    FieldDefinition(TQueryTableColumn::PERNODE_PEAK_MEM_MAX, TPrimitiveType::BIGINT,
        [](FieldParserContext& ctx){
          auto max_elem = max_element(ctx.record->per_host_state.cbegin(),
              ctx.record->per_host_state.cend(), PerHostPeakMemoryComparator);

          if (UNLIKELY(max_elem == ctx.record->per_host_state.cend())) {
            ctx.sql << 0;
          } else {
            ctx.sql << max_elem->second.peak_memory_usage;
          }
        }),

    // Per-Node Peak Memory Usage Mean
    FieldDefinition(TQueryTableColumn::PERNODE_PEAK_MEM_MEAN, TPrimitiveType::BIGINT,
        [](FieldParserContext& ctx){
          int64_t calc_mean = 0;

          if (LIKELY(!ctx.record->per_host_state.empty())) {
            for (const auto& host : ctx.record->per_host_state) {
              calc_mean += host.second.peak_memory_usage;
            }

            calc_mean = calc_mean / ctx.record->per_host_state.size();
          }

          ctx.sql << calc_mean;
        }),

    // SQL Statement
    FieldDefinition(TQueryTableColumn::SQL, TPrimitiveType::STRING,
        [](FieldParserContext& ctx){
          ctx.sql << "'" <<
              EscapeSql(ctx.record->redacted_sql, FLAGS_query_log_max_sql_length) << "'";
        }),

    // Query Plan
    FieldDefinition(TQueryTableColumn::PLAN, TPrimitiveType::STRING,
        [](FieldParserContext& ctx){
          ctx.sql << "'"
              << EscapeSql(ctx.record->base_state->plan, FLAGS_query_log_max_plan_length)
              << "'";
        }),

    // Tables Queried
    FieldDefinition(TQueryTableColumn::TABLES_QUERIED, TPrimitiveType::STRING,
        [](FieldParserContext& ctx){
          ctx.sql << "'" << PrintTableList(ctx.record->tables) << "'";
        }),

    }}; // FIELDS_PARSERS const array

} //namespace workload_management

} // namespace impala
