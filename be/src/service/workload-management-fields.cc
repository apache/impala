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
const list<FieldDefinition> FIELD_DEFINITIONS = {
    // Cluster Id
    // Required
    FieldDefinition("cluster_id", TPrimitiveType::STRING,
        [](FieldParserContext& ctx){
          ctx.sql << "'" << ctx.cluster_id << "'";
        }),

    // Query Id
    FieldDefinition("query_id", TPrimitiveType::STRING,
        [](FieldParserContext& ctx){
          ctx.sql << "'" << PrintId(ctx.record->base_state->id) << "'";
        }),

    // Session Id
    FieldDefinition("session_id", TPrimitiveType::STRING,
        [](FieldParserContext& ctx){
          ctx.sql << "'" << PrintId(ctx.record->session_id) << "'";
        }),

    // Session Type
    FieldDefinition("session_type", TPrimitiveType::STRING,
        [](FieldParserContext& ctx){
          ctx.sql << "'" << ctx.record->session_type << "'";
        }),

    // Hiveserver2 Protocol Version
    FieldDefinition("hiveserver2_protocol_version", TPrimitiveType::STRING,
        [](FieldParserContext& ctx){
          ctx.sql << "'";
          if (ctx.record->session_type == TSessionType::HIVESERVER2) {
            ctx.sql << ctx.record->hiveserver2_protocol_version;
          }
          ctx.sql << "'";
        }),

    // Effective User
    FieldDefinition("db_user", TPrimitiveType::STRING,
        [](FieldParserContext& ctx){
          ctx.sql << "'" << ctx.record->base_state->effective_user << "'";
        }),

    // DB User
    FieldDefinition("db_user_connection", TPrimitiveType::STRING,
        [](FieldParserContext& ctx){
          ctx.sql << "'" << ctx.record->db_user_connection << "'";
        }),

    // Default DB
    FieldDefinition("db_name", TPrimitiveType::STRING,
        [](FieldParserContext& ctx){
          ctx.sql << "'" << ctx.record->base_state->default_db << "'";
        }),

    // Impala Coordinator
    FieldDefinition("impala_coordinator", TPrimitiveType::STRING,
        [](FieldParserContext& ctx){
          ctx.sql << "'" <<TNetworkAddressToString(
              ExecEnv::GetInstance()->configured_backend_address()) << "'";
        }),

    // Query Status
    FieldDefinition("query_status", TPrimitiveType::STRING,
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
    FieldDefinition("query_state", TPrimitiveType::STRING,
        [](FieldParserContext& ctx){
          ctx.sql << "'" << ctx.record->base_state->query_state << "'";
        }),

    // Impala Query End State
    FieldDefinition("impala_query_end_state",
        TPrimitiveType::STRING, [](FieldParserContext& ctx){
          ctx.sql << "'" << ctx.record->impala_query_end_state << "'";
        }),

    // Query Type
    FieldDefinition("query_type", TPrimitiveType::STRING,
        [](FieldParserContext& ctx){
          ctx.sql << "'" << ctx.record->base_state->stmt_type << "'";
        }),

    // Client Network Address
    FieldDefinition("network_address", TPrimitiveType::STRING,
        [](FieldParserContext& ctx){
          ctx.sql << "'" << TNetworkAddressToString(ctx.record->client_address) << "'";
        }),

    // Query Start Time in UTC
    // Required
    FieldDefinition("start_time_utc", TPrimitiveType::TIMESTAMP,
        [](FieldParserContext& ctx){
          ctx.sql << "UNIX_MICROS_TO_UTC_TIMESTAMP(" <<
              ctx.record->base_state->start_time_us << ")";
        }),

    // Query Duration
    FieldDefinition("total_time_ms", TPrimitiveType::DECIMAL,
        [](FieldParserContext& ctx){
          _write_decimal(ctx, (ctx.record->base_state->end_time_us -
              ctx.record->base_state->start_time_us), MICROS_TO_MILLIS);
        }, DURATION_DECIMAL_PRECISION, DURATION_DECIMAL_SCALE),

    // Query Options set by Configuration
    FieldDefinition("query_opts_config", TPrimitiveType::STRING,
        [](FieldParserContext& ctx){
          const string opts_str = DebugQueryOptions(ctx.record->query_options);
          ctx.sql << "'" << EscapeSql(opts_str) << "'";
        }),

    // Resource Pool
    FieldDefinition("resource_pool", TPrimitiveType::STRING,
        [](FieldParserContext& ctx){
          ctx.sql << "'" << EscapeSql(ctx.record->base_state->resource_pool) << "'";
        }),

    // Per-host Memory Estimate
    FieldDefinition("per_host_mem_estimate", TPrimitiveType::BIGINT,
        [](FieldParserContext& ctx){
          ctx.sql << ctx.record->per_host_mem_estimate;
        }),

    // Dedicated Coordinator Memory Estimate
    FieldDefinition("dedicated_coord_mem_estimate", TPrimitiveType::BIGINT,
        [](FieldParserContext& ctx){
          ctx.sql << ctx.record->dedicated_coord_mem_estimate;
        }),

    // Per-Host Fragment Instances
    FieldDefinition("per_host_fragment_instances", TPrimitiveType::STRING,
        [](FieldParserContext& ctx){
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
    FieldDefinition("backends_count", TPrimitiveType::INT,
        [](FieldParserContext& ctx){
          if (ctx.record->per_host_state.empty()) {
            ctx.sql << 0;
          } else {
            ctx.sql << ctx.record->per_host_state.size();
          }
        }),

    // Admission Result
    FieldDefinition("admission_result", TPrimitiveType::STRING,
        [](FieldParserContext& ctx){
          ctx.sql << "'" << ctx.record->admission_result << "'";
        }),

    // Cluster Memory Admitted
    FieldDefinition("cluster_memory_admitted", TPrimitiveType::BIGINT,
        [](FieldParserContext& ctx){
          ctx.sql << ctx.record->base_state->cluster_mem_est;
        }),

    // Executor Group
    FieldDefinition("executor_group", TPrimitiveType::STRING,
        [](FieldParserContext& ctx){
          ctx.sql << "'" << ctx.record->executor_group << "'";
        }),

    // Executor Groups
    FieldDefinition("executor_groups", TPrimitiveType::STRING,
        [](FieldParserContext& ctx){
          ctx.sql << "'" << EscapeSql(ctx.record->executor_groups) << "'";
        }),

    // Exec Summary (also known as the operator summary)
    FieldDefinition("exec_summary", TPrimitiveType::STRING,
        [](FieldParserContext& ctx){
          ctx.sql << "'" << EscapeSql(ctx.record->exec_summary) << "'";
        }),

    // Number of rows fetched
    FieldDefinition("num_rows_fetched", TPrimitiveType::BIGINT,
        [](FieldParserContext& ctx){
          ctx.sql << ctx.record->base_state->num_rows_fetched;
        }),

    // Row Materialization Rate
    FieldDefinition("row_materialization_rows_per_sec", TPrimitiveType::BIGINT,
        [](FieldParserContext& ctx){
          ctx.sql << ctx.record->row_materialization_rate;
        }),

    // Row Materialization Time
    FieldDefinition("row_materialization_time_ms", TPrimitiveType::DECIMAL,
        [](FieldParserContext& ctx){
          _write_decimal(ctx, ctx.record->row_materialization_time, NANOS_TO_MILLIS);
        }, DURATION_DECIMAL_PRECISION, DURATION_DECIMAL_SCALE),

    // Compressed Bytes Spilled to Disk
    FieldDefinition("compressed_bytes_spilled", TPrimitiveType::BIGINT,
        [](FieldParserContext& ctx){
          ctx.sql << ctx.record->compressed_bytes_spilled;
        }),

    // Events Timeline Planning Finished
    FieldDefinition("event_planning_finished", TPrimitiveType::DECIMAL,
        [](FieldParserContext& ctx){
          _write_event(ctx, PLANNING_FINISHED);
        }, DURATION_DECIMAL_PRECISION, DURATION_DECIMAL_SCALE),

    // Events Timeline Submit for Admission
    FieldDefinition("event_submit_for_admission", TPrimitiveType::DECIMAL,
        [](FieldParserContext& ctx){
          _write_event(ctx, SUBMIT_FOR_ADMISSION);
        }, DURATION_DECIMAL_PRECISION, DURATION_DECIMAL_SCALE),

    // Events Timeline Completed Admission
    FieldDefinition("event_completed_admission", TPrimitiveType::DECIMAL,
        [](FieldParserContext& ctx){
          _write_event(ctx, COMPLETED_ADMISSION);
        }, DURATION_DECIMAL_PRECISION, DURATION_DECIMAL_SCALE),

    // Events Timeline All Execution Backends Started
    FieldDefinition("event_all_backends_started", TPrimitiveType::DECIMAL,
        [](FieldParserContext& ctx){
          _write_event(ctx, ALL_BACKENDS_STARTED);
        }, DURATION_DECIMAL_PRECISION, DURATION_DECIMAL_SCALE),

    // Events Timeline Rows Available
    FieldDefinition("event_rows_available", TPrimitiveType::DECIMAL,
        [](FieldParserContext& ctx){
          _write_event(ctx, ROWS_AVAILABLE);
        }, DURATION_DECIMAL_PRECISION, DURATION_DECIMAL_SCALE),

    // Events Timeline First Row Fetched
    FieldDefinition("event_first_row_fetched", TPrimitiveType::DECIMAL,
        [](FieldParserContext& ctx){
          _write_event(ctx, FIRST_ROW_FETCHED);
        }, DURATION_DECIMAL_PRECISION, DURATION_DECIMAL_SCALE),

    // Events Timeline Last Row Fetched
    FieldDefinition("event_last_row_fetched", TPrimitiveType::DECIMAL,
        [](FieldParserContext& ctx){
          _write_event(ctx, LAST_ROW_FETCHED);
        }, DURATION_DECIMAL_PRECISION, DURATION_DECIMAL_SCALE),

    // Events Timeline Unregister Query
    FieldDefinition("event_unregister_query", TPrimitiveType::DECIMAL,
        [](FieldParserContext& ctx){
          _write_event(ctx, UNREGISTER_QUERY);
        }, DURATION_DECIMAL_PRECISION, DURATION_DECIMAL_SCALE),

    // Read IO Wait Time Total
    FieldDefinition("read_io_wait_total_ms", TPrimitiveType::DECIMAL,
        [](FieldParserContext& ctx){
          _write_decimal(ctx, ctx.record->read_io_wait_time_total, NANOS_TO_MILLIS);
        }, DURATION_DECIMAL_PRECISION, DURATION_DECIMAL_SCALE),

    // Read IO Wait Time Mean
    FieldDefinition("read_io_wait_mean_ms", TPrimitiveType::DECIMAL,
        [](FieldParserContext& ctx){
          _write_decimal(ctx, ctx.record->read_io_wait_time_mean, NANOS_TO_MILLIS);
        }, DURATION_DECIMAL_PRECISION, DURATION_DECIMAL_SCALE),

    // Bytes Read from the Data Cache Total
    FieldDefinition("bytes_read_cache_total", TPrimitiveType::BIGINT,
        [](FieldParserContext& ctx){
          ctx.sql << ctx.record->bytes_read_cache_total;
        }),

    // Bytes Read Total
    FieldDefinition("bytes_read_total", TPrimitiveType::BIGINT,
        [](FieldParserContext& ctx){
          ctx.sql << ctx.record->bytes_read_total;
        }),

    // Per-Node Peak Memory Usage Min
    FieldDefinition("pernode_peak_mem_min", TPrimitiveType::BIGINT,
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
    FieldDefinition("pernode_peak_mem_max", TPrimitiveType::BIGINT,
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
    FieldDefinition("pernode_peak_mem_mean", TPrimitiveType::BIGINT,
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
    FieldDefinition("sql", TPrimitiveType::STRING,
        [](FieldParserContext& ctx){
          ctx.sql << "'" <<
              EscapeSql(ctx.record->redacted_sql, FLAGS_query_log_max_sql_length) << "'";
        }),

    // Query Plan
    FieldDefinition("plan", TPrimitiveType::STRING,
        [](FieldParserContext& ctx){
          ctx.sql << "'"
              << EscapeSql(ctx.record->base_state->plan, FLAGS_query_log_max_plan_length)
              << "'";
        }),

    // Tables Queried
    FieldDefinition("tables_queried", TPrimitiveType::STRING,
        [](FieldParserContext& ctx){
          ctx.sql << "'" << PrintTableList(ctx.record->tables) << "'";
        }),

    }; // FIELDS_PARSERS constant list

} //namespace workload_management

} // namespace impala
