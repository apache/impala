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

namespace cpp impala
namespace java org.apache.impala.thrift

# Must be kept in-sync with workload-management-fields.cc
# Used as column names, so do not change existing enums.
# When adding new columns, review the default for query_log_max_queued to maintain
#   query_log_max_queued * len(TQueryTableColumn) < statement_expression_limit(250k)
enum TQueryTableColumn {
    CLUSTER_ID
    QUERY_ID
    SESSION_ID
    SESSION_TYPE
    HIVESERVER2_PROTOCOL_VERSION
    DB_USER
    DB_USER_CONNECTION
    DB_NAME
    IMPALA_COORDINATOR
    QUERY_STATUS
    QUERY_STATE
    IMPALA_QUERY_END_STATE
    QUERY_TYPE
    NETWORK_ADDRESS
    START_TIME_UTC
    TOTAL_TIME_MS
    QUERY_OPTS_CONFIG
    RESOURCE_POOL
    PER_HOST_MEM_ESTIMATE
    DEDICATED_COORD_MEM_ESTIMATE
    PER_HOST_FRAGMENT_INSTANCES
    BACKENDS_COUNT
    ADMISSION_RESULT
    CLUSTER_MEMORY_ADMITTED
    EXECUTOR_GROUP
    EXECUTOR_GROUPS
    EXEC_SUMMARY
    NUM_ROWS_FETCHED
    ROW_MATERIALIZATION_ROWS_PER_SEC
    ROW_MATERIALIZATION_TIME_MS
    COMPRESSED_BYTES_SPILLED
    EVENT_PLANNING_FINISHED
    EVENT_SUBMIT_FOR_ADMISSION
    EVENT_COMPLETED_ADMISSION
    EVENT_ALL_BACKENDS_STARTED
    EVENT_ROWS_AVAILABLE
    EVENT_FIRST_ROW_FETCHED
    EVENT_LAST_ROW_FETCHED
    EVENT_UNREGISTER_QUERY
    READ_IO_WAIT_TOTAL_MS
    READ_IO_WAIT_MEAN_MS
    BYTES_READ_CACHE_TOTAL
    BYTES_READ_TOTAL
    PERNODE_PEAK_MEM_MIN
    PERNODE_PEAK_MEM_MAX
    PERNODE_PEAK_MEM_MEAN
    SQL
    PLAN
    TABLES_QUERIED
}
