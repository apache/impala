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

#include "system-table-scanner.h"

#include <memory>
#include <boost/algorithm/string.hpp>
#include <gutil/strings/substitute.h>

#include "gen-cpp/SystemTables_types.h"
#include "runtime/decimal-value.h"
#include "runtime/decimal-value.inline.h"
#include "runtime/exec-env.h"
#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"
#include "runtime/query-driver.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/timestamp-value.h"
#include "runtime/timestamp-value.inline.h"
#include "runtime/tuple-row.h"
#include "service/query-state-record.h"
#include "util/debug-util.h"
#include "common/names.h"

using namespace boost::algorithm;
using strings::Substitute;

DECLARE_string(cluster_id);

namespace impala {

static const string ERROR_MEM_LIMIT_EXCEEDED =
    "SystemTableScanNode::$0() failed to allocate $1 bytes.";

// Constant declaring how to convert from micro and nano seconds to milliseconds.
static constexpr double MICROS_TO_MILLIS = 1000;
static constexpr double NANOS_TO_MILLIS = 1000000;

Status SystemTableScanner::CreateScanner(RuntimeState* state, RuntimeProfile* profile,
    TSystemTableName::type table_name, std::unique_ptr<SystemTableScanner>* scanner) {
  switch (table_name) {
    case TSystemTableName::IMPALA_QUERY_LIVE:
      *scanner = make_unique<QueryScanner>(state, profile);
      break;
    default:
      return Status(ErrorMsg(TErrorCode::NOT_IMPLEMENTED_ERROR,
          Substitute("Unknown table name: $0", table_name)));
  }
  return Status::OK();
}

/// Write a string value to a STRING slot, allocating memory from 'pool'. Returns
/// an error if memory cannot be allocated without exceeding a memory limit.
Status SystemTableScanner::WriteStringSlot(
    const char* data, int len, MemPool* pool, void* slot) {
  char* buffer = reinterpret_cast<char*>(pool->TryAllocateUnaligned(len));
  if (UNLIKELY(buffer == nullptr)) {
    string details = Substitute(ERROR_MEM_LIMIT_EXCEEDED, "WriteStringSlot", len);
    return pool->mem_tracker()->MemLimitExceeded(state_, details, len);
  }
  memcpy(buffer, data, len);
  reinterpret_cast<StringValue*>(slot)->Assign(buffer, len);
  return Status::OK();
}

Status SystemTableScanner::WriteStringSlot(const string& str, MemPool* pool, void* slot) {
  return WriteStringSlot(str.data(), str.size(), pool, slot);
}

static void WriteUnixTimestampSlot(int64_t unix_time_micros, void* slot) {
  *reinterpret_cast<TimestampValue*>(slot) =
      TimestampValue::UtcFromUnixTimeMicros(unix_time_micros);
}

static void WriteBigIntSlot(int64_t value, void* slot) {
  *reinterpret_cast<int64_t*>(slot) = value;
}

static void WriteIntSlot(int32_t value, void* slot) {
  *reinterpret_cast<int32_t*>(slot) = value;
}

static void WriteDecimalSlot(
    const ColumnType& type, double value, void* slot) {
  bool overflow = false;
  switch (type.GetByteSize()) {
    case 4:
      *reinterpret_cast<Decimal4Value*>(slot) =
          Decimal4Value::FromDouble(type, value, false, &overflow);
    case 8:
      *reinterpret_cast<Decimal8Value*>(slot) =
          Decimal8Value::FromDouble(type, value, false, &overflow);
    case 16:
      *reinterpret_cast<Decimal16Value*>(slot) =
          Decimal16Value::FromDouble(type, value, false, &overflow);
  }
  DCHECK(!overflow);
}

QueryScanner::QueryScanner(RuntimeState* state, RuntimeProfile* profile)
    : SystemTableScanner(state, profile),
      active_query_collection_timer_(ADD_TIMER(profile_, "ActiveQueryCollectionTime")),
      pending_query_collection_timer_(ADD_TIMER(profile_, "PendingQueryCollectionTime"))
      {}

Status QueryScanner::Open() {
  ImpalaServer* server = ExecEnv::GetInstance()->impala_server();

  // Get a sorted list of state snapshots for all active queries. This mimics the
  // behavior of ImpalaHttpHandler::QueryStateHandler. Using snapshots avoids potential
  // memory violations of trying to use a ClientRequestState pointer without holding a
  // shared_ptr to the QueryDriver and avoids keeping the query open longer than
  // necessary (via that shared_ptr). The cost is that we allocate memory for the
  // QueryStateRecords, which should be relatively small.
  {
    SCOPED_TIMER(active_query_collection_timer_);
    server->query_driver_map_.DoFuncForAllEntries(
        [&](const std::shared_ptr<QueryDriver>& query_driver) {
          query_records_.emplace_back(make_shared<QueryStateExpanded>(
              *query_driver->GetActiveClientRequestState()));
        });
  }

  unordered_set<TUniqueId> running_queries;
  running_queries.reserve(query_records_.size());
  for (const auto& r : query_records_) {
    running_queries.insert(r->base_state->id);
  }

  // It's possible for a query to appear in both query_driver_map_ and completed_queries_
  // if it's been added to completed_queries_ in CloseClientRequestState and has not yet
  // been removed from query_driver_map_ in QueryDriver::Unregister. Collection order
  // ensures we don't miss one by collecting before it's been added to completed_queries_,
  // then after it's added to completed_queries_ and removed from query_driver_map_.
  // Avoid adding entries if they already exist.
  {
    SCOPED_TIMER(pending_query_collection_timer_);
    for (const auto& r : server->GetCompletedQueries()) {
      if (running_queries.find(r->base_state->id) == running_queries.end()) {
        query_records_.emplace_back(r);
      }
    }
  }

  if (query_records_.empty()) eos_ = true;
  return Status::OK();
}

static void WriteEvent(const QueryStateExpanded& query, const SlotDescriptor* slot_desc,
    void* slot, QueryEvent name) {
  const auto& event = query.events.find(name);
  DCHECK(event != query.events.end());
  WriteDecimalSlot(slot_desc->type(), event->second / NANOS_TO_MILLIS, slot);
}

Status QueryScanner::MaterializeNextTuple(
    MemPool* pool, Tuple* tuple, const TupleDescriptor* tuple_desc) {
  DCHECK(!query_records_.empty());
  const QueryStateExpanded& query = *query_records_.front();
  const QueryStateRecord& record = *query.base_state;
  ExecEnv* exec_env = ExecEnv::GetInstance();
  // Verify there are no clustering columns (partitions) to offset col_pos.
  DCHECK_EQ(0, tuple_desc->table_desc()->num_clustering_cols());
  for (const SlotDescriptor* slot_desc : tuple_desc->slots()) {
    void* slot = tuple->GetSlot(slot_desc->tuple_offset());

    switch (slot_desc->col_pos()) {
      case TQueryTableColumn::CLUSTER_ID:
        RETURN_IF_ERROR(WriteStringSlot(FLAGS_cluster_id, pool, slot));
        break;
      case TQueryTableColumn::QUERY_ID:
        RETURN_IF_ERROR(WriteStringSlot(PrintId(record.id), pool, slot));
        break;
      case TQueryTableColumn::SESSION_ID:
        RETURN_IF_ERROR(WriteStringSlot(PrintId(query.session_id), pool, slot));
        break;
      case TQueryTableColumn::SESSION_TYPE:
        RETURN_IF_ERROR(WriteStringSlot(to_string(query.session_type), pool, slot));
        break;
      case TQueryTableColumn::HIVESERVER2_PROTOCOL_VERSION:
        if (query.session_type == TSessionType::HIVESERVER2) {
          RETURN_IF_ERROR(WriteStringSlot(
              Substitute("V$0", 1 + query.hiveserver2_protocol_version), pool, slot));
        }
        break;
      case TQueryTableColumn::DB_USER:
        RETURN_IF_ERROR(WriteStringSlot(record.effective_user, pool, slot));
        break;
      case TQueryTableColumn::DB_USER_CONNECTION:
        RETURN_IF_ERROR(WriteStringSlot(query.db_user_connection, pool, slot));
        break;
      case TQueryTableColumn::DB_NAME:
        RETURN_IF_ERROR(WriteStringSlot(record.default_db, pool, slot));
        break;
      case TQueryTableColumn::IMPALA_COORDINATOR:
        RETURN_IF_ERROR(WriteStringSlot(
            TNetworkAddressToString(exec_env->configured_backend_address()), pool, slot));
        break;
      case TQueryTableColumn::QUERY_STATUS:
        RETURN_IF_ERROR(WriteStringSlot(record.query_status.ok() ?
            "OK" : record.query_status.msg().msg(), pool, slot));
        break;
      case TQueryTableColumn::QUERY_STATE:
        RETURN_IF_ERROR(WriteStringSlot(record.query_state, pool, slot));
        break;
      case TQueryTableColumn::IMPALA_QUERY_END_STATE:
        RETURN_IF_ERROR(WriteStringSlot(query.impala_query_end_state, pool, slot));
        break;
      case TQueryTableColumn::QUERY_TYPE:
        RETURN_IF_ERROR(WriteStringSlot(to_string(record.stmt_type), pool, slot));
        break;
      case TQueryTableColumn::NETWORK_ADDRESS:
        RETURN_IF_ERROR(WriteStringSlot(
            TNetworkAddressToString(query.client_address), pool, slot));
        break;
      case TQueryTableColumn::START_TIME_UTC:
        WriteUnixTimestampSlot(record.start_time_us, slot);
        break;
      case TQueryTableColumn::TOTAL_TIME_MS: {
        const int64_t end_time_us =
            record.end_time_us > 0 ? record.end_time_us : UnixMicros();
        double duration_us = (end_time_us - record.start_time_us) / MICROS_TO_MILLIS;
        WriteDecimalSlot(slot_desc->type(), duration_us, slot);
        break;
      }
      case TQueryTableColumn::QUERY_OPTS_CONFIG:
        RETURN_IF_ERROR(WriteStringSlot(
            DebugQueryOptions(query.query_options), pool, slot));
        break;
      case TQueryTableColumn::RESOURCE_POOL:
        RETURN_IF_ERROR(WriteStringSlot(record.resource_pool, pool, slot));
        break;
      case TQueryTableColumn::PER_HOST_MEM_ESTIMATE:
        WriteBigIntSlot(query.per_host_mem_estimate, slot);
        break;
      case TQueryTableColumn::DEDICATED_COORD_MEM_ESTIMATE:
        WriteBigIntSlot(query.dedicated_coord_mem_estimate, slot);
        break;
      case TQueryTableColumn::PER_HOST_FRAGMENT_INSTANCES:
        if (!query.per_host_state.empty()) {
          stringstream ss;
          for (const auto& state : query.per_host_state) {
            ss << TNetworkAddressToString(state.first) << "="
               << state.second.fragment_instance_count << ",";
          }
          string s = ss.str();
          s.pop_back();
          RETURN_IF_ERROR(WriteStringSlot(s, pool, slot));
        }
        break;
      case TQueryTableColumn::BACKENDS_COUNT:
        DCHECK_LE(query.per_host_state.size(), numeric_limits<int32_t>::max());
        WriteIntSlot(query.per_host_state.size(), slot);
        break;
      case TQueryTableColumn::ADMISSION_RESULT:
        RETURN_IF_ERROR(WriteStringSlot(query.admission_result, pool, slot));
        break;
      case TQueryTableColumn::CLUSTER_MEMORY_ADMITTED:
        WriteBigIntSlot(record.cluster_mem_est, slot);
        break;
      case TQueryTableColumn::EXECUTOR_GROUP:
        RETURN_IF_ERROR(WriteStringSlot(query.executor_group, pool, slot));
        break;
      case TQueryTableColumn::EXECUTOR_GROUPS:
        RETURN_IF_ERROR(WriteStringSlot(query.executor_groups, pool, slot));
        break;
      case TQueryTableColumn::EXEC_SUMMARY:
        RETURN_IF_ERROR(WriteStringSlot(query.exec_summary, pool, slot));
        break;
      case TQueryTableColumn::NUM_ROWS_FETCHED:
        WriteBigIntSlot(record.num_rows_fetched, slot);
        break;
      case TQueryTableColumn::ROW_MATERIALIZATION_ROWS_PER_SEC:
        WriteBigIntSlot(query.row_materialization_rate, slot);
        break;
      case TQueryTableColumn::ROW_MATERIALIZATION_TIME_MS:
        WriteDecimalSlot(slot_desc->type(),
            query.row_materialization_time / NANOS_TO_MILLIS, slot);
        break;
      case TQueryTableColumn::COMPRESSED_BYTES_SPILLED:
        WriteBigIntSlot(query.compressed_bytes_spilled, slot);
        break;
      case TQueryTableColumn::EVENT_PLANNING_FINISHED:
        WriteEvent(query, slot_desc, slot, QueryEvent::PLANNING_FINISHED);
        break;
      case TQueryTableColumn::EVENT_SUBMIT_FOR_ADMISSION:
        WriteEvent(query, slot_desc, slot, QueryEvent::SUBMIT_FOR_ADMISSION);
        break;
      case TQueryTableColumn::EVENT_COMPLETED_ADMISSION:
        WriteEvent(query, slot_desc, slot, QueryEvent::COMPLETED_ADMISSION);
        break;
      case TQueryTableColumn::EVENT_ALL_BACKENDS_STARTED:
        WriteEvent(query, slot_desc, slot, QueryEvent::ALL_BACKENDS_STARTED);
        break;
      case TQueryTableColumn::EVENT_ROWS_AVAILABLE:
        WriteEvent(query, slot_desc, slot, QueryEvent::ROWS_AVAILABLE);
        break;
      case TQueryTableColumn::EVENT_FIRST_ROW_FETCHED:
        WriteEvent(query, slot_desc, slot, QueryEvent::FIRST_ROW_FETCHED);
        break;
      case TQueryTableColumn::EVENT_LAST_ROW_FETCHED:
        WriteEvent(query, slot_desc, slot, QueryEvent::LAST_ROW_FETCHED);
        break;
      case TQueryTableColumn::EVENT_UNREGISTER_QUERY:
        WriteEvent(query, slot_desc, slot, QueryEvent::UNREGISTER_QUERY);
        break;
      case TQueryTableColumn::READ_IO_WAIT_TOTAL_MS:
        WriteDecimalSlot(slot_desc->type(),
            query.read_io_wait_time_total / NANOS_TO_MILLIS, slot);
        break;
      case TQueryTableColumn::READ_IO_WAIT_MEAN_MS:
        WriteDecimalSlot(slot_desc->type(),
            query.read_io_wait_time_mean / NANOS_TO_MILLIS, slot);
        break;
      case TQueryTableColumn::BYTES_READ_CACHE_TOTAL:
        WriteBigIntSlot(query.bytes_read_cache_total, slot);
        break;
      case TQueryTableColumn::BYTES_READ_TOTAL:
        WriteBigIntSlot(query.bytes_read_total, slot);
        break;
      case TQueryTableColumn::PERNODE_PEAK_MEM_MIN:
        if (auto min_elem = min_element(query.per_host_state.cbegin(),
                query.per_host_state.cend(), PerHostPeakMemoryComparator);
            LIKELY(min_elem != query.per_host_state.cend())) {
          WriteBigIntSlot(min_elem->second.peak_memory_usage, slot);
        }
        break;
      case TQueryTableColumn::PERNODE_PEAK_MEM_MAX:
        if (auto max_elem = max_element(query.per_host_state.cbegin(),
                query.per_host_state.cend(), PerHostPeakMemoryComparator);
            LIKELY(max_elem != query.per_host_state.cend())) {
          WriteBigIntSlot(max_elem->second.peak_memory_usage, slot);
        }
        break;
      case TQueryTableColumn::PERNODE_PEAK_MEM_MEAN:
        if (LIKELY(!query.per_host_state.empty())) {
          int64_t calc_mean = 0;
          for (const auto& host : query.per_host_state) {
            calc_mean += host.second.peak_memory_usage;
          }
          calc_mean /= query.per_host_state.size();
          WriteBigIntSlot(calc_mean, slot);
        }
        break;
      case TQueryTableColumn::SQL:
        RETURN_IF_ERROR(WriteStringSlot(record.stmt, pool, slot));
        break;
      case TQueryTableColumn::PLAN:
        RETURN_IF_ERROR(WriteStringSlot(
            trim_left_copy_if(record.plan, is_any_of("\n")), pool, slot));
        break;
      case TQueryTableColumn::TABLES_QUERIED:
        if (!query.tables.empty()) {
          RETURN_IF_ERROR(WriteStringSlot(PrintTableList(query.tables), pool, slot));
        }
        break;
      default:
        DCHECK(false) << "Unknown column position " << slot_desc->col_pos();
    }
  }

  query_records_.pop_front();
  if (query_records_.empty()) eos_ = true;
  return Status::OK();
}

} /* namespace impala */
