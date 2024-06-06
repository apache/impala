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

#include <cstdint>
#include <map>
#include <unordered_map>
#include <vector>

#include "gen-cpp/ExecStats_types.h"
#include "gen-cpp/Types_types.h"
#include "util/network-util.h"

namespace impala {

class ClientRequestState;

/// Snapshot of a query's state, archived in the query log. Not mutated after
/// construction.  Please update EstimateSize() if field member changed.
struct QueryStateRecord {
  /// Compressed representation of profile returned by RuntimeProfile::Compress().
  /// Must be initialised to a valid value if this is a completed query.
  /// Empty if this was initialised from a running query.
  const std::vector<uint8_t> compressed_profile;

  /// Query id
  TUniqueId id;

  /// Queries are run and authorized on behalf of the effective_user.
  /// If there is no delegated user, this will be the connected user. Otherwise, it
  /// will be set to the delegated user.
  std::string effective_user;

  /// If true, effective_user has access to the runtime profile and execution
  /// summary.
  bool user_has_profile_access;

  /// default db for this query
  std::string default_db;

  /// SQL statement text
  std::string stmt;

  /// Text representation of plan
  std::string plan;

  /// DDL, DML etc.
  TStmtType::type stmt_type;

  /// True if the query required a coordinator fragment
  bool has_coord;

  /// The number of scan ranges that have completed.
  int64_t num_completed_scan_ranges;

  /// The total number of scan ranges.
  int64_t total_scan_ranges;

  /// The number of fragment instances that have completed.
  int64_t num_completed_fragment_instances;

  /// The total number of fragment instances.
  int64_t total_fragment_instances;

  /// The number of rows fetched by the client
  int64_t num_rows_fetched;

  /// Total time spent returning rows to the client and other client-side processing.
  int64_t client_fetch_wait_time_ns;

  /// The state of the query as of this snapshot. The possible values for the
  /// query_state = union(beeswax::QueryState, ClientRequestState::RetryState). This is
  /// necessary so that the query_state can accurately reflect if a query has been
  /// retried or not. This string is not displayed in the runtime profiles, it is only
  /// displayed on the /queries endpoint of the Web UI when listing out the state of
  /// each query. This is necessary so that users can clearly see if a query has been
  /// retried or not.
  std::string query_state;

  /// The beeswax::QueryState of the query as of this snapshot.
  beeswax::QueryState::type beeswax_query_state;

  /// Start and end time of the query, in Unix microseconds.
  /// A query whose end_time_us is 0 indicates that it is an in-flight query.
  /// These two variables are initialized with the corresponding values from
  /// ClientRequestState.
  int64_t start_time_us, end_time_us;

  /// The request waited time in ms for queued.
  int64_t wait_time_ms;

  /// Total peak memory usage by this query at all backends.
  int64_t total_peak_mem_usage;

  /// The cluster wide estimated memory usage of this query.
  int64_t cluster_mem_est;

  /// Total bytes read by this query at all backends.
  int64_t bytes_read;

  /// The total number of bytes sent (across the network) by this query in exchange
  /// nodes. Does not include remote reads, data written to disk, or data sent to the
  /// client.
  int64_t bytes_sent;

  // Query timeline from summary profile.
  std::string timeline;

  /// Summary of execution for this query.
  TExecSummary exec_summary;

  Status query_status;

  /// Timeline of important query events
  TEventSequence event_sequence;

  /// Save the query plan fragments so that the plan tree can be rendered on the debug
  /// webpages.
  vector<TPlanFragment> fragments;

  // If true, this query has no more rows to return
  bool all_rows_returned;

  // The most recent time this query was actively being processed, in Unix milliseconds.
  int64_t last_active_time_ms;

  /// Resource pool to which the request was submitted for admission, or an empty
  /// string if this request doesn't go through admission control.
  std::string resource_pool;

  /// True if this query was retried, false otherwise.
  bool was_retried = false;

  /// If this query was retried, the query id of the retried query.
  std::unique_ptr<const TUniqueId> retried_query_id;

  /// Initialise from 'exec_state' of a completed query. 'compressed_profile' must be
  /// a runtime profile decompressed with RuntimeProfile::Compress().
  QueryStateRecord(
      const ClientRequestState& exec_state, std::vector<uint8_t>&& compressed_profile);

  /// Initialize from 'exec_state' of a running query
  QueryStateRecord(const ClientRequestState& exec_state);

  /// Default constructor used only when participating in collections
  QueryStateRecord() { }

  struct StartTimeComparator {
    /// Comparator that sorts by start time.
    bool operator() (const QueryStateRecord& lhs, const QueryStateRecord& rhs) const;
  };

  private:
  // Common initialization for constructors.
  void Init(const ClientRequestState& exec_state);
}; // struct QueryStateRecord

/// Return the estimated size of given record in bytes.
/// It does not meant to return exact byte size of given QueryStateRecord in memory,
/// but should account for compressed_profile vector of record.
int64_t EstimateSize(const QueryStateRecord* record);

/// Stores relevant information about each backend executor. Used by the
/// QueryStateExpanded struct.
struct PerHostState {
  // Fragment Instances Count
  int32_t fragment_instance_count = 0;

  // Peak Memory Usage
  int64_t peak_memory_usage = 0;
}; // struct PerHostState

/// Comparator function that compares two PerHostState structs based on the
/// peak_memory_usage member of the struct.
bool PerHostPeakMemoryComparator(const std::pair<TNetworkAddress, PerHostState>& a,
    const std::pair<TNetworkAddress, PerHostState>& b);

/// The query events are stored in two separate vectors, one for the labels and the other
/// for the values. This iterator unifies the two into a single iterator that yields a
/// std::pair<string, int64_t> with the first being the event name and the second being
/// the event timestamp. This iterator supports one-time forward pass and range based
/// for loops.
class EventsTimelineIterator {
public:
  using iter_t = const std::pair<const std::string, const std::int64_t>;
  using iterator_category = std::input_iterator_tag;
  using value_type = iter_t;
  using difference_type = std::size_t;
  using pointer = iter_t*;
  using reference = iter_t&;

  /// Constructor that starts the iterator at index 0 of the vectors.
  EventsTimelineIterator(const std::vector<std::string>* labels,
      const std::vector<std::int64_t>* timestamps);

  /// Constructor that starts the iterator at the specified index of the vectors.
  EventsTimelineIterator(const std::vector<std::string>* labels,
      const std::vector<std::int64_t>* timestamps, size_t cur);

  /// Yields up the current position of the iterator.
  iter_t operator*() const;
  EventsTimelineIterator& operator++();

  /// Moves to the iterator to next position.
  EventsTimelineIterator operator++(int);

  /// Compare two iterators.
  bool operator==(const EventsTimelineIterator& other) const;
  bool operator!=(const EventsTimelineIterator& other) const;

  /// Functions to support range based for loops.
  EventsTimelineIterator begin();
  EventsTimelineIterator end();

private:
  const std::vector<std::string>* labels_;
  const std::vector<int64_t>* timestamps_;
  size_t cur_;
}; // class EventsTimelineIterator

// Enum of all query events that are relevant to workload management.
// Note: if adding to this enum, also add to the initialization of the events member of
// the QueryStateExpanded struct.
enum QueryEvent {
  PLANNING_FINISHED,
  SUBMIT_FOR_ADMISSION,
  COMPLETED_ADMISSION,
  ALL_BACKENDS_STARTED,
  ROWS_AVAILABLE,
  FIRST_ROW_FETCHED,
  LAST_ROW_FETCHED,
  UNREGISTER_QUERY
};

/// Expanded snapshot of the query including its state along with other fields relevant
/// to workload management. Not mutated after construction.
struct QueryStateExpanded {

  /// Base Query State
  const std::shared_ptr<QueryStateRecord> base_state;

  /// User set query options.
  TQueryOptions query_options;

  /// Impala assigned session id for the client session.
  TUniqueId session_id;

  /// Type of the session the client opened.
  TSessionType::type session_type;

  /// Version of the Hiveserver2 protocol used by the client (if connected using HS2).
  /// The value of this field is undefined unless session_type is
  /// TSessionType::HIVESERVER2. It is the responsibility of the consumer to first verify
  /// the session_type before referencing this value stored in this struct member.
  apache::hive::service::cli::thrift::TProtocolVersion::type hiveserver2_protocol_version;

  /// Name of the user that connected to Impala.
  std::string db_user_connection;

  /// Impala Query End State
  std::string impala_query_end_state;

  /// Address of the client that ran this query.
  TNetworkAddress client_address;

  /// Per-Host Memory Estimate in Bytes
  /// Calculated before considering the MAX_MEM_ESTIMATE_FOR_ADMISSION query option.
  int64_t per_host_mem_estimate = 0;

  /// Dedicated Coordinator Memory Estimate in Bytes
  int64_t dedicated_coord_mem_estimate = 0;

  /// Per-Host State
  std::map<TNetworkAddress, PerHostState, TNetworkAddressComparator> per_host_state;

  /// Admission Result
  std::string admission_result;

  /// Executor Group that Executed the Query
  std::string executor_group;

  /// Redacted SQL
  std::string redacted_sql;

  /// Exec Summary Pretty Printed
  std::string exec_summary;

  /// Row Materialization Rate (bytes per second)
  int64_t row_materialization_rate = 0;

  /// Row Materialization Time (microseconds)
  int64_t row_materialization_time = 0;

  /// Compressed Bytes Spilled
  int64_t compressed_bytes_spilled = 0;

  /// Read IO Wait Time Total (microseconds)
  int64_t read_io_wait_time_total = 0;

  /// Read IO Wait Time Mean (microseconds)
  int64_t read_io_wait_time_mean = 0;

  /// Total Bytes Read from the Data Cache
  int64_t bytes_read_cache_total = 0;

  /// Total Bytes Read
  int64_t bytes_read_total = 0;

  /// Executor Groups
  std::string executor_groups;

  /// Events
  /// Guaranteed to have one element for every member of the QueryEvent enum.
  std::unordered_map<QueryEvent, std::int64_t> events = {
    {PLANNING_FINISHED, 0},
    {SUBMIT_FOR_ADMISSION, 0},
    {COMPLETED_ADMISSION, 0},
    {ALL_BACKENDS_STARTED, 0},
    {ROWS_AVAILABLE, 0},
    {FIRST_ROW_FETCHED, 0},
    {LAST_ROW_FETCHED, 0},
    {UNREGISTER_QUERY, 0}
  };

  /// Events Timeline Empty
  bool events_timeline_empty() const;

  /// Events Timeline Iterator
  EventsTimelineIterator EventsTimeline() const;

  // Source tables accessed by this query.
  std::vector<TTableName> tables;

  /// Required data will be copied from the provided ClientRequestState into members of
  /// the struct.
  QueryStateExpanded(const ClientRequestState& exec_state,
      const std::shared_ptr<QueryStateRecord> base_state_src = nullptr);
}; // struct QueryStateExpanded

} // namespace impala
