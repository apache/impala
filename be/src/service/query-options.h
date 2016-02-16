// Copyright 2014 Cloudera Inc.
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

#ifndef IMPALA_SERVICE_QUERY_OPTIONS_H
#define IMPALA_SERVICE_QUERY_OPTIONS_H

#include <string>
#include <map>
#include <bitset>

#include "common/status.h"

/// Utility methods to process per-query options

namespace impala {

class TQueryOptions;

// Macro to help generate functions that use or manipulate query options.
// If the DCHECK is hit then handle the missing query option below and update
// the DCHECK.
#define QUERY_OPTS_TABLE\
  DCHECK_EQ(_TImpalaQueryOptions_VALUES_TO_NAMES.size(),\
      TImpalaQueryOptions::DISABLE_ROW_RUNTIME_FILTERING + 1);\
  QUERY_OPT_FN(abort_on_default_limit_exceeded, ABORT_ON_DEFAULT_LIMIT_EXCEEDED)\
  QUERY_OPT_FN(abort_on_error, ABORT_ON_ERROR)\
  QUERY_OPT_FN(allow_unsupported_formats, ALLOW_UNSUPPORTED_FORMATS)\
  QUERY_OPT_FN(batch_size, BATCH_SIZE)\
  QUERY_OPT_FN(debug_action, DEBUG_ACTION)\
  QUERY_OPT_FN(default_order_by_limit, DEFAULT_ORDER_BY_LIMIT)\
  QUERY_OPT_FN(disable_cached_reads, DISABLE_CACHED_READS)\
  QUERY_OPT_FN(disable_outermost_topn, DISABLE_OUTERMOST_TOPN)\
  QUERY_OPT_FN(disable_codegen, DISABLE_CODEGEN)\
  QUERY_OPT_FN(explain_level, EXPLAIN_LEVEL)\
  QUERY_OPT_FN(hbase_cache_blocks, HBASE_CACHE_BLOCKS)\
  QUERY_OPT_FN(hbase_caching, HBASE_CACHING)\
  QUERY_OPT_FN(max_errors, MAX_ERRORS)\
  QUERY_OPT_FN(max_io_buffers, MAX_IO_BUFFERS)\
  QUERY_OPT_FN(max_scan_range_length, MAX_SCAN_RANGE_LENGTH)\
  QUERY_OPT_FN(mem_limit, MEM_LIMIT)\
  QUERY_OPT_FN(num_nodes, NUM_NODES)\
  QUERY_OPT_FN(num_scanner_threads, NUM_SCANNER_THREADS)\
  QUERY_OPT_FN(compression_codec, COMPRESSION_CODEC)\
  QUERY_OPT_FN(parquet_file_size, PARQUET_FILE_SIZE)\
  QUERY_OPT_FN(request_pool, REQUEST_POOL)\
  QUERY_OPT_FN(reservation_request_timeout, RESERVATION_REQUEST_TIMEOUT)\
  QUERY_OPT_FN(sync_ddl, SYNC_DDL)\
  QUERY_OPT_FN(v_cpu_cores, V_CPU_CORES)\
  QUERY_OPT_FN(rm_initial_mem, RM_INITIAL_MEM)\
  QUERY_OPT_FN(query_timeout_s, QUERY_TIMEOUT_S)\
  QUERY_OPT_FN(max_block_mgr_memory, MAX_BLOCK_MGR_MEMORY)\
  QUERY_OPT_FN(appx_count_distinct, APPX_COUNT_DISTINCT)\
  QUERY_OPT_FN(disable_unsafe_spills, DISABLE_UNSAFE_SPILLS)\
  QUERY_OPT_FN(seq_compression_mode, SEQ_COMPRESSION_MODE)\
  QUERY_OPT_FN(exec_single_node_rows_threshold, EXEC_SINGLE_NODE_ROWS_THRESHOLD)\
  QUERY_OPT_FN(optimize_partition_key_scans, OPTIMIZE_PARTITION_KEY_SCANS)\
  QUERY_OPT_FN(replica_preference, REPLICA_PREFERENCE)\
  QUERY_OPT_FN(random_replica, RANDOM_REPLICA)\
  QUERY_OPT_FN(scan_node_codegen_threshold, SCAN_NODE_CODEGEN_THRESHOLD)\
  QUERY_OPT_FN(disable_streaming_preaggregations, DISABLE_STREAMING_PREAGGREGATIONS)\
  QUERY_OPT_FN(runtime_filter_mode, RUNTIME_FILTER_MODE)\
  QUERY_OPT_FN(runtime_bloom_filter_size, RUNTIME_BLOOM_FILTER_SIZE);\
  QUERY_OPT_FN(runtime_filter_wait_time_ms, RUNTIME_FILTER_WAIT_TIME_MS)\
  QUERY_OPT_FN(disable_row_runtime_filtering, DISABLE_ROW_RUNTIME_FILTERING);

/// Converts a TQueryOptions struct into a map of key, value pairs.
void TQueryOptionsToMap(const TQueryOptions& query_options,
    std::map<std::string, std::string>* configuration);

/// Returns a comma-delimted string of the contents of query_options. The output does not
/// contain key-value pairs where the value matches the default value specified in the
/// TQueryOptions definition (regardless of whether or not it was explicitly or
/// implicitly set to the default value).
std::string DebugQueryOptions(const TQueryOptions& query_options);

/// Bitmask for the values of TQueryOptions.
/// TODO: Find a way to set the size based on the number of fields.
typedef std::bitset<64> QueryOptionsMask;

/// Updates the query options in dst from those in src where the query option is set
/// (i.e. src->__isset.PROPERTY is true) and the corresponding bit in mask is set. If
/// mask has no set bits, no options are set. If all bits are set, then all options
/// that were set on src are copied to dst.
void OverlayQueryOptions(const TQueryOptions& src, const QueryOptionsMask& mask,
    TQueryOptions* dst);

/// Set the key/value pair in TQueryOptions. It will override existing setting in
/// query_options. The bit corresponding to query option 'key' in set_query_options_mask
/// is set.
Status SetQueryOption(const std::string& key, const std::string& value,
    TQueryOptions* query_options, QueryOptionsMask* set_query_options_mask);

/// Parse a "," separated key=value pair of query options and set it in 'query_options'.
/// If the same query option is specified more than once, the last one wins. The
/// set_query_options_mask bitmask is updated to reflect the query options which were
/// set. Return an error if the input is invalid (bad format or invalid query option).
Status ParseQueryOptions(const std::string& options, TQueryOptions* query_options,
    QueryOptionsMask* set_query_options_mask);

}

#endif
