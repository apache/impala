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

#include "service/query-options.h"

#include <limits>
#include <sstream>

#include <boost/algorithm/string.hpp>
#include <gutil/strings/strip.h>
#include <gutil/strings/substitute.h>

#include "exprs/timezone_db.h"
#include "gen-cpp/ImpalaInternalService_types.h"
#include "runtime/runtime-filter.h"
#include "service/query-option-parser.h"
#include "util/debug-util.h"
#include "util/parse-util.h"

DECLARE_int64(min_buffer_size);

using beeswax::TQueryOptionLevel;
using boost::algorithm::iequals;
using boost::algorithm::is_any_of;
using boost::algorithm::split;
using boost::algorithm::token_compress_on;
using boost::algorithm::trim;
using std::to_string;
using namespace impala;
using namespace strings;

DECLARE_int32(idle_session_timeout);

void impala::OverlayQueryOptions(
    const TQueryOptions& src, const QueryOptionsMask& mask, TQueryOptions* dst) {
  DCHECK_GT(mask.size(), _TImpalaQueryOptions_VALUES_TO_NAMES.size())
      << "Size of QueryOptionsMask must be increased.";
#define QUERY_OPT_FN(NAME, ENUM, LEVEL) \
  if (src.__isset.NAME && mask[TImpalaQueryOptions::ENUM]) dst->__set_##NAME(src.NAME);
#define REMOVED_QUERY_OPT_FN(NAME, ENUM)
  QUERY_OPTS_TABLE
#undef QUERY_OPT_FN
#undef REMOVED_QUERY_OPT_FN
}

// Choose different print function based on the type.
template <typename T, typename std::enable_if_t<std::is_enum<T>::value>* = nullptr>
string PrintQueryOptionValue(const T& option) {
  return PrintValue(option);
}

template <typename T, typename std::enable_if_t<std::is_arithmetic<T>::value>* = nullptr>
string PrintQueryOptionValue(const T& option) {
  return std::to_string(option);
}

const string& PrintQueryOptionValue(const std::string& option) {
  return option;
}

const string PrintQueryOptionValue(const impala::TCompressionCodec& compression_codec) {
  if (compression_codec.codec != THdfsCompression::ZSTD) {
    return Substitute("$0", PrintValue(compression_codec.codec));
  } else {
    return Substitute("$0:$1", PrintValue(compression_codec.codec),
        compression_codec.compression_level);
  }
}

std::ostream& impala::operator<<(
    std::ostream& out, const std::set<impala::TRuntimeFilterType::type>& filter_types) {
  bool first = true;
  for (const auto& t : filter_types) {
    if (!first) out << ",";
    out << t;
    first = false;
  }
  return out;
}

const string PrintQueryOptionValue(
    const std::set<impala::TRuntimeFilterType::type>& filter_types) {
  std::stringstream val;
  val << filter_types;
  return val.str();
}

void impala::TQueryOptionsToMap(
    const TQueryOptions& query_options, std::map<string, string>* configuration) {
#define QUERY_OPT_FN(NAME, ENUM, LEVEL)                                    \
  {                                                                        \
    if (query_options.__isset.NAME) {                                      \
      (*configuration)[#ENUM] = PrintQueryOptionValue(query_options.NAME); \
    } else {                                                               \
      (*configuration)[#ENUM] = "";                                        \
    }                                                                      \
  }
#define REMOVED_QUERY_OPT_FN(NAME, ENUM) (*configuration)[#ENUM] = "";
  QUERY_OPTS_TABLE
#undef QUERY_OPT_FN
#undef REMOVED_QUERY_OPT_FN
}

// Resets query_options->option to its default value.
static void ResetQueryOption(const int option, TQueryOptions* query_options) {
  const static TQueryOptions defaults;
  switch (option) {
#define QUERY_OPT_FN(NAME, ENUM, LEVEL)                  \
  case TImpalaQueryOptions::ENUM:                        \
    query_options->__isset.NAME = defaults.__isset.NAME; \
    query_options->NAME = defaults.NAME;                 \
    break;
#define REMOVED_QUERY_OPT_FN(NAME, ENUM)
    QUERY_OPTS_TABLE
#undef QUERY_OPT_FN
#undef REMOVED_QUERY_OPT_FN
  }
}

static TQueryOptions DefaultQueryOptions() {
  TQueryOptions defaults;
  // default value of idle_session_timeout is set by a command line flag.
  defaults.__set_idle_session_timeout(FLAGS_idle_session_timeout);
  return defaults;
}

inline bool operator!=(const TCompressionCodec& a, const TCompressionCodec& b) {
  return (a.codec != b.codec || a.compression_level != b.compression_level);
}

string impala::DebugQueryOptions(const TQueryOptions& query_options) {
  const static TQueryOptions defaults = DefaultQueryOptions();
  int i = 0;
  std::stringstream ss;
#define QUERY_OPT_FN(NAME, ENUM, LEVEL)                                     \
  if (query_options.__isset.NAME                                            \
      && (!defaults.__isset.NAME || query_options.NAME != defaults.NAME)) { \
    if (i++ > 0) ss << ",";                                                 \
    ss << #ENUM << "=" << query_options.NAME;                               \
  }
#define REMOVED_QUERY_OPT_FN(NAME, ENUM)
  QUERY_OPTS_TABLE
#undef QUERY_OPT_FN
#undef REMOVED_QUERY_OPT_FN
  return ss.str();
}

// Returns the TImpalaQueryOptions enum for the given "key". Input is case insensitive.
// Return -1 if the input is an invalid option.
static int GetQueryOptionForKey(const string& key) {
  std::map<int, const char*>::const_iterator itr =
      _TImpalaQueryOptions_VALUES_TO_NAMES.begin();
  for (; itr != _TImpalaQueryOptions_VALUES_TO_NAMES.end(); ++itr) {
    if (iequals(key, (*itr).second)) {
      return itr->first;
    }
  }
  return -1;
}

// Return true if we can ignore a reference to this removed query option.
static bool IsRemovedQueryOption(const string& key) {
#define QUERY_OPT_FN(NAME, ENUM, LEVEL)
#define REMOVED_QUERY_OPT_FN(NAME, ENUM) \
  if (iequals(key, #NAME)) {             \
    return true;                         \
  }
  QUERY_OPTS_TABLE
#undef QUERY_OPT_FN
#undef REMOVED_QUERY_OPT_FN
  return false;
}

// Return true if the given value is true (case-insensitive) or 1.
static bool IsTrue(const string& value) {
  return iequals(value, "true") || iequals(value, "1");
}

// Note that we allow numerical values for boolean and enum options. This is because
// TQueryOptionsToMap() will output the numerical values, and we need to parse its output
// configuration.
Status impala::SetQueryOption(const string& key, const string& value,
    TQueryOptions* query_options, QueryOptionsMask* set_query_options_mask) {
  int option_int = GetQueryOptionForKey(key);
  if (option_int < 0) {
    return Status(Substitute("Invalid query option: $0", key));
  }

  TImpalaQueryOptions::type option = static_cast<TImpalaQueryOptions::type>(option_int);

  if (value.empty()) {
    ResetQueryOption(option, query_options);
    if (set_query_options_mask != nullptr) {
      DCHECK_LT(option, set_query_options_mask->size());
      set_query_options_mask->reset(option);
    }
  } else {
    switch (option) {
      case TImpalaQueryOptions::ABORT_ON_ERROR: {
        query_options->__set_abort_on_error(IsTrue(value));
        break;
      }
      case TImpalaQueryOptions::MAX_ERRORS: {
        int32_t int32_t_val = 0;
        RETURN_IF_ERROR(QueryOptionParser::Parse<int32_t>(option, value, &int32_t_val));
        query_options->__set_max_errors(int32_t_val);
        break;
      }
      case TImpalaQueryOptions::DISABLE_CODEGEN: {
        query_options->__set_disable_codegen(IsTrue(value));
        break;
      };
      case TImpalaQueryOptions::BATCH_SIZE: {
        int32_t int32_t_val = 0;
        RETURN_IF_ERROR(QueryOptionParser::ParseAndCheckInclusiveRange<int32_t>(
            option, value, 0, 65536, &int32_t_val));
        query_options->__set_batch_size(int32_t_val);
        break;
      };
      case TImpalaQueryOptions::MEM_LIMIT: {
        MemSpec mem_spec_val{};
        RETURN_IF_ERROR(QueryOptionParser::Parse<MemSpec>(option, value, &mem_spec_val));
        query_options->__set_mem_limit(mem_spec_val.value);
        break;
      };
      case TImpalaQueryOptions::NUM_NODES: {
        int32_t int32_t_val = 0;
        RETURN_IF_ERROR(QueryOptionParser::Parse<int32_t>(option, value, &int32_t_val));
        query_options->__set_num_nodes(int32_t_val);
        break;
      };
      case TImpalaQueryOptions::MAX_SCAN_RANGE_LENGTH: {
        MemSpec mem_spec_val{};
        RETURN_IF_ERROR(QueryOptionParser::Parse<MemSpec>(option, value, &mem_spec_val));
        query_options->__set_max_scan_range_length(mem_spec_val.value);
        break;
      }
      case TImpalaQueryOptions::NUM_SCANNER_THREADS: {
        int32_t int32_t_val = 0;
        RETURN_IF_ERROR(QueryOptionParser::Parse<int32_t>(option, value, &int32_t_val));
        query_options->__set_num_scanner_threads(int32_t_val);
        break;
      };
      case TImpalaQueryOptions::DEBUG_ACTION: {
        query_options->__set_debug_action(value);
        break;
      };
      case TImpalaQueryOptions::COMPRESSION_CODEC: {
        THdfsCompression::type enum_type;
        int compression_level;
        RETURN_IF_ERROR(
            ParseUtil::ParseCompressionCodec(value, &enum_type, &compression_level));
        TCompressionCodec compression_codec;
        compression_codec.__set_codec(enum_type);
        if (enum_type == THdfsCompression::ZSTD) {
          compression_codec.__set_compression_level(compression_level);
        }
        query_options->__set_compression_codec(compression_codec);
        break;
      }
      case TImpalaQueryOptions::HBASE_CACHING: {
        int32_t int32_t_val = 0;
        RETURN_IF_ERROR(QueryOptionParser::Parse<int32_t>(option, value, &int32_t_val));
        query_options->__set_hbase_caching(int32_t_val);
        break;
      };
      case TImpalaQueryOptions::HBASE_CACHE_BLOCKS: {
        query_options->__set_hbase_cache_blocks(IsTrue(value));
        break;
      };
      case TImpalaQueryOptions::PARQUET_FILE_SIZE: {
        MemSpec mem_spec_val{};
        RETURN_IF_ERROR(QueryOptionParser::ParseAndCheckInclusiveRange<MemSpec>(
            option, value, {0}, {numeric_limits<int32_t>::max()}, &mem_spec_val));
        query_options->__set_parquet_file_size(mem_spec_val.value);
        break;
      }
      case TImpalaQueryOptions::EXPLAIN_LEVEL: {
        TExplainLevel::type enum_type;
        RETURN_IF_ERROR(GetThriftEnum(
            value, "explain level", _TExplainLevel_VALUES_TO_NAMES, &enum_type));
        query_options->__set_explain_level(enum_type);
        break;
      }
      case TImpalaQueryOptions::SYNC_DDL: {
        query_options->__set_sync_ddl(IsTrue(value));
        break;
      };
      case TImpalaQueryOptions::REQUEST_POOL: {
        query_options->__set_request_pool(value);
        break;
      };
      case TImpalaQueryOptions::DISABLE_OUTERMOST_TOPN: {
        query_options->__set_disable_outermost_topn(IsTrue(value));
        break;
      };
      case TImpalaQueryOptions::QUERY_TIMEOUT_S: {
        int32_t int32_t_val = 0;
        RETURN_IF_ERROR(QueryOptionParser::ParseAndCheckNonNegative<int32_t>(
            option, value, &int32_t_val));
        query_options->__set_query_timeout_s(int32_t_val);
        break;
      }
      case TImpalaQueryOptions::BUFFER_POOL_LIMIT: {
        MemSpec mem_spec_val{};
        RETURN_IF_ERROR(QueryOptionParser::Parse<MemSpec>(option, value, &mem_spec_val));
        query_options->__set_buffer_pool_limit(mem_spec_val.value);
        break;
      }
      case TImpalaQueryOptions::APPX_COUNT_DISTINCT: {
        query_options->__set_appx_count_distinct(IsTrue(value));
        break;
      }
      case TImpalaQueryOptions::DISABLE_UNSAFE_SPILLS: {
        query_options->__set_disable_unsafe_spills(IsTrue(value));
        break;
      }
      case TImpalaQueryOptions::EXEC_SINGLE_NODE_ROWS_THRESHOLD: {
        int32_t int32_t_val = 0;
        RETURN_IF_ERROR(QueryOptionParser::Parse<int32_t>(option, value, &int32_t_val));
        query_options->__set_exec_single_node_rows_threshold(int32_t_val);
        break;
      };
      case TImpalaQueryOptions::OPTIMIZE_PARTITION_KEY_SCANS: {
        query_options->__set_optimize_partition_key_scans(IsTrue(value));
        break;
      };
      case TImpalaQueryOptions::OPTIMIZE_SIMPLE_LIMIT: {
        query_options->__set_optimize_simple_limit(IsTrue(value));
        break;
      };
      case TImpalaQueryOptions::REPLICA_PREFERENCE: {
        std::map<int, const char*> valid_enums_values = {
            {0, "CACHE_LOCAL"}, {2, "DISK_LOCAL"}, {4, "REMOTE"}};
        TReplicaPreference::type enum_type;
        RETURN_IF_ERROR(GetThriftEnum(
            value, "replica memory distance preference", valid_enums_values, &enum_type));
        query_options->__set_replica_preference(enum_type);
        break;
      }
      case TImpalaQueryOptions::SCHEDULE_RANDOM_REPLICA: {
        query_options->__set_schedule_random_replica(IsTrue(value));
        break;
      };
      case TImpalaQueryOptions::DISABLE_STREAMING_PREAGGREGATIONS: {
        query_options->__set_disable_streaming_preaggregations(IsTrue(value));
        break;
      };
      case TImpalaQueryOptions::RUNTIME_FILTER_MODE: {
        TRuntimeFilterMode::type enum_type;
        RETURN_IF_ERROR(GetThriftEnum(value, "runtime filter mode",
            _TRuntimeFilterMode_VALUES_TO_NAMES, &enum_type));
        query_options->__set_runtime_filter_mode(enum_type);
        break;
      }
      case TImpalaQueryOptions::RUNTIME_FILTER_MAX_SIZE: {
        MemSpec mem_spec_val{};
        RETURN_IF_ERROR(QueryOptionParser::Parse<MemSpec>(option, value, &mem_spec_val));
        RETURN_IF_ERROR(QueryOptionValidator<MemSpec>::InclusiveRange(option,
            mem_spec_val, {RuntimeFilterBank::MIN_BLOOM_FILTER_SIZE},
            {RuntimeFilterBank::MAX_BLOOM_FILTER_SIZE}));
        if (mem_spec_val.value < FLAGS_min_buffer_size
            // last condition is to unblock the highly improbable case where the
            // min_buffer_size is greater than RuntimeFilterBank::MAX_BLOOM_FILTER_SIZE.
            && FLAGS_min_buffer_size <= RuntimeFilterBank::MAX_BLOOM_FILTER_SIZE) {
          return Status(Substitute("$0 should not be less than $1 which is the minimum "
                                   "buffer size that can be allocated by the buffer pool",
              PrintValue(option), FLAGS_min_buffer_size));
        }
        query_options->__set_runtime_filter_max_size(mem_spec_val.value);
        break;
      }
      case TImpalaQueryOptions::RUNTIME_FILTER_MIN_SIZE: {
        MemSpec mem_spec_val{};
        RETURN_IF_ERROR(QueryOptionParser::ParseAndCheckInclusiveRange<MemSpec>(option,
            value, {RuntimeFilterBank::MIN_BLOOM_FILTER_SIZE},
            {RuntimeFilterBank::MAX_BLOOM_FILTER_SIZE}, &mem_spec_val));
        query_options->__set_runtime_filter_min_size(mem_spec_val.value);
        break;
      }
      case TImpalaQueryOptions::RUNTIME_BLOOM_FILTER_SIZE: {
        MemSpec mem_spec_val{};
        RETURN_IF_ERROR(QueryOptionParser::ParseAndCheckInclusiveRange<MemSpec>(option,
            value, {RuntimeFilterBank::MIN_BLOOM_FILTER_SIZE},
            {RuntimeFilterBank::MAX_BLOOM_FILTER_SIZE}, &mem_spec_val));
        query_options->__set_runtime_bloom_filter_size(mem_spec_val.value);
        break;
      }
      case TImpalaQueryOptions::RUNTIME_FILTER_WAIT_TIME_MS: {
        int32_t int32_t_val = 0;
        RETURN_IF_ERROR(QueryOptionParser::ParseAndCheckInclusiveLowerBound<int32_t>(
            option, value, 0, &int32_t_val));
        query_options->__set_runtime_filter_wait_time_ms(int32_t_val);
        break;
      }
      case TImpalaQueryOptions::DISABLE_ROW_RUNTIME_FILTERING: {
        query_options->__set_disable_row_runtime_filtering(IsTrue(value));
        break;
      };
      case TImpalaQueryOptions::MINMAX_FILTERING_LEVEL: {
        // Parse the enabled runtime filter types and validate it.
        TMinmaxFilteringLevel::type enum_type;
        RETURN_IF_ERROR(GetThriftEnum(value, "minmax filter level",
            _TMinmaxFilteringLevel_VALUES_TO_NAMES, &enum_type));
        query_options->__set_minmax_filtering_level(enum_type);
        break;
      };
      case TImpalaQueryOptions::MINMAX_FILTER_THRESHOLD: {
        double double_val = 0.0f;
        RETURN_IF_ERROR(QueryOptionParser::ParseAndCheckInclusiveRange<double>(
            option, value, 0.0, 1.0, &double_val));
        query_options->__set_minmax_filter_threshold(double_val);
        break;
      }
      case TImpalaQueryOptions::MAX_NUM_RUNTIME_FILTERS: {
        int32_t int32_t_val = 0;
        RETURN_IF_ERROR(QueryOptionParser::ParseAndCheckNonNegative<int32_t>(
            option, value, &int32_t_val));
        query_options->__set_max_num_runtime_filters(int32_t_val);
        break;
      }
      case TImpalaQueryOptions::PARQUET_ANNOTATE_STRINGS_UTF8: {
        query_options->__set_parquet_annotate_strings_utf8(IsTrue(value));
        break;
      }
      case TImpalaQueryOptions::PARQUET_FALLBACK_SCHEMA_RESOLUTION: {
        TSchemaResolutionStrategy::type enum_type;
        RETURN_IF_ERROR(GetThriftEnum(value, "parquet fallback schema resolution",
            _TSchemaResolutionStrategy_VALUES_TO_NAMES, &enum_type));
        query_options->__set_parquet_fallback_schema_resolution(enum_type);
        break;
      }
      case TImpalaQueryOptions::PARQUET_ARRAY_RESOLUTION: {
        TParquetArrayResolution::type enum_type;
        RETURN_IF_ERROR(GetThriftEnum(value, "parquet array resolution",
            _TParquetArrayResolution_VALUES_TO_NAMES, &enum_type));
        query_options->__set_parquet_array_resolution(enum_type);
        break;
      }
      case TImpalaQueryOptions::MT_DOP: {
        int32_t int32_t_val = 0;
        RETURN_IF_ERROR(QueryOptionParser::ParseAndCheckInclusiveRange<int32_t>(
            option, value, 0, 64, &int32_t_val));
        query_options->__set_mt_dop(int32_t_val);
        break;
      }
      case TImpalaQueryOptions::S3_SKIP_INSERT_STAGING: {
        query_options->__set_s3_skip_insert_staging(IsTrue(value));
        break;
      }
      case TImpalaQueryOptions::PREFETCH_MODE: {
        TPrefetchMode::type enum_type;
        RETURN_IF_ERROR(GetThriftEnum(
            value, "prefetch mode", _TPrefetchMode_VALUES_TO_NAMES, &enum_type));
        query_options->__set_prefetch_mode(enum_type);
        break;
      }
      case TImpalaQueryOptions::STRICT_MODE: {
        query_options->__set_strict_mode(IsTrue(value));
        break;
      }
      case TImpalaQueryOptions::SCRATCH_LIMIT: {
        // Parse the scratch limit spec and validate it.
        if (iequals(value, "-1")) {
          query_options->__set_scratch_limit(-1);
        } else {
          MemSpec mem_spec_val{};
          RETURN_IF_ERROR(
              QueryOptionParser::Parse<MemSpec>(option, value, &mem_spec_val));
          query_options->__set_scratch_limit(mem_spec_val.value);
        }
        break;
      }
      case TImpalaQueryOptions::ENABLE_EXPR_REWRITES: {
        query_options->__set_enable_expr_rewrites(IsTrue(value));
        break;
      }
      case TImpalaQueryOptions::ENABLE_CNF_REWRITES: {
        query_options->__set_enable_cnf_rewrites(IsTrue(value));
        break;
      }
      case TImpalaQueryOptions::DECIMAL_V2: {
        query_options->__set_decimal_v2(IsTrue(value));
        break;
      }
      case TImpalaQueryOptions::PARQUET_DICTIONARY_FILTERING: {
        query_options->__set_parquet_dictionary_filtering(IsTrue(value));
        break;
      }
      case TImpalaQueryOptions::PARQUET_BLOOM_FILTERING: {
        query_options->__set_parquet_bloom_filtering(IsTrue(value));
        break;
      }
      case TImpalaQueryOptions::PARQUET_BLOOM_FILTER_WRITE: {
        TParquetBloomFilterWrite::type enum_type;
        RETURN_IF_ERROR(GetThriftEnum(value, "Parquet Bloom filter write",
            _TParquetBloomFilterWrite_VALUES_TO_NAMES, &enum_type));
        query_options->__set_parquet_bloom_filter_write(enum_type);
        break;
      }
      case TImpalaQueryOptions::PARQUET_READ_STATISTICS: {
        query_options->__set_parquet_read_statistics(IsTrue(value));
        break;
      }
      case TImpalaQueryOptions::ORC_READ_STATISTICS: {
        query_options->__set_orc_read_statistics(IsTrue(value));
        break;
      }
      case TImpalaQueryOptions::DEFAULT_JOIN_DISTRIBUTION_MODE: {
        TJoinDistributionMode::type enum_type;
        RETURN_IF_ERROR(GetThriftEnum(value, "default join distribution mode",
            _TJoinDistributionMode_VALUES_TO_NAMES, &enum_type));
        query_options->__set_default_join_distribution_mode(enum_type);
        break;
      }
      case TImpalaQueryOptions::DISABLE_CODEGEN_ROWS_THRESHOLD: {
        int32_t int32_t_val = 0;
        RETURN_IF_ERROR(QueryOptionParser::ParseAndCheckNonNegative<int32_t>(
            option, value, &int32_t_val));
        query_options->__set_disable_codegen_rows_threshold(int32_t_val);
        break;
      }
      case TImpalaQueryOptions::DEFAULT_SPILLABLE_BUFFER_SIZE: {
        MemSpec mem_spec_val{};
        RETURN_IF_ERROR(QueryOptionParser::Parse<MemSpec>(option, value, &mem_spec_val));
        RETURN_IF_ERROR(QueryOptionValidator<MemSpec>::InclusiveRange(
            option, mem_spec_val, {0}, {SPILLABLE_BUFFER_LIMIT}));
        RETURN_IF_ERROR(QueryOptionValidator<MemSpec>::PowerOf2(option, mem_spec_val));
        query_options->__set_default_spillable_buffer_size(mem_spec_val.value);
        break;
      }
      case TImpalaQueryOptions::MIN_SPILLABLE_BUFFER_SIZE: {
        MemSpec mem_spec_val{};
        RETURN_IF_ERROR(QueryOptionParser::Parse<MemSpec>(option, value, &mem_spec_val));
        RETURN_IF_ERROR(QueryOptionValidator<MemSpec>::InclusiveRange(
            option, mem_spec_val, {0}, {SPILLABLE_BUFFER_LIMIT}));
        RETURN_IF_ERROR(QueryOptionValidator<MemSpec>::PowerOf2(option, mem_spec_val));
        query_options->__set_min_spillable_buffer_size(mem_spec_val.value);
        break;
      }
      case TImpalaQueryOptions::MAX_ROW_SIZE: {
        MemSpec mem_spec_val{};
        RETURN_IF_ERROR(QueryOptionParser::ParseAndCheckInclusiveRange<MemSpec>(
            option, value, {1}, {ROW_SIZE_LIMIT}, &mem_spec_val));
        query_options->__set_max_row_size(mem_spec_val.value);
        break;
      }
      case TImpalaQueryOptions::IDLE_SESSION_TIMEOUT: {
        int32_t int32_t_val = 0;
        RETURN_IF_ERROR(QueryOptionParser::ParseAndCheckNonNegative<int32_t>(
            option, value, &int32_t_val));
        query_options->__set_idle_session_timeout(int32_t_val);
        break;
      }
      case TImpalaQueryOptions::COMPUTE_STATS_MIN_SAMPLE_SIZE: {
        MemSpec mem_spec_val{};
        RETURN_IF_ERROR(QueryOptionParser::ParseAndCheckNonNegative<MemSpec>(
            option, value, &mem_spec_val));
        query_options->__set_compute_stats_min_sample_size(mem_spec_val.value);
        break;
      }
      case TImpalaQueryOptions::COMPUTE_COLUMN_MINMAX_STATS: {
        query_options->__set_compute_column_minmax_stats(IsTrue(value));
        break;
      }
      case TImpalaQueryOptions::SHOW_COLUMN_MINMAX_STATS: {
        query_options->__set_show_column_minmax_stats(IsTrue(value));
        break;
      }
      case TImpalaQueryOptions::EXEC_TIME_LIMIT_S: {
        int32_t int32_t_val = 0;
        RETURN_IF_ERROR(QueryOptionParser::ParseAndCheckNonNegative<int32_t>(
            option, value, &int32_t_val));
        query_options->__set_exec_time_limit_s(int32_t_val);
        break;
      }
      case TImpalaQueryOptions::SHUFFLE_DISTINCT_EXPRS: {
        query_options->__set_shuffle_distinct_exprs(IsTrue(value));
        break;
      }
      case TImpalaQueryOptions::MAX_MEM_ESTIMATE_FOR_ADMISSION: {
        MemSpec mem_spec_val{};
        RETURN_IF_ERROR(QueryOptionParser::Parse<MemSpec>(option, value, &mem_spec_val));
        query_options->__set_max_mem_estimate_for_admission(mem_spec_val.value);
        break;
      }
      case TImpalaQueryOptions::THREAD_RESERVATION_LIMIT: {
        int32_t int32_t_val = 0;
        RETURN_IF_ERROR(QueryOptionParser::ParseAndCheckInclusiveLowerBound<int32_t>(
            option, value, -1, &int32_t_val));
        query_options->__set_thread_reservation_limit(int32_t_val);
        break;
      }
      case TImpalaQueryOptions::THREAD_RESERVATION_AGGREGATE_LIMIT: {
        int32_t int32_t_val = 0;
        RETURN_IF_ERROR(QueryOptionParser::ParseAndCheckInclusiveLowerBound<int32_t>(
            option, value, -1, &int32_t_val));
        query_options->__set_thread_reservation_aggregate_limit(int32_t_val);
        break;
      }
      case TImpalaQueryOptions::KUDU_READ_MODE: {
        TKuduReadMode::type enum_type;
        RETURN_IF_ERROR(GetThriftEnum(
            value, "Kudu read mode", _TKuduReadMode_VALUES_TO_NAMES, &enum_type));
        query_options->__set_kudu_read_mode(enum_type);
        break;
      }
      case TImpalaQueryOptions::ALLOW_ERASURE_CODED_FILES: {
        query_options->__set_allow_erasure_coded_files(IsTrue(value));
        break;
      }
      case TImpalaQueryOptions::TIMEZONE: {
        // Leading/trailing " and ' characters are stripped because the / character
        // cannot be entered unquoted in some contexts.
        string timezone = value;
        TrimString(&timezone, "'\"");
        timezone = timezone.empty() ? TimezoneDatabase::LocalZoneName() : timezone;
        if (TimezoneDatabase::FindTimezone(timezone) == nullptr) {
          return Status(Substitute("Invalid timezone name '$0'.", timezone));
        }
        query_options->__set_timezone(timezone);
        break;
      }
      case TImpalaQueryOptions::SCAN_BYTES_LIMIT: {
        MemSpec mem_spec_val{};
        RETURN_IF_ERROR(QueryOptionParser::Parse<MemSpec>(option, value, &mem_spec_val));
        query_options->__set_scan_bytes_limit(mem_spec_val.value);
        break;
      }
      case TImpalaQueryOptions::CPU_LIMIT_S: {
        int64_t int64_t_val = 0;
        RETURN_IF_ERROR(QueryOptionParser::ParseAndCheckNonNegative<int64_t>(
            option, value, &int64_t_val));
        query_options->__set_cpu_limit_s(int64_t_val);
        break;
      }
      case TImpalaQueryOptions::TOPN_BYTES_LIMIT: {
        MemSpec mem_spec_val{};
        RETURN_IF_ERROR(QueryOptionParser::Parse<MemSpec>(option, value, &mem_spec_val));
        query_options->__set_topn_bytes_limit(mem_spec_val.value);
        break;
      }
      case TImpalaQueryOptions::CLIENT_IDENTIFIER: {
        query_options->__set_client_identifier(value);
        break;
      }
      case TImpalaQueryOptions::RESOURCE_TRACE_RATIO: {
        double double_val = 0.0f;
        RETURN_IF_ERROR(QueryOptionParser::ParseAndCheckInclusiveRange<double>(
            option, value, 0.0, 1.0, &double_val));
        query_options->__set_resource_trace_ratio(double_val);
        break;
      }
      case TImpalaQueryOptions::PLANNER_TESTCASE_MODE: {
        query_options->__set_planner_testcase_mode(IsTrue(value));
        break;
      }
      case TImpalaQueryOptions::NUM_REMOTE_EXECUTOR_CANDIDATES: {
        int32_t int32_t_val = 0;
        RETURN_IF_ERROR(QueryOptionParser::ParseAndCheckInclusiveRange<int32_t>(
            option, value, 0, 16, &int32_t_val));
        query_options->__set_num_remote_executor_candidates(int32_t_val);
        break;
      }
      case TImpalaQueryOptions::NUM_ROWS_PRODUCED_LIMIT: {
        int64_t int64_t_val = 0;
        RETURN_IF_ERROR(QueryOptionParser::ParseAndCheckNonNegative<int64_t>(
            option, value, &int64_t_val));
        query_options->__set_num_rows_produced_limit(int64_t_val);
        break;
      }
      case TImpalaQueryOptions::DEFAULT_FILE_FORMAT: {
        THdfsFileFormat::type enum_type;
        RETURN_IF_ERROR(GetThriftEnum(
            value, "default file format", _THdfsFileFormat_VALUES_TO_NAMES, &enum_type));
        query_options->__set_default_file_format(enum_type);
        break;
      }
      case TImpalaQueryOptions::PARQUET_TIMESTAMP_TYPE: {
        TParquetTimestampType::type enum_type;
        RETURN_IF_ERROR(GetThriftEnum(value, "Parquet timestamp type",
            _TParquetTimestampType_VALUES_TO_NAMES, &enum_type));
        query_options->__set_parquet_timestamp_type(enum_type);
        break;
      }
      case TImpalaQueryOptions::PARQUET_READ_PAGE_INDEX: {
        query_options->__set_parquet_read_page_index(IsTrue(value));
        break;
      }
      case TImpalaQueryOptions::PARQUET_WRITE_PAGE_INDEX: {
        query_options->__set_parquet_write_page_index(IsTrue(value));
        break;
      }
      case TImpalaQueryOptions::PARQUET_PAGE_ROW_COUNT_LIMIT: {
        int32_t int32_t_val = 0;
        RETURN_IF_ERROR(QueryOptionParser::ParseAndCheckInclusiveLowerBound<int32_t>(
            option, value, 1, &int32_t_val));
        query_options->__set_parquet_page_row_count_limit(int32_t_val);
        break;
      }
      case TImpalaQueryOptions::DISABLE_HDFS_NUM_ROWS_ESTIMATE: {
        query_options->__set_disable_hdfs_num_rows_estimate(IsTrue(value));
        break;
      }
      case TImpalaQueryOptions::DEFAULT_HINTS_INSERT_STATEMENT: {
        query_options->__set_default_hints_insert_statement(value);
        break;
      }
      case TImpalaQueryOptions::SPOOL_QUERY_RESULTS: {
        query_options->__set_spool_query_results(IsTrue(value));
        break;
      }
      case TImpalaQueryOptions::MAX_RESULT_SPOOLING_MEM: {
        MemSpec mem_spec_val{};
        RETURN_IF_ERROR(QueryOptionParser::Parse<MemSpec>(option, value, &mem_spec_val));
        query_options->__set_max_result_spooling_mem(mem_spec_val.value);
        break;
      }
      case TImpalaQueryOptions::MAX_SPILLED_RESULT_SPOOLING_MEM: {
        MemSpec mem_spec_val{};
        RETURN_IF_ERROR(QueryOptionParser::Parse<MemSpec>(option, value, &mem_spec_val));
        query_options->__set_max_spilled_result_spooling_mem(mem_spec_val.value);
        break;
      }
      case TImpalaQueryOptions::DEFAULT_TRANSACTIONAL_TYPE: {
        TTransactionalType::type enum_type;
        RETURN_IF_ERROR(GetThriftEnum(value, "default transactional type",
            _TTransactionalType_VALUES_TO_NAMES, &enum_type));
        query_options->__set_default_transactional_type(enum_type);
        break;
      }
      case TImpalaQueryOptions::STATEMENT_EXPRESSION_LIMIT: {
        int32_t int32_t_val = 0;
        RETURN_IF_ERROR(QueryOptionParser::ParseAndCheckInclusiveLowerBound<int32_t>(
            option, value, MIN_STATEMENT_EXPRESSION_LIMIT, &int32_t_val));
        query_options->__set_statement_expression_limit(int32_t_val);
        break;
      }
      case TImpalaQueryOptions::MAX_STATEMENT_LENGTH_BYTES: {
        MemSpec mem_spec_val{};
        RETURN_IF_ERROR(QueryOptionParser::ParseAndCheckInclusiveRange<MemSpec>(option,
            value, {MIN_MAX_STATEMENT_LENGTH_BYTES},
            {std::numeric_limits<int32_t>::max()}, &mem_spec_val));
        query_options->__set_max_statement_length_bytes(mem_spec_val.value);
        break;
      }
      case TImpalaQueryOptions::DISABLE_DATA_CACHE: {
        query_options->__set_disable_data_cache(IsTrue(value));
        break;
      }
      case TImpalaQueryOptions::DISABLE_HBASE_NUM_ROWS_ESTIMATE: {
        query_options->__set_disable_hbase_num_rows_estimate(IsTrue(value));
        break;
      }
      case TImpalaQueryOptions::FETCH_ROWS_TIMEOUT_MS: {
        int64_t int64_t_val = 0;
        RETURN_IF_ERROR(QueryOptionParser::ParseAndCheckNonNegative<int64_t>(
            option, value, &int64_t_val));
        query_options->__set_fetch_rows_timeout_ms(int64_t_val);
        break;
      }
      case TImpalaQueryOptions::NOW_STRING: {
        query_options->__set_now_string(value);
        break;
      }
      case TImpalaQueryOptions::PARQUET_OBJECT_STORE_SPLIT_SIZE: {
        // The MIN_SYNTHETIC_BLOCK_SIZE from HdfsPartition.java. HdfsScanNode.java forces
        // the block size to be greater than or equal to this value, so reject any
        // attempt to set PARQUET_OBJECT_STORE_SPLIT_SIZE to a value lower than
        // MIN_SYNTHETIC_BLOCK_SIZE.
        constexpr int min_synthetic_block_size = 1024 * 1024;
        MemSpec mem_spec_val{};
        RETURN_IF_ERROR(QueryOptionParser::ParseAndCheckInclusiveLowerBound<MemSpec>(
            option, value, {min_synthetic_block_size}, &mem_spec_val));
        query_options->__set_parquet_object_store_split_size(mem_spec_val.value);
        break;
      }
      case TImpalaQueryOptions::MEM_LIMIT_EXECUTORS: {
        MemSpec mem_spec_val{};
        RETURN_IF_ERROR(QueryOptionParser::Parse<MemSpec>(option, value, &mem_spec_val));
        query_options->__set_mem_limit_executors(mem_spec_val.value);
        break;
      }
      case TImpalaQueryOptions::BROADCAST_BYTES_LIMIT: {
        MemSpec mem_spec_val{};
        RETURN_IF_ERROR(QueryOptionParser::Parse<MemSpec>(option, value, &mem_spec_val));
        query_options->__set_broadcast_bytes_limit(mem_spec_val.value);
        break;
      }
      case TImpalaQueryOptions::RETRY_FAILED_QUERIES: {
        query_options->__set_retry_failed_queries(IsTrue(value));
        break;
      }
      case TImpalaQueryOptions::PREAGG_BYTES_LIMIT: {
        MemSpec mem_spec_val{};
        RETURN_IF_ERROR(QueryOptionParser::Parse<MemSpec>(option, value, &mem_spec_val));
        query_options->__set_preagg_bytes_limit(mem_spec_val.value);
        break;
      }
      case TImpalaQueryOptions::MAX_CNF_EXPRS: {
        int32_t int32_t_val = 0;
        RETURN_IF_ERROR(QueryOptionParser::ParseAndCheckInclusiveLowerBound<int32_t>(
            option, value, -1, &int32_t_val));
        query_options->__set_max_cnf_exprs(int32_t_val);
        break;
      }
      case TImpalaQueryOptions::KUDU_SNAPSHOT_READ_TIMESTAMP_MICROS: {
        int64_t int64_t_val = 0;
        RETURN_IF_ERROR(QueryOptionParser::ParseAndCheckNonNegative<int64_t>(
            option, value, &int64_t_val));
        query_options->__set_kudu_snapshot_read_timestamp_micros(int64_t_val);
        break;
      }
      case TImpalaQueryOptions::ENABLED_RUNTIME_FILTER_TYPES: {
        std::set<TRuntimeFilterType::type> filter_types;
        if (iequals(value, "all")) {
          for (const auto& kv : _TRuntimeFilterType_VALUES_TO_NAMES) {
            filter_types.insert(static_cast<TRuntimeFilterType::type>(kv.first));
          }
        } else {
          // Parse and verify the enabled runtime filter types.
          vector<string> str_types;
          split(str_types, value, is_any_of(","), token_compress_on);
          for (const auto& t : str_types) {
            TRuntimeFilterType::type filter_type;
            RETURN_IF_ERROR(GetThriftEnum(t, "runtime filter type",
                _TRuntimeFilterType_VALUES_TO_NAMES, &filter_type));
            filter_types.insert(filter_type);
          }
        }
        query_options->__set_enabled_runtime_filter_types(filter_types);
        break;
      }
      case TImpalaQueryOptions::ASYNC_CODEGEN: {
        query_options->__set_async_codegen(IsTrue(value));
        break;
      }
      case TImpalaQueryOptions::DISABLE_CODEGEN_CACHE: {
        query_options->__set_disable_codegen_cache(IsTrue(value));
        break;
      }
      case TImpalaQueryOptions::CODEGEN_CACHE_MODE: {
        TCodeGenCacheMode::type enum_type;
        RETURN_IF_ERROR(GetThriftEnum(
            value, "CodeGen Cache Mode", _TCodeGenCacheMode_VALUES_TO_NAMES, &enum_type));
        query_options->__set_codegen_cache_mode(enum_type);
        break;
      }
      case TImpalaQueryOptions::ENABLE_DISTINCT_SEMI_JOIN_OPTIMIZATION: {
        query_options->__set_enable_distinct_semi_join_optimization(IsTrue(value));
        break;
      }
      case TImpalaQueryOptions::SORT_RUN_BYTES_LIMIT: {
        MemSpec mem_spec_val{};
        RETURN_IF_ERROR(QueryOptionParser::Parse<MemSpec>(option, value, &mem_spec_val));
        query_options->__set_sort_run_bytes_limit(mem_spec_val.value);
        break;
      }
      case TImpalaQueryOptions::MAX_FS_WRITERS: {
        int32_t int32_t_val = 0;
        RETURN_IF_ERROR(QueryOptionParser::ParseAndCheckNonNegative<int32_t>(
            option, value, &int32_t_val));
        query_options->__set_max_fs_writers(int32_t_val);
        break;
      }
      case TImpalaQueryOptions::REFRESH_UPDATED_HMS_PARTITIONS: {
        query_options->__set_refresh_updated_hms_partitions(IsTrue(value));
        break;
      }
      case TImpalaQueryOptions::SPOOL_ALL_RESULTS_FOR_RETRIES: {
        query_options->__set_spool_all_results_for_retries(IsTrue(value));
        break;
      }
      case TImpalaQueryOptions::RUNTIME_FILTER_ERROR_RATE: {
        double double_val = 0.0f;
        RETURN_IF_ERROR(QueryOptionParser::ParseAndCheckExclusiveRange<double>(
            option, value, 0.0, 1.0, &double_val));
        query_options->__set_runtime_filter_error_rate(double_val);
        break;
      }
      case TImpalaQueryOptions::USE_LOCAL_TZ_FOR_UNIX_TIMESTAMP_CONVERSIONS: {
        query_options->__set_use_local_tz_for_unix_timestamp_conversions(IsTrue(value));
        break;
      }
      case TImpalaQueryOptions::CONVERT_LEGACY_HIVE_PARQUET_UTC_TIMESTAMPS: {
        query_options->__set_convert_legacy_hive_parquet_utc_timestamps(IsTrue(value));
        break;
      }
      case TImpalaQueryOptions::ENABLE_OUTER_JOIN_TO_INNER_TRANSFORMATION: {
        query_options->__set_enable_outer_join_to_inner_transformation(IsTrue(value));
        break;
      }
      case TImpalaQueryOptions::TARGETED_KUDU_SCAN_RANGE_LENGTH: {
        MemSpec mem_spec_val{};
        RETURN_IF_ERROR(QueryOptionParser::Parse<MemSpec>(option, value, &mem_spec_val));
        query_options->__set_targeted_kudu_scan_range_length(mem_spec_val.value);
        break;
      }
      case TImpalaQueryOptions::REPORT_SKEW_LIMIT: {
        double double_val = 0.0f;
        RETURN_IF_ERROR(QueryOptionParser::Parse<double>(option, value, &double_val));
        query_options->__set_report_skew_limit(double_val);
        break;
      }
      case TImpalaQueryOptions::USE_DOP_FOR_COSTING: {
        query_options->__set_use_dop_for_costing(IsTrue(value));
        break;
      }
      case TImpalaQueryOptions::BROADCAST_TO_PARTITION_FACTOR: {
        double double_val = 0.0f;
        RETURN_IF_ERROR(QueryOptionParser::ParseAndCheckInclusiveRange<double>(
            option, value, 0.0, 1000.0, &double_val));
        query_options->__set_broadcast_to_partition_factor(double_val);
        break;
      }
      case TImpalaQueryOptions::JOIN_ROWS_PRODUCED_LIMIT: {
        int64_t int64_t_val = 0;
        RETURN_IF_ERROR(QueryOptionParser::ParseAndCheckNonNegative<int64_t>(
            option, value, &int64_t_val));
        query_options->__set_join_rows_produced_limit(int64_t_val);
        break;
      }
      case TImpalaQueryOptions::UTF8_MODE: {
        query_options->__set_utf8_mode(IsTrue(value));
        break;
      }
      case TImpalaQueryOptions::ANALYTIC_RANK_PUSHDOWN_THRESHOLD: {
        int64_t int64_t_val = 0;
        RETURN_IF_ERROR(QueryOptionParser::ParseAndCheckInclusiveLowerBound<int64_t>(
            option, value, -1, &int64_t_val));
        query_options->__set_analytic_rank_pushdown_threshold(int64_t_val);
        break;
      }
      case TImpalaQueryOptions::DEFAULT_NDV_SCALE: {
        int32_t int32_t_val = 0;
        RETURN_IF_ERROR(QueryOptionParser::ParseAndCheckInclusiveRange<int32_t>(
            option, value, 1, 10, &int32_t_val));
        query_options->__set_default_ndv_scale(int32_t_val);
        break;
      }
      case TImpalaQueryOptions::KUDU_REPLICA_SELECTION: {
        // Parse the kudu replica selection and validate it.
        TKuduReplicaSelection::type enum_type;
        RETURN_IF_ERROR(GetThriftEnum(value, "kudu replica selection",
            _TKuduReplicaSelection_VALUES_TO_NAMES, &enum_type));
        query_options->__set_kudu_replica_selection(enum_type);
        break;
      }
      case TImpalaQueryOptions::DELETE_STATS_IN_TRUNCATE: {
        query_options->__set_delete_stats_in_truncate(IsTrue(value));
        break;
      }
      case TImpalaQueryOptions::MINMAX_FILTER_SORTED_COLUMNS: {
        query_options->__set_minmax_filter_sorted_columns(IsTrue(value));
        break;
      }
      case TImpalaQueryOptions::MINMAX_FILTER_FAST_CODE_PATH: {
        TMinmaxFilterFastCodePathMode::type enum_type;
        RETURN_IF_ERROR(GetThriftEnum(value, "minmax filter fast code path type",
            _TMinmaxFilterFastCodePathMode_VALUES_TO_NAMES, &enum_type));
        query_options->__set_minmax_filter_fast_code_path(enum_type);
        break;
      }
      case TImpalaQueryOptions::ENABLE_KUDU_TRANSACTION: {
        query_options->__set_enable_kudu_transaction(IsTrue(value));
        break;
      }
      case TImpalaQueryOptions::MINMAX_FILTER_PARTITION_COLUMNS: {
        query_options->__set_minmax_filter_partition_columns(IsTrue(value));
        break;
      }
      case TImpalaQueryOptions::ENABLE_ASYNC_DDL_EXECUTION: {
        query_options->__set_enable_async_ddl_execution(IsTrue(value));
        break;
      }
      case TImpalaQueryOptions::PARQUET_LATE_MATERIALIZATION_THRESHOLD: {
        int32_t int32_t_val = 0;
        RETURN_IF_ERROR(QueryOptionParser::Parse<int32_t>(option, value, &int32_t_val));
        RETURN_IF_ERROR(
            QueryOptionValidator<int32_t>::InclusiveLowerBound(option, int32_t_val, -1));
        RETURN_IF_ERROR(QueryOptionValidator<int32_t>::NotEquals(option, int32_t_val, 0));
        query_options->__set_parquet_late_materialization_threshold(int32_t_val);
        break;
      }
      case TImpalaQueryOptions::PARQUET_DICTIONARY_RUNTIME_FILTER_ENTRY_LIMIT: {
        int32_t int32_t_val = 0;
        RETURN_IF_ERROR(QueryOptionParser::ParseAndCheckNonNegative<int32_t>(
            option, value, &int32_t_val));
        query_options->__set_parquet_dictionary_runtime_filter_entry_limit(int32_t_val);
        break;
      }
      case TImpalaQueryOptions::ENABLE_ASYNC_LOAD_DATA_EXECUTION: {
        query_options->__set_enable_async_load_data_execution(IsTrue(value));
        break;
      }
      case TImpalaQueryOptions::ABORT_JAVA_UDF_ON_EXCEPTION: {
        query_options->__set_abort_java_udf_on_exception(IsTrue(value));
        break;
      }
      case TImpalaQueryOptions::ORC_ASYNC_READ: {
        query_options->__set_orc_async_read(IsTrue(value));
        break;
      }
      case TImpalaQueryOptions::RUNTIME_IN_LIST_FILTER_ENTRY_LIMIT: {
        int32_t int32_t_val = 0;
        RETURN_IF_ERROR(QueryOptionParser::ParseAndCheckNonNegative<int32_t>(
            option, value, &int32_t_val));
        query_options->__set_runtime_in_list_filter_entry_limit(int32_t_val);
        break;
      }
      case TImpalaQueryOptions::LOCK_MAX_WAIT_TIME_S: {
        int32_t int32_t_val = 0;
        RETURN_IF_ERROR(QueryOptionParser::ParseAndCheckNonNegative<int32_t>(
            option, value, &int32_t_val));
        query_options->__set_lock_max_wait_time_s(int32_t_val);
        break;
      }
      case TImpalaQueryOptions::ENABLE_REPLAN: {
        query_options->__set_enable_replan(IsTrue(value));
        break;
      }
      case TImpalaQueryOptions::TEST_REPLAN: {
        query_options->__set_test_replan(IsTrue(value));
        break;
      }
      case TImpalaQueryOptions::ORC_SCHEMA_RESOLUTION: {
        TSchemaResolutionStrategy::type enum_type;
        RETURN_IF_ERROR(GetThriftEnum(value, "orc schema resolution",
            _TSchemaResolutionStrategy_VALUES_TO_NAMES, &enum_type));
        query_options->__set_orc_schema_resolution(enum_type);
        break;
      }
      case TImpalaQueryOptions::EXPAND_COMPLEX_TYPES: {
        query_options->__set_expand_complex_types(IsTrue(value));
        break;
      }
      case TImpalaQueryOptions::FALLBACK_DB_FOR_FUNCTIONS: {
        query_options->__set_fallback_db_for_functions(value);
        break;
      }
      default:
        if (IsRemovedQueryOption(key)) {
          LOG(WARNING) << "Ignoring attempt to set removed query option '" << key << "'";
          return Status::OK();
        }
        // We hit this DCHECK(false) if we forgot to add the corresponding entry here
        // when we add a new query option.
        LOG(ERROR) << "Missing exec option implementation: " << key;
        DCHECK(false);
        break;
    }
    if (set_query_options_mask != nullptr) {
      DCHECK_LT(option, set_query_options_mask->size());
      set_query_options_mask->set(option);
    }
  }
  return Status::OK();
}

Status impala::ParseQueryOptions(const string& options, TQueryOptions* query_options,
    QueryOptionsMask* set_query_options_mask) {
  if (options.length() == 0) return Status::OK();
  vector<string> kv_pairs;
  split(kv_pairs, options, is_any_of(","), token_compress_on);
  // Construct an error status which is used to aggregate errors encountered during
  // parsing. It is only returned if the number of error details is greater than 0.
  Status errorStatus = Status::Expected("Errors parsing query options");
  for (string& kv_string : kv_pairs) {
    trim(kv_string);
    if (kv_string.length() == 0) continue;
    vector<string> key_value;
    split(key_value, kv_string, is_any_of("="), token_compress_on);
    if (key_value.size() != 2) {
      errorStatus.MergeStatus(
          Status(Substitute("Invalid configuration option '$0'.", kv_string)));
      continue;
    }
    errorStatus.MergeStatus(SetQueryOption(
        key_value[0], key_value[1], query_options, set_query_options_mask));
  }
  if (errorStatus.msg().details().size() > 0) return errorStatus;
  return Status::OK();
}

Status impala::ValidateQueryOptions(TQueryOptions* query_options) {
  // Validate that max_result_spooling_mem <=
  // max_spilled_result_spooling_mem (a value of 0 means memory is unbounded).
  int64_t max_mem = query_options->max_result_spooling_mem;
  int64_t max_spilled_mem = query_options->max_spilled_result_spooling_mem;
  if (max_mem == 0 && max_spilled_mem != 0) {
    return Status("If max_result_spooling_mem is set to 0 (unbounded) "
                  "max_spilled_result_spooling_mem must be set to 0 (unbounded) as "
                  "well.");
  }
  if (max_spilled_mem != 0 && max_spilled_mem < max_mem) {
    return Status(Substitute("max_spilled_result_spooling_mem '$0' must be greater than "
                             "max_result_spooling_mem '$1'",
        max_spilled_mem, max_mem));
  }
  return Status::OK();
}

void impala::PopulateQueryOptionLevels(QueryOptionLevels* query_option_levels){
#define QUERY_OPT_FN(NAME, ENUM, LEVEL) \
  { (*query_option_levels)[#ENUM] = LEVEL; }
#define REMOVED_QUERY_OPT_FN(NAME, ENUM) \
  { (*query_option_levels)[#ENUM] = TQueryOptionLevel::REMOVED; }
    QUERY_OPTS_TABLE QUERY_OPT_FN(
        support_start_over, SUPPORT_START_OVER, TQueryOptionLevel::ADVANCED)
#undef QUERY_OPT_FN
#undef REMOVED_QUERY_OPT_FN
}

Status impala::ResetAllQueryOptions(
    TQueryOptions* query_options, QueryOptionsMask* set_query_options_mask) {
  static const TQueryOptions defaults = DefaultQueryOptions();
#define QUERY_OPT_FN(NAME, ENUM, LEVEL)                  \
  if (query_options->NAME != defaults.NAME) {            \
    query_options->__isset.NAME = defaults.__isset.NAME; \
    query_options->NAME = defaults.NAME;                 \
    int option = GetQueryOptionForKey(#NAME);            \
    DCHECK_GE(option, 0);                                \
    if (set_query_options_mask != nullptr) {             \
      DCHECK_LT(option, set_query_options_mask->size()); \
      set_query_options_mask->reset(option);             \
    }                                                    \
  }
#define REMOVED_QUERY_OPT_FN(NAME, ENUM)
  QUERY_OPTS_TABLE
#undef QUERY_OPT_FN
#undef REMOVED_QUERY_OPT_FN
  return Status::OK();
}
