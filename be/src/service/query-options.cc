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

#include "runtime/runtime-filter.h"
#include "util/debug-util.h"
#include "util/mem-info.h"
#include "util/parse-util.h"
#include "util/string-parser.h"
#include "exprs/timezone_db.h"
#include "gen-cpp/ImpalaInternalService_types.h"

#include <sstream>
#include <boost/algorithm/string.hpp>
#include <gutil/strings/strip.h>
#include <gutil/strings/substitute.h>

#include "common/names.h"

DECLARE_int64(min_buffer_size);

using boost::algorithm::iequals;
using boost::algorithm::is_any_of;
using boost::algorithm::token_compress_on;
using boost::algorithm::split;
using boost::algorithm::trim;
using std::to_string;
using beeswax::TQueryOptionLevel;
using namespace impala;
using namespace strings;

DECLARE_int32(idle_session_timeout);

// Utility method to wrap ParseUtil::ParseMemSpec() by returning a Status instead of an
// int.
static Status ParseMemValue(const string& value, const string& key, int64_t* result) {
  bool is_percent;
  *result = ParseUtil::ParseMemSpec(value, &is_percent, MemInfo::physical_mem());
  if (*result < 0) {
    return Status("Failed to parse " + key + " from '" + value + "'.");
  }
  if (is_percent) {
    return Status("Invalid " + key + " with percent '" + value + "'.");
  }
  return Status::OK();
}

void impala::OverlayQueryOptions(const TQueryOptions& src, const QueryOptionsMask& mask,
    TQueryOptions* dst) {
  DCHECK_GT(mask.size(), _TImpalaQueryOptions_VALUES_TO_NAMES.size()) <<
      "Size of QueryOptionsMask must be increased.";
#define QUERY_OPT_FN(NAME, ENUM, LEVEL)\
  if (src.__isset.NAME && mask[TImpalaQueryOptions::ENUM]) dst->__set_##NAME(src.NAME);
#define REMOVED_QUERY_OPT_FN(NAME, ENUM)
  QUERY_OPTS_TABLE
#undef QUERY_OPT_FN
#undef REMOVED_QUERY_OPT_FN
}

// Choose different print function based on the type.
// TODO: In thrift 0.11.0 operator << is implemented for enums and this indirection can be
// removed.
template <typename T, typename std::enable_if_t<std::is_enum<T>::value>* = nullptr>
string PrintQueryOptionValue(const T& option) {
  return PrintThriftEnum(option);
}

template <typename T, typename std::enable_if_t<std::is_arithmetic<T>::value>* = nullptr>
string PrintQueryOptionValue(const T& option) {
  return std::to_string(option);
}

const string& PrintQueryOptionValue(const std::string& option)  {
  return option;
}

const string PrintQueryOptionValue(const impala::TCompressionCodec& compression_codec) {
  if (compression_codec.codec != THdfsCompression::ZSTD) {
    return Substitute("$0", PrintThriftEnum(compression_codec.codec));
  } else {
    return Substitute("$0:$1", PrintThriftEnum(compression_codec.codec),
        compression_codec.compression_level);
  }
}

void impala::TQueryOptionsToMap(const TQueryOptions& query_options,
    map<string, string>* configuration) {
#define QUERY_OPT_FN(NAME, ENUM, LEVEL)\
  {\
    if (query_options.__isset.NAME) { \
      (*configuration)[#ENUM] = PrintQueryOptionValue(query_options.NAME); \
    } else { \
      (*configuration)[#ENUM] = ""; \
    }\
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
#define QUERY_OPT_FN(NAME, ENUM, LEVEL)\
    case TImpalaQueryOptions::ENUM:\
      query_options->__isset.NAME = defaults.__isset.NAME;\
      query_options->NAME = defaults.NAME;\
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

inline bool operator!=(const TCompressionCodec& a,
  const TCompressionCodec& b) {
  return (a.codec != b.codec || a.compression_level != b.compression_level);
}

string impala::DebugQueryOptions(const TQueryOptions& query_options) {
  const static TQueryOptions defaults = DefaultQueryOptions();
  int i = 0;
  stringstream ss;
#define QUERY_OPT_FN(NAME, ENUM, LEVEL)\
  if (query_options.__isset.NAME &&\
      (!defaults.__isset.NAME || query_options.NAME != defaults.NAME)) {\
    if (i++ > 0) ss << ",";\
    ss << #ENUM << "=" << query_options.NAME;\
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
  map<int, const char*>::const_iterator itr =
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
  if (iequals(key, #NAME)) { \
    return true; \
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
  int option = GetQueryOptionForKey(key);
  if (option < 0) {
    return Status(Substitute("Invalid query option: $0", key));
  } else if (value == "") {
    ResetQueryOption(option, query_options);
    if (set_query_options_mask != nullptr) {
      DCHECK_LT(option, set_query_options_mask->size());
      set_query_options_mask->reset(option);
    }
  } else {
    switch (option) {
      case TImpalaQueryOptions::ABORT_ON_ERROR:
        query_options->__set_abort_on_error(IsTrue(value));
        break;
      case TImpalaQueryOptions::MAX_ERRORS:
        query_options->__set_max_errors(atoi(value.c_str()));
        break;
      case TImpalaQueryOptions::DISABLE_CODEGEN:
        query_options->__set_disable_codegen(IsTrue(value));
        break;
      case TImpalaQueryOptions::BATCH_SIZE: {
        StringParser::ParseResult status;
        int val = StringParser::StringToInt<int>(value.c_str(),
            static_cast<int>(value.size()), &status);
        if (status != StringParser::PARSE_SUCCESS || val < 0 || val > 65536) {
          return Status(Substitute("Invalid batch size '$0'. Valid sizes are in"
              "[0, 65536]", value));
        }
        query_options->__set_batch_size(val);
        break;
      }
      case TImpalaQueryOptions::MEM_LIMIT: {
        // Parse the mem limit spec and validate it.
        int64_t bytes_limit;
        RETURN_IF_ERROR(ParseMemValue(value, "query memory limit", &bytes_limit));
        query_options->__set_mem_limit(bytes_limit);
        break;
      }
      case TImpalaQueryOptions::NUM_NODES:
        query_options->__set_num_nodes(atoi(value.c_str()));
        break;
      case TImpalaQueryOptions::MAX_SCAN_RANGE_LENGTH: {
        int64_t scan_length = 0;
        RETURN_IF_ERROR(ParseMemValue(value, "scan range length", &scan_length));
        query_options->__set_max_scan_range_length(scan_length);
        break;
      }
      case TImpalaQueryOptions::NUM_SCANNER_THREADS:
        query_options->__set_num_scanner_threads(atoi(value.c_str()));
        break;
      case TImpalaQueryOptions::DEBUG_ACTION:
        query_options->__set_debug_action(value.c_str());
        break;
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
      case TImpalaQueryOptions::HBASE_CACHING:
        query_options->__set_hbase_caching(atoi(value.c_str()));
        break;
      case TImpalaQueryOptions::HBASE_CACHE_BLOCKS:
        query_options->__set_hbase_cache_blocks(IsTrue(value));
        break;
      case TImpalaQueryOptions::PARQUET_FILE_SIZE: {
        int64_t file_size;
        RETURN_IF_ERROR(ParseMemValue(value, "parquet file size", &file_size));
        if (file_size > numeric_limits<int32_t>::max()) {
          // Do not allow values greater than or equal to 2GB since hdfsOpenFile() from
          // the HDFS API gets an int32 blocksize parameter (see HDFS-8949).
          stringstream ss;
          ss << "The PARQUET_FILE_SIZE query option must be less than 2GB.";
          return Status(ss.str());
        } else {
          query_options->__set_parquet_file_size(file_size);
        }
        break;
      }
      case TImpalaQueryOptions::EXPLAIN_LEVEL: {
        TExplainLevel::type enum_type;
        RETURN_IF_ERROR(GetThriftEnum(value, "explain level",
            _TExplainLevel_VALUES_TO_NAMES, &enum_type));
        query_options->__set_explain_level(enum_type);
        break;
      }
      case TImpalaQueryOptions::SYNC_DDL:
        query_options->__set_sync_ddl(IsTrue(value));
        break;
      case TImpalaQueryOptions::REQUEST_POOL:
        query_options->__set_request_pool(value);
        break;
      case TImpalaQueryOptions::DISABLE_OUTERMOST_TOPN:
        query_options->__set_disable_outermost_topn(IsTrue(value));
        break;
      case TImpalaQueryOptions::QUERY_TIMEOUT_S: {
        StringParser::ParseResult result;
        const int32_t timeout_s =
            StringParser::StringToInt<int32_t>(value.c_str(), value.length(), &result);
        if (result != StringParser::PARSE_SUCCESS || timeout_s < 0) {
          return Status(
              Substitute("Invalid query timeout: '$0'. "
                         "Only non-negative numbers are allowed.", value));
        }
        query_options->__set_query_timeout_s(timeout_s);
        break;
      }
      case TImpalaQueryOptions::BUFFER_POOL_LIMIT: {
        int64_t mem;
        RETURN_IF_ERROR(ParseMemValue(value, "buffer pool limit", &mem));
        query_options->__set_buffer_pool_limit(mem);
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
      case TImpalaQueryOptions::EXEC_SINGLE_NODE_ROWS_THRESHOLD:
        query_options->__set_exec_single_node_rows_threshold(atoi(value.c_str()));
        break;
      case TImpalaQueryOptions::OPTIMIZE_PARTITION_KEY_SCANS:
        query_options->__set_optimize_partition_key_scans(IsTrue(value));
        break;
      case TImpalaQueryOptions::OPTIMIZE_SIMPLE_LIMIT:
        query_options->__set_optimize_simple_limit(IsTrue(value));
        break;
      case TImpalaQueryOptions::REPLICA_PREFERENCE: {
        map<int, const char *> valid_enums_values = {
            {0, "CACHE_LOCAL"},
            {2, "DISK_LOCAL"},
            {4, "REMOTE"}
        };
        TReplicaPreference::type enum_type;
        RETURN_IF_ERROR(GetThriftEnum(value, "replica memory distance preference",
            valid_enums_values, &enum_type));
        query_options->__set_replica_preference(enum_type);
        break;
      }
      case TImpalaQueryOptions::SCHEDULE_RANDOM_REPLICA:
        query_options->__set_schedule_random_replica(IsTrue(value));
        break;
      case TImpalaQueryOptions::DISABLE_STREAMING_PREAGGREGATIONS:
        query_options->__set_disable_streaming_preaggregations(IsTrue(value));
        break;
      case TImpalaQueryOptions::RUNTIME_FILTER_MODE: {
        TRuntimeFilterMode::type enum_type;
        RETURN_IF_ERROR(GetThriftEnum(value, "runtime filter mode",
            _TRuntimeFilterMode_VALUES_TO_NAMES, &enum_type));
        query_options->__set_runtime_filter_mode(enum_type);
        break;
      }
      case TImpalaQueryOptions::RUNTIME_FILTER_MAX_SIZE:
      case TImpalaQueryOptions::RUNTIME_FILTER_MIN_SIZE:
      case TImpalaQueryOptions::RUNTIME_BLOOM_FILTER_SIZE: {
        int64_t size;
        RETURN_IF_ERROR(ParseMemValue(value, "Bloom filter size", &size));
        if (size < RuntimeFilterBank::MIN_BLOOM_FILTER_SIZE ||
            size > RuntimeFilterBank::MAX_BLOOM_FILTER_SIZE) {
          return Status(Substitute("$0 is not a valid Bloom filter size for $1. "
                  "Valid sizes are in [$2, $3].", value, PrintThriftEnum(
                      static_cast<TImpalaQueryOptions::type>(option)),
                  RuntimeFilterBank::MIN_BLOOM_FILTER_SIZE,
                  RuntimeFilterBank::MAX_BLOOM_FILTER_SIZE));
        }
        if (option == TImpalaQueryOptions::RUNTIME_FILTER_MAX_SIZE
            && size < FLAGS_min_buffer_size
            // last condition is to unblock the highly improbable case where the
            // min_buffer_size is greater than RuntimeFilterBank::MAX_BLOOM_FILTER_SIZE.
            && FLAGS_min_buffer_size <= RuntimeFilterBank::MAX_BLOOM_FILTER_SIZE) {
          return Status(Substitute("$0 should not be less than $1 which is the minimum "
              "buffer size that can be allocated by the buffer pool",
              PrintThriftEnum(static_cast<TImpalaQueryOptions::type>(option)),
              FLAGS_min_buffer_size));
        }
        if (option == TImpalaQueryOptions::RUNTIME_BLOOM_FILTER_SIZE) {
          query_options->__set_runtime_bloom_filter_size(size);
        } else if (option == TImpalaQueryOptions::RUNTIME_FILTER_MIN_SIZE) {
          query_options->__set_runtime_filter_min_size(size);
        } else if (option == TImpalaQueryOptions::RUNTIME_FILTER_MAX_SIZE) {
          query_options->__set_runtime_filter_max_size(size);
        }
        break;
      }
      case TImpalaQueryOptions::RUNTIME_FILTER_WAIT_TIME_MS: {
        StringParser::ParseResult result;
        const int32_t time_ms =
            StringParser::StringToInt<int32_t>(value.c_str(), value.length(), &result);
        if (result != StringParser::PARSE_SUCCESS || time_ms < 0) {
          return Status(
              Substitute("$0 is not a valid wait time. Valid sizes are in [0, $1].",
                  value, numeric_limits<int32_t>::max()));
        }
        query_options->__set_runtime_filter_wait_time_ms(time_ms);
        break;
      }
      case TImpalaQueryOptions::DISABLE_ROW_RUNTIME_FILTERING:
        query_options->__set_disable_row_runtime_filtering(IsTrue(value));
        break;
      case TImpalaQueryOptions::MINMAX_FILTERING_LEVEL:
        // Parse the enabled runtime filter types and validate it.
        TMinmaxFilteringLevel::type enum_type;
        RETURN_IF_ERROR(GetThriftEnum(value, "minmax filter level",
            _TMinmaxFilteringLevel_VALUES_TO_NAMES, &enum_type));
        query_options->__set_minmax_filtering_level(enum_type);
        break;
      case TImpalaQueryOptions::MINMAX_FILTER_THRESHOLD: {
        StringParser::ParseResult status;
        double val = StringParser::StringToFloat<double>(
            value.c_str(), static_cast<int>(value.size()), &status);
        if (status != StringParser::PARSE_SUCCESS || val < 0.0 || val > 1.0) {
          return Status(
              Substitute("Invalid minmax filter threshold '$0'. Valid sizes are in"
                         "[0.0, 1.0]",
                  value));
        }
        query_options->__set_minmax_filter_threshold(val);
        break;
      }
      case TImpalaQueryOptions::MAX_NUM_RUNTIME_FILTERS: {
        StringParser::ParseResult status;
        int val = StringParser::StringToInt<int>(value.c_str(), value.size(), &status);
        if (status != StringParser::PARSE_SUCCESS) {
          return Status(Substitute("Invalid number of runtime filters: '$0'.", value));
        }
        if (val < 0) {
          return Status(Substitute("Invalid number of runtime filters: '$0'. "
              "Only positive values are allowed.", val));
        }
        query_options->__set_max_num_runtime_filters(val);
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
        StringParser::ParseResult result;
        const int32_t dop =
            StringParser::StringToInt<int32_t>(value.c_str(), value.length(), &result);
        if (result != StringParser::PARSE_SUCCESS || dop < 0 || dop > 64) {
          return Status(
              Substitute("$0 is not valid for mt_dop. Valid values are in "
                "[0, 64].", value));
        }
        query_options->__set_mt_dop(dop);
        break;
      }
      case TImpalaQueryOptions::S3_SKIP_INSERT_STAGING: {
        query_options->__set_s3_skip_insert_staging(IsTrue(value));
        break;
      }
      case TImpalaQueryOptions::PREFETCH_MODE: {
        TPrefetchMode::type enum_type;
        RETURN_IF_ERROR(GetThriftEnum(value, "prefetch mode",
            _TPrefetchMode_VALUES_TO_NAMES, &enum_type));
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
          int64_t bytes_limit;
          RETURN_IF_ERROR(ParseMemValue(value, "Scratch space memory limit",
              &bytes_limit));
          query_options->__set_scratch_limit(bytes_limit);
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
        StringParser::ParseResult status;
        int val = StringParser::StringToInt<int>(value.c_str(), value.size(), &status);
        if (status != StringParser::PARSE_SUCCESS) {
          return Status(Substitute("Invalid threshold: '$0'.", value));
        }
        if (val < 0) {
          return Status(Substitute(
              "Invalid threshold: '$0'. Only positive values are allowed.", val));
        }
        query_options->__set_disable_codegen_rows_threshold(val);
        break;
      }
      case TImpalaQueryOptions::DEFAULT_SPILLABLE_BUFFER_SIZE: {
        int64_t buffer_size_bytes;
        RETURN_IF_ERROR(
            ParseMemValue(value, "Default spillable buffer size", &buffer_size_bytes));
        if (!BitUtil::IsPowerOf2(buffer_size_bytes)) {
          return Status(
              Substitute("Default spillable buffer size must be a power of two: $0",
                  buffer_size_bytes));
        }
        if (buffer_size_bytes > SPILLABLE_BUFFER_LIMIT) {
          return Status(Substitute(
              "Default spillable buffer size must be less than or equal to: $0",
              SPILLABLE_BUFFER_LIMIT));
        }
        query_options->__set_default_spillable_buffer_size(buffer_size_bytes);
        break;
      }
      case TImpalaQueryOptions::MIN_SPILLABLE_BUFFER_SIZE: {
        int64_t buffer_size_bytes;
        RETURN_IF_ERROR(
            ParseMemValue(value, "Minimum spillable buffer size", &buffer_size_bytes));
        if (!BitUtil::IsPowerOf2(buffer_size_bytes)) {
          return Status(
              Substitute("Minimum spillable buffer size must be a power of two: $0",
                  buffer_size_bytes));
        }
        if (buffer_size_bytes > SPILLABLE_BUFFER_LIMIT) {
          return Status(Substitute(
              "Minimum spillable buffer size must be less than or equal to: $0",
              SPILLABLE_BUFFER_LIMIT));
        }
        query_options->__set_min_spillable_buffer_size(buffer_size_bytes);
        break;
      }
      case TImpalaQueryOptions::MAX_ROW_SIZE: {
        int64_t max_row_size_bytes;
        RETURN_IF_ERROR(ParseMemValue(value, "Max row size", &max_row_size_bytes));
        if (max_row_size_bytes <= 0 || max_row_size_bytes > ROW_SIZE_LIMIT) {
          return Status(
              Substitute("Invalid max row size of $0. Valid sizes are in [$1, $2]", value,
                  1, ROW_SIZE_LIMIT));
        }
        query_options->__set_max_row_size(max_row_size_bytes);
        break;
      }
      case TImpalaQueryOptions::IDLE_SESSION_TIMEOUT: {
        StringParser::ParseResult result;
        const int32_t requested_timeout =
            StringParser::StringToInt<int32_t>(value.c_str(), value.length(), &result);
        if (result != StringParser::PARSE_SUCCESS || requested_timeout < 0) {
          return Status(
              Substitute("Invalid idle session timeout: '$0'. "
                         "Only non-negative numbers are allowed.", value));
        }
        query_options->__set_idle_session_timeout(requested_timeout);
        break;
      }
      case TImpalaQueryOptions::COMPUTE_STATS_MIN_SAMPLE_SIZE: {
        int64_t min_sample_size;
        RETURN_IF_ERROR(ParseMemValue(value, "Min sample size", &min_sample_size));
        if (min_sample_size < 0) {
          return Status(
              Substitute("Min sample size must be greater or equal to zero: $0", value));
        }
        query_options->__set_compute_stats_min_sample_size(min_sample_size);
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
        StringParser::ParseResult result;
        const int32_t time_limit =
            StringParser::StringToInt<int32_t>(value.c_str(), value.length(), &result);
        if (result != StringParser::PARSE_SUCCESS || time_limit < 0) {
          return Status(
              Substitute("Invalid query time limit: '$0'. "
                         "Only non-negative numbers are allowed.", value));
        }
        query_options->__set_exec_time_limit_s(time_limit);
        break;
      }
      case TImpalaQueryOptions::SHUFFLE_DISTINCT_EXPRS: {
        query_options->__set_shuffle_distinct_exprs(IsTrue(value));
        break;
      }
      case TImpalaQueryOptions::MAX_MEM_ESTIMATE_FOR_ADMISSION: {
        int64_t bytes_limit;
        RETURN_IF_ERROR(ParseMemValue(
            value, "max memory estimate for admission", &bytes_limit));
        query_options->__set_max_mem_estimate_for_admission(bytes_limit);
        break;
      }
      case TImpalaQueryOptions::THREAD_RESERVATION_LIMIT:
      case TImpalaQueryOptions::THREAD_RESERVATION_AGGREGATE_LIMIT: {
        // Parsing logic is identical for these two options.
        StringParser::ParseResult status;
        int val = StringParser::StringToInt<int>(value.c_str(), value.size(), &status);
        if (status != StringParser::PARSE_SUCCESS) {
          return Status(Substitute("Invalid thread count: '$0'.", value));
        }
        if (val < -1) {
          return Status(Substitute("Invalid thread count: '$0'. "
              "Only -1 and non-negative values are allowed.", val));
        }
        if (option == TImpalaQueryOptions::THREAD_RESERVATION_LIMIT) {
          query_options->__set_thread_reservation_limit(val);
        } else {
          DCHECK_EQ(option, TImpalaQueryOptions::THREAD_RESERVATION_AGGREGATE_LIMIT);
          query_options->__set_thread_reservation_aggregate_limit(val);
        }
        break;
      }
      case TImpalaQueryOptions::KUDU_READ_MODE: {
        TKuduReadMode::type enum_type;
        RETURN_IF_ERROR(GetThriftEnum(value, "Kudu read mode",
            _TKuduReadMode_VALUES_TO_NAMES, &enum_type));
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
        int64_t bytes_limit;
        RETURN_IF_ERROR(ParseMemValue(value, "query scan bytes limit", &bytes_limit));
        query_options->__set_scan_bytes_limit(bytes_limit);
        break;
      }
      case TImpalaQueryOptions::CPU_LIMIT_S: {
        StringParser::ParseResult result;
        const int64_t cpu_limit_s =
            StringParser::StringToInt<int64_t>(value.c_str(), value.length(), &result);
        if (result != StringParser::PARSE_SUCCESS || cpu_limit_s < 0) {
          return Status(
              Substitute("Invalid CPU limit: '$0'. "
                         "Only non-negative numbers are allowed.", value));
        }
        query_options->__set_cpu_limit_s(cpu_limit_s);
        break;
      }
      case TImpalaQueryOptions::TOPN_BYTES_LIMIT: {
        int64_t topn_bytes_limit;
        RETURN_IF_ERROR(ParseMemValue(value, "topn bytes limit", &topn_bytes_limit));
        query_options->__set_topn_bytes_limit(topn_bytes_limit);
        break;
      }
      case TImpalaQueryOptions::CLIENT_IDENTIFIER: {
        query_options->__set_client_identifier(value);
        break;
      }
      case TImpalaQueryOptions::RESOURCE_TRACE_RATIO: {
        StringParser::ParseResult result;
        const double val =
            StringParser::StringToFloat<double>(value.c_str(), value.length(), &result);
        if (result != StringParser::PARSE_SUCCESS || val < 0 || val > 1) {
          return Status(Substitute("Invalid resource trace ratio: '$0'. "
                                   "Only values from 0 to 1 are allowed.",
              value));
        }
        query_options->__set_resource_trace_ratio(val);
        break;
      }
      case TImpalaQueryOptions::PLANNER_TESTCASE_MODE: {
        query_options->__set_planner_testcase_mode(IsTrue(value));
        break;
      }
      case TImpalaQueryOptions::NUM_REMOTE_EXECUTOR_CANDIDATES: {
        StringParser::ParseResult result;
        const int64_t num_remote_executor_candidates =
            StringParser::StringToInt<int64_t>(value.c_str(), value.length(), &result);
        if (result != StringParser::PARSE_SUCCESS ||
            num_remote_executor_candidates < 0 || num_remote_executor_candidates > 16) {
          return Status(
              Substitute("$0 is not valid for num_remote_executor_candidates. "
                         "Valid values are in [0, 16].", value));
        }
        query_options->__set_num_remote_executor_candidates(
            num_remote_executor_candidates);
        break;
      }
      case TImpalaQueryOptions::NUM_ROWS_PRODUCED_LIMIT: {
        StringParser::ParseResult result;
        const int64_t num_rows_produced_limit =
            StringParser::StringToInt<int64_t>(value.c_str(), value.length(), &result);
        if (result != StringParser::PARSE_SUCCESS || num_rows_produced_limit < 0) {
          return Status(Substitute("Invalid rows returned limit: '$0'. "
                                   "Only non-negative numbers are allowed.", value));
        }
        query_options->__set_num_rows_produced_limit(num_rows_produced_limit);
        break;
      }
      case TImpalaQueryOptions::DEFAULT_FILE_FORMAT: {
        THdfsFileFormat::type enum_type;
        RETURN_IF_ERROR(GetThriftEnum(value, "default file format",
            _THdfsFileFormat_VALUES_TO_NAMES, &enum_type));
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
        StringParser::ParseResult result;
        const int32_t row_count_limit =
            StringParser::StringToInt<int32_t>(value.c_str(), value.length(), &result);
        if (result != StringParser::PARSE_SUCCESS || row_count_limit <= 0) {
          return Status("Parquet page row count limit must be a positive integer.");
        }
        query_options->__set_parquet_page_row_count_limit(row_count_limit);
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
        int64_t max_result_spooling_mem;
        RETURN_IF_ERROR(ParseMemValue(value, "max result spooling memory",
            &max_result_spooling_mem));
        query_options->__set_max_result_spooling_mem(
            max_result_spooling_mem);
        break;
      }
      case TImpalaQueryOptions::MAX_SPILLED_RESULT_SPOOLING_MEM: {
        int64_t max_spilled_result_spooling_mem;
        RETURN_IF_ERROR(ParseMemValue(value, "max spilled result spooling memory",
            &max_spilled_result_spooling_mem));
        query_options->__set_max_spilled_result_spooling_mem(
            max_spilled_result_spooling_mem);
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
        StringParser::ParseResult result;
        const int32_t statement_expression_limit =
          StringParser::StringToInt<int32_t>(value.c_str(), value.length(), &result);
        if (result != StringParser::PARSE_SUCCESS ||
            statement_expression_limit < MIN_STATEMENT_EXPRESSION_LIMIT) {
          return Status(Substitute("Invalid statement expression limit: $0 "
              "Valid values are in [$1, $2]", value, MIN_STATEMENT_EXPRESSION_LIMIT,
              std::numeric_limits<int32_t>::max()));
        }
        query_options->__set_statement_expression_limit(statement_expression_limit);
        break;
      }
      case TImpalaQueryOptions::MAX_STATEMENT_LENGTH_BYTES: {
        int64_t max_statement_length_bytes;
        RETURN_IF_ERROR(ParseMemValue(value, "max statement length bytes",
            &max_statement_length_bytes));
        if (max_statement_length_bytes < MIN_MAX_STATEMENT_LENGTH_BYTES ||
            max_statement_length_bytes > std::numeric_limits<int32_t>::max()) {
          return Status(Substitute("Invalid maximum statement length: $0 "
              "Valid values are in [$1, $2]", max_statement_length_bytes,
              MIN_MAX_STATEMENT_LENGTH_BYTES, std::numeric_limits<int32_t>::max()));
        }
        query_options->__set_max_statement_length_bytes(max_statement_length_bytes);
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
        StringParser::ParseResult result;
        const int64_t requested_timeout =
            StringParser::StringToInt<int64_t>(value.c_str(), value.length(), &result);
        if (result != StringParser::PARSE_SUCCESS || requested_timeout < 0) {
          return Status(
              Substitute("Invalid fetch rows timeout: '$0'. "
                         "Only non-negative numbers are allowed.", value));
        }
        query_options->__set_fetch_rows_timeout_ms(requested_timeout);
        break;
      }
      case TImpalaQueryOptions::NOW_STRING: {
        query_options->__set_now_string(value);
        break;
      }
      case TImpalaQueryOptions::PARQUET_OBJECT_STORE_SPLIT_SIZE: {
        int64_t parquet_object_store_split_size;
        RETURN_IF_ERROR(ParseMemValue(
            value, "parquet object store split size", &parquet_object_store_split_size));
        // The MIN_SYNTHETIC_BLOCK_SIZE from HdfsPartition.java. HdfsScanNode.java forces
        // the block size to be greater than or equal to this value, so reject any
        // attempt to set PARQUET_OBJECT_STORE_SPLIT_SIZE to a value lower than
        // MIN_SYNTHETIC_BLOCK_SIZE.
        int min_synthetic_block_size = 1024 * 1024;
        if (parquet_object_store_split_size < min_synthetic_block_size) {
          return Status(Substitute("Invalid parquet object store split size: '$0'. Must "
                                   "be greater than or equal to '$1'.",
              value, min_synthetic_block_size));
        }
        query_options->__set_parquet_object_store_split_size(
            parquet_object_store_split_size);
        break;
      }
      case TImpalaQueryOptions::MEM_LIMIT_EXECUTORS: {
        // Parse the mem limit spec and validate it.
        int64_t bytes_limit;
        RETURN_IF_ERROR(
            ParseMemValue(value, "query memory limit for executors", &bytes_limit));
        query_options->__set_mem_limit_executors(bytes_limit);
        break;
      }
      case TImpalaQueryOptions::BROADCAST_BYTES_LIMIT: {
        // Parse the broadcast_bytes limit and validate it
        int64_t broadcast_bytes_limit;
        RETURN_IF_ERROR(
            ParseMemValue(value, "broadcast bytes limit for join operations",
                &broadcast_bytes_limit));
        query_options->__set_broadcast_bytes_limit(broadcast_bytes_limit);
        break;
      }
      case TImpalaQueryOptions::RETRY_FAILED_QUERIES: {
        query_options->__set_retry_failed_queries(IsTrue(value));
        break;
      }
      case TImpalaQueryOptions::PREAGG_BYTES_LIMIT: {
        // Parse the preaggregation bytes limit and validate it
        int64_t preagg_bytes_limit;
        RETURN_IF_ERROR(
            ParseMemValue(value, "preaggregation bytes limit", &preagg_bytes_limit));
        query_options->__set_preagg_bytes_limit(preagg_bytes_limit);
        break;
      }
      case TImpalaQueryOptions::MAX_CNF_EXPRS: {
        StringParser::ParseResult result;
        const int32_t requested_max_cnf_exprs =
            StringParser::StringToInt<int32_t>(value.c_str(), value.length(), &result);
        if (result != StringParser::PARSE_SUCCESS || requested_max_cnf_exprs < -1) {
          return Status(
              Substitute("Invalid max cnf exprs : '$0'. "
                         "Only -1 and non-negative numbers are allowed.", value));
        }
        query_options->__set_max_cnf_exprs(requested_max_cnf_exprs);
        break;
      }
      case TImpalaQueryOptions::KUDU_SNAPSHOT_READ_TIMESTAMP_MICROS: {
        StringParser::ParseResult result;
        const int64_t timestamp =
            StringParser::StringToInt<int64_t>(value.c_str(), value.length(), &result);
        if (result != StringParser::PARSE_SUCCESS || timestamp < 0) {
          return Status(Substitute("Invalid Kudu snapshot read timestamp: Only "
              "non-negative numbers are allowed.", value));
        }
        query_options->__set_kudu_snapshot_read_timestamp_micros(timestamp);
        break;
      }
      case TImpalaQueryOptions::ENABLED_RUNTIME_FILTER_TYPES: {
        // Parse the enabled runtime filter types and validate it.
        TEnabledRuntimeFilterTypes::type enum_type;
        RETURN_IF_ERROR(GetThriftEnum(value, "enabled runtime filter types",
            _TEnabledRuntimeFilterTypes_VALUES_TO_NAMES, &enum_type));
        query_options->__set_enabled_runtime_filter_types(enum_type);
        break;
      }
      case TImpalaQueryOptions::ASYNC_CODEGEN: {
        query_options->__set_async_codegen(IsTrue(value));
        break;
      }
      case TImpalaQueryOptions::ENABLE_DISTINCT_SEMI_JOIN_OPTIMIZATION: {
        query_options->__set_enable_distinct_semi_join_optimization(IsTrue(value));
        break;
      }
      case TImpalaQueryOptions::SORT_RUN_BYTES_LIMIT: {
        // Parse the sort bytes limit and validate it
        int64_t sort_run_bytes_limit;
        RETURN_IF_ERROR(
            ParseMemValue(value, "sort run bytes limit", &sort_run_bytes_limit));
        query_options->__set_sort_run_bytes_limit(sort_run_bytes_limit);
        break;
      }
      case TImpalaQueryOptions::MAX_FS_WRITERS: {
        StringParser::ParseResult result;
        const int32_t max_fs_writers =
            StringParser::StringToInt<int32_t>(value.c_str(), value.length(), &result);
        if (result != StringParser::PARSE_SUCCESS || max_fs_writers < 0) {
          return Status(Substitute("$0 is not valid for MAX_FS_WRITERS. Only "
                                   "non-negative numbers are allowed.",
              value));
        }
        query_options->__set_max_fs_writers(max_fs_writers);
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
        StringParser::ParseResult result;
        const double val =
            StringParser::StringToFloat<double>(value.c_str(), value.length(), &result);
        if (result != StringParser::PARSE_SUCCESS || val <= 0 || val >= 1) {
          return Status(Substitute("Invalid runtime filter error rate: "
                "'$0'. Only values between 0 to 1 (exclusive) are allowed.", value));
        }
        query_options->__set_runtime_filter_error_rate(val);
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
        int64_t scan_length = 0;
        RETURN_IF_ERROR(
            ParseMemValue(value, "targeted kudu scan range length", &scan_length));
        query_options->__set_targeted_kudu_scan_range_length(scan_length);
        break;
      }
      case TImpalaQueryOptions::REPORT_SKEW_LIMIT: {
        StringParser::ParseResult result;
        const double skew_threshold =
            StringParser::StringToFloat<double>(value.c_str(), value.length(), &result);
        if (result != StringParser::PARSE_SUCCESS) {
          return Status(
              Substitute("$0 is not valid for REPORT_SKEW_LIMIT. Only numeric "
                         "values (such as 1.0) are allowed.",
                  value));
        }
        query_options->__set_report_skew_limit(skew_threshold);
        break;
      }
      case TImpalaQueryOptions::USE_DOP_FOR_COSTING: {
        query_options->__set_use_dop_for_costing(IsTrue(value));
        break;
      }
      case TImpalaQueryOptions::BROADCAST_TO_PARTITION_FACTOR: {
        StringParser::ParseResult result;
        const double val =
            StringParser::StringToFloat<double>(value.c_str(), value.length(), &result);
        if (result != StringParser::PARSE_SUCCESS || val < 0 || val > 1000) {
          return Status(Substitute("Invalid broadcast to partition factor '$0'. "
                                   "Only values from 0 to 1000 are allowed.",
              value));
        }
        query_options->__set_broadcast_to_partition_factor(val);
        break;
      }
      case TImpalaQueryOptions::JOIN_ROWS_PRODUCED_LIMIT: {
        StringParser::ParseResult result;
        const int64_t join_rows_produced_limit =
            StringParser::StringToInt<int64_t>(value.c_str(), value.length(), &result);
        if (result != StringParser::PARSE_SUCCESS || join_rows_produced_limit < 0) {
          return Status(Substitute("Invalid join rows produced limit: '$0'. "
                                   "Only non-negative numbers are allowed.", value));
        }
        query_options->__set_join_rows_produced_limit(join_rows_produced_limit);
        break;
      }
      case TImpalaQueryOptions::UTF8_MODE: {
        query_options->__set_utf8_mode(IsTrue(value));
        break;
      }
      case TImpalaQueryOptions::ANALYTIC_RANK_PUSHDOWN_THRESHOLD: {
        StringParser::ParseResult status;
        int64_t val =
            StringParser::StringToInt<int64_t>(value.c_str(), value.size(), &status);
        if (status != StringParser::PARSE_SUCCESS) {
          return Status(Substitute("Invalid threshold: '$0'.", value));
        }
        if (val < -1) {
          return Status(Substitute("Invalid threshold: '$0'. Only non-negative values "
                "and -1 are allowed.", val));
        }
        query_options->__set_analytic_rank_pushdown_threshold(val);
        break;
      }
      case TImpalaQueryOptions::DEFAULT_NDV_SCALE: {
        StringParser::ParseResult result;
        const int32_t scale =
            StringParser::StringToInt<int32_t>(value.c_str(), value.length(), &result);
        if (value == nullptr || result != StringParser::PARSE_SUCCESS || scale < 1 ||
            scale > 10) {
          return Status(
              Substitute("Invalid NDV scale: '$0'. "
                         "Only integer value in [1, 10] is allowed.", value));
        }
        query_options->__set_default_ndv_scale(scale);
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
      case TImpalaQueryOptions::MINMAX_FILTER_PARTITION_COLUMNS:
        query_options->__set_minmax_filter_partition_columns(IsTrue(value));
        break;
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
    if (set_query_options_mask != NULL) {
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
  for (string& kv_string: kv_pairs) {
    trim(kv_string);
    if (kv_string.length() == 0) continue;
    vector<string> key_value;
    split(key_value, kv_string, is_any_of("="), token_compress_on);
    if (key_value.size() != 2) {
      errorStatus.MergeStatus(
          Status(Substitute("Invalid configuration option '$0'.", kv_string)));
      continue;
    }
    errorStatus.MergeStatus(SetQueryOption(key_value[0], key_value[1], query_options,
        set_query_options_mask));
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

void impala::PopulateQueryOptionLevels(QueryOptionLevels* query_option_levels)
{
#define QUERY_OPT_FN(NAME, ENUM, LEVEL)\
  {\
    (*query_option_levels)[#ENUM] = LEVEL;\
  }
#define REMOVED_QUERY_OPT_FN(NAME, ENUM)\
  {\
    (*query_option_levels)[#ENUM] = TQueryOptionLevel::REMOVED;\
  }
  QUERY_OPTS_TABLE
  QUERY_OPT_FN(support_start_over, SUPPORT_START_OVER, TQueryOptionLevel::ADVANCED)
#undef QUERY_OPT_FN
#undef REMOVED_QUERY_OPT_FN
}
