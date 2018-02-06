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
#include "gen-cpp/ImpalaInternalService_types.h"

#include <sstream>
#include <boost/algorithm/string.hpp>
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

void impala::TQueryOptionsToMap(const TQueryOptions& query_options,
    map<string, string>* configuration) {
#define QUERY_OPT_FN(NAME, ENUM, LEVEL)\
  {\
    if (query_options.__isset.NAME) { \
      stringstream val;\
      val << query_options.NAME;\
      (*configuration)[#ENUM] = val.str();\
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
        query_options->__set_abort_on_error(
            iequals(value, "true") || iequals(value, "1"));
        break;
      case TImpalaQueryOptions::MAX_ERRORS:
        query_options->__set_max_errors(atoi(value.c_str()));
        break;
      case TImpalaQueryOptions::DISABLE_CODEGEN:
        query_options->__set_disable_codegen(
            iequals(value, "true") || iequals(value, "1"));
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
      case TImpalaQueryOptions::ALLOW_UNSUPPORTED_FORMATS:
        query_options->__set_allow_unsupported_formats(
            iequals(value, "true") || iequals(value, "1"));
        break;
      case TImpalaQueryOptions::DEBUG_ACTION:
        query_options->__set_debug_action(value.c_str());
        break;
      case TImpalaQueryOptions::SEQ_COMPRESSION_MODE: {
        if (iequals(value, "block")) {
          query_options->__set_seq_compression_mode(THdfsSeqCompressionMode::BLOCK);
        } else if (iequals(value, "record")) {
          query_options->__set_seq_compression_mode(THdfsSeqCompressionMode::RECORD);
        } else {
          stringstream ss;
          ss << "Invalid sequence file compression mode: " << value;
          return Status(ss.str());
        }
        break;
      }
      case TImpalaQueryOptions::COMPRESSION_CODEC: {
        if (iequals(value, "none")) {
          query_options->__set_compression_codec(THdfsCompression::NONE);
        } else if (iequals(value, "gzip")) {
          query_options->__set_compression_codec(THdfsCompression::GZIP);
        } else if (iequals(value, "bzip2")) {
          query_options->__set_compression_codec(THdfsCompression::BZIP2);
        } else if (iequals(value, "default")) {
          query_options->__set_compression_codec(THdfsCompression::DEFAULT);
        } else if (iequals(value, "snappy")) {
          query_options->__set_compression_codec(THdfsCompression::SNAPPY);
        } else if (iequals(value, "snappy_blocked")) {
          query_options->__set_compression_codec(THdfsCompression::SNAPPY_BLOCKED);
        } else {
          stringstream ss;
          ss << "Invalid compression codec: " << value;
          return Status(ss.str());
        }
        break;
      }
      case TImpalaQueryOptions::HBASE_CACHING:
        query_options->__set_hbase_caching(atoi(value.c_str()));
        break;
      case TImpalaQueryOptions::HBASE_CACHE_BLOCKS:
        query_options->__set_hbase_cache_blocks(
            iequals(value, "true") || iequals(value, "1"));
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
      case TImpalaQueryOptions::EXPLAIN_LEVEL:
        if (iequals(value, "minimal") || iequals(value, "0")) {
          query_options->__set_explain_level(TExplainLevel::MINIMAL);
        } else if (iequals(value, "standard") || iequals(value, "1")) {
          query_options->__set_explain_level(TExplainLevel::STANDARD);
        } else if (iequals(value, "extended") || iequals(value, "2")) {
          query_options->__set_explain_level(TExplainLevel::EXTENDED);
        } else if (iequals(value, "verbose") || iequals(value, "3")) {
          query_options->__set_explain_level(TExplainLevel::VERBOSE);
        } else {
          return Status(Substitute("Invalid explain level '$0'. Valid levels are"
              " MINIMAL(0), STANDARD(1), EXTENDED(2) and VERBOSE(3).", value));
        }
        break;
      case TImpalaQueryOptions::SYNC_DDL:
        query_options->__set_sync_ddl(iequals(value, "true") || iequals(value, "1"));
        break;
      case TImpalaQueryOptions::REQUEST_POOL:
        query_options->__set_request_pool(value);
        break;
      case TImpalaQueryOptions::DISABLE_OUTERMOST_TOPN:
        query_options->__set_disable_outermost_topn(
            iequals(value, "true") || iequals(value, "1"));
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
        query_options->__set_appx_count_distinct(
            iequals(value, "true") || iequals(value, "1"));
        break;
      }
      case TImpalaQueryOptions::DISABLE_UNSAFE_SPILLS: {
        query_options->__set_disable_unsafe_spills(
            iequals(value, "true") || iequals(value, "1"));
        break;
      }
      case TImpalaQueryOptions::EXEC_SINGLE_NODE_ROWS_THRESHOLD:
        query_options->__set_exec_single_node_rows_threshold(atoi(value.c_str()));
        break;
      case TImpalaQueryOptions::OPTIMIZE_PARTITION_KEY_SCANS:
        query_options->__set_optimize_partition_key_scans(
            iequals(value, "true") || iequals(value, "1"));
        break;
      case TImpalaQueryOptions::REPLICA_PREFERENCE:
        if (iequals(value, "cache_local") || iequals(value, "0")) {
          query_options->__set_replica_preference(TReplicaPreference::CACHE_LOCAL);
        } else if (iequals(value, "disk_local") || iequals(value, "2")) {
          query_options->__set_replica_preference(TReplicaPreference::DISK_LOCAL);
        } else if (iequals(value, "remote") || iequals(value, "4")) {
          query_options->__set_replica_preference(TReplicaPreference::REMOTE);
        } else {
          return Status(Substitute("Invalid replica memory distance preference '$0'."
              "Valid values are CACHE_LOCAL(0), DISK_LOCAL(2), REMOTE(4)", value));
        }
        break;
      case TImpalaQueryOptions::SCHEDULE_RANDOM_REPLICA:
        query_options->__set_schedule_random_replica(
            iequals(value, "true") || iequals(value, "1"));
        break;
      case TImpalaQueryOptions::DISABLE_STREAMING_PREAGGREGATIONS:
        query_options->__set_disable_streaming_preaggregations(
            iequals(value, "true") || iequals(value, "1"));
        break;
      case TImpalaQueryOptions::RUNTIME_FILTER_MODE:
        if (iequals(value, "off") || iequals(value, "0")) {
          query_options->__set_runtime_filter_mode(TRuntimeFilterMode::OFF);
        } else if (iequals(value, "local") || iequals(value, "1")) {
          query_options->__set_runtime_filter_mode(TRuntimeFilterMode::LOCAL);
        } else if (iequals(value, "global") || iequals(value, "2")) {
          query_options->__set_runtime_filter_mode(TRuntimeFilterMode::GLOBAL);
        } else {
          return Status(Substitute("Invalid runtime filter mode '$0'. Valid modes are"
              " OFF(0), LOCAL(1) or GLOBAL(2).", value));
        }
        break;
      case TImpalaQueryOptions::RUNTIME_FILTER_MAX_SIZE:
      case TImpalaQueryOptions::RUNTIME_FILTER_MIN_SIZE:
      case TImpalaQueryOptions::RUNTIME_BLOOM_FILTER_SIZE: {
        int64_t size;
        RETURN_IF_ERROR(ParseMemValue(value, "Bloom filter size", &size));
        if (size < RuntimeFilterBank::MIN_BLOOM_FILTER_SIZE ||
            size > RuntimeFilterBank::MAX_BLOOM_FILTER_SIZE) {
          return Status(Substitute("$0 is not a valid Bloom filter size for $1. "
                  "Valid sizes are in [$2, $3].", value, PrintTImpalaQueryOptions(
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
              PrintTImpalaQueryOptions(static_cast<TImpalaQueryOptions::type>(option)),
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
        query_options->__set_disable_row_runtime_filtering(
            iequals(value, "true") || iequals(value, "1"));
        break;
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
        query_options->__set_parquet_annotate_strings_utf8(
            iequals(value, "true") || iequals(value, "1"));
        break;
      }
      case TImpalaQueryOptions::PARQUET_FALLBACK_SCHEMA_RESOLUTION: {
        if (iequals(value, "position") ||
            iequals(value, to_string(TParquetFallbackSchemaResolution::POSITION))) {
          query_options->__set_parquet_fallback_schema_resolution(
              TParquetFallbackSchemaResolution::POSITION);
        } else if (iequals(value, "name") ||
                   iequals(value, to_string(TParquetFallbackSchemaResolution::NAME))) {
          query_options->__set_parquet_fallback_schema_resolution(
              TParquetFallbackSchemaResolution::NAME);
        } else {
          return Status(Substitute("Invalid PARQUET_FALLBACK_SCHEMA_RESOLUTION option: "
              "'$0'. Valid options are 'POSITION' and 'NAME'.", value));
        }
        break;
      }
      case TImpalaQueryOptions::PARQUET_ARRAY_RESOLUTION: {
        if (iequals(value, "three_level") ||
            value == to_string(TParquetArrayResolution::THREE_LEVEL)) {
          query_options->__set_parquet_array_resolution(
              TParquetArrayResolution::THREE_LEVEL);
        } else if (iequals(value, "two_level") ||
            value == to_string(TParquetArrayResolution::TWO_LEVEL)) {
          query_options->__set_parquet_array_resolution(
              TParquetArrayResolution::TWO_LEVEL);
        } else if (iequals(value, "two_level_then_three_level") ||
            value == to_string(TParquetArrayResolution::TWO_LEVEL_THEN_THREE_LEVEL)) {
          query_options->__set_parquet_array_resolution(
              TParquetArrayResolution::TWO_LEVEL_THEN_THREE_LEVEL);
        } else {
          return Status(Substitute("Invalid PARQUET_ARRAY_RESOLUTION option: '$0'. "
              "Valid options are 'THREE_LEVEL', 'TWO_LEVEL' and "
              "'TWO_LEVEL_THEN_THREE_LEVEL'.", value));
        }
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
        query_options->__set_s3_skip_insert_staging(
            iequals(value, "true") || iequals(value, "1"));
        break;
      }
      case TImpalaQueryOptions::PREFETCH_MODE: {
        if (iequals(value, "NONE") || iequals(value, "0")) {
          query_options->__set_prefetch_mode(TPrefetchMode::NONE);
        } else if (iequals(value, "HT_BUCKET") || iequals(value, "1")) {
          query_options->__set_prefetch_mode(TPrefetchMode::HT_BUCKET);
        } else {
          return Status(Substitute("Invalid prefetch mode '$0'. Valid modes are "
              "NONE(0) or HT_BUCKET(1)", value));
        }
        break;
      }
      case TImpalaQueryOptions::STRICT_MODE: {
        query_options->__set_strict_mode(
            iequals(value, "true") || iequals(value, "1"));
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
        query_options->__set_enable_expr_rewrites(
            iequals(value, "true") || iequals(value, "1"));
        break;
      }
      case TImpalaQueryOptions::DECIMAL_V2: {
        query_options->__set_decimal_v2(iequals(value, "true") || iequals(value, "1"));
        break;
      }
      case TImpalaQueryOptions::PARQUET_DICTIONARY_FILTERING: {
        query_options->__set_parquet_dictionary_filtering(
            iequals(value, "true") || iequals(value, "1"));
        break;
      }
      case TImpalaQueryOptions::PARQUET_READ_STATISTICS: {
        query_options->__set_parquet_read_statistics(
            iequals(value, "true") || iequals(value, "1"));
        break;
      }
      case TImpalaQueryOptions::DEFAULT_JOIN_DISTRIBUTION_MODE: {
        if (iequals(value, "BROADCAST") || iequals(value, "0")) {
          query_options->__set_default_join_distribution_mode(
              TJoinDistributionMode::BROADCAST);
        } else if (iequals(value, "SHUFFLE") || iequals(value, "1")) {
          query_options->__set_default_join_distribution_mode(
              TJoinDistributionMode::SHUFFLE);
        } else {
          return Status(Substitute("Invalid default_join_distribution_mode '$0'. "
              "Valid values are BROADCAST or SHUFFLE", value));
        }
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
