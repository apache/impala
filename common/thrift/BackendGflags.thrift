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

enum TReservedWordsVersion {
  IMPALA_2_11
  IMPALA_3_0
}

// Used to pass gflags from backend to frontend, JniCatalog and JniFrontend
// Attributes without comments correspond to gflags
struct TBackendGflags {
  1: required string sentry_config

  2: required bool load_auth_to_local_rules

  3: required i32 non_impala_java_vlog

  4: required i32 impala_log_lvl

  5: required i64 inc_stats_size_limit_bytes

  6: required string lineage_event_log_dir

  7: required bool load_catalog_in_background

  8: required i32 num_metadata_loading_threads

  9: required string principal

  10: required string authorization_policy_file

  11: required string server_name

  12: required string authorization_policy_provider_class

  13: required string kudu_master_hosts

  14: required string local_library_path

  15: required i32 read_size

  16: required i32 kudu_operation_timeout_ms

  17: required i32 initial_hms_cnxn_timeout_s

  18: required bool enable_stats_extrapolation

  19: required i64 sentry_catalog_polling_frequency_s

  20: required i32 max_hdfs_partitions_parallel_load

  21: required i32 max_nonhdfs_partitions_parallel_load

  22: required TReservedWordsVersion reserved_words_version

  23: required double max_filter_error_rate

  24: required i64 min_buffer_size
}
