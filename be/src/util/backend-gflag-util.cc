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

#include "common/global-flags.h"

#include "gen-cpp/BackendGflags_types.h"
#include "rpc/jni-thrift-util.h"
#include "util/backend-gflag-util.h"
#include "util/logging-support.h"


// Configs for the Frontend and the Catalog.
DECLARE_bool(load_catalog_in_background);
DECLARE_bool(load_auth_to_local_rules);
DECLARE_bool(enable_stats_extrapolation);
DECLARE_int32(non_impala_java_vlog);
DECLARE_int32(num_metadata_loading_threads);
DECLARE_int32(max_hdfs_partitions_parallel_load);
DECLARE_int32(max_nonhdfs_partitions_parallel_load);
DECLARE_int32(initial_hms_cnxn_timeout_s);
DECLARE_int32(kudu_operation_timeout_ms);
DECLARE_int64(sentry_catalog_polling_frequency_s);
DECLARE_int64(inc_stats_size_limit_bytes);
DECLARE_string(principal);
DECLARE_string(lineage_event_log_dir);
DECLARE_string(principal);
DECLARE_string(local_library_dir);
DECLARE_string(server_name);
DECLARE_string(authorization_policy_file);
DECLARE_string(authorization_policy_provider_class);
DECLARE_string(authorized_proxy_user_config);
DECLARE_string(authorized_proxy_user_config_delimiter);
DECLARE_string(kudu_master_hosts);
DECLARE_string(reserved_words_version);
DECLARE_string(sentry_config);
DECLARE_double(max_filter_error_rate);
DECLARE_int64(min_buffer_size);

namespace impala {

Status GetThriftBackendGflags(JNIEnv* jni_env, jbyteArray* cfg_bytes) {
  TBackendGflags cfg;
  cfg.__set_authorization_policy_file(FLAGS_authorization_policy_file);
  cfg.__set_load_catalog_in_background(FLAGS_load_catalog_in_background);
  cfg.__set_server_name(FLAGS_server_name);
  cfg.__set_sentry_config(FLAGS_sentry_config);
  cfg.__set_authorization_policy_provider_class(
      FLAGS_authorization_policy_provider_class);
  cfg.__set_kudu_master_hosts(FLAGS_kudu_master_hosts);
  cfg.__set_read_size(FLAGS_read_size);
  cfg.__set_num_metadata_loading_threads(FLAGS_num_metadata_loading_threads);
  cfg.__set_max_hdfs_partitions_parallel_load(FLAGS_max_hdfs_partitions_parallel_load);
  cfg.__set_max_nonhdfs_partitions_parallel_load(
      FLAGS_max_nonhdfs_partitions_parallel_load);
  cfg.__set_initial_hms_cnxn_timeout_s(FLAGS_initial_hms_cnxn_timeout_s);
  cfg.__set_sentry_config(FLAGS_sentry_config);
  // auth_to_local rules are read if --load_auth_to_local_rules is set to true
  // and impala is kerberized.
  cfg.__set_load_auth_to_local_rules(FLAGS_load_auth_to_local_rules);
  cfg.__set_principal(FLAGS_principal);
  cfg.__set_impala_log_lvl(FlagToTLogLevel(FLAGS_v));
  cfg.__set_non_impala_java_vlog(FlagToTLogLevel(FLAGS_non_impala_java_vlog));
  cfg.__set_inc_stats_size_limit_bytes(FLAGS_inc_stats_size_limit_bytes);
  cfg.__set_enable_stats_extrapolation(FLAGS_enable_stats_extrapolation);
  cfg.__set_lineage_event_log_dir(FLAGS_lineage_event_log_dir);
  cfg.__set_local_library_path(FLAGS_local_library_dir);
  cfg.__set_kudu_operation_timeout_ms(FLAGS_kudu_operation_timeout_ms);
  cfg.__set_sentry_catalog_polling_frequency_s(FLAGS_sentry_catalog_polling_frequency_s);
  if (FLAGS_reserved_words_version == "2.11.0") {
    cfg.__set_reserved_words_version(TReservedWordsVersion::IMPALA_2_11);
  } else {
    DCHECK_EQ(FLAGS_reserved_words_version, "3.0.0");
    cfg.__set_reserved_words_version(TReservedWordsVersion::IMPALA_3_0);
  }
  cfg.__set_max_filter_error_rate(FLAGS_max_filter_error_rate);
  cfg.__set_min_buffer_size(FLAGS_min_buffer_size);
  RETURN_IF_ERROR(SerializeThriftMsg(jni_env, &cfg, cfg_bytes));
  return Status::OK();
}
}
