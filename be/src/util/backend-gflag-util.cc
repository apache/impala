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

#include "common/version.h"
#include "gen-cpp/BackendGflags_types.h"
#include "rpc/jni-thrift-util.h"
#include "util/backend-gflag-util.h"
#include "util/logging-support.h"


// Configs for the Frontend and the Catalog.
DECLARE_bool(load_catalog_in_background);
DECLARE_bool(load_auth_to_local_rules);
DECLARE_bool(enable_stats_extrapolation);
DECLARE_bool(enable_orc_scanner);
DECLARE_bool(use_local_catalog);
DECLARE_int32(local_catalog_cache_expiration_s);
DECLARE_int32(local_catalog_cache_mb);
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
DECLARE_string(authorization_policy_provider_class);
DECLARE_string(authorized_proxy_user_config);
DECLARE_string(authorized_proxy_user_config_delimiter);
DECLARE_string(authorized_proxy_group_config);
DECLARE_string(authorized_proxy_group_config_delimiter);
DECLARE_string(catalog_topic_mode);
DECLARE_string(kudu_master_hosts);
DECLARE_string(reserved_words_version);
DECLARE_string(sentry_config);
DECLARE_double(max_filter_error_rate);
DECLARE_int64(min_buffer_size);
DECLARE_bool(disable_catalog_data_ops_debug_only);
DECLARE_int32(invalidate_tables_timeout_s);
DECLARE_bool(invalidate_tables_on_memory_pressure);
DECLARE_double(invalidate_tables_gc_old_gen_full_threshold);
DECLARE_double(invalidate_tables_fraction_on_memory_pressure);
DECLARE_int32(local_catalog_max_fetch_retries);
DECLARE_int64(kudu_scanner_thread_estimated_bytes_per_column);
DECLARE_int64(kudu_scanner_thread_max_estimated_bytes);
DECLARE_int32(catalog_max_parallel_partial_fetch_rpc);
DECLARE_int64(catalog_partial_fetch_rpc_queue_timeout_s);
DECLARE_int64(exchg_node_buffer_size_bytes);
DECLARE_int32(kudu_mutation_buffer_size);
DECLARE_int32(kudu_error_buffer_size);
DECLARE_int32(hms_event_polling_interval_s);
DECLARE_string(authorization_factory_class);
DECLARE_bool(unlock_mt_dop);
DECLARE_string(ranger_service_type);
DECLARE_string(ranger_app_id);
DECLARE_string(authorization_provider);
DECLARE_bool(recursively_list_partitions);
DECLARE_string(query_event_hook_classes);
DECLARE_int32(query_event_hook_nthreads);

namespace impala {

Status GetThriftBackendGflags(JNIEnv* jni_env, jbyteArray* cfg_bytes) {
  TBackendGflags cfg;
  cfg.__set_load_catalog_in_background(FLAGS_load_catalog_in_background);
  cfg.__set_enable_orc_scanner(FLAGS_enable_orc_scanner);
  cfg.__set_use_local_catalog(FLAGS_use_local_catalog);
  cfg.__set_local_catalog_cache_mb(FLAGS_local_catalog_cache_mb);
  cfg.__set_local_catalog_cache_expiration_s(
    FLAGS_local_catalog_cache_expiration_s);
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
  cfg.__set_authorized_proxy_group_config(FLAGS_authorized_proxy_group_config);
  cfg.__set_disable_catalog_data_ops_debug_only(
      FLAGS_disable_catalog_data_ops_debug_only);
  cfg.__set_catalog_topic_mode(FLAGS_catalog_topic_mode);
  cfg.__set_invalidate_tables_timeout_s(FLAGS_invalidate_tables_timeout_s);
  cfg.__set_invalidate_tables_on_memory_pressure(
      FLAGS_invalidate_tables_on_memory_pressure);
  cfg.__set_invalidate_tables_gc_old_gen_full_threshold(
      FLAGS_invalidate_tables_gc_old_gen_full_threshold);
  cfg.__set_invalidate_tables_fraction_on_memory_pressure(
      FLAGS_invalidate_tables_fraction_on_memory_pressure);
  cfg.__set_local_catalog_max_fetch_retries(FLAGS_local_catalog_max_fetch_retries);
  cfg.__set_kudu_scanner_thread_estimated_bytes_per_column(
      FLAGS_kudu_scanner_thread_estimated_bytes_per_column);
  cfg.__set_kudu_scanner_thread_max_estimated_bytes(
      FLAGS_kudu_scanner_thread_max_estimated_bytes);
  cfg.__set_catalog_max_parallel_partial_fetch_rpc(
      FLAGS_catalog_max_parallel_partial_fetch_rpc);
  cfg.__set_catalog_partial_fetch_rpc_queue_timeout_s(
      FLAGS_catalog_partial_fetch_rpc_queue_timeout_s);
  cfg.__set_exchg_node_buffer_size_bytes(
      FLAGS_exchg_node_buffer_size_bytes);
  cfg.__set_kudu_mutation_buffer_size(FLAGS_kudu_mutation_buffer_size);
  cfg.__set_kudu_error_buffer_size(FLAGS_kudu_error_buffer_size);
  cfg.__set_hms_event_polling_interval_s(FLAGS_hms_event_polling_interval_s);
  cfg.__set_impala_build_version(::GetDaemonBuildVersion());
  cfg.__set_authorization_factory_class(FLAGS_authorization_factory_class);
  cfg.__set_unlock_mt_dop(FLAGS_unlock_mt_dop);
  cfg.__set_ranger_service_type(FLAGS_ranger_service_type);
  cfg.__set_ranger_app_id(FLAGS_ranger_app_id);
  cfg.__set_authorization_provider(FLAGS_authorization_provider);
  cfg.__set_recursively_list_partitions(FLAGS_recursively_list_partitions);
  cfg.__set_query_event_hook_classes(FLAGS_query_event_hook_classes);
  cfg.__set_query_event_hook_nthreads(FLAGS_query_event_hook_nthreads);
  RETURN_IF_ERROR(SerializeThriftMsg(jni_env, &cfg, cfg_bytes));
  return Status::OK();
}
}
