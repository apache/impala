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
#include "gutil/strings/substitute.h"
#include "kudu/util/flag_tags.h"
#include "rpc/jni-thrift-util.h"
#include "util/backend-gflag-util.h"
#include "util/logging-support.h"
#include "util/os-util.h"


// Configs for the Frontend and the Catalog.
DECLARE_bool(load_catalog_in_background);
DECLARE_bool(load_auth_to_local_rules);
DECLARE_bool(enable_stats_extrapolation);
DECLARE_bool(use_local_catalog);
DECLARE_int32(local_catalog_cache_expiration_s);
DECLARE_int32(local_catalog_cache_mb);
DECLARE_int32(non_impala_java_vlog);
DECLARE_int32(num_metadata_loading_threads);
DECLARE_int32(max_hdfs_partitions_parallel_load);
DECLARE_int32(max_nonhdfs_partitions_parallel_load);
DECLARE_int32(initial_hms_cnxn_timeout_s);
DECLARE_int32(kudu_operation_timeout_ms);
DECLARE_int64(inc_stats_size_limit_bytes);
DECLARE_string(principal);
DECLARE_string(lineage_event_log_dir);
DECLARE_string(principal);
DECLARE_string(local_library_dir);
DECLARE_string(server_name);
DECLARE_string(authorized_proxy_group_config);
DECLARE_bool(enable_shell_based_groups_mapping_support);
DECLARE_string(catalog_topic_mode);
DECLARE_string(kudu_master_hosts);
DECLARE_string(reserved_words_version);
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
DECLARE_bool(enable_insert_events);
DECLARE_string(authorization_factory_class);
DECLARE_string(ranger_service_type);
DECLARE_string(ranger_app_id);
DECLARE_string(authorization_provider);
DECLARE_bool(recursively_list_partitions);
DECLARE_string(query_event_hook_classes);
DECLARE_int32(query_event_hook_nthreads);
DECLARE_bool(is_executor);
DECLARE_bool(is_coordinator);
DECLARE_bool(use_dedicated_coordinator_estimates);
DECLARE_string(blacklisted_dbs);
DECLARE_bool(unlock_zorder_sort);
DECLARE_string(blacklisted_tables);
DECLARE_string(min_privilege_set_for_show_stmts);
DECLARE_int32(num_expected_executors);
DECLARE_int32(num_check_authorization_threads);
DECLARE_bool(use_customized_user_groups_mapper_for_ranger);
DECLARE_bool(compact_catalog_topic);
DECLARE_bool(enable_incremental_metadata_updates);
DECLARE_int64(topic_update_tbl_max_wait_time_ms);
DECLARE_int32(catalog_max_lock_skipped_topic_updates);
DECLARE_string(scratch_dirs);
DECLARE_int32(max_wait_time_for_sync_ddl_s);
DECLARE_bool(start_hms_server);
DECLARE_int32(hms_port);
DECLARE_bool(fallback_to_hms_on_errors);
DECLARE_bool(enable_catalogd_hms_cache);
DECLARE_string(kudu_sasl_protocol_name);
DECLARE_bool(invalidate_hms_cache_on_ddls);
DECLARE_bool(hms_event_incremental_refresh_transactional_table);
DECLARE_bool(auto_check_compaction);
DECLARE_bool(enable_sync_to_latest_event_on_ddls);
DECLARE_bool(pull_table_types_and_comments);

// HS2 SAML2.0 configuration
// Defined here because TAG_FLAG caused issues in global-flags.cc
DEFINE_string(saml2_keystore_path, "",
    "Keystore path to the saml2 client. This keystore is used to store the "
    "key pair used to sign the authentication requests when saml2_sign_requests "
    "is set to true. If the path doesn't exist, HiveServer2 will attempt to "
    "create a keystore using the default configurations otherwise it will use "
    "the one provided. Setting this is required for SAML authentication.");

DEFINE_string(saml2_keystore_password_cmd, "",
    "Command that outputs the password to the keystore used to sign the authentication "
    "requests. Setting this is required for SAML authentication.");
TAG_FLAG(saml2_keystore_password_cmd, sensitive);

DEFINE_string(saml2_private_key_password_cmd, "",
    "Command that outputs the password for the private key which is stored in the "
    "keystore pointed by saml2_keystore_path. This key is used to sign the "
    "authentication request if saml2_sign_requests is set to true.");
TAG_FLAG(saml2_private_key_password_cmd, sensitive);

DEFINE_string(saml2_idp_metadata, "",
    "IDP metadata file for the SAML configuration. This metadata file must be "
    "exported from the external identity provider. This is used to validate the SAML "
    "assertions received. Setting this is required for SAML authentication");

DEFINE_string(saml2_sp_entity_id, "",
    "Service provider entity id for this impalad. This must match with the "
    "SP id on the external identity provider. If this is not set, saml2_sp_callback_url "
    "will be used as the SP id.");

DEFINE_string(saml2_sp_callback_url, "",
    "Callback URL where SAML responses should be posted. Currently this must be "
    "configured at the same port number as the --hs2_http_port flag.");

DEFINE_bool(saml2_want_assertations_signed, true,
    "When this configuration is set to true, Impala will validate the signature "
    "of the assertions received at the callback url. 'False' should be only used "
    "for testing as it makes the protocol unsecure.");

DEFINE_bool(saml2_sign_requests, false,
    "When this configuration is set to true, Impala will sign the SAML requests "
    "which can be validated by the IDP provider.");

DEFINE_int32(saml2_callback_token_ttl, 30000,
    "Time (in milliseconds) for which the token issued by service provider is valid.");

DEFINE_string(saml2_group_attribute_name, "",
    "The attribute name in the SAML assertion which would "
    "be used to compare for the group name matching. By default it is empty "
    "which would allow any authenticated user. If this value is set then "
    "saml2_group_filter must be set to a non-empty value.");

DEFINE_string(saml2_group_filter, "",
    "Comma separated list of group names which will be allowed when SAML "
    "authentication is enabled.");

DEFINE_bool_hidden(saml2_ee_test_mode, false,
    "If true, no signature is checked and bearer token validation returns "
    "401 Unauthorized to allow checking cookies dealing with Thrift protocol. "
    "Should be only used in test environments." );

DEFINE_bool(enable_column_masking, true,
    "If false, disable the column masking feature. Defaults to be true.");

DEFINE_bool(enable_row_filtering, true,
    "If false, disable the row filtering feature. Defaults to be true. Enabling this flag"
    " requires enable_column_masking to be true.");

DEFINE_bool(allow_ordinals_in_having, false,
    "If true, allow using ordinals in HAVING clause. This non-standard feature is "
    "supported in Impala 3.x and earlier. We intend to disable it since 4.0. So it "
    "defaults to be false. See IMPALA-7844.");

DEFINE_int32(warn_catalog_response_size_mb, 50,
    "Threshold in MB to log a warning for large catalogd response size.");

DEFINE_int32(warn_catalog_response_duration_s, 60,
    "Threshold in seconds to log a warning for slow catalogd response.");

DEFINE_string(startup_filesystem_check_directories, "/",
    "Comma separated list of directories to list on startup to verify access to the "
    "filesystem. The default is to list the root of the filesytem. This can be "
    "specified to a subdirectory to avoid accesses to the root of the filesystem. "
    "To disable the startup check, specify the empty string.");

DEFINE_bool(use_hms_column_order_for_hbase_tables, false,
    "Use the column order in HMS for HBase tables instead of ordering the columns by "
    "family/qualifier. Keeping the default as false for backward compatibility.");

DEFINE_string(ignored_dir_prefix_list, ".,_tmp.,_spark_metadata",
    "Comma separated list to specify the prefix for tmp/staging dirs that catalogd should"
    " skip in loading file metadata.");

namespace impala {

Status GetConfigFromCommand(const string& flag_cmd, string& result) {
  result.clear();
  if (flag_cmd.empty()) return Status::OK();
  if (!RunShellProcess(flag_cmd, &result, true, {"JAVA_TOOL_OPTIONS"})) {
    return Status(strings::Substitute("$0 failed with output: '$1'", flag_cmd, result));
  }
  return Status::OK();
}

Status PopulateThriftBackendGflags(TBackendGflags& cfg) {
  cfg.__set_load_catalog_in_background(FLAGS_load_catalog_in_background);
  cfg.__set_use_local_catalog(FLAGS_use_local_catalog);
  cfg.__set_local_catalog_cache_mb(FLAGS_local_catalog_cache_mb);
  cfg.__set_local_catalog_cache_expiration_s(
    FLAGS_local_catalog_cache_expiration_s);
  cfg.__set_server_name(FLAGS_server_name);
  cfg.__set_kudu_master_hosts(FLAGS_kudu_master_hosts);
  cfg.__set_read_size(FLAGS_read_size);
  cfg.__set_num_metadata_loading_threads(FLAGS_num_metadata_loading_threads);
  cfg.__set_max_hdfs_partitions_parallel_load(FLAGS_max_hdfs_partitions_parallel_load);
  cfg.__set_max_nonhdfs_partitions_parallel_load(
      FLAGS_max_nonhdfs_partitions_parallel_load);
  cfg.__set_initial_hms_cnxn_timeout_s(FLAGS_initial_hms_cnxn_timeout_s);
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
  if (FLAGS_reserved_words_version == "2.11.0") {
    cfg.__set_reserved_words_version(TReservedWordsVersion::IMPALA_2_11);
  } else {
    DCHECK_EQ(FLAGS_reserved_words_version, "3.0.0");
    cfg.__set_reserved_words_version(TReservedWordsVersion::IMPALA_3_0);
  }
  cfg.__set_max_filter_error_rate(FLAGS_max_filter_error_rate);
  cfg.__set_min_buffer_size(FLAGS_min_buffer_size);
  cfg.__set_authorized_proxy_group_config(FLAGS_authorized_proxy_group_config);
  cfg.__set_enable_shell_based_groups_mapping_support(
      FLAGS_enable_shell_based_groups_mapping_support);
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
  cfg.__set_enable_insert_events(FLAGS_enable_insert_events);
  cfg.__set_impala_build_version(::GetDaemonBuildVersion());
  cfg.__set_authorization_factory_class(FLAGS_authorization_factory_class);
  cfg.__set_ranger_service_type(FLAGS_ranger_service_type);
  cfg.__set_ranger_app_id(FLAGS_ranger_app_id);
  cfg.__set_authorization_provider(FLAGS_authorization_provider);
  cfg.__set_recursively_list_partitions(FLAGS_recursively_list_partitions);
  cfg.__set_query_event_hook_classes(FLAGS_query_event_hook_classes);
  cfg.__set_query_event_hook_nthreads(FLAGS_query_event_hook_nthreads);
  cfg.__set_is_executor(FLAGS_is_executor);
  cfg.__set_is_coordinator(FLAGS_is_coordinator);
  cfg.__set_use_dedicated_coordinator_estimates(
      FLAGS_use_dedicated_coordinator_estimates);
  cfg.__set_blacklisted_dbs(FLAGS_blacklisted_dbs);
  cfg.__set_unlock_zorder_sort(FLAGS_unlock_zorder_sort);
  cfg.__set_blacklisted_tables(FLAGS_blacklisted_tables);
  cfg.__set_min_privilege_set_for_show_stmts(FLAGS_min_privilege_set_for_show_stmts);
  cfg.__set_num_expected_executors(FLAGS_num_expected_executors);
  cfg.__set_num_check_authorization_threads(FLAGS_num_check_authorization_threads);
  cfg.__set_use_customized_user_groups_mapper_for_ranger(
      FLAGS_use_customized_user_groups_mapper_for_ranger);
  cfg.__set_enable_column_masking(FLAGS_enable_column_masking);
  cfg.__set_enable_row_filtering(FLAGS_enable_row_filtering);
  cfg.__set_compact_catalog_topic(FLAGS_compact_catalog_topic);
  cfg.__set_enable_incremental_metadata_updates(
      FLAGS_enable_incremental_metadata_updates);
  cfg.__set_topic_update_tbl_max_wait_time_ms(FLAGS_topic_update_tbl_max_wait_time_ms);
  cfg.__set_catalog_max_lock_skipped_topic_updates(
      FLAGS_catalog_max_lock_skipped_topic_updates);
  cfg.__set_saml2_keystore_path(FLAGS_saml2_keystore_path);
  string saml2_keystore_password;
  RETURN_IF_ERROR(GetConfigFromCommand(
      FLAGS_saml2_keystore_password_cmd, saml2_keystore_password));
  cfg.__set_saml2_keystore_password(saml2_keystore_password);
  string saml2_private_key_password;
  RETURN_IF_ERROR(GetConfigFromCommand(
      FLAGS_saml2_private_key_password_cmd,saml2_private_key_password));
  cfg.__set_saml2_private_key_password(saml2_private_key_password);
  cfg.__set_saml2_idp_metadata(FLAGS_saml2_idp_metadata);
  cfg.__set_saml2_sp_entity_id(FLAGS_saml2_sp_entity_id);
  cfg.__set_saml2_sp_callback_url(FLAGS_saml2_sp_callback_url);
  cfg.__set_saml2_want_assertations_signed(FLAGS_saml2_want_assertations_signed);
  cfg.__set_saml2_sign_requests(FLAGS_saml2_sign_requests);
  cfg.__set_saml2_callback_token_ttl(FLAGS_saml2_callback_token_ttl);
  cfg.__set_saml2_group_attribute_name(FLAGS_saml2_group_attribute_name);
  cfg.__set_saml2_group_filter(FLAGS_saml2_group_filter);
  cfg.__set_saml2_ee_test_mode(FLAGS_saml2_ee_test_mode);
  cfg.__set_scratch_dirs(FLAGS_scratch_dirs);
  cfg.__set_max_wait_time_for_sync_ddl_s(FLAGS_max_wait_time_for_sync_ddl_s);
  cfg.__set_allow_ordinals_in_having(FLAGS_allow_ordinals_in_having);
  cfg.__set_start_hms_server(FLAGS_start_hms_server);
  cfg.__set_hms_port(FLAGS_hms_port);
  cfg.__set_fallback_to_hms_on_errors(FLAGS_fallback_to_hms_on_errors);
  cfg.__set_enable_catalogd_hms_cache(FLAGS_enable_catalogd_hms_cache);
  cfg.__set_kudu_sasl_protocol_name(FLAGS_kudu_sasl_protocol_name);
  cfg.__set_warn_catalog_response_size_mb(FLAGS_warn_catalog_response_size_mb);
  cfg.__set_warn_catalog_response_duration_s(FLAGS_warn_catalog_response_duration_s);
  cfg.__set_invalidate_hms_cache_on_ddls(FLAGS_invalidate_hms_cache_on_ddls);
  cfg.__set_startup_filesystem_check_directories(
      FLAGS_startup_filesystem_check_directories);
  cfg.__set_hms_event_incremental_refresh_transactional_table(
      FLAGS_hms_event_incremental_refresh_transactional_table);
  cfg.__set_auto_check_compaction(FLAGS_auto_check_compaction);
  cfg.__set_enable_sync_to_latest_event_on_ddls(
      FLAGS_enable_sync_to_latest_event_on_ddls);
  cfg.__set_pull_table_types_and_comments(FLAGS_pull_table_types_and_comments);
  cfg.__set_use_hms_column_order_for_hbase_tables(
      FLAGS_use_hms_column_order_for_hbase_tables);
  cfg.__set_ignored_dir_prefix_list(FLAGS_ignored_dir_prefix_list);
  return Status::OK();
}

Status GetThriftBackendGFlagsForJNI(JNIEnv* jni_env, jbyteArray* cfg_bytes) {
  TBackendGflags cfg;
  RETURN_IF_ERROR(PopulateThriftBackendGflags(cfg));
  RETURN_IF_ERROR(SerializeThriftMsg(jni_env, &cfg, cfg_bytes));
  return Status::OK();
}

}
