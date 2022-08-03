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
  IMPALA_2_11 = 0
  IMPALA_3_0 = 1
}

// Used to pass gflags from backend to frontend, JniCatalog and JniFrontend
// Attributes without comments correspond to gflags
struct TBackendGflags {
  // REMOVED: 1: required string sentry_config

  2: required bool load_auth_to_local_rules

  3: required i32 non_impala_java_vlog

  4: required i32 impala_log_lvl

  5: required i64 inc_stats_size_limit_bytes

  6: required string lineage_event_log_dir

  7: required bool load_catalog_in_background

  8: required i32 num_metadata_loading_threads

  9: required string principal

  // REMOVED: 10: required string authorization_policy_file

  11: required string server_name

  // REMOVED: 12: required string authorization_policy_provider_class

  13: required string kudu_master_hosts

  14: required string local_library_path

  15: required i32 read_size

  16: required i32 kudu_operation_timeout_ms

  17: required i32 initial_hms_cnxn_timeout_s

  18: required bool enable_stats_extrapolation

  // REMOVED: 19: required i64 sentry_catalog_polling_frequency_s

  20: required i32 max_hdfs_partitions_parallel_load

  21: required i32 max_nonhdfs_partitions_parallel_load

  22: required TReservedWordsVersion reserved_words_version

  23: required double max_filter_error_rate

  24: required i64 min_buffer_size

  // REMOVED: 25: required bool enable_orc_scanner

  26: required string authorized_proxy_group_config

  27: required bool use_local_catalog

  28: required bool disable_catalog_data_ops_debug_only

  29: required i32 local_catalog_cache_mb

  30: required i32 local_catalog_cache_expiration_s

  32: required string catalog_topic_mode

  33: required i32 invalidate_tables_timeout_s

  34: required bool invalidate_tables_on_memory_pressure

  35: required double invalidate_tables_gc_old_gen_full_threshold

  36: required double invalidate_tables_fraction_on_memory_pressure

  37: required i32 local_catalog_max_fetch_retries

  38: required i64 kudu_scanner_thread_estimated_bytes_per_column

  39: required i64 kudu_scanner_thread_max_estimated_bytes

  40: required i32 catalog_max_parallel_partial_fetch_rpc

  41: required i64 catalog_partial_fetch_rpc_queue_timeout_s

  42: required i64 exchg_node_buffer_size_bytes

  43: required i32 kudu_mutation_buffer_size

  44: required i32 kudu_error_buffer_size

  45: required i32 hms_event_polling_interval_s

  46: required string impala_build_version

  47: required string authorization_factory_class

  // REMOVED: 48: required bool unlock_mt_dop

  49: required string ranger_service_type

  50: required string ranger_app_id

  51: required string authorization_provider

  52: required bool recursively_list_partitions

  53: required string query_event_hook_classes

  54: required i32 query_event_hook_nthreads

  55: required bool is_executor

  56: required bool is_coordinator

  57: required bool use_dedicated_coordinator_estimates

  58: required string blacklisted_dbs

  59: required string blacklisted_tables

  60: required bool unlock_zorder_sort

  61: required string min_privilege_set_for_show_stmts

  // REMOVED: 62: required bool mt_dop_auto_fallback

  63: required i32 num_expected_executors

  64: required i32 num_check_authorization_threads

  65: required bool use_customized_user_groups_mapper_for_ranger

  66: required bool enable_column_masking

  67: required bool enable_insert_events

  68: required bool compact_catalog_topic

  69: required bool enable_incremental_metadata_updates

  70: required i64 topic_update_tbl_max_wait_time_ms

  71: required i32 catalog_max_lock_skipped_topic_updates

  72: required string saml2_keystore_path

  73: required string saml2_keystore_password

  74: required string saml2_private_key_password

  75: required string saml2_idp_metadata

  76: required string saml2_sp_entity_id

  77: required string saml2_sp_callback_url

  78: required bool saml2_want_assertations_signed

  79: required bool saml2_sign_requests

  80: required i32 saml2_callback_token_ttl

  81: required string saml2_group_attribute_name

  82: required string saml2_group_filter

  83: required bool saml2_ee_test_mode

  84: required string scratch_dirs

  85: required bool enable_row_filtering

  86: required i32 max_wait_time_for_sync_ddl_s

  87: required bool allow_ordinals_in_having

  88: required bool start_hms_server

  89: required i32 hms_port

  90: required bool fallback_to_hms_on_errors

  91: required bool enable_catalogd_hms_cache

  92: required string kudu_sasl_protocol_name

  93: required i32 warn_catalog_response_size_mb

  94: required i32 warn_catalog_response_duration_s

  95: required bool invalidate_hms_cache_on_ddls

  96: required string startup_filesystem_check_directories

  97: required bool hms_event_incremental_refresh_transactional_table

  98: required bool enable_shell_based_groups_mapping_support

  99: required bool auto_check_compaction

  100: required bool enable_sync_to_latest_event_on_ddls

  101: required bool pull_table_types_and_comments

  102: required bool use_hms_column_order_for_hbase_tables

  103: required string ignored_dir_prefix_list
}
