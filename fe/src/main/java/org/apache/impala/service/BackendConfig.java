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

package org.apache.impala.service;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic
    .HADOOP_SECURITY_AUTH_TO_LOCAL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.impala.analysis.SqlScanner;
import org.apache.impala.thrift.TBackendGflags;
import org.apache.impala.thrift.TGeospatialLibrary;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * This class is meant to provide the FE with impalad backend configuration parameters,
 * including command line arguments.
 */
public class BackendConfig {
  public static BackendConfig INSTANCE;

  private TBackendGflags backendCfg_;

  // Thresholds in warning slow or large catalog responses.
  private long warnCatalogResponseSize_;
  private long warnCatalogResponseDurationMs_;

  private BackendConfig(TBackendGflags cfg) {
    backendCfg_ = cfg;
    warnCatalogResponseSize_ = cfg.warn_catalog_response_size_mb * 1024L * 1024L;
    warnCatalogResponseDurationMs_ = cfg.warn_catalog_response_duration_s * 1000L;
  }

  public static void create(TBackendGflags cfg) {
    BackendConfig.create(cfg, true);
  }

  public static void create(TBackendGflags cfg, boolean initialize) {
    Preconditions.checkNotNull(cfg);
    INSTANCE = new BackendConfig(cfg);
    if (initialize) {
      SqlScanner.init(cfg.getReserved_words_version());
      initAuthToLocal();
    }
  }

  public TBackendGflags getBackendCfg() { return backendCfg_; }
  public long getReadSize() { return backendCfg_.read_size; }
  public boolean getComputeLineage() {
    // Lineage is computed in the fe if --lineage_event_log_dir is configured or
    // a query event hook is configured with --query_event_hook_classes. The hook
    // may or may not consume the lineage but we still include it.
    return !Strings.isNullOrEmpty(backendCfg_.lineage_event_log_dir) ||
        !Strings.isNullOrEmpty(getQueryExecHookClasses());
  }
  public long getIncStatsMaxSize() { return backendCfg_.inc_stats_size_limit_bytes; }
  public boolean isStatsExtrapolationEnabled() {
    return backendCfg_.enable_stats_extrapolation;
  }
  public boolean isAuthToLocalEnabled() {
    return backendCfg_.load_auth_to_local_rules &&
        !Strings.isNullOrEmpty(backendCfg_.principal);
  }
  public int getKuduClientTimeoutMs() { return backendCfg_.kudu_operation_timeout_ms; }

  public String getKuduSaslProtocolName() { return backendCfg_.kudu_sasl_protocol_name; }

  public String getImpalaBuildVersion() { return backendCfg_.impala_build_version; }
  public int getImpalaLogLevel() { return backendCfg_.impala_log_lvl; }
  public int getNonImpalaJavaVlogLevel() { return backendCfg_.non_impala_java_vlog; }

  public int maxHdfsPartsParallelLoad() {
    return backendCfg_.max_hdfs_partitions_parallel_load;
  }

  public int maxNonHdfsPartsParallelLoad() {
    return backendCfg_.max_nonhdfs_partitions_parallel_load;
  }

  public double getMaxFilterErrorRate() { return backendCfg_.max_filter_error_rate; }

  public long getMinBufferSize() { return backendCfg_.min_buffer_size; }

  public boolean isAuthorizedProxyGroupEnabled() {
    return !Strings.isNullOrEmpty(backendCfg_.authorized_proxy_group_config);
  }

  public boolean isShellBasedGroupsMappingEnabled() {
    return backendCfg_.enable_shell_based_groups_mapping_support;
  }

  public boolean disableCatalogDataOpsDebugOnly() {
    return backendCfg_.disable_catalog_data_ops_debug_only;
  }

  public int getInvalidateTablesTimeoutS() {
    return backendCfg_.invalidate_tables_timeout_s;
  }

  public boolean invalidateTablesOnMemoryPressure() {
    return backendCfg_.invalidate_tables_on_memory_pressure;
  }

  public double getInvalidateTablesGcOldGenFullThreshold() {
    return backendCfg_.invalidate_tables_gc_old_gen_full_threshold;
  }

  public double getInvalidateTablesFractionOnMemoryPressure() {
    return backendCfg_.invalidate_tables_fraction_on_memory_pressure;
  }

  public int getLocalCatalogMaxFetchRetries() {
    return backendCfg_.local_catalog_max_fetch_retries;
  }

  public int getCatalogMaxParallelPartialFetchRpc() {
    return backendCfg_.catalog_max_parallel_partial_fetch_rpc;
  }

  public long getCatalogPartialFetchRpcQueueTimeoutS() {
    return backendCfg_.catalog_partial_fetch_rpc_queue_timeout_s;
  }

  public long getHMSPollingIntervalInSeconds() {
    return backendCfg_.hms_event_polling_interval_s;
  }

  public boolean isInsertEventsEnabled() { return backendCfg_.enable_insert_events; }

  /**
   * Returns the value of the `authorization_factory_class` flag or `null` if
   * the flag was not specified.
   *
   * @return value of the `authorization_factory_class` flag or `null` if not specified
   */
  public String getAuthorizationFactoryClass() {
    final String val =  backendCfg_.getAuthorization_factory_class();
    return "".equals(val) ? null : val;
  }

  public boolean recursivelyListPartitions() {
    return backendCfg_.recursively_list_partitions;
  }

  public String getRangerServiceType() {
    return backendCfg_.getRanger_service_type();
  }

  public String getRangerAppId() {
    return backendCfg_.getRanger_app_id();
  }

  public String getAuthorizationProvider() {
    return backendCfg_.getAuthorization_provider();
  }

  public String getQueryExecHookClasses() {
    return backendCfg_.getQuery_event_hook_classes();
  }

  public int getNumQueryExecHookThreads() {
    return backendCfg_.getQuery_event_hook_nthreads();
  }

  public boolean useDedicatedCoordinatorEstimates() {
    return !backendCfg_.is_executor && backendCfg_.use_dedicated_coordinator_estimates;
  }

  public String getBlacklistedDbs() {
    return backendCfg_.blacklisted_dbs;
  }

  public String getBlacklistedTables() {
    return backendCfg_.blacklisted_tables;
  }

  public boolean isZOrderSortUnlocked() {
    return backendCfg_.unlock_zorder_sort;
  }

  public void setZOrderSortUnlocked(boolean zOrdering) {
    backendCfg_.setUnlock_zorder_sort(zOrdering);
  }

  public String getMinPrivilegeSetForShowStmts() {
    return backendCfg_.min_privilege_set_for_show_stmts;
  }

  public int getNumCheckAuthorizationThreads() {
    return backendCfg_.num_check_authorization_threads;
  }

  public boolean useCustomizedUserGroupsMapperForRanger() {
    return backendCfg_.use_customized_user_groups_mapper_for_ranger;
  }

  public void setColumnMaskingEnabled(boolean columnMaskingEnabled) {
    backendCfg_.setEnable_column_masking(columnMaskingEnabled);
  }

  public void setRowFilteringEnabled(boolean rowFilteringEnabled) {
    backendCfg_.setEnable_row_filtering(rowFilteringEnabled);
  }

  public boolean isColumnMaskingEnabled() { return backendCfg_.enable_column_masking; }
  public boolean isRowFilteringEnabled() { return backendCfg_.enable_row_filtering; }

  public boolean isCompactCatalogTopic() { return backendCfg_.compact_catalog_topic; }

  public boolean isIncrementalMetadataUpdatesEnabled() {
    return backendCfg_.enable_incremental_metadata_updates;
  }

  public String getSaml2KeystorePath() { return backendCfg_.saml2_keystore_path; }

  public String getSaml2KeystorePassword() {
    return backendCfg_.saml2_keystore_password;
  }

  public String getSaml2PrivateKeyPassword() {
    return backendCfg_.saml2_private_key_password;
  }

  public String getSaml2IdpMetadata() { return backendCfg_.saml2_idp_metadata; }

  public String getSaml2SpEntityId() { return backendCfg_.saml2_sp_entity_id; }

  public String getSaml2SpCallbackUrl() { return backendCfg_.saml2_sp_callback_url; }

  public boolean getSaml2WantAsserationsSigned() {
    return backendCfg_.saml2_want_assertations_signed;
  }

  public boolean getSaml2SignRequest() { return backendCfg_.saml2_sign_requests; }

  public int getSaml2CallbackTokenTtl() {
    return backendCfg_.saml2_callback_token_ttl;
  }

  public String getSaml2GroupAttibuteName() { return backendCfg_.saml2_group_attribute_name; }

  public String getSaml2GroupFilter() { return backendCfg_.saml2_group_filter; }

  public boolean getSaml2EETestMode() { return backendCfg_.saml2_ee_test_mode; }

  public String getScratchDirs() { return backendCfg_.scratch_dirs; }

  public boolean getAllowOrdinalsInHaving() {
    return backendCfg_.allow_ordinals_in_having;
  }

  public void setAllowOrdinalsInHaving(boolean allow_ordinals_in_having) {
    backendCfg_.allow_ordinals_in_having = allow_ordinals_in_having;
  }

  public long getWarnCatalogResponseSize() { return warnCatalogResponseSize_; }

  public long getWarnCatalogResponseDurationMs() {
    return warnCatalogResponseDurationMs_;
  }

  // Inits the auth_to_local configuration in the static KerberosName class.
  private static void initAuthToLocal() {
    // If auth_to_local is enabled, we read the configuration hadoop.security.auth_to_local
    // from core-site.xml and use it for principal to short name conversion. If it is not,
    // we use the defaultRule ("RULE:[1:$1] RULE:[2:$1]"), which just extracts the user
    // name from any principal of form a@REALM or a/b@REALM. If auth_to_local is enabled
    // and hadoop.security.auth_to_local is not specified in the hadoop configs, we use
    // the "DEFAULT" rule that just extracts the username from any principal in the
    // cluster's local realm. For more details on principal to short name translation,
    // refer to org.apache.hadoop.security.KerberosName.
    final String defaultRule = "RULE:[1:$1] RULE:[2:$1]";
    final Configuration conf = new Configuration();
    if (INSTANCE.isAuthToLocalEnabled()) {
      KerberosName.setRules(conf.get(HADOOP_SECURITY_AUTH_TO_LOCAL, "DEFAULT"));
    } else {
      // just extract the simple user name
      KerberosName.setRules(defaultRule);
    }
  }

  public boolean isDedicatedCoordinator() {
    return (backendCfg_.is_executor == false) && (backendCfg_.is_coordinator == true);
  }

  public int getMaxWaitTimeForSyncDdlSecs() {
    return backendCfg_.max_wait_time_for_sync_ddl_s;
  }

  public boolean startHmsServer() {
    return backendCfg_.start_hms_server;
  }

  public int getHMSPort() {
    return backendCfg_.hms_port;
  }

  public boolean fallbackToHMSOnErrors() {
    return backendCfg_.fallback_to_hms_on_errors;
  }

  @VisibleForTesting
  public void setEnableCatalogdHMSCache(boolean flag) {
    backendCfg_.enable_catalogd_hms_cache = flag;
  }

  public boolean enableCatalogdHMSCache() {
    return backendCfg_.enable_catalogd_hms_cache;
  }

  public boolean invalidateCatalogdHMSCacheOnDDLs() {
    return backendCfg_.invalidate_hms_cache_on_ddls;
  }

  public String getStartupFilesystemCheckDirectories() {
    return backendCfg_.startup_filesystem_check_directories;
  }

  public boolean getHMSEventIncrementalRefreshTransactionalTable() {
    return backendCfg_.hms_event_incremental_refresh_transactional_table;
  }

  public boolean isAutoCheckCompaction() {
    return backendCfg_.auto_check_compaction;
  }

  @VisibleForTesting
  public void setInvalidateCatalogdHMSCacheOnDDLs(boolean flag) {
    backendCfg_.invalidate_hms_cache_on_ddls = flag;
  }

  public boolean enableSyncToLatestEventOnDdls() {
    return backendCfg_.enable_sync_to_latest_event_on_ddls;
  }

  @VisibleForTesting
  public void setEnableSyncToLatestEventOnDdls(boolean flag) {
    backendCfg_.enable_sync_to_latest_event_on_ddls = flag;
  }

  public boolean enableReloadEvents() {
    return backendCfg_.enable_reload_events;
  }

  @VisibleForTesting
  public void setEnableReloadEvents(boolean flag) {
    backendCfg_.enable_reload_events = flag;
  }

  public boolean enableSkippingOlderEvents() {
    return backendCfg_.enable_skipping_older_events;
  }

  @VisibleForTesting
  public void setSkippingOlderEvents(boolean flag) {
    backendCfg_.enable_skipping_older_events = flag;
  }

  public boolean pullTableTypesAndComments() {
    return backendCfg_.pull_table_types_and_comments;
  }

  public boolean useHmsColumnOrderForHBaseTables() {
    return backendCfg_.use_hms_column_order_for_hbase_tables;
  }

  public String getIgnoredDirPrefixList() {
    return backendCfg_.ignored_dir_prefix_list;
  }

  public TGeospatialLibrary getGeospatialLibrary() {
    return backendCfg_.geospatial_library;
  }

  public double getQueryCpuCountDivisor() { return backendCfg_.query_cpu_count_divisor; }

  public boolean isProcessingCostUseEqualExprWeight() {
    return backendCfg_.processing_cost_use_equal_expr_weight;
  }

  public long getMinProcessingPerThread() {
    return backendCfg_.min_processing_per_thread;
  }

  public boolean isSkipResourceCheckingOnLastExecutorGroupSet() {
    return backendCfg_.skip_resource_checking_on_last_executor_group_set;
  }

  public int getThriftRpcMaxMessageSize() {
    // With IMPALA-13020, the C++ max message size is a 64-bit integer,
    // but the Java max message size is still 32-bit. Cap the Java value
    // at Integer.MAX_VALUE;
    return (int) Math.min(backendCfg_.thrift_rpc_max_message_size, Integer.MAX_VALUE);
  }

  public String getFileMetadataReloadProperties() {
    return backendCfg_.file_metadata_reload_properties;
  }

  @VisibleForTesting
  public void setFileMetadataReloadProperties(String newPropertiesConfig) {
    backendCfg_.file_metadata_reload_properties = newPropertiesConfig;
  }

  public float getScanRangeCostFactor() {
    return (float) backendCfg_.scan_range_cost_factor;
  }

  public boolean useJammWeigher() {
    return backendCfg_.use_jamm_weigher;
  }

  public int icebergReloadNewFilesThreshold() {
    return backendCfg_.iceberg_reload_new_files_threshold;
  }

  public boolean icebergAllowDatafileInTableLocationOnly() {
    return backendCfg_.iceberg_allow_datafiles_in_table_location_only;
  }

  public void setIcebergAllowDatafileInTableLocationOnly(boolean flag) {
    backendCfg_.iceberg_allow_datafiles_in_table_location_only = flag;
  }

  public boolean isJsonScannerEnabled() {
    return backendCfg_.enable_json_scanner;
  }

  public double getMaxFilterErrorRateFromFullScan() {
    return backendCfg_.max_filter_error_rate_from_full_scan;
  }

  public int catalogOperationLogSize() {
    return backendCfg_.catalog_operation_log_size >= 0 ?
        backendCfg_.catalog_operation_log_size : Integer.MAX_VALUE;
  }

  public String getHostname() {
    return backendCfg_.hostname;
  }

  public boolean allowCatalogCacheOpFromMaskedUsers() {
    return backendCfg_.allow_catalog_cache_op_from_masked_users;
  }

  public String debugActions() { return backendCfg_.debug_actions; }

  public boolean isInvalidateMetadataOnEventProcessFailureEnabled() {
    return backendCfg_.invalidate_metadata_on_event_processing_failure;
  }

  public boolean isInvalidateGlobalMetadataOnEventProcessFailureEnabled() {
    return backendCfg_.invalidate_global_metadata_on_event_processing_failure;
  }

  public void setInvalidateGlobalMetadataOnEventProcessFailure(boolean isEnabled) {
    backendCfg_.invalidate_global_metadata_on_event_processing_failure = isEnabled;
  }

  public String getProcessEventFailureEventTypes() {
    return backendCfg_.inject_process_event_failure_event_types;
  }

  public double getProcessEventFailureRatio() {
    return backendCfg_.inject_process_event_failure_ratio;
  }

  public boolean enableWorkloadMgmt() {
    return backendCfg_.enable_workload_mgmt;
  }

  @VisibleForTesting
  public void setEnableWorkloadMgmt(boolean enableWorkloadMgmt) {
    backendCfg_.enable_workload_mgmt = enableWorkloadMgmt;
  }

  public String queryLogTableName() {
    return backendCfg_.query_log_table_name;
  }

  public boolean isMinimalTopicMode() {
    return backendCfg_.catalog_topic_mode.equalsIgnoreCase("minimal");
  }

  public double getQueryCpuRootFactor() { return backendCfg_.query_cpu_root_factor; }

  public String getDefaultSkippedHmsEventTypes() {
    return backendCfg_.default_skipped_hms_event_types;
  }

  public String getCommonHmsEventTypes() {
    return backendCfg_.common_hms_event_types;
  }

  public int getDbcpMaxConnPoolSize() {
    return backendCfg_.dbcp_max_conn_pool_size;
  }

  public int getDbcpMaxWaitMillisForConn() {
    return backendCfg_.dbcp_max_wait_millis_for_conn;
  }

  public int getDbcpDataSourceIdleTimeoutInSeconds() {
    return backendCfg_.dbcp_data_source_idle_timeout;
  }

  public boolean isReleaseBuild() {
    return backendCfg_.is_release_build;
  }

  public boolean isCatalogdHAEnabled() {
    return backendCfg_.enable_catalogd_ha;
  }
}
