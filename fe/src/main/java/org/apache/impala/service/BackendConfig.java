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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * This class is meant to provide the FE with impalad backend configuration parameters,
 * including command line arguments.
 */
public class BackendConfig {
  public static BackendConfig INSTANCE;

  private TBackendGflags backendCfg_;

  private BackendConfig(TBackendGflags cfg) {
    backendCfg_ = cfg;
  }

  public static void create(TBackendGflags cfg) {
    Preconditions.checkNotNull(cfg);
    INSTANCE = new BackendConfig(cfg);
    SqlScanner.init(cfg.getReserved_words_version());
    initAuthToLocal();
  }

  public TBackendGflags getBackendCfg() { return backendCfg_; }
  public long getReadSize() { return backendCfg_.read_size; }
  public boolean getComputeLineage() {
    return !Strings.isNullOrEmpty(backendCfg_.lineage_event_log_dir);
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

  public String getImpalaBuildVersion() { return backendCfg_.impala_build_version; }
  public int getImpalaLogLevel() { return backendCfg_.impala_log_lvl; }
  public int getNonImpalaJavaVlogLevel() { return backendCfg_.non_impala_java_vlog; }
  public long getSentryCatalogPollingFrequency() {
    return backendCfg_.sentry_catalog_polling_frequency_s;
  }

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

  public boolean disableCatalogDataOpsDebugOnly() {
    return backendCfg_.disable_catalog_data_ops_debug_only;
  }

  public boolean pullIncrementalStatistics() {
    return backendCfg_.pull_incremental_statistics;
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

  public boolean isOrcScannerEnabled() {
    return backendCfg_.enable_orc_scanner;
  }

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

  public boolean isMtDopUnlocked() {
    return backendCfg_.unlock_mt_dop;
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
}
