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

package org.apache.impala.util;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.impala.authorization.User;
import org.apache.impala.common.ByteUnits;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.InternalException;
import org.apache.impala.common.JniUtil;
import org.apache.impala.common.RuntimeEnv;
import org.apache.impala.thrift.TErrorCode;
import org.apache.impala.thrift.TPoolConfigParams;
import org.apache.impala.thrift.TPoolConfig;
import org.apache.impala.thrift.TResolveRequestPoolParams;
import org.apache.impala.thrift.TResolveRequestPoolResult;
import org.apache.impala.thrift.TStatus;
import org.apache.impala.util.FileWatchService.FileChangeListener;
import org.apache.impala.yarn.server.resourcemanager.scheduler.fair.AllocationConfiguration;
import org.apache.impala.yarn.server.resourcemanager.scheduler.fair.AllocationFileLoaderService;
import org.apache.impala.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

/**
 * Admission control utility class that provides user to request pool mapping, ACL
 * enforcement, and pool configuration values. Pools are configured via a fair scheduler
 * allocation file (fair-scheduler.xml) and Llama configuration (llama-site.xml). This
 * class wraps a number of Hadoop classes to provide the user to pool mapping,
 * authorization, and accessing memory resource limits, all of which are specified in
 * the fair scheduler allocation file. The other pool limits are specified in the
 * Llama configuration, and those properties are accessed via the standard
 * {@link Configuration} API.
 *
 * Both the allocation configuration and Llama configuration files are watched for
 * changes and reloaded when necessary. The allocation file is watched/loaded using the
 * Yarn {@link AllocationFileLoaderService} and the Llama configuration uses a subclass of
 * the {@link FileWatchService}. There are two different mechanisms because there is
 * different parsing/configuration code for the allocation file and the Llama
 * configuration (which is a regular Hadoop conf file so it can use the
 * {@link Configuration} class). start() and stop() will start/stop watching and reloading
 * both of these files.
 *
 * A single instance is created by the backend and lasts the duration of the process.
 */
public class RequestPoolService {
  final static Logger LOG = LoggerFactory.getLogger(RequestPoolService.class);

  // Used to ensure start() has been called before any other methods can be used.
  private final AtomicBoolean running_;

  // Key for the default maximum number of running queries ("placed reservations")
  // property. The per-pool key name is this key with the pool name appended, e.g.
  // "{key}.{pool}".
  private final static String MAX_PLACED_RESERVATIONS_KEY =
      "llama.am.throttling.maximum.placed.reservations";

  // Default value for the maximum.placed.reservations property. Note that this value
  // differs from the current Llama default of 10000.
  private final static int MAX_PLACED_RESERVATIONS_DEFAULT = -1;

  // Key for the default maximum number of queued requests ("queued reservations")
  // property. The per-pool key name is this key with the pool name appended, e.g.
  // "{key}.{pool}".
  private final static String MAX_QUEUED_RESERVATIONS_KEY =
      "llama.am.throttling.maximum.queued.reservations";

  // Default value for the maximum.queued.reservations property. Note that this value
  // differs from the current Llama default of 0 which disables queuing.
  private final static int MAX_QUEUED_RESERVATIONS_DEFAULT = 200;

  // Key for the pool queue timeout (milliseconds).
  private final static String QUEUE_TIMEOUT_KEY =
      "impala.admission-control.pool-queue-timeout-ms";

  // Key for the pool default query options. Query options are specified as a
  // comma delimited string of 'key=value' pairs, e.g. 'key1=val1,key2=val2'.
  private final static String QUERY_OPTIONS_KEY =
      "impala.admission-control.pool-default-query-options";

  // Keys for the pool max and min query mem limits (in bytes) respectively.
  private final static String MAX_QUERY_MEM_LIMIT_BYTES =
      "impala.admission-control.max-query-mem-limit";
  private final static String MIN_QUERY_MEM_LIMIT_BYTES =
      "impala.admission-control.min-query-mem-limit";

  // Key for specifying if the mem_limit query option can override max/min mem limits
  // of the pool.
  private final static String CLAMP_MEM_LIMIT_QUERY_OPTION =
      "impala.admission-control.clamp-mem-limit-query-option";

  // Key for specifying the "Max mt_dop" configuration of the pool
  private final static String MAX_MT_DOP = "impala.admission-control.max-mt-dop";

  // Keys for the pool max query cpu core per node and coordinator respectively.
  private final static String MAX_QUERY_CPU_CORE_PER_NODE_LIMIT =
      "impala.admission-control.max-query-cpu-core-per-node-limit";
  private final static String MAX_QUERY_CPU_CORE_COORDINATOR_LIMIT =
      "impala.admission-control.max-query-cpu-core-coordinator-limit";

  // String format for a per-pool configuration key. First parameter is the key for the
  // default, e.g. MAX_PLACED_RESERVATIONS_KEY, and the second parameter is the
  // pool name.
  private final static String PER_POOL_CONFIG_KEY_FORMAT = "%s.%s";

  // Watches for changes to the fair scheduler allocation file.
  @VisibleForTesting
  final AllocationFileLoaderService allocLoader_;

  // Provides access to the fair scheduler allocation file. An AtomicReference becaus it
  // is reset when the allocation configuration file changes and other threads access it.
  private final AtomicReference<AllocationConfiguration> allocationConf_;

  // Watches the configuration file for changes.
  @VisibleForTesting
  final FileWatchService confWatcher_;

  // Used by this class to access to the configs provided by the configuration.
  // This is replaced when the configuration file changes.
  private volatile Configuration conf_;

  // URL of the configuration file.
  private final URL confUrl_;

  // Reference of single instance of RequestPoolService.
  private static RequestPoolService single_instance_ = null;

  /**
   * Updates the configuration when the file changes. The file is confUrl_
   * and it will exist when this is created (or RequestPoolService will not start). If
   * the file is later removed, warnings will be written to the log but the previous
   * configuration will still be accessible.
   */
  private final class ConfWatcher implements FileChangeListener {
    public void onFileChange() {
      // If confUrl_ is null the watcher should not have been created.
      Preconditions.checkNotNull(confUrl_);
      LOG.info("Loading configuration: " + confUrl_.getFile());
      Configuration conf = new Configuration();
      conf.addResource(confUrl_);
      conf_ = conf;
    }
  }

  /**
   * Static method to create singleton instance of RequestPoolService class.
   * This API is called by backend code through JNI, or called by unit-test code.
   */
  public static RequestPoolService getInstance(
      final String fsAllocationPath, final String sitePath, boolean isTest) {
    // For frontend and backend tests, different request pools could be created with
    // different configurations in one process so we have to allow multiple instances
    // to be created for frontend and backend tests.
    if (isTest) return (new RequestPoolService(fsAllocationPath, sitePath));

    if (single_instance_ == null) {
      single_instance_ = new RequestPoolService(fsAllocationPath, sitePath);
    }
    return single_instance_;
  }

  /**
   * Static method to return singleton instance of RequestPoolService class.
   * This API is called by frontend Java code. An instance should be already created
   * by backend before this API is called except only default pool is used.
   */
  public static RequestPoolService getInstance() {
    if (single_instance_ == null) {
      LOG.info("Default pool only, scheduler allocation is not specified.");
    }
    return single_instance_;
  }

  /**
   * Creates a RequestPoolService instance with a configuration containing the specified
   * fair-scheduler.xml and llama-site.xml.
   *
   * @param fsAllocationPath path to the fair scheduler allocation file.
   * @param sitePath path to the configuration file.
   */
  private RequestPoolService(final String fsAllocationPath, final String sitePath) {
    Preconditions.checkNotNull(fsAllocationPath);
    running_ = new AtomicBoolean(false);
    allocationConf_ = new AtomicReference<>();
    URL fsAllocationURL = getURL(fsAllocationPath);
    if (fsAllocationURL == null) {
      throw new IllegalArgumentException(
          "Unable to find allocation configuration file: " + fsAllocationPath);
    }
    // Load the default Hadoop configuration files for picking up overrides like custom
    // group mapping plugins etc.
    Configuration allocConf = new Configuration();
    allocConf.set(FairSchedulerConfiguration.ALLOCATION_FILE, fsAllocationURL.getPath());
    allocLoader_ = new AllocationFileLoaderService();
    allocLoader_.init(allocConf);

    if (!Strings.isNullOrEmpty(sitePath)) {
      confUrl_ = getURL(sitePath);
      if (confUrl_ == null) {
        throw new IllegalArgumentException(
            "Unable to find configuration file: " + sitePath);
      }
      conf_ = new Configuration(false);
      conf_.addResource(confUrl_);
      confWatcher_ =
          new FileWatchService(new File(confUrl_.getPath()), new ConfWatcher());
    } else {
      confWatcher_ = null;
      confUrl_ = null;
    }
  }

  /**
   * Returns a {@link URL} for the file if it exists, null otherwise.
   */
  @VisibleForTesting
  private static URL getURL(String path) {
    Preconditions.checkNotNull(path);
    File file = new File(path);
    file = file.getAbsoluteFile();
    if (!file.exists()) {
      LOG.error("Unable to find specified file: " + path);
      return null;
    }
    try {
      return file.toURI().toURL();
    } catch (MalformedURLException ex) {
      LOG.error("Unable to construct URL for file: " + path, ex);
      return null;
    }
  }

  /**
   * Starts the RequestPoolService instance. It does the initial loading of the
   * configuration and starts the automatic reloading.
   */
  public void start() {
    Preconditions.checkState(!running_.get());
    allocLoader_.setReloadListener(allocationConf_::set);
    allocLoader_.start();
    try {
      allocLoader_.reloadAllocations();
    } catch (Exception ex) {
      try {
        stopInternal();
      } catch (Exception stopEx) {
        LOG.error("Unable to stop AllocationFileLoaderService after failed start.",
            stopEx);
      }
      throw new RuntimeException(ex);
    }
    if (confWatcher_ != null) confWatcher_.start();
    running_.set(true);
  }

  /**
   * Stops the RequestPoolService instance. Only used by tests.
   */
  public void stop() {
    Preconditions.checkState(running_.get());
    stopInternal();
  }

  /**
   * Stops the RequestPoolService instance without checking the running state. Only
   * called by stop() (which is only used in tests) or by start() if a failure occurs.
   * Should not be called more than once.
   */
  private void stopInternal() {
    running_.set(false);
    if (confWatcher_ != null) confWatcher_.stop();
    allocLoader_.stop();
  }

  /**
   * Resolves a user and pool to the pool specified by the allocation placement policy
   * and checks if the user is authorized to submit requests.
   *
   * @param resolvePoolParams {@link TResolveRequestPoolParams}
   * @return {@link TResolveRequestPoolResult}
   */
  public TResolveRequestPoolResult resolveRequestPool(
      TResolveRequestPoolParams resolvePoolParams) throws InternalException {
    Preconditions.checkState(running_.get());
    String requestedPool = resolvePoolParams.getRequested_pool();
    String user = resolvePoolParams.getUser();
    TResolveRequestPoolResult result = new TResolveRequestPoolResult();
    String errorMessage = null;
    String pool = null;
    try {
      pool = assignToPool(requestedPool, user);
    } catch (IOException ex) {
      errorMessage = ex.getMessage();
      if (errorMessage.startsWith("No groups found for user")) {
        // The error thrown when using the 'primaryGroup' or 'secondaryGroup' rules and
        // the user does not exist are not helpful.
        errorMessage = String.format(
            "Failed to resolve user '%s' to a pool while evaluating the " +
            "'primaryGroup' or 'secondaryGroup' queue placement rules because no " +
            "groups were found for the user. This is likely because the user does not " +
            "exist on the local operating system.", resolvePoolParams.getUser());
      }
      LOG.warn(String.format("Error assigning to pool. requested='%s', user='%s', msg=%s",
          requestedPool, user, errorMessage), ex);
    }
    if (pool == null) {
      if (errorMessage == null) {
        // This occurs when assignToPool returns null (not an error), i.e. if the pool
        // cannot be resolved according to the policy.
        result.setStatus(new TStatus(TErrorCode.OK, Lists.newArrayList()));
      } else {
        // If Yarn throws an exception, return an error status.
        result.setStatus(
            new TStatus(TErrorCode.INTERNAL_ERROR, Lists.newArrayList(errorMessage)));
      }
    } else {
      result.setResolved_pool(pool);
      result.setHas_access(hasAccess(pool, user));
      result.setStatus(new TStatus(TErrorCode.OK, Lists.newArrayList()));
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("resolveRequestPool(pool={}, user={}): resolved_pool={}, has_access={}",
          resolvePoolParams.getRequested_pool(), resolvePoolParams.getUser(),
          result.resolved_pool, result.has_access);
    }
    return result;
  }

  /**
   * Gets the pool configuration values for the specified pool.
   *
   * @param pool name.
   * @return {@link TPoolConfig}
   */
  public TPoolConfig getPoolConfig(String pool) {
    Preconditions.checkState(running_.get());
    TPoolConfig result = new TPoolConfig();
    long maxMemoryMb = allocationConf_.get().getMaxResources(pool).getMemory();
    result.setMax_mem_resources(
        maxMemoryMb == Integer.MAX_VALUE ? -1 : maxMemoryMb * ByteUnits.MEGABYTE);
    if (conf_ == null) {
      result.setMax_requests(MAX_PLACED_RESERVATIONS_DEFAULT);
      result.setMax_queued(MAX_QUEUED_RESERVATIONS_DEFAULT);
      result.setDefault_query_options("");
    } else {
      // Capture the current conf_ in case it changes while we're using it.
      Configuration currentConf = conf_;
      result.setMax_requests(getPoolConfigValue(currentConf, pool,
          MAX_PLACED_RESERVATIONS_KEY, MAX_PLACED_RESERVATIONS_DEFAULT));
      result.setMax_queued(getPoolConfigValue(currentConf, pool,
          MAX_QUEUED_RESERVATIONS_KEY, MAX_QUEUED_RESERVATIONS_DEFAULT));

      // Only return positive values. Admission control has a default from gflags.
      long queueTimeoutMs = getPoolConfigValue(currentConf, pool, QUEUE_TIMEOUT_KEY, -1L);
      if (queueTimeoutMs > 0) result.setQueue_timeout_ms(queueTimeoutMs);
      result.setDefault_query_options(
          getPoolConfigValue(currentConf, pool, QUERY_OPTIONS_KEY, ""));
      result.setMax_query_mem_limit(
          getPoolConfigValue(currentConf, pool, MAX_QUERY_MEM_LIMIT_BYTES, 0L));
      result.setMin_query_mem_limit(
          getPoolConfigValue(currentConf, pool, MIN_QUERY_MEM_LIMIT_BYTES, 0L));
      result.setClamp_mem_limit_query_option(
          getPoolConfigValue(currentConf, pool, CLAMP_MEM_LIMIT_QUERY_OPTION, true));
      result.setMax_mt_dop(
          getPoolConfigValue(currentConf, pool, MAX_MT_DOP, -1));
      result.setMax_query_cpu_core_per_node_limit(
          getPoolConfigValue(currentConf, pool, MAX_QUERY_CPU_CORE_PER_NODE_LIMIT, 0L));
      result.setMax_query_cpu_core_coordinator_limit(getPoolConfigValue(
          currentConf, pool, MAX_QUERY_CPU_CORE_COORDINATOR_LIMIT, 0L));
    }
    if (LOG.isTraceEnabled()) {
      LOG.debug("getPoolConfig(pool={}): max_mem_resources={}, max_requests={},"
              + " max_queued={},  queue_timeout_ms={}, default_query_options={},"
              + " max_query_mem_limit={}, min_query_mem_limit={},"
              + " clamp_mem_limit_query_option={}, max_query_cpu_core_per_node_limit={},"
              + " max_query_cpu_core_coordinator_limit={}",
          pool, result.max_mem_resources, result.max_requests, result.max_queued,
          result.queue_timeout_ms, result.default_query_options,
          result.max_query_mem_limit, result.min_query_mem_limit,
          result.clamp_mem_limit_query_option, result.max_query_cpu_core_per_node_limit,
          result.max_query_cpu_core_coordinator_limit);
    }
    return result;
  }

  /**
   * Looks up the per-pool integer config from the Configuration. First checks for
   * a per-pool value, then a default set in the config, and lastly to the specified
   * 'defaultValue'.
   *
   * @param conf The Configuration to use, provided so the caller can ensure the same
   *        Configuration is used to look up multiple properties.
   */
  private long getPoolConfigValue(
      Configuration conf, String pool, String key, long defaultValue) {
    return conf.getLong(String.format(PER_POOL_CONFIG_KEY_FORMAT, key, pool),
        conf.getLong(key, defaultValue));
  }

  /**
   * Looks up the per-pool String config from the Configuration. See above.
   */
  private String getPoolConfigValue(
      Configuration conf, String pool, String key, String defaultValue) {
    return conf.get(String.format(PER_POOL_CONFIG_KEY_FORMAT, key, pool),
        conf.get(key, defaultValue));
  }

  /**
   * Looks up the per-pool Boolean config from the Configuration. See above.
   */
  private boolean getPoolConfigValue(
      Configuration conf, String pool, String key, boolean defaultValue) {
    return conf.getBoolean(String.format(PER_POOL_CONFIG_KEY_FORMAT, key, pool),
        conf.getBoolean(key, defaultValue));
  }

  /**
   * Resolves the actual pool to use via the allocation placement policy. The policy may
   * change the requested pool.
   *
   * @param requestedPool The requested pool. May not be null, an empty string indicates
   * the policy should return the default pool for this user.
   * @param user The user, must not be null or empty.
   * @return the actual pool to use, null if a pool could not be resolved.
   */
  @VisibleForTesting
  String assignToPool(String requestedPool, String user)
      throws InternalException, IOException {
    Preconditions.checkState(running_.get());
    Preconditions.checkNotNull(requestedPool);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(user));
    // Convert the user name to a short name (e.g. 'user1@domain' to 'user1') because
    // assignAppToQueue() will check group membership which should always be done on
    // the short name of the principal.
    String shortName = new User(user).getShortName();
    return allocationConf_.get().getPlacementPolicy().assignAppToQueue(
        requestedPool.isEmpty() ? YarnConfiguration.DEFAULT_QUEUE_NAME : requestedPool,
        shortName);
  }

  /**
   * Indicates if a user has access to the pool.
   *
   * @param pool the pool to check if the user has access to. NOTE: it should always be
   * called with a pool returned by the {@link #assignToPool(String, String)} method.
   * @param user the user to check if it has access to the pool.
   * @return True if the user has access to the pool.
   */
  @VisibleForTesting
  boolean hasAccess(String pool, String user) throws InternalException {
    Preconditions.checkState(running_.get());
    Preconditions.checkArgument(!Strings.isNullOrEmpty(pool));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(user));
    // Convert the user name to a short name (e.g. 'user1@domain' to 'user1') because
    // the UserGroupInformation will check group membership which should always be done
    // on the short name of the principal.
    String shortName;
    User requestingUser = new User(user);
    shortName = requestingUser.getShortName();
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(shortName);
    return allocationConf_.get().hasAccess(pool, QueueACL.SUBMIT_APPLICATIONS, ugi);
  }

  /**
   * Returns the AllocationConfiguration corresponding to this instance of
   * RequestPoolService.
   */
  @VisibleForTesting
  AllocationConfiguration getAllocationConfig() {
    Preconditions.checkState(RuntimeEnv.INSTANCE.isTestEnv());
    return allocationConf_.get();
  }
}
