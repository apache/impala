// Copyright 2014 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.util;

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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.AllocationConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.AllocationFileLoaderService;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.authorization.User;
import com.cloudera.impala.common.ByteUnits;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.common.JniUtil;
import com.cloudera.impala.thrift.TErrorCode;
import com.cloudera.impala.thrift.TPoolConfigParams;
import com.cloudera.impala.thrift.TPoolConfig;
import com.cloudera.impala.thrift.TResolveRequestPoolParams;
import com.cloudera.impala.thrift.TResolveRequestPoolResult;
import com.cloudera.impala.thrift.TStatus;
import com.cloudera.impala.util.FileWatchService.FileChangeListener;
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

  private final static TBinaryProtocol.Factory protocolFactory_ =
      new TBinaryProtocol.Factory();
  // Used to ensure start() has been called before any other methods can be used.
  private final AtomicBoolean running_;

  // Key for the default maximum number of running queries ("placed reservations")
  // property. The per-pool key name is this key with the pool name appended, e.g.
  // "{key}.{pool}". This is a llama-site.xml configuration.
  final static String LLAMA_MAX_PLACED_RESERVATIONS_KEY =
      "llama.am.throttling.maximum.placed.reservations";

  // Default value for the maximum.placed.reservations property. Note that this value
  // differs from the current Llama default of 10000.
  final static int LLAMA_MAX_PLACED_RESERVATIONS_DEFAULT = -1;

  // Key for the default maximum number of queued requests ("queued reservations")
  // property. The per-pool key name is this key with the pool name appended, e.g.
  // "{key}.{pool}". This is a llama-site.xml configuration.
  final static String LLAMA_MAX_QUEUED_RESERVATIONS_KEY =
      "llama.am.throttling.maximum.queued.reservations";

  // Default value for the maximum.queued.reservations property. Note that this value
  // differs from the current Llama default of 0 which disables queuing.
  final static int LLAMA_MAX_QUEUED_RESERVATIONS_DEFAULT = 200;

  // Key for the pool queue timeout (milliseconds). This is be specified in the
  // llama-site.xml but is Impala-specific and Llama does not use this.
  final static String QUEUE_TIMEOUT_KEY = "impala.admission-control.pool-queue-timeout-ms";

  // Default value of the pool queue timeout (ms).
  final static int QUEUE_TIMEOUT_MS_DEFAULT = 60 * 1000;

  // Key for the pool default query options. Query options are specified as a
  // comma delimited string of 'key=value' pairs, e.g. 'key1=val1,key2=val2'.
  // This is specified in the llama-site.xml but is Impala-specific and Llama does not
  // use this.
  final static String QUERY_OPTIONS_KEY = "impala.admission-control.pool-default-query-options";

  // String format for a per-pool configuration key. First parameter is the key for the
  // default, e.g. LLAMA_MAX_PLACED_RESERVATIONS_KEY, and the second parameter is the
  // pool name.
  final static String LLAMA_PER_POOL_CONFIG_KEY_FORMAT = "%s.%s";

  // Watches for changes to the fair scheduler allocation file.
  @VisibleForTesting
  final AllocationFileLoaderService allocLoader_;

  // Provides access to the fair scheduler allocation file. An AtomicReference becaus it
  // is reset when the allocation configuration file changes and other threads access it.
  private final AtomicReference<AllocationConfiguration> allocationConf_;

  // Watches the Llama configuration file for changes.
  @VisibleForTesting
  final FileWatchService llamaConfWatcher_;

  // Used by this class to access to the configs provided by the Llama configuration.
  // This is replaced when the Llama configuration file changes.
  private volatile Configuration llamaConf_;

  // URL of the Llama configuration file.
  private final URL llamaConfUrl_;

  /**
   * Updates the Llama configuration when the file changes. The file is llamaConfUrl_
   * and it will exist when this is created (or RequestPoolService will not start). If
   * the file is later removed, warnings will be written to the log but the previous
   * configuration will still be accessible.
   */
  private final class LlamaConfWatcher implements FileChangeListener {
    public void onFileChange() {
      // If llamaConfUrl_ is null the watcher should not have been created.
      Preconditions.checkNotNull(llamaConfUrl_);
      LOG.info("Loading Llama configuration: " + llamaConfUrl_.getFile());
      Configuration conf = new Configuration();
      conf.addResource(llamaConfUrl_);
      llamaConf_ = conf;
    }
  }

  /**
   * Creates a RequestPoolService instance with a configuration containing the specified
   * fair-scheduler.xml and llama-site.xml.
   *
   * @param fsAllocationPath path to the fair scheduler allocation file.
   * @param llamaSitePath path to the Llama configuration file.
   */
  public RequestPoolService(final String fsAllocationPath, final String llamaSitePath) {
    Preconditions.checkNotNull(fsAllocationPath);
    running_ = new AtomicBoolean(false);
    allocationConf_ = new AtomicReference<AllocationConfiguration>();
    URL fsAllocationURL = getURL(fsAllocationPath);
    if (fsAllocationURL == null) {
      throw new IllegalArgumentException(
          "Unable to find allocation configuration file: " + fsAllocationPath);
    }
    Configuration allocConf = new Configuration(false);
    allocConf.set(FairSchedulerConfiguration.ALLOCATION_FILE, fsAllocationURL.getPath());
    allocLoader_ = new AllocationFileLoaderService();
    allocLoader_.init(allocConf);

    if (!Strings.isNullOrEmpty(llamaSitePath)) {
      llamaConfUrl_ = getURL(llamaSitePath);
      if (llamaConfUrl_ == null) {
        throw new IllegalArgumentException(
            "Unable to find Llama configuration file: " + llamaSitePath);
      }
      llamaConf_ = new Configuration(false);
      llamaConf_.addResource(llamaConfUrl_);
      llamaConfWatcher_ = new FileWatchService(new File(llamaConfUrl_.getPath()),
          new LlamaConfWatcher());
    } else {
      llamaConfWatcher_ = null;
      llamaConfUrl_ = null;
    }
  }

  /**
   * Returns a {@link URL} for the file if it exists, null otherwise.
   */
  @VisibleForTesting
  static URL getURL(String path) {
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
    allocLoader_.setReloadListener(new AllocationFileLoaderService.Listener() {
      @Override
      public void onReload(AllocationConfiguration info) {
        allocationConf_.set(info);
      }
    });
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
    if (llamaConfWatcher_ != null) llamaConfWatcher_.start();
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
    if (llamaConfWatcher_ != null) llamaConfWatcher_.stop();
    allocLoader_.stop();
  }

  /**
   * Resolves a user and pool to the pool specified by the allocation placement policy
   * and checks if the user is authorized to submit requests.
   *
   * @param thriftResolvePoolParams Serialized {@link TResolveRequestPoolParams}
   * @return serialized {@link TResolveRequestPoolResult}
   */
  public byte[] resolveRequestPool(byte[] thriftResolvePoolParams)
      throws ImpalaException {
    TResolveRequestPoolParams resolvePoolParams = new TResolveRequestPoolParams();
    JniUtil.deserializeThrift(protocolFactory_, resolvePoolParams,
        thriftResolvePoolParams);
    TResolveRequestPoolResult result = resolveRequestPool(resolvePoolParams);
    LOG.info("resolveRequestPool(pool={}, user={}): resolved_pool={}, has_access={}",
        new Object[] { resolvePoolParams.getRequested_pool(), resolvePoolParams.getUser(),
                       result.resolved_pool, result.has_access });
    try {
      return new TSerializer(protocolFactory_).serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  @VisibleForTesting
  TResolveRequestPoolResult resolveRequestPool(
      TResolveRequestPoolParams resolvePoolParams) {
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
        result.setStatus(new TStatus(TErrorCode.OK, Lists.<String>newArrayList()));
      } else {
        // If Yarn throws an exception, return an error status.
        result.setStatus(
            new TStatus(TErrorCode.INTERNAL_ERROR, Lists.newArrayList(errorMessage)));
      }
    } else {
      result.setResolved_pool(pool);
      result.setHas_access(hasAccess(pool, user));
      result.setStatus(new TStatus(TErrorCode.OK, Lists.<String>newArrayList()));
    }
    return result;
  }

  /**
   * Gets the pool configuration values for the specified pool.
   *
   * @param thriftPoolConfigParams Serialized {@link TPoolConfigParams}
   * @return serialized {@link TPoolConfig}
   */
  public byte[] getPoolConfig(byte[] thriftPoolConfigParams) throws ImpalaException {
    Preconditions.checkState(running_.get());
    TPoolConfigParams poolConfigParams = new TPoolConfigParams();
    JniUtil.deserializeThrift(protocolFactory_, poolConfigParams,
        thriftPoolConfigParams);
    TPoolConfig result = getPoolConfig(poolConfigParams.getPool());
    try {
      return new TSerializer(protocolFactory_).serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  @VisibleForTesting
  TPoolConfig getPoolConfig(String pool) {
    TPoolConfig result = new TPoolConfig();
    long maxMemoryMb = allocationConf_.get().getMaxResources(pool).getMemory();
    result.setMax_mem_resources(
        maxMemoryMb == Integer.MAX_VALUE ? -1 : (long) maxMemoryMb * ByteUnits.MEGABYTE);
    if (llamaConf_ == null) {
      result.setMax_requests(LLAMA_MAX_PLACED_RESERVATIONS_DEFAULT);
      result.setMax_queued(LLAMA_MAX_QUEUED_RESERVATIONS_DEFAULT);
      result.setDefault_query_options("");
    } else {
      // Capture the current llamaConf_ in case it changes while we're using it.
      Configuration currentLlamaConf = llamaConf_;
      result.setMax_requests(getLlamaPoolConfigValue(currentLlamaConf, pool,
          LLAMA_MAX_PLACED_RESERVATIONS_KEY,
          LLAMA_MAX_PLACED_RESERVATIONS_DEFAULT));
      result.setMax_queued(getLlamaPoolConfigValue(currentLlamaConf, pool,
          LLAMA_MAX_QUEUED_RESERVATIONS_KEY,
          LLAMA_MAX_QUEUED_RESERVATIONS_DEFAULT));

      // Only return positive values. Admission control has a default from gflags.
      int queueTimeoutMs = getLlamaPoolConfigValue(currentLlamaConf, pool,
          QUEUE_TIMEOUT_KEY, -1);
      if (queueTimeoutMs > 0) result.setQueue_timeout_ms(queueTimeoutMs);
      result.setDefault_query_options(getLlamaPoolConfigValue(currentLlamaConf, pool,
          QUERY_OPTIONS_KEY, ""));
    }
    LOG.info("getPoolConfig(pool={}): max_mem_resources={}, max_requests={}, " +
        "max_queued={},  queue_timeout_ms={}, default_query_options={}",
        new Object[] { pool, result.max_mem_resources, result.max_requests,
            result.max_queued, result.queue_timeout_ms, result.default_query_options });
    return result;
  }

  /**
   * Looks up the per-pool integer config from the llama Configuration. First checks for
   * a per-pool value, then a default set in the config, and lastly to the specified
   * 'defaultValue'.
   *
   * @param conf The Configuration to use, provided so the caller can ensure the same
   *        Configuration is used to look up multiple properties.
   */
  private int getLlamaPoolConfigValue(Configuration conf, String pool, String key,
      int defaultValue) {
    return conf.getInt(String.format(LLAMA_PER_POOL_CONFIG_KEY_FORMAT, key, pool),
        conf.getInt(key, defaultValue));
  }

  /**
   * Looks up the per-pool String config from the llama Configuration. See above.
   */
  private String getLlamaPoolConfigValue(Configuration conf, String pool, String key,
      String defaultValue) {
    return conf.get(String.format(LLAMA_PER_POOL_CONFIG_KEY_FORMAT, key, pool),
        conf.get(key, defaultValue));
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
      throws IOException {
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
  boolean hasAccess(String pool, String user) {
    Preconditions.checkState(running_.get());
    Preconditions.checkArgument(!Strings.isNullOrEmpty(pool));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(user));
    // Convert the user name to a short name (e.g. 'user1@domain' to 'user1') because
    // the UserGroupInformation will check group membership which should always be done
    // on the short name of the principal.
    String shortName = new User(user).getShortName();
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(shortName);
    return allocationConf_.get().hasAccess(pool, QueueACL.SUBMIT_APPLICATIONS, ugi);
  }
}
