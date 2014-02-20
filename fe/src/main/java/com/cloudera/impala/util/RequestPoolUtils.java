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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.AllocationConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.AllocationFileLoaderService;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.common.JniUtil;
import com.cloudera.impala.thrift.TPoolConfigParams;
import com.cloudera.impala.thrift.TPoolConfigResult;
import com.cloudera.impala.thrift.TResolveRequestPoolParams;
import com.cloudera.impala.thrift.TResolveRequestPoolResult;
import com.cloudera.impala.util.FileWatchService.FileChangeListener;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

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
public class RequestPoolUtils {
  final static Logger LOG = LoggerFactory.getLogger(RequestPoolUtils.class);

  private final static TBinaryProtocol.Factory protocolFactory_ =
      new TBinaryProtocol.Factory();
  // Used to ensure start() has been called before any other methods can be used.
  private final AtomicBoolean running_;

  // Key for the default maximum number of running queries ("placed reservations")
  // property. The per-pool key name is this key with the pool name appended, e.g.
  // "{key}.{pool}".
  final static String LLAMA_MAX_PLACED_RESERVATIONS_KEY =
      "llama.am.throttling.maximum.placed.reservations";

  // If the default maximum.placed.reservations property isn't set in the config, Llama
  // uses this default value.
  final static int LLAMA_MAX_PLACED_RESERVATIONS_DEFAULT = 20;

  // Key for the default maximum number of queued requests ("queued reservations")
  // property. The per-pool key name is this key with the pool name appended, e.g.
  // "{key}.{pool}".
  final static String LLAMA_MAX_QUEUED_RESERVATIONS_KEY =
      "llama.am.throttling.maximum.queued.reservations";

  // If the default maximum.queued.reservations property isn't set in the config, Llama
  // uses this default value.
  final static int LLAMA_MAX_QUEUED_RESERVATIONS_DEFAULT = 50;

  // String format for a per-pool configuration key. First parameter is the key for the
  // default, e.g. LLAMA_MAX_PLACED_RESERVATIONS_KEY, and the second parameter is the
  // pool name.
  final static String LLAMA_PER_POOL_CONFIG_KEY_FORMAT = "%s.%s";
  // TODO: Find a common place for constants like these.
  final static long ONE_MEGABYTE = 1024 * 1024; // One megabyte in bytes.

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
   * and it will exist when this is created (or RequestPoolUtils will not start). If
   * the file is later removed, warnings will be written to the log but the previous
   * configuration will still be accessible.
   */
  private final class LlamaConfWatcher implements FileChangeListener {
    public void onFileChange() {
      Configuration conf = new Configuration();
      conf.addResource(llamaConfUrl_);
      llamaConf_ = conf;
    }
  }

  /**
   * Creates a RequestPoolUtils instance with a configuration containing the specified
   * fair-scheduler.xml and llama-site.xml. Both files must either be specified as
   * absolute paths or relative paths on the classpath.
   *
   * @param fsAllocationPath path to the fair scheduler allocation file.
   * @param llamaSitePath path to the Llama configuration file.
   */
  public RequestPoolUtils(final String fsAllocationPath, final String llamaSitePath) {
    Preconditions.checkNotNull(fsAllocationPath);
    running_ = new AtomicBoolean(false);
    allocationConf_ = new AtomicReference<AllocationConfiguration>();
    if (getURL(fsAllocationPath) == null) {
      throw new IllegalArgumentException("Allocation configuration file " +
          fsAllocationPath + " is not an absolute path or a file on the classpath.");
    }
    Configuration allocConf = new Configuration(false);
    allocConf.set(FairSchedulerConfiguration.ALLOCATION_FILE, fsAllocationPath);
    allocLoader_ = new AllocationFileLoaderService();
    allocLoader_.init(allocConf);

    if (!Strings.isNullOrEmpty(llamaSitePath)) {
      llamaConfUrl_ = getURL(llamaSitePath);
      if (llamaConfUrl_ == null) {
        throw new IllegalArgumentException("Llama configuration file " + llamaSitePath +
            " is not an absolute path or a file on the classpath.");
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
   * Returns a {@link URL} for the file which is either an absolute path or a file on
   * the classpath.
   */
  @VisibleForTesting
  static URL getURL(String path) {
    Preconditions.checkNotNull(path);
    File file = new File(path);
    if (file.isAbsolute()) {
      try {
        return file.toURI().toURL();
      } catch (MalformedURLException ex) {
        LOG.error("Unable to find specified file: " + path, ex);
        return null;
      }
    }
    URL url = Thread.currentThread().getContextClassLoader().getResource(path);
    if (url == null) {
      LOG.warn("Configuration file '{}' not found on the classpath.", path);
      return null;
    } else if (!url.getProtocol().equalsIgnoreCase("file")) {
      throw new RuntimeException("Allocation file " + url
          + " found on the classpath is not on the local filesystem.");
    } else {
      return url;
    }
  }

  /**
   * Starts the RequestPoolUtils instance. It does the initial loading of the
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
      stop();
      throw new RuntimeException(ex);
    }
    if (llamaConfWatcher_ != null) llamaConfWatcher_.start();
    running_.set(true);
  }

  /**
   * Stops the RequestPoolUtils instance. Only used by tests.
   */
  public void stop() {
    Preconditions.checkState(running_.get());
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
      throws ImpalaException, IOException {
    TResolveRequestPoolParams resolvePoolParams = new TResolveRequestPoolParams();
    JniUtil.deserializeThrift(protocolFactory_, resolvePoolParams,
        thriftResolvePoolParams);
    TResolveRequestPoolResult result = new TResolveRequestPoolResult();
    String pool = assignToPool(resolvePoolParams.getRequested_pool(),
        resolvePoolParams.getUser());
    if (pool == null) {
      result.setResolved_pool("");
      result.setHas_access(false);
    } else {
      result.setResolved_pool(pool);
      result.setHas_access(hasAccess(
          result.getResolved_pool(),
          resolvePoolParams.getUser()));
    }
    LOG.trace("resolveRequestPool(pool={}, user={}): resolved_pool={}, has_access={}",
        new Object[] {resolvePoolParams.getRequested_pool(), resolvePoolParams.getUser(),
        result.resolved_pool, result.has_access});
    try {
      return new TSerializer(protocolFactory_).serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  /**
   * Gets the pool configuration values for the specified pool.
   *
   * @param thriftPoolConfigParams Serialized {@link TPoolConfigParams}
   * @return serialized {@link TPoolConfigResult}
   */
  public byte[] getPoolConfig(byte[] thriftPoolConfigParams) throws ImpalaException {
    Preconditions.checkState(running_.get());
    TPoolConfigParams poolConfigParams = new TPoolConfigParams();
    JniUtil.deserializeThrift(protocolFactory_, poolConfigParams,
        thriftPoolConfigParams);
    TPoolConfigResult result = getPoolConfig(poolConfigParams.getPool());
    try {
      return new TSerializer(protocolFactory_).serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  @VisibleForTesting
  TPoolConfigResult getPoolConfig(String pool) {
    TPoolConfigResult result = new TPoolConfigResult();
    int maxMemoryMb = allocationConf_.get().getMaxResources(pool).getMemory();
    result.setMem_limit(
        maxMemoryMb == Integer.MAX_VALUE ? -1 : (long) maxMemoryMb * ONE_MEGABYTE);
    // Capture the current llamaConf_ in case it changes while we're using it.
    Configuration currentLlamaConf = llamaConf_;
    result.setMax_requests(getLlamaPoolConfigValue(currentLlamaConf, pool,
        LLAMA_MAX_PLACED_RESERVATIONS_KEY,
        LLAMA_MAX_PLACED_RESERVATIONS_DEFAULT));
    result.setMax_queued(getLlamaPoolConfigValue(currentLlamaConf, pool,
        LLAMA_MAX_QUEUED_RESERVATIONS_KEY,
        LLAMA_MAX_QUEUED_RESERVATIONS_DEFAULT));
    LOG.trace("getPoolConfig(pool={}): mem_limit={}, max_requests={}, max_queued={}",
        new Object[] { pool, result.mem_limit, result.max_requests, result.max_queued });
    return result;
  }

  /**
   * Looks up the per-pool Llama config, first checking for a per-pool value, then a
   * default set in the config, and lastly to the specified 'defaultValue'.
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
    return allocationConf_.get().getPlacementPolicy().assignAppToQueue(
        requestedPool, user);
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
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
    return allocationConf_.get().hasAccess(pool, QueueACL.SUBMIT_APPLICATIONS, ugi);
  }
}
