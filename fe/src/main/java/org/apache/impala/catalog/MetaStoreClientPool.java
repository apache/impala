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

package org.apache.impala.catalog;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

/**
 * Manages a pool of RetryingMetaStoreClient connections. If the connection pool is empty
 * a new client is created and added to the pool. The idle pool can expand till a maximum
 * size of MAX_HMS_CONNECTION_POOL_SIZE, beyond which the connections are closed.
 *
 * This default implementation reads the Hive metastore configuration from the HiveConf
 * object passed in the c'tor. If you are looking for a temporary HMS instance created
 * from scratch for unit tests, refer to EmbeddedMetastoreClientPool class. It mocks an
 * actual HMS by creating a temporary Derby backend database on the fly. It should not
 * be used for production Catalog server instances.
 */
public class MetaStoreClientPool {
  // Key for config option read from hive-site.xml
  private static final String HIVE_METASTORE_CNXN_DELAY_MS_CONF =
      "impala.catalog.metastore.cnxn.creation.delay.ms";
  private static final int DEFAULT_HIVE_METASTORE_CNXN_DELAY_MS_CONF = 0;
  // Maximum number of idle metastore connections in the connection pool at any point.
  private static final int MAX_HMS_CONNECTION_POOL_SIZE = 32;

  private final AtomicInteger numHmsClientsInUse_ = new AtomicInteger(0);

  // Number of milliseconds to sleep between creation of HMS connections. Used to debug
  // IMPALA-825.
  private final int clientCreationDelayMs_;

  private static final Logger LOG = Logger.getLogger(MetaStoreClientPool.class);

  private final ConcurrentLinkedQueue<MetaStoreClient> clientPool_ =
      new ConcurrentLinkedQueue<MetaStoreClient>();
  private Boolean poolClosed_ = false;
  private final Object poolCloseLock_ = new Object();
  private final HiveConf hiveConf_;

  // Required for creating an instance of RetryingMetaStoreClient.
  private static final HiveMetaHookLoader dummyHookLoader = new HiveMetaHookLoader() {
    @Override
    public HiveMetaHook getHook(org.apache.hadoop.hive.metastore.api.Table tbl)
        throws MetaException {
      return null;
    }
  };

  /**
   * A wrapper around the RetryingMetaStoreClient that manages interactions with the
   * connection pool. This implements the AutoCloseable interface and hence the callers
   * should use the try-with-resources statement while creating an instance.
   */
  public class MetaStoreClient implements AutoCloseable {
    private final IMetaStoreClient hiveClient_;
    private boolean isInUse_;

    /**
     * Creates a new instance of MetaStoreClient.
     * 'cnxnTimeoutSec' specifies the time MetaStoreClient will wait to establish first
     * connection to the HMS before giving up and failing out with an exception.
     */
    private MetaStoreClient(HiveConf hiveConf, int cnxnTimeoutSec) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Creating MetaStoreClient. Pool Size = " + clientPool_.size());
      }

      long retryDelaySeconds = hiveConf.getTimeVar(
          HiveConf.ConfVars.METASTORE_CLIENT_CONNECT_RETRY_DELAY, TimeUnit.SECONDS);
      long retryDelayMillis = retryDelaySeconds * 1000;
      long endTimeMillis = System.currentTimeMillis() + cnxnTimeoutSec * 1000;
      IMetaStoreClient hiveClient = null;
      while (true) {
        try {
          hiveClient = RetryingMetaStoreClient.getProxy(hiveConf, dummyHookLoader,
              HiveMetaStoreClient.class.getName());
          break;
        } catch (Exception e) {
          // If time is up, throw an unchecked exception
          long delayUntilMillis = System.currentTimeMillis() + retryDelayMillis;
          if (delayUntilMillis >= endTimeMillis) {
            throw new MetastoreClientInstantiationException(e);
          }

          LOG.warn("Failed to connect to Hive MetaStore. Retrying.", e);
          while (delayUntilMillis > System.currentTimeMillis()) {
            try {
              Thread.sleep(delayUntilMillis - System.currentTimeMillis());
            } catch (InterruptedException | IllegalArgumentException ignore) {}
          }
        }
      }
      hiveClient_ = hiveClient;
      isInUse_ = false;
    }

    /**
     * Returns the internal RetryingMetaStoreClient object.
     */
    public IMetaStoreClient getHiveClient() {
      return hiveClient_;
    }

    /**
     * Returns this client back to the connection pool. If the connection pool has been
     * closed, just close the Hive client connection.
     */
    @Override
    public void close() {
      Preconditions.checkState(isInUse_);
      isInUse_ = false;
      numHmsClientsInUse_.decrementAndGet();
      // Ensure the connection isn't returned to the pool if the pool has been closed
      // or if the number of connections in the pool exceeds MAX_HMS_CONNECTION_POOL_SIZE.
      // This lock is needed to ensure proper behavior when a thread reads poolClosed
      // is false, but a call to pool.close() comes in immediately afterward.
      synchronized (poolCloseLock_) {
        if (poolClosed_ || clientPool_.size() >= MAX_HMS_CONNECTION_POOL_SIZE) {
          hiveClient_.close();
        } else {
          clientPool_.offer(this);
        }
      }
    }

    // Marks this client as in use
    private void markInUse() {
      Preconditions.checkState(!isInUse_);
      isInUse_ = true;
      numHmsClientsInUse_.incrementAndGet();
    }
  }

  public MetaStoreClientPool(int initialSize, int initialCnxnTimeoutSec) {
    this(initialSize, initialCnxnTimeoutSec, new HiveConf(MetaStoreClientPool.class));
  }

  public MetaStoreClientPool(int initialSize, int initialCnxnTimeoutSec,
      HiveConf hiveConf) {
    hiveConf_ = hiveConf;
    clientCreationDelayMs_ = hiveConf_.getInt(HIVE_METASTORE_CNXN_DELAY_MS_CONF,
        DEFAULT_HIVE_METASTORE_CNXN_DELAY_MS_CONF);
    initClients(initialSize, initialCnxnTimeoutSec);
  }

  /**
   * Initialize client pool with 'numClients' client.
   * 'initialCnxnTimeoutSec' specifies the time (in seconds) the first client will wait to
   * establish an initial connection to the HMS.
   */
  public void initClients(int numClients, int initialCnxnTimeoutSec) {
    Preconditions.checkState(clientPool_.size() == 0);
    if (numClients > 0) {
      clientPool_.add(new MetaStoreClient(hiveConf_, initialCnxnTimeoutSec));
      for (int i = 0; i < numClients - 1; ++i) {
        clientPool_.add(new MetaStoreClient(hiveConf_, 0));
      }
    }
  }

  /**
   * Gets a client from the pool. If the pool is empty a new client is created.
   */
  public MetaStoreClient getClient() {
    // The MetaStoreClient c'tor relies on knowing the Hadoop version by asking
    // org.apache.hadoop.util.VersionInfo. The VersionInfo class relies on opening
    // the 'common-version-info.properties' file as a resource from hadoop-common*.jar
    // using the Thread's context classloader. If necessary, set the Thread's context
    // classloader, otherwise VersionInfo will fail in it's c'tor.
    if (Thread.currentThread().getContextClassLoader() == null) {
      Thread.currentThread().setContextClassLoader(ClassLoader.getSystemClassLoader());
    }

    MetaStoreClient client = clientPool_.poll();
    // The pool was empty so create a new client and return that.
    // Serialize client creation to defend against possible race conditions accessing
    // local Kerberos state (see IMPALA-825).
    if (client == null) {
      synchronized (this) {
        try {
          Thread.sleep(clientCreationDelayMs_);
        } catch (InterruptedException e) {
          /* ignore */
        }
        client = new MetaStoreClient(hiveConf_, 0);
      }
    }
    client.markInUse();
    return client;
  }

  /**
   * Removes all items from the connection pool and closes all Hive Meta Store client
   * connections. Can be called multiple times.
   */
  public void close() {
    // Ensure no more items get added to the pool once close is called.
    synchronized (poolCloseLock_) {
      if (poolClosed_) { return; }
      poolClosed_ = true;
    }

    MetaStoreClient client = null;
    while ((client = clientPool_.poll()) != null) {
      client.getHiveClient().close();
    }
  }

  public int getNumHmsClientsIdle() { return clientPool_.size(); }
  public int getNumHmsClientsInUse() { return numHmsClientsInUse_.get(); }
}
