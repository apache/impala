// Copyright 2012 Cloudera Inc.
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

package com.cloudera.impala.catalog;

import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

/**
 * Manages a pool of HiveMetaStoreClient connections. If the connection pool is empty
 * a new client is created and added to the pool. There is no size limit.
 */
public class MetaStoreClientPool {
  // Key for config option read from hive-site.xml
  private static final String HIVE_METASTORE_CNXN_DELAY_MS_CONF =
      "impala.catalog.metastore.cnxn.creation.delay.ms";
  private static final int DEFAULT_HIVE_METASTORE_CNXN_DELAY_MS_CONF = 0;
  // Number of milliseconds to sleep between creation of HMS connections. Used to debug
  // IMPALA-825.
  private final int clientCreationDelayMs_;

  private static final Logger LOG = Logger.getLogger(MetaStoreClientPool.class);

  private final ConcurrentLinkedQueue<MetaStoreClient> clientPool_ =
      new ConcurrentLinkedQueue<MetaStoreClient>();
  private Boolean poolClosed_ = false;
  private final Object poolCloseLock_ = new Object();
  private final HiveConf hiveConf_;

  /**
   * A wrapper around the HiveMetaStoreClient that manages interactions with the
   * connection pool.
   */
  public class MetaStoreClient {
    private final HiveMetaStoreClient hiveClient_;
    private boolean isInUse_;

    private MetaStoreClient(HiveConf hiveConf) {
      try {
        LOG.debug("Creating MetaStoreClient. Pool Size = " + clientPool_.size());
        this.hiveClient_ = new HiveMetaStoreClient(hiveConf);
      } catch (Exception e) {
        // Turn in to an unchecked exception
        throw new IllegalStateException(e);
      }
      this.isInUse_ = false;
    }

    /**
     * Returns the internal HiveMetaStoreClient object.
     */
    public HiveMetaStoreClient getHiveClient() {
      return hiveClient_;
    }

    /**
     * Returns this client back to the connection pool. If the connection pool has been
     * closed, just close the Hive client connection.
     */
    public void release() {
      Preconditions.checkState(isInUse_);
      isInUse_ = false;
      // Ensure the connection isn't returned to the pool if the pool has been closed.
      // This lock is needed to ensure proper behavior when a thread reads poolClosed
      // is false, but a call to pool.close() comes in immediately afterward.
      synchronized (poolCloseLock_) {
        if (poolClosed_) {
          hiveClient_.close();
        } else {
          // TODO: Currently the pool does not work properly because we cannot
          // reuse MetastoreClient connections. No reason to add this client back
          // to the pool. See HIVE-5181.
          // clientPool.add(this);
          hiveClient_.close();
        }
      }
    }

    // Marks this client as in use
    private void markInUse() {
      isInUse_ = true;
    }
  }

  public MetaStoreClientPool(int initialSize) {
    this(initialSize, new HiveConf(MetaStoreClientPool.class));
  }

  public MetaStoreClientPool(int initialSize, HiveConf hiveConf) {
    this.hiveConf_ = hiveConf;
    clientCreationDelayMs_ = hiveConf_.getInt(HIVE_METASTORE_CNXN_DELAY_MS_CONF,
        DEFAULT_HIVE_METASTORE_CNXN_DELAY_MS_CONF);
    addClients(initialSize);
  }

  /**
   * Add numClients to the client pool.
   */
  public void addClients(int numClients) {
    for (int i = 0; i < numClients; ++i) {
      clientPool_.add(new MetaStoreClient(hiveConf_));
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
    synchronized (this) {
      try {
        Thread.sleep(clientCreationDelayMs_);
      } catch (InterruptedException e) {
        /* ignore */
      }
      if (client == null) {
        client = new MetaStoreClient(hiveConf_);
      } else {
        // TODO: Due to Hive Metastore bugs, there is leftover state from previous client
        // connections so we are unable to reuse the same connection. For now simply
        // reconnect each time. One possible culprit is HIVE-5181.
        client = new MetaStoreClient(hiveConf_);
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
}
