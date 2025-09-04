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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.common.Pair;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.util.ThreadNameAnnotator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Condition;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Manage parallel fetching of metadata from Metastore used to reset the Catalog.
 *
 * This class can either be in active state (parallel fetch is ongoing) or inactive
 * state (no fetch is ongoing). The active state is indicated by the 'fetchingDbs_'
 * queue not being empty. When in active state, Catalog operation that want to look up for
 * any database must wait until the fetch task for that database is done and polled out of
 * CatalogResetManager by Catalog.
 *
 * Catalog should call beginFetch() to start the parallel metadata fetch. It then follows
 * up by calling peekResettingDb() and pollResettingDb() continuously until all tasks
 * polled. Must call stop() to clean up the executor service and reset the state. Most of
 * the methods in this class must be called while holding the write lock of the
 * CatalogServiceCatalog's version lock.
 */
public class CatalogResetManager {
  private static final Logger LOG = LoggerFactory.getLogger(CatalogResetManager.class);

  // Maximum number of threads to use for fetching metadata from Metastore.
  private static final int MAX_NUM_THREADS =
      BackendConfig.INSTANCE.getCatalogResetMaxThreads();

  // Maximum number of fetch tasks to submit to executor service.
  // This is intended to prevent too many fetch task from occupying memory.
  private static final int MAX_FETCH_TASK = MAX_NUM_THREADS * 2;

  // The catalog service that this reset manager is associated with.
  private final CatalogServiceCatalog catalog_;

  // Condition to wait and signal when 'versionLock_.writeLock()' is being held/release by
  // reset(). Must wait on this if 'fetchingDbs_' is not empty.
  private final Condition fetchMetadataCondition_;

  // A queue of database that undergoes metadata fetch.
  // If not empty, the elements are always in lexicographic order head to tail and should
  // not contain any blacklisted Dbs. If empty, then no reset operation is currently
  // running. Initialized as ConcurrentLinkedQueue in constructor.
  private final Queue<Pair<String, Future<PrefetchedDatabaseObjects>>> fetchingDbs_;

  // A queue of database names that are pending to be fetched.
  // The elements are always in lexicographic order head to tail and should not contain
  // any blacklisted Dbs.
  private final Queue<String> pendingDbNames_;

  // Executor service to run the metadata fetch tasks in parallel.
  private ExecutorService executorService_ = null;

  public CatalogResetManager(CatalogServiceCatalog catalog) {
    catalog_ = catalog;
    fetchMetadataCondition_ = catalog.getLock().writeLock().newCondition();
    fetchingDbs_ = new ConcurrentLinkedQueue<>();
    pendingDbNames_ = new LinkedList<>();
  }

  private boolean threadIsHoldingWriteLock() {
    return catalog_.getLock().writeLock().isHeldByCurrentThread();
  }

  /**
   * Begin a metadata fetch for the given list of database names.
   * This CatalogResetManager must not be active, and the executor service must be
   * stopped before calling this method.
   */
  protected void beginFetch(List<String> dbNames) {
    Preconditions.checkState(threadIsHoldingWriteLock());
    Preconditions.checkState(
        !isActive(), "Cannot begin reset while another reset is active.");
    Preconditions.checkState(
        executorService_ == null, "Existing executor service must be stopped first.");

    executorService_ = Executors.newFixedThreadPool(MAX_NUM_THREADS,
        new ThreadFactoryBuilder().setNameFormat("DatabaseResetMonitor-%d").build());
    dbNames.stream()
        .map(String::toLowerCase)
        .filter(dbName -> {
          boolean isBlacklisted = catalog_.isBlacklistedDbInternal(dbName);
          if (isBlacklisted) {
            LOG.info("Skipping reset for blacklisted database: " + dbName);
          }
          return !isBlacklisted;
        })
        .sorted()
        .forEachOrdered(dbName -> pendingDbNames_.add(dbName));
    scheduleNextFetch();
  }

  // Schedule the fetch task for the next database.
  private void scheduleNextFetch() {
    while (!pendingDbNames_.isEmpty() && fetchingDbs_.size() < MAX_FETCH_TASK) {
      String dbName = pendingDbNames_.poll();
      Future<PrefetchedDatabaseObjects> future =
          executorService_.submit(new MetastoreFetchTask(dbName));
      fetchingDbs_.add(Pair.create(dbName, future));
    }
  }

  /**
   * Returns True if there is an ongoing fetch operation.
   * Does not need to hold versionLock_.writeLock() to call, but the status might change
   * after method returns.
   */
  protected boolean isActive() {
    return !fetchingDbs_.isEmpty();
  }

  /**
   * Stop the metadata fetch operation.
   */
  protected void stop() {
    if (executorService_ != null) {
      executorService_.shutdown();
      executorService_ = null;
    }
    pendingDbNames_.clear();
    fetchingDbs_.clear();
  }

  /**
   * Signal all threads waiting on resetMetadataCondition_.
   * Must hold versionLock_.writeLock().
   */
  protected void signalAllWaiters() {
    Preconditions.checkState(threadIsHoldingWriteLock());
    fetchMetadataCondition_.signalAll();
  }

  /**
   * Peek the next fetching database.
   * Does not need to hold versionLock_.writeLock() to call, but it might change
   * after method return if something else call {@link #pollFetchingDb()} concurrently.
   */
  protected Pair<String, Future<PrefetchedDatabaseObjects>> peekFetchingDb() {
    return fetchingDbs_.peek();
  }

  /**
   * Poll the next fetching database and schedule the next reset task.
   * Must hold versionLock_.writeLock().
   */
  protected Pair<String, Future<PrefetchedDatabaseObjects>> pollFetchingDb() {
    Preconditions.checkState(threadIsHoldingWriteLock());
    Pair<String, Future<PrefetchedDatabaseObjects>> pair = fetchingDbs_.poll();
    scheduleNextFetch();
    if (fetchingDbs_.isEmpty()) stop();
    return pair;
  }

  /**
   * Return a list of all currently resetting databases.
   * Must hold versionLock_.writeLock().
   */
  protected List<String> allFetcingDbList() {
    Preconditions.checkState(threadIsHoldingWriteLock());
    return Stream
        .concat(fetchingDbs_.stream().map(Pair::getFirst), pendingDbNames_.stream())
        .collect(Collectors.toList());
  }

  /**
   * Wait until all parallel fetch finish.
   * Must hold versionLock_.writeLock().
   */
  protected void waitFullMetadataFetch() {
    Preconditions.checkState(threadIsHoldingWriteLock());
    while (isActive()) {
      try {
        fetchMetadataCondition_.await();
      } catch (InterruptedException ex) {
        // IMPALA-915: Handle this properly if we support cancel query during frontend
        // compilation. For now, maintain current behavior (block everything during
        // INVALIDATE METADATA) by ignoring and continue waiting.
        // fetchingDbs_ will eventually be cleared.
      }
    }
  }

  /**
   * Wait until it is ensured that given 'dbName' has been polled out.
   * This method will lower case 'dbName' for matching.
   * Must hold versionLock_.writeLock().
   */
  protected void waitOngoingMetadataFetch(String dbName) {
    waitOngoingMetadataFetch(ImmutableList.of(dbName));
  }

  /**
   * Wait until it is ensured that all 'dbNames' has been polled out.
   * This method will lower case 'dbNames' and sort them for matching.
   * Must hold versionLock_.writeLock().
   */
  protected void waitOngoingMetadataFetch(List<String> dbNames) {
    Preconditions.checkState(threadIsHoldingWriteLock());
    List<String> lowerDbNames =
        dbNames.stream().map(String::toLowerCase).sorted().collect(Collectors.toList());
    int unlockedDbs = 0;
    while (unlockedDbs < lowerDbNames.size()) {
      String lowerDbName = lowerDbNames.get(unlockedDbs);
      boolean hasWait = false;
      while (isPendingFetch(lowerDbName)) {
        if (!hasWait) {
          LOG.info("Waiting metadata reset for database " + lowerDbName);
          hasWait = true;
        }
        try {
          fetchMetadataCondition_.await();
        } catch (InterruptedException ex) {
          // IMPALA-915: Handle this properly if we support cancel query during frontend
          // compilation. For now, maintain current behavior (block everything during
          // INVALIDATE METADATA) by ignoring and continue waiting.
          // fetchingDbs_ will eventually be cleared.
        }
      }
      if (hasWait && lowerDbNames.size() > 1) {
        // Back to first Db to ensure that none of 'dbNames' are ever under invalidation.
        unlockedDbs = 0;
      } else {
        // Only advance to next Db if not wait in this iteration.
        unlockedDbs++;
      }
    }
  }

  private String dbNameAtFetchQueueHead() {
    if (fetchingDbs_.isEmpty()) return null;
    Pair<String, Future<PrefetchedDatabaseObjects>> pair = fetchingDbs_.peek();
    if (pair == null) return null;
    return pair.first;
  }

  /**
   * Return True if given lowerCaseDbName is currently in fetch queue or pending queue.
   * Must hold versionLock_.writeLock() and lowerCaseDbName must be in lower case.
   */
  protected boolean isPendingFetch(String lowerCaseDbName) {
    Preconditions.checkState(threadIsHoldingWriteLock());
    String fetchingDbHead = dbNameAtFetchQueueHead();
    return fetchingDbHead != null && lowerCaseDbName.compareTo(fetchingDbHead) >= 0;
  }

  private class MetastoreFetchTask implements Callable<PrefetchedDatabaseObjects> {
    private final String dbName_;

    public MetastoreFetchTask(String dbName) { this.dbName_ = dbName; }

    @Override
    public PrefetchedDatabaseObjects call() throws Exception {
      long startTime = System.currentTimeMillis();
      try (MetaStoreClient msClient = catalog_.getMetaStoreClient();
           ThreadNameAnnotator tna =
               new ThreadNameAnnotator(String.format("Prefetching %s db", dbName_));) {
        // Fetch the database, functions, and table metadata from HMS.
        Database msDb = msClient.getHiveClient().getDatabase(dbName_);
        List<Function> javaFunctions = new ArrayList<>();
        for (String javaFn : msClient.getHiveClient().getFunctions(dbName_, "*")) {
          javaFunctions.add(msClient.getHiveClient().getFunction(dbName_, javaFn));
        }
        List<TableMeta> tableMetas = CatalogServiceCatalog.getTableMetaFromHive(
            msClient, dbName_, /*tblName*/ null);
        long duration = System.currentTimeMillis() - startTime;
        return new PrefetchedDatabaseObjects(msDb, tableMetas,
            catalog_.extractNativeImpalaFunctions(msDb),
            catalog_.extractJavaFunctions(javaFunctions), duration);
      }
    }
  }

  public static class PrefetchedDatabaseObjects {
    private final Database msDb_;
    private final List<TableMeta> tableMetas_;
    private final List<org.apache.impala.catalog.Function> nativeFunctions_;
    private final List<org.apache.impala.catalog.Function> javaFunctions_;
    private final long durationMs_;

    public PrefetchedDatabaseObjects(Database msDb, List<TableMeta> tableMetas,
        List<org.apache.impala.catalog.Function> nativeFunctions,
        List<org.apache.impala.catalog.Function> javaFunctions, long durationMs) {
      this.msDb_ = msDb;
      this.nativeFunctions_ = nativeFunctions;
      this.javaFunctions_ = javaFunctions;
      this.tableMetas_ = tableMetas;
      this.durationMs_ = durationMs;
    }

    public Database getMsDb() { return msDb_; }

    public List<org.apache.impala.catalog.Function> getNativeFunctions() {
      return nativeFunctions_;
    }

    public List<org.apache.impala.catalog.Function> getJavaFunctions() {
      return javaFunctions_;
    }

    public List<TableMeta> getTableMetas() { return tableMetas_; }

    public long getDurationMs() { return durationMs_; }
  }
}
