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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.impala.common.Pair;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.util.EventSequence;
import org.apache.impala.util.HdfsCachingUtil;
import org.apache.impala.util.NoOpEventSequence;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
* Class that manages scheduling the loading of table metadata from the Hive Metastore and
* the Hadoop NameNode. Loads tables using a pool of table loading threads. New load
* requests can be submitted using loadAsync(), which will schedule the load when the
* next thread becomes available.  Also manages prioritized background table loading by
* reading from a deque of table names to determine which table to load next. Tables added
* to the head of the deque will be loaded before tables added to the tail, so the loading
* order can be prioritized (see prioritizeLoad()/backgroundLoad()).
*/
public class TableLoadingMgr {
  /**
   * Represents the result of an asynchronous Table loading request. Calling
   * get() will block until the Table has completed loading. When finished
   * processing the request, call close() to clean up.
   */
  public class LoadRequest {
    private final Future<Table> tblTask_;
    private final TTableName tblName_;

    private LoadRequest(TTableName tblName, Future<Table> tblTask) {
      tblTask_ = tblTask;
      tblName_ = tblName;
    }

    /**
     * Blocks until the table has finished loading and returns the result. If any errors
     * were encountered while loading the table an IncompleteTable will be returned.
     */
    public Table get() {
      Table tbl;
      try {
        LOG.info("Loading metadata for table: " +
            tblName_.db_name + "." + tblName_.table_name);
        LOG.info(String.format("Remaining items in queue: %s. Loads in progress: %s",
            tableLoadingDeque_.size(), loadingTables_.size()));
        tbl = tblTask_.get();
      } catch (Exception e) {
        tbl = IncompleteTable.createFailedMetadataLoadTable(
            catalog_.getDb(tblName_.getDb_name()), tblName_.getTable_name(),
            new TableLoadingException(e.getMessage(), e));
      }
      Preconditions.checkState(tbl.isLoaded());
      return tbl;
    }

    /**
     * Cleans up the in-flight load request matching the given table name. Will not
     * cancel the load if it is still in progress, frees a slot should another
     * load for the same table come in. Can be called multiple times.
     */
    public void close() {
      synchronized (loadingTables_) {
        if (loadingTables_.get(tblName_) == tblTask_) loadingTables_.remove(tblName_);
      }
    }
  }

  private static final Logger LOG = Logger.getLogger(TableLoadingMgr.class);

  // A thread safe blocking deque that is used to prioritize the loading of table
  // metadata. The CatalogServer has a background thread that will always add unloaded
  // tables to the tail of the deque. However, a call to prioritizeLoad() will add
  // tables to the head of the deque. The next table to load is always taken from the
  // head of the deque. May contain the same table multiple times, but a second
  // attempt to load the table metadata will be a no-op.
  private final LinkedBlockingDeque<TTableName> tableLoadingDeque_ =
      new LinkedBlockingDeque<TTableName>();

  // Maps from table name to a boolean indicating whether that table is currently
  // being loaded by a table loading thread. Used to prevent adding superfluous
  // entries to the deque, and to ensure that only a single table loading thread
  // is consumed per table.
  // Entries are added to this map immediately before being added to the deque and
  // removed after a load has completed.
  // Once the load of a table begins, its associated boolean is set to true, and
  // attempts to load the same table by a different thread become no-ops.
  // This map is different from loadingTables_ because the latter tracks all in-flight
  // loads - even those being processed by threads other than table loading threads.
  private final Map<TTableName, AtomicBoolean> tableLoadingBarrier_ =
      new ConcurrentHashMap<>();

  // Map of table name to a FutureTask associated with the table load. Used to
  // prevent duplicate loads of the same table.
  private final Map<TTableName, FutureTask<Table>> loadingTables_ =
      new ConcurrentHashMap<>();

  // Map of table name to the cache directives that are being waited on for that table.
  // Once all directives have completed, the table's metadata will be refreshed and
  // the table will be removed from this map.
  // A caching operation may take a long time to complete, so to maximize query
  // throughput it is preferable to allow the user to continue to run queries against
  // the table while a cache request completes in the background.
  private final Map<TTableName, List<Long>> pendingTableCacheDirs_ = new HashMap<>();

  // The number of parallel threads to use to load table metadata. Should be set to a
  // value that provides good throughput while not putting too much stress on the
  // metastore.
  private final int numLoadingThreads_;

  // Pool of numLoadingThreads_ threads that loads table metadata. If additional tasks
  // are submitted to the pool after it is full, they will be queued and executed when
  // the next thread becomes available. There is no hard upper limit on the number of
  // pending tasks (no work will be rejected, but memory consumption is unbounded).
  private final ExecutorService tblLoadingPool_;

  // Thread that incrementally refreshes tables in the background. Used to update a
  // table's metadata after a long running operation completes, such as marking a
  // table as cached. There is no hard upper limit on the number of pending tasks
  // (no work will be rejected, but memory consumption is unbounded). If this thread
  // dies it will be automatically restarted.
  // The tables to process are read from the resfreshThreadWork_ queue.
  ExecutorService asyncRefreshThread_ = Executors.newSingleThreadExecutor(
      new ThreadFactoryBuilder().setNameFormat("TableAsyncRefreshThread").build());

  // Tables for the async refresh thread to process. Synchronization must be handled
  // externally.
  private final LinkedBlockingQueue<Pair<TTableName, String>> refreshThreadWork_ =
      new LinkedBlockingQueue<>();

  private final CatalogServiceCatalog catalog_;
  private final TableLoader tblLoader_;

  public TableLoadingMgr(CatalogServiceCatalog catalog, int numLoadingThreads) {
    catalog_ = catalog;
    tblLoader_ = new TableLoader(catalog_);
    numLoadingThreads_ = numLoadingThreads;
    tblLoadingPool_ = Executors.newFixedThreadPool(numLoadingThreads_,
        new ThreadFactoryBuilder().setNameFormat("TableLoadingThread-%d").build());

    // Start the background table loading submitter threads.
    startTableLoadingSubmitterThreads();

    // Start the asyncRefreshThread_. Currently used to wait for cache directives to
    // complete in the background.
    asyncRefreshThread_.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        while(true) {
          Pair<TTableName, String> work = refreshThreadWork_.take();
          execAsyncRefreshWork(work.first, /* reason=*/work.second);
        }
      }});
  }

  /**
   * Prioritizes the loading of the given table.
   */
  public void prioritizeLoad(TTableName tblName) {
    AtomicBoolean isLoading =
        tableLoadingBarrier_.putIfAbsent(tblName, new AtomicBoolean(false));
    // Only queue the table if a load is not already in progress.
    if (isLoading != null && isLoading.get()) return;
    tableLoadingDeque_.offerFirst(tblName);
  }

  /**
   * Submits a single table for background (low priority) loading.
   */
  public void backgroundLoad(TTableName tblName) {
    // Only queue for background loading if the table isn't already queued or
    // currently being loaded.
    if (tableLoadingBarrier_.putIfAbsent(tblName, new AtomicBoolean(false)) == null) {
      tableLoadingDeque_.offerLast(tblName);
    }
  }

  /**
   * Adds a list of cache directive IDs to watch for the given table name.
   * The asyncRefreshThread_ will process the cache directives and once all directives
   * complete (data has been cached or no progress is being made), the
   * asyncRefreshThread_ will refresh the table metadata. After processing the
   * request the watch will be deleted.
   */
  public void watchCacheDirs(List<Long> cacheDirIds, final TTableName tblName,
      final String reason) {
    synchronized (pendingTableCacheDirs_) {
      // A single table may have multiple pending cache requests since one request
      // gets submitted per-partition.
      List<Long> existingCacheReqIds = pendingTableCacheDirs_.get(tblName);
      if (existingCacheReqIds == null) {
        existingCacheReqIds = cacheDirIds;
        pendingTableCacheDirs_.put(tblName, cacheDirIds);
        refreshThreadWork_.add(Pair.create(tblName, reason));
      } else {
        existingCacheReqIds.addAll(cacheDirIds);
      }
    }
  }

  /**
   * Loads a table asynchronously, returning a LoadRequest that can be used to get
   * the result (a Table). If there is already a load in flight for this table name,
   * the same underlying loading task (Future) will be used, helping to prevent duplicate
   * loads of the same table.
   */
  public LoadRequest loadAsync(final TTableName tblName, final long createdEventId,
      final String reason, final EventSequence catalogTimeline)
      throws DatabaseNotFoundException {
    final Db parentDb = catalog_.getDb(tblName.getDb_name());
    if (parentDb == null) {
      throw new DatabaseNotFoundException(
          "Database '" + tblName.getDb_name() + "' was not found.");
    }

    FutureTask<Table> tableLoadTask = new FutureTask<Table>(new Callable<Table>() {
        @Override
        public Table call() throws Exception {
          catalogTimeline.markEvent("Start loading table");
          return tblLoader_.load(parentDb, tblName.table_name, createdEventId, reason,
              catalogTimeline);
        }});

    FutureTask<Table> existingValue = loadingTables_.putIfAbsent(tblName, tableLoadTask);
    if (existingValue == null) {
      // There was no existing value, submit a new load request.
      tblLoadingPool_.execute(tableLoadTask);
    } else {
      tableLoadTask = existingValue;
    }
    return new LoadRequest(tblName, tableLoadTask);
  }

  /**
   * Starts table loading submitter threads in a fixed sized thread pool with a size
   * defined by NUM_TBL_LOADING_THREADS. Each thread polls the tableLoadingDeque_
   * for new tables to load. Note these threads are just for submitting the
   * load request, the real table loading threads are in tblLoadingPool_.
   * There is a discussion here: https://issues.apache.org/jira/browse/IMPALA-9140
   * which well explained the table loading mechanism.
   */
  private void startTableLoadingSubmitterThreads() {
    ExecutorService submitterLoadingPool =
      Executors.newFixedThreadPool(numLoadingThreads_,
          new ThreadFactoryBuilder()
              .setNameFormat("TableLoadingSubmitterThread-%d").build());
    try {
      for (int i = 0; i < numLoadingThreads_; ++i) {
        submitterLoadingPool.execute(new Runnable() {
          @Override
          public void run() {
            while (true) {
              try {
                loadNextTable();
              } catch (Exception e) {
                LOG.error("Error loading table: ", e);
                // Ignore exception.
              }
            }
          }
        });
      }
    } finally {
      submitterLoadingPool.shutdown();
    }
  }

  /**
   * Gets the next table name to load off the head of the table loading queue. If
   * the queue is empty, this will block until a new table is added.
   */
  private void loadNextTable() throws InterruptedException {
    // Always get the next table from the head of the deque.
    final TTableName tblName = tableLoadingDeque_.takeFirst();
    AtomicBoolean isLoading = tableLoadingBarrier_.get(tblName);
    if (isLoading == null || !isLoading.compareAndSet(false, true)) {
      // Another thread has already completed the load or the load is still in progress.
      // Return so this thread can work on another table in the queue.
      LOG.info("Metadata load request already in progress for table: " +
          tblName.db_name + "." + tblName.table_name);
      return;
    }
    try {
      // TODO: Instead of calling "getOrLoad" here we could call "loadAsync". We would
      // just need to add a mechanism for moving loaded tables into the Catalog.
      catalog_.getOrLoadTable(tblName.getDb_name(), tblName.getTable_name(),
          "background load", null);
    } catch (CatalogException e) {
      // Ignore.
    } finally {
      tableLoadingBarrier_.remove(tblName);
    }
  }

  /**
   * Reloads the metadata of the given table to pick up the new cached block location
   * information. Only reloads the metadata if the table is already loaded. The rationale
   * is that if the metadata has not been loaded yet, then it needs to be reloaded
   * anyway, and if the table failed to load, then we do not want to hide errors by
   * reloading it 'silently' in response to the completion of an HDFS caching request.
   */
  private void execAsyncRefreshWork(TTableName tblName, String reason) {
    if (!waitForCacheDirs(tblName)) return;
    try {
      Table tbl = catalog_.getTable(tblName.getDb_name(), tblName.getTable_name());
      if (tbl == null || tbl instanceof IncompleteTable || !tbl.isLoaded()) return;
      catalog_.reloadTable(tbl, reason, NoOpEventSequence.INSTANCE);
    } catch (CatalogException e) {
      LOG.error("Error reloading cached table: ", e);
    }
  }

  /**
   * Waits for all pending cache directives on a table to complete.
   * Returns true if a refresh is needed and false if a refresh is not needed.
   */
  private boolean waitForCacheDirs(TTableName tblName) {
    boolean isRefreshNeeded = false;
    // Keep processing cache directives for this table until there are none left.
    while (true) {
      // Get all pending requests for this table.
      List<Long> cacheDirIds = null;
      synchronized (pendingTableCacheDirs_) {
        cacheDirIds = pendingTableCacheDirs_.remove(tblName);
      }
      if (cacheDirIds == null || cacheDirIds.size() == 0) return isRefreshNeeded;
      isRefreshNeeded = true;

      // Wait for each cache request to complete.
      for (Long dirId: cacheDirIds) {
        if (dirId == null) continue;
        try {
          HdfsCachingUtil.waitForDirective(dirId);
        } catch (Exception e) {
          LOG.error(String.format(
              "Error waiting for cache request %d to complete: ", dirId), e);
        }
      }
    }
  }

  public int numRemainingItems() {
    return tableLoadingDeque_.size();
  }

  public int numLoadsInProgress() {
    return loadingTables_.size();
  }
}
