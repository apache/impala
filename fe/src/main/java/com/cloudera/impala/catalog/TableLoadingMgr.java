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

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import com.cloudera.impala.thrift.TTableName;
import com.cloudera.impala.util.HdfsCachingUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

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
        tbl = tblTask_.get();
      } catch (Exception e) {
        tbl = IncompleteTable.createFailedMetadataLoadTable(
            TableId.createInvalidId(), catalog_.getDb(tblName_.getDb_name()),
            tblName_.getTable_name(), new TableLoadingException(e.getMessage(), e));
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

  // A thread safe HashSet of table names that are in the tableLoadingDeque_. Used to
  // efficiently check for existence of items in the deque.
  // Updates may lead/lag updates to the tableLoadingDeque_ - they are added to this set
  // immediately before being added to the deque and removed immediately after removing
  // from the deque. The fact the updates are not synchronized shouldn't impact
  // functionality since this set is only used for efficient lookups.
  private final Set<TTableName> tableLoadingSet_ =
      Collections.synchronizedSet(new HashSet<TTableName>());

  // Map of table name to a FutureTask associated with the table load. Used to
  // prevent duplicate loads of the same table.
  private final ConcurrentHashMap<TTableName, FutureTask<Table>> loadingTables_ =
      new ConcurrentHashMap<TTableName, FutureTask<Table>>();

  // Map of table name to the cache directives that are being waited on for that table.
  // Once all directives have completed, the table's metadata will be refreshed and
  // the table will be removed from this map.
  // A caching operation may take a long time to complete, so to maximize query
  // throughput it is preferable to allow the user to continue to run queries against
  // the table while a cache request completes in the background.
  private final Map<TTableName, List<Long>> pendingTableCacheDirs_ = Maps.newHashMap();

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
  ExecutorService asyncRefreshThread_ = Executors.newSingleThreadExecutor();

  // Tables for the async refresh thread to process. Synchronization must be handled
  // externally.
  private final LinkedBlockingQueue<TTableName> refreshThreadWork_ =
      new LinkedBlockingQueue<TTableName>();

  private final CatalogServiceCatalog catalog_;
  private final TableLoader tblLoader_;

  public TableLoadingMgr(CatalogServiceCatalog catalog, int numLoadingThreads) {
    catalog_ = catalog;
    tblLoader_ = new TableLoader(catalog_);
    numLoadingThreads_ = numLoadingThreads;
    tblLoadingPool_ = Executors.newFixedThreadPool(numLoadingThreads_);

    // Start the background table loading threads.
    startTableLoadingThreads();

    // Start the asyncRefreshThread_. Currently used to wait for cache directives to
    // complete in the background.
    asyncRefreshThread_.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        while(true) {
          execAsyncRefreshWork(refreshThreadWork_.take());
        }
      }});
  }

  /**
   * Prioritizes the loading of the given table.
   */
  public void prioritizeLoad(TTableName tblName) {
    tableLoadingSet_.add(tblName);
    tableLoadingDeque_.offerFirst(tblName);
  }

  /**
   * Submits a single table for background (low priority) loading.
   */
  public void backgroundLoad(TTableName tblName) {
    // Only queue for background loading if the table doesn't already exist
    // in the table loading set.
    if (tableLoadingSet_.add(tblName)) {
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
  public void watchCacheDirs(List<Long> cacheDirIds, final TTableName tblName) {
    synchronized (pendingTableCacheDirs_) {
      // A single table may have multiple pending cache requests since one request
      // gets submitted per-partition.
      List<Long> existingCacheReqIds = pendingTableCacheDirs_.get(tblName);
      if (existingCacheReqIds == null) {
        existingCacheReqIds = cacheDirIds;
        pendingTableCacheDirs_.put(tblName, cacheDirIds);
        refreshThreadWork_.add(tblName);
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
  public LoadRequest loadAsync(final TTableName tblName)
      throws DatabaseNotFoundException {
    final Db parentDb = catalog_.getDb(tblName.getDb_name());
    if (parentDb == null) {
      throw new DatabaseNotFoundException(
          "Database '" + tblName.getDb_name() + "' was not found.");
    }

    FutureTask<Table> tableLoadTask = new FutureTask<Table>(new Callable<Table>() {
        @Override
        public Table call() throws Exception {
          return tblLoader_.load(parentDb, tblName.table_name);
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
   * Starts table loading threads in a fixed sized thread pool with a size
   * defined by NUM_TBL_LOADING_THREADS. Each thread polls the tableLoadingDeque_
   * for new tables to load.
   */
  private void startTableLoadingThreads() {
    ExecutorService loadingPool = Executors.newFixedThreadPool(numLoadingThreads_);
    try {
      for (int i = 0; i < numLoadingThreads_; ++i) {
        loadingPool.execute(new Runnable() {
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
      loadingPool.shutdown();
    }
  }

  /**
   * Gets the next table name to load off the head of the table loading queue. If
   * the queue is empty, this will block until a new table is added.
   */
  private void loadNextTable() throws InterruptedException {
    // Always get the next table from the head of the deque.
    final TTableName tblName = tableLoadingDeque_.takeFirst();
    tableLoadingSet_.remove(tblName);
    LOG.debug("Loading next table. Remaining items in queue: "
        + tableLoadingDeque_.size());
    try {
      // TODO: Instead of calling "getOrLoad" here we could call "loadAsync". We would
      // just need to add a mechanism for moving loaded tables into the Catalog.
      catalog_.getOrLoadTable(tblName.getDb_name(), tblName.getTable_name());
    } catch (CatalogException e) {
      // Ignore.
    }
  }

  /**
   * Executes all async refresh work for the specified table name.
   */
  private void execAsyncRefreshWork(TTableName tblName) {
    if (!waitForCacheDirs(tblName)) return;
    try {
      // Reload the table metadata to pickup the new cached block location information.
      catalog_.reloadTable(tblName);
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
}
