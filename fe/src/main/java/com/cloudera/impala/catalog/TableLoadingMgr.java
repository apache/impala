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
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.log4j.Logger;

import com.cloudera.impala.thrift.TTableName;
import com.google.common.base.Preconditions;

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

  // The number of parallel threads to use to load table metadata. Should be set to a
  // value that provides good throughput while not putting too much stress on the
  // metastore.
  private final int numLoadingThreads_;

  // Pool of numLoadingThreads_ threads that loads table metadata. If additional tasks
  // are submitted to the pool after it is full, they will be queued and executed when
  // the next thread becomes available. There is no hard upper limit on the number of
  // pending tasks (no work will be rejected, but memory consumption is unbounded).
  ExecutorService tblLoadingPool_;

  private final CatalogServiceCatalog catalog_;
  private final TableLoader tblLoader_;

  public TableLoadingMgr(CatalogServiceCatalog catalog, int numLoadingThreads) {
    catalog_ = catalog;
    tblLoader_ = new TableLoader(catalog_);
    numLoadingThreads_ = numLoadingThreads;
    tblLoadingPool_ = Executors.newFixedThreadPool(numLoadingThreads_);

    // Start the background table loading threads.
    startTableLoadingThreads();
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
   * Loads a table asynchronously, returning a LoadRequest that can be used to get
   * the result (a Table). If there is already a load in flight for this table name,
   * the same underlying loading task (Future) will be used, helping to prevent duplicate
   * loads of the same table.
   * Can also be used to perform an incremental refresh of an existing table, by passing
   * the previous Table value in previousTbl. This may speedup the loading process, but
   * may return a stale object.
   */
  public LoadRequest loadAsync(final TTableName tblName, final Table previousTbl)
      throws DatabaseNotFoundException {
    final Db parentDb = catalog_.getDb(tblName.getDb_name());
    if (parentDb == null) {
      throw new DatabaseNotFoundException(
          "Database '" + tblName.getDb_name() + "' was not found.");
    }

    FutureTask<Table> tableLoadTask = new FutureTask<Table>(new Callable<Table>() {
        @Override
        public Table call() throws Exception {
          return tblLoader_.load(parentDb, tblName.table_name,
              previousTbl);
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
    TTableName tblName = tableLoadingDeque_.takeFirst();
    tableLoadingSet_.remove(tblName);
    LOG.debug("Loading next table. Remaining items in queue: "
        + tableLoadingDeque_.size());
    try {
      // TODO: Instead of calling "getOrLoad" here we could call "loadAsync". We would
      // just need to add a mechanism for moving loaded tables into the Catalog.
      catalog_.getOrLoadTable(tblName.getDb_name(), tblName.getTable_name());
    } catch (DatabaseNotFoundException e) {
      // Ignore.
    }
  }
}
