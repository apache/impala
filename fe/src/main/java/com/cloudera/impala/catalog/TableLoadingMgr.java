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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.log4j.Logger;

import com.cloudera.impala.thrift.TTableName;

/**
* Class that manages scheduling the loading of table metadata from the Hive Metastore and
* the Hadoop NameNode. Loads tables using a pool of table loading threads. Each thread
* reads from a deque of table names to determine which table to load next. Tables added
* to the head of the deque will be loaded before tables added to the tail, so the loading
* order can be prioritized (see prioritizeLoad()/backgroundLoad()).
*/
public class TableLoadingMgr {
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

  // The number of parallel threads to use to load table metadata. Should be set to a
  // value that provides good throughput while not putting too much stress on the
  // metastore.
  private final int numLoadingThreads_;

  private final CatalogServiceCatalog catalog_;

  public TableLoadingMgr(CatalogServiceCatalog catalog, int numLoadingThreads) {
    catalog_ = catalog;
    numLoadingThreads_ = numLoadingThreads;

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

    Db db = catalog_.getDb(tblName.getDb_name());
    if (db == null) return;

    // Get the table, which will load the table's metadata if needed.
    db.getTable(tblName.getTable_name());
  }
}