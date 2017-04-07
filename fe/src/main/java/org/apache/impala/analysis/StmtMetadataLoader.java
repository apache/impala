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

package org.apache.impala.analysis;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.ImpaladCatalog;
import org.apache.impala.catalog.Table;
import org.apache.impala.catalog.View;
import org.apache.impala.common.InternalException;
import org.apache.impala.service.Frontend;
import org.apache.impala.util.EventSequence;
import org.apache.impala.util.TUniqueIdUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Loads all table and view metadata relevant for a single SQL statement and returns the
 * loaded tables in a StmtTableCache. Optionally marks important loading events in an
 * EventSequence.
 */
public class StmtMetadataLoader {
  private final static Logger LOG = LoggerFactory.getLogger(StmtMetadataLoader.class);

  // Events are triggered when at least the set number of catalog updates have passed.
  private final long DEBUG_LOGGING_NUM_CATALOG_UPDATES = 10;
  private final long RETRY_LOAD_NUM_CATALOG_UPDATES = 20;

  private final Frontend fe_;
  private final String sessionDb_;
  private final EventSequence timeline_;

  // Results of the loading process. See StmtTableCache.
  private final Set<String> dbs_ = Sets.newHashSet();
  private final Map<TableName, Table> loadedTbls_ = Maps.newHashMap();

  // Metrics for the metadata load.
  // Number of prioritizedLoad() RPCs issued to the catalogd.
  private int numLoadRequestsSent_ = 0;
  // Number of catalog topic updates received from the statestore.
  private int numCatalogUpdatesReceived_ = 0;

  /**
   * Contains all statement-relevant tables and database names as well as the latest
   * ImpaladCatalog. An entry in the tables map is guaranteed to point to a loaded
   * table. This could mean the table was loaded successfully or a load was attempted
   * but failed. The absence of a table or database name indicates that object was not
   * in the Catalog at the time this StmtTableCache was generated.
   */
  public static final class StmtTableCache {
    public final ImpaladCatalog catalog;
    public final Set<String> dbs;
    public final Map<TableName, Table> tables;

    public StmtTableCache(ImpaladCatalog catalog, Set<String> dbs,
        Map<TableName, Table> tables) {
      this.catalog = Preconditions.checkNotNull(catalog);
      this.dbs = Preconditions.checkNotNull(dbs);
      this.tables = Preconditions.checkNotNull(tables);
      validate();
    }

    private void validate() {
      // Checks that all entries in 'tables' have a matching entry in 'dbs'.
      for (TableName tbl: tables.keySet()) {
        Preconditions.checkState(dbs.contains(tbl.getDb()));
      }
    }
  }

  /**
   * The 'fe' and 'sessionDb' arguments must be non-null. A null 'timeline' may be passed
   * if no events should be marked.
   */
  public StmtMetadataLoader(Frontend fe, String sessionDb, EventSequence timeline) {
    fe_ = Preconditions.checkNotNull(fe);
    sessionDb_ = Preconditions.checkNotNull(sessionDb);
    timeline_ = timeline;
  }

  // Getters for testing
  public EventSequence getTimeline() { return timeline_; }
  public int getNumLoadRequestsSent() { return numLoadRequestsSent_; }
  public int getNumCatalogUpdatesReceived() { return numCatalogUpdatesReceived_; }

  /**
   * Collects and loads all tables and views required to analyze the given statement.
   * Marks the start and end of metadata loading in 'timeline_' if it is non-NULL.
   * Must only be called once for a single statement.
   */
  public StmtTableCache loadTables(StatementBase stmt) throws InternalException {
    Set<TableName> requiredTables = collectTableCandidates(stmt);
    return loadTables(requiredTables);
  }

  /**
   * Loads the tables/views with the given names and returns them. As views become
   * loaded, the set of table/views still to be loaded is expanded based on the view
   * definitions. For tables/views missing metadata this function issues a loading
   * request to the catalog server and then waits for the metadata to arrive through
   * a statestore topic update.
   * This function succeeds even across catalog restarts for the following reasons:
   * - The loading process is strictly additive, i.e., a new loaded table may be added
   *   to the 'loadedTbls_' map, but an existing entry is never removed, even if the
   *   equivalent table in the impalad catalog is different.
   * - Tables on the impalad side are not modified in place. This means that an entry in
   *   the 'loadedTbls_' will always remain in the loaded state.
   * Tables/views that are already loaded are simply included in the result.
   * Marks the start and end of metadata loading in 'timeline_' if it is non-NULL.
   * Must only be called once for a single statement.
   */
  public StmtTableCache loadTables(Set<TableName> tbls) throws InternalException {
    Preconditions.checkState(dbs_.isEmpty() && loadedTbls_.isEmpty());
    Preconditions.checkState(numLoadRequestsSent_ == 0);
    Preconditions.checkState(numCatalogUpdatesReceived_ == 0);
    ImpaladCatalog catalog = fe_.getCatalog();
    Set<TableName> missingTbls = getMissingTables(catalog, tbls);
    // There are no missing tables. Return to avoid making an RPC to the CatalogServer
    // and adding events to the timeline.
    if (missingTbls.isEmpty()) {
      if (timeline_ != null) {
        timeline_.markEvent(
            String.format("Metadata of all %d tables cached", loadedTbls_.size()));
      }
      return new StmtTableCache(catalog, dbs_, loadedTbls_);
    }

    if (timeline_ != null) timeline_.markEvent("Metadata load started");
    long startTimeMs = System.currentTimeMillis();

    // All tables for which we have requested a prioritized load.
    Set<TableName> requestedTbls = Sets.newHashSet();

    // Loading a fixed set of tables happens in two steps:
    // 1) Issue a loading request RPC to the catalogd.
    // 2) Wait for the loaded tables to arrive via the statestore.
    // The second step could take a while and we should avoid repeatedly issuing
    // redundant RPCs to the catalogd. This flag indicates whether a loading RPC
    // should be issued. See below for more details in which circumstances this
    // flag is set to true.
    boolean issueLoadRequest = true;
    // Loop until all the missing tables are loaded in the Impalad's catalog cache.
    // In every iteration of this loop we wait for one catalog update to arrive.
    while (!missingTbls.isEmpty()) {
      if (issueLoadRequest) {
        catalog.prioritizeLoad(missingTbls);
        ++numLoadRequestsSent_;
        requestedTbls.addAll(missingTbls);
      }

      // Catalog may have been restarted, always use the latest reference.
      ImpaladCatalog currCatalog = fe_.getCatalog();
      boolean hasCatalogRestarted = currCatalog != catalog;
      if (hasCatalogRestarted && LOG.isWarnEnabled()) {
        LOG.warn(String.format(
            "Catalog restart detected while waiting for table metadata. " +
            "Current catalog service id: %s. Previous catalog service id: %s",
            TUniqueIdUtil.PrintId(currCatalog.getCatalogServiceId()),
            TUniqueIdUtil.PrintId(catalog.getCatalogServiceId())));

      }
      catalog = currCatalog;

      // Log progress and wait time for debugging.
      if (hasCatalogRestarted
          || (numCatalogUpdatesReceived_ > 0
              && numCatalogUpdatesReceived_ % DEBUG_LOGGING_NUM_CATALOG_UPDATES == 0)) {
        if (LOG.isInfoEnabled()) {
          long endTimeMs = System.currentTimeMillis();
          LOG.info(String.format("Waiting for table metadata. " +
              "Waited for %d catalog updates and %dms. Tables remaining: %s",
              numCatalogUpdatesReceived_, endTimeMs - startTimeMs, missingTbls));
        }
      }

      // Wait for the next catalog update and then revise the loaded/missing tables.
      catalog.waitForCatalogUpdate(Frontend.MAX_CATALOG_UPDATE_WAIT_TIME_MS);
      Set<TableName> newMissingTbls = getMissingTables(catalog, missingTbls);
      // Issue a load request for the new missing tables in these cases:
      // 1) Catalog has restarted so all in-flight loads have been lost
      // 2) There are new missing tables due to view expansion
      issueLoadRequest = hasCatalogRestarted || !missingTbls.containsAll(newMissingTbls);
      // 3) Periodically retry to avoid a hang due to anomalies/bugs, e.g.,
      //    a previous load request was somehow lost on the catalog side, or the table
      //    was invalidated after being loaded but before being sent to this impalad
      if (!issueLoadRequest && numCatalogUpdatesReceived_ > 0
          && numCatalogUpdatesReceived_ % RETRY_LOAD_NUM_CATALOG_UPDATES == 0) {
        issueLoadRequest = true;
        if (LOG.isInfoEnabled()) {
          long endTimeMs = System.currentTimeMillis();
          LOG.info(String.format("Re-sending prioritized load request. " +
              "Waited for %d catalog updates and %dms.",
              numCatalogUpdatesReceived_, endTimeMs - startTimeMs));
        }
      }
      missingTbls = newMissingTbls;
      ++numCatalogUpdatesReceived_;
    }
    if (timeline_ != null) {
      timeline_.markEvent(String.format("Metadata load finished. " +
          "loaded-tables=%d/%d load-requests=%d catalog-updates=%d",
          requestedTbls.size(), loadedTbls_.size(), numLoadRequestsSent_,
          numCatalogUpdatesReceived_));
    }

    return new StmtTableCache(catalog, dbs_, loadedTbls_);
  }

  /**
   * Determines whether the 'tbls' are loaded in the given catalog or not. Adds the names
   * of referenced databases that exist to 'dbs_', and loaded tables to 'loadedTbls_'.
   * Returns the set of tables that are not loaded. Recursively collects loaded/missing
   * tables from views. Uses 'sessionDb_' to construct table candidates from views with
   * Path.getCandidateTables(). Non-existent tables are ignored and not returned or
   * added to 'loadedTbls_'.
   */
  private Set<TableName> getMissingTables(ImpaladCatalog catalog, Set<TableName> tbls) {
    Set<TableName> missingTbls = Sets.newHashSet();
    Set<TableName> viewTbls = Sets.newHashSet();
    for (TableName tblName: tbls) {
      if (loadedTbls_.containsKey(tblName)) continue;
      Db db = catalog.getDb(tblName.getDb());
      if (db == null) continue;
      dbs_.add(tblName.getDb());
      Table tbl = db.getTable(tblName.getTbl());
      if (tbl == null) continue;
      if (!tbl.isLoaded()) {
        missingTbls.add(tblName);
        continue;
      }
      loadedTbls_.put(tblName, tbl);
      if (tbl instanceof View) {
        viewTbls.addAll(collectTableCandidates(((View) tbl).getQueryStmt()));
      }
    }
    // Recursively collect loaded/missing tables from loaded views.
    if (!viewTbls.isEmpty()) missingTbls.addAll(getMissingTables(catalog, viewTbls));
    return missingTbls;
  }

  /**
   * Returns the set of tables whose metadata needs to be loaded for the analysis of the
   * given 'stmt' to succeed. This is done by collecting all table references from 'stmt'
   * and generating all possible table-path resolutions considered during analysis.
   * Uses 'sessionDb_' to construct the candidate tables with Path.getCandidateTables().
   */
  private Set<TableName> collectTableCandidates(StatementBase stmt) {
    Preconditions.checkNotNull(stmt);
    List<TableRef> tblRefs = Lists.newArrayList();
    stmt.collectTableRefs(tblRefs);
    Set<TableName> tableNames = Sets.newHashSet();
    for (TableRef ref: tblRefs) {
      tableNames.addAll(Path.getCandidateTables(ref.getPath(), sessionDb_));
    }
    return tableNames;
  }
}
