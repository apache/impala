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

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.impala.authorization.TableMask;
import org.apache.impala.authorization.User;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.FeCatalog;
import org.apache.impala.catalog.FeDb;
import org.apache.impala.catalog.FeIncompleteTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.FeView;
import org.apache.impala.catalog.MaterializedViewHdfsTable;
import org.apache.impala.catalog.Table;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.InternalException;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.service.Frontend;
import org.apache.impala.thrift.TUniqueId;
import org.apache.impala.util.AcidUtils;
import org.apache.impala.util.EventSequence;
import org.apache.impala.util.TUniqueIdUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

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
  private final User user_;
  private final TUniqueId queryId_;

  // Results of the loading process. See StmtTableCache.
  private final Set<String> dbs_ = new HashSet<>();
  private final Map<TableName, FeTable> loadedOrFailedTbls_ = new HashMap<>();

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
    public final FeCatalog catalog;
    public final Set<String> dbs;
    public final Map<TableName, FeTable> tables;

    public StmtTableCache(FeCatalog catalog, Set<String> dbs,
        Map<TableName, FeTable> tables) {
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
   * if no events should be marked. A null 'user' may be passed if don't need to resolve
   * column-masking/row-filtering policies.
   */
  public StmtMetadataLoader(Frontend fe, String sessionDb, EventSequence timeline,
      User user, TUniqueId queryId) {
    fe_ = Preconditions.checkNotNull(fe);
    sessionDb_ = Preconditions.checkNotNull(sessionDb);
    timeline_ = timeline;
    user_ = user;
    queryId_ = queryId;
  }

  /**
   * Constructor used by callers that don't need to resolve column-masking/row-filtering
   * policies (which may introduce new tables), e.g. MetadataOp to get columns, primary
   * keys, cross reference of a table.
   */
  public StmtMetadataLoader(Frontend fe, String sessionDb, EventSequence timeline) {
    this(fe, sessionDb, timeline, null, null);
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
   *   to the 'loadedOrFailedTbls_' map, but an existing entry is never removed, even if
   *   the equivalent table in the impalad catalog is different.
   * - Tables on the impalad side are not modified in place. This means that an entry in
   *   the 'loadedOrFailedTbls_' will always remain in the loaded state.
   * Tables/views that are already loaded are simply included in the result.
   * Marks the start and end of metadata loading in 'timeline_' if it is non-NULL.
   * Must only be called once for a single statement.
   */
  public StmtTableCache loadTables(Set<TableName> tbls) throws InternalException {
    Preconditions.checkState(dbs_.isEmpty() && loadedOrFailedTbls_.isEmpty());
    Preconditions.checkState(numLoadRequestsSent_ == 0);
    Preconditions.checkState(numCatalogUpdatesReceived_ == 0);
    FeCatalog catalog = fe_.getCatalog();
    // missingTblsSnapshot builds tableName to table in the db mapping for tables that
    // are either not loaded or have failed to load due to recoverable error in the
    // previous queries. It is used to detect change of table in db since the time it is
    // added to missingTblsSnapshot when the table is loaded.
    Map<TableName, FeTable> missingTblsSnapshot = new HashMap<>();
    // TableName is added to missingTbls set as long as table is not loaded or the table
    // in db has not changed(i.e., table in db is same as table in missingTblsSnapshot).
    Set<TableName> missingTbls = getMissingTables(catalog, tbls, missingTblsSnapshot);
    // There are no missing tables. Return to avoid making an RPC to the CatalogServer
    // and adding events to the timeline.
    if (missingTbls.isEmpty()) {
      if (timeline_ != null) {
        timeline_.markEvent(String.format("Metadata of all %d tables cached",
            loadedOrFailedTbls_.size()));
      }
      fe_.getImpaladTableUsageTracker().recordTableUsage(loadedOrFailedTbls_.keySet());
      return new StmtTableCache(catalog, dbs_, loadedOrFailedTbls_);
    }

    if (timeline_ != null) timeline_.markEvent("Metadata load started");
    long startTimeMs = System.currentTimeMillis();

    // All tables for which we have requested a prioritized load.
    Set<TableName> requestedTbls = new HashSet<>();

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
    //
    // This isn't relevant for LocalCatalog, since we loaded all of the table references
    // on-demand in the first recursive call to 'getMissingTables' above.
    while (!missingTbls.isEmpty()) {
      if (issueLoadRequest) {
        catalog.prioritizeLoad(missingTbls, queryId_);
        ++numLoadRequestsSent_;
        requestedTbls.addAll(missingTbls);
      }

      // Catalog may have been restarted, always use the latest reference.
      FeCatalog currCatalog = fe_.getCatalog();
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
      Set<TableName> newMissingTbls =
          getMissingTables(catalog, missingTbls, missingTblsSnapshot);
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
      long storageLoadTimeNano = 0;
      // Calculate the total storage loading time for this query (not including
      // the tables already loaded before the query was called).
      storageLoadTimeNano =
          loadedOrFailedTbls_.values()
              .stream()
              .filter(Table.class::isInstance)
              .map(Table.class::cast)
              .filter(loadedTbl -> requestedTbls.contains(loadedTbl.getTableName()))
              .mapToLong(Table::getStorageLoadTime)
              .sum();
      timeline_.markEvent(String.format("Metadata load finished. "
              + "loaded-tables=%d/%d load-requests=%d catalog-updates=%d "
              + "storage-load-time=%dms",
          requestedTbls.size(), loadedOrFailedTbls_.size(), numLoadRequestsSent_,
          numCatalogUpdatesReceived_,
          TimeUnit.MILLISECONDS.convert(storageLoadTimeNano, TimeUnit.NANOSECONDS)));

      if (MetastoreShim.getMajorVersion() > 2) {
        StringBuilder validIdsBuf = new StringBuilder("Loaded ValidWriteIdLists");
        validIdsBuf.append(" for transactional tables: ");
        boolean hasAcidTbls = false;
        for (FeTable iTbl : loadedOrFailedTbls_.values()) {
          if (iTbl instanceof FeIncompleteTable) continue;
          if (AcidUtils.isTransactionalTable(iTbl.getMetaStoreTable().getParameters())) {
            validIdsBuf.append("\n");
            validIdsBuf.append("           ");
            validIdsBuf.append(iTbl.getValidWriteIds().writeToString());
            hasAcidTbls = true;
          }
        }
        validIdsBuf.append("\n");
        validIdsBuf.append("             ");
        if (hasAcidTbls) timeline_.markEvent(validIdsBuf.toString());
      }
    }
    fe_.getImpaladTableUsageTracker().recordTableUsage(loadedOrFailedTbls_.keySet());
    return new StmtTableCache(catalog, dbs_, loadedOrFailedTbls_);
  }

  /**
   * Determines whether the 'tbls' are loaded in the given catalog or not. Adds the names
   * of referenced databases that exist to 'dbs_', and loaded tables to
   * 'loadedOrFailedTbls_'.
   * Returns the set of tables that are not loaded. Recursively collects loaded/missing
   * tables from views. Uses 'sessionDb_' to construct table candidates from views with
   * Path.getCandidateTables(). Non-existent tables are ignored and not returned or
   * added to 'loadedOrFailedTbls_'.
   */
  private Set<TableName> getMissingTables(FeCatalog catalog, Set<TableName> tbls,
      Map<TableName, FeTable> missingTblsSnapshot) {
    Set<TableName> missingTbls = new HashSet<>();
    Set<TableName> viewTbls = new HashSet<>();
    for (TableName tblName: tbls) {
      if (loadedOrFailedTbls_.containsKey(tblName)) continue;
      FeDb db = catalog.getDb(tblName.getDb());
      if (db == null) continue;
      dbs_.add(tblName.getDb());
      FeTable tbl = db.getTable(tblName.getTbl());
      if (tbl == null) continue;
      if (!tbl.isLoaded()
          || (tbl instanceof FeIncompleteTable
                 && ((FeIncompleteTable) tbl).isLoadFailedByRecoverableError())) {
        // Add table to missingTblsSnapshot only for the first time(putIfAbsent) if the
        // table is not loaded or the previous load has failed due to recoverable error.
        missingTblsSnapshot.putIfAbsent(tblName, tbl);
      }
      if (!tbl.isLoaded() || missingTblsSnapshot.get(tblName) == tbl) {
        missingTbls.add(tblName);
        continue;
      }
      loadedOrFailedTbls_.put(tblName, tbl);
      if (tbl instanceof FeView) {
        viewTbls.addAll(collectTableCandidates(((FeView) tbl).getQueryStmt()));
      } else if (tbl instanceof MaterializedViewHdfsTable) {
        Set<TableName> mvSrcTableNames = collectTableCandidates(
            ((MaterializedViewHdfsTable) tbl).getQueryStmt());
        ((MaterializedViewHdfsTable) tbl).addSrcTables(mvSrcTableNames);
        viewTbls.addAll(mvSrcTableNames);
      }
      // Adds tables/views introduced by column-masking/row-filtering policies.
      if (!(tbl instanceof FeIncompleteTable)
          && fe_.getAuthzFactory().getAuthorizationConfig().isEnabled()
          && fe_.getAuthzFactory().supportsTableMasking() && user_ != null) {
        try {
          viewTbls.addAll(collectPolicyTables(tbl));
        } catch (Exception e) {
          LOG.error("Failed to collect policy tables for {}", tblName, e);
        }
      }
    }
    // Recursively collect loaded/missing tables from loaded views.
    if (!viewTbls.isEmpty()) {
      missingTbls.addAll(getMissingTables(catalog, viewTbls, missingTblsSnapshot));
    }
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
    List<TableRef> tblRefs = new ArrayList<>();
    // The information about whether table masking is supported is not available to
    // ResetMetadataStmt so we collect the TableRef for ResetMetadataStmt whenever
    // applicable. Skip this if allow_catalog_cache_op_from_masked_users=true because
    // we don't need column info for fetching column-masking policies.
    if (stmt instanceof ResetMetadataStmt
        && fe_.getAuthzFactory().getAuthorizationConfig().isEnabled()
        && fe_.getAuthzFactory().supportsTableMasking()
        && !BackendConfig.INSTANCE.allowCatalogCacheOpFromMaskedUsers()) {
      TableName tableName = ((ResetMetadataStmt) stmt).getTableName();
      if (tableName != null) tblRefs.add(new TableRef(tableName.toPath(), null));
    } else {
      stmt.collectTableRefs(tblRefs);
    }
    Set<TableName> tableNames = new HashSet<>();
    for (TableRef ref: tblRefs) {
      tableNames.addAll(Path.getCandidateTables(ref.getPath(), sessionDb_));
    }
    return tableNames;
  }

  @VisibleForTesting
  Set<TableName> collectPolicyTables(FeTable tbl)
      throws InternalException, AnalysisException {
    if (tbl instanceof FeIncompleteTable) return Collections.emptySet();
    Set<TableName> tableNames = new HashSet<>();
    String dbName = tbl.getDb().getName();
    String tblName = tbl.getName();
    List<Column> columns = tbl.getColumnsInHiveOrder();
    TableMask tableMask = new TableMask(fe_.getAuthzChecker(), dbName, tblName, columns,
        user_);
    if (tableMask.needsMaskingOrFiltering()) {
      for (Column col : columns) {
        // Use authzCtx=null to avoid audits and privilege checks.
        SelectStmt stmt = tableMask.createColumnMaskStmt(
            col.getName(), col.getType(), /*authzCtx*/ null);
        if (stmt == null) continue;
        tableNames.addAll(collectTableCandidates(stmt));
      }
      // Use authzCtx=null to avoid audits and privilege checks.
      SelectStmt filterStmt = tableMask.createRowFilterStmt(/*authzCtx*/null);
      if (filterStmt != null) tableNames.addAll(collectTableCandidates(filterStmt));
    }
    return tableNames;
  }
}
