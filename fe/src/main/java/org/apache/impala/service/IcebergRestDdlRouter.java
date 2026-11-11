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

package org.apache.impala.service;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

import org.apache.impala.analysis.AnalysisContext.AnalysisResult;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.CreateDbStmt;
import org.apache.impala.analysis.DropDbStmt;
import org.apache.impala.analysis.DropTableOrViewStmt;
import org.apache.impala.catalog.CatalogRef;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.catalog.local.LocalCatalogException;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TDdlType;
import org.apache.impala.util.CatalogBlacklistUtils;
import org.apache.impala.util.IcebergUtil;

import com.google.common.base.Preconditions;

/**
 * Decides how an analyzed DDL statement is executed with respect to Iceberg REST
 * catalogs: through CatalogD, on the coordinator through a REST catalog (see
 * {@link IcebergDdlExecutor}), or not at all. Only the statement types that
 * IcebergDdlExecutor implements are considered; everything else goes to CatalogD.
 *
 * <p>The catalog that serves the target is resolved afresh for each statement, see
 * {@link org.apache.impala.catalog.FeCatalog#getCatalogRef}, and has to agree with the
 * table that analysis saw, so that the statement is executed against the table that
 * was authorized. When it cannot be decided, the statement fails, even on a table of
 * CatalogD; otherwise statements on targets served by CatalogD go to CatalogD as before.
 */
final class IcebergRestDdlRouter {
  /** The way a DDL statement is executed. */
  static final class Route {
    static final Route CATALOGD = new Route(null, null);

    // Name of the REST catalog that executes the DDL on the coordinator, or null.
    private final @Nullable String restCatalogName_;
    // Result of a DDL that is a no-op, or null.
    private final @Nullable List<String> noopSummary_;

    private Route(@Nullable String restCatalogName, @Nullable List<String> noopSummary) {
      restCatalogName_ = restCatalogName;
      noopSummary_ = noopSummary;
    }

    static Route restCatalog(String restCatalogName) {
      return new Route(Preconditions.checkNotNull(restCatalogName), null);
    }

    static Route noOp(String summary) {
      return new Route(null, Collections.singletonList(summary));
    }

    /** The REST catalog that executes the DDL, or null when it goes to CatalogD. */
    @Nullable String getRestCatalogName() { return restCatalogName_; }

    boolean isNoOp() { return noopSummary_ != null; }

    List<String> getNoopSummary() {
      Preconditions.checkState(isNoOp());
      return noopSummary_;
    }
  }

  private IcebergRestDdlRouter() {}

  /** Returns how the DDL in 'analysis' is executed. */
  static Route route(AnalysisResult analysis) throws AnalysisException {
    if (analysis.isDropTableOrViewStmt()) {
      return routeDropTableOrView(analysis.getAnalyzer(),
          analysis.getDropTableOrViewStmt());
    }
    if (analysis.isDropDbStmt()) {
      return routeDropDb(analysis.getAnalyzer(), analysis.getDropDbStmt());
    }
    if (analysis.isCreateDbStmt()) {
      return routeCreateDb(analysis.getAnalyzer(), analysis.getCreateDbStmt());
    }
    return Route.CATALOGD;
  }

  /**
   * Routes DROP TABLE/VIEW [IF EXISTS] to the catalog that serves its target now. The
   * statement is rejected when that catalog cannot be decided, or when it is not the one
   * the analyzed table came from, e.g. because the table was dropped and recreated in
   * another catalog since analysis. On a target served by a REST catalog:
   * - a table that was loaded or failed to load is dropped through the REST catalog,
   *   which needs a unique name. DROP VIEW IF EXISTS on a loaded table is a no-op, as
   *   REST catalogs serve no views.
   * - a missing target, which analysis lets through with IF EXISTS only, is a no-op.
   *   Only the ANY privilege was checked on it, and a REST catalog would drop whatever
   *   it finds under the name, e.g. a mixed-case table that Impala cannot resolve.
   * The summary of a no-op is the one CatalogD gives.
   */
  private static Route routeDropTableOrView(Analyzer analyzer, DropTableOrViewStmt stmt)
      throws AnalysisException {
    String dbName = stmt.getDb().toLowerCase();
    String tblName = stmt.getTbl().toLowerCase();
    String fullName = dbName + "." + tblName;
    CatalogRef catalogRef;
    try {
      catalogRef = analyzer.getCatalog().getCatalogRef(dbName, tblName);
    } catch (LocalCatalogException e) {
      throw new AnalysisException(e.getMessage(), e);
    }
    FeTable table;
    boolean loaded = true;
    try {
      table = analyzer.getTable(
          analyzer.getFqTableName(stmt.getTableName()), /*mustExist=*/false);
    } catch (TableLoadingException e) {
      table = null;
      loaded = false;
    }
    boolean restTable = table instanceof FeIcebergTable
        && ((FeIcebergTable) table).hasProviderDerivedCapabilities();
    if (table != null && restTable != catalogRef.isRest()
        || restTable && !Objects.equals(((FeIcebergTable) table).getCatalogName(),
            catalogRef.getRestCatalogName())) {
      throw new AnalysisException(String.format(
          "Table %s is served by %s, not by the catalog it was analyzed from. Retry.",
          fullName, catalogRef));
    }
    if (!catalogRef.isRest()) return Route.CATALOGD;
    if (table == null && loaded) {
      Preconditions.checkState(stmt.isIfExists());
      if (CatalogBlacklistUtils.isDbBlacklisted(dbName)
          || CatalogBlacklistUtils.isTableBlacklisted(dbName, tblName)) {
        return Route.noOp("Can't drop blacklisted table: " + fullName);
      }
      return Route.noOp((stmt.isDropTable() ? "Table" : "View") + " does not exist.");
    }
    if (catalogRef.getRestCatalogName() == null) {
      throw Analyzer.restCatalogNotUnique(
          stmt.isDropTable() ? TDdlType.DROP_TABLE : TDdlType.DROP_VIEW, fullName);
    }
    if (table != null && !stmt.isDropTable()) {
      Preconditions.checkState(stmt.isIfExists());
      return Route.noOp("Drop view is not allowed on a table.");
    }
    return Route.restCatalog(catalogRef.getRestCatalogName());
  }

  /**
   * Routes DROP DATABASE [IF EXISTS] to the catalog that serves the database now. The
   * statement is rejected when that catalog cannot be decided, e.g. because several
   * catalogs list the database. Unlike a table, a database carries no catalog from
   * analysis to check against: it is authorized by name. On a database served by a
   * REST catalog:
   * - an existing database is dropped through the REST catalog, which needs a unique
   *   name. CASCADE is rejected: REST catalogs drop empty namespaces only.
   * - a missing database, which analysis lets through with IF EXISTS only, is a no-op,
   *   as for a missing table.
   * The summary of a no-op is the one CatalogD gives.
   */
  private static Route routeDropDb(Analyzer analyzer, DropDbStmt stmt)
      throws AnalysisException {
    String dbName = stmt.getDb().toLowerCase();
    CatalogRef catalogRef;
    try {
      catalogRef = analyzer.getCatalog().getCatalogRef(dbName);
    } catch (LocalCatalogException e) {
      throw new AnalysisException(e.getMessage(), e);
    }
    if (!catalogRef.isRest()) return Route.CATALOGD;
    if (!analyzer.dbExists(dbName)) {
      Preconditions.checkState(stmt.getIfExists());
      if (CatalogBlacklistUtils.isDbBlacklisted(dbName)) {
        return Route.noOp("Can't drop blacklisted database: " + dbName);
      }
      return Route.noOp("Database does not exist.");
    }
    if (catalogRef.getRestCatalogName() == null) {
      throw Analyzer.restCatalogNotUnique(TDdlType.DROP_DATABASE, dbName);
    }
    if (stmt.getCascade()) {
      throw new AnalysisException(
          "DROP DATABASE CASCADE is not supported on Iceberg REST catalogs: " + dbName);
    }
    return Route.restCatalog(catalogRef.getRestCatalogName());
  }

  /**
   * Routes CREATE DATABASE [IF NOT EXISTS]. Analysis rejects it with multiple catalogs,
   * so the one catalog creates it: a REST catalog creates a namespace, which needs a
   * unique name. IF NOT EXISTS on an existing database is a no-op, with the summary that
   * CatalogD gives.
   */
  private static Route routeCreateDb(Analyzer analyzer, CreateDbStmt stmt)
      throws AnalysisException {
    String dbName = stmt.getDb().toLowerCase();
    Preconditions.checkState(!analyzer.getCatalog().isMultiCatalog());
    CatalogRef catalogRef = analyzer.getCatalog().getCatalogRef(dbName);
    if (!catalogRef.isRest()) return Route.CATALOGD;
    if (analyzer.dbExists(dbName)) {
      Preconditions.checkState(stmt.getIfNotExists());
      return Route.noOp("Database already exists.");
    }
    if (catalogRef.getRestCatalogName() == null) {
      throw Analyzer.restCatalogNotUnique(TDdlType.CREATE_DATABASE, dbName);
    }
    if (stmt.hasManagedLocation()) {
      throw new AnalysisException(
          "MANAGEDLOCATION is not supported on Iceberg REST catalogs: " + dbName);
    }
    String reserved = IcebergUtil.findReservedProperty(
        stmt.getDbProperties().keySet(), IcebergUtil.REST_RESERVED_DB_PROPERTIES);
    if (reserved != null) {
      throw new AnalysisException(IcebergDdlExecutor.reservedDbPropertyError(
          reserved, dbName));
    }
    return Route.restCatalog(catalogRef.getRestCatalogName());
  }
}
