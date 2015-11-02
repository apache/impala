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

package com.cloudera.impala.testutil;

import com.cloudera.impala.authorization.AuthorizationConfig;
import com.cloudera.impala.catalog.CatalogException;
import com.cloudera.impala.catalog.CatalogServiceCatalog;
import com.cloudera.impala.catalog.Db;
import com.cloudera.impala.catalog.HdfsCachePool;
import com.cloudera.impala.catalog.ImpaladCatalog;
import com.cloudera.impala.catalog.Table;
import com.google.common.base.Preconditions;

/**
 * Mock catalog used for running FE tests that allows lazy-loading of tables without a
 * running catalogd/statestored.
 */
public class ImpaladTestCatalog extends ImpaladCatalog {
  // Used to load missing table metadata when running the FE tests.
  private final CatalogServiceCatalog srcCatalog_;

  public ImpaladTestCatalog() {
    this(AuthorizationConfig.createAuthDisabledConfig());
  }

  /**
   * Takes an AuthorizationConfig to bootstrap the backing CatalogServiceCatalog.
   */
  public ImpaladTestCatalog(AuthorizationConfig authzConfig) {
    super();
    CatalogServiceCatalog catalogServerCatalog =
        CatalogServiceTestCatalog.createWithAuth(authzConfig.getSentryConfig());
    // Bootstrap the catalog by adding all dbs, tables, and functions.
    for (Db db: catalogServerCatalog.getDbs(null)) {
      // Adding DB should include all tables/fns in that database.
      addDb(db);
    }
    authPolicy_ = ((CatalogServiceTestCatalog) catalogServerCatalog).getAuthPolicy();
    srcCatalog_ = catalogServerCatalog;
    setIsReady();
  }

  @Override
  public HdfsCachePool getHdfsCachePool(String poolName) {
    return srcCatalog_.getHdfsCachePool(poolName);
  }

  /**
   * Reloads all metadata from the source catalog.
   */
  public void reset() throws CatalogException {
    srcCatalog_.reset();
  }

  /**
   * Overrides ImpaladCatalog.getTable to load the table metadata if it is missing.
   */
  @Override
  public Table getTable(String dbName, String tableName)
      throws CatalogException {
    Table existingTbl = super.getTable(dbName, tableName);
    // Table doesn't exist or is already loaded. Just return it.
    if (existingTbl == null || existingTbl.isLoaded()) return existingTbl;

    // The table was not yet loaded. Load it in to the catalog and try getTable()
    // again.
    Table newTbl = srcCatalog_.getOrLoadTable(dbName,  tableName);
    Preconditions.checkNotNull(newTbl);
    Preconditions.checkState(newTbl.isLoaded());
    Db db = getDb(dbName);
    Preconditions.checkNotNull(db);
    db.addTable(newTbl);
    return super.getTable(dbName, tableName);
  }
}