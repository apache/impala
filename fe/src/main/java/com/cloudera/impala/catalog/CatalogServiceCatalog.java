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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.common.Pair;
import com.cloudera.impala.thrift.TCatalog;
import com.cloudera.impala.thrift.TCatalogObject;
import com.cloudera.impala.thrift.TCatalogObjectType;
import com.cloudera.impala.thrift.TGetAllCatalogObjectsResponse;
import com.cloudera.impala.thrift.TTable;
import com.cloudera.impala.thrift.TUniqueId;
import com.google.common.collect.Lists;

/**
 * Specialized Catalog that implements the CatalogService specific Catalog
 * APIs. The CatalogServiceCatalog manages loading of all the catalog metadata
 * and processing of DDL requests. For each DDL request, the CatalogServiceCatalog
 * will return the catalog version that the update will show up in. The client
 * can then wait until the statestore sends an update that contains that catalog
 * version.
 */
public class CatalogServiceCatalog extends Catalog {
  private static final Logger LOG = Logger.getLogger(CatalogServiceCatalog.class);
  private final TUniqueId catalogServiceId_;

  /**
   * Initialize the CatalogServiceCatalog, loading all table and database metadata
   * immediately.
   */
  public CatalogServiceCatalog(TUniqueId catalogServiceId) {
    this(catalogServiceId, CatalogInitStrategy.IMMEDIATE);
  }

  /**
   * Constructor used to speed up testing by allowing for lazily loading
   * the Catalog metadata.
   */
  public CatalogServiceCatalog(TUniqueId catalogServiceId,
      CatalogInitStrategy initStrategy) {
    super(initStrategy);
    catalogServiceId_ = catalogServiceId;
  }

  /**
   * Returns all known objects in the Catalog (Tables, Views, Databases, and
   * Functions). Some metadata may be skipped for objects that have a catalog
   * version < the specified "fromVersion".
   */
  public TGetAllCatalogObjectsResponse getCatalogObjects(long fromVersion) {
    TGetAllCatalogObjectsResponse resp = new TGetAllCatalogObjectsResponse();
    resp.setObjects(new ArrayList<TCatalogObject>());
    resp.setMax_catalog_version(Catalog.INITIAL_CATALOG_VERSION);

    // Take a lock on the catalog to ensure this update contains a consistent snapshot
    // of all items in the catalog.
    catalogLock_.readLock().lock();
    try {
      for (String dbName: getDbNames(null)) {
        Db db = getDb(dbName);
        if (db == null) {
          LOG.error("Database: " + dbName + " was expected to be in the catalog " +
              "cache. Skipping database and all child objects for this update.");
          continue;
        }
        TCatalogObject catalogDb = new TCatalogObject(TCatalogObjectType.DATABASE,
            db.getCatalogVersion());
        catalogDb.setDb(db.toThrift());
        resp.addToObjects(catalogDb);

        for (String tblName: db.getAllTableNames()) {
          TCatalogObject catalogTbl = new TCatalogObject(TCatalogObjectType.TABLE,
              Catalog.INITIAL_CATALOG_VERSION);
          Table tbl = getTableNoThrow(dbName, tblName);
          if (tbl == null) {
            LOG.error("Table: " + tblName + " was expected to be in the catalog " +
                "cache. Skipping table for this update.");
            continue;
          }

          // Only add the extended metadata if this table's version is >=
          // the fromVersion.
          if (tbl.getCatalogVersion() >= fromVersion) {
            try {
              catalogTbl.setTable(tbl.toThrift());
            } catch (Exception e) {
              LOG.debug(String.format("Error calling toThrift() on table %s.%s: %s",
                  dbName, tblName, e.getMessage()), e);
              continue;
            }
            catalogTbl.setCatalog_version(tbl.getCatalogVersion());
          } else {
            catalogTbl.setTable(new TTable(dbName, tblName));
          }
          resp.addToObjects(catalogTbl);
        }

        for (String signature: db.getAllFunctionSignatures(null)) {
          Function fn = db.getFunction(signature);
          if (fn == null) continue;
          TCatalogObject function = new TCatalogObject(TCatalogObjectType.FUNCTION,
              fn.getCatalogVersion());
          function.setType(TCatalogObjectType.FUNCTION);
          function.setFn(fn.toThrift());
          resp.addToObjects(function);
        }
      }

      // Each update should contain a single "TCatalog" object which is used to
      // pass overall state on the catalog, such as the current version and the
      // catalog service id.
      TCatalogObject catalog = new TCatalogObject();
      catalog.setType(TCatalogObjectType.CATALOG);
      // By setting the catalog version to the latest catalog version at this point,
      // it ensure impalads will always bump their versions, even in the case where
      // an object has been dropped.
      catalog.setCatalog_version(Catalog.getCatalogVersion());
      catalog.setCatalog(new TCatalog(catalogServiceId_));
      resp.addToObjects(catalog);

      // The max version is the max catalog version of all items in the update.
      resp.setMax_catalog_version(Catalog.getCatalogVersion());
      return resp;
    } finally {
      catalogLock_.readLock().unlock();
    }
  }

  /**
   * Returns the Table object for the given dbName/tableName. This will trigger a
   * metadata load if the table metadata is not yet cached. This method does not
   * throw, if there are any issues loading the table metadata a
   * IncompleteTable will be returned instead of raising an exception.
   */
  public Table getTableNoThrow(String dbName, String tableName) {
    Db db = getDb(dbName);
    if (db == null) return null;
    try {
      Table table = db.getTable(tableName);
      if (table == null) return null;
      return table;
    } catch (ImpalaException e) {
      return new IncompleteTable(getNextTableId(), db, tableName, e);
    }
  }

  @Override
  public long reset() {
    catalogLock_.writeLock().lock();

    // Since UDFs/UDAs are not persisted in the metastore, we won't clear
    // them across reset. To do this, we store all the functions before
    // clearing and restore them after.
    // TODO: Everything about this. Persist them.
    List<Pair<String, HashMap<String, List<Function>>>> functions =
        Lists.newArrayList();
    for (Db db: dbCache_.getAllObjects()) {
      if (db.numFunctions() == 0) continue;
      functions.add(Pair.create(db.getName(), db.getAllFunctions()));
    }

    try {
      // Reset the dbs.
      resetInternal();

      // Restore UDFs/UDAs.
      for (Pair<String, HashMap<String, List<Function>>> dbFns: functions) {
        Db db = null;
        try {
          db = dbCache_.get(dbFns.first);
        } catch (Exception e) {
          continue;
        }
        if (db == null) {
          // DB no longer exists.
          // TODO: We could restore this DB and then add the functions back.
          continue;
        }

        for (List<Function> fns: dbFns.second.values()) {
          for (Function fn: fns) {
            db.addFunction(fn);
          }
        }
      }
      return getCatalogVersion();
    } finally {
      catalogLock_.writeLock().unlock();
    }
  }
}
