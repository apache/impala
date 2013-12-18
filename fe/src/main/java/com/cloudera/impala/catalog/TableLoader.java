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

import java.util.EnumSet;

import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;

import com.cloudera.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.service.FeSupport;
import com.cloudera.impala.thrift.TCatalogObject;
import com.cloudera.impala.thrift.TCatalogObjectType;
import com.cloudera.impala.thrift.TTable;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheLoader;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

/**
 * A cache loader that defines how table metadata should be loaded. Impalads
 * load their table metadata by contacting the Catalog Server. The Catalog Server
 * loads its table metadata by contacting the Hive Metastore / HDFS / etc.
 */
public abstract class TableLoader extends CacheLoader<String, Table> {
  // Parent database of this table.
  protected final Db db_;

  private TableLoader(Db db) {
    db_ = db;
  }

  /**
   * Factory method that creates the proper TableLoader based on the given
   * database's parent catalog.
   */
  public static TableLoader createTableLoader(Db parentDb) {
    Preconditions.checkNotNull(parentDb);
    if (parentDb.getParentCatalog() instanceof ImpaladCatalog) {
      return new CatalogServiceTableLoader(parentDb);
    } else if (parentDb.getParentCatalog() instanceof CatalogServiceCatalog) {
      return new MetaStoreTableLoader(parentDb);
    }
    throw new IllegalStateException("Unexpected Catalog type: " +
        parentDb.getParentCatalog().getClass().getName());
  }

  /**
   * Load the given table name.
   */
  @Override
  public Table load(String tblName) throws Exception {
    return loadTable(tblName, null);
  }

  /**
   * Computes a replacement value for the Table based on an already-cached value.
   *
   * Returns (as a ListenableFuture) the future new value of the table.
   * The returned Future should not be null. Using a Future allows for a
   * synchronous or asynchronous implementation or reload().
   */
  @Override
  public ListenableFuture<Table> reload(String tableName, Table cachedValue)
      throws ImpalaException {
    SettableFuture<Table> newValue = SettableFuture.create();
    Table table = loadTable(tableName, cachedValue);
    newValue.set(table);
    return newValue;
  }

  /**
   * Implementation for how a table is loaded. Generally this is the
   * only method that needs to be implemented.
   * @param tblName - The name of the table.
   * @param cachedEntry - An existing cached table that can be reused to speed up
   *                      the loading process for a "reload" operation.
   */
  protected abstract Table loadTable(String tableName, Table cachedValue)
      throws TableNotFoundException;


  /**
   * TableLoader that loads table metadata from the Hive Metastore. Used by
   * the CatalogServer and updates the object's catalog version on successful
   * load()/reload().
   */
  private static class MetaStoreTableLoader extends TableLoader {
    // Set of supported table types.
    private static EnumSet<TableType> SUPPORTED_TABLE_TYPES = EnumSet.of(
        TableType.EXTERNAL_TABLE, TableType.MANAGED_TABLE, TableType.VIRTUAL_VIEW);

    // Lock used to serialize calls to the Hive MetaStore to work around MetaStore
    // concurrency bugs. Currently used to serialize calls to "getTable()" due to
    // HIVE-5457.
    private static final Object metastoreAccessLock_ = new Object();

    public MetaStoreTableLoader(Db db) {
      super(db);
    }

    /**
     * Creates the Impala representation of Hive/HBase metadata for one table.
     * Calls load() on the appropriate instance of Table subclass.
     * oldCacheEntry is the existing cache entry and might still contain valid info to
     * help speed up metadata loading. oldCacheEntry is null if there is no existing
     * cache entry (i.e. during fresh load).
     * @return new instance of HdfsTable or HBaseTable
     *         null if the table does not exist
     * @throws TableLoadingException if there was an error loading the table.
     * @throws TableNotFoundException if the table was not found
     */
    @Override
    protected Table loadTable(String tblName, Table cacheEntry)
        throws TableNotFoundException {
      Catalog catalog = db_.getParentCatalog();
      MetaStoreClient msClient = catalog.getMetaStoreClient();
      Table table;
      // turn all exceptions into TableLoadingException
      try {
        org.apache.hadoop.hive.metastore.api.Table msTbl = null;
        // All calls to getTable() need to be serialized due to HIVE-5457.
        synchronized (metastoreAccessLock_) {
          msTbl = msClient.getHiveClient().getTable(db_.getName(), tblName);
        }
        // Check that the Hive TableType is supported
        TableType tableType = TableType.valueOf(msTbl.getTableType());
        if (!SUPPORTED_TABLE_TYPES.contains(tableType)) {
          throw new TableLoadingException(String.format(
              "Unsupported table type '%s' for: %s.%s",
              tableType, db_.getName(), tblName));
        }

        // Create a table of appropriate type and have it load itself
        table = Table.fromMetastoreTable(catalog.getNextTableId(), db_, msTbl);
        if (table == null) {
          throw new TableLoadingException(
              "Unrecognized table type for table: " + msTbl.getTableName());
        }
        table.load(cacheEntry, msClient.getHiveClient(), msTbl);
      } catch (TableLoadingException e) {
        table = IncompleteTable.createFailedMetadataLoadTable(
            catalog.getNextTableId(), db_, tblName, e);
      } catch (NoSuchObjectException e) {
        throw new TableNotFoundException("Table not found: " + tblName, e);
      } catch (Exception e) {
        table = IncompleteTable.createFailedMetadataLoadTable(
            catalog.getNextTableId(), db_, tblName, new TableLoadingException(
            "Failed to load metadata for table: " + tblName, e));
      } finally {
        msClient.release();
      }
      // Set the new catalog version for the table and return it.
      table.setCatalogVersion(CatalogServiceCatalog.incrementAndGetCatalogVersion());
      return table;
    }
  }

  /**
   * A TableLoader that loads metadata from the CatalogServer.
   */
  private static class CatalogServiceTableLoader extends TableLoader {
    public CatalogServiceTableLoader(Db db) {
      super(db);
    }

    @Override
    public ListenableFuture<Table> reload(String tableName, Table cachedValue)
        throws ImpalaException {
      // To protect against a concurrency issue between add(CatalogObject) and
      // reload(), reload() is not supported on a CatalogServiceTableLoader. See comment
      // in the CatalogObjectCache for more details.
      throw new IllegalStateException("Calling reload() on a CatalogServiceTableLoader" +
        " is not supported.");
    }

    @Override
    protected Table loadTable(String tblName, Table cacheEntry)
        throws TableNotFoundException {
      TCatalogObject objectDesc = new TCatalogObject();
      objectDesc.setType(TCatalogObjectType.TABLE);
      objectDesc.setTable(new TTable());
      objectDesc.getTable().setDb_name(db_.getName());
      objectDesc.getTable().setTbl_name(tblName);
      TCatalogObject catalogObject;
      try {
        catalogObject = FeSupport.GetCatalogObject(objectDesc);
      } catch (InternalException e) {
        return IncompleteTable.createFailedMetadataLoadTable(
            TableId.createInvalidId(), db_, tblName, e);
      }

      if (!catalogObject.isSetTable()) {
        throw new TableNotFoundException(
            String.format("Table not found: %s.%s", db_.getName(), tblName));
      }

      Table newTable;
      try {
        newTable = Table.fromThrift(db_, catalogObject.getTable());
      } catch (TableLoadingException e) {
        newTable = IncompleteTable.createFailedMetadataLoadTable(
            TableId.createInvalidId(), db_, tblName, e);
      }
      newTable.setCatalogVersion(catalogObject.getCatalog_version());
      return newTable;
    }
  }
}