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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import com.google.common.base.Preconditions;

/**
 * A cache loader that defines how table metadata should be loaded. Impalads
 * load their table metadata by contacting the Catalog Server. The Catalog Server
 * loads its table metadata by contacting the Hive Metastore / HDFS / etc.
 */
public abstract class TableLoader extends CacheLoader<String, Table> {
  private final static Logger LOG = LoggerFactory.getLogger(TableLoader.class);

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
    Preconditions.checkState(
        parentDb.getParentCatalog() instanceof CatalogServiceCatalog);
    return new MetaStoreTableLoader(parentDb);
  }

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
     * cachedValue is the existing cache entry and might still contain valid info to
     * help speed up metadata loading. cachedValue is null if there is no existing
     * cache entry (i.e. during fresh load).
     * The catalogVersion parameter specifies what version will be assigned
     * to the newly loaded object.
     * Returns new instance of Table, or null if the table does not exist. If there
     * were any errors loading the table metadata an IncompleteTable will be returned
     * that contains details on the error.
     */
    @Override
    public Table load(String tblName, Table cachedValue, long catalogVersion) {
      String fullTblName = db_.getName() + "." + tblName;
      LOG.info("Loading metadata for: " + fullTblName);
      Preconditions.checkState(db_.getParentCatalog() instanceof CatalogServiceCatalog);
      CatalogServiceCatalog catalog = (CatalogServiceCatalog) db_.getParentCatalog();

      MetaStoreClient msClient = null;
      Table table;
      // turn all exceptions into TableLoadingException
      try {
        msClient = catalog.getMetaStoreClient();
        org.apache.hadoop.hive.metastore.api.Table msTbl = null;
        // All calls to getTable() need to be serialized due to HIVE-5457.
        synchronized (metastoreAccessLock_) {
          msTbl = msClient.getHiveClient().getTable(db_.getName(), tblName);
        }
        // Check that the Hive TableType is supported
        TableType tableType = TableType.valueOf(msTbl.getTableType());
        if (!SUPPORTED_TABLE_TYPES.contains(tableType)) {
          throw new TableLoadingException(String.format(
              "Unsupported table type '%s' for: %s", tableType, fullTblName));
        }

        // Create a table of appropriate type and have it load itself
        table = Table.fromMetastoreTable(catalog.getNextTableId(), db_, msTbl);
        if (table == null) {
          throw new TableLoadingException(
              "Unrecognized table type for table: " + fullTblName);
        }
        table.load(cachedValue, msClient.getHiveClient(), msTbl);
      } catch (TableLoadingException e) {
        table = IncompleteTable.createFailedMetadataLoadTable(
            catalog.getNextTableId(), db_, tblName, e);
      } catch (NoSuchObjectException e) {
        TableLoadingException tableDoesNotExist = new TableLoadingException(
            "Table " + fullTblName + " no longer exists in the Hive MetaStore. " +
            "Run 'invalidate metadata " + fullTblName + "' to update the Impala " +
            "catalog.");
        table = IncompleteTable.createFailedMetadataLoadTable(
            catalog.getNextTableId(), db_, tblName, tableDoesNotExist);
      } catch (Exception e) {
        table = IncompleteTable.createFailedMetadataLoadTable(
            catalog.getNextTableId(), db_, tblName, new TableLoadingException(
            "Failed to load metadata for table: " + fullTblName + ". Running " +
            "'invalidate metadata " + fullTblName + "' may resolve this problem.", e));
      } finally {
        if (msClient != null) msClient.release();
      }
      // Set the new catalog version for the table and return it.
      table.setCatalogVersion(catalogVersion);
      return table;
    }

    @Override
    public long getNextCatalogVersion() {
      CatalogServiceCatalog catalog = (CatalogServiceCatalog) db_.getParentCatalog();
      return catalog.incrementAndGetCatalogVersion();
    }
  }
}
