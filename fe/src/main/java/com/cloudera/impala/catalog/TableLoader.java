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
import java.util.Set;

import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.log4j.Logger;

import com.cloudera.impala.catalog.MetaStoreClientPool.MetaStoreClient;

/**
 * Class that implements the logic for how a table's metadata should be loaded from
 * the Hive Metastore / HDFS / etc.
 */
public class TableLoader {
  private static final Logger LOG = Logger.getLogger(TableLoader.class);

  // Set of supported table types.
  private static EnumSet<TableType> SUPPORTED_TABLE_TYPES = EnumSet.of(
      TableType.EXTERNAL_TABLE, TableType.MANAGED_TABLE, TableType.VIRTUAL_VIEW);

  private final CatalogServiceCatalog catalog_;

  // Lock used to serialize calls to the Hive MetaStore to work around MetaStore
  // concurrency bugs. Currently used to serialize calls to "getTable()" due to
  // HIVE-5457.
  private static final Object metastoreAccessLock_ = new Object();

  public TableLoader(CatalogServiceCatalog catalog) {
    catalog_ = catalog;
  }

  /**
   * Creates the Impala representation of Hive/HBase metadata for one table.
   * Calls load() on the appropriate instance of Table subclass.
   * Returns new instance of Table, If there were any errors loading the table metadata
   * an IncompleteTable will be returned that contains details on the error.
   */
  public Table load(Db db, String tblName) {
    String fullTblName = db.getName() + "." + tblName;
    LOG.info("Loading metadata for: " + fullTblName);
    MetaStoreClient msClient = null;
    Table table;
    // turn all exceptions into TableLoadingException
    try {
      msClient = catalog_.getMetaStoreClient();
      org.apache.hadoop.hive.metastore.api.Table msTbl = null;
      // All calls to getTable() need to be serialized due to HIVE-5457.
      synchronized (metastoreAccessLock_) {
        msTbl = msClient.getHiveClient().getTable(db.getName(), tblName);
      }
      // Check that the Hive TableType is supported
      TableType tableType = TableType.valueOf(msTbl.getTableType());
      if (!SUPPORTED_TABLE_TYPES.contains(tableType)) {
        throw new TableLoadingException(String.format(
            "Unsupported table type '%s' for: %s", tableType, fullTblName));
      }

      // Create a table of appropriate type and have it load itself
      table = Table.fromMetastoreTable(catalog_.getNextTableId(), db, msTbl);
      if (table == null) {
        throw new TableLoadingException(
            "Unrecognized table type for table: " + fullTblName);
      }
      table.load(false, msClient.getHiveClient(), msTbl);
      table.validate();
    } catch (TableLoadingException e) {
      table = IncompleteTable.createFailedMetadataLoadTable(
          TableId.createInvalidId(), db, tblName, e);
    } catch (NoSuchObjectException e) {
      TableLoadingException tableDoesNotExist = new TableLoadingException(
          "Table " + fullTblName + " no longer exists in the Hive MetaStore. " +
          "Run 'invalidate metadata " + fullTblName + "' to update the Impala " +
          "catalog.");
      table = IncompleteTable.createFailedMetadataLoadTable(
          TableId.createInvalidId(), db, tblName, tableDoesNotExist);
    } catch (Exception e) {
      table = IncompleteTable.createFailedMetadataLoadTable(
          catalog_.getNextTableId(), db, tblName, new TableLoadingException(
          "Failed to load metadata for table: " + fullTblName + ". Running " +
          "'invalidate metadata " + fullTblName + "' may resolve this problem.", e));
    } finally {
      if (msClient != null) msClient.release();
    }
    return table;
  }
}
