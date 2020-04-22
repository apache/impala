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

package org.apache.impala.catalog.local;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.FeCatalogUtils;
import org.apache.impala.catalog.FeHBaseTable;
import org.apache.impala.catalog.local.MetaProvider.TableMetaRef;
import org.apache.impala.common.Pair;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.thrift.TTableDescriptor;
import org.apache.impala.thrift.TTableType;

public class LocalHbaseTable extends LocalTable implements FeHBaseTable {
  // Name of table in HBase.
  // 'this.name' is the alias of the HBase table in Hive.
  // It's also referenced in msTbl so it doesn't take additional space here
  private String hbaseTableName_;

  // Cached column families. Used primarily for speeding up row stats estimation
  // (see IMPALA-4211).
  // TODO: revisit after caching is implemented for local catalog
  private HColumnDescriptor[] columnFamilies_ = null;

  private LocalHbaseTable(LocalDb db, Table msTbl, TableMetaRef ref, ColumnMap cols) {
    super(db, msTbl, ref, cols);
    hbaseTableName_ = Util.getHBaseTableName(msTbl);
  }

  static LocalHbaseTable loadFromHbase(LocalDb db, Table msTable, TableMetaRef ref) {
    try {
      // Warm up the connection and verify the table exists.
      Util.getHBaseTable(Util.getHBaseTableName(msTable)).close();
      // since we don't support composite hbase rowkeys yet, all hbase tables have a
      // single clustering col
      ColumnMap cmap = new ColumnMap(Util.loadColumns(msTable), 1,
          msTable.getDbName() + "." + msTable.getTableName(), /*isFullAcidSchema=*/false);
      return new LocalHbaseTable(db, msTable, ref, cmap);
    } catch (IOException | MetaException | SerDeException e) {
      throw new LocalCatalogException(e);
    }
  }

  @Override
  public TTableDescriptor toThriftDescriptor(int tableId,
      Set<Long> referencedPartitions) {
    TTableDescriptor tableDescriptor =
        new TTableDescriptor(tableId, TTableType.HBASE_TABLE,
            FeCatalogUtils.getTColumnDescriptors(this), 1, getHBaseTableName(),
            db_.getName());
    tableDescriptor.setHbaseTable(Util.getTHBaseTable(this));
    return tableDescriptor;
  }

  @Override
  public Pair<Long, Long> getEstimatedRowStats(byte[] startRowKey, byte[] endRowKey) {
    return Util.getEstimatedRowStats(this, startRowKey, endRowKey);
  }

  @Override
  public String getHBaseTableName() {
    return hbaseTableName_;
  }

  @Override
  public TResultSet getTableStats() {
    return Util.getTableStats(this);
  }

  @Override
  public HColumnDescriptor[] getColumnFamilies() throws IOException {
    if (columnFamilies_ == null) {
      try (org.apache.hadoop.hbase.client.Table hBaseTable = Util
          .getHBaseTable(getHBaseTableName())) {
        columnFamilies_ = hBaseTable.getTableDescriptor().getColumnFamilies();
      }
    }
    return columnFamilies_;
  }

  @Override
  public List<Column> getColumnsInHiveOrder() {
    return getColumns();
  }

  /**
   * Returns the storage handler class for HBase tables read by Hive.
   */
  @Override
  public String getStorageHandlerClassName() {
    return Util.HBASE_STORAGE_HANDLER;
  }

}
