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

package org.apache.impala.catalog;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.impala.common.Pair;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.thrift.TTable;
import org.apache.impala.thrift.TTableDescriptor;
import org.apache.impala.thrift.TTableType;
import org.apache.impala.util.EventSequence;

import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;

/**
 * Impala representation of HBase table metadata,
 * as loaded from Hive's metastore.
 * This implies that we inherit the metastore's limitations related to HBase,
 * for example the lack of support for composite HBase row keys.
 * We sort the HBase columns (cols) by family/qualifier
 * to simplify the retrieval logic in the backend, since
 * HBase returns data ordered by family/qualifier.
 * This implies that a "select *"-query on an HBase table
 * will not have the columns ordered as they were declared in the DDL.
 * They will be ordered by family/qualifier.
 */
public class HBaseTable extends Table implements FeHBaseTable {
  // Input format class for HBase tables read by Hive.
  private static final String HBASE_INPUT_FORMAT =
      "org.apache.hadoop.hive.hbase.HiveHBaseTableInputFormat";

  // Serialization class for HBase tables set in the corresponding Metastore table.
  private static final String HBASE_SERIALIZATION_LIB =
      "org.apache.hadoop.hive.hbase.HBaseSerDe";


  // Name of table in HBase.
  // 'this.name' is the alias of the HBase table in Hive.
  private String hbaseTableName_;


  // Cached column families. Used primarily for speeding up row stats estimation
  // (see IMPALA-4211).
  private HColumnDescriptor[] columnFamilies_ = null;

  protected HBaseTable(org.apache.hadoop.hive.metastore.api.Table msTbl, Db db,
      String name, String owner) {
    super(msTbl, db, name, owner);
  }

  /**
   * Returns true if the given Metastore Table represents an HBase table.
   * Versions of Hive/HBase are inconsistent which HBase related fields are set
   * (e.g., HIVE-6548 changed the input format to null).
   * For maximum compatibility consider all known fields that indicate an HBase table.
   */
  public static boolean isHBaseTable(org.apache.hadoop.hive.metastore.api.Table msTbl) {
    if (msTbl.getParameters() != null &&
        msTbl.getParameters().containsKey(Util.HBASE_STORAGE_HANDLER)) {
      return true;
    }
    StorageDescriptor sd = msTbl.getSd();
    if (sd == null) return false;
    if (sd.getInputFormat() != null && sd.getInputFormat().equals(HBASE_INPUT_FORMAT)) {
      return true;
    } else return sd.getSerdeInfo() != null &&
        sd.getSerdeInfo().getSerializationLib() != null &&
        sd.getSerdeInfo().getSerializationLib().equals(HBASE_SERIALIZATION_LIB);
  }

  /**
   * For hbase tables, we can support tables with columns we don't understand at
   * all (e.g. map) as long as the user does not select those. This is in contrast
   * to hdfs tables since we typically need to understand all columns to make sense
   * of the file at all.
   */
  @Override
  public void load(boolean reuseMetadata, IMetaStoreClient client,
      org.apache.hadoop.hive.metastore.api.Table msTbl, String reason,
      EventSequence catalogTimeline) throws TableLoadingException {
    Preconditions.checkNotNull(getMetaStoreTable());
    Table.LOADING_TABLES.incrementAndGet();
    try (Timer.Context timer = getMetrics().getTimer(Table.LOAD_DURATION_METRIC).time()) {
      msTable_ = msTbl;
      final Timer.Context storageLoadTimer =
          getMetrics().getTimer(Table.LOAD_DURATION_STORAGE_METADATA).time();
      List<Column> cols;
      try {
        hbaseTableName_ = Util.getHBaseTableName(getMetaStoreTable());
        // Warm up the connection and verify the table exists.
        Util.getHBaseTable(hbaseTableName_).close();
        catalogTimeline.markEvent("Checked HBase table exists");
        columnFamilies_ = null;
        // Warm up the connection and verify the table exists.
        getColumnFamilies();
        cols = Util.loadColumns(msTable_);
      } finally {
        storageMetadataLoadTime_ = storageLoadTimer.stop();
      }
      clearColumns();
      for (Column col : cols) addColumn(col);
      // Set table stats.
      setTableStats(msTable_);
      // since we don't support composite hbase rowkeys yet, all hbase tables have a
      // single clustering col
      numClusteringCols_ = 1;
      loadAllColumnStats(client, catalogTimeline);
      refreshLastUsedTime();
    } catch (Exception e) {
      throw new TableLoadingException("Failed to load metadata for HBase table: " + name_,
          e);
    } finally {
      Table.LOADING_TABLES.decrementAndGet();
    }
  }

  @Override
  protected void loadFromThrift(TTable table) throws TableLoadingException {
    super.loadFromThrift(table);
    try {
      hbaseTableName_ = Util.getHBaseTableName(getMetaStoreTable());
      columnFamilies_ = null;
      // Warm up the connection and verify the table exists.
      getColumnFamilies();
    } catch (Exception e) {
      throw new TableLoadingException(
          "Failed to load metadata for HBase table from thrift table: " + name_, e);
    }
  }

  @Override
  public Pair<Long, Long> getEstimatedRowStats(byte[] startRowKey,
      byte[] endRowKey) {
    return Util.getEstimatedRowStats(this, startRowKey, endRowKey);
  }

  /**
   * Hive returns the columns in order of their declaration for HBase tables.
   */
  @Override
  public List<Column> getColumnsInHiveOrder() {
    return getColumns();
  }

  @Override
  public TTableDescriptor toThriftDescriptor(int tableId,
      Set<Long> referencedPartitions) {
    TTableDescriptor tableDescriptor =
        new TTableDescriptor(tableId, TTableType.HBASE_TABLE, getTColumnDescriptors(),
            numClusteringCols_, name_, db_.getName());
    tableDescriptor.setHbaseTable(Util.getTHBaseTable(this));
    return tableDescriptor;
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
  public TCatalogObjectType getCatalogObjectType() {
    return TCatalogObjectType.TABLE;
  }

  @Override
  public TTable toThrift() {
    TTable table = super.toThrift();
    table.setTable_type(TTableType.HBASE_TABLE);
    table.setHbase_table(Util.getTHBaseTable(this));
    return table;
  }

  /**
   * Returns the storage handler class for HBase tables read by Hive.
   */
  @Override
  public String getStorageHandlerClassName() {
    return Util.HBASE_STORAGE_HANDLER;
  }

  /**
   * Fetch or use cached column families.
   */
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
}
