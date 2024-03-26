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

import static org.apache.impala.analysis.Analyzer.ACCESSTYPE_READ;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.impala.common.InternalException;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.service.FeSupport;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TQueryTableColumn;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.thrift.TResultSetMetadata;
import org.apache.impala.thrift.TSystemTable;
import org.apache.impala.thrift.TSystemTableName;
import org.apache.impala.thrift.TTable;
import org.apache.impala.thrift.TTableDescriptor;
import org.apache.impala.thrift.TTableType;
import org.apache.impala.util.EventSequence;
import org.apache.impala.util.TResultRowBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.base.Preconditions;

/**
 * Represents a system table reflecting backend internal state.
 */
public final class SystemTable extends Table {
  public static final String QUERY_LIVE = "impala_query_live";
  private static final Map<String, TSystemTableName> SYSTEM_TABLE_NAME_MAP =
      ImmutableMap.of(QUERY_LIVE, TSystemTableName.QUERY_LIVE);

  // Constants declaring how durations measured in milliseconds will be stored in the db.
  // Must match constants with the same name declared in workload-management-fields.cc.
  private static final int DURATION_DECIMAL_PRECISION = 18;
  private static final int DURATION_DECIMAL_SCALE = 3;

  private SystemTable(org.apache.hadoop.hive.metastore.api.Table msTable, Db db,
      String name, String owner) {
    super(msTable, db, name, owner);
  }

  // Get Type for a TQueryTableColumn
  private static Type getColumnType(TQueryTableColumn column) {
    switch (column) {
      case START_TIME_UTC:
        return Type.TIMESTAMP;
      case PER_HOST_MEM_ESTIMATE:
      case DEDICATED_COORD_MEM_ESTIMATE:
      case CLUSTER_MEMORY_ADMITTED:
      case NUM_ROWS_FETCHED:
      case ROW_MATERIALIZATION_ROWS_PER_SEC:
      case COMPRESSED_BYTES_SPILLED:
      case BYTES_READ_CACHE_TOTAL:
      case BYTES_READ_TOTAL:
      case PERNODE_PEAK_MEM_MIN:
      case PERNODE_PEAK_MEM_MAX:
      case PERNODE_PEAK_MEM_MEAN:
        return Type.BIGINT;
      case BACKENDS_COUNT:
        return Type.INT;
      case TOTAL_TIME_MS:
      case ROW_MATERIALIZATION_TIME_MS:
      case EVENT_PLANNING_FINISHED:
      case EVENT_SUBMIT_FOR_ADMISSION:
      case EVENT_COMPLETED_ADMISSION:
      case EVENT_ALL_BACKENDS_STARTED:
      case EVENT_ROWS_AVAILABLE:
      case EVENT_FIRST_ROW_FETCHED:
      case EVENT_LAST_ROW_FETCHED:
      case EVENT_UNREGISTER_QUERY:
      case READ_IO_WAIT_TOTAL_MS:
      case READ_IO_WAIT_MEAN_MS:
        return ScalarType.createDecimalType(
            DURATION_DECIMAL_PRECISION, DURATION_DECIMAL_SCALE);
      default:
        return Type.STRING;
    }
  }

  public static SystemTable CreateQueryLiveTable(Db db, String owner) {
    List<FieldSchema> fsList = new ArrayList<FieldSchema>();
    for (TQueryTableColumn column : TQueryTableColumn.values()) {
      // The type string must be lowercase for Hive to read the column metadata properly.
      String typeSql = getColumnType(column).toSql().toLowerCase();
      FieldSchema fs = new FieldSchema(column.name().toLowerCase(), typeSql, "");
      fsList.add(fs);
    }
    org.apache.hadoop.hive.metastore.api.Table msTable =
        createMetastoreTable(db.getName(), QUERY_LIVE, owner, fsList);

    SystemTable table = new SystemTable(msTable, db, QUERY_LIVE, owner);
    for (TQueryTableColumn column : TQueryTableColumn.values()) {
      table.addColumn(new Column(
          column.name().toLowerCase(), getColumnType(column), column.ordinal()));
    }
    return table;
  }

  public TSystemTableName getSystemTableName() {
    return SYSTEM_TABLE_NAME_MAP.get(getName());
  }

  @Override
  public TTableDescriptor toThriftDescriptor(int tableId,
      Set<Long> referencedPartitions) {
    // Create thrift descriptors to send to the BE.
    TTableDescriptor tableDescriptor =
        new TTableDescriptor(tableId, TTableType.SYSTEM_TABLE, getTColumnDescriptors(),
            numClusteringCols_, name_, db_.getName());
    tableDescriptor.setSystemTable(getTSystemTable());
    return tableDescriptor;
  }

  @Override
  public long getNumRows() {
    try {
      // Return an estimate of the number of live queries assuming balanced load across
      // coordinators.
      return FeSupport.NumLiveQueries() * FeSupport.GetCoordinators().getAddressesSize();
    } catch (InternalException e) {
      return super.getNumRows();
    }
  }

  /**
   * Returns a thrift structure for the system table.
   */
  private TSystemTable getTSystemTable() {
    return new TSystemTable(getSystemTableName());
  }

  @Override
  public TCatalogObjectType getCatalogObjectType() {
    return TCatalogObjectType.TABLE;
  }

  @Override
  public void load(boolean reuseMetadata, IMetaStoreClient client,
      org.apache.hadoop.hive.metastore.api.Table msTbl, String reason,
      EventSequence catalogTimeline) throws TableLoadingException {
    // Table is always loaded.
    Preconditions.checkState(false);
  }

  /**
   * Returns a thrift structure representing the table.
   */
  @Override
  public TTable toThrift() {
    TTable table = super.toThrift();
    table.setTable_type(TTableType.SYSTEM_TABLE);
    table.setSystem_table(getTSystemTable());
    return table;
  }

  /**
   * Returns statistics on this table as a tabular result set. Used for the SHOW
   * TABLE STATS statement. The schema of the returned TResultSet is set inside
   * this method.
   */
  public TResultSet getTableStats() {
    TResultSet result = new TResultSet();
    TResultSetMetadata resultSchema = new TResultSetMetadata();
    resultSchema.addToColumns(new TColumn("#Rows", Type.BIGINT.toThrift()));
    result.setSchema(resultSchema);
    TResultRowBuilder rowBuilder = new TResultRowBuilder();
    rowBuilder.add(getNumRows());
    result.addToRows(rowBuilder.get());
    return result;
  }

  private static org.apache.hadoop.hive.metastore.api.Table
      createMetastoreTable(String dbName, String tableName, String owner,
          List<FieldSchema> columns) {
    // Based on CatalogOpExecutor#createMetaStoreTable
    org.apache.hadoop.hive.metastore.api.Table tbl =
        new org.apache.hadoop.hive.metastore.api.Table();
    tbl.setDbName(dbName);
    tbl.setTableName(tableName);
    tbl.setOwner(owner);
    tbl.setParameters(new HashMap<String, String>());
    tbl.setTableType(TableType.MANAGED_TABLE.toString());
    tbl.setPartitionKeys(new ArrayList<FieldSchema>());
    if (MetastoreShim.getMajorVersion() > 2) {
      MetastoreShim.setTableAccessType(tbl, ACCESSTYPE_READ);
    }

    StorageDescriptor sd = new StorageDescriptor();
    sd.setSerdeInfo(new org.apache.hadoop.hive.metastore.api.SerDeInfo());
    sd.getSerdeInfo().setParameters(new HashMap<>());
    sd.setCompressed(false);
    sd.setBucketCols(new ArrayList<>(0));
    sd.setSortCols(new ArrayList<>(0));
    sd.setCols(columns);
    tbl.setSd(sd);

    return tbl;
  }
}
