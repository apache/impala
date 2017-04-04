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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.xml.bind.DatatypeConverter;

import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.impala.analysis.ColumnDef;
import org.apache.impala.analysis.KuduPartitionParam;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.service.CatalogOpExecutor;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TKuduPartitionByHashParam;
import org.apache.impala.thrift.TKuduPartitionByRangeParam;
import org.apache.impala.thrift.TKuduPartitionParam;
import org.apache.impala.thrift.TKuduTable;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.thrift.TResultSetMetadata;
import org.apache.impala.thrift.TTable;
import org.apache.impala.thrift.TTableDescriptor;
import org.apache.impala.thrift.TTableType;
import org.apache.impala.util.KuduUtil;
import org.apache.impala.util.TResultRowBuilder;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.LocatedTablet;
import org.apache.kudu.client.PartitionSchema;
import org.apache.kudu.client.PartitionSchema.HashBucketSchema;
import org.apache.kudu.client.PartitionSchema.RangeSchema;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Representation of a Kudu table in the catalog cache.
 */
public class KuduTable extends Table {

  // Alias to the string key that identifies the storage handler for Kudu tables.
  public static final String KEY_STORAGE_HANDLER =
      hive_metastoreConstants.META_TABLE_STORAGE;

  // Key to access the table name from the table properties.
  public static final String KEY_TABLE_NAME = "kudu.table_name";

  // Key to access the columns used to build the (composite) key of the table.
  // Deprecated - Used only for error checking.
  public static final String KEY_KEY_COLUMNS = "kudu.key_columns";

  // Key to access the master host from the table properties. Error handling for
  // this string is done in the KuduClient library.
  // TODO: Rename kudu.master_addresses to kudu.master_host will break compatibility
  // with older versions.
  public static final String KEY_MASTER_HOSTS = "kudu.master_addresses";

  // Kudu specific value for the storage handler table property keyed by
  // KEY_STORAGE_HANDLER.
  // TODO: Fix the storage handler name (see IMPALA-4271).
  public static final String KUDU_STORAGE_HANDLER =
      "com.cloudera.kudu.hive.KuduStorageHandler";

  // Key to specify the number of tablet replicas.
  public static final String KEY_TABLET_REPLICAS = "kudu.num_tablet_replicas";

  // Table name in the Kudu storage engine. It may not neccessarily be the same as the
  // table name specified in the CREATE TABLE statement; the latter
  // is stored in Table.name_. Reasons why KuduTable.kuduTableName_ and Table.name_ may
  // differ:
  // 1. For managed tables, 'kuduTableName_' is prefixed with 'impala::<db_name>' to
  // avoid conficts. TODO: Remove this when Kudu supports databases.
  // 2. The user may specify a table name using the 'kudu.table_name' table property.
  private String kuduTableName_;

  // Comma separated list of Kudu master hosts with optional ports.
  private String kuduMasters_;

  // Primary key column names.
  private final List<String> primaryKeyColumnNames_ = Lists.newArrayList();

  // Partitioning schemes of this Kudu table. Both range and hash-based partitioning are
  // supported.
  private final List<KuduPartitionParam> partitionBy_ = Lists.newArrayList();

  // Schema of the underlying Kudu table.
  private org.apache.kudu.Schema kuduSchema_;

  protected KuduTable(org.apache.hadoop.hive.metastore.api.Table msTable,
      Db db, String name, String owner) {
    super(msTable, db, name, owner);
    kuduTableName_ = msTable.getParameters().get(KuduTable.KEY_TABLE_NAME);
    kuduMasters_ = msTable.getParameters().get(KuduTable.KEY_MASTER_HOSTS);
  }

  @Override
  public TCatalogObjectType getCatalogObjectType() { return TCatalogObjectType.TABLE; }

  @Override
  public String getStorageHandlerClassName() { return KUDU_STORAGE_HANDLER; }

  /**
   * Returns the columns in the order they have been created
   */
  @Override
  public ArrayList<Column> getColumnsInHiveOrder() { return getColumns(); }

  public static boolean isKuduTable(org.apache.hadoop.hive.metastore.api.Table msTbl) {
    return KUDU_STORAGE_HANDLER.equals(msTbl.getParameters().get(KEY_STORAGE_HANDLER));
  }

  public String getKuduTableName() { return kuduTableName_; }
  public String getKuduMasterHosts() { return kuduMasters_; }
  public org.apache.kudu.Schema getKuduSchema() { return kuduSchema_; }

  public List<String> getPrimaryKeyColumnNames() {
    return ImmutableList.copyOf(primaryKeyColumnNames_);
  }

  public List<KuduPartitionParam> getPartitionBy() {
    return ImmutableList.copyOf(partitionBy_);
  }

  /**
   * Returns the range-based partitioning of this table if it exists, null otherwise.
   */
  private KuduPartitionParam getRangePartitioning() {
    for (KuduPartitionParam partitionParam: partitionBy_) {
      if (partitionParam.getType() == KuduPartitionParam.Type.RANGE) {
        return partitionParam;
      }
    }
    return null;
  }

  /**
   * Returns the column names of the table's range-based partitioning or an empty
   * list if the table doesn't have a range-based partitioning.
   */
  public List<String> getRangePartitioningColNames() {
    KuduPartitionParam rangePartitioning = getRangePartitioning();
    if (rangePartitioning == null) return Collections.<String>emptyList();
    return rangePartitioning.getColumnNames();
  }

  /**
   * Loads the metadata of a Kudu table.
   *
   * Schema and partitioning schemes are loaded directly from Kudu whereas column stats
   * are loaded from HMS. The function also updates the table schema in HMS in order to
   * propagate alterations made to the Kudu table to HMS.
   */
  @Override
  public void load(boolean dummy /* not used */, IMetaStoreClient msClient,
      org.apache.hadoop.hive.metastore.api.Table msTbl) throws TableLoadingException {
    msTable_ = msTbl;
    // This is set to 0 for Kudu tables.
    // TODO: Change this to reflect the number of pk columns and modify all the
    // places (e.g. insert stmt) that currently make use of this parameter.
    numClusteringCols_ = 0;
    kuduTableName_ = msTable_.getParameters().get(KuduTable.KEY_TABLE_NAME);
    Preconditions.checkNotNull(kuduTableName_);
    kuduMasters_ = msTable_.getParameters().get(KuduTable.KEY_MASTER_HOSTS);
    Preconditions.checkNotNull(kuduMasters_);
    org.apache.kudu.client.KuduTable kuduTable = null;
    numRows_ = getRowCount(msTable_.getParameters());

    // Connect to Kudu to retrieve table metadata
    try (KuduClient kuduClient = KuduUtil.createKuduClient(getKuduMasterHosts())) {
      kuduTable = kuduClient.openTable(kuduTableName_);
    } catch (KuduException e) {
      throw new TableLoadingException(String.format(
          "Error opening Kudu table '%s', Kudu error: %s",
          kuduTableName_, e.getMessage()));
    }
    Preconditions.checkNotNull(kuduTable);

    // Load metadata from Kudu and HMS
    try {
      loadSchema(kuduTable);
      loadPartitionByParams(kuduTable);
      loadAllColumnStats(msClient);
    } catch (ImpalaRuntimeException e) {
      throw new TableLoadingException("Error loading metadata for Kudu table " +
          kuduTableName_, e);
    }

    // Update the table schema in HMS.
    try {
      long lastDdlTime = CatalogOpExecutor.calculateDdlTime(msTable_);
      msTable_.putToParameters("transient_lastDdlTime", Long.toString(lastDdlTime));
      msTable_.putToParameters(StatsSetupConst.DO_NOT_UPDATE_STATS,
          StatsSetupConst.TRUE);
      msClient.alter_table(msTable_.getDbName(), msTable_.getTableName(), msTable_);
    } catch (TException e) {
      throw new TableLoadingException(e.getMessage());
    }
  }

  /**
   * Loads the schema from the Kudu table including column definitions and primary key
   * columns. Replaces the columns in the HMS table with the columns from the Kudu table.
   * Throws an ImpalaRuntimeException if Kudu column data types cannot be mapped to
   * Impala data types.
   */
  private void loadSchema(org.apache.kudu.client.KuduTable kuduTable)
      throws ImpalaRuntimeException {
    Preconditions.checkNotNull(kuduTable);
    clearColumns();
    primaryKeyColumnNames_.clear();
    List<FieldSchema> cols = msTable_.getSd().getCols();
    cols.clear();
    int pos = 0;
    kuduSchema_ = kuduTable.getSchema();
    for (ColumnSchema colSchema: kuduSchema_.getColumns()) {
      KuduColumn kuduCol = KuduColumn.fromColumnSchema(colSchema, pos);
      Preconditions.checkNotNull(kuduCol);
      // Add the HMS column
      cols.add(new FieldSchema(kuduCol.getName(), kuduCol.getType().toSql().toLowerCase(),
          null));
      if (kuduCol.isKey()) primaryKeyColumnNames_.add(kuduCol.getName());
      addColumn(kuduCol);
      ++pos;
    }
  }

  private void loadPartitionByParams(org.apache.kudu.client.KuduTable kuduTable) {
    Preconditions.checkNotNull(kuduTable);
    Schema tableSchema = kuduTable.getSchema();
    PartitionSchema partitionSchema = kuduTable.getPartitionSchema();
    Preconditions.checkState(!colsByPos_.isEmpty());
    partitionBy_.clear();
    for (HashBucketSchema hashBucketSchema: partitionSchema.getHashBucketSchemas()) {
      List<String> columnNames = Lists.newArrayList();
      for (int colId: hashBucketSchema.getColumnIds()) {
        columnNames.add(getColumnNameById(tableSchema, colId));
      }
      partitionBy_.add(KuduPartitionParam.createHashParam(columnNames,
          hashBucketSchema.getNumBuckets()));
    }
    RangeSchema rangeSchema = partitionSchema.getRangeSchema();
    List<Integer> columnIds = rangeSchema.getColumns();
    if (columnIds.isEmpty()) return;
    List<String> columnNames = Lists.newArrayList();
    for (int colId: columnIds) columnNames.add(getColumnNameById(tableSchema, colId));
    // We don't populate the split values because Kudu's API doesn't currently support
    // retrieving the split values for range partitions.
    // TODO: File a Kudu JIRA.
    partitionBy_.add(KuduPartitionParam.createRangeParam(columnNames, null));
  }

  /**
   * Returns the name of a Kudu column with id 'colId'.
   */
  private String getColumnNameById(Schema tableSchema, int colId) {
    Preconditions.checkNotNull(tableSchema);
    ColumnSchema col = tableSchema.getColumnByIndex(tableSchema.getColumnIndex(colId));
    Preconditions.checkNotNull(col);
    return col.getName();
  }

  /**
   * Creates a temporary KuduTable object populated with the specified properties but has
   * an invalid TableId and is not added to the Kudu storage engine or the
   * HMS. This is used for CTAS statements.
   */
  public static KuduTable createCtasTarget(Db db,
      org.apache.hadoop.hive.metastore.api.Table msTbl, List<ColumnDef> columnDefs,
      List<String> primaryKeyColumnNames, List<KuduPartitionParam> partitionParams) {
    KuduTable tmpTable = new KuduTable(msTbl, db, msTbl.getTableName(), msTbl.getOwner());
    int pos = 0;
    for (ColumnDef colDef: columnDefs) {
      tmpTable.addColumn(new Column(colDef.getColName(), colDef.getType(), pos++));
    }
    tmpTable.primaryKeyColumnNames_.addAll(primaryKeyColumnNames);
    tmpTable.partitionBy_.addAll(partitionParams);
    return tmpTable;
  }

  @Override
  public TTable toThrift() {
    TTable table = super.toThrift();
    table.setTable_type(TTableType.KUDU_TABLE);
    table.setKudu_table(getTKuduTable());
    return table;
  }

  @Override
  protected void loadFromThrift(TTable thriftTable) throws TableLoadingException {
    super.loadFromThrift(thriftTable);
    TKuduTable tkudu = thriftTable.getKudu_table();
    kuduTableName_ = tkudu.getTable_name();
    kuduMasters_ = Joiner.on(',').join(tkudu.getMaster_addresses());
    primaryKeyColumnNames_.clear();
    primaryKeyColumnNames_.addAll(tkudu.getKey_columns());
    loadPartitionByParamsFromThrift(tkudu.getPartition_by());
  }

  private void loadPartitionByParamsFromThrift(List<TKuduPartitionParam> params) {
    partitionBy_.clear();
    for (TKuduPartitionParam param: params) {
      if (param.isSetBy_hash_param()) {
        TKuduPartitionByHashParam hashParam = param.getBy_hash_param();
        partitionBy_.add(KuduPartitionParam.createHashParam(
            hashParam.getColumns(), hashParam.getNum_partitions()));
      } else {
        Preconditions.checkState(param.isSetBy_range_param());
        TKuduPartitionByRangeParam rangeParam = param.getBy_range_param();
        partitionBy_.add(KuduPartitionParam.createRangeParam(rangeParam.getColumns(),
            null));
      }
    }
  }

  @Override
  public TTableDescriptor toThriftDescriptor(int tableId,
      Set<Long> referencedPartitions) {
    TTableDescriptor desc = new TTableDescriptor(tableId, TTableType.KUDU_TABLE,
        getTColumnDescriptors(), numClusteringCols_, kuduTableName_, db_.getName());
    desc.setKuduTable(getTKuduTable());
    return desc;
  }

  private TKuduTable getTKuduTable() {
    TKuduTable tbl = new TKuduTable();
    tbl.setKey_columns(Preconditions.checkNotNull(primaryKeyColumnNames_));
    tbl.setMaster_addresses(Lists.newArrayList(kuduMasters_.split(",")));
    tbl.setTable_name(kuduTableName_);
    Preconditions.checkNotNull(partitionBy_);
    // IMPALA-5154: partitionBy_ may be empty if Kudu table created outside Impala,
    // partition_by must be explicitly created because the field is required.
    tbl.partition_by = Lists.newArrayList();
    for (KuduPartitionParam partitionParam: partitionBy_) {
      tbl.addToPartition_by(partitionParam.toThrift());
    }
    return tbl;
  }

  public boolean isPrimaryKeyColumn(String name) {
    return primaryKeyColumnNames_.contains(name);
  }

  public TResultSet getTableStats() throws ImpalaRuntimeException {
    TResultSet result = new TResultSet();
    TResultSetMetadata resultSchema = new TResultSetMetadata();
    result.setSchema(resultSchema);

    resultSchema.addToColumns(new TColumn("# Rows", Type.INT.toThrift()));
    resultSchema.addToColumns(new TColumn("Start Key", Type.STRING.toThrift()));
    resultSchema.addToColumns(new TColumn("Stop Key", Type.STRING.toThrift()));
    resultSchema.addToColumns(new TColumn("Leader Replica", Type.STRING.toThrift()));
    resultSchema.addToColumns(new TColumn("# Replicas", Type.INT.toThrift()));

    try (KuduClient client = KuduUtil.createKuduClient(getKuduMasterHosts())) {
      org.apache.kudu.client.KuduTable kuduTable = client.openTable(kuduTableName_);
      List<LocatedTablet> tablets =
          kuduTable.getTabletsLocations(BackendConfig.INSTANCE.getKuduClientTimeoutMs());
      if (tablets.isEmpty()) {
        TResultRowBuilder builder = new TResultRowBuilder();
        result.addToRows(
            builder.add("-1").add("N/A").add("N/A").add("N/A").add("-1").get());
        return result;
      }
      for (LocatedTablet tab: tablets) {
        TResultRowBuilder builder = new TResultRowBuilder();
        builder.add("-1");   // The Kudu client API doesn't expose tablet row counts.
        builder.add(DatatypeConverter.printHexBinary(
            tab.getPartition().getPartitionKeyStart()));
        builder.add(DatatypeConverter.printHexBinary(
            tab.getPartition().getPartitionKeyEnd()));
        LocatedTablet.Replica leader = tab.getLeaderReplica();
        if (leader == null) {
          // Leader might be null, if it is not yet available (e.g. during
          // leader election in Kudu)
          builder.add("Leader n/a");
        } else {
          builder.add(leader.getRpcHost() + ":" + leader.getRpcPort().toString());
        }
        builder.add(tab.getReplicas().size());
        result.addToRows(builder.get());
      }

    } catch (Exception e) {
      throw new ImpalaRuntimeException("Error accessing Kudu for table stats.", e);
    }
    return result;
  }

  public TResultSet getRangePartitions() throws ImpalaRuntimeException {
    TResultSet result = new TResultSet();
    TResultSetMetadata resultSchema = new TResultSetMetadata();
    result.setSchema(resultSchema);

    // Build column header
    String header = "RANGE (" + Joiner.on(',').join(getRangePartitioningColNames()) + ")";
    resultSchema.addToColumns(new TColumn(header, Type.STRING.toThrift()));
    try (KuduClient client = KuduUtil.createKuduClient(getKuduMasterHosts())) {
      org.apache.kudu.client.KuduTable kuduTable = client.openTable(kuduTableName_);
      // The Kudu table API will return the partitions in sorted order by value.
      List<String> partitions = kuduTable.getFormattedRangePartitions(
          BackendConfig.INSTANCE.getKuduClientTimeoutMs());
      if (partitions.isEmpty()) {
        TResultRowBuilder builder = new TResultRowBuilder();
        result.addToRows(builder.add("").get());
        return result;
      }
      for (String partition: partitions) {
        TResultRowBuilder builder = new TResultRowBuilder();
        builder.add(partition);
        result.addToRows(builder.get());
      }
    } catch (Exception e) {
      throw new ImpalaRuntimeException("Error accessing Kudu for table partitions.", e);
    }
    return result;
  }
}
