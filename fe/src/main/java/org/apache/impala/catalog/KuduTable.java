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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.impala.analysis.ColumnDef;
import org.apache.impala.analysis.KuduPartitionParam;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TKuduPartitionByHashParam;
import org.apache.impala.thrift.TKuduPartitionByRangeParam;
import org.apache.impala.thrift.TKuduPartitionParam;
import org.apache.impala.thrift.TKuduTable;
import org.apache.impala.thrift.TTable;
import org.apache.impala.thrift.TTableDescriptor;
import org.apache.impala.thrift.TTableType;
import org.apache.impala.util.KuduUtil;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.HiveMetastoreConfig;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.thrift.TException;

import com.codahale.metrics.Timer;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Representation of a Kudu table in the catalog cache.
 */
public class KuduTable extends Table implements FeKuduTable {

  // Alias to the string key that identifies the storage handler for Kudu tables.
  public static final String KEY_STORAGE_HANDLER =
      hive_metastoreConstants.META_TABLE_STORAGE;

  // Key to access the table name from the table properties.
  public static final String KEY_TABLE_NAME = "kudu.table_name";

  // Key to access the table ID from the table properties.
  public static final String KEY_TABLE_ID = "kudu.table_id";

  // Key to access the columns used to build the (composite) key of the table.
  // Deprecated - Used only for error checking.
  public static final String KEY_KEY_COLUMNS = "kudu.key_columns";

  // Key to access the master host from the table properties. Error handling for
  // this string is done in the KuduClient library.
  // TODO: Rename kudu.master_addresses to kudu.master_host will break compatibility
  // with older versions.
  public static final String KEY_MASTER_HOSTS = "kudu.master_addresses";

  // Kudu specific value for the legacy storage handler table property keyed by
  // KEY_STORAGE_HANDLER. This is expected to be deprecated eventually.
  public static final String KUDU_LEGACY_STORAGE_HANDLER =
      "com.cloudera.kudu.hive.KuduStorageHandler";

  // Kudu specific value for the storage handler table property keyed by
  // KEY_STORAGE_HANDLER.
  public static final String KUDU_STORAGE_HANDLER =
      "org.apache.hadoop.hive.kudu.KuduStorageHandler";

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

  // Primary key column names, the column names are all in lower case.
  private final List<String> primaryKeyColumnNames_ = new ArrayList<>();

  // Partitioning schemes of this Kudu table. Both range and hash-based partitioning are
  // supported.
  private List<KuduPartitionParam> partitionBy_;

  // Schema of the underlying Kudu table.
  private org.apache.kudu.Schema kuduSchema_;

  protected KuduTable(org.apache.hadoop.hive.metastore.api.Table msTable,
      Db db, String name, String owner) {
    super(msTable, db, name, owner);
    kuduTableName_ = msTable.getParameters().get(KuduTable.KEY_TABLE_NAME);
    kuduMasters_ = msTable.getParameters().get(KuduTable.KEY_MASTER_HOSTS);
    if (kuduTableName_ == null || kuduTableName_.isEmpty()) {
      // When 'kudu.table_name' property is empty, it implies Kudu/HMS
      // integration is enabled.
      // TODO: remove this hack once Kudu support 'kudu.table_name'
      // property with the new storage handler.
      populateDefaultTableName(msTable, /* isHMSIntegrationEnabled */true);
    }
  }

  @Override
  public TCatalogObjectType getCatalogObjectType() { return TCatalogObjectType.TABLE; }

  @Override
  public String getStorageHandlerClassName() { return KUDU_STORAGE_HANDLER; }

  /**
   * Returns the columns in the order they have been created
   */
  @Override
  public List<Column> getColumnsInHiveOrder() { return getColumns(); }

  public static boolean isKuduStorageHandler(String handler) {
    return handler != null && (handler.equals(KUDU_LEGACY_STORAGE_HANDLER) ||
                               handler.equals(KUDU_STORAGE_HANDLER));
  }

  public static boolean isKuduTable(org.apache.hadoop.hive.metastore.api.Table msTbl) {
    return isKuduStorageHandler(msTbl.getParameters().get(KEY_STORAGE_HANDLER));
  }

  @Override
  public String getKuduTableName() { return kuduTableName_; }
  @Override
  public String getKuduMasterHosts() { return kuduMasters_; }

  public org.apache.kudu.Schema getKuduSchema() { return kuduSchema_; }

  @Override
  public List<String> getPrimaryKeyColumnNames() {
    return ImmutableList.copyOf(primaryKeyColumnNames_);
  }

  @Override
  public List<KuduPartitionParam> getPartitionBy() {
    Preconditions.checkState(partitionBy_ != null);
    return ImmutableList.copyOf(partitionBy_);
  }

  /**
   * Populates the default table name.
   */
  private void populateDefaultTableName(
      org.apache.hadoop.hive.metastore.api.Table msTbl,
      boolean isHMSIntegrationEnabled) {
    kuduTableName_ = KuduUtil.getDefaultKuduTableName(
        msTbl.getDbName(), msTbl.getTableName(), isHMSIntegrationEnabled);
    msTbl.getParameters().put(KuduTable.KEY_TABLE_NAME, kuduTableName_);
  }

  /**
   * Get the Hive Metastore configuration from Kudu masters.
   */
  private static HiveMetastoreConfig getHiveMetastoreConfig(String kuduMasters)
      throws ImpalaRuntimeException {
    Preconditions.checkNotNull(kuduMasters);
    Preconditions.checkArgument(!kuduMasters.isEmpty());
    KuduClient kuduClient = KuduUtil.getKuduClient(kuduMasters);
    HiveMetastoreConfig hmsConfig;
    try {
      hmsConfig = kuduClient.getHiveMetastoreConfig();
    } catch (KuduException e) {
      throw new ImpalaRuntimeException(
          String.format("Error determining if Kudu's integration with " +
              "the Hive Metastore is enabled: %s", e.getMessage()));
    }
    return hmsConfig;
  }

  /**
   * Check with Kudu master to see if Kudu's integration with the Hive Metastore
   * is enabled.
   */
  public static boolean isHMSIntegrationEnabled(String kuduMasters)
      throws ImpalaRuntimeException {
    return getHiveMetastoreConfig(kuduMasters) != null;
  }

  /**
   * Check with the Kudu master to see if its Kudu-HMS integration is enabled;
   * if so, validate that it is integrated with the same Hive Metastore that
   * Impala is configured to use.
   */
  public static boolean isHMSIntegrationEnabledAndValidate(String kuduMasters,
      String hmsUris) throws ImpalaRuntimeException {
    Preconditions.checkNotNull(hmsUris);
    Preconditions.checkArgument(!hmsUris.isEmpty());
    HiveMetastoreConfig hmsConfig = getHiveMetastoreConfig(kuduMasters);
    if (hmsConfig == null) {
      return false;
    }
    // Validate Kudu is configured to use the same HMS as Impala. We consider
    // it is the case as long as Kudu and Impala are configured to talk to
    // the HMS with the same host address(es).
    final String kuduHmsUris = hmsConfig.getHiveMetastoreUris();
    Set<String> hmsHosts;
    Set<String> kuduHmsHosts;
    try {
      hmsHosts = parseHosts(hmsUris);
      kuduHmsHosts = parseHosts(kuduHmsUris);
    } catch (URISyntaxException e) {
      throw new ImpalaRuntimeException(
          String.format("Error parsing URI: %s", e.getMessage()));
    }
    if (hmsHosts != null && kuduHmsHosts != null && hmsHosts.equals(kuduHmsHosts)) {
      return true;
    }
    throw new ImpalaRuntimeException(
       String.format("Kudu is integrated with a different Hive Metastore " +
           "than that used by Impala, Kudu is configured to use the HMS: " +
           "%s, while Impala is configured to use the HMS: %s",
           kuduHmsUris, hmsUris));
  }

  /**
   * Parse the given URIs and return a set of hosts in the URIs.
   */
  private static Set<String> parseHosts(String uris) throws URISyntaxException {
    String[] urisString = uris.split(",");
    Set<String> parsedHosts = new HashSet<>();

    for (String s : urisString) {
      s.trim();
      URI tmpUri = new URI(s);
      parsedHosts.add(tmpUri.getHost());
    }
    return parsedHosts;
  }

  /**
   * Load schema and partitioning schemes directly from Kudu.
   */
  public void loadSchemaFromKudu() throws ImpalaRuntimeException {
    // This is set to 0 for Kudu tables.
    // TODO: Change this to reflect the number of pk columns and modify all the
    // places (e.g. insert stmt) that currently make use of this parameter.
    numClusteringCols_ = 0;
    org.apache.kudu.client.KuduTable kuduTable = null;
    // Connect to Kudu to retrieve table metadata
    KuduClient kuduClient = KuduUtil.getKuduClient(getKuduMasterHosts());
    try {
      kuduTable = kuduClient.openTable(kuduTableName_);
    } catch (KuduException e) {
      throw new ImpalaRuntimeException(
          String.format("Error opening Kudu table '%s', Kudu error: %s", kuduTableName_,
              e.getMessage()));
    }
    Preconditions.checkNotNull(kuduTable);

    loadSchema(kuduTable);
    Preconditions.checkState(!colsByPos_.isEmpty());
    partitionBy_ = Utils.loadPartitionByParams(kuduTable);
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
      org.apache.hadoop.hive.metastore.api.Table msTbl, String reason)
      throws TableLoadingException {
    final Timer.Context context =
        getMetrics().getTimer(Table.LOAD_DURATION_METRIC).time();
    try {
      // Copy the table to check later if anything has changed.
      msTable_ = msTbl.deepCopy();
      kuduTableName_ = msTable_.getParameters().get(KuduTable.KEY_TABLE_NAME);
      if (kuduTableName_ == null || kuduTableName_.isEmpty()) {
        // When 'kudu.table_name' property is empty, it implies Kudu/HMS
        // integration is enabled.
        // TODO: remove this hack once Kudu support 'kudu.table_name'
        // property with the new storage handler.
        populateDefaultTableName(msTable_, /* isHMSIntegrationEnabled */true);
      }
      kuduMasters_ = msTable_.getParameters().get(KuduTable.KEY_MASTER_HOSTS);
      if (kuduMasters_ == null || kuduMasters_.isEmpty()) {
        throw new TableLoadingException("No " + KuduTable.KEY_MASTER_HOSTS +
            " property found for Kudu table " + kuduTableName_);
      }
      setTableStats(msTable_);
      // Load metadata from Kudu and HMS
      try {
        loadSchemaFromKudu();
        loadAllColumnStats(msClient);
      } catch (ImpalaRuntimeException e) {
        throw new TableLoadingException("Error loading metadata for Kudu table " +
            kuduTableName_, e);
      }
      refreshLastUsedTime();
      // Avoid updating HMS if the schema didn't change.
      if (msTable_.equals(msTbl)) return;

      // Update the table schema in HMS.
      try {
        // HMS would fill this table property if it was not set, but as metadata written
        // with alter_table is not read back in case of Kudu tables, it has to be set here
        // to ensure that HMS and catalogd have the same timestamp.
        updateTimestampProperty(msTable_, TBL_PROP_LAST_DDL_TIME);
        msTable_.putToParameters(StatsSetupConst.DO_NOT_UPDATE_STATS,
            StatsSetupConst.TRUE);
        msClient.alter_table(msTable_.getDbName(), msTable_.getTableName(), msTable_);
      } catch (TException e) {
        throw new TableLoadingException(e.getMessage());
      }
    } finally {
      context.stop();
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
    boolean isHMSIntegrationEnabled = KuduTable.isHMSIntegrationEnabled(kuduMasters_);
    List<FieldSchema> cols = msTable_.getSd().getCols();
    if (!isHMSIntegrationEnabled) {
      cols.clear();
    }

    int pos = 0;
    kuduSchema_ = kuduTable.getSchema();
    for (ColumnSchema colSchema: kuduSchema_.getColumns()) {
      KuduColumn kuduCol = KuduColumn.fromColumnSchema(colSchema, pos);
      Preconditions.checkNotNull(kuduCol);
      // Only update the HMS column definition when Kudu/HMS integration
      // is disabled.
      if (!isHMSIntegrationEnabled) {
        cols.add(new FieldSchema(kuduCol.getName(),
            kuduCol.getType().toSql().toLowerCase(), null));
      }
      if (kuduCol.isKey()) primaryKeyColumnNames_.add(kuduCol.getName());
      addColumn(kuduCol);
      ++pos;
    }
  }


  /**
   * Creates a temporary KuduTable object populated with the specified properties but has
   * an invalid TableId and is not added to the Kudu storage engine or the
   * HMS. This is used for CTAS statements.
   */
  public static KuduTable createCtasTarget(Db db,
      org.apache.hadoop.hive.metastore.api.Table msTbl, List<ColumnDef> columnDefs,
      List<ColumnDef> primaryKeyColumnDefs, List<KuduPartitionParam> partitionParams) {
    KuduTable tmpTable = new KuduTable(msTbl, db, msTbl.getTableName(), msTbl.getOwner());
    int pos = 0;
    for (ColumnDef colDef: columnDefs) {
      tmpTable.addColumn(new Column(colDef.getColName(), colDef.getType(), pos++));
    }
    for (ColumnDef pkColDef: primaryKeyColumnDefs) {
      tmpTable.primaryKeyColumnNames_.add(pkColDef.getColName());
    }
    tmpTable.partitionBy_ = ImmutableList.copyOf(partitionParams);
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
    partitionBy_ = loadPartitionByParamsFromThrift(tkudu.getPartition_by());
  }

  private static List<KuduPartitionParam> loadPartitionByParamsFromThrift(
      List<TKuduPartitionParam> params) {
    List<KuduPartitionParam> ret= new ArrayList<>();
    for (TKuduPartitionParam param: params) {
      if (param.isSetBy_hash_param()) {
        TKuduPartitionByHashParam hashParam = param.getBy_hash_param();
        ret.add(KuduPartitionParam.createHashParam(
            hashParam.getColumns(), hashParam.getNum_partitions()));
      } else {
        Preconditions.checkState(param.isSetBy_range_param());
        TKuduPartitionByRangeParam rangeParam = param.getBy_range_param();
        ret.add(KuduPartitionParam.createRangeParam(rangeParam.getColumns(),
            null));
      }
    }
    return ret;
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
    tbl.partition_by = new ArrayList<>();
    for (KuduPartitionParam partitionParam: partitionBy_) {
      tbl.addToPartition_by(partitionParam.toThrift());
    }
    return tbl;
  }
}
