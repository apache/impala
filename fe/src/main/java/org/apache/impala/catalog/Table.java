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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.impala.analysis.TableName;
import org.apache.impala.catalog.events.InFlightEvents;
import org.apache.impala.catalog.monitor.CatalogMonitor;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.common.Metrics;
import org.apache.impala.common.Pair;
import org.apache.impala.common.RuntimeEnv;
import org.apache.impala.service.MetadataOp;
import org.apache.impala.thrift.TAccessLevel;
import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TColumnDescriptor;
import org.apache.impala.thrift.TGetPartialCatalogObjectRequest;
import org.apache.impala.thrift.TGetPartialCatalogObjectResponse;
import org.apache.impala.thrift.TPartialTableInfo;
import org.apache.impala.thrift.TTable;
import org.apache.impala.thrift.TTableDescriptor;
import org.apache.impala.thrift.TTableInfoSelector;
import org.apache.impala.thrift.TTableStats;
import org.apache.impala.util.AcidUtils;
import org.apache.impala.util.HdfsCachingUtil;
import org.apache.log4j.Logger;

import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Base class for table metadata.
 *
 * This includes the concept of clustering columns, which are columns by which the table
 * data is physically clustered. In other words, if two rows share the same values
 * for the clustering columns, those two rows are most likely colocated. Note that this
 * is more general than Hive's CLUSTER BY ... INTO BUCKETS clause (which partitions
 * a key range into a fixed number of buckets).
 */
public abstract class Table extends CatalogObjectImpl implements FeTable {
  private static final Logger LOG = Logger.getLogger(Table.class);
  protected org.apache.hadoop.hive.metastore.api.Table msTable_;
  protected final Db db_;
  protected final String name_;
  protected final String owner_;
  protected TAccessLevel accessLevel_ = TAccessLevel.READ_WRITE;
  // Lock protecting this table
  private final ReentrantLock tableLock_ = new ReentrantLock();

  // Number of clustering columns.
  protected int numClusteringCols_;

  // Contains the estimated number of rows and optional file bytes. Non-null. Member
  // values of -1 indicate an unknown statistic.
  protected TTableStats tableStats_;

  // Estimated size (in bytes) of this table metadata. Stored in an AtomicLong to allow
  // this field to be accessed without holding the table lock.
  protected AtomicLong estimatedMetadataSize_ = new AtomicLong(0);

  // Number of metadata operations performed on that table since it was loaded.
  // Stored in an AtomicLong to allow this field to be accessed without holding the
  // table lock.
  protected AtomicLong metadataOpsCount_ = new AtomicLong(0);

  // Number of files that the table has.
  // Stored in an AtomicLong to allow this field to be accessed without holding the
  // table lock.
  protected AtomicLong numFiles_ = new AtomicLong(0);

  // Metrics for this table
  protected final Metrics metrics_ = new Metrics();

  // colsByPos[i] refers to the ith column in the table. The first numClusteringCols are
  // the clustering columns.
  protected final ArrayList<Column> colsByPos_ = new ArrayList<>();

  // map from lowercase column name to Column object.
  private final Map<String, Column> colsByName_ = new HashMap<>();

  // List of SQL constraints associated with the table.
  private final SqlConstraints sqlConstraints_ = new SqlConstraints(new ArrayList<>(),
      new ArrayList<>());

  // Type of this table (array of struct) that mirrors the columns. Useful for analysis.
  protected final ArrayType type_ = new ArrayType(new StructType());

  // True if this object is stored in an Impalad catalog cache.
  protected boolean storedInImpaladCatalogCache_ = false;

  // Time spent in the source systems loading/reloading the fs metadata for the table.
  protected long storageMetadataLoadTime_ = 0;

  // Last used time of this table in nanoseconds as returned by
  // CatalogdTableInvalidator.nanoTime(). This is only set in catalogd and not used by
  // impalad.
  protected long lastUsedTime_;

  // tracks the in-flight metastore events for this table. Used by Events processor to
  // avoid unnecessary refresh when the event is received
  private final InFlightEvents inFlightEvents = new InFlightEvents();

  // Table metrics. These metrics are applicable to all table types. Each subclass of
  // Table can define additional metrics specific to that table type.
  public static final String REFRESH_DURATION_METRIC = "refresh-duration";
  public static final String ALTER_DURATION_METRIC = "alter-duration";

  // The time to load all the table metadata.
  public static final String LOAD_DURATION_METRIC = "load-duration";

  // Storage related to file system operations during metadata loading.
  // The amount of time spent loading metadata from the underlying storage layer.
  public static final String LOAD_DURATION_STORAGE_METADATA =
      "load-duration.storage-metadata";

  // The time for HMS to fetch table object and the real schema loading time.
  // Normally, the code path is "msClient.getHiveClient().getTable(dbName, tblName)"
  public static final String HMS_LOAD_TBL_SCHEMA = "hms-load-tbl-schema";

  // Load all column stats, this is part of table metadata loading
  // The code path is HdfsTable.loadAllColumnStats()
  public static final String LOAD_DURATION_ALL_COLUMN_STATS =
      "load-duration.all-column-stats";

  // Table property key for storing the time of the last DDL operation.
  public static final String TBL_PROP_LAST_DDL_TIME = "transient_lastDdlTime";

  // Table property key for storing the last time when Impala executed COMPUTE STATS.
  public static final String TBL_PROP_LAST_COMPUTE_STATS_TIME =
      "impala.lastComputeStatsTime";

  // Table property key for storing table type externality.
  public static final String TBL_PROP_EXTERNAL_TABLE = "EXTERNAL";

  // Table property key to determined if HMS translated a managed table to external table
  public static final String TBL_PROP_EXTERNAL_TABLE_PURGE = "external.table.purge";

  protected Table(org.apache.hadoop.hive.metastore.api.Table msTable, Db db,
      String name, String owner) {
    msTable_ = msTable;
    db_ = db;
    name_ = name.toLowerCase();
    owner_ = owner;
    tableStats_ = new TTableStats(-1);
    tableStats_.setTotal_file_bytes(-1);
    initMetrics();
  }

  /**
   * Returns if the given HMS table is an external table (uses table type if
   * available or else uses table properties). Implementation is based on org.apache
   * .hadoop.hive.metastore.utils.MetaStoreUtils.isExternalTable()
   */
  public static boolean isExternalTable(
      org.apache.hadoop.hive.metastore.api.Table msTbl) {
    // HIVE-19253: table property can also indicate an external table.
    return (msTbl.getTableType().equalsIgnoreCase(TableType.EXTERNAL_TABLE.toString()) ||
        ("TRUE").equalsIgnoreCase(msTbl.getParameters().get(TBL_PROP_EXTERNAL_TABLE)));
  }

  /**
   * In certain versions of Hive (See HIVE-22158) HMS translates a managed table to a
   * external and sets additional property of "external.table.purge" = true. This
   * method can be used to identify such translated tables.
   */
  public static boolean isExternalPurgeTable(
      org.apache.hadoop.hive.metastore.api.Table msTbl) {
    return isExternalTable(msTbl) && Boolean
        .parseBoolean(msTbl.getParameters().get(TBL_PROP_EXTERNAL_TABLE_PURGE));
  }

  public ReentrantLock getLock() { return tableLock_; }
  @Override
  public abstract TTableDescriptor toThriftDescriptor(
      int tableId, Set<Long> referencedPartitions);

  @Override // FeTable
  public abstract TCatalogObjectType getCatalogObjectType();

  public long getMetadataOpsCount() { return metadataOpsCount_.get(); }
  public long getEstimatedMetadataSize() { return estimatedMetadataSize_.get(); }
  public long getNumFiles() { return numFiles_.get(); }
  public long getMedianTableLoadingTime() {
    return (long)metrics_.getTimer(LOAD_DURATION_METRIC).getSnapshot().getMedian();
  }
  public long getMaxTableLoadingTime() {
    return metrics_.getTimer(LOAD_DURATION_METRIC).getSnapshot().getMax();
  }
  public long get75TableLoadingTime() {
    return (long)metrics_.getTimer(LOAD_DURATION_METRIC).
        getSnapshot().get75thPercentile();
  }
  public long get95TableLoadingTime() {
    return (long)metrics_.getTimer(LOAD_DURATION_METRIC).
        getSnapshot().get95thPercentile();
  }
  public long get99TableLoadingTime() {
    return (long)metrics_.getTimer(LOAD_DURATION_METRIC).
        getSnapshot().get99thPercentile();
  }
  public long getTableLoadingCounts() {
    return metrics_.getTimer(LOAD_DURATION_METRIC).getCount();
  }

  public void setEstimatedMetadataSize(long estimatedMetadataSize) {
    estimatedMetadataSize_.set(estimatedMetadataSize);
    if (!isStoredInImpaladCatalogCache()) {
      CatalogMonitor.INSTANCE.getCatalogTableMetrics().updateLargestTables(this);
    }
  }

  public void incrementMetadataOpsCount() {
    metadataOpsCount_.incrementAndGet();
    if (!isStoredInImpaladCatalogCache()) {
      CatalogMonitor.INSTANCE.getCatalogTableMetrics().updateFrequentlyAccessedTables(
          this);
    }
  }

  public void updateTableLoadingTime() {
    if (!isStoredInImpaladCatalogCache()) {
      CatalogMonitor.INSTANCE.getCatalogTableMetrics().updateLongMetadataLoadingTables(
          this);
    }
  }

  public void setNumFiles(long numFiles) {
    numFiles_.set(numFiles);
    if (!isStoredInImpaladCatalogCache()) {
      CatalogMonitor.INSTANCE.getCatalogTableMetrics().updateHighFileCountTables(this);
    }
  }

  public void initMetrics() {
    metrics_.addTimer(REFRESH_DURATION_METRIC);
    metrics_.addTimer(ALTER_DURATION_METRIC);
    metrics_.addTimer(LOAD_DURATION_METRIC);
    metrics_.addTimer(LOAD_DURATION_STORAGE_METADATA);
    metrics_.addTimer(HMS_LOAD_TBL_SCHEMA);
    metrics_.addTimer(LOAD_DURATION_ALL_COLUMN_STATS);
  }

  public Metrics getMetrics() { return metrics_; }

  // Returns storage wait time during metadata load.
  public long getStorageLoadTime() { return storageMetadataLoadTime_; }

  // Returns true if this table reference comes from the impalad catalog cache or if it
  // is loaded from the testing framework. Returns false if this table reference points
  // to a table stored in the catalog server.
  public boolean isStoredInImpaladCatalogCache() {
    return storedInImpaladCatalogCache_ || RuntimeEnv.INSTANCE.isTestEnv();
  }

  public long getLastUsedTime() {
    Preconditions.checkState(lastUsedTime_ != 0 &&
        !isStoredInImpaladCatalogCache());
    return lastUsedTime_;
  }

  public void updateHMSLoadTableSchemaTime(long hmsLoadTimeNS) {
    this.metrics_.getTimer(Table.HMS_LOAD_TBL_SCHEMA).
        update(hmsLoadTimeNS, TimeUnit.NANOSECONDS);
  }

  /**
   * Populate members of 'this' from metastore info. If 'reuseMetadata' is true, reuse
   * valid existing metadata.
   */
  public abstract void load(boolean reuseMetadata, IMetaStoreClient client,
      org.apache.hadoop.hive.metastore.api.Table msTbl, String reason)
      throws TableLoadingException;

  /**
   * Sets 'tableStats_' by extracting the table statistics from the given HMS table.
   */
  public void setTableStats(org.apache.hadoop.hive.metastore.api.Table msTbl) {
    tableStats_ = new TTableStats(FeCatalogUtils.getRowCount(msTbl.getParameters()));
    tableStats_.setTotal_file_bytes(FeCatalogUtils.getTotalSize(msTbl.getParameters()));
  }

  public void addColumn(Column col) {
    colsByPos_.add(col);
    colsByName_.put(col.getName().toLowerCase(), col);
    ((StructType) type_.getItemType()).addField(
        new StructField(col.getName(), col.getType(), col.getComment()));
  }

  public void clearColumns() {
    colsByPos_.clear();
    colsByName_.clear();
    ((StructType) type_.getItemType()).clearFields();
  }

  // Returns a list of all column names for this table which we expect to have column
  // stats in the HMS. This exists because, when we request the column stats from HMS,
  // including a column name that does not have stats causes the
  // getTableColumnStatistics() to return nothing. For Hdfs tables, partition columns do
  // not have column stats in the HMS, but HBase table clustering columns do have column
  // stats. This method allows each table type to volunteer the set of columns we should
  // ask the metastore for in loadAllColumnStats().
  protected List<String> getColumnNamesWithHmsStats() {
    List<String> ret = new ArrayList<>();
    for (String name: colsByName_.keySet()) ret.add(name);
    return ret;
  }

  /**
   * Loads column statistics for all columns in this table from the Hive metastore. Any
   * errors are logged and ignored, since the absence of column stats is not critical to
   * the correctness of the system.
   */
  protected void loadAllColumnStats(IMetaStoreClient client) {
    final Timer.Context columnStatsLdContext =
        getMetrics().getTimer(LOAD_DURATION_ALL_COLUMN_STATS).time();
    try {
      if (LOG.isTraceEnabled()) LOG.trace("Loading column stats for table: " + name_);
      List<ColumnStatisticsObj> colStats;

      // We need to only query those columns which may have stats; asking HMS for other
      // columns causes loadAllColumnStats() to return nothing.
      // TODO(todd): this no longer seems to be true - asking for a non-existent column
      // is just ignored, and the columns that do exist are returned.
      List<String> colNames = getColumnNamesWithHmsStats();

      try {
        colStats = MetastoreShim.getTableColumnStatistics(client, db_.getName(), name_,
            colNames);
      } catch (Exception e) {
        LOG.warn("Could not load column statistics for: " + getFullName(), e);
        return;
      }
      FeCatalogUtils.injectColumnStats(colStats, this);
    } finally {
      columnStatsLdContext.stop();
    }
  }

  /**
   * Creates a table of the appropriate type based on the given hive.metastore.api.Table
   * object.
   */
  public static Table fromMetastoreTable(Db db,
      org.apache.hadoop.hive.metastore.api.Table msTbl) {
    CatalogInterners.internFieldsInPlace(msTbl);
    Table table = null;
    // Create a table of appropriate type
    if (MetadataOp.TABLE_TYPE_VIEW.equals(
          MetastoreShim.mapToInternalTableType(msTbl.getTableType()))) {
      table = new View(msTbl, db, msTbl.getTableName(), msTbl.getOwner());
    } else if (HBaseTable.isHBaseTable(msTbl)) {
      table = new HBaseTable(msTbl, db, msTbl.getTableName(), msTbl.getOwner());
    } else if (KuduTable.isKuduTable(msTbl)) {
      table = new KuduTable(msTbl, db, msTbl.getTableName(), msTbl.getOwner());
    } else if (DataSourceTable.isDataSourceTable(msTbl)) {
      // It's important to check if this is a DataSourceTable before HdfsTable because
      // DataSourceTables are still represented by HDFS tables in the metastore but
      // have a special table property to indicate that Impala should use an external
      // data source.
      table = new DataSourceTable(msTbl, db, msTbl.getTableName(), msTbl.getOwner());
    } else if (HdfsFileFormat.isHdfsInputFormatClass(msTbl.getSd().getInputFormat())) {
      table = new HdfsTable(msTbl, db, msTbl.getTableName(), msTbl.getOwner());
    }
    return table;
  }

  /**
   * Factory method that creates a new Table from its Thrift representation.
   * Determines the type of table to create based on the Thrift table provided.
   */
  public static Table fromThrift(Db parentDb, TTable thriftTable)
      throws TableLoadingException {
    CatalogInterners.internFieldsInPlace(thriftTable);
    Table newTable;
    if (!thriftTable.isSetLoad_status() && thriftTable.isSetMetastore_table())  {
      newTable = Table.fromMetastoreTable(parentDb, thriftTable.getMetastore_table());
    } else {
      newTable =
          IncompleteTable.createUninitializedTable(parentDb, thriftTable.getTbl_name());
    }
    newTable.loadFromThrift(thriftTable);
    newTable.validate();
    return newTable;
  }

  @Override // FeTable
  public boolean isClusteringColumn(Column c) {
    return c.getPosition() < numClusteringCols_;
  }

  protected void loadFromThrift(TTable thriftTable) throws TableLoadingException {
    List<TColumn> columns = new ArrayList<TColumn>();
    columns.addAll(thriftTable.getClustering_columns());
    columns.addAll(thriftTable.getColumns());

    colsByPos_.clear();
    colsByPos_.ensureCapacity(columns.size());
    try {
      for (int i = 0; i < columns.size(); ++i) {
        Column col = Column.fromThrift(columns.get(i));
        colsByPos_.add(col.getPosition(), col);
        colsByName_.put(col.getName().toLowerCase(), col);
        ((StructType) type_.getItemType()).addField(
            new StructField(col.getName(), col.getType(), col.getComment()));
      }
    } catch (ImpalaRuntimeException e) {
      throw new TableLoadingException(String.format("Error loading schema for " +
          "table '%s'", getName()), e);
    }

    numClusteringCols_ = thriftTable.getClustering_columns().size();
    if (thriftTable.isSetTable_stats()) tableStats_ = thriftTable.getTable_stats();

    // Default to READ_WRITE access if the field is not set.
    accessLevel_ = thriftTable.isSetAccess_level() ? thriftTable.getAccess_level() :
        TAccessLevel.READ_WRITE;

    storageMetadataLoadTime_ = thriftTable.getStorage_metadata_load_time_ns();

    storedInImpaladCatalogCache_ = true;
  }

  /**
   * Checks preconditions for this table to function as expected. Currently only checks
   * that all entries in colsByName_ use lower case keys.
   */
  public void validate() throws TableLoadingException {
    for (String colName: colsByName_.keySet()) {
      if (!colName.equals(colName.toLowerCase())) {
        throw new TableLoadingException(
            "Expected lower case column name but found: " + colName);
      }
    }
  }

  /**
   * Must be called with 'tableLock_' held to protect against concurrent modifications
   * while producing the TTable result.
   */
  public TTable toThrift() {
    // It would be simple to acquire and release the lock in this function.
    // However, in most cases toThrift() is called after modifying a table for which
    // the table lock should already be held, and we want the toThrift() to be consistent
    // with the modification. So this check helps us identify places where the lock
    // acquisition is probably missing entirely.
    if (!tableLock_.isHeldByCurrentThread()) {
      throw new IllegalStateException(
          "Table.toThrift() called without holding the table lock: " +
              getFullName() + " " + getClass().getName());
    }

    TTable table = new TTable(db_.getName(), name_);
    table.setAccess_level(accessLevel_);
    table.setStorage_metadata_load_time_ns(storageMetadataLoadTime_);

    // Populate both regular columns and clustering columns (if there are any).
    table.setColumns(new ArrayList<>());
    table.setClustering_columns(new ArrayList<>());
    for (int i = 0; i < colsByPos_.size(); ++i) {
      TColumn colDesc = colsByPos_.get(i).toThrift();
      // Clustering columns come first.
      if (i < numClusteringCols_) {
        table.addToClustering_columns(colDesc);
      } else {
        table.addToColumns(colDesc);
      }
    }

    table.setMetastore_table(getMetaStoreTable());
    table.setTable_stats(tableStats_);
    return table;
  }

  public TCatalogObject toMinimalTCatalogObject() {
    TCatalogObject catalogObject =
        new TCatalogObject(getCatalogObjectType(), getCatalogVersion());
    catalogObject.setTable(new TTable());
    catalogObject.getTable().setDb_name(getDb().getName());
    catalogObject.getTable().setTbl_name(getName());
    return catalogObject;
  }

  /**
   * Override parent implementation that will finally call toThrift() which requires
   * holding the table lock. However, it's not guaranteed that caller holds the table
   * lock (IMPALA-9136). Here we use toMinimalTCatalogObject() directly since only db
   * name and table name are needed.
   */
  @Override
  public final String getUniqueName() {
    return Catalog.toCatalogObjectKey(toMinimalTCatalogObject());
  }

  @Override
  protected void setTCatalogObject(TCatalogObject catalogObject) {
    catalogObject.setTable(toThrift());
  }

  /**
   * Return partial info about this table. This is called only on the catalogd to
   * service GetPartialCatalogObject RPCs.
   */
  public TGetPartialCatalogObjectResponse getPartialInfo(
      TGetPartialCatalogObjectRequest req) throws TableLoadingException {
    Preconditions.checkState(isLoaded(), "unloaded table: %s", getFullName());
    TTableInfoSelector selector = Preconditions.checkNotNull(req.table_info_selector,
        "no table_info_selector");

    TGetPartialCatalogObjectResponse resp = new TGetPartialCatalogObjectResponse();
    resp.setObject_version_number(getCatalogVersion());
    resp.table_info = new TPartialTableInfo();
    resp.table_info.setStorage_metadata_load_time_ns(storageMetadataLoadTime_);
    storageMetadataLoadTime_ = 0;
    if (selector.want_hms_table) {
      // TODO(todd): the deep copy could be a bit expensive. Unfortunately if we took
      // a reference to this object, and let it escape out of the lock, it would be
      // racy since the TTable is modified in place by some DDLs (eg 'dropTableStats').
      // We either need to ensure that TTable is cloned on every write, or we need to
      // ensure that serialization of the GetPartialCatalogObjectResponse object
      // is done while we continue to hold the table lock.
      resp.table_info.setHms_table(getMetaStoreTable().deepCopy());
    }
    if (selector.want_stats_for_column_names != null) {
      List<ColumnStatisticsObj> statsList = Lists.newArrayListWithCapacity(
          selector.want_stats_for_column_names.size());
      for (String colName: selector.want_stats_for_column_names) {
        Column col = getColumn(colName);
        if (col == null) continue;

        // Don't return stats for HDFS partitioning columns, since these are computed
        // by the coordinator based on the partition map itself. This makes the
        // behavior consistent with the HMS stats-fetching APIs.
        //
        // NOTE: we _do_ have to return stats for HBase clustering columns, because
        // those aren't simple value partitions.
        if (this instanceof FeFsTable && isClusteringColumn(col)) continue;

        ColumnStatisticsData tstats = col.getStats().toHmsCompatibleThrift(col.getType());
        if (tstats == null) continue;
        // TODO(todd): it seems like the column type is not used? maybe worth not
        // setting it here to save serialization.
        statsList.add(new ColumnStatisticsObj(colName, col.getType().toString(), tstats));
      }
      resp.table_info.setColumn_stats(statsList);
    }
    if (getMetaStoreTable() != null &&
        AcidUtils.isTransactionalTable(getMetaStoreTable().getParameters())) {
      Preconditions.checkState(getValidWriteIds() != null);
      resp.table_info.setValid_write_ids(
          MetastoreShim.convertToTValidWriteIdList(getValidWriteIds()));
    }
    return resp;
  }
  /**
   * @see FeCatalogUtils#parseColumnType(FieldSchema, String)
   */
  protected Type parseColumnType(FieldSchema fs) throws TableLoadingException {
    return FeCatalogUtils.parseColumnType(fs, getName());
  }

  @Override // FeTable
  public Db getDb() { return db_; }

  @Override // FeTable
  public String getName() { return name_; }

  @Override // FeTable
  public String getFullName() { return (db_ != null ? db_.getName() + "." : "") + name_; }

  @Override // FeTable
  public TableName getTableName() {
    return new TableName(db_ != null ? db_.getName() : null, name_);
  }

  @Override // FeTable
  public List<Column> getColumns() { return colsByPos_; }

  @Override // FeTable
  public SqlConstraints getSqlConstraints()  { return sqlConstraints_; }

  @Override // FeTable
  public List<String> getColumnNames() { return Column.toColumnNames(colsByPos_); }

  /**
   * Returns a list of thrift column descriptors ordered by position.
   */
  public List<TColumnDescriptor> getTColumnDescriptors() {
    return FeCatalogUtils.getTColumnDescriptors(this);
  }

  /**
   * Subclasses should override this if they provide a storage handler class. Currently
   * only HBase tables need to provide a storage handler.
   */
  @Override // FeTable
  public String getStorageHandlerClassName() { return null; }

  @Override // FeTable
  public List<Column> getColumnsInHiveOrder() {
    List<Column> columns = Lists.newArrayList(getNonClusteringColumns());
    if (getMetaStoreTable() != null &&
        AcidUtils.isFullAcidTable(getMetaStoreTable().getParameters())) {
      // Remove synthetic "row__id" column.
      Preconditions.checkState(columns.get(0).getName().equals("row__id"));
      columns.remove(0);
    }
    columns.addAll(getClusteringColumns());
    return Collections.unmodifiableList(columns);
  }

  @Override // FeTable
  public List<Column> getClusteringColumns() {
    return Collections.unmodifiableList(colsByPos_.subList(0, numClusteringCols_));
  }

  @Override // FeTable
  public List<Column> getNonClusteringColumns() {
    return Collections.unmodifiableList(colsByPos_.subList(numClusteringCols_,
        colsByPos_.size()));
  }

  @Override // FeTable
  public Column getColumn(String name) { return colsByName_.get(name.toLowerCase()); }

  @Override // FeTable
  public org.apache.hadoop.hive.metastore.api.Table getMetaStoreTable() {
    return msTable_;
  }

  @Override // FeTable
  public String getOwnerUser() {
    if (msTable_ == null) return null;
    return msTable_.getOwner();
  }

  public void setMetaStoreTable(org.apache.hadoop.hive.metastore.api.Table msTbl) {
    msTable_ = msTbl;
    CatalogInterners.internFieldsInPlace(msTable_);
  }

  @Override // FeTable
  public int getNumClusteringCols() { return numClusteringCols_; }

  /**
   * Sets the number of clustering columns. This method should only be used for tests and
   * the caller must make sure that the value matches any columns that were added to the
   * table.
   */
  public void setNumClusteringCols(int n) {
    Preconditions.checkState(RuntimeEnv.INSTANCE.isTestEnv());
    numClusteringCols_ = n;
  }

  @Override // FeTable
  public long getNumRows() { return tableStats_.num_rows; }

  @Override // FeTable
  public TTableStats getTTableStats() { return tableStats_; }

  @Override // FeTable
  public ArrayType getType() { return type_; }

  /**
   * If the table is cached, it returns a <cache pool name, replication factor> pair
   * and adds the table cached directive ID to 'cacheDirIds'. Otherwise, it
   * returns a <null, 0> pair.
   */
  public Pair<String, Short> getTableCacheInfo(List<Long> cacheDirIds) {
    String cachePoolName = null;
    Short cacheReplication = 0;
    Long cacheDirId = HdfsCachingUtil.getCacheDirectiveId(msTable_.getParameters());
    if (cacheDirId != null) {
      try {
        cachePoolName = HdfsCachingUtil.getCachePool(cacheDirId);
        cacheReplication = HdfsCachingUtil.getCacheReplication(cacheDirId);
        Preconditions.checkNotNull(cacheReplication);
        if (numClusteringCols_ == 0) cacheDirIds.add(cacheDirId);
      } catch (ImpalaRuntimeException e) {
        // Catch the error so that the actual update to the catalog can progress,
        // this resets caching for the table though
        LOG.error(
            String.format("Cache directive %d was not found, uncache the table %s " +
                "to remove this message.", cacheDirId, getFullName()));
        cacheDirId = null;
      }
    }
    return new Pair<String, Short>(cachePoolName, cacheReplication);
  }

  /**
   * The implementations of hashCode() and equals() functions are using table names as
   * unique identifiers of tables. Hence, they should be used with caution and not in
   * cases where truly unique table objects are needed.
   */
  @Override
  public int hashCode() { return getFullName().hashCode(); }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) return false;
    if (!(obj instanceof Table)) return false;
    return getFullName().equals(((Table) obj).getFullName());
  }

  /**
   *  Updates a table property with the current system time in seconds precision.
   */
  public static void updateTimestampProperty(
      org.apache.hadoop.hive.metastore.api.Table msTbl, String propertyKey) {
    msTbl.putToParameters(propertyKey, Long.toString(System.currentTimeMillis() / 1000));
  }

  public void refreshLastUsedTime() {
    lastUsedTime_ = CatalogdTableInvalidator.nanoTime();
  }

  /**
   * Removes a given version from the collection of version numbers for in-flight events
   * @param isInsertEvent If true, remove eventId from list of eventIds for in-flight
   * Insert events. If false, remove versionNumber from list of versions for in-flight DDL
   * events
   * @param versionNumber when isInsertEvent is true, it's eventId to remove
   * when isInsertEvent is false, it's version number to remove
   * @return true if version was successfully removed, false if didn't exist
   */
  public boolean removeFromVersionsForInflightEvents(
      boolean isInsertEvent, long versionNumber) {
    Preconditions.checkState(tableLock_.isHeldByCurrentThread(),
        "removeFromVersionsForInFlightEvents called without taking the table lock on "
            + getFullName());
    return inFlightEvents.remove(isInsertEvent, versionNumber);
  }

  /**
   * Adds a version number to the collection of versions for in-flight events. If the
   * collection is already at the max size defined by
   * <code>MAX_NUMBER_OF_INFLIGHT_EVENTS</code>, then it ignores the given version and
   * does not add it
   * @param isInsertEvent if true, add eventId to list of eventIds for in-flight Insert
   * events. If false, add versionNumber to list of versions for in-flight DDL events
   * @param versionNumber when isInsertEvent is true, it's eventId to add
   * when isInsertEvent is false, it's version number to add
   * @return True if version number was added, false if the collection is at its max
   * capacity
   */
  public void addToVersionsForInflightEvents(boolean isInsertEvent, long versionNumber) {
    // we generally don't take locks on Incomplete tables since they are atomically
    // replaced during load
    Preconditions.checkState(
        this instanceof IncompleteTable || tableLock_.isHeldByCurrentThread());
    if (!inFlightEvents.add(isInsertEvent, versionNumber)) {
      LOG.warn(String.format("Could not add %s version to the table %s. This could "
          + "cause unnecessary refresh of the table when the event is received by the "
              + "Events processor.", versionNumber, getFullName()));
    }
  }

  @Override
  public long getWriteId() {
    return MetastoreShim.getWriteIdFromMSTable(msTable_);
  }

  @Override
  public ValidWriteIdList getValidWriteIds() {
    return null;
  }
}
