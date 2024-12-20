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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.types.Types;
import org.apache.impala.authorization.AuthorizationChecker;
import org.apache.impala.authorization.AuthorizationPolicy;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.DataSource;
import org.apache.impala.catalog.FileDescriptor;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.HdfsCachePool;
import org.apache.impala.catalog.HdfsFileFormat;
import org.apache.impala.catalog.HdfsPartitionLocationCompressor;
import org.apache.impala.catalog.HdfsStorageDescriptor;
import org.apache.impala.catalog.IcebergContentFileStore;
import org.apache.impala.catalog.IcebergFileMetadataLoader;
import org.apache.impala.catalog.IcebergTableLoadingException;
import org.apache.impala.catalog.PuffinStatsLoader;
import org.apache.impala.catalog.SqlConstraints;
import org.apache.impala.catalog.VirtualColumn;
import org.apache.impala.catalog.iceberg.GroupedContentFiles;
import org.apache.impala.catalog.iceberg.IcebergRESTCatalog;
import org.apache.impala.catalog.local.LocalIcebergTable.TableParams;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.common.Pair;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.thrift.TBriefTableMeta;
import org.apache.impala.thrift.TIcebergContentFileStore;
import org.apache.impala.thrift.TIcebergTable;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TPartialTableInfo;
import org.apache.impala.thrift.TValidWriteIdList;
import org.apache.impala.util.AcidUtils;
import org.apache.impala.util.IcebergSchemaConverter;
import org.apache.impala.util.ListMap;
import org.apache.thrift.TException;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.errorprone.annotations.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.SnapshotSummary.TOTAL_FILE_SIZE_PROP;
import static org.apache.iceberg.SnapshotSummary.TOTAL_RECORDS_PROP;
import static org.apache.impala.analysis.Analyzer.ACCESSTYPE_READ;

public class IcebergMetaProvider implements MetaProvider {
  private final static Logger LOG = LoggerFactory.getLogger(IcebergMetaProvider.class);

  private AtomicReference<? extends AuthorizationChecker> authzChecker_;
  private final AuthorizationPolicy authPolicy_ = new AuthorizationPolicy();

  private IcebergRESTCatalog iceCatalog_;

  Properties properties_;

  public IcebergMetaProvider(Properties properties) {
    properties_ = properties;
    iceCatalog_ = initCatalog();
  }

  public String getURI() {
    return "Iceberg REST (" + iceCatalog_.getUri() + ")";
  }

  private IcebergRESTCatalog initCatalog() {
    return IcebergRESTCatalog.getInstance(properties_);
  }

  public void setAuthzChecker(
      AtomicReference<? extends AuthorizationChecker> authzChecker) {
    authzChecker_ = authzChecker;
  }

  @Override
  public Iterable<HdfsCachePool> getHdfsCachePools() {
    throw new UnsupportedOperationException(
        "HDFSCachePools are not supported in IcebergMetaProvider");
  }

  @Override
  public AuthorizationPolicy getAuthPolicy() {
    return authPolicy_;
  }

  @Override
  public boolean isReady() {
    // Direct provider is always ready since we don't need to wait for
    // an update from any external process.
    return true;
  }

  @Override
  public void waitForIsReady(long timeoutMs) {
    // NOOP
  }

  @Override
  public ImmutableList<String> loadDbList() throws TException {
    return iceCatalog_.listNamespaces();
  }

  @Override
  public Database loadDb(String dbName) throws TException {
    Database db = new Database();
    db.setName(dbName);
    return db;
  }

  @Override
  public ImmutableCollection<TBriefTableMeta> loadTableList(String dbName)
      throws TException {
    ImmutableList.Builder<TBriefTableMeta> ret = ImmutableList.builder();
    Namespace ns = Namespace.of(dbName);
    for (TableIdentifier tid : iceCatalog_.listTables(ns.toString())) {
      try {
        org.apache.iceberg.Table tbl = iceCatalog_.loadTable(tid, null, null);
        TBriefTableMeta briefMeta = new TBriefTableMeta(getIcebergTableName(tbl));
        briefMeta.setMsType("TABLE");
        ret.add(briefMeta);
      } catch (NoSuchTableException | IcebergTableLoadingException e) {
        // Ignore tables that cannot be loaded.
        LOG.error(e.toString());
      }
    }
    return ret.build();
  }

  String getIcebergTableName(org.apache.iceberg.Table tbl) {
    return tbl.name().substring(tbl.name().lastIndexOf('.') + 1);
  }

  @Override
  public Pair<Table, TableMetaRef> getTableIfPresent(String dbName, String tblName) {
    try {
      return loadTable(dbName, tblName);
    } catch (TException e) {
      LOG.error("Failed to load table", e);
      return null;
    }
  }

  @Override
  public Pair<Table, TableMetaRef> loadTable(String dbName, String tableName)
      throws TException {
    try {
      Table msTable = new Table();
      msTable.setDbName(dbName);
      Namespace ns = Namespace.of(dbName);
      org.apache.iceberg.Table tbl = iceCatalog_.loadTable(
          TableIdentifier.of(ns, tableName), null, null);
      msTable.setTableName(getIcebergTableName(tbl));
      msTable.setSd(createStorageDescriptor(tbl));
      // Iceberg partitioning is not stored in HMS.
      msTable.setPartitionKeys(Collections.emptyList());
      msTable.setParameters(createTableProps(tbl));
      msTable.setTableType(TableType.EXTERNAL_TABLE.toString());
      // Only allow READONLY operations.
      MetastoreShim.setTableAccessType(msTable, ACCESSTYPE_READ);
      long loadingTime = System.currentTimeMillis();
      TableMetaRef ref = new TableMetaRefImpl(dbName, tableName, msTable, tbl,
          loadingTime);
      return Pair.create(msTable, ref);
    } catch (ImpalaRuntimeException|IcebergTableLoadingException e) {
      throw new IllegalStateException(
          String.format("Error loading Iceberg table %s.%s", dbName, tableName), e);
    }
  }

  private StorageDescriptor createStorageDescriptor(org.apache.iceberg.Table tbl)
      throws ImpalaRuntimeException {
    StorageDescriptor sd = new StorageDescriptor();
    sd.setInputFormat(HdfsFileFormat.ICEBERG.inputFormat());
    sd.setOutputFormat(HdfsFileFormat.ICEBERG.outputFormat());
    sd.setSortCols(Collections.emptyList());
    SerDeInfo serde = new SerDeInfo();
    serde.setSerializationLib(HdfsFileFormat.ICEBERG.serializationLib());
    serde.setParameters(Collections.emptyMap());
    sd.setSerdeInfo(serde);
    sd.setCols(IcebergSchemaConverter.convertToHiveSchema(tbl.schema()));
    Path p = new Path(tbl.location());
    sd.setLocation(FileSystemUtil.createFullyQualifiedPath(p).toString());
    return sd;
  }

  private Map<String, String> createTableProps(org.apache.iceberg.Table tbl) {
    Map<String, String> props = new HashMap<>(tbl.properties());
    Snapshot currentSnapshot = tbl.currentSnapshot();
    if (currentSnapshot != null) {
      if (props.get(StatsSetupConst.ROW_COUNT) == null) {
        props.put(StatsSetupConst.ROW_COUNT,
            String.valueOf(currentSnapshot.summary().get(TOTAL_RECORDS_PROP)));
      }
      if (props.get(StatsSetupConst.TOTAL_SIZE) == null) {
        props.put(StatsSetupConst.TOTAL_SIZE,
            String.valueOf(currentSnapshot.summary().get(TOTAL_FILE_SIZE_PROP)));
      }
    }
    return props;
  }

  @Override
  public String loadNullPartitionKeyValue() throws MetaException, TException {
    return "__HIVE_DEFAULT_PARTITION__";
  }

  @Override
  public List<PartitionRef> loadPartitionList(TableMetaRef table)
      throws MetaException, TException {
    TableMetaRefImpl ref = (TableMetaRefImpl)table;
    Preconditions.checkState(!ref.isPartitioned());
    return ImmutableList.of(new PartitionRefImpl(PartitionRefImpl.UNPARTITIONED_NAME));
  }

  @Override
  public SqlConstraints loadConstraints(
      TableMetaRef table, Table msTbl) throws TException {
    return null;
  }

  @Override
  public Map<String, PartitionMetadata> loadPartitionsByRefs(
      TableMetaRef table, List<String> partitionColumnNames,
      ListMap<TNetworkAddress> hostIndex,
      List<PartitionRef> partitionRefs) throws CatalogException, TException {
    Map<String, PartitionMetadata> ret = new HashMap<>();
    ret.put("", new PartitionMetadataImpl(((TableMetaRefImpl)table).msTable_));
    return ret;
  }

  /**
   * We model partitions slightly differently to Hive. So, in the case of an
   * unpartitioned table, we have to create a fake Partition object which has the
   * metadata of the table.
   */
  private Map<String, PartitionMetadata> loadUnpartitionedPartition(
      TableMetaRefImpl table, List<PartitionRef> partitionRefs,
      ListMap<TNetworkAddress> hostIndex) throws CatalogException {
    Map<String, PartitionMetadata> ret = new HashMap<>();
    ret.put("", new PartitionMetadataImpl(((TableMetaRefImpl)table).msTable_));
    return ret;
  }

  @Override
  public List<String> loadFunctionNames(String dbName) throws TException {
    throw new UnsupportedOperationException(
        "Functions not supported by IcebergMetaProvider");
  }


  @Override
  public ImmutableList<Function> loadFunction(String dbName, String functionName)
      throws TException {
    throw new UnsupportedOperationException(
        "Functions not supported by IcebergMetaProvider");
  }

  @Override
  public ImmutableList<DataSource> loadDataSources() throws TException {
    throw new UnsupportedOperationException(
        "DataSource not supported by IcebergMetaProvider");
  }

  @Override
  public DataSource loadDataSource(String dsName) throws TException {
    throw new UnsupportedOperationException(
        "DataSource not supported by IcebergMetaProvider");
  }

  @Override
  public List<ColumnStatisticsObj> loadTableColumnStatistics(TableMetaRef table,
      List<String> colNames) throws TException {
    Preconditions.checkArgument(table instanceof TableMetaRefImpl);
    TableMetaRefImpl tblImpl = (TableMetaRefImpl) table;
    org.apache.iceberg.Table iceTbl = tblImpl.iceApiTbl_;
    Map<Integer, PuffinStatsLoader.PuffinStatsRecord> puffinStats =
        PuffinStatsLoader.loadPuffinStats(iceTbl, tblImpl.fullName(),
            -1, Collections.emptySet());

    List<ColumnStatisticsObj> res = new ArrayList<>();
    for (String colName : colNames) {
      org.apache.iceberg.types.Types.NestedField field =
          iceTbl.schema().findField(colName);
      int fieldId = field.fieldId();
      PuffinStatsLoader.PuffinStatsRecord stats = puffinStats.get(fieldId);
      if (stats != null) {
        long ndv = stats.ndv;

        LongColumnStatsData ndvData = new LongColumnStatsData();
        ndvData.setNumDVs(ndv);

        ColumnStatisticsData ndvColStatsData = createNdvColStatsData(field.type(), ndv);
        ColumnStatisticsObj statsObj = new ColumnStatisticsObj(colName,
            "" /* should be the type */, ndvColStatsData);
        res.add(statsObj);
      }
    }
    return res;
  }

  private ColumnStatisticsData createNdvColStatsData(
      org.apache.iceberg.types.Type type, long ndv) {
    if (type instanceof Types.BooleanType) {
      // No NDV can be set for BooleanColumnStatsData.
      BooleanColumnStatsData ndvData = new BooleanColumnStatsData();
      return ColumnStatisticsData.booleanStats(ndvData);
    } else if (type instanceof Types.IntegerType
        || type instanceof Types.LongType
        || type instanceof Types.TimestampType) {
      LongColumnStatsData ndvData = new LongColumnStatsData();
      ndvData.setNumDVs(ndv);
      return ColumnStatisticsData.longStats(ndvData);
    } else if (type instanceof Types.DateType) {
      DateColumnStatsData ndvData = new DateColumnStatsData();
      ndvData.setNumDVs(ndv);
      return ColumnStatisticsData.dateStats(ndvData);
    } else if (type instanceof Types.FloatType || type instanceof Types.DoubleType) {
      DoubleColumnStatsData ndvData = new DoubleColumnStatsData();
      ndvData.setNumDVs(ndv);
      return ColumnStatisticsData.doubleStats(ndvData);
    } else if (type instanceof Types.StringType) {
      StringColumnStatsData ndvData = new StringColumnStatsData();
      ndvData.setNumDVs(ndv);
      return ColumnStatisticsData.stringStats(ndvData);
    } else if (type instanceof Types.BinaryType) {
      // No NDV can be set for BinaryColumnStatsData.
      BinaryColumnStatsData ndvData = new BinaryColumnStatsData();
      return ColumnStatisticsData.binaryStats(ndvData);
    } else if (type instanceof Types.DecimalType) {
      // No NDV can be set for DecimalColumnStatsData.
      DecimalColumnStatsData ndvData = new DecimalColumnStatsData();
      ndvData.setNumDVs(ndv);
      return ColumnStatisticsData.decimalStats(ndvData);
    } else {
      return new ColumnStatisticsData();
    }
  }

  @Immutable
  private static class PartitionRefImpl implements PartitionRef {
    private static final String UNPARTITIONED_NAME = "";
    private final String name_;

    public PartitionRefImpl(String name) {
      this.name_ = name;
    }

    @Override
    public String getName() {
      return name_;
    }
  }

  private static class PartitionMetadataImpl implements PartitionMetadata {
    private final Table msTable_;

    public PartitionMetadataImpl(Table msTable) {
      this.msTable_ = msTable;
    }

    @Override
    public Map<String, String> getHmsParameters() { return Collections.emptyMap(); }

    @Override
    public long getWriteId() {
      return -1;
    }

    @Override
    public HdfsStorageDescriptor getInputFormatDescriptor() {
      String tblName = msTable_.getDbName() + "." + msTable_.getTableName();
      try {
        return HdfsStorageDescriptor.fromStorageDescriptor(tblName, msTable_.getSd());
      } catch (HdfsStorageDescriptor.InvalidStorageDescriptorException e) {
        throw new LocalCatalogException(String.format(
            "Invalid input format descriptor for table %s", tblName), e);
      }
    }

    @Override
    public HdfsPartitionLocationCompressor.Location getLocation() {
      return new HdfsPartitionLocationCompressor(0).new Location(
          msTable_.getSd().getLocation());
    }

    @Override
    public ImmutableList<FileDescriptor> getFileDescriptors() {
      return ImmutableList.of();
    }

    @Override
    public ImmutableList<FileDescriptor> getInsertFileDescriptors() {
      return ImmutableList.of();
    }

    @Override
    public ImmutableList<FileDescriptor> getDeleteFileDescriptors() {
      return ImmutableList.of();
    }

    @Override
    public boolean hasIncrementalStats() {
      return false; // Incremental stats not supported for Iceberg tables.
    }

    @Override
    public byte[] getPartitionStats() {
      return null;
    }

    @Override
    public boolean isMarkedCached() {
      return false;
    }

    @Override
    public long getLastCompactionId() {
      throw new UnsupportedOperationException("Compaction id is not provided with " +
          "IcebergMetaProvider implementation");
    }
  }

  private class TableMetaRefImpl implements TableMetaRef {

    private final String dbName_;
    private final String tableName_;
    private final Table msTable_;
    private final long loadingTimeMs_;
    private final HdfsPartitionLocationCompressor partitionLocationCompressor_;
    private final org.apache.iceberg.Table iceApiTbl_;

    public TableMetaRefImpl(String dbName, String tableName, Table msTable,
                            org.apache.iceberg.Table iceApiTbl, long loadingTimeMs) {
      this.dbName_ = dbName;
      this.tableName_ = tableName;
      this.msTable_ = msTable;
      this.iceApiTbl_ = iceApiTbl;
      this.loadingTimeMs_ = loadingTimeMs;
      this.partitionLocationCompressor_ = new HdfsPartitionLocationCompressor(
          msTable.getPartitionKeysSize(),
          Lists.newArrayList(msTable.getSd().getLocation()));
    }

    @Override
    public boolean isPartitioned() {
      return msTable_.getPartitionKeysSize() != 0;
    }

    @Override
    public boolean isMarkedCached() {
      return false;
    }

    @Override
    public List<String> getPartitionPrefixes() {
      return partitionLocationCompressor_.getPrefixes();
    }

    @Override
    public boolean isTransactional() {
      return AcidUtils.isTransactionalTable(msTable_.getParameters());
    }

    public String fullName() { return dbName_ + "." + tableName_; }

    @Override
    public List<VirtualColumn> getVirtualColumns() {
      List<VirtualColumn> ret = new ArrayList<>();
      ret.add(VirtualColumn.INPUT_FILE_NAME);
      ret.add(VirtualColumn.FILE_POSITION);
      ret.add(VirtualColumn.PARTITION_SPEC_ID);
      ret.add(VirtualColumn.ICEBERG_PARTITION_SERIALIZED);
      ret.add(VirtualColumn.ICEBERG_DATA_SEQUENCE_NUMBER);
      return ret;
    }

    @Override
    public long getCatalogVersion() {
      return 0;
    }

    @Override
    public long getLoadedTimeMs() {
      return loadingTimeMs_;
    }
  }

  @Override
  public TValidWriteIdList getValidWriteIdList(TableMetaRef ref) {
    return null;
  }

  /**
   * Fetches the latest compaction id from HMS and compares with partition metadata in
   * cache. If a partition is stale due to compaction, removes it from metas.
   */
  public List<PartitionRef> checkLatestCompaction(String dbName, String tableName,
      TableMetaRef table, Map<PartitionRef, PartitionMetadata> metas) throws TException {
    return Collections.emptyList();
  }

  @Override
  public TPartialTableInfo loadIcebergTable(final TableMetaRef table) throws TException {
    TableMetaRefImpl tableRefImpl = (TableMetaRefImpl)table;
    TableParams tableParams = new TableParams(tableRefImpl.msTable_);
    org.apache.iceberg.Table apiTable = loadIcebergApiTable(table, tableParams,
        tableRefImpl.msTable_);

    TPartialTableInfo ret = new TPartialTableInfo();
    TIcebergTable iceTable = new TIcebergTable();
    if (apiTable.currentSnapshot() != null) {
      iceTable.setCatalog_snapshot_id(apiTable.currentSnapshot().snapshotId());
    }
    iceTable.setDefault_partition_spec_id(apiTable.spec().specId());
    ListMap<TNetworkAddress> hostIndex = new ListMap<>();
    iceTable.setContent_files(getTContentFileStore(table, apiTable, hostIndex));
    iceTable.setPartition_stats(Collections.emptyMap());
    ret.setIceberg_table(iceTable);
    ret.setNetwork_addresses(hostIndex.getList());
    return ret;
  }

  @Override
  public org.apache.iceberg.Table loadIcebergApiTable(final TableMetaRef table,
      TableParams params, Table msTable) throws TException {
    return ((TableMetaRefImpl)table).iceApiTbl_;
  }

  public String getLocation(final TableMetaRef table) {
    return ((TableMetaRefImpl)table).msTable_.getSd().getLocation();
  }

  private TIcebergContentFileStore getTContentFileStore(final TableMetaRef table,
      org.apache.iceberg.Table apiTable, ListMap<TNetworkAddress> hostIndex) {
    try {
      TableScan scan = apiTable.newScan();
      GroupedContentFiles groupedFiles = new GroupedContentFiles(scan.planFiles());
      IcebergFileMetadataLoader iceFml = new IcebergFileMetadataLoader(
          apiTable, Collections.emptyList(), hostIndex, groupedFiles,
          false);
      iceFml.load();
      IcebergContentFileStore contentFileStore = new IcebergContentFileStore(
          apiTable, iceFml.getLoadedIcebergFds(), groupedFiles);
      return contentFileStore.toThrift();
    } catch (Exception e) {
      throw new IllegalStateException(
          "Exception occurred during loading Iceberg file metadata", e);
    }
  }
}
