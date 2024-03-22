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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.impala.analysis.IcebergPartitionField;
import org.apache.impala.analysis.IcebergPartitionSpec;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.analysis.TimeTravelSpec;
import org.apache.impala.analysis.TimeTravelSpec.Kind;
import org.apache.impala.catalog.CatalogObject.ThriftObjectType;
import org.apache.impala.catalog.HdfsPartition.FileBlock;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.catalog.iceberg.GroupedContentFiles;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.common.Pair;
import org.apache.impala.common.PrintUtils;
import org.apache.impala.common.Reference;
import org.apache.impala.fb.FbFileBlock;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TCompressionCodec;
import org.apache.impala.thrift.THdfsCompression;
import org.apache.impala.thrift.THdfsFileDesc;
import org.apache.impala.thrift.THdfsPartition;
import org.apache.impala.thrift.THdfsTable;
import org.apache.impala.thrift.TIcebergCatalog;
import org.apache.impala.thrift.TIcebergFileFormat;
import org.apache.impala.thrift.TIcebergPartitionStats;
import org.apache.impala.thrift.TIcebergPartitionTransformType;
import org.apache.impala.thrift.TIcebergTable;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.thrift.TResultSetMetadata;
import org.apache.impala.util.HdfsCachingUtil;
import org.apache.impala.util.IcebergSchemaConverter;
import org.apache.impala.util.IcebergUtil;
import org.apache.impala.util.ListMap;
import org.apache.impala.util.TResultRowBuilder;
import org.apache.thrift.TException;

import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Frontend interface for interacting with an Iceberg-backed table.
 */
public interface FeIcebergTable extends FeFsTable {
  final static Logger LOG = LoggerFactory.getLogger(FeIcebergTable.class);
  /**
   * Return content file store.
   */
  IcebergContentFileStore getContentFileStore();

  /**
   * Return the partition stats from iceberg table
   */
  Map<String, TIcebergPartitionStats> getIcebergPartitionStats();

  /**
   * Return the hdfs table transformed from iceberg table
   */
  FeFsTable getFeFsTable();

  /**
   * Return iceberg catalog type from table properties
   */
  TIcebergCatalog getIcebergCatalog();

  /**
   * Returns the cached Iceberg Table object that stores the metadata loaded by Iceberg.
   */
  Table getIcebergApiTable();

  /**
   * Return Iceberg catalog location, we use this location to load metadata from Iceberg
   * When using 'hadoop.tables', this value equals to table location
   * When using 'hadoop.catalog', this value equals to 'iceberg.catalog_location'
   */
  String getIcebergCatalogLocation();

  /**
   * Return iceberg file format from table properties
   */
  TIcebergFileFormat getIcebergFileFormat();

  /**
   * Return iceberg parquet compression codec from table properties
   */
  TCompressionCodec getIcebergParquetCompressionCodec();

  /**
   * Return iceberg parquet row group size in bytes from table properties
   */
  long getIcebergParquetRowGroupSize();

  /**
   * Return iceberg parquet plain page size in bytes from table properties
   */
  long getIcebergParquetPlainPageSize();

  /**
   * Return iceberg parquet dictionary page size in bytes from table properties
   */
  long getIcebergParquetDictPageSize();

  /**
   * Return the table location of Iceberg table
   * When using 'hadoop.tables', this value is a normal table location
   * When using 'hadoop.catalog', this value is 'iceberg.catalog_location' + identifier
   */
  String getIcebergTableLocation();

  /**
   * Return the Iceberg partition spec info
   */
  List<IcebergPartitionSpec> getPartitionSpecs();

  /**
   *  Return the latest partition spec.
   */
  IcebergPartitionSpec getDefaultPartitionSpec();

  /**
   *  Return the ID used for getting the default partititon spec.
   */
  int getDefaultPartitionSpecId();

  default IcebergPartitionSpec getPartitionSpec(int specId) {
    for (IcebergPartitionSpec spec : getPartitionSpecs()) {
      if (spec.getSpecId() == specId) return spec;
    }
    return null;
  }

  default int getFormatVersion() {
    return ((BaseTable)getIcebergApiTable()).operations().current().formatVersion();
  }

  /**
   * @return the Iceberg schema.
   */
  default Schema getIcebergSchema() {
    return getIcebergApiTable().schema();
  }

  @Override
  default List<String> getPrimaryKeyColumnNames() throws TException {
    return Lists.newArrayList(getIcebergSchema().identifierFieldNames());
  }

  @Override
  default boolean isCacheable() {
    return getFeFsTable().isCacheable();
  }

  @Override
  default boolean isLocationCacheable() {
    return getFeFsTable().isLocationCacheable();
  }

  @Override
  default boolean isMarkedCached() {
    return getFeFsTable().isMarkedCached();
  }

  @Override
  default String getLocation() {
    return getFeFsTable().getLocation();
  }

  @Override
  default String getNullPartitionKeyValue() {
    return getFeFsTable().getNullPartitionKeyValue();
  }

  @Override
  default String getHdfsBaseDir() {
    return getFeFsTable().getHdfsBaseDir();
  }

  @Override
  default FileSystemUtil.FsType getFsType() {
    return getFeFsTable().getFsType();
  }

  @Override
  default long getTotalHdfsBytes() {
    return getFeFsTable().getTotalHdfsBytes();
  }

  @Override
  default boolean usesAvroSchemaOverride() {
    return getFeFsTable().usesAvroSchemaOverride();
  }

  @Override
  default Set<HdfsFileFormat> getFileFormats() {
    return getFeFsTable().getFileFormats();
  }

  @Override
  default boolean hasWriteAccessToBaseDir() {
    return getFeFsTable().hasWriteAccessToBaseDir();
  }

  @Override
  default String getFirstLocationWithoutWriteAccess() {
    return getFeFsTable().getFirstLocationWithoutWriteAccess();
  }

  @Override
  default TResultSet getTableStats() {
    return getFeFsTable().getTableStats();
  }

  @Override
  default Collection<? extends PrunablePartition> getPartitions() {
    return getFeFsTable().getPartitions();
  }

  @Override
  default Set<Long> getPartitionIds() {
    return getFeFsTable().getPartitionIds();
  }

  @Override
  default Map<Long, ? extends PrunablePartition> getPartitionMap() {
    return getFeFsTable().getPartitionMap();
  }

  @Override
  default TreeMap<LiteralExpr, Set<Long>> getPartitionValueMap(int col) {
    return getFeFsTable().getPartitionValueMap(col);
  }

  @Override
  default Set<Long> getNullPartitionIds(int colIdx) {
    return getFeFsTable().getNullPartitionIds(colIdx);
  }

  @Override
  default List<? extends FeFsPartition> loadPartitions(Collection<Long> ids) {
    return getFeFsTable().loadPartitions(ids);
  }

  @Override
  default SqlConstraints getSqlConstraints() {
    return getFeFsTable().getSqlConstraints();
  }

  @Override
  default ListMap<TNetworkAddress> getHostIndex() {
    return getFeFsTable().getHostIndex();
  }

  /**
   * @return true if there's at least one partition spec that has at least one non-VOID
   * partition field.
   */
  default boolean isPartitioned() {
    for (IcebergPartitionSpec spec : getPartitionSpecs()) {
      if (spec.getIcebergPartitionFieldsSize() == 0) continue;
      for (IcebergPartitionField partField : spec.getIcebergPartitionFields()) {
        if (partField.getTransformType() != TIcebergPartitionTransformType.VOID) {
          return true;
        }
      }
    }
    return false;
  }

  @Override /* FeTable */
  default boolean isComputedPartitionColumn(Column col) {
    Preconditions.checkState(col instanceof IcebergColumn);
    IcebergColumn iceCol = (IcebergColumn)col;
    IcebergPartitionSpec spec = getDefaultPartitionSpec();
    if (spec == null || !spec.hasPartitionFields()) return false;

    for (IcebergPartitionField partField : spec.getIcebergPartitionFields()) {
      if (iceCol.getFieldId() == partField.getSourceId()) return true;
    }
    return false;
  }

  THdfsTable transformToTHdfsTable(boolean updatePartitionFlag, ThriftObjectType type);

  /**
   * Returns the current snapshot id of the Iceberg API table if it exists, otherwise
   * returns -1.
   */
  default long snapshotId() {
    if (getIcebergApiTable() != null && getIcebergApiTable().currentSnapshot() != null) {
      return getIcebergApiTable().currentSnapshot().snapshotId();
    }
    return -1;
  }

  default TIcebergFileFormat getWriteFileFormat() {
    return IcebergUtil.getIcebergFileFormat(
        getIcebergApiTable().properties().getOrDefault(
            TableProperties.DEFAULT_FILE_FORMAT,
            TableProperties.DEFAULT_FILE_FORMAT_DEFAULT));
  }

  default TIcebergFileFormat getDeleteFileFormat() {
    String deleteFormat =
        getIcebergApiTable().properties().get(TableProperties.DELETE_DEFAULT_FILE_FORMAT);
    if (deleteFormat != null) {
      return IcebergUtil.getIcebergFileFormat(deleteFormat);
    }
    // If delete file format is not specified, use  write format.
    return getWriteFileFormat();
  }

  /**
   * Sets 'tableStats_' for the Iceberg table by it's partition stats.
   * TODO: Now the calculation of V2 Iceberg table is not accurate. After
   * IMPALA-11516(Return better partition stats for V2 tables) is ready, this method can
   * be considered to replace
   * {@link Table#setTableStats(org.apache.hadoop.hive.metastore.api.Table)}.
   */
  default void setIcebergTableStats() {
    Preconditions.checkState(getTTableStats() != null);
    Preconditions.checkState(getIcebergPartitionStats() != null);
    if (getTTableStats().getNum_rows() < 0) {
      getTTableStats().setNum_rows(Utils.calculateNumRows(this));
    }
    getTTableStats().setTotal_file_bytes(Utils.calculateFileSizeInBytes(this));
  }

  static void setIcebergStorageDescriptor(
      org.apache.hadoop.hive.metastore.api.Table hmsTable) {
    hmsTable.getSd().setInputFormat(HdfsFileFormat.ICEBERG.inputFormat());
    hmsTable.getSd().setOutputFormat(HdfsFileFormat.ICEBERG.outputFormat());
    hmsTable.getSd().getSerdeInfo().setSerializationLib(
        HdfsFileFormat.ICEBERG.serializationLib());
  }

  static void resetIcebergStorageDescriptor(
      org.apache.hadoop.hive.metastore.api.Table modifiedTable,
      org.apache.hadoop.hive.metastore.api.Table originalTable) {
    modifiedTable.getSd().setInputFormat(originalTable.getSd().getInputFormat());
    modifiedTable.getSd().setOutputFormat(originalTable.getSd().getOutputFormat());
    modifiedTable.getSd().getSerdeInfo().setSerializationLib(
        originalTable.getSd().getSerdeInfo().getSerializationLib());
  }

  /**
   * Utility functions
   */
  public static abstract class Utils {

    // We need 'FileStatus#modification_time' to know whether the file is changed,
    // meanwhile this property cannot be obtained from the Iceberg metadata. But Iceberg
    // files are immutable, so we can use a special default value.
    private static final int DEFAULT_MODIFICATION_TIME = 1;

    /**
     * Returns true if FeIcebergTable file format is columnar: parquet or orc
     */
    public static boolean isColumnar(FeIcebergTable table) {
      HdfsFileFormat format = IcebergUtil.toHdfsFileFormat(table.getIcebergFileFormat());
      return format == HdfsFileFormat.PARQUET || format == HdfsFileFormat.ORC;
    }

    /**
     * Get file info for the given fe iceberg table.
     */
    public static TResultSet getIcebergTableFiles(FeIcebergTable table,
        TResultSet result) {
      List<FileDescriptor> orderedFds = Lists.newArrayList(
          table.getContentFileStore().getAllFiles());
      Collections.sort(orderedFds);
      for (FileDescriptor fd : orderedFds) {
        TResultRowBuilder rowBuilder = new TResultRowBuilder();
        String absPath = fd.getAbsolutePath(table.getLocation());
        rowBuilder.add(absPath);
        rowBuilder.add(PrintUtils.printBytes(fd.getFileLength()));
        rowBuilder.add("");
        rowBuilder.add(FileSystemUtil.getErasureCodingPolicy(new Path(absPath)));
        result.addToRows(rowBuilder.get());
      }
      return result;
    }

    /**
     * Get partition stats for the given fe iceberg table.
     */
    public static TResultSet getPartitionStats(FeIcebergTable table) {
      TResultSet result = new TResultSet();
      TResultSetMetadata resultSchema = new TResultSetMetadata();
      result.setSchema(resultSchema);
      result.setRows(new ArrayList<>());

      resultSchema.addToColumns(new TColumn("Partition", Type.STRING.toThrift()));
      resultSchema.addToColumns(new TColumn("Number Of Rows", Type.BIGINT.toThrift()));
      resultSchema.addToColumns(new TColumn("Number Of Files", Type.BIGINT.toThrift()));

      Map<String, TIcebergPartitionStats> nameToStats = getOrderedPartitionStats(table);
      for (Map.Entry<String, TIcebergPartitionStats> partitionInfo : nameToStats
          .entrySet()) {
        TResultRowBuilder builder = new TResultRowBuilder();
        builder.add(partitionInfo.getKey());
        builder.add(partitionInfo.getValue().getNum_rows());
        builder.add(partitionInfo.getValue().getNum_files());
        result.addToRows(builder.get());
      }
      return result;
    }

    /**
     * Get partition stats for the given fe iceberg table ordered by partition name.
     */
    private static Map<String, TIcebergPartitionStats> getOrderedPartitionStats(
        FeIcebergTable table) {
      return table.getIcebergPartitionStats()
          .entrySet()
          .stream()
          .sorted(Map.Entry.comparingByKey())
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
              (oldValue, newValue) -> oldValue, LinkedHashMap::new));
    }

    /**
     * Get table stats for the given fe iceberg table.
     */
    public static TResultSet getTableStats(FeIcebergTable table) {
      TResultSet result = new TResultSet();

      TResultSetMetadata resultSchema = new TResultSetMetadata();
      resultSchema.addToColumns(new TColumn("#Rows", Type.BIGINT.toThrift()));
      resultSchema.addToColumns(new TColumn("#Files", Type.BIGINT.toThrift()));
      resultSchema.addToColumns(new TColumn("Size", Type.STRING.toThrift()));
      resultSchema.addToColumns(new TColumn("Bytes Cached", Type.STRING.toThrift()));
      resultSchema.addToColumns(new TColumn("Cache Replication", Type.STRING.toThrift()));
      resultSchema.addToColumns(new TColumn("Format", Type.STRING.toThrift()));
      resultSchema.addToColumns(new TColumn("Incremental stats", Type.STRING.toThrift()));
      resultSchema.addToColumns(new TColumn("Location", Type.STRING.toThrift()));
      resultSchema.addToColumns(new TColumn("EC Policy", Type.STRING.toThrift()));
      result.setSchema(resultSchema);

      TResultRowBuilder rowBuilder = new TResultRowBuilder();
      rowBuilder.add(table.getNumRows());
      rowBuilder.add(table.getContentFileStore().getNumFiles());
      rowBuilder.addBytes(table.getTTableStats().getTotal_file_bytes());
      if (!table.isMarkedCached()) {
        rowBuilder.add("NOT CACHED");
        rowBuilder.add("NOT CACHED");
      } else {
        long cachedBytes = 0L;
        for (FileDescriptor fd: table.getContentFileStore().getAllFiles()) {
          int numBlocks = fd.getNumFileBlocks();
          for (int i = 0; i < numBlocks; ++i) {
            FbFileBlock block = fd.getFbFileBlock(i);
            if (FileBlock.hasCachedReplica(block)) {
              cachedBytes += FileBlock.getLength(block);
            }
          }
        }
        rowBuilder.addBytes(cachedBytes);

        Short rep = HdfsCachingUtil
            .getCachedCacheReplication(table.getMetaStoreTable().getParameters());
        rowBuilder.add(rep.toString());
      }
      rowBuilder.add(table.getIcebergFileFormat().toString());
      rowBuilder.add(Boolean.FALSE.toString());
      rowBuilder.add(table.getLocation());
      rowBuilder.add(
          FileSystemUtil.getErasureCodingPolicy(new Path(table.getLocation())));
      result.addToRows(rowBuilder.get());

      return result;
    }

    /**
     * Calculate num rows for the given iceberg table by it's partition stats.
     * The result is computed by all DataFiles without any DeleteFile.
     */
    public static long calculateNumRows(FeIcebergTable table) {
      return table.getIcebergPartitionStats().values().stream()
          .mapToLong(TIcebergPartitionStats::getNum_rows).sum();
    }

    /**
     * Calculate file size in bytes for the given iceberg table by it's partition stats.
     * The result is computed by all ContentFiles, including DataFile and DeleteFile.
     */
    public static long calculateFileSizeInBytes(FeIcebergTable table) {
      return table.getIcebergPartitionStats().values().stream()
          .mapToLong(TIcebergPartitionStats::getFile_size_in_bytes).sum();
    }

    /**
     * Get the field schema list of the current PartitionSpec from Iceberg table.
     *
     * @return a list of {@link org.apache.hadoop.hive.metastore.api.FieldSchema}
     */
    public static List<FieldSchema> getPartitionTransformKeys(FeIcebergTable table)
        throws ImpalaRuntimeException {
      Table icebergTable = table.getIcebergApiTable();

      if (icebergTable.specs().isEmpty()) {
        return null;
      }

      PartitionSpec latestSpec = icebergTable.spec();
      Map<String, Integer> transformParams = IcebergUtil.getPartitionTransformParams(
          latestSpec);
      List<FieldSchema> fieldSchemaList = Lists.newArrayList();
      for (PartitionField field : latestSpec.fields()) {
        FieldSchema fieldSchema = new FieldSchema();
        fieldSchema.setName(table.getIcebergSchema().findColumnName(field.sourceId()));
        fieldSchema.setType(
            IcebergUtil.getPartitionTransform(field, transformParams).toSql());
        fieldSchemaList.add(fieldSchema);
      }
      return fieldSchemaList;
    }

    /**
     * Get Iceberg table catalog location by table properties
     */
    public static String getIcebergCatalogLocation(FeIcebergTable table) {
      if (table.getIcebergCatalog() == TIcebergCatalog.HADOOP_CATALOG) {
        return getIcebergCatalogLocation(table.getMetaStoreTable());
      } else {
        return table.getIcebergTableLocation();
      }
    }

    /**
     * When using 'hadoop.catalog', we need to use this method to get qualified catalog
     * location, for example: transform '/test-warehouse/hadoop_catalog_test' to
     * 'hdfs://localhost:20500/test-warehouse/hadoop_catalog_test'
     */
    public static String getIcebergCatalogLocation(
        org.apache.hadoop.hive.metastore.api.Table msTable) {
      String location =
          msTable.getParameters().get(IcebergTable.ICEBERG_CATALOG_LOCATION);
      return FileSystemUtil.createFullyQualifiedPath(new Path(location)).toString();
    }

    /**
     * Get iceberg parquet compression codec from hms table properties
     */
    public static TCompressionCodec getIcebergParquetCompressionCodec(
        org.apache.hadoop.hive.metastore.api.Table msTable) {
      THdfsCompression codec = IcebergUtil.getIcebergParquetCompressionCodec(
          msTable.getParameters().get(IcebergTable.PARQUET_COMPRESSION_CODEC));
      if (codec == null) codec = IcebergTable.DEFAULT_PARQUET_COMPRESSION_CODEC;
      TCompressionCodec compression = new TCompressionCodec(codec);

      // Compression level is interesting only if ZSTD codec is used.
      if (codec == THdfsCompression.ZSTD) {
        int clevel = IcebergTable.DEFAULT_PARQUET_ZSTD_COMPRESSION_LEVEL;

        String clevelTblProp = msTable.getParameters().get(
            IcebergTable.PARQUET_COMPRESSION_LEVEL);
        if (clevelTblProp != null) {
          Integer cl = Ints.tryParse(clevelTblProp);
          if (cl != null && cl >= IcebergTable.MIN_PARQUET_COMPRESSION_LEVEL &&
              cl <= IcebergTable.MAX_PARQUET_COMPRESSION_LEVEL) {
            clevel = cl;
          }
        }
        compression.setCompression_level(clevel);
      }

      return compression;
    }

    /**
     * Get iceberg parquet row group size from hms table properties
     */
    public static long getIcebergParquetRowGroupSize(
        org.apache.hadoop.hive.metastore.api.Table msTable) {
      return IcebergUtil.getIcebergParquetRowGroupSize(
          msTable.getParameters().get(IcebergTable.PARQUET_ROW_GROUP_SIZE));
    }

    /**
     * Get iceberg parquet plain page size from hms table properties
     */
    public static long getIcebergParquetPlainPageSize(
        org.apache.hadoop.hive.metastore.api.Table msTable) {
      return IcebergUtil.getIcebergParquetPageSize(
          msTable.getParameters().get(IcebergTable.PARQUET_PLAIN_PAGE_SIZE));
    }

    /**
     * Get iceberg parquet dictionary page size from hms table properties
     */
    public static long getIcebergParquetDictPageSize(
        org.apache.hadoop.hive.metastore.api.Table msTable) {
      return IcebergUtil.getIcebergParquetPageSize(
          msTable.getParameters().get(IcebergTable.PARQUET_DICT_PAGE_SIZE));
    }

    public static TIcebergTable getTIcebergTable(FeIcebergTable icebergTable) {
      return getTIcebergTable(icebergTable, ThriftObjectType.FULL);
    }

    public static TIcebergTable getTIcebergTable(FeIcebergTable icebergTable,
        ThriftObjectType type) {
      TIcebergTable tIcebergTable = new TIcebergTable();
      tIcebergTable.setTable_location(icebergTable.getIcebergTableLocation());

      for (IcebergPartitionSpec partitionSpec : icebergTable.getPartitionSpecs()) {
        tIcebergTable.addToPartition_spec(partitionSpec.toThrift());
      }
      tIcebergTable.setDefault_partition_spec_id(
          icebergTable.getDefaultPartitionSpecId());

      if (type == ThriftObjectType.FULL) {
        tIcebergTable.setContent_files(icebergTable.getContentFileStore().toThrift());
      }

      tIcebergTable.setCatalog_snapshot_id(icebergTable.snapshotId());
      tIcebergTable.setParquet_compression_codec(
          icebergTable.getIcebergParquetCompressionCodec());
      tIcebergTable.setParquet_row_group_size(
          icebergTable.getIcebergParquetRowGroupSize());
      tIcebergTable.setParquet_plain_page_size(
          icebergTable.getIcebergParquetPlainPageSize());
      tIcebergTable.setParquet_dict_page_size(
          icebergTable.getIcebergParquetDictPageSize());
      tIcebergTable.setPartition_stats(icebergTable.getIcebergPartitionStats());
      return tIcebergTable;
    }

    /**
     * Load the file descriptors from the thrift-encoded 'tFileDescMap'. Optionally
     * translate the file descriptors with the given 'networkAddresses'/'hostIndex'.
     */
    public static Map<String, FileDescriptor> loadFileDescMapFromThrift(
        Map<String, THdfsFileDesc> tFileDescMap,
        List<TNetworkAddress> networkAddresses,
        ListMap<TNetworkAddress> hostIndex) {
      Map<String, FileDescriptor> fileDescMap = new HashMap<>();
      if (tFileDescMap == null) return fileDescMap;
      for (Map.Entry<String, THdfsFileDesc> entry : tFileDescMap.entrySet()) {
        FileDescriptor fd = FileDescriptor.fromThrift(entry.getValue());
        Preconditions.checkNotNull(fd);
        if (networkAddresses == null) {
          fileDescMap.put(entry.getKey(), fd);
        } else {
          Preconditions.checkNotNull(hostIndex);
          fileDescMap.put(entry.getKey(),
              fd.cloneWithNewHostIndex(networkAddresses, hostIndex));
        }
      }
      return fileDescMap;
    }

    /**
     * Get FileDescriptor by data file location
     */
    public static HdfsPartition.FileDescriptor getFileDescriptor(
        ContentFile<?> contentFile, FeIcebergTable table) throws IOException {
      Path fileLoc = FileSystemUtil.createFullyQualifiedPath(
          new Path(contentFile.path().toString()));
      FileSystem fsForPath = FileSystemUtil.getFileSystemForPath(fileLoc);
      FileStatus fileStatus;
      if (FileSystemUtil.supportsStorageIds(fsForPath)) {
        fileStatus = createLocatedFileStatus(fileLoc, fsForPath);
      } else {
        // For OSS service (e.g. S3A, COS, OSS, etc), we create FileStatus ourselves.
        fileStatus = createFileStatus(contentFile, fileLoc);
      }
      return getFileDescriptor(fsForPath, fileStatus, table);
    }

    private static HdfsPartition.FileDescriptor getFileDescriptor(FileSystem fs,
        FileStatus fileStatus, FeIcebergTable table) throws IOException {
      Reference<Long> numUnknownDiskIds = new Reference<>(0L);

      String absPath = null;
      Path tableLoc = new Path(table.getIcebergTableLocation());
      String relPath = FileSystemUtil.relativizePathNoThrow(
          fileStatus.getPath(), tableLoc);
      if (relPath == null) {
        if (Utils.requiresDataFilesInTableLocation(table)) {
          throw new RuntimeException(fileStatus.getPath()
              + " is outside of the Iceberg table location " + tableLoc);
        }
        absPath = fileStatus.getPath().toString();
      }

      if (!FileSystemUtil.supportsStorageIds(fs)) {
        return HdfsPartition.FileDescriptor.createWithNoBlocks(
            fileStatus, relPath, absPath);
      }

      BlockLocation[] locations;
      if (fileStatus instanceof LocatedFileStatus) {
        locations = ((LocatedFileStatus) fileStatus).getBlockLocations();
      } else {
        locations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
      }

      return HdfsPartition.FileDescriptor.create(fileStatus, relPath, locations,
          table.getHostIndex(), fileStatus.isEncrypted(), fileStatus.isErasureCoded(),
          numUnknownDiskIds, absPath);
    }

    /**
     * Returns the FileDescriptors loaded by the internal HdfsTable. To avoid returning
     * the metadata files the resultset is limited to the files that are tracked by
     * Iceberg. Both the HdfsBaseDir and the DataFile path can contain the scheme in their
     * path, using org.apache.hadoop.fs.Path to normalize the paths.
     */
    public static IcebergContentFileStore loadAllPartition(
        IcebergTable table, GroupedContentFiles icebergFiles)
        throws IOException, ImpalaRuntimeException {
      Map<String, HdfsPartition.FileDescriptor> hdfsFileDescMap = new HashMap<>();
      Collection<HdfsPartition> partitions =
          ((HdfsTable)table.getFeFsTable()).partitionMap_.values();
      for (HdfsPartition partition : partitions) {
        for (FileDescriptor fileDesc : partition.getFileDescriptors()) {
          Path path = new Path(fileDesc.getAbsolutePath(table.getHdfsBaseDir()));
          hdfsFileDescMap.put(path.toUri().getPath(), fileDesc);
        }
      }
      IcebergContentFileStore fileStore = new IcebergContentFileStore();
      Pair<String, HdfsPartition.FileDescriptor> pathHashAndFd;
      for (DataFile dataFile : icebergFiles.dataFilesWithoutDeletes) {
        pathHashAndFd = getPathHashAndFd(dataFile, table, hdfsFileDescMap);
        fileStore.addDataFileWithoutDeletes(pathHashAndFd.first, pathHashAndFd.second);
      }
      for (DataFile dataFile : icebergFiles.dataFilesWithDeletes) {
        pathHashAndFd = getPathHashAndFd(dataFile, table, hdfsFileDescMap);
        fileStore.addDataFileWithDeletes(pathHashAndFd.first, pathHashAndFd.second);
      }
      for (DeleteFile deleteFile : icebergFiles.positionDeleteFiles) {
        pathHashAndFd = getPathHashAndFd(deleteFile, table, hdfsFileDescMap);
        fileStore.addPositionDeleteFile(pathHashAndFd.first, pathHashAndFd.second);
      }
      for (DeleteFile deleteFile : icebergFiles.equalityDeleteFiles) {
        pathHashAndFd = getPathHashAndFd(deleteFile, table, hdfsFileDescMap);
        fileStore.addEqualityDeleteFile(pathHashAndFd.first, pathHashAndFd.second);
      }
      return fileStore;
    }

    private static Pair<String, HdfsPartition.FileDescriptor> getPathHashAndFd(
        ContentFile<?> contentFile, IcebergTable table,
        Map<String, HdfsPartition.FileDescriptor> hdfsFileDescMap) throws IOException {
      String pathHash = IcebergUtil.getFilePathHash(contentFile);
      HdfsPartition.FileDescriptor fd = getOrCreateIcebergFd(
          table, hdfsFileDescMap, contentFile);
      return new Pair<>(pathHash, fd);
    }

    private static FileDescriptor getOrCreateIcebergFd(IcebergTable table,
        Map<String, HdfsPartition.FileDescriptor> hdfsFileDescMap,
        ContentFile<?> contentFile) throws IllegalArgumentException, IOException {
      Path path = new Path(contentFile.path().toString());
      HdfsPartition.FileDescriptor iceFd = null;
      if (hdfsFileDescMap.containsKey(path.toUri().getPath())) {
        HdfsPartition.FileDescriptor fsFd = hdfsFileDescMap.get(
            path.toUri().getPath());
        iceFd = fsFd.cloneWithFileMetadata(
            IcebergUtil.createIcebergMetadata(table, contentFile));
      } else {
        if (Utils.requiresDataFilesInTableLocation(table)) {
          LOG.warn("Iceberg file '{}' cannot be found in the HDFS recursive"
           + "file listing results.", path);
        }
        HdfsPartition.FileDescriptor fileDesc = getFileDescriptor(contentFile, table);
        iceFd = fileDesc.cloneWithFileMetadata(
            IcebergUtil.createIcebergMetadata(table, contentFile));
      }
      return iceFd;
    }

    /**
     * Return the PartitionContent loaded by the internal HdfsTable. To avoid returning
     * the metadata files the result set is limited to the files that are tracked by
     * Iceberg. Both the number of rows and number of files show in partitionStats.
     * TODO(IMPALA-11516): Return better partition stats for V2 tables.
     */
    public static Map<String, TIcebergPartitionStats> loadPartitionStats(
        IcebergTable table, GroupedContentFiles icebergFiles) {
      Map<String, TIcebergPartitionStats> nameToStats = new HashMap<>();
      for (ContentFile<?> contentFile : icebergFiles.getAllContentFiles()) {
        String name = getPartitionKey(table, contentFile);
        nameToStats.put(name, mergePartitionStats(nameToStats, contentFile, name));
      }
      return nameToStats;
    }

    /**
     * Return the iceberg partition statistics for merging partitionNameToStats and
     * contentFile based on partitionName. We only count num_rows for FileContent.DATA.
     */
    private static TIcebergPartitionStats mergePartitionStats(
        Map<String, TIcebergPartitionStats> partitionNameToStats,
        ContentFile<?> contentFile, String partitionName) {
      TIcebergPartitionStats info;
      if (partitionNameToStats.containsKey(partitionName)) {
        info = partitionNameToStats.get(partitionName);
        if (contentFile.content().equals(FileContent.DATA)) {
          info.num_rows += contentFile.recordCount();
        }
        info.num_files += 1;
        info.file_size_in_bytes += contentFile.fileSizeInBytes();
      } else {
        info = new TIcebergPartitionStats();
        if (contentFile.content().equals(FileContent.DATA)) {
          info.num_rows = contentFile.recordCount();
        }
        info.num_files = 1;
        info.file_size_in_bytes = contentFile.fileSizeInBytes();
      }
      return info;
    }

    /**
     * Get iceberg partition from a dataFile and wrapper to a json string
     */
    public static String getPartitionKey(IcebergTable table, ContentFile<?> contentFile) {
      PartitionSpec spec = table.getIcebergApiTable().specs().get(contentFile.specId());
      Map<String, String> fieldNameToPartitionValue = new LinkedHashMap<>();
      for (int i = 0; i < spec.fields().size(); ++i) {
        Object partValue = contentFile.partition().get(i, Object.class);
        String partValueString = null;
        if (partValue != null) {
          partValueString = partValue.toString();
        }
        fieldNameToPartitionValue.put(spec.fields().get(i).name(), partValueString);
      }
      return JSONValue.toJSONString(fieldNameToPartitionValue);
    }

    /**
     * Get iceberg partition spec by iceberg table metadata
     */
    public static List<IcebergPartitionSpec> loadPartitionSpecByIceberg(
        FeIcebergTable table) throws ImpalaRuntimeException {
      List<IcebergPartitionSpec> ret = new ArrayList<>();
      Table iceApiTable = table.getIcebergApiTable();
      for (PartitionSpec spec : iceApiTable.specs().values()) {
        ret.add(convertPartitionSpec(iceApiTable.schema(), spec));
      }
      return ret;
    }

    public static IcebergPartitionSpec convertPartitionSpec(Schema schema,
        PartitionSpec spec) throws ImpalaRuntimeException {
      List<IcebergPartitionField> fields = new ArrayList<>();
      Map<String, Integer> transformParams =
          IcebergUtil.getPartitionTransformParams(spec);
      for (PartitionField field : spec.fields()) {
        fields.add(new IcebergPartitionField(field.sourceId(), field.fieldId(),
            spec.schema().findColumnName(field.sourceId()), field.name(),
            IcebergUtil.getPartitionTransform(field, transformParams),
            IcebergSchemaConverter.toImpalaType(
                field.transform().getResultType(schema.findType(field.sourceId())))));
      }
      return new IcebergPartitionSpec(spec.specId(), fields);
    }

    public static IcebergPartitionSpec getDefaultPartitionSpec(
        FeIcebergTable feIcebergTable) {
      List<IcebergPartitionSpec> specs = feIcebergTable.getPartitionSpecs();
      Preconditions.checkState(specs != null);
      if (specs.isEmpty()) return null;
      int defaultSpecId = feIcebergTable.getIcebergApiTable().spec().specId();
      Preconditions.checkState(specs.size() > defaultSpecId);
      return specs.get(defaultSpecId);
    }

    /**
     * Update iceberg table partition file format by table file format
     */
    public static void updateIcebergPartitionFileFormat(FeIcebergTable icebergTable,
        THdfsTable hdfsTable) {
      for (Map.Entry<Long, THdfsPartition> entry : hdfsTable.getPartitions().entrySet()) {
        THdfsPartition partition = entry.getValue();
        partition.getHdfs_storage_descriptor().setFileFormat(
            IcebergUtil.toTHdfsFileFormat(icebergTable.getIcebergFileFormat()));
      }
    }

    /**
     * Get record count for a table with the given time travel spec. Returns -1 when
     * the record count cannot be retrieved from the table summary.
     * If 'travelSpec' is null then the current snapshot is being used.
     */
    public static long getRecordCountV1(Table icebergTable, TimeTravelSpec travelSpec) {
      Map<String, String> summary = getSnapshotSummary(icebergTable, travelSpec);
      if (summary == null) return -1;

      String totalRecordsStr = summary.get(SnapshotSummary.TOTAL_RECORDS_PROP);
      if (Strings.isNullOrEmpty(totalRecordsStr)) return -1;
      try {
        return Long.parseLong(totalRecordsStr);
      } catch (NumberFormatException ex) {
        LOG.warn("Failed to get {} from iceberg table summary. Table name: {}, " +
                 "Table location: {}, Prop value: {}",
            SnapshotSummary.TOTAL_RECORDS_PROP, icebergTable.name(),
            icebergTable.location(), totalRecordsStr, ex);
      }
      return -1;
    }

    /**
     * Return the record count that is calculated by all DataFiles without deletes.
     */
    public static long getRecordCountV2(FeIcebergTable table, TimeTravelSpec travelSpec)
        throws AnalysisException {
      if (travelSpec == null) {
        return table.getContentFileStore()
            .getDataFilesWithoutDeletes().stream()
            .mapToLong(file -> file.getFbFileMetadata().icebergMetadata().recordCount())
            .sum();
      }
      try {
        return IcebergUtil.getIcebergFiles(table, Lists.newArrayList(), travelSpec)
            .dataFilesWithoutDeletes.stream()
            .mapToLong(ContentFile::recordCount)
            .sum();
      } catch (TableLoadingException e) {
        throw new AnalysisException("Failed to get record count of Iceberg V2 table: "
            + table.getFullName() ,e);
      }
    }

    /**
     * Return true if the Iceberg has DeleteFiles. Only non-dangling delete files count
     * so we don't use the snapshot summary for this.
     */
    public static boolean hasDeleteFiles(FeIcebergTable table,
        TimeTravelSpec travelSpec) throws AnalysisException {
      if (travelSpec == null) {
        IcebergContentFileStore fileStore = table.getContentFileStore();
        return !fileStore.getPositionDeleteFiles().isEmpty()
            || !fileStore.getEqualityDeleteFiles().isEmpty();
      } else {
        try {
          GroupedContentFiles groupedFiles =
              IcebergUtil.getIcebergFiles(table, Lists.newArrayList(), travelSpec);
          return !groupedFiles.positionDeleteFiles.isEmpty()
              || !groupedFiles.equalityDeleteFiles.isEmpty();
        } catch (TableLoadingException e) {
          throw new AnalysisException("Failed to get record count of Iceberg V2 table: "
              + table.getFullName(), e);
        }
      }
    }

    /**
     * Get the snapshot summary from the Iceberg table.
     */
    private static Map<String, String> getSnapshotSummary(Table icebergTable,
        TimeTravelSpec travelSpec) {
      Snapshot snapshot = getIcebergSnapshot(icebergTable, travelSpec);
      // There are no snapshots for the tables created for the first time.
      if (snapshot == null) return null;
      return snapshot.summary();
    }

    /**
     * Get time-travel snapshot or current snapshot of the Iceberg table.
     * Only current snapshot can return null.
     */
    private static Snapshot getIcebergSnapshot(Table icebergTable,
        TimeTravelSpec travelSpec) {
      if (travelSpec == null) return icebergTable.currentSnapshot();
      Snapshot snapshot;
      if (travelSpec.getKind().equals(Kind.VERSION_AS_OF)) {
        long snapshotId = travelSpec.getAsOfVersion();
        snapshot = icebergTable.snapshot(snapshotId);
        Preconditions.checkArgument(snapshot != null, "Cannot find snapshot with ID %s",
            snapshotId);
      } else {
        long timestampMillis = travelSpec.getAsOfMillis();
        long snapshotId = SnapshotUtil.snapshotIdAsOfTime(icebergTable, timestampMillis);
        snapshot = icebergTable.snapshot(snapshotId);
        Preconditions.checkArgument(snapshot != null,
            "Cannot find snapshot with ID %s, timestampMillis %s", snapshotId,
            timestampMillis);
      }
      return snapshot;
    }

    public static boolean requiresDataFilesInTableLocation(FeIcebergTable icebergTable) {
      Table icebergApiTable = icebergTable.getIcebergApiTable();
      Preconditions.checkNotNull(icebergApiTable);
      Map<String, String> properties = icebergApiTable.properties();
      if (BackendConfig.INSTANCE.icebergAllowDatafileInTableLocationOnly()) {
        return true;
      }
      return !(PropertyUtil.propertyAsBoolean(properties,
          TableProperties.OBJECT_STORE_ENABLED,
          TableProperties.OBJECT_STORE_ENABLED_DEFAULT)
          || StringUtils.isNotEmpty(properties.get(TableProperties.WRITE_DATA_LOCATION))
          || StringUtils
          .isNotEmpty(properties.get(TableProperties.WRITE_LOCATION_PROVIDER_IMPL))
          || StringUtils.isNotEmpty(properties.get(TableProperties.OBJECT_STORE_PATH))
          || StringUtils
          .isNotEmpty(properties.get(TableProperties.WRITE_FOLDER_STORAGE_LOCATION)));
    }

    public static FileStatus createLocatedFileStatus(Path path, FileSystem fs)
        throws IOException {
      FileStatus fileStatus = fs.getFileStatus(path);
      Preconditions.checkState(fileStatus.isFile());
      BlockLocation[] blockLocations = fs.getFileBlockLocations(fileStatus, 0L,
          fileStatus.getLen());
      return new LocatedFileStatus(fileStatus, blockLocations);
    }

    public static FileStatus createFileStatus(ContentFile<?> contentFile, Path path) {
      return new FileStatus(contentFile.fileSizeInBytes(), false, 0, 0,
          DEFAULT_MODIFICATION_TIME, path);
    }

    public static long getTotalNumberOfFiles(FeIcebergTable icebergTable,
        TimeTravelSpec travelSpec)
        throws ImpalaRuntimeException {
      Map<String, String> snapshotSummary = getSnapshotSummary(
          icebergTable.getIcebergApiTable(),
          travelSpec);
      if (snapshotSummary == null) {
        throw new ImpalaRuntimeException("Invalid Iceberg snapshot summary");
      }
      try {
        String totalDataFilesProp = snapshotSummary.get(
            SnapshotSummary.TOTAL_DATA_FILES_PROP);
        String totalDeleteFilesProp = snapshotSummary.getOrDefault(
            SnapshotSummary.TOTAL_DELETE_FILES_PROP, "0");
        long totalDataFiles = Long.parseLong(totalDataFilesProp);
        long totalDeleteFiles = Long.parseLong(totalDeleteFilesProp);
        return totalDataFiles + totalDeleteFiles;
      } catch (NumberFormatException e) {
        throw new ImpalaRuntimeException("Invalid Iceberg snapshot summary value");
      }
    }
  }
}
