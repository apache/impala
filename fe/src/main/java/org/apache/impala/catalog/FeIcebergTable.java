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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.impala.analysis.IcebergPartitionField;
import org.apache.impala.analysis.IcebergPartitionSpec;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.common.Reference;
import org.apache.impala.compat.HdfsShim;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TCompressionCodec;
import org.apache.impala.thrift.THdfsCompression;
import org.apache.impala.thrift.THdfsFileDesc;
import org.apache.impala.thrift.THdfsTable;
import org.apache.impala.thrift.THdfsPartition;
import org.apache.impala.thrift.TIcebergCatalog;
import org.apache.impala.thrift.TIcebergFileFormat;
import org.apache.impala.thrift.TIcebergTable;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.thrift.TResultSetMetadata;
import org.apache.impala.util.IcebergUtil;
import org.apache.impala.util.ListMap;
import org.apache.impala.util.TResultRowBuilder;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Frontend interface for interacting with an Iceberg-backed table.
 */
public interface FeIcebergTable extends FeFsTable {
  final static Logger LOG = LoggerFactory.getLogger(FeIcebergTable.class);
  /**
   * FileDescriptor map
   */
  Map<String, HdfsPartition.FileDescriptor> getPathHashToFileDescMap();

  /**
   * Return the hdfs table transformed from iceberg table
   */
  FeFsTable getFeFsTable();

  /**
   * Return iceberg catalog type from table properties
   */
  TIcebergCatalog getIcebergCatalog();

  /**
   * Returns the BaseTable that stores the metadata loaded by Iceberg.
   */
  BaseTable getIcebergBaseTable();

  /**
   * Returns the TableMetadata from the BaseTable.
   */
  default TableMetadata getIcebergTableMetadata() {
    return getIcebergBaseTable().operations().current();
  }

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

  /**
   * @return the Iceberg schema.
   */
  Schema getIcebergSchema();

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

  /**
   * Current snapshot id of the table.
   */
  default long snapshotId() {
    if (getIcebergBaseTable().currentSnapshot() != null) {
      return getIcebergTableMetadata().currentSnapshot().snapshotId();
    }
    return -1;
  }

  /**
   * Utility functions
   */
  public static abstract class Utils {
    /**
     * Returns true if FeIcebergTable file format is columnar: parquet or orc
     */
    public static boolean isColumnar(FeIcebergTable table) {
      HdfsFileFormat format = IcebergUtil.toHdfsFileFormat(table.getIcebergFileFormat());
      return format == HdfsFileFormat.PARQUET || format == HdfsFileFormat.ORC;
    }

    public static TResultSet getPartitionSpecs(FeIcebergTable table)
        throws TableLoadingException {
      TResultSet result = new TResultSet();
      TResultSetMetadata resultSchema = new TResultSetMetadata();
      result.setSchema(resultSchema);

      resultSchema.addToColumns(new TColumn("Partition Id", Type.BIGINT.toThrift()));
      resultSchema.addToColumns(new TColumn("Source Id", Type.BIGINT.toThrift()));
      resultSchema.addToColumns(new TColumn("Field Id", Type.BIGINT.toThrift()));
      resultSchema.addToColumns(new TColumn("Field Name", Type.STRING.toThrift()));
      resultSchema.addToColumns(new TColumn("Field Partition Transform",
          Type.STRING.toThrift()));

      TableMetadata metadata = table.getIcebergTableMetadata();
      if (!metadata.specs().isEmpty()) {
        // Just show the current PartitionSpec from Iceberg table metadata
        PartitionSpec latestSpec = metadata.spec();
        HashMap<String, Integer> transformParams =
            IcebergUtil.getPartitionTransformParams(latestSpec);
        for(PartitionField field : latestSpec.fields()) {
          TResultRowBuilder builder = new TResultRowBuilder();
          builder.add(latestSpec.specId());
          builder.add(field.sourceId());
          builder.add(field.fieldId());
          builder.add(field.name());
          builder.add(IcebergUtil.getPartitionTransform(field, transformParams).toSql());
          result.addToRows(builder.get());
        }
      }
      return result;
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
      TIcebergTable tIcebergTable = new TIcebergTable();
      tIcebergTable.setTable_location(icebergTable.getIcebergTableLocation());

      for (IcebergPartitionSpec partitionSpec : icebergTable.getPartitionSpecs()) {
        tIcebergTable.addToPartition_spec(partitionSpec.toThrift());
      }
      tIcebergTable.setDefault_partition_spec_id(
          icebergTable.getDefaultPartitionSpecId());

      tIcebergTable.setPath_hash_to_file_descriptor(
          convertPathHashToFileDescMap(icebergTable));

      tIcebergTable.setParquet_compression_codec(
          icebergTable.getIcebergParquetCompressionCodec());
      tIcebergTable.setParquet_row_group_size(
          icebergTable.getIcebergParquetRowGroupSize());
      tIcebergTable.setParquet_plain_page_size(
          icebergTable.getIcebergParquetPlainPageSize());
      tIcebergTable.setParquet_dict_page_size(
      icebergTable.getIcebergParquetDictPageSize());
      tIcebergTable.setIceberg_base_table(serializeIcebergBaseTable(
          icebergTable.getIcebergBaseTable()));
      return tIcebergTable;
    }

    public static Map<String, THdfsFileDesc> convertPathHashToFileDescMap(
        FeIcebergTable icebergTable) {
      Map<String, THdfsFileDesc> ret = new HashMap<>();
      for (Map.Entry<String, HdfsPartition.FileDescriptor> entry :
          icebergTable.getPathHashToFileDescMap().entrySet()) {
        ret.put(entry.getKey(), entry.getValue().toThrift());
      }
      return ret;
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

    public static byte[] serializeIcebergBaseTable(BaseTable baseTable) {
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      ObjectOutputStream objectOutputStream = null;
      byte[] result = new byte[]{};
      try {
        objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
        objectOutputStream.writeObject(baseTable);
        objectOutputStream.flush();
        result = byteArrayOutputStream.toByteArray();
      } catch (IOException e) {
        LOG.warn("Failed to serialize Iceberg BaseTable: " + e.getMessage());
      }
      finally {
        try {
          byteArrayOutputStream.close();
        } catch (IOException ex) {
          // ignore close exception
        }
      }
      return result;
    }

    public static BaseTable deserializeIcebergBaseTable(byte[] serializedBaseTable)
        throws TableLoadingException {
      Object baseTable = null;
      ByteArrayInputStream inputStream = new ByteArrayInputStream(serializedBaseTable);
      ObjectInput objectInput = null;
      try {
        objectInput = new ObjectInputStream(inputStream);
        baseTable = objectInput.readObject();
      } catch (Exception e) {
        throw new TableLoadingException("Failed to deserialize Iceberg BaseTable.", e);
      } finally {
        try {
          if (objectInput != null) {
            objectInput.close();
          }
        } catch (IOException ex) {
          // ignore close exception
        }
      }
      return (BaseTable)baseTable;
    }

    /**
     * Get FileDescriptor by data file location
     */
    public static HdfsPartition.FileDescriptor getFileDescriptor(Path fileLoc,
        Path tableLoc, ListMap<TNetworkAddress> hostIndex) throws IOException {
      FileSystem fs = FileSystemUtil.getFileSystemForPath(tableLoc);
      FileStatus fileStatus = fs.getFileStatus(fileLoc);
      return getFileDescriptor(fs, tableLoc, fileStatus, hostIndex);
    }

    private static HdfsPartition.FileDescriptor getFileDescriptor(FileSystem fs,
        Path tableLoc, FileStatus fileStatus, ListMap<TNetworkAddress> hostIndex)
        throws IOException {
      Reference<Long> numUnknownDiskIds = new Reference<Long>(Long.valueOf(0));
      String relPath = FileSystemUtil.relativizePath(fileStatus.getPath(), tableLoc);

      if (!FileSystemUtil.supportsStorageIds(fs)) {
        return HdfsPartition.FileDescriptor.createWithNoBlocks(fileStatus, relPath);
      }

      BlockLocation[] locations;
      if (fileStatus instanceof LocatedFileStatus) {
        locations = ((LocatedFileStatus)fileStatus).getBlockLocations();
      } else {
        locations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
      }

      return HdfsPartition.FileDescriptor.create(fileStatus, relPath, locations,
          hostIndex, HdfsShim.isErasureCoded(fileStatus), numUnknownDiskIds);
    }

    /**
     * Returns the FileDescriptors loaded by the internal HdfsTable. To avoid returning
     * the metadata files the resulset is limited to the files that are tracked by
     * Iceberg. Both the HdfsBaseDir and the DataFile path can contain the scheme in their
     * path, using org.apache.hadoop.fs.Path to normalize the paths.
     */
    public static Map<String, HdfsPartition.FileDescriptor> loadAllPartition(
        IcebergTable table) throws IOException, TableLoadingException {
      Map<String, HdfsPartition.FileDescriptor> hdfsFileDescMap = new HashMap<>();
      Collection<HdfsPartition> partitions =
          ((HdfsTable)table.getFeFsTable()).partitionMap_.values();
      for (HdfsPartition partition : partitions) {
        for (FileDescriptor fileDesc : partition.getFileDescriptors()) {
            Path path = new Path(table.getHdfsBaseDir() + Path.SEPARATOR +
                fileDesc.getRelativePath());
            hdfsFileDescMap.put(path.toUri().getPath(), fileDesc);
        }
      }
      Map<String, HdfsPartition.FileDescriptor> fileDescMap = new HashMap<>();
      List<DataFile> dataFileList = IcebergUtil.getIcebergDataFiles(table,
          new ArrayList<>(), /*timeTravelSpecl=*/null);
      for (DataFile dataFile : dataFileList) {
          Path path = new Path(dataFile.path().toString());
          if (hdfsFileDescMap.containsKey(path.toUri().getPath())) {
            String pathHash = IcebergUtil.getDataFilePathHash(dataFile);
            fileDescMap.put(pathHash, hdfsFileDescMap.get(path.toUri().getPath()));
          } else {
            LOG.warn("Iceberg DataFile '{}' cannot be found in the HDFS recursive file "
                + "listing results.", path.toString());
            HdfsPartition.FileDescriptor fileDesc = getFileDescriptor(
                new Path(dataFile.path().toString()),
                new Path(table.getIcebergTableLocation()), table.getHostIndex());
            fileDescMap.put(IcebergUtil.getDataFilePathHash(dataFile), fileDesc);
          }
      }
      return fileDescMap;
    }

    /**
     * Get iceberg partition spec by iceberg table metadata
     */
    public static List<IcebergPartitionSpec> loadPartitionSpecByIceberg(
        TableMetadata metadata) throws TableLoadingException {
      List<IcebergPartitionSpec> ret = new ArrayList<>();
      for (PartitionSpec spec : metadata.specs()) {
        ret.add(convertPartitionSpec(spec));
      }
      return ret;
    }

    public static IcebergPartitionSpec convertPartitionSpec(PartitionSpec spec)
        throws TableLoadingException {
      List<IcebergPartitionField> fields = new ArrayList<>();;
      HashMap<String, Integer> transformParams =
          IcebergUtil.getPartitionTransformParams(spec);
      for (PartitionField field : spec.fields()) {
        fields.add(new IcebergPartitionField(field.sourceId(), field.fieldId(),
            spec.schema().findColumnName(field.sourceId()), field.name(),
            IcebergUtil.getPartitionTransform(field, transformParams)));
      }
      return new IcebergPartitionSpec(spec.specId(), fields);
    }

    public static IcebergPartitionSpec getDefaultPartitionSpec(
        FeIcebergTable feIcebergTable) {
      List<IcebergPartitionSpec> specs = feIcebergTable.getPartitionSpecs();
      Preconditions.checkState(specs != null);
      if (specs.isEmpty()) return null;
      int defaultSpecId = feIcebergTable.getDefaultPartitionSpecId();
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
  }
}
