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
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.types.Types;
import org.apache.impala.analysis.IcebergPartitionField;
import org.apache.impala.analysis.IcebergPartitionSpec;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.common.Reference;
import org.apache.impala.compat.HdfsShim;
import org.apache.impala.thrift.TColumn;
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

/**
 * Frontend interface for interacting with an Iceberg-backed table.
 */
public interface FeIcebergTable extends FeFsTable {

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

      TableMetadata metadata = IcebergUtil.getIcebergTableMetadata(table);
      if (!metadata.specs().isEmpty()) {
        // Just show the latest PartitionSpec from iceberg table metadata
        PartitionSpec latestSpec = metadata.specs().get(metadata.specs().size() - 1);
        for(PartitionField field : latestSpec.fields()) {
          TResultRowBuilder builder = new TResultRowBuilder();
          builder.add(latestSpec.specId());
          builder.add(field.sourceId());
          builder.add(field.fieldId());
          builder.add(field.name());
          builder.add(IcebergUtil.getPartitionTransform(field).toString());
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
     * Get iceberg table file format from hms table properties
     */
    public static TIcebergFileFormat getIcebergFileFormat(
        org.apache.hadoop.hive.metastore.api.Table msTable) {
      TIcebergFileFormat fileFormat = IcebergUtil.getIcebergFileFormat(
          msTable.getParameters().get(IcebergTable.ICEBERG_FILE_FORMAT));
      return fileFormat == null ? TIcebergFileFormat.PARQUET : fileFormat;
    }


    public static TIcebergTable getTIcebergTable(FeIcebergTable icebergTable) {
      TIcebergTable tIcebergTable = new TIcebergTable();
      tIcebergTable.setTable_location(icebergTable.getIcebergTableLocation());

      for (IcebergPartitionSpec partitionSpec : icebergTable.getPartitionSpecs()) {
        tIcebergTable.addToPartition_spec(partitionSpec.toThrift());
      }
      tIcebergTable.setDefault_partition_spec_id(
          icebergTable.getDefaultPartitionSpecId());

      for (Map.Entry<String, HdfsPartition.FileDescriptor> entry :
          icebergTable.getPathHashToFileDescMap().entrySet()) {
        tIcebergTable.putToPath_hash_to_file_descriptor(entry.getKey(),
          entry.getValue().toThrift());
      }
      return tIcebergTable;
    }

    /**
     * Get FileDescriptor by data file location
     */
    private static HdfsPartition.FileDescriptor getFileDescriptor(Path fileLoc,
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
     * Get all FileDescriptor from iceberg table without any predicates.
     */
    public static Map<String, HdfsPartition.FileDescriptor> loadAllPartition(
        FeIcebergTable table) throws IOException {
      // Empty predicates
      List<DataFile> dataFileList = IcebergUtil.getIcebergDataFiles(table,
          new ArrayList<>());

      Map<String, HdfsPartition.FileDescriptor> fileDescMap = new HashMap<>();
      for (DataFile file : dataFileList) {
        HdfsPartition.FileDescriptor fileDesc = getFileDescriptor(
            new Path(file.path().toString()),
            new Path(table.getIcebergTableLocation()), table.getHostIndex());
        fileDescMap.put(IcebergUtil.getDataFilePathHash(file), fileDesc);
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
        List<IcebergPartitionField> fields = new ArrayList<>();;
        for (PartitionField field : spec.fields()) {
          fields.add(new IcebergPartitionField(field.sourceId(), field.fieldId(),
              spec.schema().findColumnName(field.sourceId()), field.name(),
              IcebergUtil.getPartitionTransform(field)));
        }
        ret.add(new IcebergPartitionSpec(spec.specId(), fields));
      }
      return ret;
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
        partition.setFileFormat(IcebergUtil.toTHdfsFileFormat(
            icebergTable.getIcebergFileFormat()));
      }
    }
  }
}
