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

package org.apache.impala.util;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import org.apache.impala.catalog.IcebergStructField;
import org.apache.impala.common.Pair;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.transforms.PartitionSpecVisitor;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.impala.analysis.IcebergPartitionField;
import org.apache.impala.analysis.IcebergPartitionSpec;
import org.apache.impala.analysis.IcebergPartitionTransform;
import org.apache.impala.catalog.Catalog;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.HdfsFileFormat;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.catalog.iceberg.IcebergHadoopCatalog;
import org.apache.impala.catalog.iceberg.IcebergHadoopTables;
import org.apache.impala.catalog.iceberg.IcebergHiveCatalog;
import org.apache.impala.catalog.iceberg.IcebergCatalog;
import org.apache.impala.catalog.iceberg.IcebergCatalogs;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.thrift.TCompressionCodec;
import org.apache.impala.thrift.TCreateTableParams;
import org.apache.impala.thrift.THdfsCompression;
import org.apache.impala.thrift.THdfsFileFormat;
import org.apache.impala.thrift.TIcebergCatalog;
import org.apache.impala.thrift.TIcebergFileFormat;
import org.apache.impala.thrift.TIcebergPartitionField;
import org.apache.impala.thrift.TIcebergPartitionSpec;
import org.apache.impala.thrift.TIcebergPartitionTransform;
import org.apache.impala.thrift.TIcebergPartitionTransformType;

public class IcebergUtil {
  private static final int ICEBERG_EPOCH_YEAR = 1970;
  private static final int ICEBERG_EPOCH_MONTH = 1;
  private static final int ICEBERG_EPOCH_DAY = 1;
  private static final int ICEBERG_EPOCH_HOUR = 0;

  /**
   * Returns the corresponding catalog implementation for 'feTable'.
   */
  public static IcebergCatalog getIcebergCatalog(FeIcebergTable feTable)
      throws ImpalaRuntimeException {
    return getIcebergCatalog(feTable.getIcebergCatalog(),
        feTable.getIcebergCatalogLocation());
  }

  /**
   * Returns the corresponding catalog implementation.
   */
  public static IcebergCatalog getIcebergCatalog(TIcebergCatalog catalog, String location)
      throws ImpalaRuntimeException {
    switch (catalog) {
      case HADOOP_TABLES: return IcebergHadoopTables.getInstance();
      case HIVE_CATALOG: return IcebergHiveCatalog.getInstance();
      case HADOOP_CATALOG: return new IcebergHadoopCatalog(location);
      case CATALOGS: return IcebergCatalogs.getInstance();
      default: throw new ImpalaRuntimeException (
          "Unexpected catalog type: " + catalog.toString());
    }
  }

  /**
   * Helper method to load native Iceberg table for 'feTable'.
   */
  public static Table loadTable(FeIcebergTable feTable) throws TableLoadingException {
    return loadTable(feTable.getIcebergCatalog(), getIcebergTableIdentifier(feTable),
        feTable.getIcebergCatalogLocation(), feTable.getMetaStoreTable().getParameters());
  }

  /**
   * Helper method to load native Iceberg table.
   */
  public static Table loadTable(TIcebergCatalog catalog, TableIdentifier tableId,
      String location, Map<String, String> tableProps) throws TableLoadingException {
    try {
      IcebergCatalog cat = getIcebergCatalog(catalog, location);
      return cat.loadTable(tableId, location, tableProps);
    } catch (ImpalaRuntimeException e) {
      throw new TableLoadingException(String.format(
          "Failed to load Iceberg table: %s at location: %s",
          tableId, location), e);
    }
  }

  /**
   * Get TableMetadata by FeIcebergTable
   */
  public static TableMetadata getIcebergTableMetadata(FeIcebergTable table)
      throws TableLoadingException {
    BaseTable iceTable = (BaseTable)IcebergUtil.loadTable(table);
    return iceTable.operations().current();
  }

  /**
   * Get TableMetadata by related info tableName is table full name, usually
   * database.table
   */
  public static TableMetadata getIcebergTableMetadata(TIcebergCatalog catalog,
      TableIdentifier tableId, String location, Map<String, String> tableProps)
      throws TableLoadingException {
    BaseTable baseTable = (BaseTable)IcebergUtil.loadTable(catalog,
        tableId, location, tableProps);
    return baseTable.operations().current();
  }

  /**
   * Get Iceberg table identifier by table property
   */
  public static TableIdentifier getIcebergTableIdentifier(FeIcebergTable table) {
    return getIcebergTableIdentifier(table.getMetaStoreTable());
  }

  public static TableIdentifier getIcebergTableIdentifier(
      org.apache.hadoop.hive.metastore.api.Table msTable) {
    String name = msTable.getParameters().get(IcebergTable.ICEBERG_TABLE_IDENTIFIER);
    if (name == null || name.isEmpty()) {
      // Iceberg's Catalogs API uses table property 'name' for the table id.
      name = msTable.getParameters().get(Catalogs.NAME);
    }
    if (name == null || name.isEmpty()) {
      return TableIdentifier.of(msTable.getDbName(), msTable.getTableName());
    }

    // If database not been specified in property, use default
    if (!name.contains(".")) {
      return TableIdentifier.of(Catalog.DEFAULT_DB, name);
    }
    return TableIdentifier.parse(name);
  }

  /**
   * Get Iceberg UpdateSchema from 'feTable', usually use UpdateSchema to update Iceberg
   * table schema.
   */
  public static UpdateSchema getIcebergUpdateSchema(FeIcebergTable feTable)
      throws TableLoadingException, ImpalaRuntimeException {
    return getIcebergCatalog(feTable).loadTable(feTable).updateSchema();
  }

  /**
   * Build iceberg PartitionSpec from TIcebergPartitionSpec.
   * partition columns are all from source columns, this is different from hdfs table.
   */
  public static PartitionSpec createIcebergPartition(Schema schema,
      TIcebergPartitionSpec partSpec) throws ImpalaRuntimeException {
    if (partSpec == null) return PartitionSpec.unpartitioned();

    List<TIcebergPartitionField> partitionFields = partSpec.getPartition_fields();
    PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
    for (TIcebergPartitionField partitionField : partitionFields) {
      TIcebergPartitionTransformType transformType =
          partitionField.getTransform().getTransform_type();
      if (transformType == TIcebergPartitionTransformType.IDENTITY) {
        builder.identity(partitionField.getField_name());
      } else if (transformType == TIcebergPartitionTransformType.HOUR) {
        builder.hour(partitionField.getField_name());
      } else if (transformType == TIcebergPartitionTransformType.DAY) {
        builder.day(partitionField.getField_name());
      } else if (transformType == TIcebergPartitionTransformType.MONTH) {
        builder.month(partitionField.getField_name());
      } else if (transformType == TIcebergPartitionTransformType.YEAR) {
        builder.year(partitionField.getField_name());
      } else if (transformType == TIcebergPartitionTransformType.BUCKET) {
        builder.bucket(partitionField.getField_name(),
            partitionField.getTransform().getTransform_param());
      } else if (transformType == TIcebergPartitionTransformType.TRUNCATE) {
        builder.truncate(partitionField.getField_name(),
            partitionField.getTransform().getTransform_param());
      } else {
        throw new ImpalaRuntimeException(String.format("Skip partition: %s, %s",
            partitionField.getField_name(), transformType));
      }
    }
    return builder.build();
  }

  /**
   * Returns true if 'msTable' uses HiveCatalog.
   */
  public static boolean isHiveCatalog(
      org.apache.hadoop.hive.metastore.api.Table msTable) {
    TIcebergCatalog tCat = getTIcebergCatalog(msTable);
    if (tCat == TIcebergCatalog.HIVE_CATALOG) return true;
    if (tCat == TIcebergCatalog.CATALOGS) {
      String catName = msTable.getParameters().get(IcebergTable.ICEBERG_CATALOG);
      tCat = IcebergCatalogs.getInstance().getUnderlyingCatalogType(catName);
      return tCat == TIcebergCatalog.HIVE_CATALOG;
    }
    return false;
  }

  /**
   * Get iceberg table catalog type from hms table properties
   * use HiveCatalog as default
   */
  public static TIcebergCatalog getTIcebergCatalog(
      org.apache.hadoop.hive.metastore.api.Table msTable) {
    return getTIcebergCatalog(
        msTable.getParameters().get(IcebergTable.ICEBERG_CATALOG));
  }

  /**
   * Get TIcebergCatalog from a string, usually from table properties
   */
  public static TIcebergCatalog getTIcebergCatalog(String catalog){
    if ("hadoop.tables".equalsIgnoreCase(catalog)) {
      return TIcebergCatalog.HADOOP_TABLES;
    } else if ("hadoop.catalog".equalsIgnoreCase(catalog)) {
      return TIcebergCatalog.HADOOP_CATALOG;
    } else if ("hive.catalog".equalsIgnoreCase(catalog) ||
               catalog == null) {
      return TIcebergCatalog.HIVE_CATALOG;
    }
    return TIcebergCatalog.CATALOGS;
  }

  /**
   * Return the underlying Iceberg catalog when Iceberg Catalogs is being used, simply
   * return the Iceberg catalog otherwise.
   */
  public static TIcebergCatalog getUnderlyingCatalog(
      org.apache.hadoop.hive.metastore.api.Table msTable) {
    return getUnderlyingCatalog(
        msTable.getParameters().get(IcebergTable.ICEBERG_CATALOG));
  }

  /**
   * Return the underlying Iceberg catalog when Iceberg Catalogs is being used, simply
   * return the Iceberg catalog otherwise.
   */
  public static TIcebergCatalog getUnderlyingCatalog(String catalog) {
    TIcebergCatalog tCat = getTIcebergCatalog(catalog);
    if (tCat == TIcebergCatalog.CATALOGS) {
      return IcebergCatalogs.getInstance().getUnderlyingCatalogType(catalog);
    }
    return tCat;
  }

  /**
   * Get Iceberg table catalog location with 'iceberg.catalog_location' when using
   * 'hadoop.catalog'
   */
  public static String getIcebergCatalogLocation(
      org.apache.hadoop.hive.metastore.api.Table msTable) {
    return msTable.getParameters().get(IcebergTable.ICEBERG_CATALOG_LOCATION);
  }

  /**
   * Get TIcebergFileFormat from a string, usually from table properties.
   * Returns PARQUET when 'format' is null. Returns null for invalid formats.
   */
  public static TIcebergFileFormat getIcebergFileFormat(String format) {
    if ("PARQUET".equalsIgnoreCase(format) || format == null) {
      return TIcebergFileFormat.PARQUET;
    } else if ("ORC".equalsIgnoreCase(format)) {
      return TIcebergFileFormat.ORC;
    }
    return null;
  }

  /**
   * Map from parquet compression codec names to a compression type.
   * The list of parquet supported compression codecs was taken from
   * hdfs-parquet-table-writer.cc.
   */
  public static final ImmutableMap<String, THdfsCompression> PARQUET_CODEC_MAP =
      ImmutableMap.<String, THdfsCompression>builder().
          put("none", THdfsCompression.NONE).
          put("gzip", THdfsCompression.GZIP).
          put("snappy", THdfsCompression.SNAPPY).
          put("lz4", THdfsCompression.LZ4).
          put("zstd", THdfsCompression.ZSTD).
          build();

  public static THdfsCompression getIcebergParquetCompressionCodec(String codec) {
    if (codec == null) return IcebergTable.DEFAULT_PARQUET_COMPRESSION_CODEC;
    return PARQUET_CODEC_MAP.get(codec.toLowerCase());
  }

  public static long getIcebergParquetRowGroupSize(String rowGroupSize) {
    if (rowGroupSize == null) return IcebergTable.UNSET_PARQUET_ROW_GROUP_SIZE;

    Long rgSize = Longs.tryParse(rowGroupSize);
    if (rgSize == null || rgSize < IcebergTable.MIN_PARQUET_ROW_GROUP_SIZE ||
        rgSize > IcebergTable.MAX_PARQUET_ROW_GROUP_SIZE) {
      return IcebergTable.UNSET_PARQUET_ROW_GROUP_SIZE;
    }
    return rgSize;
  }

  public static long getIcebergParquetPageSize(String pageSize) {
    if (pageSize == null) return IcebergTable.UNSET_PARQUET_PAGE_SIZE;

    Long pSize = Longs.tryParse(pageSize);
    if (pSize == null || pSize < IcebergTable.MIN_PARQUET_PAGE_SIZE ||
        pSize > IcebergTable.MAX_PARQUET_PAGE_SIZE) {
      return IcebergTable.UNSET_PARQUET_PAGE_SIZE;
    }
    return pSize;
  }

  public static IcebergPartitionTransform getPartitionTransform(
      PartitionField field, HashMap<String, Integer> transformParams)
      throws TableLoadingException {
    String type = field.transform().toString();
    String transformMappingKey = getPartitonTransformMappingKey(field.sourceId(),
        getPartitionTransformType(type));
    return getPartitionTransform(type, transformParams.get(transformMappingKey));
  }

  public static IcebergPartitionTransform getPartitionTransform(String transformType,
      Integer transformParam) throws TableLoadingException {
    return new IcebergPartitionTransform(getPartitionTransformType(transformType),
        transformParam);
  }

  public static IcebergPartitionTransform getPartitionTransform(String transformType)
      throws TableLoadingException {
    return getPartitionTransform(transformType, null);
  }

  public static TIcebergPartitionTransformType getPartitionTransformType(
      String transformType) throws TableLoadingException {
    Preconditions.checkNotNull(transformType);
    transformType = transformType.toUpperCase();
    if ("IDENTITY".equals(transformType)) {
      return TIcebergPartitionTransformType.IDENTITY;
    } else if (transformType != null && transformType.startsWith("BUCKET")) {
      return TIcebergPartitionTransformType.BUCKET;
    } else if (transformType != null && transformType.startsWith("TRUNCATE")) {
      return TIcebergPartitionTransformType.TRUNCATE;
    }
    switch (transformType) {
      case "HOUR":  case "HOURS":  return TIcebergPartitionTransformType.HOUR;
      case "DAY":   case "DAYS":   return TIcebergPartitionTransformType.DAY;
      case "MONTH": case "MONTHS": return TIcebergPartitionTransformType.MONTH;
      case "YEAR":  case "YEARS":  return TIcebergPartitionTransformType.YEAR;
      default:
        throw new TableLoadingException("Unsupported iceberg partition type: " +
            transformType);
    }
  }

  private static String getPartitonTransformMappingKey(int sourceId,
      TIcebergPartitionTransformType transformType) {
    return sourceId + "_" + transformType.toString();
  }

  /**
   * Gets a PartitionSpec object and returns a mapping between a field in the
   * PartitionSpec and its transform's parameter. Only Bucket and Truncate transforms
   * have a parameter, for other transforms this mapping will have a null.
   * source ID and the transform type are needed together to uniquely identify a specific
   * field in the PartitionSpec. (Unfortunaltely, fieldId is not available in the Visitor
   * class below.)
   * The reason for implementing the PartitionSpecVisitor below was that Iceberg doesn't
   * expose the interface of the transform types outside of their package and the only
   * way to get the transform's parameter is implementing this visitor class.
   */
  public static HashMap<String, Integer> getPartitionTransformParams(PartitionSpec spec)
      throws TableLoadingException {
    List<Pair<String, Integer>> transformParams = PartitionSpecVisitor.visit(
        spec.schema(), spec, new PartitionSpecVisitor<Pair<String, Integer>>() {
          @Override
          public Pair<String, Integer> identity(String sourceName, int sourceId) {
            String mappingKey = getPartitonTransformMappingKey(sourceId,
                TIcebergPartitionTransformType.IDENTITY);
            return new Pair<String, Integer>(mappingKey, null);
          }

          @Override
          public Pair<String, Integer> bucket(String sourceName, int sourceId,
              int numBuckets) {
            String mappingKey = getPartitonTransformMappingKey(sourceId,
                TIcebergPartitionTransformType.BUCKET);
            return new Pair<String, Integer>(mappingKey, numBuckets);
          }

          @Override
          public Pair<String, Integer> truncate(String sourceName, int sourceId,
              int width) {
            String mappingKey = getPartitonTransformMappingKey(sourceId,
                TIcebergPartitionTransformType.TRUNCATE);
            return new Pair<String, Integer>(mappingKey, width);
          }

          @Override
          public Pair<String, Integer> year(String sourceName, int sourceId) {
            String mappingKey = getPartitonTransformMappingKey(sourceId,
                TIcebergPartitionTransformType.YEAR);
            return new Pair<String, Integer>(mappingKey, null);
          }

          @Override
          public Pair<String, Integer> month(String sourceName, int sourceId) {
            String mappingKey = getPartitonTransformMappingKey(sourceId,
                TIcebergPartitionTransformType.MONTH);
            return new Pair<String, Integer>(mappingKey, null);
          }

          @Override
          public Pair<String, Integer> day(String sourceName, int sourceId) {
            String mappingKey = getPartitonTransformMappingKey(sourceId,
                TIcebergPartitionTransformType.DAY);
            return new Pair<String, Integer>(mappingKey, null);
          }

          @Override
          public Pair<String, Integer> hour(String sourceName, int sourceId) {
            String mappingKey = getPartitonTransformMappingKey(sourceId,
                TIcebergPartitionTransformType.HOUR);
            return new Pair<String, Integer>(mappingKey, null);
          }
        });
    // Move the content of the List into a HashMap for faster querying in the future.
    HashMap<String, Integer> result = Maps.newHashMap();
    for (Pair<String, Integer> transformParam : transformParams) {
      result.put(transformParam.first, transformParam.second);
    }
    return result;
  }

  /**
   * Transform TIcebergFileFormat to THdfsFileFormat
   */
  public static THdfsFileFormat toTHdfsFileFormat(TIcebergFileFormat format) {
    switch (format) {
      case ORC:
        return THdfsFileFormat.ORC;
      case PARQUET:
      default:
        return THdfsFileFormat.PARQUET;
    }
  }

  /**
   * Transform TIcebergFileFormat to HdfsFileFormat
   */
  public static HdfsFileFormat toHdfsFileFormat(TIcebergFileFormat format) {
    return HdfsFileFormat.fromThrift(toTHdfsFileFormat(format));
  }

  /**
   * Transform TIcebergFileFormat to HdfsFileFormat
   */
  public static HdfsFileFormat toHdfsFileFormat(String format) {
    return HdfsFileFormat.fromThrift(toTHdfsFileFormat(getIcebergFileFormat(format)));
  }

  /**
   * Get iceberg data file by file system table location and iceberg predicates
   */
  public static List<DataFile> getIcebergDataFiles(FeIcebergTable table,
      List<UnboundPredicate> predicates) throws TableLoadingException {
    if (table.snapshotId() == -1) return Collections.emptyList();
    BaseTable baseTable =  (BaseTable)IcebergUtil.loadTable(table);
    TableScan scan = baseTable.newScan().useSnapshot(table.snapshotId());
    for (UnboundPredicate predicate : predicates) {
      scan = scan.filter(predicate);
    }

    List<DataFile> dataFileList = new ArrayList<>();
    for (FileScanTask task : scan.planFiles()) {
      dataFileList.add(task.file());
    }
    return dataFileList;
  }

  /**
   * Use DataFile path to generate 128-bit Murmur3 hash as map key, cached in memory
   */
  public static String getDataFilePathHash(DataFile dataFile) {
    Hasher hasher = Hashing.murmur3_128().newHasher();
    hasher.putUnencodedChars(dataFile.path().toString());
    return hasher.hash().toString();
  }

  /**
   * Converts Flat Buffer file format to Iceberg file format.
   */
  public static org.apache.iceberg.FileFormat fbFileFormatToIcebergFileFormat(
      byte fbFileFormat) throws ImpalaRuntimeException {
    switch (fbFileFormat){
      case org.apache.impala.fb.FbFileFormat.PARQUET:
          return org.apache.iceberg.FileFormat.PARQUET;
      default:
          throw new ImpalaRuntimeException(String.format("Unexpected file format: %s",
              org.apache.impala.fb.FbFileFormat.name(fbFileFormat)));
    }
  }

  /**
   * Iceberg's PartitionData class is hidden, so we implement it on our own.
   */
  public static class PartitionData implements StructLike {
    private final Object[] values;

    private PartitionData(int size) {
      this.values = new Object[size];
    }

    @Override
    public int size() {
      return values.length;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(int pos, Class<T> javaClass) {
      return javaClass.cast(values[pos]);
    }

    @Override
    public <T> void set(int pos, T value) {
      if (value instanceof ByteBuffer) {
        // ByteBuffer is not Serializable
        ByteBuffer buffer = (ByteBuffer) value;
        byte[] bytes = new byte[buffer.remaining()];
        buffer.duplicate().get(bytes);
        values[pos] = bytes;
      } else {
        values[pos] = value;
      }
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (other == null || getClass() != other.getClass()) {
        return false;
      }

      PartitionData that = (PartitionData) other;
      return Arrays.equals(values, that.values);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(values);
    }
  }

  /**
   * Create a PartitionData object from a partition path and its descriptors.
   */
  public static PartitionData partitionDataFromPath(Types.StructType partitionType,
      IcebergPartitionSpec spec, String path) throws ImpalaRuntimeException {
    if (path == null || path.isEmpty()) return null;

    PartitionData data = new PartitionData(spec.getIcebergPartitionFieldsSize());
    String[] partitions = path.split("/", -1);
    for (int i = 0; i < partitions.length; i += 1) {
      IcebergPartitionField field = spec.getIcebergPartitionFields().get(i);
      String[] parts = partitions[i].split("=", 2);
      Preconditions.checkArgument(parts.length == 2 && parts[0] != null &&
          field.getFieldName().equals(parts[0]), "Invalid partition: %s", partitions[i]);
      TIcebergPartitionTransformType transformType = field.getTransformType();
      data.set(i, getPartitionValue(
          partitionType.fields().get(i).type(), transformType, parts[1]));
    }
    return data;
  }

  /**
   * Read a value from a partition path with respect to its type and partition
   * transformation.
   */
  public static Object getPartitionValue(Type type,
      TIcebergPartitionTransformType transformType, String stringValue)
      throws ImpalaRuntimeException {
    String HIVE_NULL = MetaStoreUtil.DEFAULT_NULL_PARTITION_KEY_VALUE;
    if (stringValue == null || stringValue.equals(HIVE_NULL)) return null;

    if (transformType == TIcebergPartitionTransformType.IDENTITY ||
        transformType == TIcebergPartitionTransformType.TRUNCATE ||
        transformType == TIcebergPartitionTransformType.BUCKET ||
        transformType == TIcebergPartitionTransformType.DAY) {
      // These partition transforms are handled successfully by Iceberg's API.
      return Conversions.fromPartitionString(type, stringValue);
    }
    switch (transformType) {
      case YEAR: return parseYearToTransformYear(stringValue);
      case MONTH: return parseMonthToTransformMonth(stringValue);
      case HOUR: return parseHourToTransformHour(stringValue);
    }
    throw new ImpalaRuntimeException("Unexpected partition transform: " + transformType);
  }

  /**
   * In the partition path years are represented naturally, e.g. 1984. However, we need
   * to convert it to an integer which represents the years from 1970. So, for 1984 the
   * return value should be 14.
   */
  private static Integer parseYearToTransformYear(String yearStr) {
    Integer year = Integer.valueOf(yearStr);
    return year - ICEBERG_EPOCH_YEAR;
  }

  /**
   * In the partition path months are represented as <year>-<month>, e.g. 2021-01. We
   * need to convert it to a single integer which represents the months from '1970-01'.
   */
  private static Integer parseMonthToTransformMonth(String monthStr)
      throws ImpalaRuntimeException {
    String[] parts = monthStr.split("-", -1);
    Preconditions.checkState(parts.length == 2);
    Integer year = Integer.valueOf(parts[0]);
    Integer month = Integer.valueOf(parts[1]);
    int years = year - ICEBERG_EPOCH_YEAR;
    int months = month - ICEBERG_EPOCH_MONTH;
    return years * 12 + months;
  }

  /**
   * In the partition path hours are represented as <year>-<month>-<day>-<hour>, e.g.
   * 1970-01-01-01. We need to convert it to a single integer which represents the hours
   * from '1970-01-01 00:00:00'.
   */
  private static Integer parseHourToTransformHour(String hourStr) {
    final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
    String[] parts = hourStr.split("-", -1);
    Preconditions.checkState(parts.length == 4);
    Integer year = Integer.valueOf(parts[0]);
    Integer month = Integer.valueOf(parts[1]);
    Integer day = Integer.valueOf(parts[2]);
    Integer hour = Integer.valueOf(parts[3]);
    OffsetDateTime datetime = OffsetDateTime.of(
        LocalDateTime.of(year, month, day, hour, /*minute=*/0),
        ZoneOffset.UTC);
    return (int)ChronoUnit.HOURS.between(EPOCH, datetime);
  }

  public static TCompressionCodec parseParquetCompressionCodec(
      boolean onCreateTbl, Map<String, String> tblProperties, StringBuilder errMsg) {
    String codecTblProp = tblProperties.get(IcebergTable.PARQUET_COMPRESSION_CODEC);
    THdfsCompression codec = getIcebergParquetCompressionCodec(codecTblProp);
    if (codec == null) {
      errMsg.append("Invalid parquet compression codec for Iceberg table: ")
          .append(codecTblProp);
      return null;
    }

    TCompressionCodec compressionCodec = new TCompressionCodec();
    if (tblProperties.containsKey(IcebergTable.PARQUET_COMPRESSION_CODEC)) {
      compressionCodec.setCodec(codec);
    }

    if (onCreateTbl && codec != THdfsCompression.ZSTD) {
      if (tblProperties.containsKey(IcebergTable.PARQUET_COMPRESSION_LEVEL)) {
        errMsg.append("Parquet compression level cannot be set for codec ")
          .append(codec)
          .append(". Only ZSTD codec supports compression level table property.");
        return null;
      }
    } else if (tblProperties.containsKey(IcebergTable.PARQUET_COMPRESSION_LEVEL)) {
      String clevelTblProp = tblProperties.get(IcebergTable.PARQUET_COMPRESSION_LEVEL);
      Integer clevel = Ints.tryParse(clevelTblProp);
      if (clevel == null) {
        errMsg.append("Invalid parquet compression level for Iceberg table: ")
            .append(clevelTblProp);
        return null;
      } else if (clevel < IcebergTable.MIN_PARQUET_COMPRESSION_LEVEL ||
          clevel > IcebergTable.MAX_PARQUET_COMPRESSION_LEVEL) {
        errMsg.append("Parquet compression level for Iceberg table should fall in " +
            "the range of [")
            .append(String.valueOf(IcebergTable.MIN_PARQUET_COMPRESSION_LEVEL))
            .append("..")
            .append(String.valueOf(IcebergTable.MAX_PARQUET_COMPRESSION_LEVEL))
            .append("]");
        return null;
      }
      compressionCodec.setCompression_level(clevel);
    }
    return compressionCodec;
  }

  public static Long parseParquetRowGroupSize(Map<String, String> tblProperties,
      StringBuilder errMsg) {
    if (tblProperties.containsKey(IcebergTable.PARQUET_ROW_GROUP_SIZE)) {
      String propVal = tblProperties.get(IcebergTable.PARQUET_ROW_GROUP_SIZE);
      Long rowGroupSize = Longs.tryParse(propVal);
      if (rowGroupSize == null) {
        errMsg.append("Invalid parquet row group size for Iceberg table: ")
            .append(propVal);
        return null;
      } else if (rowGroupSize < IcebergTable.MIN_PARQUET_ROW_GROUP_SIZE ||
          rowGroupSize > IcebergTable.MAX_PARQUET_ROW_GROUP_SIZE) {
        errMsg.append("Parquet row group size for Iceberg table should ")
            .append("fall in the range of [")
            .append(String.valueOf(IcebergTable.MIN_PARQUET_ROW_GROUP_SIZE))
            .append("..")
            .append(String.valueOf(IcebergTable.MAX_PARQUET_ROW_GROUP_SIZE))
            .append("]");
        return null;
      }
      return rowGroupSize;
    }
    return IcebergTable.UNSET_PARQUET_ROW_GROUP_SIZE;
  }

  public static Long parseParquetPageSize(Map<String, String> tblProperties,
      String property, String descr, StringBuilder errMsg) {
    if (tblProperties.containsKey(property)) {
      String propVal = tblProperties.get(property);
      Long pageSize = Longs.tryParse(propVal);
      if (pageSize == null) {
        errMsg.append("Invalid parquet ")
            .append(descr)
            .append(" for Iceberg table: ")
            .append(propVal);
        return null;
      } else if (pageSize < IcebergTable.MIN_PARQUET_PAGE_SIZE ||
          pageSize > IcebergTable.MAX_PARQUET_PAGE_SIZE) {
        errMsg.append("Parquet ")
            .append(descr)
            .append(" for Iceberg table should fall in the range of [")
            .append(String.valueOf(IcebergTable.MIN_PARQUET_PAGE_SIZE))
            .append("..")
            .append(String.valueOf(IcebergTable.MAX_PARQUET_PAGE_SIZE))
            .append("]");
        return null;
      }
      return pageSize;
    }
    return IcebergTable.UNSET_PARQUET_PAGE_SIZE;
  }
}
