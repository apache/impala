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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.google.common.collect.Maps;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import org.apache.impala.common.Pair;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.transforms.PartitionSpecVisitor;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.types.Types;
import org.apache.impala.analysis.IcebergPartitionTransform;
import org.apache.impala.catalog.ArrayType;
import org.apache.impala.catalog.Catalog;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.HdfsFileFormat;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.MapType;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.StructField;
import org.apache.impala.catalog.StructType;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.thrift.TCreateTableParams;
import org.apache.impala.thrift.THdfsFileFormat;
import org.apache.impala.thrift.TIcebergCatalog;
import org.apache.impala.thrift.TIcebergFileFormat;
import org.apache.impala.thrift.TIcebergPartitionField;
import org.apache.impala.thrift.TIcebergPartitionTransform;
import org.apache.impala.thrift.TIcebergPartitionTransformType;

public class IcebergUtil {

  /**
   * Get HadoopTables by impala cluster related config
   */
  public static HadoopTables getHadoopTables() {
    return new HadoopTables(FileSystemUtil.getConfiguration());
  }

  /**
   * Get HadoopCatalog by impala cluster related config
   */
  public static HadoopCatalog getHadoopCatalog(String location) {
    return new HadoopCatalog(FileSystemUtil.getConfiguration(), location);
  }

  /**
   * Get BaseTable from FeIcebergTable
   */
  public static BaseTable getBaseTable(FeIcebergTable table) {
    return getBaseTable(table.getIcebergCatalog(), getIcebergTableIdentifier(table),
        table.getIcebergCatalogLocation());
  }

  /**
   * Get BaseTable from each parameters
   */
  public static BaseTable getBaseTable(TIcebergCatalog catalog, String tableName,
      String location) {
    if (catalog == TIcebergCatalog.HADOOP_CATALOG) {
      return getBaseTableByHadoopCatalog(tableName, location);
    } else {
      // We use HadoopTables as default Iceberg catalog type
      HadoopTables hadoopTables = IcebergUtil.getHadoopTables();
      return (BaseTable) hadoopTables.load(location);
    }
  }

  /**
   * Use location, namespace(database) and name(table) to get BaseTable by HadoopCatalog
   */
  private static BaseTable getBaseTableByHadoopCatalog(String tableName,
      String catalogLoc) {
    HadoopCatalog hadoopCatalog = IcebergUtil.getHadoopCatalog(catalogLoc);
    return (BaseTable) hadoopCatalog.loadTable(TableIdentifier.parse(tableName));
  }

  /**
   * Get TableMetadata by FeIcebergTable
   */
  public static TableMetadata getIcebergTableMetadata(FeIcebergTable table) {
    return getIcebergTableMetadata(table.getIcebergCatalog(),
        getIcebergTableIdentifier(table), table.getIcebergCatalogLocation());
  }

  /**
   * Get TableMetadata by related info
   * tableName is table full name, usually database.table
   */
  public static TableMetadata getIcebergTableMetadata(TIcebergCatalog catalog,
      String tableName, String location) {
    BaseTable baseTable = getBaseTable(catalog, tableName, location);
    return baseTable.operations().current();
  }

  /**
   * Get Iceberg table identifier by table property
   */
  public static String getIcebergTableIdentifier(FeIcebergTable table) {
    return getIcebergTableIdentifier(table.getMetaStoreTable());
  }

  public static String getIcebergTableIdentifier(
      org.apache.hadoop.hive.metastore.api.Table msTable) {
    String name = msTable.getParameters().get(IcebergTable.ICEBERG_TABLE_IDENTIFIER);
    if (name == null || name.isEmpty()) {
      return msTable.getDbName() + "." + msTable.getTableName();
    }

    // If database not been specified in property, use default
    if (!name.contains(".")) {
      return Catalog.DEFAULT_DB + "." + name;
    }
    return name;
  }

  /**
   * Build iceberg PartitionSpec by parameters.
   * partition columns are all from source columns, this is different from hdfs table.
   */
  public static PartitionSpec createIcebergPartition(Schema schema,
      TCreateTableParams params) throws ImpalaRuntimeException {
    if (params.getPartition_spec() == null) {
      return PartitionSpec.unpartitioned();
    }
    PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
    List<TIcebergPartitionField> partitionFields =
        params.getPartition_spec().getPartition_fields();
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
   * Get iceberg table catalog type from hms table properties
   * use HadoopCatalog as default
   */
  public static TIcebergCatalog getIcebergCatalog(
      org.apache.hadoop.hive.metastore.api.Table msTable) {
    TIcebergCatalog catalog = getIcebergCatalog(
        msTable.getParameters().get(IcebergTable.ICEBERG_CATALOG));
    return catalog == null ? TIcebergCatalog.HADOOP_CATALOG : catalog;
  }

  /**
   * Get TIcebergCatalog from a string, usually from table properties
   */
  public static TIcebergCatalog getIcebergCatalog(String catalog){
    if ("hadoop.tables".equalsIgnoreCase(catalog)) {
      return TIcebergCatalog.HADOOP_TABLES;
    } else if ("hadoop.catalog".equalsIgnoreCase(catalog)) {
      return TIcebergCatalog.HADOOP_CATALOG;
    }
    return null;
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
   * Get TIcebergFileFormat from a string, usually from table properties
   */
  public static TIcebergFileFormat getIcebergFileFormat(String format){
    if ("PARQUET".equalsIgnoreCase(format)) return TIcebergFileFormat.PARQUET;
    return null;
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
    transformType = transformType.toUpperCase();
    if ("IDENTITY".equals(transformType)) {
      return TIcebergPartitionTransformType.IDENTITY;
    } else if ("HOUR".equals(transformType)) {
      return TIcebergPartitionTransformType.HOUR;
    } else if ("DAY".equals(transformType)) {
      return TIcebergPartitionTransformType.DAY;
    } else if ("MONTH".equals(transformType)) {
      return TIcebergPartitionTransformType.MONTH;
    } else if ("YEAR".equals(transformType)) {
      return TIcebergPartitionTransformType.YEAR;
    } else if (transformType != null && transformType.startsWith("BUCKET")) {
      return TIcebergPartitionTransformType.BUCKET;
    } else if (transformType != null && transformType.startsWith("TRUNCATE")) {
      return TIcebergPartitionTransformType.TRUNCATE;
    } else {
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
   * Transform iceberg type to impala type
   */
  public static Type toImpalaType(org.apache.iceberg.types.Type t)
      throws TableLoadingException {
    switch (t.typeId()) {
      case BOOLEAN:
        return Type.BOOLEAN;
      case INTEGER:
        return Type.INT;
      case LONG:
        return Type.BIGINT;
      case FLOAT:
        return Type.FLOAT;
      case DOUBLE:
        return Type.DOUBLE;
      case STRING:
        return Type.STRING;
      case DATE:
        return Type.DATE;
      case BINARY:
        return Type.BINARY;
      case TIMESTAMP:
        return Type.TIMESTAMP;
      case DECIMAL:
        Types.DecimalType decimal = (Types.DecimalType) t;
        return ScalarType.createDecimalType(decimal.precision(), decimal.scale());
      case LIST: {
        Types.ListType listType = (Types.ListType) t;
        return new ArrayType(toImpalaType(listType.elementType()));
      }
      case MAP: {
        Types.MapType mapType = (Types.MapType) t;
        return new MapType(toImpalaType(mapType.keyType()),
            toImpalaType(mapType.valueType()));
      }
      case STRUCT: {
        Types.StructType structType = (Types.StructType) t;
        List<StructField> structFields = new ArrayList<>();
        List<Types.NestedField> nestedFields = structType.fields();
        for (Types.NestedField nestedField : nestedFields) {
          structFields.add(new StructField(nestedField.name(),
              toImpalaType(nestedField.type())));
        }
        return new StructType(structFields);
      }
      default:
        throw new TableLoadingException(String.format(
            "Iceberg type '%s' is not supported in Impala", t.typeId()));
    }
  }

  /**
   * Get iceberg data file by file system table location and iceberg predicates
   */
  public static List<DataFile> getIcebergDataFiles(FeIcebergTable table,
      List<UnboundPredicate> predicates) {
    BaseTable baseTable = IcebergUtil.getBaseTable(table);
    TableScan scan = baseTable.newScan();
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
}
