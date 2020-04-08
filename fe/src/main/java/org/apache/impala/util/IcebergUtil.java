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
import java.util.List;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.hadoop.HadoopTableOperations;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Type.TypeID;
import org.apache.impala.catalog.ArrayType;
import org.apache.impala.catalog.MapType;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.StructField;
import org.apache.impala.catalog.StructType;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.thrift.TCreateTableParams;
import org.apache.impala.thrift.TIcebergPartitionField;
import org.apache.impala.thrift.TIcebergPartitionTransform;

public class IcebergUtil {

  /**
   * Get HadoopTables by impala cluster related config
   */
  public static HadoopTables getHadoopTables() {
    return new HadoopTables(FileSystemUtil.getConfiguration());
  }

  /**
   * Get BaseTable by iceberg file system table location
   */
  public static BaseTable getBaseTable(String tableLocation) {
    HadoopTables tables = IcebergUtil.getHadoopTables();
    return (BaseTable) tables.load(tableLocation);
  }

  /**
   * Get TableMetadata by iceberg file system table location
   */
  public static TableMetadata getIcebergTableMetadata(String tableLocation) {
    HadoopTableOperations operations = (HadoopTableOperations)
        getBaseTable(tableLocation).operations();
    return operations.current();
  }

  /**
   * Build iceberg PartitionSpec by parameters.
   * partition columns are all from source columns, this is different from hdfs table.
   */
  public static PartitionSpec createIcebergPartition(Schema schema,
                                                     TCreateTableParams params)
      throws ImpalaRuntimeException {
    if (params.getPartition_spec() == null) {
      return null;
    }
    PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
    List<TIcebergPartitionField> partitionFields =
        params.getPartition_spec().getPartition_fields();
    for (TIcebergPartitionField partitionField : partitionFields) {
      if (partitionField.getField_type() == TIcebergPartitionTransform.IDENTITY) {
        builder.identity(partitionField.getField_name());
      } else if (partitionField.getField_type() == TIcebergPartitionTransform.HOUR) {
        builder.hour(partitionField.getField_name());
      } else if (partitionField.getField_type() == TIcebergPartitionTransform.DAY) {
        builder.day(partitionField.getField_name());
      } else if (partitionField.getField_type() == TIcebergPartitionTransform.MONTH) {
        builder.month(partitionField.getField_name());
      } else if (partitionField.getField_type() == TIcebergPartitionTransform.YEAR) {
        builder.year(partitionField.getField_name());
      } else {
        throw new ImpalaRuntimeException(String.format("Skip partition: %s, %s",
            partitionField.getField_name(), partitionField.getField_type()));
      }
    }
    return builder.build();
  }

  /**
   * Build TIcebergPartitionTransform by iceberg PartitionField
   */
  public static TIcebergPartitionTransform getPartitionTransform(PartitionField field)
      throws TableLoadingException {
    String type = field.transform().toString();
    return getPartitionTransform(type);
  }

  public static TIcebergPartitionTransform getPartitionTransform(String type)
      throws TableLoadingException {
    type = type.toUpperCase();
    if ("IDENTITY".equals(type)) {
      return TIcebergPartitionTransform.IDENTITY;
    } else if ("HOUR".equals(type)) {
      return TIcebergPartitionTransform.HOUR;
    } else if ("DAY".equals(type)) {
      return TIcebergPartitionTransform.DAY;
    } else if ("MONTH".equals(type)) {
      return TIcebergPartitionTransform.MONTH;
    } else if ("YEAR".equals(type)) {
      return TIcebergPartitionTransform.YEAR;
    } else if ("BUCKET".equals(type)) {
      return TIcebergPartitionTransform.BUCKET;
    } else if ("TRUNCATE".equals(type)) {
      return TIcebergPartitionTransform.TRUNCATE;
    } else {
      throw new TableLoadingException("Unsupported iceberg partition type: " +
          type);
    }
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
}
