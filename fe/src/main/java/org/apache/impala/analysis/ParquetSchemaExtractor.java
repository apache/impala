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

package org.apache.impala.analysis;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.*;
import org.apache.impala.catalog.ArrayType;
import org.apache.impala.catalog.MapType;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.StructField;
import org.apache.impala.catalog.StructType;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.util.FileAnalysisUtil;

/**
 * Provides a helper function (extract()) which extracts the Impala schema from a given
 * Parquet file.
 */
class ParquetSchemaExtractor {
  private final static String ERROR_MSG =
      "Failed to convert Parquet type\n%s\nto an Impala %s type:\n%s\n";

  /**
   * Reads the first block from the given HDFS file and returns the Parquet schema.
   * Throws Analysis exception for any failure, such as failing to read the file
   * or failing to parse the contents.
   */
  private static org.apache.parquet.schema.MessageType loadParquetSchema(Path pathToFile)
      throws AnalysisException {
    FileAnalysisUtil.CheckIfFile(pathToFile);
    ParquetMetadata readFooter = null;
    try {
      readFooter = ParquetFileReader.readFooter(FileSystemUtil.getConfiguration(),
          pathToFile, ParquetMetadataConverter.NO_FILTER);
    } catch (FileNotFoundException e) {
      throw new AnalysisException("File not found: " + e);
    } catch (IOException e) {
      throw new AnalysisException("Failed to open file as a parquet file: " + e);
    } catch (RuntimeException e) {
      // Parquet throws a generic RuntimeException when reading a non-parquet file
      if (e.toString().contains("is not a Parquet file")) {
        throw new AnalysisException("File is not a parquet file: " + pathToFile);
      }
      // otherwise, who knows what we caught, throw it back up
      throw e;
    }
     return readFooter.getFileMetaData().getSchema();
  }

  /**
   * Converts a "primitive" Parquet type to an Impala type.
   * A primitive type is a non-nested type with no annotations.
   */
  private static Type convertPrimitiveParquetType(
      org.apache.parquet.schema.Type parquetType)
      throws AnalysisException {
    Preconditions.checkState(parquetType.isPrimitive());
    PrimitiveType prim = parquetType.asPrimitiveType();
    switch (prim.getPrimitiveTypeName()) {
      case BINARY: return Type.STRING;
      case BOOLEAN: return Type.BOOLEAN;
      case DOUBLE: return Type.DOUBLE;
      case FIXED_LEN_BYTE_ARRAY:
        throw new AnalysisException(
            "Unsupported parquet type FIXED_LEN_BYTE_ARRAY for field " +
                parquetType.getName());
      case FLOAT: return Type.FLOAT;
      case INT32: return Type.INT;
      case INT64: return Type.BIGINT;
      case INT96: return Type.TIMESTAMP;
      default:
        Preconditions.checkState(false, "Unexpected parquet primitive type: " +
               prim.getPrimitiveTypeName());
        return null;
    }
  }

  /**
   * Converts a Parquet group type to an Impala map Type. We support both standard
   * Parquet map representations, as well as legacy. Legacy representations are handled
   * according to this specification:
   * https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#backward-compatibility-rules-1
   *
   * Standard representation of a map in Parquet:
   * <optional | required> group <name> (MAP) { <-- outerGroup is pointing at this
   * repeated group key_value {
   *     required <key-type> key;
   *     <optional | required> <value-type> value;
   *   }
   * }
   */
  private static MapType convertMap(org.apache.parquet.schema.GroupType outerGroup)
      throws AnalysisException {
    if (outerGroup.getFieldCount() != 1){
      throw new AnalysisException(String.format(ERROR_MSG, outerGroup.toString(),
          "MAP", "The logical MAP type must have exactly 1 inner field."));
    }

    org.apache.parquet.schema.Type innerField = outerGroup.getType(0);
    if (!innerField.isRepetition(org.apache.parquet.schema.Type.Repetition.REPEATED)){
      throw new AnalysisException(String.format(ERROR_MSG, outerGroup.toString(),
          "MAP", "The logical MAP type must have a repeated inner field."));
    }
    if (innerField.isPrimitive()) {
      throw new AnalysisException(String.format(ERROR_MSG, outerGroup.toString(),
          "MAP", "The inner field of the logical MAP type must be a group."));
    }

    org.apache.parquet.schema.GroupType innerGroup = innerField.asGroupType();
    // It does not matter whether innerGroup has an annotation or not (for example it may
    // be annotated with MAP_KEY_VALUE). We treat the case that innerGroup has an
    // annotation and the case the innerGroup does not have an annotation the same.
    if (innerGroup.getFieldCount() != 2) {
      throw new AnalysisException(String.format(ERROR_MSG, outerGroup.toString(),
          "MAP", "The inner field of the logical MAP type must have exactly 2 fields."));
    }

    org.apache.parquet.schema.Type key = innerGroup.getType(0);
    if (!key.getName().equals("key")) {
      throw new AnalysisException(String.format(ERROR_MSG, outerGroup.toString(),
          "MAP", "The name of the first field of the inner field of the logical MAP " +
          "type must be 'key'"));
    }
    if (!key.isPrimitive()) {
      throw new AnalysisException(String.format(ERROR_MSG, outerGroup.toString(),
          "MAP", "The key type of the logical MAP type must be primitive."));
    }
    org.apache.parquet.schema.Type value = innerGroup.getType(1);
    if (!value.getName().equals("value")) {
      throw new AnalysisException(String.format(ERROR_MSG, outerGroup.toString(),
          "MAP", "The name of the second field of the inner field of the logical MAP " +
          "type must be 'value'"));
    }

    return new MapType(convertParquetType(key), convertParquetType(value));
  }

  /**
   * Converts a Parquet group type to an Impala struct Type.
   */
  private static StructType convertStruct(org.apache.parquet.schema.GroupType outerGroup)
      throws AnalysisException {
    List<StructField> structFields = new ArrayList<>();
    for (org.apache.parquet.schema.Type field: outerGroup.getFields()) {
      StructField f = new StructField(field.getName(), convertParquetType(field));
      structFields.add(f);
    }
    return new StructType(structFields);
  }

  /**
   * Converts a Parquet group type to an Impala array Type. We can handle the standard
   * representation, but also legacy representations for backwards compatibility.
   * Legacy representations are handled according to this specification:
   * https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#backward-compatibility-rules
   *
   * Standard representation of an array in Parquet:
   * <optional | required> group <name> (LIST) { <-- outerGroup is pointing at this
   *   repeated group list {
   *     <optional | required> <element-type> element;
   *   }
   * }
   */
  private static ArrayType convertArray(org.apache.parquet.schema.GroupType outerGroup)
      throws AnalysisException {
    if (outerGroup.getFieldCount() != 1) {
      throw new AnalysisException(String.format(ERROR_MSG, outerGroup.toString(),
          "LIST", "The logical LIST type must have exactly 1 inner field."));
    }

    org.apache.parquet.schema.Type innerField = outerGroup.getType(0);
    if (!innerField.isRepetition(org.apache.parquet.schema.Type.Repetition.REPEATED)) {
      throw new AnalysisException(String.format(ERROR_MSG, outerGroup.toString(),
          "LIST", "The inner field of the logical LIST type must be repeated."));
    }
    if (innerField.isPrimitive() || innerField.getOriginalType() != null) {
      // From the Parquet Spec:
      // 1. If the repeated field is not a group then it's type is the element type.
      //
      // If innerField is a group, but originalType is not null, the element type is
      // based on the logical type.
      return new ArrayType(convertParquetType(innerField));
    }

    org.apache.parquet.schema.GroupType innerGroup = innerField.asGroupType();
    if (innerGroup.getFieldCount() != 1) {
      // From the Parquet Spec:
      // 2. If the repeated field is a group with multiple fields, then it's type is a
      //    struct.
      return new ArrayType(convertStruct(innerGroup));
    }

    return new ArrayType(convertParquetType(innerGroup.getType(0)));
  }

  /**
   * Converts a "logical" Parquet type to an Impala column type.
   * A Parquet type is considered logical when it has an annotation. The annotation is
   * stored as a "OriginalType". The Parquet documentation refers to these as logical
   * types, so we use that terminology here.
   */
  private static Type convertLogicalParquetType(
      org.apache.parquet.schema.Type parquetType) throws AnalysisException {
    // The Parquet API is responsible for deducing logical type if only converted type
    // is set.
    LogicalTypeAnnotation logicalType = parquetType.getLogicalTypeAnnotation();

    if (logicalType instanceof ListLogicalTypeAnnotation) {
      return convertArray(parquetType.asGroupType());
    }
    if (logicalType instanceof MapLogicalTypeAnnotation
        || logicalType instanceof MapKeyValueTypeAnnotation) {
      // MAP_KEY_VALUE annotation should not be used any more. However, according to the
      // Parquet spec, some existing data incorrectly uses MAP_KEY_VALUE in place of MAP.
      // For backward-compatibility, a group annotated with MAP_KEY_VALUE that is not
      // contained by a MAP-annotated group should be handled as a MAP-annotated group.
      return convertMap(parquetType.asGroupType());
    }

    PrimitiveType prim = parquetType.asPrimitiveType();
    if (prim.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.BINARY &&
        (logicalType instanceof StringLogicalTypeAnnotation
            || logicalType instanceof EnumLogicalTypeAnnotation)) {
      // UTF8 is the type annotation Parquet uses for strings
      // ENUM is the type annotation Parquet uses to indicate that
      // the original data type, before conversion to parquet, had been enum.
      // Applications which do not have enumerated types (e.g. Impala)
      // should interpret it as a string.
      // We check to make sure it applies to BINARY to avoid errors if there is a bad
      // annotation.
      return Type.STRING;
    }

    if (prim.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT64 &&
        logicalType instanceof TimestampLogicalTypeAnnotation) {
      return Type.TIMESTAMP;
    }

    if (prim.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT32 &&
        logicalType instanceof DateLogicalTypeAnnotation) {
      return Type.DATE;
    }

    if ((prim.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT32
        || prim.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT64)
        && logicalType instanceof IntLogicalTypeAnnotation
        && ((IntLogicalTypeAnnotation) logicalType).isSigned() == true) {
      // Map signed integer types to an supported Impala column type
      switch (((IntLogicalTypeAnnotation) logicalType).getBitWidth()) {
        case 8: return Type.TINYINT;
        case 16: return Type.SMALLINT;
        case 32: return Type.INT;
        case 64: return Type.BIGINT;
      }
    }

    if (logicalType instanceof DecimalLogicalTypeAnnotation) {
      DecimalLogicalTypeAnnotation decimal = (DecimalLogicalTypeAnnotation) logicalType;
      return ScalarType.createDecimalType(decimal.getPrecision(), decimal.getScale());
    }

    throw new AnalysisException(
        "Unsupported logical parquet type " + logicalType + " (primitive type is " +
            prim.getPrimitiveTypeName().name() + ") for field " +
            parquetType.getName());
  }

  /**
   * Converts a Parquet type into an Impala type.
   */
  private static Type convertParquetType(org.apache.parquet.schema.Type field)
      throws AnalysisException {
    Type type = null;
    // TODO for 2.3: If a field is not annotated with LIST, it can still be sometimes
    // interpreted as an array. The following 2 examples should be interpreted as an array
    // of integers, but this is currently not done.
    // 1. repeated int int_col;
    // 2. required group int_arr {
    //      repeated group list {
    //        required int element;
    //      }
    //    }
    if (field.getLogicalTypeAnnotation() != null) {
      type = convertLogicalParquetType(field);
    } else if (field.isPrimitive()) {
      type = convertPrimitiveParquetType(field);
    } else {
      // If field is not primitive, it must be a struct.
      type = convertStruct(field.asGroupType());
    }
    return type;
  }

  /**
   * Parses a Parquet file stored in HDFS and returns the corresponding Impala schema.
   * This fails with an analysis exception if any errors occur reading the file,
   * parsing the Parquet schema, or if the Parquet types cannot be represented in Impala.
   */
  static List<ColumnDef> extract(HdfsUri location) throws AnalysisException {
    org.apache.parquet.schema.MessageType parquetSchema =
        loadParquetSchema(location.getPath());
    List<org.apache.parquet.schema.Type> fields = parquetSchema.getFields();
    List<ColumnDef> schema = new ArrayList<>();

    for (org.apache.parquet.schema.Type field: fields) {
      Type type = convertParquetType(field);
      Preconditions.checkNotNull(type);
      String colName = field.getName();
      Map<ColumnDef.Option, Object> option = new HashMap<>();
      option.put(ColumnDef.Option.COMMENT, "Inferred from Parquet file.");
      schema.add(new ColumnDef(colName, new TypeDef(type), option));
    }
    return schema;
  }
}
