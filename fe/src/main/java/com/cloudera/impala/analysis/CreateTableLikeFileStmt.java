// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.analysis;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;

import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.OriginalType;
import parquet.schema.PrimitiveType;

import com.cloudera.impala.authorization.Privilege;
import com.cloudera.impala.catalog.ArrayType;
import com.cloudera.impala.catalog.HdfsCompression;
import com.cloudera.impala.catalog.HdfsFileFormat;
import com.cloudera.impala.catalog.MapType;
import com.cloudera.impala.catalog.RowFormat;
import com.cloudera.impala.catalog.ScalarType;
import com.cloudera.impala.catalog.StructField;
import com.cloudera.impala.catalog.StructType;
import com.cloudera.impala.catalog.Type;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.FileSystemUtil;
import com.cloudera.impala.thrift.THdfsFileFormat;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;


/**
 * Represents a CREATE TABLE tablename LIKE fileformat '/path/to/file' statement
 * where the schema is inferred from the given file. Does not partition the table by
 * default.
 */
public class CreateTableLikeFileStmt extends CreateTableStmt {
  private final HdfsUri schemaLocation_;
  private final THdfsFileFormat schemaFileFormat_;
  private final static String ERROR_MSG =
      "Failed to convert Parquet type\n%s\nto an Impala %s type:\n%s\n";

  public CreateTableLikeFileStmt(TableName tableName, THdfsFileFormat schemaFileFormat,
      HdfsUri schemaLocation, List<ColumnDef> partitionColumnDescs,
      boolean isExternal, String comment, RowFormat rowFormat,
      THdfsFileFormat fileFormat, HdfsUri location, HdfsCachingOp cachingOp,
      boolean ifNotExists, Map<String, String> tblProperties,
      Map<String, String> serdeProperties) {
    super(tableName, new ArrayList<ColumnDef>(), partitionColumnDescs,
        isExternal, comment, rowFormat,
        fileFormat, location, cachingOp, ifNotExists, tblProperties, serdeProperties);
    schemaLocation_ = schemaLocation;
    schemaFileFormat_ = schemaFileFormat;
  }

  /**
   * Reads the first block from the given HDFS file and returns the Parquet schema.
   * Throws Analysis exception for any failure, such as failing to read the file
   * or failing to parse the contents.
   */
  private static parquet.schema.MessageType loadParquetSchema(Path pathToFile)
      throws AnalysisException {
    try {
      FileSystem fs = pathToFile.getFileSystem(FileSystemUtil.getConfiguration());
      if (!fs.isFile(pathToFile)) {
        throw new AnalysisException("Cannot infer schema, path is not a file: " +
                                    pathToFile);
      }
    } catch (IOException e) {
      throw new AnalysisException("Failed to connect to filesystem:" + e);
    } catch (IllegalArgumentException e) {
      throw new AnalysisException(e.getMessage());
    }
    ParquetMetadata readFooter = null;
    try {
      readFooter = ParquetFileReader.readFooter(FileSystemUtil.getConfiguration(),
          pathToFile);
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
  private static Type convertPrimitiveParquetType(parquet.schema.Type parquetType)
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
  private static MapType convertMap(parquet.schema.GroupType outerGroup)
      throws AnalysisException {
    if (outerGroup.getFieldCount() != 1){
      throw new AnalysisException(String.format(ERROR_MSG, outerGroup.toString(),
          "MAP", "The logical MAP type must have exactly 1 inner field."));
    }

    parquet.schema.Type innerField = outerGroup.getType(0);
    if (!innerField.isRepetition(parquet.schema.Type.Repetition.REPEATED)){
      throw new AnalysisException(String.format(ERROR_MSG, outerGroup.toString(),
          "MAP", "The logical MAP type must have a repeated inner field."));
    }
    if (innerField.isPrimitive()) {
      throw new AnalysisException(String.format(ERROR_MSG, outerGroup.toString(),
          "MAP", "The inner field of the logical MAP type must be a group."));
    }

    parquet.schema.GroupType innerGroup = innerField.asGroupType();
    // It does not matter whether innerGroup has an annotation or not (for example it may
    // be annotated with MAP_KEY_VALUE). We treat the case that innerGroup has an
    // annotation and the case the innerGroup does not have an annotation the same.
    if (innerGroup.getFieldCount() != 2) {
      throw new AnalysisException(String.format(ERROR_MSG, outerGroup.toString(),
          "MAP", "The inner field of the logical MAP type must have exactly 2 fields."));
    }

    parquet.schema.Type key = innerGroup.getType(0);
    if (!key.getName().equals("key")) {
      throw new AnalysisException(String.format(ERROR_MSG, outerGroup.toString(),
          "MAP", "The name of the first field of the inner field of the logical MAP " +
          "type must be 'key'"));
    }
    if (!key.isPrimitive()) {
      throw new AnalysisException(String.format(ERROR_MSG, outerGroup.toString(),
          "MAP", "The key type of the logical MAP type must be primitive."));
    }
    parquet.schema.Type value = innerGroup.getType(1);
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
  private static StructType convertStruct(parquet.schema.GroupType outerGroup)
      throws AnalysisException {
    ArrayList<StructField> structFields = new ArrayList<StructField>();
    for (parquet.schema.Type field: outerGroup.getFields()) {
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
  private static ArrayType convertArray(parquet.schema.GroupType outerGroup)
      throws AnalysisException {
    if (outerGroup.getFieldCount() != 1) {
      throw new AnalysisException(String.format(ERROR_MSG, outerGroup.toString(),
          "LIST", "The logical LIST type must have exactly 1 inner field."));
    }

    parquet.schema.Type innerField = outerGroup.getType(0);
    if (!innerField.isRepetition(parquet.schema.Type.Repetition.REPEATED)) {
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

    parquet.schema.GroupType innerGroup = innerField.asGroupType();
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
  private static Type convertLogicalParquetType(parquet.schema.Type parquetType)
      throws AnalysisException {
    OriginalType orig = parquetType.getOriginalType();
    if (orig == OriginalType.LIST) {
      return convertArray(parquetType.asGroupType());
    }
    if (orig == OriginalType.MAP || orig == OriginalType.MAP_KEY_VALUE) {
      // MAP_KEY_VALUE annotation should not be used any more. However, according to the
      // Parquet spec, some existing data incorrectly uses MAP_KEY_VALUE in place of MAP.
      // For backward-compatibility, a group annotated with MAP_KEY_VALUE that is not
      // contained by a MAP-annotated group should be handled as a MAP-annotated group.
      return convertMap(parquetType.asGroupType());
    }

    PrimitiveType prim = parquetType.asPrimitiveType();
    if (prim.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.BINARY &&
        orig == OriginalType.UTF8) {
      // UTF8 is the type annotation Parquet uses for strings
      // We check to make sure it applies to BINARY to avoid errors if there is a bad
      // annotation.
      return Type.STRING;
    }

    if (orig == OriginalType.DECIMAL) {
      return ScalarType.createDecimalType(prim.getDecimalMetadata().getPrecision(),
                                           prim.getDecimalMetadata().getScale());
    }

    throw new AnalysisException(
        "Unsupported logical parquet type " + orig + " (primitive type is " +
            prim.getPrimitiveTypeName().name() + ") for field " +
            parquetType.getName());
  }

  /**
   * Converts a Parquet type into an Impala type.
   */
  private static Type convertParquetType(parquet.schema.Type field)
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
    if (field.getOriginalType() != null) {
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
  private static List<ColumnDef> extractParquetSchema(HdfsUri location)
      throws AnalysisException {
    parquet.schema.MessageType parquetSchema = loadParquetSchema(location.getPath());
    List<parquet.schema.Type> fields = parquetSchema.getFields();
    List<ColumnDef> schema = new ArrayList<ColumnDef>();

    for (parquet.schema.Type field: fields) {
      Type type = convertParquetType(field);
      Preconditions.checkNotNull(type);
      String colName = field.getName();
      schema.add(new ColumnDef(colName, new TypeDef(type),
          "Inferred from Parquet file."));
    }
    return schema;
  }

  @Override
  public String toSql() {
    ArrayList<String> colsSql = Lists.newArrayList();
    ArrayList<String> partitionColsSql = Lists.newArrayList();
    HdfsCompression compression = HdfsCompression.fromFileName(
        schemaLocation_.toString());
    String s = ToSqlUtils.getCreateTableSql(getDb(),
        getTbl() + " __LIKE_FILEFORMAT__ ",  getComment(), colsSql, partitionColsSql,
        getTblProperties(), getSerdeProperties(), isExternal(), getIfNotExists(),
        getRowFormat(), HdfsFileFormat.fromThrift(getFileFormat()),
        compression, null, getLocation().toString());
    s = s.replace("__LIKE_FILEFORMAT__", "LIKE " + schemaFileFormat_ + " " +
        schemaLocation_.toString());
    return s;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    schemaLocation_.analyze(analyzer, Privilege.ALL, FsAction.READ_WRITE);
    switch (schemaFileFormat_) {
      case PARQUET:
        getColumnDefs().addAll(extractParquetSchema(schemaLocation_));
        break;
      default:
        throw new AnalysisException("Unsupported file type for schema inference: "
            + schemaFileFormat_);
    }
    super.analyze(analyzer);
  }
}
