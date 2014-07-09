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

import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.OriginalType;
import parquet.schema.PrimitiveType;

import com.cloudera.impala.authorization.Privilege;
import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.catalog.HdfsCompression;
import com.cloudera.impala.catalog.HdfsFileFormat;
import com.cloudera.impala.catalog.RowFormat;
import com.cloudera.impala.catalog.ScalarType;
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

  public CreateTableLikeFileStmt(TableName tableName, THdfsFileFormat schemaFileFormat,
      HdfsUri schemaLocation, List<ColumnDesc> partitionColumnDescs,
      boolean isExternal, String comment, RowFormat rowFormat,
      THdfsFileFormat fileFormat, HdfsUri location, HdfsCachingOp cachingOp,
      boolean ifNotExists, Map<String, String> tblProperties,
      Map<String, String> serdeProperties) {
    super(tableName, new ArrayList<ColumnDesc>(), partitionColumnDescs,
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
      throw new AnalysisException("Failed to connect to HDFS:" + e);
    }
    ParquetMetadata readFooter = null;
    try {
      readFooter = ParquetFileReader.readFooter(FileSystemUtil.getConfiguration(),
          pathToFile);
    } catch (FileNotFoundException e) {
      throw new AnalysisException("File not found: " + e);
    } catch (IOException e) {
      throw new AnalysisException("Failed to open HDFS file as a parquet file: " + e);
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
   * Converts a "primitive" parquet type to an Impala column type.
   * A primitive type is a non-nested type which does not have annotations.
   */
  private static Type convertPrimitiveParquetType(parquet.schema.Type parquetType)
      throws AnalysisException {
    Preconditions.checkState(parquetType.isPrimitive());
    PrimitiveType prim = parquetType.asPrimitiveType();
    switch (prim.getPrimitiveTypeName()) {
    case BINARY:
      return Type.STRING;
    case BOOLEAN:
      return Type.BOOLEAN;
    case DOUBLE:
      return Type.DOUBLE;
    case FIXED_LEN_BYTE_ARRAY:
      throw new AnalysisException(
          "Unsupported parquet type FIXED_LEN_BYTE_ARRAY for field " +
              parquetType.getName());
    case FLOAT:
      return Type.FLOAT;
    case INT32:
      return Type.INT;
    case INT64:
      return Type.BIGINT;
    case INT96:
      return Type.TIMESTAMP;
    default:
      Preconditions.checkState(false, "Unexpected parquet primitive type: " +
             prim.getPrimitiveTypeName());
      return null;
    }
  }

  /**
   * Converts a "logical" parquet type to an Impala column type.
   * A logical type is a primitive type with an annotation. The annotations are stored as
   * a "OriginalType". The parquet documentation refers to these as logical types,
   * so we use that terminology here.
   */
  private static Type convertLogicalParquetType(parquet.schema.Type parquetType)
      throws AnalysisException {
    PrimitiveType prim = parquetType.asPrimitiveType();
    OriginalType orig = parquetType.getOriginalType();
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
   * Parses a parquet file stored in HDFS and returns the corresponding Impala schema.
   * This fails with an analysis exception if any errors occur reading the file,
   * parsing the parquet schema, or if the parquet types cannot be represented in Impala.
   */
  private static List<ColumnDesc> extractParquetSchema(HdfsUri location)
      throws AnalysisException {
    parquet.schema.MessageType parquetSchema = loadParquetSchema(location.getPath());
    List<parquet.schema.Type> fields = parquetSchema.getFields();
    List<ColumnDesc> schema = new ArrayList<ColumnDesc>();

    for (parquet.schema.Type field: fields) {
      Type type = null;

      if (field.getOriginalType() != null) {
        type = convertLogicalParquetType(field);
      } else if (field.isPrimitive()) {
        type = convertPrimitiveParquetType(field);
      } else {
        throw new AnalysisException("Unsupported parquet type for field " +
            field.getName());
      }

      String colName = field.getName();
      schema.add(new ColumnDesc(colName, type, "inferred from: " + field.toString()));
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
  public void analyze(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
    schemaLocation_.analyze(analyzer, Privilege.ALL);
    switch (schemaFileFormat_) {
      case PARQUET:
        getColumnDescs().addAll(extractParquetSchema(schemaLocation_));
        break;
      default:
        throw new AnalysisException("Unsupported file type for schema inference: "
            + schemaFileFormat_);
    }
    super.analyze(analyzer);
  }

}
