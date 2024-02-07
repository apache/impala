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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.impala.thrift.THdfsFileFormat;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

/**
 * Supported HDFS file formats. Every file format specifies:
 * 1) the input format class
 * 2) the output format class
 * 3) the serialization library class
 * 4) whether scanning complex types from it is supported
 * 5) whether the file format can skip complex columns in scans and just materialize
 *    scalar typed columns
 * 6) Indicates if the given file format supports Date type.
 *
 * Important note: Always keep consistent with the classes used in Hive.
 * TODO: Kudu doesn't belong in this list. Either rename this enum or create a separate
 * list of storage engines (see IMPALA-4178).
 */
public enum HdfsFileFormat {
  RC_FILE("org.apache.hadoop.hive.ql.io.RCFileInputFormat",
      "org.apache.hadoop.hive.ql.io.RCFileOutputFormat",
      "org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe",
      false, true, false),
  TEXT("org.apache.hadoop.mapred.TextInputFormat",
      "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
      "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
      false, false, true),
  JSON("org.apache.hadoop.mapred.TextInputFormat",
      "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
      "org.apache.hadoop.hive.serde2.JsonSerDe", false, false, true),
  // LZO_TEXT is never used as an actual HdfsFileFormat. It is used only to store the
  // input format class and match against it (e.g. in HdfsCompression). Outside of this
  // file, tables that use the LZO input format class use HdfsFileFormat.TEXT.
  LZO_TEXT("com.hadoop.mapred.DeprecatedLzoTextInputFormat",
      "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
      "", false, false, true),
  SEQUENCE_FILE("org.apache.hadoop.mapred.SequenceFileInputFormat",
      "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat",
      "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe", false,
      true, false),
  AVRO("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat",
      "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat",
      "org.apache.hadoop.hive.serde2.avro.AvroSerDe",
      false, false, true),
  PARQUET("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
      "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
      "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
      true, true, true),
  ORC("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
      "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat",
      "org.apache.hadoop.hive.ql.io.orc.OrcSerde",
      true, true, true),
  KUDU("org.apache.hadoop.hive.kudu.KuduInputFormat",
      "org.apache.hadoop.hive.kudu.KuduOutputFormat",
      "org.apache.hadoop.hive.kudu.KuduSerDe", false, false, false),
  HUDI_PARQUET("org.apache.hudi.hadoop.HoodieParquetInputFormat",
      "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
      "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe", true, true, true),
  ICEBERG("org.apache.iceberg.mr.hive.HiveIcebergInputFormat",
      "org.apache.iceberg.mr.hive.HiveIcebergOutputFormat",
      "org.apache.iceberg.mr.hive.HiveIcebergSerDe", false, false, false),
  JDBC("org.apache.hadoop.hive.jdbc.JdbcInputFormat",
      "org.apache.hadoop.hive.jdbc.JdbcOutputFormat",
      "org.apache.hadoop.hive.jdbc.JdbcSerDe", false, false, true);


  private final String inputFormat_;
  private final String outputFormat_;
  private final String serializationLib_;

  // Indicates whether we support scanning complex types for this file format.
  private final boolean isComplexTypesSupported_;

  // Indicates whether the file format can skip complex columns in scans and just
  // materialize scalar typed columns. Ignored if isComplexTypesSupported_ is true.
  // TODO: Remove this once we support complex types for all file formats.
  private final boolean canSkipColumnTypes_;

  // Indicates whether we support scanning DATE type for this file format.
  private final boolean isDateTypeSupported_;

  HdfsFileFormat(String inputFormat, String outputFormat, String serializationLib,
      boolean isComplexTypesSupported, boolean canSkipColumnTypes,
      boolean isDateTypeSupported) {
    inputFormat_ = inputFormat;
    outputFormat_ = outputFormat;
    serializationLib_ = serializationLib;
    isComplexTypesSupported_ = isComplexTypesSupported;
    canSkipColumnTypes_ = canSkipColumnTypes;
    isDateTypeSupported_ = isDateTypeSupported;
  }

  public String inputFormat() { return inputFormat_; }
  public String outputFormat() { return outputFormat_; }
  public String serializationLib() { return serializationLib_; }

  // Impala supports legacy Parquet input formats and treats them internally as the most
  // modern Parquet input format.
  private static final String[] PARQUET_LEGACY_INPUT_FORMATS = {
      "com.cloudera.impala.hive.serde.ParquetInputFormat",
      "parquet.hive.DeprecatedParquetInputFormat",
      "parquet.hive.MapredParquetInputFormat"
  };

  private static final String JSON_SERDE = "org.apache.hadoop.hive.serde2.JsonSerDe";

  private static Map<String, HdfsFileFormat> VALID_INPUT_FORMATS =
      ImmutableMap.<String, HdfsFileFormat>builder()
          .put(RC_FILE.inputFormat(), RC_FILE)
          .put(TEXT.inputFormat(), TEXT)
          .put(LZO_TEXT.inputFormat(), TEXT)
          .put(SEQUENCE_FILE.inputFormat(), SEQUENCE_FILE)
          .put(AVRO.inputFormat(), AVRO)
          .put(PARQUET.inputFormat(), PARQUET)
          .put(PARQUET_LEGACY_INPUT_FORMATS[0], PARQUET)
          .put(PARQUET_LEGACY_INPUT_FORMATS[1], PARQUET)
          .put(PARQUET_LEGACY_INPUT_FORMATS[2], PARQUET)
          .put(KUDU.inputFormat(), KUDU)
          .put(ORC.inputFormat(), ORC)
          .put(HUDI_PARQUET.inputFormat(), HUDI_PARQUET)
          .put(ICEBERG.inputFormat(), ICEBERG)
          .build();

  /**
   * Returns true if the string describes an input format class that we support.
   */
  public static boolean isHdfsInputFormatClass(String inputFormatClass) {
    return VALID_INPUT_FORMATS.containsKey(inputFormatClass);
  }

  /**
   * Returns the file format associated with the input format class, or null if
   * the input format class is not supported.
   */

  public static HdfsFileFormat fromHdfsInputFormatClass(
      String inputFormatClass, String serDe) {
    Preconditions.checkNotNull(inputFormatClass);
    if (serDe != null && inputFormatClass.equals(TEXT.inputFormat())) {
      if (JSON_SERDE.equals(serDe)) {
        return HdfsFileFormat.JSON;
      }
    }
    return VALID_INPUT_FORMATS.get(inputFormatClass);
  }

  /**
   * Returns the corresponding enum for a SerDe class name. If classname is not one
   * of our supported formats, throws an IllegalArgumentException like Enum.valueOf
   */

  public static HdfsFileFormat fromJavaClassName(String className, String serDe) {
    Preconditions.checkNotNull(className);
    if (serDe != null && className.equals(TEXT.inputFormat())) {
      if (JSON_SERDE.equals(serDe)) {
        return HdfsFileFormat.JSON;
      }
    }
    if (isHdfsInputFormatClass(className)) return VALID_INPUT_FORMATS.get(className);
    throw new IllegalArgumentException(className);
  }

  public static HdfsFileFormat fromThrift(THdfsFileFormat thriftFormat) {
    switch (thriftFormat) {
      case RC_FILE: return HdfsFileFormat.RC_FILE;
      case TEXT: return HdfsFileFormat.TEXT;
      case SEQUENCE_FILE: return HdfsFileFormat.SEQUENCE_FILE;
      case AVRO: return HdfsFileFormat.AVRO;
      case ORC: return HdfsFileFormat.ORC;
      case HUDI_PARQUET: return HdfsFileFormat.HUDI_PARQUET;
      case PARQUET: return HdfsFileFormat.PARQUET;
      case KUDU: return HdfsFileFormat.KUDU;
      case ICEBERG: return HdfsFileFormat.ICEBERG;
      case JSON: return HdfsFileFormat.JSON;
      case JDBC: return HdfsFileFormat.JDBC;
      default:
        throw new RuntimeException("Unknown THdfsFileFormat: "
            + thriftFormat + " - should never happen!");
    }
  }

  public THdfsFileFormat toThrift() {
    switch (this) {
      case RC_FILE: return THdfsFileFormat.RC_FILE;
      case TEXT: return THdfsFileFormat.TEXT;
      case SEQUENCE_FILE: return THdfsFileFormat.SEQUENCE_FILE;
      case AVRO: return THdfsFileFormat.AVRO;
      case ORC: return THdfsFileFormat.ORC;
      case HUDI_PARQUET:
      case PARQUET: return THdfsFileFormat.PARQUET;
      case KUDU: return THdfsFileFormat.KUDU;
      case ICEBERG: return THdfsFileFormat.ICEBERG;
      case JSON: return THdfsFileFormat.JSON;
      case JDBC: return THdfsFileFormat.JDBC;
      default:
        throw new RuntimeException("Unknown HdfsFormat: "
            + this + " - should never happen!");
    }
  }

  public String toSql(HdfsCompression compressionType) {
    switch (this) {
      case RC_FILE: return "RCFILE";
      case ORC: return "ORC";
      case TEXT:
        if (compressionType == HdfsCompression.LZO ||
            compressionType == HdfsCompression.LZO_INDEX) {
          // It is not possible to create a table with LZO compressed text files
          // in Impala, but this is valid in Hive.
          return String.format("INPUTFORMAT '%s' OUTPUTFORMAT '%s'",
              LZO_TEXT.inputFormat(), LZO_TEXT.outputFormat());
        }
        return "TEXTFILE";
      case SEQUENCE_FILE: return "SEQUENCEFILE";
      case AVRO: return "AVRO";
      case PARQUET: return "PARQUET";
      case KUDU: return "KUDU";
      case HUDI_PARQUET: return "HUDIPARQUET";
      case ICEBERG: return "ICEBERG";
      case JSON: return "JSONFILE";
      case JDBC: return "JDBC";
      default:
        throw new RuntimeException("Unknown HdfsFormat: "
            + this + " - should never happen!");
    }
  }

  /**
   * Returns true if this file format with the given compression format is splittable.
   */
  public boolean isSplittable(HdfsCompression compression) {
    switch (this) {
      case TEXT:
      case JSON:
        return compression == HdfsCompression.NONE;
      case RC_FILE:
      case SEQUENCE_FILE:
      case AVRO:
      case PARQUET:
      case HUDI_PARQUET:
      case ORC:
      case ICEBERG:
        return true;
      case KUDU:
        return false;
      case JDBC:
        return false;
      default:
        throw new RuntimeException("Unknown HdfsFormat: "
            + this + " - should never happen!");
    }
  }

  /**
   * Returns true if Impala supports scanning complex-typed columns
   * from a table/partition with this file format.
   */
  public boolean isComplexTypesSupported() { return isComplexTypesSupported_; }

  /**
   * Returns true if this file format can skip complex typed columns and materialize
   * only scalar typed columns.
   */
  public boolean canSkipComplexTypes() { return canSkipColumnTypes_; }

  /**
   * Returns true if Impala supports scanning DATE typed columns from a table/partition of
   * this file format
   */
  public boolean isDateTypeSupported() { return isDateTypeSupported_; }

  /**
   * Returns a list with all formats for which isComplexTypesSupported() is true.
   */
  public static List<HdfsFileFormat> complexTypesFormats() {
    List<HdfsFileFormat> result = new ArrayList<>();
    for (HdfsFileFormat f: values()) {
      if (f.isComplexTypesSupported()) result.add(f);
    }
    return result;
  }

  /**
   * Returns true if the format is Parquet, false otherwise.
   */
  public boolean isParquetBased() {
    return this == HdfsFileFormat.PARQUET || this == HdfsFileFormat.HUDI_PARQUET;
  }
}
