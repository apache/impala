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

package com.cloudera.impala.catalog;

import java.util.List;
import java.util.Map;

import com.cloudera.impala.thrift.THdfsFileFormat;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

/**
 * Supported HDFS file formats. Every file format specifies:
 * 1) the input format class
 * 2) the output format class
 * 3) the serialization library class
 * 4) whether scanning complex types from it is supported
 *
 * Important note: Always keep consistent with the classes used in Hive.
 */
public enum HdfsFileFormat {
  RC_FILE("org.apache.hadoop.hive.ql.io.RCFileInputFormat",
      "org.apache.hadoop.hive.ql.io.RCFileOutputFormat",
      "org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe",
      false),
  TEXT("org.apache.hadoop.mapred.TextInputFormat",
      "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
      "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
      false),
  LZO_TEXT("com.hadoop.mapred.DeprecatedLzoTextInputFormat",
      "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
      "",
      false),
  SEQUENCE_FILE("org.apache.hadoop.mapred.SequenceFileInputFormat",
      "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat",
      "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe", false),
  AVRO("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat",
      "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat",
      "org.apache.hadoop.hive.serde2.avro.AvroSerDe",
      false),
  PARQUET("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
      "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
      "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
      true);

  private final String inputFormat_;
  private final String outputFormat_;
  private final String serializationLib_;

  // Indicates whether we support scanning complex types for this file format.
  private final boolean isComplexTypesSupported_;

  HdfsFileFormat(String inputFormat, String outputFormat, String serializationLib,
      boolean isComplexTypesSupported) {
    inputFormat_ = inputFormat;
    outputFormat_ = outputFormat;
    serializationLib_ = serializationLib;
    isComplexTypesSupported_ = isComplexTypesSupported;
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

  private static final Map<String, HdfsFileFormat> VALID_INPUT_FORMATS =
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
  public static HdfsFileFormat fromHdfsInputFormatClass(String inputFormatClass) {
    Preconditions.checkNotNull(inputFormatClass);
    return VALID_INPUT_FORMATS.get(inputFormatClass);
  }

  /**
   * Returns the corresponding enum for a SerDe class name. If classname is not one
   * of our supported formats, throws an IllegalArgumentException like Enum.valueOf
   */
  public static HdfsFileFormat fromJavaClassName(String className) {
    Preconditions.checkNotNull(className);
    if (isHdfsInputFormatClass(className)) return VALID_INPUT_FORMATS.get(className);
    throw new IllegalArgumentException(className);
  }

  public static HdfsFileFormat fromThrift(THdfsFileFormat thriftFormat) {
    switch (thriftFormat) {
      case RC_FILE: return HdfsFileFormat.RC_FILE;
      case TEXT: return HdfsFileFormat.TEXT;
      case SEQUENCE_FILE: return HdfsFileFormat.SEQUENCE_FILE;
      case AVRO: return HdfsFileFormat.AVRO;
      case PARQUET: return HdfsFileFormat.PARQUET;
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
      case PARQUET: return THdfsFileFormat.PARQUET;
      default:
        throw new RuntimeException("Unknown HdfsFormat: "
            + this + " - should never happen!");
    }
  }

  public String toSql(HdfsCompression compressionType) {
    switch (this) {
      case RC_FILE: return "RCFILE";
      case TEXT:
        if (compressionType == HdfsCompression.LZO ||
            compressionType == HdfsCompression.LZO_INDEX) {
          // TODO: Update this when we can write LZO text.
          // It is not currently possible to create a table with LZO compressed text files
          // in Impala, but this is valid in Hive.
          return String.format("INPUTFORMAT '%s' OUTPUTFORMAT '%s'",
              LZO_TEXT.inputFormat(), LZO_TEXT.outputFormat());
        }
        return "TEXTFILE";
      case SEQUENCE_FILE: return "SEQUENCEFILE";
      case AVRO: return "AVRO";
      case PARQUET: return "PARQUET";
      default:
        throw new RuntimeException("Unknown HdfsFormat: "
            + this + " - should never happen!");
    }
  }

  /*
   * Checks whether a file is supported in Impala based on the file extension.
   * Returns true if the file format is supported. If the file format is not
   * supported, then it returns false and 'errorMsg' contains details on the
   * incompatibility.
   *
   * Impala supports LZO, GZIP, SNAPPY and BZIP2 on text files for partitions that have
   * been declared in the metastore as TEXT. LZO files can have their own input format.
   * For now, raise an error on any other type.
   */
  public boolean isFileCompressionTypeSupported(String fileName,
      StringBuilder errorMsg) {
    // Check to see if the file has a compression suffix.
    // TODO: Add LZ4
    HdfsCompression compressionType = HdfsCompression.fromFileName(fileName);
    switch (compressionType) {
      case LZO:
      case LZO_INDEX:
        // Index files are read by the LZO scanner directly.
      case GZIP:
      case SNAPPY:
      case BZIP2:
      case NONE:
        return true;
      case DEFLATE:
        // TODO: Ensure that text/deflate works correctly
        if (this == TEXT) {
          errorMsg.append("Expected compressed text file with {.lzo,.gzip,.snappy,.bz2} "
              + "suffix: " + fileName);
          return false;
        } else {
          return true;
        }
      default:
        errorMsg.append("Unknown compression suffix: " + fileName);
        return false;
    }
  }

  /**
   * Returns true if this file format with the given compression format is splittable.
   */
  public boolean isSplittable(HdfsCompression compression) {
    switch (this) {
      case TEXT:
        return compression == HdfsCompression.NONE;
      case RC_FILE:
      case SEQUENCE_FILE:
      case AVRO:
        return true;
      case PARQUET:
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
   * Returns a list with all formats for which isComplexTypesSupported() is true.
   */
  public static List<HdfsFileFormat> complexTypesFormats() {
    List<HdfsFileFormat> result = Lists.newArrayList();
    for (HdfsFileFormat f: values()) {
      if (f.isComplexTypesSupported()) result.add(f);
    }
    return result;
  }
}
