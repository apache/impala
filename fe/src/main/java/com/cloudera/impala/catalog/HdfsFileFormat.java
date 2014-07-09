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

import java.util.Map;

import com.cloudera.impala.thrift.THdfsFileFormat;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

/**
 * Supported HDFS file formats
 */
public enum HdfsFileFormat {
  RC_FILE,
  TEXT,
  SEQUENCE_FILE,
  AVRO,
  PARQUET;

  // Input format class for RCFile tables read by Hive.
  private static final String RCFILE_INPUT_FORMAT =
      "org.apache.hadoop.hive.ql.io.RCFileInputFormat";

  // Input format class for Text tables read by Hive.
  private static final String TEXT_INPUT_FORMAT =
      "org.apache.hadoop.mapred.TextInputFormat";

  // Input format class for LZO compressed Text tables read by Hive.
  public static final String LZO_TEXT_INPUT_FORMAT =
      "com.hadoop.mapred.DeprecatedLzoTextInputFormat";

  // Output format class for LZO compressed Text tables read by Hive.
  public static final String LZO_TEXT_OUTPUT_FORMAT =
      "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat";

  // Input format class for Sequence file tables read by Hive.
  private static final String SEQUENCE_INPUT_FORMAT =
      "org.apache.hadoop.mapred.SequenceFileInputFormat";

  // Input format class for Parquet tables read by Hive.
  // The location (i.e. java class path) for the SerDe has
  // changed during its development. Impala will treat any
  // of these format classes as Parquet
  private static final String[] PARQUET_INPUT_FORMATS = {
      "com.cloudera.impala.hive.serde.ParquetInputFormat",
      "parquet.hive.DeprecatedParquetInputFormat",
      "parquet.hive.MapredParquetInputFormat",
      "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
  };

  // Input format class for Avro tables read by hive.
  private static final String AVRO_INPUT_FORMAT =
      "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat";

  private static final Map<String, HdfsFileFormat> VALID_FORMATS =
      ImmutableMap.<String, HdfsFileFormat>builder()
          .put(RCFILE_INPUT_FORMAT, RC_FILE)
          .put(TEXT_INPUT_FORMAT, TEXT)
          .put(LZO_TEXT_INPUT_FORMAT, TEXT)
          .put(SEQUENCE_INPUT_FORMAT, SEQUENCE_FILE)
          .put(AVRO_INPUT_FORMAT, AVRO)
          .put(PARQUET_INPUT_FORMATS[0], PARQUET)
          .put(PARQUET_INPUT_FORMATS[1], PARQUET)
          .put(PARQUET_INPUT_FORMATS[2], PARQUET)
          .put(PARQUET_INPUT_FORMATS[3], PARQUET)
          .build();
  /**
   * Returns true if the string describes an input format class that we support.
   */
  public static boolean isHdfsFormatClass(String formatClass) {
    return VALID_FORMATS.containsKey(formatClass);
  }

  /**
   * Returns the file format associated with the input format class, or null if
   * the input format class is not supported.
   */
  public static HdfsFileFormat fromHdfsInputFormatClass(String inputFormatClass) {
    Preconditions.checkNotNull(inputFormatClass);
    return VALID_FORMATS.get(inputFormatClass);
  }

  /**
   * Returns the corresponding enum for a SerDe class name. If classname is not one
   * of our supported formats, throws an IllegalArgumentException like Enum.valueOf
   */
  public static HdfsFileFormat fromJavaClassName(String className) {
    Preconditions.checkNotNull(className);
    if (isHdfsFormatClass(className)) {
      return VALID_FORMATS.get(className);
    }
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
              LZO_TEXT_INPUT_FORMAT,
              LZO_TEXT_OUTPUT_FORMAT);
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

}
