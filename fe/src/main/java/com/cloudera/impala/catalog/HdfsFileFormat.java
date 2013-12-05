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
  LZO_TEXT,
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
  private static final String LZO_TEXT_INPUT_FORMAT =
      "com.hadoop.mapred.DeprecatedLzoTextInputFormat";

  // Output format class for LZO compressed Text tables read by Hive.
  private static final String LZO_TEXT_OUTPUT_FORMAT =
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
      "parquet.hive.DeprecatedParquetInputFormat"
  };

  // Input format class for Avro tables read by hive.
  private static final String AVRO_INPUT_FORMAT =
      "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat";

  private static final Map<String, HdfsFileFormat> VALID_FORMATS =
      ImmutableMap.<String, HdfsFileFormat>builder()
          .put(RCFILE_INPUT_FORMAT, RC_FILE)
          .put(TEXT_INPUT_FORMAT, TEXT)
          .put(LZO_TEXT_INPUT_FORMAT, LZO_TEXT)
          .put(SEQUENCE_INPUT_FORMAT, SEQUENCE_FILE)
          .put(AVRO_INPUT_FORMAT, AVRO)
          .put(PARQUET_INPUT_FORMATS[0], PARQUET)
          .put(PARQUET_INPUT_FORMATS[1], PARQUET)
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
      case LZO_TEXT: return HdfsFileFormat.LZO_TEXT;
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
      case LZO_TEXT: return THdfsFileFormat.LZO_TEXT;
      case SEQUENCE_FILE: return THdfsFileFormat.SEQUENCE_FILE;
      case AVRO: return THdfsFileFormat.AVRO;
      case PARQUET: return THdfsFileFormat.PARQUET;
      default:
        throw new RuntimeException("Unknown HdfsFormat: "
            + this + " - should never happen!");
    }
  }

  public String toSql() {
    switch (this) {
      case RC_FILE: return "RCFILE";
      case TEXT: return "TEXTFILE";
      case SEQUENCE_FILE: return "SEQUENCEFILE";
      case AVRO: return "AVRO";
      case PARQUET: return "PARQUET";
      case LZO_TEXT:
        // It is not currently possible to create a table with LZO compressed text files
        // in Impala, but this is valid in Hive.
        return String.format("INPUTFORMAT '%s' OUTPUTFORMAT '%s'",
            LZO_TEXT_INPUT_FORMAT, LZO_TEXT_OUTPUT_FORMAT);
      default:
        throw new RuntimeException("Unknown HdfsFormat: "
            + this + " - should never happen!");
    }
  }

}
