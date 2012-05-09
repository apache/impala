// Copyright (c) 2012 Cloudera, Inc. All rights reserved.
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
  SEQUENCE_FILE;

  // Input format class for RCFile tables read by Hive.
  private static final String rcfileInputFormat =
      "org.apache.hadoop.hive.ql.io.RCFileInputFormat";

  // Input format class for Text tables read by Hive.
  private static final String textInputFormat =
      "org.apache.hadoop.mapred.TextInputFormat";

  //Input format class for Text tables read by Hive.
  private static final String sequenceInputFormat =
      "org.apache.hadoop.mapred.SequenceFileInputFormat";


  private static final Map<String, HdfsFileFormat> VALID_FORMATS =
      ImmutableMap.of(rcfileInputFormat, RC_FILE,
                      textInputFormat, TEXT,
                      sequenceInputFormat, SEQUENCE_FILE);

  /**
   * Returns true if the string describes an input format class that we support.
   */
  public static boolean isHdfsFormatClass(String formatClass) {
    return VALID_FORMATS.containsKey(formatClass);
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

  public THdfsFileFormat toThrift() {
    switch (this) {
    case RC_FILE:
      return THdfsFileFormat.RC_FILE;
    case TEXT:
      return THdfsFileFormat.TEXT;
    case SEQUENCE_FILE:
      return THdfsFileFormat.SEQUENCE_FILE;
    default:
      throw new RuntimeException("Unknown HdfsFormat: "
          + this + " - should never happen!");
    }
  }
}