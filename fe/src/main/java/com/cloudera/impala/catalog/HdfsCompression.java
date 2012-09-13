// Copyright (c) 2012 Cloudera, Inc. All rights reserved.
package com.cloudera.impala.catalog;

import java.util.Map;

import com.cloudera.impala.thrift.THdfsFileFormat;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

/**
 * Support for recognizing compression suffixes on data files.
 * Compression of a file is recognized in mapreduce by looking for suffixes of
 * supported codecs. 
 * For now Impala only supports LZO which requires a specific HIVE input class.
 */
public enum HdfsCompression {
  NONE,
  DEFLATE,
  GZIP,
  BZIP2,
  LZO,
  LZO_INDEX; //Lzo index file.

  /* Map from a suffix to a compression type */
  private static final Map<String, HdfsCompression> SUFFIX_MAP =
      ImmutableMap.of("deflate", DEFLATE,
          "gz", GZIP,
          "bz2", BZIP2,
          "lzo", LZO,
          "index", LZO_INDEX);

  /* Given a file name return its compression type, if any. */
  public static HdfsCompression fromFileName(String fileName) {
    int index = fileName.lastIndexOf(".");
    if (index == -1) return NONE;

    String suffix = fileName.substring(index + 1);
    if (SUFFIX_MAP.containsKey(suffix)) {
      return SUFFIX_MAP.get(suffix);
    }

    return NONE;
  }
}
