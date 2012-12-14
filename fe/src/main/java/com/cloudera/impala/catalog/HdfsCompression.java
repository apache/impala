// Copyright (c) 2012 Cloudera, Inc. All rights reserved.
package com.cloudera.impala.catalog;

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
  SNAPPY,
  LZO,
  LZO_INDEX; //Lzo index file.

  /* Map from a suffix to a compression type */
  private static final ImmutableMap<String, HdfsCompression> SUFFIX_MAP =
      ImmutableMap.<String, HdfsCompression>builder().
          put("deflate", DEFLATE).
          put("gz", GZIP).
          put("bz2", BZIP2).
          put("snappy", SNAPPY).
          put("lzo", LZO).
          put("index", LZO_INDEX).
          build();

  /* Given a file name return its compression type, if any. */
  public static HdfsCompression fromFileName(String fileName) {
    int index = fileName.lastIndexOf(".");
    if (index == -1) {
      return NONE;
    }

    String suffix = fileName.substring(index + 1);
    if (SUFFIX_MAP.containsKey(suffix)) {
      return SUFFIX_MAP.get(suffix);
    }

    return NONE;
  }
}
