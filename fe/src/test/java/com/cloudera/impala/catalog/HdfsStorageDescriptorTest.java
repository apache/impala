// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.catalog;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.hadoop.hive.serde.Constants;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class HdfsStorageDescriptorTest {
  @Test
  public void delimitersInCorrectOrder() {
    final List<String> DELIMITER_KEYS =
      ImmutableList.of(Constants.LINE_DELIM, Constants.FIELD_DELIM,
        Constants.COLLECTION_DELIM, Constants.MAPKEY_DELIM, Constants.ESCAPE_CHAR,
        Constants.QUOTE_CHAR);

    assertEquals(DELIMITER_KEYS, HdfsStorageDescriptor.DELIMITER_KEYS);
  }
}
