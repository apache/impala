// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.catalog;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.hadoop.hive.serde.serdeConstants;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class HdfsStorageDescriptorTest {
  @Test
  public void delimitersInCorrectOrder() {
    final List<String> DELIMITER_KEYS =
      ImmutableList.of(serdeConstants.LINE_DELIM, serdeConstants.FIELD_DELIM,
        serdeConstants.COLLECTION_DELIM, serdeConstants.MAPKEY_DELIM, 
        serdeConstants.ESCAPE_CHAR, serdeConstants.QUOTE_CHAR);

    assertEquals(DELIMITER_KEYS, HdfsStorageDescriptor.DELIMITER_KEYS);
  }
}
