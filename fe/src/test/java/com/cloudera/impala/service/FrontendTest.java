// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.service;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.junit.Test;

public class FrontendTest {

  @Test
  public void runTest() throws MetaException {
    Frontend fe = new Frontend(true);
    // for now, we only test that the c'tor works
    // TODO: test more functionality
  }
}
