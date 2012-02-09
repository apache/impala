// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.service;

import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.junit.Assert;
import org.junit.Test;

import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.common.ImpalaException;
import org.apache.hadoop.hive.metastore.api.MetaException;

public class FrontendTest {

  @Test
  public void runTest() throws MetaException {
    Frontend fe = new Frontend();
    // for now, we only test that the c'tor works
    // TODO: test more functionality
  }
}
