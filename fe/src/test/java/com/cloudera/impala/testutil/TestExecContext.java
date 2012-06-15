// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.testutil;

import com.google.common.base.Objects;

/*
 * Describes execution details for a query.
 */
public class TestExecContext {
  private final boolean abortOnError;
  private final int batchSize;
  private final boolean disableCodegen;
  private final int maxErrors;
  private final int numNodes;

  public TestExecContext(int numNodes, int batchSize, boolean disableCodegen,
                         boolean abortOnError, int maxErrors) {
    this.numNodes = numNodes;
    this.batchSize = batchSize;
    this.disableCodegen = disableCodegen;
    this.abortOnError = abortOnError;
    this.maxErrors = maxErrors;
  }

  public boolean getAbortOnError() {
    return abortOnError;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public boolean isCodegenDisabled() {
    return disableCodegen;
  }

  public int getMaxErrors() {
    return maxErrors;
  }

  public int getNumNodes() {
    return numNodes;
  }

  @Override
  public String toString() {
      return Objects.toStringHelper(this).add("NumNodes", numNodes)
                                         .add("BatchSize",batchSize)
                                         .add("IsCodegenDisabled", disableCodegen)
                                         .add("AbortOnError", abortOnError)
                                         .add("MaxErrors", maxErrors)
                                         .toString();
  }
}