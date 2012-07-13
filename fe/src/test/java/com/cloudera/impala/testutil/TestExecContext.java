// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.testutil;

import com.google.common.base.Objects;

/*
 * Describes execution details for a query.
 * TODO: replace it with TQueryOptions
 */
public class TestExecContext {
  private final boolean abortOnError;
  private final int batchSize;
  private final boolean disableCodegen;
  private final int maxErrors;
  private final int numNodes;
  private final long maxScanRangeLength;
  private final int fileBufferSize;

  public TestExecContext(int numNodes, int batchSize, boolean disableCodegen,
                         boolean abortOnError, int maxErrors, long maxScanRangeLength,
                         int fileBufferSize) {
    this.numNodes = numNodes;
    this.batchSize = batchSize;
    this.disableCodegen = disableCodegen;
    this.abortOnError = abortOnError;
    this.maxErrors = maxErrors;
    this.maxScanRangeLength = maxScanRangeLength;
    this.fileBufferSize = fileBufferSize;
  }

  public TestExecContext(int numNodes, int batchSize, boolean disableCodegen,
      boolean abortOnError, int maxErrors) {
    this(numNodes, batchSize, disableCodegen, abortOnError, maxErrors, 0, 0);
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

  public long getMaxScanRangeLength() {
    return maxScanRangeLength;
  }

  public int getFileBufferSize() {
    return fileBufferSize;
  }

  @Override
  public String toString() {
      return Objects.toStringHelper(this).add("NumNodes", numNodes)
                                         .add("BatchSize",batchSize)
                                         .add("IsCodegenDisabled", disableCodegen)
                                         .add("AbortOnError", abortOnError)
                                         .add("MaxErrors", maxErrors)
                                         .add("MaxScanRangeLength", maxScanRangeLength)
                                         .add("FileBufferSize", fileBufferSize)
                                         .toString();
  }
}