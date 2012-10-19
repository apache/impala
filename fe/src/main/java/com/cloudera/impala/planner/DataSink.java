// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import com.cloudera.impala.thrift.TDataSink;
import com.cloudera.impala.thrift.TExplainLevel;

/**
 * A DataSink describes the destination of a plan fragment's output rows.
 * The destination could be another plan fragment on a remote machine,
 * or a table into which the rows are to be inserted
 * (i.e., the destination of the last fragment of an INSERT statement).
 *
 */
public abstract class DataSink {
  /**
   * Return an explain string for the DataSink. Each line of the explain will be prefixed
   * by "prefix"
   * @param prefix each explain line will be started with the given prefix
   * @return
   */
  public abstract String getExplainString(String prefix, TExplainLevel explainLevel);

  protected abstract TDataSink toThrift();
}
