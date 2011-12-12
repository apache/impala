// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.planner;

import com.cloudera.impala.thrift.TDataSink;

/**
 * A DataSink describes the destination of a plan fragment's output rows.
 * The destination could be another plan fragment on a remote machine,
 * or a table into which the rows are to be inserted
 * (i.e., the destination of the last fragment of an INSERT statement).
 *
 */
public abstract class DataSink {
  public abstract String getExplainString();

  protected abstract TDataSink toThrift();
}
