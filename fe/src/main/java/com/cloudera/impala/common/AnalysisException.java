// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.common;

/**
 * Thrown for errors encountered during analysis of a SQL statement.
 *
 */
public class AnalysisException extends ImpalaException {
  public AnalysisException(String msg, Throwable cause) {
    super(msg, cause);
  }

  public AnalysisException(String msg) {
    super(msg);
  }
}
