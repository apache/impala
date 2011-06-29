// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.common;

/**
 * Thrown for errors encountered during the execution of a SQL statement.
 *
 */
public class RuntimeException extends ImpalaException {
  public RuntimeException(String msg, Throwable cause) {
    super(msg, cause);
  }

  public RuntimeException(String msg) {
    super(msg);
  }
}
