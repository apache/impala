// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.common;

/**
 * Thrown for errors encountered during the execution of a SQL statement.
 *
 */
public class ImpalaRuntimeException extends ImpalaException {
  public ImpalaRuntimeException(String msg, Throwable cause) {
    super(msg, cause);
  }

  public ImpalaRuntimeException(String msg) {
    super(msg);
  }
}
