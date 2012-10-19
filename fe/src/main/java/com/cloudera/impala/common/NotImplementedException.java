// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.common;

/**
 * Thrown for SQL statements that require as yet unimplemented functionality.
 *
 */
public class NotImplementedException extends ImpalaException {
  public NotImplementedException(String msg) {
    super(msg);
  }
}
