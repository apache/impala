// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.common;

/**
 * Thrown for internal server errors.
 *
 */
public class InternalException extends ImpalaException {
  public InternalException(String msg, Throwable cause) {
    super(msg, cause);
  }

  public InternalException(String msg) {
    super(msg);
  }
}
