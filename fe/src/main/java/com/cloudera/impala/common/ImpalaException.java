// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.common;

/**
 * The parent class of all custom Impala exceptions.
 *
 */
abstract public class ImpalaException extends java.lang.Exception {
  protected ImpalaException(String msg) {
    super(msg);
  }
}
