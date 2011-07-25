// Copyright (c) 2011 Cloudera, Inc. All rights reserved.
package com.cloudera.impala.jdbc;

/**
 * Helper class to throw an Exception including the last called method name.
 *
 */
public class UnsupportedOpHelper {
  public static UnsupportedOperationException newUnimplementedMethodException() {
    StackTraceElement frame = Thread.currentThread().getStackTrace()[2];
    String message = "Method not implemented: " +
        frame.getFileName() + ":" + frame.getLineNumber() + ":" + frame.getClassName() + "."
        + frame.getMethodName();
    return new UnsupportedOperationException(message);
  }
}
