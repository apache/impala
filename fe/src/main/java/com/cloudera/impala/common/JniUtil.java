// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.common;

import java.io.Writer;
import java.io.StringWriter;
import java.io.PrintWriter;

/**
 * Utility class with methods intended for JNI clients
 */
public class JniUtil {

  /**
   * Returns a throwable's exception message and full stack trace (which toString() does 
   * not)
   */
  public static String throwableToString(Throwable t) {
    Writer output = new StringWriter();
    t.printStackTrace(new PrintWriter(output));
    return output.toString();
  }

}
