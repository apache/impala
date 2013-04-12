// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.common;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;

/**
 * Utility class with methods intended for JNI clients
 */
public class JniUtil {

  /**
   * Returns a formatted string containing the simple exception name and the
   * exception message without the full stack trace. Includes the
   * the chain of causes each in a separate line.
   * Writes the full stack trace to the log.
   */
  public static String throwableToString(Throwable t) {
    Writer output = new StringWriter();
    try {
      output.write(String.format("%s: %s", t.getClass().getSimpleName(),
          t.getMessage()));
      // Follow the chain of exception causes and print them as well.
      Throwable cause = t;
      while ((cause = cause.getCause()) != null) {
        output.write(String.format("\nCAUSED BY: %s: %s",
            cause.getClass().getSimpleName(), cause.getMessage()));
      }
    } catch (IOException e) {
      throw new Error(e);
    }
    return output.toString();
  }

}
