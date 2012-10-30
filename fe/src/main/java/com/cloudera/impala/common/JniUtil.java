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
