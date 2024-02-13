// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.impala.util;

import java.util.Arrays;
import java.util.stream.Collectors;

public class ClassUtil {
  /**
   * Returns the current method name.
   *
   * Note: this method may be expensive. Use it judiciously.
   */
  public static String getMethodName() {
    StackTraceElement stackTrace = Thread.currentThread().getStackTrace()[2];
    return stackTrace.getClassName() + "." + stackTrace.getMethodName() + "()";
  }

  /**
   * Retrieves the current thread's stack trace and converts it into a string
   * representation.
   * <p> The first two elements of the stack trace are skipped:
   * <ul>
   * <li> {@code java.lang.Thread.getStackTrace()}</li>
   * <li> This method itself (i.e., {@link ClassUtil#getStackTraceForThread()})</li>
   * </ul>
   *
   * @return The caller's stack trace as a formatted string, excluding internal frames
   */
  public static String getStackTraceForThread() {
    return Arrays.stream(Thread.currentThread().getStackTrace())
        .skip(2)
        .map(StackTraceElement::toString)
        .collect(Collectors.joining("\n\tat "));
  }
}
