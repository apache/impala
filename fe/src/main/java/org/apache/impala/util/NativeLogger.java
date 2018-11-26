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

/**
 * Class that manages loading and calling the native logging library to forward
 * log4j log messages to be logged by glog.
 */
public class NativeLogger {
  private static boolean loaded_ = false;

  // Writes a log message to glog
  private native static void Log(int severity, String msg, String filename, int line);

  public static void LogToGlog(int severity, String msg, String filename, int line) {
    try {
      Log(severity, msg, filename, line);
    } catch (UnsatisfiedLinkError e) {
      loadLibrary();
      Log(severity, msg, filename, line);
    }
  }

  /**
   * Loads the native logging support library.
   */
  private static synchronized void loadLibrary() {
    if (loaded_) return;
    NativeLibUtil.loadLibrary("libloggingsupport.so");
    loaded_ = true;
  }
}