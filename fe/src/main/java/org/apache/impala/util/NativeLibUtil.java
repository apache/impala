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

import java.io.File;

public class NativeLibUtil {
  /**
   * Attempts to load the given library from all paths in java.libary.path.
   * Throws a RuntimeException if the library was unable to be loaded from
   * any location.
   */
  public static void loadLibrary(String libFileName) {
    boolean found = false;
    String javaLibPath = System.getProperty("java.library.path");
    for (String path: javaLibPath.split(":")) {
      File libFile = new File(path + File.separator + libFileName);
      if (libFile.exists()) {
        System.load(libFile.getPath());
        found = true;
        break;
      }
    }
    if (!found) {
      throw new RuntimeException("Failed to load " + libFileName + " from any " +
          "location in java.library.path (" + javaLibPath + ").");
    }
  }
}