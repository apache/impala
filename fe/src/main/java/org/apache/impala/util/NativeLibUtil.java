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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;

public class NativeLibUtil {
  private final static Logger LOG = LoggerFactory.getLogger(NativeLibUtil.class);

  /**
   * Attempts to load the given library from all paths in java.libary.path,
   * as well as the current build directory (assuming we are in a test environment).
   * Throws a RuntimeException if the library was unable to be loaded from
   * any location.
   */
  public static void loadLibrary(String libFileName) {
    List<String> candidates = new ArrayList<>(Arrays.asList(
        System.getProperty("java.library.path").split(":")));

    // Fall back to automatically finding the library in test environments.
    // This makes it easier to run tests from Eclipse without specially configuring
    // the Run Configurations.
    try {
      String myPath = NativeLibUtil.class.getProtectionDomain()
          .getCodeSource().getLocation().getPath();
      if (myPath.toString().endsWith("fe/target/classes/") ||
          myPath.toString().endsWith("fe/target/eclipse-classes/")) {
        candidates.add(myPath + "../../../be/build/latest/service/");
      }
    } catch (Exception e) {
      LOG.warn("Unable to get path for NativeLibUtil class", e);
    }

    for (String path: candidates) {
      File libFile = new File(path + File.separator + libFileName);
      if (libFile.exists()) {
        System.load(libFile.getPath());
        return;
      }
    }

    throw new RuntimeException("Failed to load " + libFileName + " from any " +
        "candidate location:\n" + Joiner.on("\n").join(candidates));
  }
}
