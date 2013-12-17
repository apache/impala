// Copyright 2013 Cloudera Inc.
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

package com.cloudera.impala.util;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Process-related utilities.
 */
public class ProcessUtil {
  private final static Logger LOG = LoggerFactory.getLogger(ProcessUtil.class);

  /**
   * Returns the ID of the current process, or null if an exception is thrown.
   * This is necessary because Java does not have a good way to get the pid of the
   * current process.
   */
  public static Integer getCurrentProcessId() {
    try {
      return Integer.valueOf(new File("/proc/self").getCanonicalFile().getName());
    } catch (Exception e) {
      LOG.warn("Unable to get ID of current process", e);
      return null;
    }
  }
}
