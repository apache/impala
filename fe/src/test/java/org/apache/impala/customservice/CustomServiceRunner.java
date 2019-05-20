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

package org.apache.impala.customservice;

import java.io.IOException;

/**
 * Runs a minicluster component with custom flags.
 *
 * In order to prevent this from affecting other tests, we filter on the package name
 * to run FE custom cluster tests when we run the python custom cluster tests, so this
 * should not be used outside of this package.
 */
class CustomServiceRunner {

  /**
   *  Restarts a specified minicluster component (e.g kudu) in a separate process
   *  with the specified environment.
   */
  public static int RestartMiniclusterComponent(String component,
      String[] envp) throws IOException, InterruptedException {
    String command = System.getenv().get("IMPALA_HOME") +
        "/testdata/cluster/admin restart " + component;
    Process p = Runtime.getRuntime().exec(command, envp);
    p.waitFor();
    return p.exitValue();
  }
}
