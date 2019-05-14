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

package org.apache.impala.customcluster;

import java.io.IOException;

/**
 * Runs an Impala cluster with custom flags.
 *
 * In order to prevent this from affecting other tests, we filter on the package name to
 * run FE custom cluster tests when we run the python custom cluster tests, so this should
 * not be used outside of this package.
 */
class CustomClusterRunner {
  public static int StartImpalaCluster() throws IOException, InterruptedException {
    return StartImpalaCluster("");
  }

  /**
   * Starts Impala and passes 'args' to the impalads, catalogd, and statestored.
   */
  public static int StartImpalaCluster(String args)
      throws IOException, InterruptedException {
    Process p = Runtime.getRuntime().exec(new String[] {"start-impala-cluster.py",
        "--impalad_args", args, "--catalogd_args", args, "--state_store_args", args});
    p.waitFor();
    return p.exitValue();
  }
}
