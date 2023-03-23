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
import java.util.Map;
import java.util.HashMap;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

/**
 * Runs an Impala cluster with custom flags.
 *
 * In order to prevent this from affecting other tests, we filter on the package name to
 * run FE custom cluster tests when we run the python custom cluster tests, so this should
 * not be used outside of this package.
 */
class CustomClusterRunner {
  private static final Logger LOG = Logger.getLogger(CustomClusterRunner.class);

  public static int StartImpalaCluster() throws IOException, InterruptedException {
    return StartImpalaCluster("");
  }

  public static int StartImpalaCluster(String args)
      throws IOException, InterruptedException {
    return StartImpalaCluster(args, new HashMap<String, String>());
  }

  public static int StartImpalaCluster(String args, Map<String, String> env)
      throws IOException, InterruptedException {
    return StartImpalaCluster(args, env, "");
  }

  /**
   * Starts Impala, setting environment variables in 'env', and passing 'args' to the
   * impalads, catalogd, and statestored, and 'startArgs' to start-impala-cluster.py.
   */
  public static int StartImpalaCluster(String args, Map<String, String> env,
      String startArgs) throws IOException, InterruptedException {
    return StartImpalaCluster(args, args, args, "", env, startArgs);
  }

  public static int StartImpalaCluster(String impaladArgs, String catalogdArgs,
      String statestoredArgs) throws IOException, InterruptedException {
    return StartImpalaCluster(
        impaladArgs, catalogdArgs, statestoredArgs, "", new HashMap<String, String>(),
        "");
  }

  public static int StartImpalaCluster(String impaladArgs, String catalogdArgs,
      String statestoredArgs, String startArgs) throws IOException, InterruptedException {
    return StartImpalaCluster(impaladArgs, catalogdArgs, statestoredArgs, "",
        new HashMap<String, String>(), startArgs);
  }

  public static int StartImpalaCluster(String impaladArgs, String catalogdArgs,
      String statestoredArgs, Map<String, String> env, String startArgs)
      throws IOException, InterruptedException {
    return StartImpalaCluster(impaladArgs, catalogdArgs, statestoredArgs, "",
        env, startArgs);
  }

  /**
   * Starts Impala, setting environment variables in 'env', and passing 'impalad_args',
   * 'catalogd_args', 'statestored_args', 'admissiond_args' and 'startArgs' to
   * start-impala-cluster.py.
   */
  public static int StartImpalaCluster(String impaladArgs, String catalogdArgs,
      String statestoredArgs, String admissiondArgs, Map<String, String> env,
      String startArgs)
      throws IOException, InterruptedException {
    ProcessBuilder pb;
    if (!admissiondArgs.isEmpty()) {
      pb = new ProcessBuilder(new String[] {"start-impala-cluster.py",
          "--impalad_args", impaladArgs, "--catalogd_args", catalogdArgs,
          "--state_store_args", statestoredArgs, "--enable_admission_service", "true",
          "--admissiond_args", admissiondArgs, startArgs});
    } else {
      pb = new ProcessBuilder(new String[] {"start-impala-cluster.py",
          "--impalad_args", impaladArgs, "--catalogd_args", catalogdArgs,
          "--state_store_args", statestoredArgs, startArgs});
    }
    pb.redirectErrorStream(true);
    Map<String, String> origEnv = pb.environment();
    origEnv.putAll(env);
    Process p = pb.start();
    p.waitFor();
    // Print out the output of the process, for debugging. We only need to print stdout,
    // as stderr is redirected to it above.
    LOG.info(IOUtils.toString(p.getInputStream()));
    return p.exitValue();
  }
}
