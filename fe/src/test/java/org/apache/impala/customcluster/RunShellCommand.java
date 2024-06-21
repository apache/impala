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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * Helper class to run a shell command.
 */
class RunShellCommand {
  /**
   * Tuple wrapping stdout, stderr from Run.
   */
  public static class Output {
    public Output(String out, String err) {
      stdout = out;
      stderr = err;
    }

    public String stdout;
    public String stderr;
  }

  /**
   * Run a shell command 'cmd'. If 'shouldSucceed' is true, the command is expected to
   * succeed, otherwise it is expected to fail. Returns the output (stdout, stderr) of the
   * command.
   */
  public static Output Run(String[] cmd, boolean shouldSucceed, String expectedOut,
      String expectedErr) throws Exception {
    // run the command with the env variables inherited from the current process
    return Run(cmd, null, shouldSucceed, expectedOut, expectedErr);
  }

  /**
   * Run a shell command 'cmd' with custom 'env' variables.
   * If 'shouldSucceed' is true, the command is expected to
   * succeed, otherwise it is expected to fail. Returns the output (stdout, stderr) of the
   * command.
   */
  public static Output Run(String[] cmd, String[] env, boolean shouldSucceed,
                           String expectedOut, String expectedErr) throws Exception {
    Runtime rt = Runtime.getRuntime();
    Process process = rt.exec(cmd, env);
    // Collect stderr.
    BufferedReader input = new BufferedReader(
        new InputStreamReader(process.getErrorStream()));
    StringBuffer stderrBuf = new StringBuffer();
    String line;
    while ((line = input.readLine()) != null) {
      stderrBuf.append(line);
      stderrBuf.append('\n');
    }
    String stderr = stderrBuf.toString();
    assertTrue(stderr, stderr.contains(expectedErr));
    // Collect the stdout (which has the resultsets).
    input = new BufferedReader(new InputStreamReader(process.getInputStream()));
    StringBuffer stdoutBuf = new StringBuffer();
    while ((line = input.readLine()) != null) {
      stdoutBuf.append(line);
      stdoutBuf.append('\n');
    }
    int expectedReturn = shouldSucceed ? 0 : 1;
    assertEquals(stderr.toString(), expectedReturn, process.waitFor());
    // If the query succeeds, assert that the output is correct.
    String stdout = stdoutBuf.toString();
    if (shouldSucceed) assertTrue(stdout, stdout.contains(expectedOut));
    return new Output(stdout, stderr);
  }
}
