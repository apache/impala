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

/**
 * Contains runtime-specific parameters such as the number of CPU cores. Currently only
 * used in Plan cost estimation. The static RuntimeEnv members can be set so that tests
 * can rely on a machine-independent RuntimeEnv.
 */
public class RuntimeEnv {
  public static RuntimeEnv INSTANCE = new RuntimeEnv();

  private int numCores;

  public RuntimeEnv() {
    reset();
  }

  /**
   * Resets this RuntimeEnv back to its machine-dependent state.
   */
  public void reset() {
    numCores = Runtime.getRuntime().availableProcessors();
  }

  public int getNumCores() { return numCores; }
  public void setNumCores(int numCores) { this.numCores = numCores; }
}
