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

package org.apache.impala.common;

/**
 * Contains runtime-specific parameters such as the number of CPU cores. Currently only
 * used in Plan cost estimation. The static RuntimeEnv members can be set so that tests
 * can rely on a machine-independent RuntimeEnv.
 */
public class RuntimeEnv {
  public static RuntimeEnv INSTANCE = new RuntimeEnv();

  private int numCores_;

  // Indicates whether this is an environment for testing.
  private boolean isTestEnv_;

  // Whether we should do the same mt_dop validation in frontend tests as in the Impala
  // service.
  private boolean enableMtDopValidation_;

  // Whether we should allow ROLLUP/CUBE/GROUPING SETS queries to pass analysis
  private boolean enableGroupingSetsValidation_;

  public RuntimeEnv() {
    reset();
  }

  /**
   * Resets this RuntimeEnv back to its machine-dependent state.
   */
  public void reset() {
    numCores_ = Runtime.getRuntime().availableProcessors();
    isTestEnv_ = false;
    enableMtDopValidation_ = false;
    enableGroupingSetsValidation_ = true;
  }

  public int getNumCores() { return numCores_; }
  public void setNumCores(int numCores) { this.numCores_ = numCores; }
  public void setTestEnv(boolean v) { isTestEnv_ = v; }
  public boolean isTestEnv() { return isTestEnv_; }
  public boolean isMtDopValidationEnabled() { return enableMtDopValidation_; }
  public void setEnableMtDopValidation(boolean v) { enableMtDopValidation_ = v; }
  public boolean isGroupingSetsValidationEnabled() {
    return enableGroupingSetsValidation_;
  }
  public void setEnableGroupingSetsValidation(boolean v) {
    enableGroupingSetsValidation_ = v;
  }

}
