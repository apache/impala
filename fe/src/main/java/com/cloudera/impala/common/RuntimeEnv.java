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

import java.lang.System;

import com.cloudera.impala.thrift.TStartupOptions;
import com.cloudera.impala.service.FeSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains runtime-specific parameters such as the number of CPU cores. Currently only
 * used in Plan cost estimation. The static RuntimeEnv members can be set so that tests
 * can rely on a machine-independent RuntimeEnv.
 */
public class RuntimeEnv {
  private final static Logger LOG = LoggerFactory.getLogger(RuntimeEnv.class);

  public static RuntimeEnv INSTANCE = new RuntimeEnv();

  private int numCores_;

  // Indicates if column lineage information should be computed for each query.
  private boolean computeLineage_;

  // Indicates whether this is an environment for testing.
  private boolean isTestEnv_;

  public RuntimeEnv() {
    reset();
    try {
      TStartupOptions opts = FeSupport.GetStartupOptions();
      computeLineage_ = opts.compute_lineage;
    } catch (InternalException e) {
      LOG.error("Error retrieving BE startup options. Shutting down JVM");
      System.exit(1);
    }
  }

  /**
   * Resets this RuntimeEnv back to its machine-dependent state.
   */
  public void reset() {
    numCores_ = Runtime.getRuntime().availableProcessors();
  }

  public int getNumCores() { return numCores_; }
  public void setNumCores(int numCores) { this.numCores_ = numCores; }
  public void setTestEnv(boolean v) { isTestEnv_ = v; }
  public boolean isTestEnv() { return isTestEnv_; }
  public boolean computeLineage() { return computeLineage_; }

}
