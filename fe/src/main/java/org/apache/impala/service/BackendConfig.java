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

package org.apache.impala.service;

import com.google.common.base.Preconditions;

/**
 * This class is meant to provide the FE with impalad backend configuration parameters,
 * including command line arguments.
 * TODO: Remove this class and either
 * a) Figure out if there's a standard way to access flags from java
 * b) Create a util/gflags.java that let's us access the be flags
 */
public class BackendConfig {
  public static BackendConfig INSTANCE = new BackendConfig();

  // Default read block size (in bytes). This is the same as
  // the default FLAGS_read_size used by the IO manager in the backend.
  private final long READ_SIZE;

  // Variables below are overriden by JniFrontend/JniCatalog with user set configuration.
  // TODO: Read from backend instead of using static variables.

  // Determines how principal to short name conversion works. See User.java for more info.
  private static boolean allowAuthToLocalRules_ = false;

  // Kudu client timeout (ms).
  private static int kuduOperationTimeoutMs_ = 3 * 60 * 1000;

  private BackendConfig() {
    // TODO: Populate these by making calls to the backend instead of default constants.
    READ_SIZE = 8 * 1024 * 1024L;
  }

  public long getReadSize() { return READ_SIZE; }

  public static boolean isAuthToLocalEnabled() { return allowAuthToLocalRules_; }
  public static void setAuthToLocal(boolean authToLocal) {
    allowAuthToLocalRules_ = authToLocal;
  }

  public static int getKuduClientTimeoutMs() { return kuduOperationTimeoutMs_; }

  public static void setKuduClientTimeoutMs(int kuduOperationTimeoutMs) {
    Preconditions.checkArgument(kuduOperationTimeoutMs > 0);
    BackendConfig.kuduOperationTimeoutMs_ = kuduOperationTimeoutMs;
  }
}
