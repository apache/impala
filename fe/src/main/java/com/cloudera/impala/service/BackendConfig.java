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

package com.cloudera.impala.service;

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

  private BackendConfig() {
    // TODO: Populate these by making calls to the backend instead of default constants.
    READ_SIZE = 8 * 1024 * 1024L;
  }

  public long getReadSize() { return READ_SIZE; }
}