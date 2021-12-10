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

package org.apache.impala.catalog;

/**
 * This class describes various options for file metadata reloading. It is
 * used when a partition metadata is being reloaded.
 * FORCE_LOAD option triggers file metadata loading in every case which can have
 * performance implications but will always ensure correctness
 * Other options i.e NO_LOAD, LOAD_IF_SD_CHANGED should only be used if
 * the user is certain that skipping of file metadata reloading
 * (for some or all partitions) won't cause any correctness issues
 */
public enum FileMetadataLoadOpts {
  NO_LOAD("do not load"),
  FORCE_LOAD("force load"),
  LOAD_IF_SD_CHANGED("load only if storage descriptor changed");

  private String description;
  FileMetadataLoadOpts(String d) {
    this.description = d;
  }

  public String getDescription() {
    return this.description;
  }
}
