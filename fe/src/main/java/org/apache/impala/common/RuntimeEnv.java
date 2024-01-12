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

import com.google.common.base.Preconditions;

import org.apache.impala.catalog.SideloadTableStats;

import java.util.HashMap;
import java.util.Map;

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

  // Map of <Db,<Table,SideloadTableStats>> that is used to simulate large scale table
  // during testing.
  private Map<String, Map<String, SideloadTableStats>> sideloadStats_ = null;

  // Map of <Db,<Table,ScanRangeLimit>> that is used to inject desired scan range limit
  // for specific table scan. Used to simulate larger number of files in a table scan.
  private Map<String, Map<String, Long>> tableScanRangeLimit_ = null;

  public RuntimeEnv() {
    reset();
  }

  /**
   * Resets this RuntimeEnv back to its machine-dependent state.
   */
  public void reset() {
    numCores_ = Runtime.getRuntime().availableProcessors();
    isTestEnv_ = false;
  }

  public int getNumCores() { return numCores_; }
  public void setNumCores(int numCores) { this.numCores_ = numCores; }
  public void setTestEnv(boolean v) { isTestEnv_ = v; }
  public boolean isTestEnv() { return isTestEnv_; }
  public boolean hasTableScanRangeLimit() { return tableScanRangeLimit_ != null; }

  /**
   * Populate the test stats with given sideloadStats.
   * isTestEnv() must be true, and caller must invalidate the table on their own to
   * trigger table stats reloading from this RuntimeEnv.
   * @param sideloadStats map containing the test stats.
   */
  public void setSideloadStats(
      Map<String, Map<String, SideloadTableStats>> sideloadStats) {
    Preconditions.checkState(isTestEnv_);
    sideloadStats_ = sideloadStats;
  }

  /**
   * Check if db.table has SideloadStas declared.
   * @param db database name.
   * @param table table name.
   * @return true if there is a SideloadStas declared for db.table. Otherwise, false.
   */
  public boolean hasSideloadStats(String db, String table) {
    return sideloadStats_ != null && sideloadStats_.containsKey(db)
        && sideloadStats_.get(db).containsKey(table);
  }

  /**
   * Get SideloadStats for specified db.name.
   * @param db database name.
   * @param table table name.
   * @return SideloadStats or null of there is no SideloadStats specified
   *         for db.table.
   */
  public SideloadTableStats getSideloadStats(String db, String table) {
    if (!hasSideloadStats(db, table)) return null;
    return sideloadStats_.get(db).get(table);
  }

  /**
   * Drop all test stats that previously set.
   * Caller must invalidate the table on their own to trigger table stats reloading
   * from Metastore.
   */
  public void dropSideloadStats() { sideloadStats_ = null; }

  /**
   * Add a scan range limit for db.table scan. isTestEnv() must be true.
   * @param db database name. Must not null nor empty.
   * @param table table name. Must not null nor empty.
   * @param scanRangeLimit the scan range limit.
   */
  public void addTableScanRangeLimit(String db, String table, long scanRangeLimit) {
    Preconditions.checkNotNull(db);
    Preconditions.checkNotNull(table);
    Preconditions.checkArgument(!db.isEmpty());
    Preconditions.checkArgument(!table.isEmpty());
    Preconditions.checkArgument(scanRangeLimit > 1);
    Preconditions.checkState(isTestEnv_);
    if (tableScanRangeLimit_ == null) tableScanRangeLimit_ = new HashMap<>();
    if (!tableScanRangeLimit_.containsKey(db)) {
      tableScanRangeLimit_.put(db, new HashMap<>());
    }
    tableScanRangeLimit_.get(db).put(table, scanRangeLimit);
  }

  /**
   * Get scan range limit for db.table.
   * @param db database name.
   * @param table table name.
   * @return scan range limit for db.table or -1 if none were specified.
   */
  public long getTableScanRangeLimit(String db, String table) {
    long scanRangeLimit = -1;
    if (isTestEnv_ && tableScanRangeLimit_ != null && tableScanRangeLimit_.containsKey(db)
        && tableScanRangeLimit_.get(db).containsKey(table)) {
      scanRangeLimit = tableScanRangeLimit_.get(db).get(table);
    }
    return scanRangeLimit;
  }

  /**
   * Drop all scan range limit that previously set.
   */
  public void dropTableScanRangeLimit() { tableScanRangeLimit_ = null; }
}
