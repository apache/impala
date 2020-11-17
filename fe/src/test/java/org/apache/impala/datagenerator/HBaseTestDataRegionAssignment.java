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

package org.apache.impala.datagenerator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.impala.planner.HBaseScanNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 * Deterministically assign regions to region servers.
 */
public class HBaseTestDataRegionAssignment {
  public class TableNotFoundException extends Exception {
    public TableNotFoundException(String s) {
      super(s);
    }
  }

  private final static Logger LOG = LoggerFactory.getLogger(
      HBaseTestDataRegionAssignment.class);
  private final Configuration conf;
  private Connection connection = null;
  private final Admin admin;
  private final List<ServerName> sortedRS; // sorted list of region server name
  private final String[] splitPoints = { "1", "3", "5", "7", "9"};

  private final static int REGION_MOVE_TIMEOUT_MILLIS = 120000;

  public HBaseTestDataRegionAssignment() throws IOException {
    conf = new Configuration();
    connection = ConnectionFactory.createConnection(conf);
    admin = connection.getAdmin();
    ClusterStatus clusterStatus = admin.getClusterStatus();
    List<ServerName> regionServerNames =
        new ArrayList<ServerName>(clusterStatus.getServers());
    ServerName master = clusterStatus.getMaster();
    regionServerNames.remove(master);
    sortedRS = new ArrayList<ServerName>(regionServerNames);
    Collections.sort(sortedRS);
  }

  public void close() throws IOException {
    admin.close();
  }

  /**
   * The table comes in already split into regions specified by splitPoints and with data
   * already loaded. Pair up adjacent regions and assign to the same server.
   * Each region pair in ([unbound:1,1:3], [3:5,5:7], [7:9,9:unbound])
   * will be on the same server.
   */
  public void performAssignment(String tableName) throws IOException,
    InterruptedException, TableNotFoundException {
    TableName table = TableName.valueOf(tableName);
    if (!admin.tableExists(table)) {
      throw new TableNotFoundException("Table " + tableName + " not found.");
    }

    // Sort the region by start key
    List<RegionInfo> regions = admin.getRegions(table);
    Preconditions.checkArgument(regions.size() == splitPoints.length + 1);
    Collections.sort(regions, RegionInfo.COMPARATOR);
    // Pair up two adjacent regions to the same region server. That is,
    // region server 1 <- regions (unbound:1), (1:3)
    // region server 2 <- regions (3:5), (5:7)
    // region server 3 <- regions (7:9), (9:unbound)
    HashMap<String, ServerName> expectedLocs = Maps.newHashMap();
    for (int i = 0; i < regions.size(); ++i) {
      RegionInfo regionInfo = regions.get(i);
      int rsIdx = (i / 2) % sortedRS.size();
      ServerName regionServerName = sortedRS.get(rsIdx);
      LOG.info("Moving " + regionInfo.getRegionNameAsString() +
               " to " + regionServerName.getAddress());
      admin.move(regionInfo.getEncodedNameAsBytes(),
          regionServerName.getServerName().getBytes());
      expectedLocs.put(regionInfo.getRegionNameAsString(), regionServerName);
    }

    // admin.move() is an asynchronous operation. Wait for the move to complete.
    // It should be done in 60 sec.
    long start = System.currentTimeMillis();
    long timeout = System.currentTimeMillis() + REGION_MOVE_TIMEOUT_MILLIS;
    while (true) {
      int matched = 0;
      List<Pair<RegionInfo, ServerName>> pairs =
          MetaTableAccessor.getTableRegionsAndLocations(connection, table);
      Preconditions.checkState(pairs.size() == regions.size());
      for (Pair<RegionInfo, ServerName> pair: pairs) {
        RegionInfo regionInfo = pair.getFirst();
        String regionName = regionInfo.getRegionNameAsString();
        ServerName serverName = pair.getSecond();
        Preconditions.checkNotNull(expectedLocs.get(regionName));
        LOG.info(regionName + " " + HBaseScanNode.printKey(regionInfo.getStartKey()) +
            " -> " +  serverName.getAddress().toString() + ", expecting " +
            expectedLocs.get(regionName));
        if (expectedLocs.get(regionName).equals(serverName)) {
           ++matched;
           continue;
        }
      }
      if (matched == regions.size()) {
        long elapsed = System.currentTimeMillis() - start;
        LOG.info("Regions moved after " + elapsed + " millis.");
        break;
      }
      if (System.currentTimeMillis() < timeout) {
        Thread.sleep(100);
        continue;
      }
      throw new IllegalStateException(
          String.format("Failed to assign regions to servers after " +
            REGION_MOVE_TIMEOUT_MILLIS + " millis."));
    }

    // Force a major compaction such that the HBase table is backed by deterministic
    // physical artifacts (files, WAL, etc.). Our #rows estimate relies on the sizes of
    // these physical artifacts.
    LOG.info("Major compacting HBase table: " + tableName);
    admin.majorCompact(table);
  }
}

