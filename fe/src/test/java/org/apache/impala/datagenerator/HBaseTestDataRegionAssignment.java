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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.PairOfSameType;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.impala.planner.HBaseScanNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 * Deterministically assigns regions to region servers.
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
  private final HBaseAdmin hbaseAdmin;
  private final List<ServerName> sortedRS; // sorted list of region server name
  private final String[] splitPoints = { "1", "3", "5", "7", "9"};

  public HBaseTestDataRegionAssignment() throws IOException {
    conf = new Configuration();
    hbaseAdmin = new HBaseAdmin(conf);
    ClusterStatus clusterStatus = hbaseAdmin.getClusterStatus();
    Collection<ServerName> regionServerNames = clusterStatus.getServers();
    sortedRS = new ArrayList<ServerName>(regionServerNames);
    Collections.sort(sortedRS);
  }

  public void close() throws IOException {
    hbaseAdmin.close();
  }

  /**
   * The table comes in already split into regions specified by splitPoints and with data
   * already loaded. Pair up adjacent regions and assign to the same server.
   * Each region pair in ([unbound:1,1:3], [3:5,5:7], [7:9,9:unbound])
   * will be on the same server.
   */
  public void performAssignment(String tableName) throws IOException,
    InterruptedException, TableNotFoundException {
    HTableDescriptor[] desc = hbaseAdmin.listTables(tableName);
    if (desc == null || desc.length == 0) {
      throw new TableNotFoundException("Table " + tableName + " not found.");
    }

    // Sort the region by start key
    List<HRegionInfo> regions = hbaseAdmin.getTableRegions(tableName.getBytes());
    Preconditions.checkArgument(regions.size() == splitPoints.length + 1);
    Collections.sort(regions);

    // Pair up two adjacent regions to the same region server. That is,
    // region server 1 <- regions (unbound:1), (1:3)
    // region server 2 <- regions (3:5), (5:7)
    // region server 3 <- regions (7:9), (9:unbound)
    NavigableMap<HRegionInfo, ServerName> expectedLocs = Maps.newTreeMap();
    for (int i = 0; i < regions.size(); ++i) {
      HRegionInfo regionInfo = regions.get(i);
      int rsIdx = (i / 2) % sortedRS.size();
      ServerName regionServerName = sortedRS.get(rsIdx);
      hbaseAdmin.move(regionInfo.getEncodedNameAsBytes(),
          regionServerName.getServerName().getBytes());
      expectedLocs.put(regionInfo, regionServerName);
    }

    // hbaseAdmin.move() is an asynchronous operation. HBase tests use sleep to wait for
    // the move to complete. It should be done in 10sec.
    int sleepCnt = 0;
    HTable hbaseTable = new HTable(conf, tableName);
    try {
      while(!expectedLocs.equals(hbaseTable.getRegionLocations()) &&
          sleepCnt < 100) {
        Thread.sleep(100);
        ++sleepCnt;
      }
      NavigableMap<HRegionInfo, ServerName> actualLocs = hbaseTable.getRegionLocations();
      Preconditions.checkArgument(expectedLocs.equals(actualLocs));

      // Log the actual region location map
      for (Map.Entry<HRegionInfo, ServerName> entry: actualLocs.entrySet()) {
        LOG.info(HBaseScanNode.printKey(entry.getKey().getStartKey()) + " -> " +
            entry.getValue().getHostAndPort());
      }

      // Force a major compaction such that the HBase table is backed by deterministic
      // physical artifacts (files, WAL, etc.). Our #rows estimate relies on the sizes of
      // these physical artifacts.
      LOG.info("Major compacting HBase table: " + tableName);
      hbaseAdmin.majorCompact(tableName);
    } finally {
      IOUtils.closeQuietly(hbaseTable);
    }
  }
}

