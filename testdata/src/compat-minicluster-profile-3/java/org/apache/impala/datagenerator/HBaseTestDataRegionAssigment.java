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
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.PairOfSameType;
import org.apache.hadoop.hbase.util.Threads;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 * Splits HBase tables into regions and deterministically assigns regions to region
 * servers.
 */
class HBaseTestDataRegionAssigment {
  public class TableNotFoundException extends Exception {
    public TableNotFoundException(String s) {
      super(s);
    }
  }

  private final static Logger LOG = LoggerFactory.getLogger(
      HBaseTestDataRegionAssigment.class);
  private final Configuration conf;
  private Connection connection = null;
  private final Admin admin;
  private final List<ServerName> sortedRS; // sorted list of region server name
  private final String[] splitPoints = { "1", "3", "5", "7", "9"};

  // Number of times to retry a series region-split/wait-for-split calls.
  private final static int MAX_SPLIT_ATTEMPTS = 10;

  // Maximum time in ms to wait for a region to be split.
  private final static int WAIT_FOR_SPLIT_TIMEOUT = 10000;

  private final static int REGION_MOVE_TIMEOUT_MILLIS = 60000;

  public HBaseTestDataRegionAssigment() throws IOException {
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
   * Split the table regions according to splitPoints and pair up adjacent regions to the
   * same server. Each region pair in ([unbound:1,1:3], [3:5,5:7], [7:9,9:unbound])
   * will be on the same server.
   * The table must have data loaded.  We attempt to split even already partially split
   * tables in order to facilitate recovery from partial transient failures.
   */
  public void performAssigment(String tableName) throws IOException,
    InterruptedException, TableNotFoundException {
    TableName table = TableName.valueOf(tableName);
    if (!admin.tableExists(table)) {
      throw new TableNotFoundException("Table " + tableName + " not found.");
    }
    if (admin.getRegions(table).size() <= splitPoints.length) {
      // Split into regions
      // The table has one region only to begin with. The logic of
      // blockUntilRegionSplit requires that the input regionName has performed a split.
      // If the table has already been split (i.e. regions count > 1), the same split
      // call will be a no-op and this will cause blockUntilRegionSplit to break.  In
      // that case, swallow the resulting exception. Other exceptions will be re-thrown.
      for (int i = 0; i < splitPoints.length; ++i) {
        admin.majorCompact(table);
        List<RegionInfo> regions = admin.getRegions(table);
        RegionInfo splitRegion = regions.get(regions.size() - 1);
        boolean done = false;
        int attempt = 0;
        for (; !done && attempt < MAX_SPLIT_ATTEMPTS; ++attempt) {
          // HBase seems to not always properly receive/process this split RPC,
          // so we need to retry the split/block several times.
          try {
            admin.split(splitRegion.getTable(), Bytes.toBytes(splitPoints[i]));
            done = blockUntilRegionSplit(conf, WAIT_FOR_SPLIT_TIMEOUT,
               splitRegion.getRegionName(), true);
          } catch (IOException ex) {
            if (!ex.getMessage().equals(
                "should not give a splitkey which equals to startkey!")) {
              throw ex;
            }
            done = true;
          }
        }
        if (!done) {
          throw new IllegalStateException(
              String.format("Failed to split region '%s' after %s attempts (%d ms).",
                  splitRegion.getRegionNameAsString(), MAX_SPLIT_ATTEMPTS,
                  MAX_SPLIT_ATTEMPTS * WAIT_FOR_SPLIT_TIMEOUT));
        }
        LOG.info(String.format("Split region '%s' after %s attempts.",
            splitRegion.getRegionNameAsString(), attempt));
      }
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
        LOG.info(regionName + " " + printKey(regionInfo.getStartKey()) + " -> " +
            serverName.getAddress().toString() + ", expecting " +
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

  /**
   * Returns non-printable characters in escaped octal, otherwise returns the characters.
   */
  public static String printKey(byte[] key) {
    StringBuilder result = new StringBuilder();
    for (int i = 0; i < key.length; ++i) {
      if (!Character.isISOControl(key[i])) {
        result.append((char) key[i]);
      } else {
        result.append("\\");
        result.append(Integer.toOctalString(key[i]));
      }
    }
    return result.toString();
  }

  /**
   * The following static methods blockUntilRegionSplit, blockUntilRegionIsOpened
   * and blockUntilRegionIsInMeta are copied from
   * org.apache.hadoop.hbase.regionserver.TestEndToEndSplitTransaction
   * to help block until a region split is completed.
   *
   * The original code was modified to return a true/false in case of success/failure.
   *
   * Blocks until the region split is complete in META and region server opens the
   * daughters
   */
  private static boolean blockUntilRegionSplit(Configuration conf, long timeout,
      final byte[] regionName, boolean waitForDaughters)
      throws IOException, InterruptedException {
    long start = System.currentTimeMillis();
    RegionInfo daughterA = null, daughterB = null;
    try (Connection conn = ConnectionFactory.createConnection(conf);
        Table metaTable = conn.getTable(TableName.META_TABLE_NAME)) {
      Result result = null;
      while ((System.currentTimeMillis() - start) < timeout) {
        result = metaTable.get(new Get(regionName));
        if (result == null) {
          break;
        }
        RegionInfo region = MetaTableAccessor.getRegionInfo(result);
        if (region.isSplitParent()) {
          PairOfSameType<RegionInfo> pair = MetaTableAccessor.getDaughterRegions(result);
          daughterA = pair.getFirst();
          daughterB = pair.getSecond();
          break;
        }
        Threads.sleep(100);
      }
      if (daughterA == null || daughterB == null) return false;

      //if we are here, this means the region split is complete or timed out
      if (waitForDaughters) {
        long rem = timeout - (System.currentTimeMillis() - start);
        blockUntilRegionIsInMeta(conn, rem, daughterA);

        rem = timeout - (System.currentTimeMillis() - start);
        blockUntilRegionIsInMeta(conn, rem, daughterB);

        rem = timeout - (System.currentTimeMillis() - start);
        blockUntilRegionIsOpened(conf, rem, daughterA);

        rem = timeout - (System.currentTimeMillis() - start);
        blockUntilRegionIsOpened(conf, rem, daughterB);
      }
    }
    return true;
  }

  private static void blockUntilRegionIsInMeta(Connection conn, long timeout,
      RegionInfo hri) throws IOException, InterruptedException {
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < timeout) {
      HRegionLocation loc = MetaTableAccessor.getRegionLocation(conn, hri);
      if (loc != null && !loc.getRegion().isOffline()) {
        break;
      }
      Threads.sleep(10);
    }
  }

  /**
  * Starting with HBase 0.95.2 the Get class' c'tor no longer accepts
  * empty key strings leading to the rather undesirable behavior that this method
  * is not guaranteed to succeed. This method repeatedly attempts to 'get' the start key
  * of the given region from the region server to detect when the region server becomes
  * available. However, the first region has an empty array as the start key causing the
  * Get c'tor to throw an exception as stated above. The end key cannot be used instead
  * because it is an exclusive upper bound.
  */
  private static void blockUntilRegionIsOpened(Configuration conf, long timeout,
      RegionInfo hri) throws IOException, InterruptedException {
    long start = System.currentTimeMillis();
    try (Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(hri.getTable())) {
      byte [] row = hri.getStartKey();
      // Check for null/empty row. If we find one, use a key that is likely to
      // be in first region. If key '0' happens not to be in the given region
      // then an exception will be thrown.
      if (row == null || row.length <= 0) row = new byte [] {'0'};
      Get get = new Get(row);
      while (System.currentTimeMillis() - start < timeout) {
        try {
          table.get(get);
          break;
        } catch(IOException ex) {
          //wait some more
        }
        Threads.sleep(10);
      }
    }
  }

  /**
   * args contains a list of hbase table names. This program will split the hbase tables
   * into regions and assign each region to a specific region server.
   */
  public static void main(String args[]) throws IOException, InterruptedException,
    TableNotFoundException {
    HBaseTestDataRegionAssigment assignment = new HBaseTestDataRegionAssigment();
    for (String htable: args) {
      assignment.performAssigment(htable);
    }
    assignment.close();
    // Exit forcefully because of HDFS-6057. Otherwise, there the JVM won't exit due to a
    // non-daemon thread still being up.
    System.exit(0);
  }
}

