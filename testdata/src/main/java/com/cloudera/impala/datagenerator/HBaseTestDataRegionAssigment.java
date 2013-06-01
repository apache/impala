// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.datagenerator;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
import java.util.Collection;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.NavigableMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.util.Merge;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.catalog.MetaEditor;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.MetaScanner;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.PairOfSameType;
import org.apache.hadoop.hbase.util.Threads;

import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private final HBaseAdmin hbaseAdmin;
  private final List<ServerName> sortedRS; // sorted list of region server name
  private final String[] splitPoints = { "1", "3", "5", "7", "9"};

  public HBaseTestDataRegionAssigment() throws IOException {
    conf = new Configuration();
    hbaseAdmin = new HBaseAdmin(conf);
    ClusterStatus clusterStatus = hbaseAdmin.getClusterStatus();
    Collection<ServerName> regionServerNames = clusterStatus.getServers();
    sortedRS = new ArrayList<ServerName>(regionServerNames);
    Collections.sort(sortedRS);
  }
  
  /**
   * Split the table regions according to splitPoints and pair up adjacent regions to the
   * same server. Each region pair in ([unbound:1,1:3], [3:5,5:7], [7:9,9:unbound])
   * will be on the same server.
   * The table must have data loaded and only a single region.
   */
  public void performAssigment(String tableName) throws IOException, 
    InterruptedException, TableNotFoundException {
    HTableDescriptor[] desc = hbaseAdmin.listTables(tableName);
    if (desc == null || desc.length == 0) {
      throw new TableNotFoundException("Table " + tableName + " not found.");
    }

    // Split into regions
    // The table should have one region only to begin with. The logic of
    // blockUntilRegionSplit requires that the input regionName has performed a split. 
    // If the table has already been split (i.e. regions count > 1), the same split
    // call will be a no-op and this will cause blockUntilRegionSplit to break.
    Preconditions.checkArgument(
        hbaseAdmin.getTableRegions(tableName.getBytes()).size() == 1);    
    for (int i = 0; i < splitPoints.length; ++i) {
      List<HRegionInfo> regions = hbaseAdmin.getTableRegions(tableName.getBytes());
      HRegionInfo splitRegion = regions.get(regions.size() - 1);
      hbaseAdmin.split(splitRegion.getRegionNameAsString(), splitPoints[i]);
      blockUntilRegionSplit(conf, 50000, splitRegion.getRegionName(), true);
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
    while(!expectedLocs.equals(hbaseTable.getRegionLocations()) &&
        sleepCnt < 100) {
      Thread.sleep(100);
      ++sleepCnt;
    }
    NavigableMap<HRegionInfo, ServerName> actualLocs = 
        (new HTable(conf, tableName)).getRegionLocations();
    Preconditions.checkArgument(expectedLocs.equals(actualLocs));
    
    // Log the actual region location map
    for (Map.Entry<HRegionInfo, ServerName> entry: actualLocs.entrySet()) {
      LOG.info(printKey(entry.getKey().getStartKey()) + " -> " +
          entry.getValue().getHostAndPort());
    }
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
   * The following static methods blockUntilRegionSplit, getRegionRow,
   * blockUntilRegionIsOpened and blockUntilRegionIsInMeta are copied from 
   * org.apache.hadoop.hbase.regionserver.TestEndToEndSplitTransaction
   * to help block until a region split is completed.
   * 
   * Blocks until the region split is complete in META and region server opens the 
   * daughters
   */
  private static void blockUntilRegionSplit(Configuration conf, long timeout,
      final byte[] regionName, boolean waitForDaughters)
      throws IOException, InterruptedException {
    long start = System.currentTimeMillis();
    HRegionInfo daughterA = null, daughterB = null;
    HTable metaTable = new HTable(conf, HConstants.META_TABLE_NAME);

    try {
      while (System.currentTimeMillis() - start < timeout) {
        Result result = getRegionRow(metaTable, regionName);
        if (result == null) {
          break;
        }

        HRegionInfo region = MetaReader.parseCatalogResult(result).getFirst();
        if(region.isSplitParent()) {
          PairOfSameType<HRegionInfo> pair = MetaReader.getDaughterRegions(result);
          daughterA = pair.getFirst();
          daughterB = pair.getSecond();
          break;
        }
        Threads.sleep(100);
      }

      //if we are here, this means the region split is complete or timed out
      if (waitForDaughters) {
        long rem = timeout - (System.currentTimeMillis() - start);
        blockUntilRegionIsInMeta(metaTable, rem, daughterA);

        rem = timeout - (System.currentTimeMillis() - start);
        blockUntilRegionIsInMeta(metaTable, rem, daughterB);

        rem = timeout - (System.currentTimeMillis() - start);
        blockUntilRegionIsOpened(conf, rem, daughterA);

        rem = timeout - (System.currentTimeMillis() - start);
        blockUntilRegionIsOpened(conf, rem, daughterB);
      }
    } finally {
      IOUtils.closeQuietly(metaTable);
    }
  }
  
  private static Result getRegionRow(HTable metaTable, byte[] regionName)
      throws IOException {
    Get get = new Get(regionName);
    return metaTable.get(get);
  }
  
  private static void blockUntilRegionIsOpened(Configuration conf, long timeout,
      HRegionInfo hri) throws IOException, InterruptedException {
    long start = System.currentTimeMillis();
    HTable table = new HTable(conf, hri.getTableName());

    try {
      Get get = new Get(hri.getStartKey());
      while (System.currentTimeMillis() - start < timeout) {
        try {
          table.get(get);
          break;
        } catch(IOException ex) {
          //wait some more
        }
        Threads.sleep(10);
      }
    } finally {
      IOUtils.closeQuietly(table);
    }
  }

  private static void blockUntilRegionIsInMeta(HTable metaTable, long timeout,
      HRegionInfo hri) throws IOException, InterruptedException {
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < timeout) {
      Result result = getRegionRow(metaTable, hri.getRegionName());
      if (result != null) {
        HRegionInfo info = MetaReader.parseCatalogResult(result).getFirst();
        if (info != null && !info.isOffline()) {
          break;
        }
      }
      Threads.sleep(10);
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
  }
}

