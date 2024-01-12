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

package org.apache.impala.testutil;

import static org.junit.Assert.fail;

import com.google.common.base.Preconditions;

import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.impala.catalog.ColumnStats;
import org.apache.impala.catalog.SideloadTableStats;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

/**
 * Parses JSON file containing column stats to inject during planner tests.
 */
public class StatsJsonParser {
  private final String fileName_;

  private Map<String, Map<String, SideloadTableStats>> dbStatsMap_;

  public StatsJsonParser(String fileName) {
    fileName_ = fileName;
    dbStatsMap_ = new HashMap<>();
  }

  /**
   * Get stats map obtained from parsing the stats json file.
   * Must call {@link #parseFile()} first.
   * @return the stats map obtained from parsing the stats json file.
   */
  public Map<String, Map<String, SideloadTableStats>> getDbStatsMap() {
    return dbStatsMap_;
  }

  /**
   * Initializes the scanner and the input stream corresponding to the test file name
   */
  private JSONObject open() {
    try {
      String testFileBaseDir =
          new File(System.getenv("IMPALA_HOME"), "testdata/workloads").getPath();
      File statsFile = new File(testFileBaseDir, fileName_);

      JSONParser parser = new JSONParser();
      return (JSONObject) parser.parse(new FileReader(statsFile));
    } catch (Exception e) {
      fail(e.getMessage());
      return null;
    }
  }

  /**
   * Create a ColumnStatisticsData from given columnStats object and add it into
   * tableStats. This should match the logic in {@link
   * ColumnStats#update(org.apache.impala.catalog.Type, ColumnStatisticsData)}
   */
  private void loadStatsForColumns(
      SideloadTableStats tableStats, JSONObject columnStats) {
    for (Object colKey : columnStats.keySet()) {
      String colName = (String) colKey;
      JSONObject colStats = (JSONObject) columnStats.get(colKey);
      String colType = (String) colStats.get("type");
      if (colType.contains("(")) { colType = colType.substring(0, colType.indexOf("(")); }

      ColumnStatisticsData statsData = new ColumnStatisticsData();

      switch (colType) {
        case "BOOLEAN":
          BooleanColumnStatsData boolStats =
              new BooleanColumnStatsData((long) colStats.get("numTrues"),
                  (long) colStats.get("numFalses"), (long) colStats.get("numNulls"));
          statsData.setBooleanStats(boolStats);
          break;
        case "TINYINT":
        case "SMALLINT":
        case "INT":
        case "BIGINT":
        case "TIMESTAMP": // Hive and Impala use LongColumnStatsData for timestamps.
          LongColumnStatsData longStats = new LongColumnStatsData(
              (long) colStats.get("numNulls"), (long) colStats.get("numDVs"));
          statsData.setLongStats(longStats);
          break;
        case "DATE":
          DateColumnStatsData dateStats = new DateColumnStatsData(
              (long) colStats.get("numNulls"), (long) colStats.get("numDVs"));
          statsData.setDateStats(dateStats);
          break;
        case "FLOAT":
        case "DOUBLE":
          DoubleColumnStatsData doubleStats = new DoubleColumnStatsData(
              (long) colStats.get("numNulls"), (long) colStats.get("numDVs"));
          statsData.setDoubleStats(doubleStats);
          break;
        case "CHAR":
          StringColumnStatsData charStats = new StringColumnStatsData();
          charStats.setNumNulls((long) colStats.get("numNulls"));
          charStats.setNumDVs((long) colStats.get("numDVs"));
          statsData.setStringStats(charStats);
          break;
        case "VARCHAR":
        case "STRING":
          Object strAvgField = colStats.get("avgColLen");
          Double strAvgColLen = (strAvgField instanceof Long) ?
              ((Long) strAvgField).doubleValue() :
              (Double) strAvgField;
          StringColumnStatsData stringStats =
              new StringColumnStatsData((long) colStats.get("maxColLen"), strAvgColLen,
                  (long) colStats.get("numNulls"), (long) colStats.get("numDVs"));
          statsData.setStringStats(stringStats);
          break;
        case "BINARY":
          Object binAvgField = colStats.get("avgColLen");
          Double binAvgColLen = (binAvgField instanceof Long) ?
              ((Long) binAvgField).doubleValue() :
              (Double) binAvgField;
          BinaryColumnStatsData binaryStats =
              new BinaryColumnStatsData((long) colStats.get("maxColLen"), binAvgColLen,
                  (long) colStats.get("numNulls"));
          statsData.setBinaryStats(binaryStats);
          break;
        case "DECIMAL":
          DecimalColumnStatsData decimalStats = new DecimalColumnStatsData(
              (long) colStats.get("numNulls"), (long) colStats.get("numDVs"));
          statsData.setDecimalStats(decimalStats);
          break;
        default: fail("Column type is invalid!"); break;
      }

      tableStats.addColumnStats(colName, statsData);
    }
  }

  private SideloadTableStats loadStatsForTable(String tableName, JSONObject tableStat) {
    long numRows = (long) tableStat.get("numRows");
    long totalSize = (long) tableStat.get("totalSize");
    SideloadTableStats parsedStats =
        new SideloadTableStats(tableName, numRows, totalSize);
    loadStatsForColumns(parsedStats, (JSONObject) tableStat.get("columns"));
    return parsedStats;
  }

  /**
   * Read the given stats json file into internal map.
   * The map can be obtained by calling {@link #getDbStatsMap()}.
   */
  public void parseFile() {
    JSONObject statsJson = open();
    Preconditions.checkNotNull(statsJson);
    dbStatsMap_ = new HashMap<>();
    for (Object dbKey : statsJson.keySet()) {
      String dbName = (String) dbKey;
      JSONObject tableStats = (JSONObject) statsJson.get(dbKey);
      Map<String, SideloadTableStats> tableStatsMap = new HashMap<>();
      for (Object tableKey : tableStats.keySet()) {
        String tableName = (String) tableKey;
        SideloadTableStats stats =
            loadStatsForTable(tableName, (JSONObject) tableStats.get(tableKey));
        tableStatsMap.put(tableName, stats);
      }
      dbStatsMap_.put(dbName, tableStatsMap);
    }
  }
}
