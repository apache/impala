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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.impala.common.Pair;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.THBaseTable;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.thrift.TResultSetMetadata;
import org.apache.impala.util.StatsHelper;
import org.apache.impala.util.TResultRowBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;

public interface FeHBaseTable extends FeTable {
  /**
   * @see Util#getEstimatedRowStats(byte[], byte[])
   */
  Pair<Long, Long> getEstimatedRowStats(byte[] startRowKey, byte[] endRowKey);

  /**
   * @see Util#getHBaseTableName(Table)
   */
  String getHBaseTableName();

  /**
   * @see Util#getTableStats(FeHBaseTable)
   */
  TResultSet getTableStats();

  /**
   * Implementations may want to cache column families. This getter is for static
   * functions in {@link Util} to access those potentially cached data.
   */
  HColumnDescriptor[] getColumnFamilies() throws IOException;

  /**
   * Utility functions for acting on FeHBaseTable.
   * When we fully move to Java 8, these can become default methods of the interface.
   */
  abstract class Util {
    // Storage handler class for HBase tables read by Hive.
    public static final String HBASE_STORAGE_HANDLER =
        "org.apache.hadoop.hive.hbase.HBaseStorageHandler";
    // Column family of HBase row key
    public static final String ROW_KEY_COLUMN_FAMILY = ":key";
    // Copied from Hive's HBaseStorageHandler.java.
    static final String DEFAULT_PREFIX = "default.";
    // Number of rows fetched during the row count estimation per region
    static final int ROW_COUNT_ESTIMATE_BATCH_SIZE = 10;
    // Keep the conf around
    static final Configuration HBASE_CONF = HBaseConfiguration.create();
    private static final Logger LOG = LoggerFactory.getLogger(FeHBaseTable.class);
    // Maximum deviation from the average to stop querying more regions
    // to estimate the row count
    private static final double DELTA_FROM_AVERAGE = 0.15;
    // Minimum number of regions that are checked to estimate the row count
    private static final int MIN_NUM_REGIONS_TO_CHECK = 5;

    // constants from Hive's HBaseSerDe.java copied here to avoid dependending on
    // hive-hbase-handler (and its transitive dependencies) These are user facing
    // properties and pretty much guaranteed to not change without breaking backwards
    // compatibility. Hence it is safe to just copy them here
    private static final String HBASE_COLUMNS_MAPPING = "hbase.columns.mapping";
    private static final String HBASE_TABLE_DEFAULT_STORAGE_TYPE =
        "hbase.table.default.storage.type";
    private static final String HBASE_KEY_COL = ":key";
    private static final String HBASE_TABLE_NAME = "hbase.table.name";

    /**
     * Table client objects are thread-unsafe and cheap to create. The HBase docs
     * recommend creating a new one for each task and then closing when done.
     */
    public static org.apache.hadoop.hbase.client.Table getHBaseTable(
        String hbaseTableName) throws IOException {
      return ConnectionHolder.getConnection().getTable(TableName.valueOf(hbaseTableName));
    }

    static org.apache.hadoop.hbase.client.Table getHBaseTable(FeHBaseTable tbl)
        throws IOException {
      return getHBaseTable(tbl.getHBaseTableName());
    }

    /**
     * Load columns from msTable in hive order. No IO is involved.
     */
    public static List<Column> loadColumns(
        org.apache.hadoop.hive.metastore.api.Table msTable)
        throws MetaException, SerDeException {
      Map<String, String> serdeParams = msTable.getSd().getSerdeInfo().getParameters();
      String hbaseColumnsMapping = serdeParams.get(HBASE_COLUMNS_MAPPING);
      if (hbaseColumnsMapping == null) {
        throw new MetaException("No hbase.columns.mapping defined in Serde.");
      }

      String hbaseTableDefaultStorageType =
          msTable.getParameters().get(HBASE_TABLE_DEFAULT_STORAGE_TYPE);
      boolean tableDefaultStorageIsBinary = false;
      if (hbaseTableDefaultStorageType != null &&
          !hbaseTableDefaultStorageType.isEmpty()) {
        if (hbaseTableDefaultStorageType.equalsIgnoreCase("binary")) {
          tableDefaultStorageIsBinary = true;
        } else if (!hbaseTableDefaultStorageType.equalsIgnoreCase("string")) {
          throw new SerDeException(
              "Error: " + HBASE_TABLE_DEFAULT_STORAGE_TYPE +
                  " parameter must be specified as" + " 'string' or 'binary'; '" +
                  hbaseTableDefaultStorageType +
                  "' is not a valid specification for this table/serde property.");
        }
      }

      // Parse HBase column-mapping string.
      List<FieldSchema> fieldSchemas = msTable.getSd().getCols();
      List<String> hbaseColumnFamilies = new ArrayList<>();
      List<String> hbaseColumnQualifiers = new ArrayList<>();
      List<Boolean> hbaseColumnBinaryEncodings = new ArrayList<>();
      parseColumnMapping(tableDefaultStorageIsBinary, hbaseColumnsMapping,
          msTable.getTableName(), fieldSchemas, hbaseColumnFamilies,
          hbaseColumnQualifiers, hbaseColumnBinaryEncodings);
      Preconditions
          .checkState(hbaseColumnFamilies.size() == hbaseColumnQualifiers.size());
      Preconditions.checkState(fieldSchemas.size() == hbaseColumnFamilies.size());

      // Populate tmp cols in the order they appear in the Hive metastore.
      // We will reorder the cols below.
      List<HBaseColumn> tmpCols = new ArrayList<>();
      // Store the key column separately.
      // TODO: Change this to an ArrayList once we support composite row keys.
      HBaseColumn keyCol = null;
      for (int i = 0; i < fieldSchemas.size(); ++i) {
        FieldSchema s = fieldSchemas.get(i);
        Type t = Type.INVALID;
        try {
          t = FeCatalogUtils.parseColumnType(s, msTable.getTableName());
        } catch (TableLoadingException e) {
          // Ignore hbase types we don't support yet. We can load the metadata
          // but won't be able to select from it.
        }
        HBaseColumn col = new HBaseColumn(s.getName(), hbaseColumnFamilies.get(i),
            hbaseColumnQualifiers.get(i), hbaseColumnBinaryEncodings.get(i), t,
            s.getComment(), -1);
        if (col.getColumnFamily().equals(ROW_KEY_COLUMN_FAMILY)) {
          // Store the row key column separately from the rest
          keyCol = col;
        }
        tmpCols.add(col);
      }
      Preconditions.checkState(keyCol != null);
      List<Column> cols = new ArrayList<>();
      // Till IMPALA-886 the backend assumed that the row key column is always first and
      // that the remaining HBase columns are ordered by columnFamily,columnQualifier.
      // If flag use_hms_column_order_for_hbase_tables is false, keep the old behavor,
      // otherwise use the HMS order (similarly as Hive).
      if (BackendConfig.INSTANCE.useHmsColumnOrderForHBaseTables()) {
        for (int i = 0; i < tmpCols.size(); ++i) {
          HBaseColumn col = tmpCols.get(i);
          col.setPosition(i);
          cols.add(col);
        }
      } else {
        // Add key col as the first column.
        tmpCols.remove(keyCol);
        keyCol.setPosition(0);
        cols.add(keyCol);
        // Sort by columnFamily/columnQualifier.
        Collections.sort(tmpCols);
        // Update the positions of the remaining columns.
        for (int i = 0; i < tmpCols.size(); ++i) {
          HBaseColumn col = tmpCols.get(i);
          col.setPosition(i + 1);
          cols.add(col);
        }
      }

      return cols;
    }

    /**
     * Parse the column description string to the column families and column
     * qualifies. This is a copy of HBaseSerDe.parseColumnMapping and
     * parseColumnStorageTypes with parts we don't use removed. The hive functions
     * are not public.
     * tableDefaultStorageIsBinary - true if table is default to binary encoding
     * columnsMappingSpec - input string format describing the table
     * fieldSchemas - input field schema from metastore table
     * columnFamilies/columnQualifiers/columnBinaryEncodings - out parameters that will be
     * filled with the column family, column qualifier and encoding for each column.
     */
    static void parseColumnMapping(boolean tableDefaultStorageIsBinary,
        String columnsMappingSpec, String tblName, List<FieldSchema> fieldSchemas,
        List<String> columnFamilies, List<String> columnQualifiers,
        List<Boolean> colIsBinaryEncoded) throws SerDeException {
      if (columnsMappingSpec == null) {
        throw new SerDeException(
            "Error: hbase.columns.mapping missing for this HBase table.");
      }

      if (columnsMappingSpec.equals("") ||
          columnsMappingSpec.equals(HBASE_KEY_COL)) {
        throw new SerDeException("Error: hbase.columns.mapping specifies only " +
            "the HBase table row key. A valid Hive-HBase table must specify at " +
            "least one additional column.");
      }

      int rowKeyIndex = -1;
      String[] columnSpecs = columnsMappingSpec.split(",");
      // If there was an implicit key column mapping, the number of columns (fieldSchemas)
      // will be one more than the number of column mapping specs.
      int fsStartIdxOffset = fieldSchemas.size() - columnSpecs.length;
      if (fsStartIdxOffset != 0 && fsStartIdxOffset != 1) {
        // This should never happen - Hive blocks creating a mismatched table and both
        // Hive and Impala currently block all column-level DDL on HBase tables.
        throw new SerDeException(String.format("Number of entries in " +
                "'hbase.columns.mapping' does not match the number of columns in the " +
                "table: %d != %d (counting the key if implicit)", columnSpecs.length,
            fieldSchemas.size()));
      }

      for (int i = 0; i < columnSpecs.length; ++i) {
        String mappingSpec = columnSpecs[i];
        String[] mapInfo = mappingSpec.split("#");
        // Trim column info so that serdeproperties with new lines still parse correctly.
        String colInfo = mapInfo[0].trim();

        int idxFirst = colInfo.indexOf(":");
        int idxLast = colInfo.lastIndexOf(":");

        if (idxFirst < 0 || !(idxFirst == idxLast)) {
          throw new SerDeException("Error: the HBase columns mapping contains a " +
              "badly formed column family, column qualifier specification.");
        }

        if (colInfo.equals(HBASE_KEY_COL)) {
          Preconditions.checkState(fsStartIdxOffset == 0);
          rowKeyIndex = i;
          columnFamilies.add(colInfo);
          columnQualifiers.add(null);
        } else {
          String[] parts = colInfo.split(":");
          Preconditions.checkState(parts.length > 0 && parts.length <= 2);
          columnFamilies.add(parts[0]);
          if (parts.length == 2) {
            columnQualifiers.add(parts[1]);
          } else {
            columnQualifiers.add(null);
          }
        }

        // Set column binary encoding
        FieldSchema fieldSchema = fieldSchemas.get(i + fsStartIdxOffset);
        boolean supportsBinaryEncoding = supportsBinaryEncoding(fieldSchema, tblName);
        if (mapInfo.length == 1) {
          // There is no column level storage specification. Use the table storage spec.
          colIsBinaryEncoded.add(tableDefaultStorageIsBinary && supportsBinaryEncoding);
        } else if (mapInfo.length == 2) {
          // There is a storage specification for the column
          String storageOption = mapInfo[1];

          if (!(storageOption.equals("-") || "string".startsWith(storageOption) ||
              "binary".startsWith(storageOption))) {
            throw new SerDeException("Error: A column storage specification is one of" +
                " the following: '-', a prefix of 'string', or a prefix of 'binary'. " +
                storageOption + " is not a valid storage option specification for " +
                fieldSchema.getName());
          }

          boolean isBinaryEncoded = false;
          if ("-".equals(storageOption)) {
            isBinaryEncoded = tableDefaultStorageIsBinary;
          } else if ("binary".startsWith(storageOption)) {
            isBinaryEncoded = true;
          }
          if (isBinaryEncoded && !supportsBinaryEncoding) {
            // Use string encoding and log a warning if the column spec is binary but the
            // column type does not support it.
            // TODO: Hive/HBase does not raise an exception, but should we?
            LOG.warn("Column storage specification for column " + fieldSchema.getName() +
                " is binary" + " but the column type " + fieldSchema.getType() +
                " does not support binary encoding. Fallback to string format.");
            isBinaryEncoded = false;
          }
          colIsBinaryEncoded.add(isBinaryEncoded);
        } else {
          // error in storage specification
          throw new SerDeException(
              "Error: " + HBASE_COLUMNS_MAPPING + " storage specification " +
                  mappingSpec + " is not valid for column: " + fieldSchema.getName());
        }
      }

      if (rowKeyIndex == -1) {
        columnFamilies.add(0, HBASE_KEY_COL);
        columnQualifiers.add(0, null);
        colIsBinaryEncoded.add(0, supportsBinaryEncoding(fieldSchemas.get(0), tblName) &&
            tableDefaultStorageIsBinary);
      }
    }


    /**
     * Get an estimate of the number of rows and bytes per row in regions between
     * startRowKey and endRowKey.
     * <p>
     * This number is calculated by incrementally checking as many region servers as
     * necessary until we observe a relatively constant row size per region on average.
     * Depending on the skew of data in the regions this can either mean that we need
     * to check only a minimal number of regions or that we will scan all regions.
     * <p>
     * The HBase region servers periodically update the master with their metrics,
     * including storefile size. We get the size of the storefiles for all regions in
     * the cluster with a single call to getClusterStatus from the master.
     * <p>
     * The accuracy of this number is determined by the number of rows that are written
     * and kept in the memstore and have not been flushed until now. A large number
     * of key-value pairs in the memstore will lead to bad estimates as this number
     * is not reflected in the storefile size that is used to estimate this number.
     * <p>
     * Currently, the algorithm does not consider the case that the key range used as a
     * parameter might be generally of different size than the rest of the region.
     *
     * @param startRowKey First row key in the range
     * @param endRowKey   Last row key in the range
     * @return The estimated number of rows in the regions between the row keys (first)
     * and the estimated row size in bytes (second).
     */
    public static Pair<Long, Long> getEstimatedRowStats(FeHBaseTable tbl,
        byte[] startRowKey, byte[] endRowKey) {
      Preconditions.checkNotNull(startRowKey);
      Preconditions.checkNotNull(endRowKey);

      if (LOG.isTraceEnabled()) {
        LOG.trace("getEstimatedRowStats for {} for key range ('{}', '{}')",
            tbl.getHBaseTableName(), Bytes.toString(startRowKey),
            Bytes.toString(endRowKey));
      }
      long startTime = System.currentTimeMillis();
      boolean isCompressed = false;
      long rowCount;
      long rowSize;
      try {
        ClusterStatus clusterStatus = getClusterStatus();
        // Check to see if things are compressed.
        // If they are we'll estimate a compression factor.
        HColumnDescriptor[] columnFamilies = tbl.getColumnFamilies();
        Preconditions.checkNotNull(columnFamilies);
        for (HColumnDescriptor desc : columnFamilies) {
          isCompressed |= desc.getCompression() != Compression.Algorithm.NONE;
        }

        // Fetch all regions for the key range
        List<HRegionLocation> locations = getRegionsInRange(tbl, startRowKey, endRowKey);
        Collections.shuffle(locations);
        // The following variables track the number and size of 'rows' in
        // HBase and allow incremental calculation of the average and standard
        // deviation.
        StatsHelper<Long> statsSize = new StatsHelper<>();
        long totalEstimatedRows = 0;

        // Collects stats samples from at least MIN_NUM_REGIONS_TO_CHECK
        // and at most all regions until the delta is small enough.
        if (LOG.isTraceEnabled()) {
          LOG.trace("Start rows sampling on " + locations.size() + " regions");
        }
        while ((statsSize.count() < MIN_NUM_REGIONS_TO_CHECK ||
            statsSize.stddev() > statsSize.mean() * DELTA_FROM_AVERAGE) &&
            statsSize.count() < locations.size()) {
          HRegionLocation currentLocation = locations.get((int) statsSize.count());
          Pair<Long, Long> tmp =
              getEstimatedRowStatsForRegion(tbl, currentLocation, isCompressed,
                  clusterStatus);
          totalEstimatedRows += tmp.first;
          statsSize.addSample(tmp.second);
          if (LOG.isTraceEnabled()) {
            LOG.trace("Estimation state: totalEstimatedRows={}, statsSize.count={}, " +
                    "statsSize.stddev={}, statsSize.mean={}",
                totalEstimatedRows, statsSize.count(), statsSize.stddev(),
                statsSize.mean());
          }
        }

        // Sum up the total size for all regions in range.
        long totalSize = 0;
        for (final HRegionLocation location : locations) {
          totalSize += getRegionSize(location, clusterStatus);
        }
        if (totalSize == 0) {
          rowCount = totalEstimatedRows;
        } else if (statsSize.mean() < 1) {
          // No meaningful row width found. The < 1 handles both the
          // no row case and the potential case where the average is
          // too small to be meaningful.
          LOG.warn("Table {}: no data available to compute " +
              "row count estimate for key range ('{}', '{}')",
              tbl.getFullName(), Bytes.toString(startRowKey), Bytes.toString(endRowKey));
          return new Pair<>(-1L, -1L);
        } else {
          rowCount = (long) (totalSize / statsSize.mean());
        }
        rowSize = (long) statsSize.mean();
        if (LOG.isTraceEnabled()) {
          LOG.trace("getEstimatedRowStats results: rowCount={}, rowSize={}, " +
              "timeElapsed={}ms", rowCount, rowSize,
              System.currentTimeMillis() - startTime);
        }
        return new Pair<>(rowCount, rowSize);
      } catch (IOException ioe) {
        // Print the stack trace, but we'll ignore it
        // as this is just an estimate.
        // TODO: Put this into the per query log.
        LOG.error("Error computing HBase row count estimate", ioe);
        return new Pair<>(-1L, -1L);
      }
    }

    /**
     * Returns statistics on this table as a tabular result set. Used for the
     * SHOW TABLE STATS statement. The schema of the returned TResultSet is set
     * inside this method.
     */
    public static TResultSet getTableStats(FeHBaseTable tbl) {
      TResultSet result = new TResultSet();
      TResultSetMetadata resultSchema = new TResultSetMetadata();
      result.setSchema(resultSchema);
      resultSchema.addToColumns(new TColumn("Region Location", Type.STRING.toThrift()));
      resultSchema.addToColumns(new TColumn("Start RowKey", Type.STRING.toThrift()));
      resultSchema.addToColumns(new TColumn("Est. #Rows", Type.BIGINT.toThrift()));
      resultSchema.addToColumns(new TColumn("Size", Type.STRING.toThrift()));

      // TODO: Consider fancier stats maintenance techniques for speeding up this process.
      // Currently, we list all regions and perform a mini-scan of each of them to
      // estimate the number of rows, the data size, etc., which is rather expensive.
      try {
        ClusterStatus clusterStatus = getClusterStatus();
        long totalNumRows = 0;
        long totalSize = 0;
        List<HRegionLocation> regions =
            getRegionsInRange(tbl, HConstants.EMPTY_END_ROW, HConstants.EMPTY_START_ROW);
        for (HRegionLocation region : regions) {
          TResultRowBuilder rowBuilder = new TResultRowBuilder();
          HRegionInfo regionInfo = region.getRegionInfo();
          Pair<Long, Long> estRowStats =
              getEstimatedRowStatsForRegion(tbl, region, false, clusterStatus);

          long numRows = estRowStats.first;
          long regionSize = getRegionSize(region, clusterStatus);
          totalNumRows += numRows;
          totalSize += regionSize;

          // Add the region location, start rowkey, number of rows and raw size.
          rowBuilder.add(String.valueOf(region.getHostname()))
              .add(Bytes.toString(regionInfo.getStartKey())).add(numRows)
              .addBytes(regionSize);
          result.addToRows(rowBuilder.get());
        }

        // Total num rows and raw region size.
        if (regions.size() > 1) {
          TResultRowBuilder rowBuilder = new TResultRowBuilder();
          rowBuilder.add("Total").add("").add(totalNumRows).addBytes(totalSize);
          result.addToRows(rowBuilder.get());
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return result;
    }


    /**
     * This method is completely copied from Hive's HBaseStorageHandler.java.
     */
    public static String getHBaseTableName(
        org.apache.hadoop.hive.metastore.api.Table tbl) {
      // Give preference to TBLPROPERTIES over SERDEPROPERTIES
      // (really we should only use TBLPROPERTIES, so this is just
      // for backwards compatibility with the original specs).
      String tableName = tbl.getParameters().get(HBASE_TABLE_NAME);
      if (tableName == null) {
        tableName =
            tbl.getSd().getSerdeInfo().getParameters().get(HBASE_TABLE_NAME);
      }
      if (tableName == null) {
        tableName = tbl.getDbName() + "." + tbl.getTableName();
        if (tableName.startsWith(DEFAULT_PREFIX)) {
          tableName = tableName.substring(DEFAULT_PREFIX.length());
        }
      }
      return tableName;
    }

    /**
     * Estimates the number of rows for a single region and returns a pair with
     * the estimated row count and the estimated size in bytes per row.
     */
    private static Pair<Long, Long> getEstimatedRowStatsForRegion(FeHBaseTable tbl,
        HRegionLocation location, boolean isCompressed, ClusterStatus clusterStatus)
        throws IOException {
      HRegionInfo info = location.getRegionInfo();

      Scan s = new Scan(info.getStartKey());
      // Get a small sample of rows
      s.setBatch(ROW_COUNT_ESTIMATE_BATCH_SIZE);
      // Try and get every version so the row's size can be used to estimate.
      s.setMaxVersions(Short.MAX_VALUE);
      // Don't cache the blocks as we don't think these are
      // necessarily important blocks.
      s.setCacheBlocks(false);
      // Try and get deletes too so their size can be counted.
      s.setRaw(false);

      long currentRowSize = 0;
      long currentRowCount = 0;

      try (org.apache.hadoop.hbase.client.Table table = getHBaseTable(tbl);
          ResultScanner rs = table.getScanner(s)) {
        // Get the the ROW_COUNT_ESTIMATE_BATCH_SIZE fetched rows
        // for a representative sample
        for (int i = 0; i < ROW_COUNT_ESTIMATE_BATCH_SIZE; ++i) {
          Result r = rs.next();
          if (r == null) break;
          // Check for empty rows, see IMPALA-1451
          if (r.isEmpty()) continue;
          ++currentRowCount;
          // To estimate the number of rows we simply use the amount of bytes
          // returned from the underlying buffer. Since HBase internally works
          // with these structures as well this gives us ok estimates.
          Cell[] cells = r.rawCells();
          for (Cell c : cells) {
            if (c instanceof KeyValue) {
              currentRowSize += KeyValue
                  .getKeyValueDataStructureSize(c.getRowLength(), c.getFamilyLength(),
                      c.getQualifierLength(), c.getValueLength(), c.getTagsLength());
            } else {
              throw new IllegalStateException(
                  "Celltype " + c.getClass().getName() + " not supported.");
            }
          }
        }
      }

      // If there are no rows then no need to estimate.
      if (currentRowCount == 0) return new Pair<>(0L, 0L);
      // Get the size.
      long currentSize = getRegionSize(location, clusterStatus);
      // estimate the number of rows.
      double bytesPerRow = currentRowSize / (double) currentRowCount;
      if (currentSize == 0) {
        return new Pair<>(currentRowCount, (long) bytesPerRow);
      }

      // Compression factor two is only a best effort guess
      long estimatedRowCount =
          (long) ((isCompressed ? 2 : 1) * (currentSize / bytesPerRow));

      return new Pair<>(estimatedRowCount, (long) bytesPerRow);
    }


    /**
     * Returns the size of the given region in bytes. Simply returns the storefile size
     * for this region from the ClusterStatus. Returns 0 in case of an error.
     */
    private static long getRegionSize(HRegionLocation location,
        ClusterStatus clusterStatus) {
      HRegionInfo info = location.getRegionInfo();
      ServerLoad serverLoad = clusterStatus.getLoad(location.getServerName());

      // If the serverLoad is null, the master doesn't have information for this region's
      // server. This shouldn't normally happen.
      if (serverLoad == null) {
        LOG.error("Unable to find server load for server: " + location.getServerName() +
            " for location " + info.getRegionNameAsString());
        return 0;
      }
      RegionLoad regionLoad = serverLoad.getRegionsLoad().get(info.getRegionName());
      if (regionLoad == null) {
        LOG.error("Unable to find regions load for server: " + location.getServerName() +
            " for location " + info.getRegionNameAsString());
        return 0;
      }
      final long megaByte = 1024L * 1024L;
      return regionLoad.getStorefileSizeMB() * megaByte;
    }

    public static THBaseTable getTHBaseTable(FeHBaseTable table) {
      THBaseTable tHbaseTable = new THBaseTable();
      tHbaseTable.setTableName(table.getHBaseTableName());
      for (Column c : table.getColumns()) {
        HBaseColumn hbaseCol = (HBaseColumn) c;
        tHbaseTable.addToFamilies(hbaseCol.getColumnFamily());
        if (hbaseCol.getColumnQualifier() != null) {
          tHbaseTable.addToQualifiers(hbaseCol.getColumnQualifier());
        } else {
          tHbaseTable.addToQualifiers("");
        }
        tHbaseTable.addToBinary_encoded(hbaseCol.isBinaryEncoded());
      }
      return tHbaseTable;
    }

    /**
     * Get the corresponding regions for an arbitrary range of keys.
     * This is copied from org.apache.hadoop.hbase.client.HTable in HBase 0.95. The
     * difference is that it does not use cache when calling getRegionLocation.
     *
     * @param tbl      An FeHBaseTable in the catalog
     * @param startKey Starting key in range, inclusive
     * @param endKey   Ending key in range, exclusive
     * @return A list of HRegionLocations corresponding to the regions that
     * contain the specified range
     * @throws IOException if a remote or network exception occurs
     */
    public static List<HRegionLocation> getRegionsInRange(FeHBaseTable tbl,
        final byte[] startKey, final byte[] endKey) throws IOException {
      long startTime = System.currentTimeMillis();
      try (org.apache.hadoop.hbase.client.Table hbaseTbl = getHBaseTable(tbl)) {
        final boolean endKeyIsEndOfTable = Bytes.equals(endKey, HConstants.EMPTY_END_ROW);
        if ((Bytes.compareTo(startKey, endKey) > 0) && !endKeyIsEndOfTable) {
          throw new IllegalArgumentException(
              "Invalid range: " + Bytes.toStringBinary(startKey) + " > " +
                  Bytes.toStringBinary(endKey));
        }
        final List<HRegionLocation> regionList = new ArrayList<>();
        byte[] currentKey = startKey;
        Connection connection = ConnectionHolder.getConnection();
        RegionLocator locator = connection.getRegionLocator(hbaseTbl.getName());
        do {
          // always reload region location info.
          HRegionLocation regionLocation = locator.getRegionLocation(currentKey, true);
          regionList.add(regionLocation);
          currentKey = regionLocation.getRegionInfo().getEndKey();
        } while (!Bytes.equals(currentKey, HConstants.EMPTY_END_ROW) &&
            (endKeyIsEndOfTable || Bytes.compareTo(currentKey, endKey) < 0));
        if (LOG.isTraceEnabled()) {
          LOG.trace("getRegionsInRange timeElapsed={}ms",
              System.currentTimeMillis() - startTime);
        }
        return regionList;
      }
    }

    /**
     * Get the cluster status, making sure we close the admin client afterwards.
     */
    static ClusterStatus getClusterStatus() throws IOException {
      try (Admin admin = ConnectionHolder.getConnection().getAdmin()) {
        return admin.getClusterStatus();
      }
    }

    private static boolean supportsBinaryEncoding(FieldSchema fs, String tblName) {
      try {
        Type colType = FeCatalogUtils.parseColumnType(fs, tblName);
        // Only boolean, integer and floating point types can use binary storage.
        return colType.isBoolean() || colType.isIntegerType() ||
            colType.isFloatingPointType();
      } catch (TableLoadingException e) {
        return false;
      }
    }

    /**
     * Connection instances are expensive to create. The HBase documentation recommends
     * one and then sharing it among threads. All operations on a connection are
     * thread-safe.
     */
    static class ConnectionHolder {
      private static Connection connection_ = null;

      static synchronized Connection getConnection() throws IOException {
        if (connection_ == null || connection_.isClosed()) {
          connection_ = ConnectionFactory.createConnection(Util.HBASE_CONF);
        }
        return connection_;
      }
    }
  }
}
