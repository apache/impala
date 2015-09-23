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

package com.cloudera.impala.catalog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hive.hbase.HBaseSerDe;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.log4j.Logger;

import com.cloudera.impala.common.Pair;
import com.cloudera.impala.thrift.TCatalogObjectType;
import com.cloudera.impala.thrift.TColumn;
import com.cloudera.impala.thrift.THBaseTable;
import com.cloudera.impala.thrift.TResultSet;
import com.cloudera.impala.thrift.TResultSetMetadata;
import com.cloudera.impala.thrift.TTable;
import com.cloudera.impala.thrift.TTableDescriptor;
import com.cloudera.impala.thrift.TTableType;
import com.cloudera.impala.util.StatsHelper;
import com.cloudera.impala.util.TResultRowBuilder;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Impala representation of HBase table metadata,
 * as loaded from Hive's metastore.
 * This implies that we inherit the metastore's limitations related to HBase,
 * for example the lack of support for composite HBase row keys.
 * We sort the HBase columns (cols) by family/qualifier
 * to simplify the retrieval logic in the backend, since
 * HBase returns data ordered by family/qualifier.
 * This implies that a "select *"-query on an HBase table
 * will not have the columns ordered as they were declared in the DDL.
 * They will be ordered by family/qualifier.
 *
 */
public class HBaseTable extends Table {
  // Maximum deviation from the average to stop querying more regions
  // to estimate the row count
  private static final double DELTA_FROM_AVERAGE = 0.15;

  private static final Logger LOG = Logger.getLogger(HBaseTable.class);

  // Copied from Hive's HBaseStorageHandler.java.
  public static final String DEFAULT_PREFIX = "default.";

  // Number of rows fetched during the row count estimation per region
  public static final int ROW_COUNT_ESTIMATE_BATCH_SIZE = 10;

  // Minimum number of regions that are checked to estimate the row count
  private static final int MIN_NUM_REGIONS_TO_CHECK = 5;

  // Column referring to HBase row key.
  // Hive (including metastore) currently doesn't support composite HBase keys.
  protected HBaseColumn rowKey_;

  // Name of table in HBase.
  // 'this.name' is the alias of the HBase table in Hive.
  protected String hbaseTableName_;

  // Input format class for HBase tables read by Hive.
  private static final String HBASE_INPUT_FORMAT =
      "org.apache.hadoop.hive.hbase.HiveHBaseTableInputFormat";

  // Serialization class for HBase tables set in the corresponding Metastore table.
  private static final String HBASE_SERIALIZATION_LIB =
      "org.apache.hadoop.hive.hbase.HBaseSerDe";

  // Storage handler class for HBase tables read by Hive.
  private static final String HBASE_STORAGE_HANDLER =
      "org.apache.hadoop.hive.hbase.HBaseStorageHandler";

  // Column family of HBase row key
  private static final String ROW_KEY_COLUMN_FAMILY = ":key";

  // Keep the conf around
  private final static Configuration hbaseConf_ = HBaseConfiguration.create();

  // Cached column families. Used primarily for speeding up row stats estimation
  // (see CDH-19292).
  private HColumnDescriptor[] columnFamilies_ = null;

  protected HBaseTable(TableId id, org.apache.hadoop.hive.metastore.api.Table msTbl,
      Db db, String name, String owner) {
    super(id, msTbl, db, name, owner);
  }

  /**
   * Connection instances are expensive to create. The HBase documentation recommends
   * one and then sharing it among threads. All operations on a connection are
   * thread-safe.
   */
  private static class ConnectionHolder {
    private static Connection connection_ = null;

    public static synchronized Connection getConnection(Configuration conf)
        throws IOException {
      if (connection_ == null || connection_.isClosed()) {
        connection_ = ConnectionFactory.createConnection(conf);
      }
      return connection_;
    }
  }

  /**
   * Table client objects are thread-unsafe and cheap to create. The HBase docs recommend
   * creating a new one for each task and then closing when done.
   */
  public org.apache.hadoop.hbase.client.Table getHBaseTable() throws IOException {
    return ConnectionHolder.getConnection(hbaseConf_)
        .getTable(TableName.valueOf(hbaseTableName_));
  }

  private void closeHBaseTable(org.apache.hadoop.hbase.client.Table table) {
    try {
      table.close();
    } catch (IOException e) {
      LOG.error("Error closing HBase table: " + hbaseTableName_, e);
    }
  }

  /**
   * Get the cluster status, making sure we close the admin client afterwards.
   */
  public ClusterStatus getClusterStatus() throws IOException {
    Admin admin = null;
    ClusterStatus clusterStatus = null;
    try {
      Connection connection = ConnectionHolder.getConnection(hbaseConf_);
      admin = connection.getAdmin();
      clusterStatus = admin.getClusterStatus();
    } finally {
      admin.close();
    }
    return clusterStatus;
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
  private void parseColumnMapping(boolean tableDefaultStorageIsBinary,
      String columnsMappingSpec, List<FieldSchema> fieldSchemas,
      List<String> columnFamilies, List<String> columnQualifiers,
      List<Boolean> colIsBinaryEncoded) throws SerDeException {
    if (columnsMappingSpec == null) {
      throw new SerDeException(
          "Error: hbase.columns.mapping missing for this HBase table.");
    }

    if (columnsMappingSpec.equals("") ||
        columnsMappingSpec.equals(HBaseSerDe.HBASE_KEY_COL)) {
      throw new SerDeException("Error: hbase.columns.mapping specifies only "
          + "the HBase table row key. A valid Hive-HBase table must specify at "
          + "least one additional column.");
    }

    int rowKeyIndex = -1;
    String[] columnSpecs = columnsMappingSpec.split(",");
    // If there was an implicit key column mapping, the number of columns (fieldSchemas)
    // will be one more than the number of column mapping specs.
    int fsStartIdxOffset = fieldSchemas.size() - columnSpecs.length;
    if (fsStartIdxOffset != 0 && fsStartIdxOffset != 1) {
      // This should never happen - Hive blocks creating a mismatched table and both Hive
      // and Impala currently block all column-level DDL on HBase tables.
      throw new SerDeException(String.format("Number of entries in " +
          "'hbase.columns.mapping' does not match the number of columns in the " +
          "table: %d != %d (counting the key if implicit)",
          columnSpecs.length, fieldSchemas.size()));
    }

    for (int i = 0; i < columnSpecs.length; ++i) {
      String mappingSpec = columnSpecs[i];
      String[] mapInfo = mappingSpec.split("#");
      // Trim column info so that serdeproperties with new lines still parse correctly.
      String colInfo = mapInfo[0].trim();

      int idxFirst = colInfo.indexOf(":");
      int idxLast = colInfo.lastIndexOf(":");

      if (idxFirst < 0 || !(idxFirst == idxLast)) {
        throw new SerDeException("Error: the HBase columns mapping contains a "
            + "badly formed column family, column qualifier specification.");
      }

      if (colInfo.equals(HBaseSerDe.HBASE_KEY_COL)) {
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
      boolean supportsBinaryEncoding = supportsBinaryEncoding(fieldSchema);
      if (mapInfo.length == 1) {
        // There is no column level storage specification. Use the table storage spec.
        colIsBinaryEncoded.add(
            new Boolean(tableDefaultStorageIsBinary && supportsBinaryEncoding));
      } else if (mapInfo.length == 2) {
        // There is a storage specification for the column
        String storageOption = mapInfo[1];

        if (!(storageOption.equals("-") || "string".startsWith(storageOption) || "binary"
            .startsWith(storageOption))) {
          throw new SerDeException("Error: A column storage specification is one of"
              + " the following: '-', a prefix of 'string', or a prefix of 'binary'. "
              + storageOption + " is not a valid storage option specification for "
              + fieldSchema.getName());
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
          LOG.warn("Column storage specification for column " + fieldSchema.getName()
              + " is binary" + " but the column type " + fieldSchema.getType() +
              " does not support binary encoding. Fallback to string format.");
          isBinaryEncoded = false;
        }
        colIsBinaryEncoded.add(isBinaryEncoded);
      } else {
        // error in storage specification
        throw new SerDeException("Error: " + HBaseSerDe.HBASE_COLUMNS_MAPPING
            + " storage specification " + mappingSpec + " is not valid for column: "
            + fieldSchema.getName());
      }
    }

    if (rowKeyIndex == -1) {
      columnFamilies.add(0, HBaseSerDe.HBASE_KEY_COL);
      columnQualifiers.add(0, null);
      colIsBinaryEncoded.add(0,
          supportsBinaryEncoding(fieldSchemas.get(0)) && tableDefaultStorageIsBinary);
    }
  }

  private boolean supportsBinaryEncoding(FieldSchema fs) {
    try {
      Type colType = parseColumnType(fs);
      // Only boolean, integer and floating point types can use binary storage.
      return colType.isBoolean() || colType.isIntegerType()
          || colType.isFloatingPointType();
    } catch (TableLoadingException e) {
      return false;
    }
  }

  @Override
  /**
   * For hbase tables, we can support tables with columns we don't understand at
   * all (e.g. map) as long as the user does not select those. This is in contrast
   * to hdfs tables since we typically need to understand all columns to make sense
   * of the file at all.
   */
  public void load(Table oldValue, HiveMetaStoreClient client,
      org.apache.hadoop.hive.metastore.api.Table msTbl) throws TableLoadingException {
    Preconditions.checkNotNull(getMetaStoreTable());
    try {
      hbaseTableName_ = getHBaseTableName(getMetaStoreTable());
      // Warm up the connection and verify the table exists.
      getHBaseTable().close();
      columnFamilies_ = null;
      Map<String, String> serdeParams =
          getMetaStoreTable().getSd().getSerdeInfo().getParameters();
      String hbaseColumnsMapping = serdeParams.get(HBaseSerDe.HBASE_COLUMNS_MAPPING);
      if (hbaseColumnsMapping == null) {
        throw new MetaException("No hbase.columns.mapping defined in Serde.");
      }

      String hbaseTableDefaultStorageType = getMetaStoreTable().getParameters().get(
          HBaseSerDe.HBASE_TABLE_DEFAULT_STORAGE_TYPE);
      boolean tableDefaultStorageIsBinary = false;
      if (hbaseTableDefaultStorageType != null &&
          !hbaseTableDefaultStorageType.isEmpty()) {
        if (hbaseTableDefaultStorageType.equalsIgnoreCase("binary")) {
          tableDefaultStorageIsBinary = true;
        } else if (!hbaseTableDefaultStorageType.equalsIgnoreCase("string")) {
          throw new SerDeException("Error: " +
              HBaseSerDe.HBASE_TABLE_DEFAULT_STORAGE_TYPE +
              " parameter must be specified as" +
              " 'string' or 'binary'; '" + hbaseTableDefaultStorageType +
              "' is not a valid specification for this table/serde property.");
        }
      }

      // Parse HBase column-mapping string.
      List<FieldSchema> fieldSchemas = getMetaStoreTable().getSd().getCols();
      List<String> hbaseColumnFamilies = new ArrayList<String>();
      List<String> hbaseColumnQualifiers = new ArrayList<String>();
      List<Boolean> hbaseColumnBinaryEncodings = new ArrayList<Boolean>();
      parseColumnMapping(tableDefaultStorageIsBinary, hbaseColumnsMapping, fieldSchemas,
          hbaseColumnFamilies, hbaseColumnQualifiers, hbaseColumnBinaryEncodings);
      Preconditions.checkState(
          hbaseColumnFamilies.size() == hbaseColumnQualifiers.size());
      Preconditions.checkState(fieldSchemas.size() == hbaseColumnFamilies.size());

      // Populate tmp cols in the order they appear in the Hive metastore.
      // We will reorder the cols below.
      List<HBaseColumn> tmpCols = Lists.newArrayList();
      // Store the key column separately.
      // TODO: Change this to an ArrayList once we support composite row keys.
      HBaseColumn keyCol = null;
      for (int i = 0; i < fieldSchemas.size(); ++i) {
        FieldSchema s = fieldSchemas.get(i);
        Type t = Type.INVALID;
        try {
          t = parseColumnType(s);
        } catch (TableLoadingException e) {
          // Ignore hbase types we don't support yet. We can load the metadata
          // but won't be able to select from it.
        }
        HBaseColumn col = new HBaseColumn(s.getName(), hbaseColumnFamilies.get(i),
            hbaseColumnQualifiers.get(i), hbaseColumnBinaryEncodings.get(i),
            t, s.getComment(), -1);
        if (col.getColumnFamily().equals(ROW_KEY_COLUMN_FAMILY)) {
          // Store the row key column separately from the rest
          keyCol = col;
        } else {
          tmpCols.add(col);
        }
      }
      Preconditions.checkState(keyCol != null);

      // The backend assumes that the row key column is always first and
      // that the remaining HBase columns are ordered by columnFamily,columnQualifier,
      // so the final position depends on the other mapped HBase columns.
      // Sort columns and update positions.
      Collections.sort(tmpCols);
      clearColumns();

      keyCol.setPosition(0);
      addColumn(keyCol);
      // Update the positions of the remaining columns
      for (int i = 0; i < tmpCols.size(); ++i) {
        HBaseColumn col = tmpCols.get(i);
        col.setPosition(i + 1);
        addColumn(col);
      }

      // Set table stats.
      numRows_ = getRowCount(super.getMetaStoreTable().getParameters());

      // since we don't support composite hbase rowkeys yet, all hbase tables have a
      // single clustering col
      numClusteringCols_ = 1;
      loadAllColumnStats(client);
    } catch (Exception e) {
      throw new TableLoadingException("Failed to load metadata for HBase table: " +
          name_, e);
    }
  }

  @Override
  protected void loadFromThrift(TTable table) throws TableLoadingException {
    super.loadFromThrift(table);
    try {
      hbaseTableName_ = getHBaseTableName(getMetaStoreTable());
      // Warm up the connection and verify the table exists.
      getHBaseTable().close();
      columnFamilies_ = null;
    } catch (Exception e) {
      throw new TableLoadingException("Failed to load metadata for HBase table from " +
          "thrift table: " + name_, e);
    }
  }

  /**
   * This method is completely copied from Hive's HBaseStorageHandler.java.
   */
  private String getHBaseTableName(org.apache.hadoop.hive.metastore.api.Table tbl) {
    // Give preference to TBLPROPERTIES over SERDEPROPERTIES
    // (really we should only use TBLPROPERTIES, so this is just
    // for backwards compatibility with the original specs).
    String tableName = tbl.getParameters().get(HBaseSerDe.HBASE_TABLE_NAME);
    if (tableName == null) {
      tableName = tbl.getSd().getSerdeInfo().getParameters().get(
          HBaseSerDe.HBASE_TABLE_NAME);
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
  private Pair<Long, Long> getEstimatedRowStatsForRegion(HRegionLocation location,
      boolean isCompressed, ClusterStatus clusterStatus) throws IOException {
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

    org.apache.hadoop.hbase.client.Table table = getHBaseTable();
    ResultScanner rs = table.getScanner(s);

    long currentRowSize = 0;
    long currentRowCount = 0;

    try {
      // Get the the ROW_COUNT_ESTIMATE_BATCH_SIZE fetched rows
      // for a representative sample
      for (int i = 0; i < ROW_COUNT_ESTIMATE_BATCH_SIZE; ++i) {
        Result r = rs.next();
        if (r == null)
          break;
        // Check for empty rows, see IMPALA-1451
        if (r.isEmpty())
          continue;
        ++currentRowCount;
        // To estimate the number of rows we simply use the amount of bytes
        // returned from the underlying buffer. Since HBase internally works
        // with these structures as well this gives us ok estimates.
        Cell[] cells = r.rawCells();
        for (Cell c : cells) {
          if (c instanceof KeyValue) {
            currentRowSize += KeyValue.getKeyValueDataStructureSize(c.getRowLength(),
                c.getFamilyLength(), c.getQualifierLength(), c.getValueLength(),
                c.getTagsLength());
          } else {
            throw new IllegalStateException("Celltype " + c.getClass().getName() +
                " not supported.");
          }
        }
      }
    } finally {
      rs.close();
      closeHBaseTable(table);
    }

    // If there are no rows then no need to estimate.
    if (currentRowCount == 0) return new Pair<Long, Long>(0L, 0L);
    // Get the size.
    long currentSize = getRegionSize(location, clusterStatus);
    // estimate the number of rows.
    double bytesPerRow = currentRowSize / (double) currentRowCount;
    if (currentSize == 0) {
      return new Pair<Long, Long>(currentRowCount, (long) bytesPerRow);
    }

    // Compression factor two is only a best effort guess
    long estimatedRowCount =
        (long) ((isCompressed ? 2 : 1) * (currentSize / bytesPerRow));

    return new Pair<Long, Long>(estimatedRowCount, (long) bytesPerRow);
  }

  /**
   * Get an estimate of the number of rows and bytes per row in regions between
   * startRowKey and endRowKey.
   *
   * This number is calculated by incrementally checking as many region servers as
   * necessary until we observe a relatively constant row size per region on average.
   * Depending on the skew of data in the regions this can either mean that we need
   * to check only a minimal number of regions or that we will scan all regions.
   *
   * The HBase region servers periodically update the master with their metrics,
   * including storefile size. We get the size of the storefiles for all regions in
   * the cluster with a single call to getClusterStatus from the master.
   *
   * The accuracy of this number is determined by the number of rows that are written
   * and kept in the memstore and have not been flushed until now. A large number
   * of key-value pairs in the memstore will lead to bad estimates as this number
   * is not reflected in the storefile size that is used to estimate this number.
   *
   * Currently, the algorithm does not consider the case that the key range used as a
   * parameter might be generally of different size than the rest of the region.
   *
   * The values computed here should be cached so that in high qps workloads
   * the nn is not overwhelmed. Could be done in load(); Synchronized to make
   * sure that only one thread at a time is using the htable.
   *
   * @param startRowKey
   *          First row key in the range
   * @param endRowKey
   *          Last row key in the range
   * @return The estimated number of rows in the regions between the row keys (first) and
   *         the estimated row size in bytes (second).
   */
  public synchronized Pair<Long, Long> getEstimatedRowStats(byte[] startRowKey,
      byte[] endRowKey) {
    Preconditions.checkNotNull(startRowKey);
    Preconditions.checkNotNull(endRowKey);

    boolean isCompressed = false;
    long rowCount = 0;
    long rowSize = 0;

    org.apache.hadoop.hbase.client.Table table = null;
    try {
      table = getHBaseTable();
      ClusterStatus clusterStatus = getClusterStatus();

      // Check to see if things are compressed.
      // If they are we'll estimate a compression factor.
      if (columnFamilies_ == null) {
        columnFamilies_ = table.getTableDescriptor().getColumnFamilies();
      }
      Preconditions.checkNotNull(columnFamilies_);
      for (HColumnDescriptor desc : columnFamilies_) {
        isCompressed |= desc.getCompression() !=  Compression.Algorithm.NONE;
      }

      // Fetch all regions for the key range
      List<HRegionLocation> locations = getRegionsInRange(table, startRowKey, endRowKey);
      Collections.shuffle(locations);
      // The following variables track the number and size of 'rows' in
      // HBase and allow incremental calculation of the average and standard
      // deviation.
      StatsHelper<Long> statsSize = new StatsHelper<Long>();
      long totalEstimatedRows = 0;

      // Collects stats samples from at least MIN_NUM_REGIONS_TO_CHECK
      // and at most all regions until the delta is small enough.
      while ((statsSize.count() < MIN_NUM_REGIONS_TO_CHECK ||
          statsSize.stddev() > statsSize.mean() * DELTA_FROM_AVERAGE) &&
          statsSize.count() < locations.size()) {
        HRegionLocation currentLocation = locations.get((int) statsSize.count());
        Pair<Long, Long> tmp = getEstimatedRowStatsForRegion(currentLocation,
            isCompressed, clusterStatus);
        totalEstimatedRows += tmp.first;
        statsSize.addSample(tmp.second);
      }

      // Sum up the total size for all regions in range.
      long totalSize = 0;
      for (final HRegionLocation location : locations) {
        totalSize += getRegionSize(location, clusterStatus);
      }
      if (totalSize == 0) {
        rowCount = totalEstimatedRows;
      } else {
        rowCount = (long) (totalSize / statsSize.mean());
      }
      rowSize = (long) statsSize.mean();
    } catch (IOException ioe) {
      // Print the stack trace, but we'll ignore it
      // as this is just an estimate.
      // TODO: Put this into the per query log.
      LOG.error("Error computing HBase row count estimate", ioe);
      return new Pair<Long, Long>(-1l, -1l);
    } finally {
      closeHBaseTable(table);
    }
    return new Pair<Long, Long>(rowCount, rowSize);
  }

  /**
   * Returns the size of the given region in bytes. Simply returns the storefile size
   * for this region from the ClusterStatus. Returns 0 in case of an error.
   */
  public long getRegionSize(HRegionLocation location, ClusterStatus clusterStatus) {
    HRegionInfo info = location.getRegionInfo();
    ServerLoad serverLoad = clusterStatus.getLoad(location.getServerName());

    // If the serverLoad is null, the master doesn't have information for this region's
    // server. This shouldn't normally happen.
    if (serverLoad == null) {
      LOG.error("Unable to find load for server: " + location.getServerName() +
          " for location " + info.getRegionName());
      return 0;
    }
    RegionLoad regionLoad = serverLoad.getRegionsLoad().get(info.getRegionName());

    final long megaByte = 1024L * 1024L;
    return regionLoad.getStorefileSizeMB() * megaByte;
  }

  /**
   * Hive returns the columns in order of their declaration for HBase tables.
   */
  @Override
  public ArrayList<Column> getColumnsInHiveOrder() {
    return getColumns();
  }

  @Override
  public TTableDescriptor toThriftDescriptor(Set<Long> referencedPartitions) {
    TTableDescriptor tableDescriptor =
        new TTableDescriptor(id_.asInt(), TTableType.HBASE_TABLE,
            getTColumnDescriptors(), numClusteringCols_, hbaseTableName_, db_.getName());
    tableDescriptor.setHbaseTable(getTHBaseTable());
    return tableDescriptor;
  }

  public String getHBaseTableName() {
    return hbaseTableName_;
  }

  public static Configuration getHBaseConf() {
    return hbaseConf_;
  }

  public int getNumNodes() {
    // TODO: implement
    return 100;
  }

  @Override
  public TCatalogObjectType getCatalogObjectType() {
    return TCatalogObjectType.TABLE;
  }

  @Override
  public TTable toThrift() {
    TTable table = super.toThrift();
    table.setTable_type(TTableType.HBASE_TABLE);
    table.setHbase_table(getTHBaseTable());
    return table;
  }

  private THBaseTable getTHBaseTable() {
    THBaseTable tHbaseTable = new THBaseTable();
    tHbaseTable.setTableName(hbaseTableName_);
    for (Column c : getColumns()) {
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
   * This is copied from org.apache.hadoop.hbase.client.HTable. The only difference is
   * that it does not use cache when calling getRegionLocation.
   * TODO: Remove this function and use HTable.getRegionsInRange when the non-cache
   * version has been ported to CDH (DISTRO-477).
   * Get the corresponding regions for an arbitrary range of keys.
   * <p>
   *
   * @param startRow
   *          Starting row in range, inclusive
   * @param endRow
   *          Ending row in range, exclusive
   * @return A list of HRegionLocations corresponding to the regions that
   *         contain the specified range
   * @throws IOException
   *           if a remote or network exception occurs
   */
  public static List<HRegionLocation> getRegionsInRange(
      org.apache.hadoop.hbase.client.Table hbaseTbl,
      final byte[] startKey, final byte[] endKey) throws IOException {
    final boolean endKeyIsEndOfTable = Bytes.equals(endKey, HConstants.EMPTY_END_ROW);
    if ((Bytes.compareTo(startKey, endKey) > 0) && !endKeyIsEndOfTable) {
      throw new IllegalArgumentException("Invalid range: " +
          Bytes.toStringBinary(startKey) + " > " + Bytes.toStringBinary(endKey));
    }
    final List<HRegionLocation> regionList = new ArrayList<HRegionLocation>();
    byte[] currentKey = startKey;
    Connection connection = ConnectionHolder.getConnection(hbaseConf_);
    // Make sure only one thread is accessing the hbaseTbl.
    synchronized (hbaseTbl) {
      RegionLocator locator = connection.getRegionLocator(hbaseTbl.getName());
      do {
        // always reload region location info.
        HRegionLocation regionLocation = locator.getRegionLocation(currentKey, true);
        regionList.add(regionLocation);
        currentKey = regionLocation.getRegionInfo().getEndKey();
      } while (!Bytes.equals(currentKey, HConstants.EMPTY_END_ROW) &&
          (endKeyIsEndOfTable || Bytes.compareTo(currentKey, endKey) < 0));
    }
    return regionList;
  }

  /**
   * Returns the storage handler class for HBase tables read by Hive.
   */
  @Override
  public String getStorageHandlerClassName() {
    return HBASE_STORAGE_HANDLER;
  }

  /**
   * Returns statistics on this table as a tabular result set. Used for the
   * SHOW TABLE STATS statement. The schema of the returned TResultSet is set
   * inside this method.
   */
  public TResultSet getTableStats() {
    TResultSet result = new TResultSet();
    TResultSetMetadata resultSchema = new TResultSetMetadata();
    result.setSchema(resultSchema);
    resultSchema.addToColumns(
        new TColumn("Region Location", Type.STRING.toThrift()));
    resultSchema.addToColumns(new TColumn("Start RowKey",
        Type.STRING.toThrift()));
    resultSchema.addToColumns(new TColumn("Est. #Rows", Type.BIGINT.toThrift()));
    resultSchema.addToColumns(new TColumn("Size", Type.STRING.toThrift()));

    org.apache.hadoop.hbase.client.Table table;
    try {
      table = getHBaseTable();
    } catch (IOException e) {
      LOG.error("Error getting HBase table " + hbaseTableName_, e);
      throw new RuntimeException(e);
    }

    // TODO: Consider fancier stats maintenance techniques for speeding up this process.
    // Currently, we list all regions and perform a mini-scan of each of them to
    // estimate the number of rows, the data size, etc., which is rather expensive.
    try {
      ClusterStatus clusterStatus = getClusterStatus();
      long totalNumRows = 0;
      long totalSize = 0;
      List<HRegionLocation> regions = HBaseTable.getRegionsInRange(table,
          HConstants.EMPTY_END_ROW, HConstants.EMPTY_START_ROW);
      for (HRegionLocation region : regions) {
        TResultRowBuilder rowBuilder = new TResultRowBuilder();
        HRegionInfo regionInfo = region.getRegionInfo();
        Pair<Long, Long> estRowStats =
            getEstimatedRowStatsForRegion(region, false, clusterStatus);

        long numRows = estRowStats.first.longValue();
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
    } finally {
      closeHBaseTable(table);
    }
    return result;
  }

  /**
   * Returns true if the given Metastore Table represents an HBase table.
   * Versions of Hive/HBase are inconsistent which HBase related fields are set
   * (e.g., HIVE-6548 changed the input format to null).
   * For maximum compatibility consider all known fields that indicate an HBase table.
   */
  public static boolean isHBaseTable(
      org.apache.hadoop.hive.metastore.api.Table msTbl) {
    if (msTbl.getParameters() != null &&
        msTbl.getParameters().containsKey(HBASE_STORAGE_HANDLER)) {
      return true;
    }
    StorageDescriptor sd = msTbl.getSd();
    if (sd == null) return false;
    if (sd.getInputFormat() != null && sd.getInputFormat().equals(HBASE_INPUT_FORMAT)) {
      return true;
    } else if (sd.getSerdeInfo() != null &&
        sd.getSerdeInfo().getSerializationLib() != null &&
        sd.getSerdeInfo().getSerializationLib().equals(HBASE_SERIALIZATION_LIB)) {
      return true;
    }
    return false;
  }
}
