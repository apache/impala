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
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.hbase.HBaseSerDe;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hive.service.cli.thrift.TColumn;
import org.apache.log4j.Logger;

import com.cloudera.impala.common.Pair;
import com.cloudera.impala.thrift.TCatalogObjectType;
import com.cloudera.impala.thrift.THBaseTable;
import com.cloudera.impala.thrift.TPrimitiveType;
import com.cloudera.impala.thrift.TResultSetMetadata;
import com.cloudera.impala.thrift.TTable;
import com.cloudera.impala.thrift.TTableDescriptor;
import com.cloudera.impala.thrift.TTableType;
import com.cloudera.impala.util.TResultRowBuilder;
import com.google.common.base.Preconditions;

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
  private static final Logger LOG = Logger.getLogger(HBaseTable.class);
  // Copied from Hive's HBaseStorageHandler.java.
  public static final String DEFAULT_PREFIX = "default.";
  public static final int ROW_COUNT_ESTIMATE_BATCH_SIZE = 10;

  // Column referring to HBase row key.
  // Hive (including metastore) currently doesn't support composite HBase keys.
  protected HBaseColumn rowKey_;
  // Name of table in HBase.
  // 'this.name' is the alias of the HBase table in Hive.
  protected String hbaseTableName_;

  // Input format class for HBase tables read by Hive.
  private static final String HBASE_INPUT_FORMAT =
      "org.apache.hadoop.hive.hbase.HiveHBaseTableInputFormat";
  // Storage handler class for HBase tables read by Hive.
  private static final String HBASE_STORAGE_HANDLER =
      "org.apache.hadoop.hive.hbase.HBaseStorageHandler";

  // Keep the conf around
  private final static Configuration hbaseConf_ = HBaseConfiguration.create();

  private HTable hTable_ = null;

  protected HBaseTable(TableId id, org.apache.hadoop.hive.metastore.api.Table msTbl,
      Db db, String name, String owner) {
    super(id, msTbl, db, name, owner);
  }

  // Parse the column description string to the column families and column
  // qualifies.  This is a copy of HBaseSerDe.parseColumnMapping and
  // parseColumnStorageTypes with parts we don't use removed. The hive functions
  // are not public.
  //  tableDefaultStorageIsBinary - true if table is default to binary encoding
  //  columnsMappingSpec - input string format describing the table
  //  fieldSchemas - input field schema from metastore table
  //  columnFamilies/columnQualifiers/columnBinaryEncodings - out parameters that will be
  //    filled with the column family, column qualifier and encoding for each column.
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

    for (int i = 0; i < columnSpecs.length; i++) {
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
        String [] parts = colInfo.split(":");
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

        if (!(storageOption.equals("-") || "string".startsWith(storageOption) ||
            "binary".startsWith(storageOption))) {
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
    PrimitiveType colType = getPrimitiveType(fs.getType());
    // Only boolean, integer and floating point types can use binary storage.
    return colType.equals(PrimitiveType.BOOLEAN) ||
          colType.isIntegerType() || colType.isFloatingPointType();
  }

  @Override
  public void load(Table oldValue, HiveMetaStoreClient client,
      org.apache.hadoop.hive.metastore.api.Table msTbl) throws TableLoadingException {
    Preconditions.checkNotNull(getMetaStoreTable());
    try {
      hbaseTableName_ = getHBaseTableName(getMetaStoreTable());
      hTable_ = new HTable(hbaseConf_, hbaseTableName_);
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
      List<HBaseColumn> tmpCols = new ArrayList<HBaseColumn>();
      for (int i = 0; i < fieldSchemas.size(); ++i) {
        FieldSchema s = fieldSchemas.get(i);
        HBaseColumn col = new HBaseColumn(s.getName(), hbaseColumnFamilies.get(i),
            hbaseColumnQualifiers.get(i), hbaseColumnBinaryEncodings.get(i),
            getPrimitiveType(s.getType()), s.getComment(), -1);
        tmpCols.add(col);
        // Load column stats from the Hive metastore into col.
        loadColumnStats(col, client);
      }

      // HBase columns are ordered by columnFamily,columnQualifier,
      // so the final position depends on the other mapped HBase columns.
      // Sort columns and update positions.
      Collections.sort(tmpCols);
      colsByPos_.clear();
      colsByName_.clear();
      for (int i = 0; i < tmpCols.size(); ++i) {
        HBaseColumn col = tmpCols.get(i);
        col.setPosition(i);
        colsByPos_.add(col);
        colsByName_.put(col.getName(), col);
      }

      // Set table stats.
      numRows_ = getRowCount(super.getMetaStoreTable().getParameters());

      // since we don't support composite hbase rowkeys yet, all hbase tables have a
      // single clustering col
      numClusteringCols_ = 1;
    } catch (Exception e) {
      throw new TableLoadingException("Failed to load metadata for HBase table: " +
          name_, e);
    }
  }

  @Override
  public void loadFromThrift(TTable table) throws TableLoadingException {
    super.loadFromThrift(table);
    try {
      hbaseTableName_ = getHBaseTableName(getMetaStoreTable());
      hTable_ = new HTable(hbaseConf_, hbaseTableName_);
    } catch (Exception e) {
      throw new TableLoadingException("Failed to load metadata for HBase table from " +
          "thrift table: " + name_, e);
    }
  }

  // This method is completely copied from Hive's HBaseStorageHandler.java.
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
   * Get an estimate of the number of rows and bytes per row in regions between
   * startRowKey and endRowKey. The more store files there are the more this will be off.
   * Also, this does not take into account any rows that are in the memstore.
   *
   * The values computed here should be cached so that in high qps workloads
   * the nn is not overwhelmed.  Could be done in load(); Synchronized to make
   * sure that only one thread at a time is using the htable.
   *
   * @param startRowKey First row key in the range
   * @param endRowKey Last row key in the range
   * @return The estimated number of rows in the regions between the row keys (first) and
   * the estimated row size in bytes (second).
   */
  public synchronized Pair<Long, Long> getEstimatedRowStats(byte[] startRowKey,
      byte[] endRowKey) {
    Preconditions.checkNotNull(startRowKey);
    Preconditions.checkNotNull(endRowKey);

    long rowSize  = 0;
    long rowCount = 0;
    long hdfsSize = 0;
    boolean isCompressed = false;

    try {
      // Check to see if things are compressed.
      // If they are we'll estimate a compression factor.
      HColumnDescriptor[] families =
          hTable_.getTableDescriptor().getColumnFamilies();
      for (HColumnDescriptor desc: families) {
        isCompressed |= desc.getCompression() != Compression.Algorithm.NONE;
      }

      // For every region in the range.
      List<HRegionLocation> locations =
          getRegionsInRange(hTable_, startRowKey, endRowKey);
      for(HRegionLocation location: locations) {
        long currentHdfsSize = 0;
        long currentRowSize  = 0;
        long currentRowCount = 0;

        HRegionInfo info = location.getRegionInfo();
        // Get the size on hdfs
        currentHdfsSize += getHdfsSize(info);

        Scan s = new Scan(info.getStartKey());
        // Get a small sample of rows
        s.setBatch(ROW_COUNT_ESTIMATE_BATCH_SIZE);
        // Try and get every version so the row's size can be used to estimate.
        s.setMaxVersions(Short.MAX_VALUE);
        // Don't cache the blocks as we don't think these are
        // necessarily important blocks.
        s.setCacheBlocks(false);
        // Try and get deletes too so their size can be counted.
        s.setRaw(true);
        ResultScanner rs = hTable_.getScanner(s);
        try {
          // And get the the ROW_COUNT_ESTIMATE_BATCH_SIZE fetched rows
          // for a representative sample
          for (int i = 0; i < ROW_COUNT_ESTIMATE_BATCH_SIZE; i++) {
            Result r = rs.next();
            if (r == null) break;
            currentRowCount += 1;
            for (Cell c: r.list()) {
              // some extra row size added to make up for shared overhead
              currentRowSize += c.getRowLength() // row key
                  + 4 // row key length field
                  + c.getFamilyLength() // Column family bytes
                  + 4  // family length field
                  + c.getQualifierLength() // qualifier bytes
                  + 4 // qualifier length field
                  + c.getValueLength() // length of the value
                  + 4 // value length field
                  + 10; // extra overhead for hfile index, checksums, metadata, etc
            }
          }
          // add these values to the cumulative totals in one shot just
          // in case there was an error in between getting the hdfs
          // size and the row/column sizes.
          hdfsSize += currentHdfsSize;
          rowCount += currentRowCount;
          rowSize  += currentRowSize;
        } finally {
          rs.close();
        }
      }
    } catch (IOException ioe) {
      // Print the stack trace, but we'll ignore it
      // as this is just an estimate.
      // TODO: Put this into the per query log.
      LOG.error("Error computing HBase row count estimate", ioe);
    }

    // If there are no rows then no need to estimate.
    if (rowCount == 0) return new Pair<Long, Long>(0L, 0L);

    // if something went wrong then set a signal value.
    if (rowSize <= 0 || hdfsSize <= 0) return new Pair<Long, Long>(-1L, -1L);

    // estimate the number of rows.
    double bytesPerRow = rowSize / (double) rowCount;
    long estimatedRowCount = (long) ((isCompressed ? 2 : 1) * (hdfsSize / bytesPerRow));

    return new Pair<Long, Long>(estimatedRowCount, (long) bytesPerRow);
  }

  /**
   * Returns the Hdfs size of the given region in bytes.
   */
  public long getHdfsSize(HRegionInfo info) throws IOException {
    Path tableDir = HTableDescriptor.getTableDir(
        FSUtils.getRootDir(hbaseConf_), Bytes.toBytes(hbaseTableName_));
    FileSystem fs = tableDir.getFileSystem(hbaseConf_);
    Path regionDir = tableDir.suffix("/" + info.getEncodedName());
    return fs.getContentSummary(regionDir).getLength();
  }

  /**
   * Returns hbase's root directory: i.e. <code>hbase.rootdir</code> from
   * the given configuration as a qualified Path.
   * Method copied from HBase FSUtils.java to avoid depending on HBase server.
   */
  public static Path getRootDir(final Configuration c) throws IOException {
    Path p = new Path(c.get(HConstants.HBASE_DIR));
    FileSystem fs = p.getFileSystem(c);
    return p.makeQualified(fs);
  }

  /**
   * Hive returns the columns in order of their declaration for HBase tables.
   */
  @Override
  public ArrayList<Column> getColumnsInHiveOrder() { return colsByPos_; }

  @Override
  public TTableDescriptor toThriftDescriptor() {
    TTableDescriptor tableDescriptor =
        new TTableDescriptor(id_.asInt(), TTableType.HBASE_TABLE, colsByPos_.size(),
            numClusteringCols_, hbaseTableName_, db_.getName());
    tableDescriptor.setHbaseTable(getTHBaseTable());
    return tableDescriptor;
  }

  public String getHBaseTableName() { return hbaseTableName_; }
  public HTable getHTable() { return hTable_; }
  public static Configuration getHBaseConf() { return hbaseConf_; }

  @Override
  public int getNumNodes() {
    // TODO: implement
    return 100;
  }

  @Override
  public TCatalogObjectType getCatalogObjectType() { return TCatalogObjectType.TABLE; }

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
    for (Column c : colsByPos_) {
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
   * @param startRow Starting row in range, inclusive
   * @param endRow Ending row in range, exclusive
   * @return A list of HRegionLocations corresponding to the regions that
   * contain the specified range
   * @throws IOException if a remote or network exception occurs
   */
  public static List<HRegionLocation> getRegionsInRange(HTable hbaseTbl,
    final byte[] startKey, final byte[] endKey) throws IOException {
    final boolean endKeyIsEndOfTable = Bytes.equals(endKey, HConstants.EMPTY_END_ROW);
    if ((Bytes.compareTo(startKey, endKey) > 0) && !endKeyIsEndOfTable) {
      throw new IllegalArgumentException("Invalid range: " +
          Bytes.toStringBinary(startKey) + " > " + Bytes.toStringBinary(endKey));
    }
    final List<HRegionLocation> regionList = new ArrayList<HRegionLocation>();
    byte[] currentKey = startKey;
    // Make sure only one thread is accessing the hbaseTbl.
    synchronized(hbaseTbl) {
      do {
        // always reload region location info.
        HRegionLocation regionLocation = hbaseTbl.getRegionLocation(currentKey, true);
        regionList.add(regionLocation);
        currentKey = regionLocation.getRegionInfo().getEndKey();
      } while (!Bytes.equals(currentKey, HConstants.EMPTY_END_ROW) &&
          (endKeyIsEndOfTable || Bytes.compareTo(currentKey, endKey) < 0));
    }
    return regionList;
  }

  /**
   * Returns the input-format class string for HBase tables read by Hive.
   */
  public static String getInputFormat() { return HBASE_INPUT_FORMAT; }

  /**
   * Returns the storage handler class for HBase tables read by Hive.
   */
  @Override
  public String getStorageHandlerClassName() { return HBASE_STORAGE_HANDLER; }

  /**
   * Returns statistics on this table as a tabular result set. Used for the
   * SHOW TABLE STATS statement. The schema of the returned TResultSet is set
   * inside this method.
   */
  public TResultSet getTableStats() {
    TResultSet result = new TResultSet();
    TResultSetMetadata resultSchema = new TResultSetMetadata();
    result.setSchema(resultSchema);
    resultSchema.addToColumns(new TColumn("Region Location", TPrimitiveType.STRING));
    resultSchema.addToColumns(new TColumn("Start RowKey",
        TPrimitiveType.STRING));
    resultSchema.addToColumns(new TColumn("Est. #Rows", TPrimitiveType.BIGINT));
    resultSchema.addToColumns(new TColumn("Size", TPrimitiveType.STRING));

    // TODO: Consider fancier stats maintenance techniques for speeding up this process.
    // Currently, we list all regions and perform a mini-scan of each of them to
    // estimate the number of rows, the data size, etc., which is rather expensive.
    try {
      long totalNumRows = 0;
      long totalHdfsSize = 0;
      List<HRegionLocation> regions = HBaseTable.getRegionsInRange(hTable_,
          HConstants.EMPTY_END_ROW, HConstants.EMPTY_START_ROW);
      for (HRegionLocation region: regions) {
        TResultRowBuilder rowBuilder = new TResultRowBuilder();
        HRegionInfo regionInfo = region.getRegionInfo();
        Pair<Long, Long> estRowStats = getEstimatedRowStats(regionInfo.getStartKey(),
            regionInfo.getEndKey());

        long numRows = estRowStats.first.longValue();
        long hdfsSize = getHdfsSize(regionInfo);
        totalNumRows += numRows;
        totalHdfsSize += hdfsSize;

        // Add the region location, start rowkey, number of rows and raw Hdfs size.
        rowBuilder.add(String.valueOf(region.getHostname()))
            .add(Bytes.toString(regionInfo.getStartKey())).add(numRows)
            .addBytes(hdfsSize);
        result.addToRows(rowBuilder.get());
      }

      // Total num rows and raw Hdfs size.
      if (regions.size() > 1) {
        TResultRowBuilder rowBuilder = new TResultRowBuilder();
        rowBuilder.add("Total").add("").add(totalNumRows).addBytes(totalHdfsSize);
        result.addToRows(rowBuilder.get());
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return result;
  }
}
