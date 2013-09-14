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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hive.hbase.HBaseSerDe;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.log4j.Logger;

import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.common.Pair;
import com.cloudera.impala.planner.DataSink;
import com.cloudera.impala.planner.HBaseTableSink;
import com.cloudera.impala.thrift.TCatalogObjectType;
import com.cloudera.impala.thrift.THBaseTable;
import com.cloudera.impala.thrift.TTableDescriptor;
import com.cloudera.impala.thrift.TTableType;
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
  protected HBaseColumn rowKey;
  // Name of table in HBase.
  // 'this.name' is the alias of the HBase table in Hive.
  protected String hbaseTableName;

  // Input format class for HBase tables read by Hive.
  private static final String hbaseInputFormat =
    "org.apache.hadoop.hive.hbase.HiveHBaseTableInputFormat";

  // Keep the conf around
  private final static Configuration hbaseConf = HBaseConfiguration.create();

  private HTable hTable = null;

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
  public void parseColumnMapping(boolean tableDefaultStorageIsBinary,
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
    String [] columnSpecs = columnsMappingSpec.split(",");

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
      // Only boolean, integer and floating point types can use binary storage.
      PrimitiveType colType = getPrimitiveType(fieldSchemas.get(i).getType());
      boolean supportBinaryEncoding = colType.equals(PrimitiveType.BOOLEAN) ||
          colType.isIntegerType() || colType.isFloatingPointType();
      String colName = fieldSchemas.get(i).getName();
      if (mapInfo.length == 1) {
        // There is no column level storage specification. Use the table storage spec.
        colIsBinaryEncoded.add(
            new Boolean(tableDefaultStorageIsBinary && supportBinaryEncoding));
      } else if (mapInfo.length == 2) {
        // There is a storage specification for the column
        String storageOption = mapInfo[1];

        if (!(storageOption.equals("-") || "string".startsWith(storageOption) ||
            "binary".startsWith(storageOption))) {
          throw new SerDeException("Error: A column storage specification is one of"
              + " the following: '-', a prefix of 'string', or a prefix of 'binary'. "
              + storageOption + " is not a valid storage option specification for "
              + colName);
        }

        boolean isBinaryEncoded = false;
        if ("-".equals(storageOption)) {
          isBinaryEncoded = tableDefaultStorageIsBinary;
        } else if ("binary".startsWith(storageOption)) {
          isBinaryEncoded = true;
        }
        if (isBinaryEncoded && !supportBinaryEncoding) {
          // Use string encoding and log a warning if the column spec is binary but the
          // column type does not support it.
          // TODO: Hive/HBase does not raise an exception, but should we?
          LOG.warn("Column storage specification for column " + colName + " is binary"
              + " but the column type " + colType.toString() + " does not support binary"
              +	" encoding. Fallback to string format.");
          isBinaryEncoded = false;
        }
        colIsBinaryEncoded.add(isBinaryEncoded);
      } else {
        // error in storage specification
        throw new SerDeException("Error: " + HBaseSerDe.HBASE_COLUMNS_MAPPING
            + " storage specification " + mappingSpec + " is not valid for column: "
            + colName);
      }
    }

    if (rowKeyIndex == -1) {
      columnFamilies.add(HBaseSerDe.HBASE_KEY_COL);
      columnQualifiers.add(null);
    }
  }

  @Override
  public void load(Table oldValue, HiveMetaStoreClient client,
      org.apache.hadoop.hive.metastore.api.Table msTbl) throws TableLoadingException {
    try {
      hbaseTableName = getHBaseTableName(msTbl);
      hTable = new HTable(hbaseConf, hbaseTableName);
      Map<String, String> serdeParam = msTbl.getSd().getSerdeInfo().getParameters();
      String hbaseColumnsMapping = serdeParam.get(HBaseSerDe.HBASE_COLUMNS_MAPPING);

      if (hbaseColumnsMapping == null) {
        throw new MetaException("No hbase.columns.mapping defined in Serde.");
      }
      String hbaseTableDefaultStorageType =
          msTbl.getParameters().get(HBaseSerDe.HBASE_TABLE_DEFAULT_STORAGE_TYPE);
      boolean tableDefaultStorageIsBinary = false;
      if (hbaseTableDefaultStorageType != null &&
          !hbaseTableDefaultStorageType.isEmpty()) {
        if (hbaseTableDefaultStorageType.equals("binary")) {
          tableDefaultStorageIsBinary = true;
        } else if (!hbaseTableDefaultStorageType.equals("string")) {
          throw new SerDeException("Error: " +
              HBaseSerDe.HBASE_TABLE_DEFAULT_STORAGE_TYPE +
              " parameter must be specified as" +
              " 'string' or 'binary'; '" + hbaseTableDefaultStorageType +
              "' is not a valid specification for this table/serde property.");
        }
      }

      // Parse HBase column-mapping string.
      List<FieldSchema> fieldSchemas = msTbl.getSd().getCols();
      List<String> hbaseColumnFamilies = new ArrayList<String>();
      List<String> hbaseColumnQualifiers = new ArrayList<String>();
      List<Boolean> hbaseColumnBinaryEncodings = new ArrayList<Boolean>();
      parseColumnMapping(tableDefaultStorageIsBinary, hbaseColumnsMapping, fieldSchemas,
          hbaseColumnFamilies, hbaseColumnQualifiers, hbaseColumnBinaryEncodings);
      Preconditions.checkState(
          hbaseColumnFamilies.size() == hbaseColumnQualifiers.size());

      // Populate tmp cols in the order they appear in the Hive metastore.
      // We will reorder the cols below.
      Preconditions.checkState(fieldSchemas.size() == hbaseColumnQualifiers.size());
      List<HBaseColumn> tmpCols = new ArrayList<HBaseColumn>();
      for (int i = 0; i < fieldSchemas.size(); ++i) {
        FieldSchema s = fieldSchemas.get(i);
        HBaseColumn col = new HBaseColumn(s.getName(), hbaseColumnFamilies.get(i),
            hbaseColumnQualifiers.get(i), hbaseColumnBinaryEncodings.get(i),
            getPrimitiveType(s), s.getComment(), -1);
        tmpCols.add(col);
      }

      // HBase columns are ordered by columnFamily,columnQualifier,
      // so the final position depends on the other mapped HBase columns.
      // Sort columns and update positions.
      Collections.sort(tmpCols);
      for (int i = 0; i < tmpCols.size(); ++i) {
        HBaseColumn col = tmpCols.get(i);
        col.setPosition(i);
        colsByPos.add(col);
        colsByName.put(col.getName(), col);
      }

      // since we don't support composite hbase rowkeys yet, all hbase tables have a
      // single clustering col
      numClusteringCols = 1;
    } catch (Exception e) {
      throw new TableLoadingException("Failed to load metadata for HBase table: " + name,
          e);
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
      Path tableDir = HTableDescriptor.getTableDir(
          FSUtils.getRootDir(hbaseConf), Bytes.toBytes(hbaseTableName));
      FileSystem fs = tableDir.getFileSystem(hbaseConf);

      // Check to see if things are compressed.
      // If they are we'll estimate a compression factor.
      HColumnDescriptor[] families =
          hTable.getTableDescriptor().getColumnFamilies();
      for (HColumnDescriptor desc: families) {
        isCompressed |= desc.getCompression() != Compression.Algorithm.NONE;
      }

      // For every region in the range.
      List<HRegionLocation> locations = getRegionsInRange(hTable, startRowKey, endRowKey);
      for(HRegionLocation location: locations) {
        long currentHdfsSize = 0;
        long currentRowSize  = 0;
        long currentRowCount = 0;

        HRegionInfo info = location.getRegionInfo();
        // Get the size on hdfs
        Path regionDir = tableDir.suffix("/" + info.getEncodedName());
        currentHdfsSize += fs.getContentSummary(regionDir).getLength();

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
        ResultScanner rs = hTable.getScanner(s);

        // And get the the ROW_COUNT_ESTIMATE_BATCH_SIZE fetched rows
        // for a representative sample
        for (int i = 0; i < ROW_COUNT_ESTIMATE_BATCH_SIZE; i++) {
          Result r = rs.next();
          if (r == null) break;
          currentRowCount += 1;
          for (KeyValue kv : r.list()) {
            // some extra row size added to make up for shared overhead
            currentRowSize += kv.getRowLength() // row key
                + 4 // row key length field
                + kv.getFamilyLength() // Column family bytes
                + 4  // family length field
                + kv.getQualifierLength() // qualifier bytes
                + 4 // qualifier length field
                + kv.getValueLength() // length of the value
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
   * Hive returns the columns in order of their declaration for HBase tables.
   */
  @Override
  public ArrayList<Column> getColumnsInHiveOrder() { return colsByPos; }

  @Override
  public TTableDescriptor toThrift() {
    THBaseTable tHbaseTable = new THBaseTable();
    tHbaseTable.setTableName(hbaseTableName);
    for (Column c : colsByPos) {
      HBaseColumn hbaseCol = (HBaseColumn) c;
      tHbaseTable.addToFamilies(hbaseCol.getColumnFamily());
      if (hbaseCol.getColumnQualifier() != null) {
        tHbaseTable.addToQualifiers(hbaseCol.getColumnQualifier());
      } else {
        tHbaseTable.addToQualifiers("");
      }
      tHbaseTable.addToBinary_encoded(hbaseCol.isBinaryEncoded());
    }
    TTableDescriptor tableDescriptor =
        new TTableDescriptor(id.asInt(), TTableType.HBASE_TABLE, colsByPos.size(),
            numClusteringCols, hbaseTableName, db.getName());
    tableDescriptor.setHbaseTable(tHbaseTable);
    return tableDescriptor;
  }

  public String getHBaseTableName() { return hbaseTableName; }

  @Override
  public int getNumNodes() {
    // TODO: implement
    return 100;
  }

  @Override
  public TCatalogObjectType getCatalogObjectType() { return TCatalogObjectType.TABLE; }

  @Override
  public DataSink createDataSink(List<Expr> partitionKeyExprs, boolean overwrite) {
    // Partition clause doesn't make sense for an HBase table.
    Preconditions.checkState(partitionKeyExprs.isEmpty());

    // HBase doesn't have a way to perform INSERT OVERWRITE
    Preconditions.checkState(overwrite == false);
    // Create the HBaseTableSink and return it.
    return new HBaseTableSink(this);
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
    do {
      // always reload region location info.
      HRegionLocation regionLocation = hbaseTbl.getRegionLocation(currentKey, true);
      regionList.add(regionLocation);
      currentKey = regionLocation.getRegionInfo().getEndKey();
    } while (!Bytes.equals(currentKey, HConstants.EMPTY_END_ROW) &&
             (endKeyIsEndOfTable || Bytes.compareTo(currentKey, endKey) < 0));
    return regionList;
  }

  /**
   * Returns the input-format class string for HBase tables read by Hive.
   */
  public static String getInputFormat() { return hbaseInputFormat; }
}
