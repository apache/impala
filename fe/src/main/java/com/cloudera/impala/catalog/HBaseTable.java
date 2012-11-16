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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.hbase.HBaseSerDe;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.serde2.SerDeException;

import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.catalog.Db.TableLoadingException;
import com.cloudera.impala.planner.DataSink;
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
  // Copied from Hive's HBaseStorageHandler.java.
  public static final String DEFAULT_PREFIX = "default.";
  // Column referring to HBase row key.
  // Hive (including metastore) currently doesn't support composite HBase keys.
  protected HBaseColumn rowKey;
  // Name of table in HBase.
  // 'this.name' is the alias of the HBase table in Hive.
  protected String hbaseTableName;

  // Input format class for HBase tables read by Hive.
  private static final String hbaseInputFormat =
    "org.apache.hadoop.hive.hbase.HiveHBaseTableInputFormat";

  protected HBaseTable(TableId id, Db db, String name, String owner) {
    super(id,db, name, owner);
  }

  // Parse the column description string to the column families and column
  // qualifies.  This is a copy of the HBaseSerDe.parseColumnMapping function
  // with parts we don't use removed.  The hive function is not public.
  //  columnsMappingSpec - input string format describing the table
  //  columnFamilies/columnQualifiers - out parameters that will be filled with the
  //    column family and column qualifies strings for each column.
  public void parseColumnMapping(String columnsMappingSpec, List<String> columnFamilies,
      List<String> columnQualifiers) throws SerDeException {

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
      String colInfo = mapInfo[0];

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
    }

    if (rowKeyIndex == -1) {
      columnFamilies.add(HBaseSerDe.HBASE_KEY_COL);
      columnQualifiers.add(null);
    }
  }

  @Override
  public Table load(HiveMetaStoreClient client,
      org.apache.hadoop.hive.metastore.api.Table msTbl) throws TableLoadingException {
    try {
      hbaseTableName = getHBaseTableName(msTbl);
      Map<String, String> serdeParam = msTbl.getSd().getSerdeInfo().getParameters();
      String hbaseColumnsMapping = serdeParam.get(HBaseSerDe.HBASE_COLUMNS_MAPPING);

      if (hbaseColumnsMapping == null) {
        throw new MetaException("No hbase.columns.mapping defined in Serde.");
      }


      // Parse HBase column-mapping string.
      List<String> hbaseColumnFamilies = new ArrayList<String>();
      List<String> hbaseColumnQualifiers = new ArrayList<String>();
      parseColumnMapping(hbaseColumnsMapping, hbaseColumnFamilies,
          hbaseColumnQualifiers);
      Preconditions.checkState(hbaseColumnFamilies.size() == hbaseColumnQualifiers.size());

      // Populate tmp cols in the order they appear in the Hive metastore.
      // We will reorder the cols below.
      List<FieldSchema> fieldSchemas = msTbl.getSd().getCols();
      Preconditions.checkState(fieldSchemas.size() == hbaseColumnQualifiers.size());
      List<HBaseColumn> tmpCols = new ArrayList<HBaseColumn>();
      for (int i = 0; i < fieldSchemas.size(); ++i) {
        FieldSchema s = fieldSchemas.get(i);
        HBaseColumn col = new HBaseColumn(s.getName(), hbaseColumnFamilies.get(i),
            hbaseColumnQualifiers.get(i), getPrimitiveType(s.getType()), -1);
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

      return this;
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
    }
    TTableDescriptor TTableDescriptor =
        new TTableDescriptor(id.asInt(), TTableType.HBASE_TABLE, colsByPos.size(),
            numClusteringCols, hbaseTableName, db.getName());
    TTableDescriptor.setHbaseTable(tHbaseTable);
    return TTableDescriptor;
  }

  public String getHBaseTableName() {
    return hbaseTableName;
  }

  public static boolean isHBaseTable(org.apache.hadoop.hive.metastore.api.Table msTbl) {
    return msTbl.getSd().getInputFormat().equals(hbaseInputFormat);
  }

  @Override
  public DataSink createDataSink(List<Expr> partitionKeyExprs, boolean overwrite) {
    // Partition clause doesn't make sense for an HBase table.
    Preconditions.checkState(partitionKeyExprs.isEmpty());
    // Overwrite doesn't make sense for an HBase table.
    Preconditions.checkState(overwrite == false);
    throw new UnsupportedOperationException("HBase Output Sink not implemented.");
  }
}
