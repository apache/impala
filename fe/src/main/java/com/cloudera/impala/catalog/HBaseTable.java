// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.catalog;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.hbase.HBaseSerDe;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.serde2.SerDeException;

import com.cloudera.impala.thrift.THBaseTable;
import com.cloudera.impala.thrift.TTable;
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
  public static final String hbaseInputFormat =
    "org.apache.hadoop.hive.hbase.HiveHBaseTableInputFormat";

  protected HBaseTable(Db db, String name, String owner) {
    super(db, name, owner);
  }

  @Override
  public Table load(HiveMetaStoreClient client,
                    org.apache.hadoop.hive.metastore.api.Table msTbl) {
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
      int keyIndex = HBaseSerDe.parseColumnMapping(hbaseColumnsMapping, hbaseColumnFamilies,
          null, hbaseColumnQualifiers, null);
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
        if (i == keyIndex) {
          rowKey = col;
        }
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
    } catch (SerDeException e) {
      throw new UnsupportedOperationException(e.toString());
    } catch (MetaException e) {
      throw new UnsupportedOperationException(e.toString());
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
  public TTable toThrift() {
    THBaseTable tHbaseTable = new THBaseTable();
    tHbaseTable.setTableName(hbaseTableName);
    for (Column c : colsByPos) {
      HBaseColumn hbaseCol = (HBaseColumn) c;
      ByteBuffer familyBytes = ByteBuffer.wrap(Bytes.toBytes(hbaseCol.getColumnFamily()));
      tHbaseTable.addToFamilies(familyBytes);
      ByteBuffer qualifierBytes = null;
      // Column qualifier is null for the row key.
      if (hbaseCol.getColumnQualifier() != null) {
        qualifierBytes = ByteBuffer.wrap(Bytes.toBytes(hbaseCol.getColumnQualifier()));
      } else {
        qualifierBytes = ByteBuffer.wrap(new byte[]{});
      }
      tHbaseTable.addToQualifiers(qualifierBytes);
    }
    TTable ttable =
        new TTable(TTableType.HBASE_TABLE, colsByPos.size(), numClusteringCols);
    ttable.setHbaseTable(tHbaseTable);
    return ttable;
  }

  public String getHBaseTableName() {
    return hbaseTableName;
  }
}
