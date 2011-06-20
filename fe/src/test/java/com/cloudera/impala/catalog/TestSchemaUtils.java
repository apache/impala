// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.catalog;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde.Constants;


public class TestSchemaUtils {
  // maps from PrimitiveType to column name
  // in alltypes table
  private static Map<PrimitiveType, String> typeToColumnNameMap =
      new HashMap<PrimitiveType, String>();
  static {
    typeToColumnNameMap.put(PrimitiveType.BOOLEAN, "bool_col");
    typeToColumnNameMap.put(PrimitiveType.TINYINT, "tinyint_col");
    typeToColumnNameMap.put(PrimitiveType.SMALLINT, "smallint_col");
    typeToColumnNameMap.put(PrimitiveType.INT, "int_col");
    typeToColumnNameMap.put(PrimitiveType.BIGINT, "bigint_col");
    typeToColumnNameMap.put(PrimitiveType.FLOAT, "float_col");
    typeToColumnNameMap.put(PrimitiveType.DOUBLE, "double_col");
    typeToColumnNameMap.put(PrimitiveType.DATE, "date_col");
    typeToColumnNameMap.put(PrimitiveType.DATETIME, "datetime_col");
    typeToColumnNameMap.put(PrimitiveType.TIMESTAMP, "timestamp_col");
    typeToColumnNameMap.put(PrimitiveType.STRING, "string_col");
  }

  private static void createTable(
      HiveMetaStoreClient client, String db,
      String name, List<FieldSchema> cols) throws Exception {
    Table tbl = new Table();
    tbl.setDbName(db);
    tbl.setTableName(name);
    StorageDescriptor sd = new StorageDescriptor();
    tbl.setSd(sd);
    sd.setCols(cols);
    sd.setCompressed(false);
    sd.setNumBuckets(1);
    // do i need this?
    sd.setSerdeInfo(new SerDeInfo());
    sd.getSerdeInfo().setName(tbl.getTableName());
    sd.getSerdeInfo().setParameters(new HashMap<String, String>());
    sd.getSerdeInfo().getParameters()
        .put(Constants.SERIALIZATION_FORMAT, "1");
    sd.setSortCols(new ArrayList<Order>());
    try {
      client.dropTable(db, name);
    } catch (NoSuchObjectException e) {
    }
    client.createTable(tbl);
  }

  private static void createTestSchema(HiveMetaStoreClient client) throws Exception {
    // AllTypes table
    ArrayList<FieldSchema> allTypesCols = new ArrayList<FieldSchema>();
    allTypesCols.add(new FieldSchema("bool_col", Constants.BOOLEAN_TYPE_NAME, ""));
    allTypesCols.add(new FieldSchema("tinyint_col", Constants.TINYINT_TYPE_NAME, ""));
    allTypesCols.add(new FieldSchema("smallint_col", Constants.SMALLINT_TYPE_NAME, ""));
    allTypesCols.add(new FieldSchema("int_col", Constants.INT_TYPE_NAME, ""));
    allTypesCols.add(new FieldSchema("bigint_col", Constants.BIGINT_TYPE_NAME, ""));
    allTypesCols.add(new FieldSchema("float_col", Constants.FLOAT_TYPE_NAME, ""));
    allTypesCols.add(new FieldSchema("double_col", Constants.DOUBLE_TYPE_NAME, ""));
    allTypesCols.add(new FieldSchema("date_col", Constants.DATE_TYPE_NAME, ""));
    allTypesCols.add(new FieldSchema("datetime_col", Constants.DATETIME_TYPE_NAME, ""));
    allTypesCols.add(new FieldSchema("timestamp_col", Constants.TIMESTAMP_TYPE_NAME, ""));
    allTypesCols.add(new FieldSchema("string_col", Constants.STRING_TYPE_NAME, ""));
    createTable(client, "default", "AllTypes", allTypesCols);

    // TestTbl
    ArrayList<FieldSchema> testTblCols = new ArrayList<FieldSchema>();
    testTblCols.add(new FieldSchema("id", Constants.BIGINT_TYPE_NAME, ""));
    testTblCols.add(new FieldSchema("name", Constants.STRING_TYPE_NAME, ""));
    testTblCols.add(new FieldSchema("zip", Constants.INT_TYPE_NAME, ""));
    createTable(client, "default", "TestTbl", testTblCols);

    // database testdb1
    Database db = new Database("testdb1", "", "?", null);
    try {
      client.createDatabase(db);
    } catch (AlreadyExistsException e) {
      // expected
    }

    // AllTypes table
    createTable(client, "testdb1", "AllTypes", allTypesCols);

    // TestTbl
    testTblCols = new ArrayList<FieldSchema>();
    testTblCols.add(new FieldSchema("id", Constants.BIGINT_TYPE_NAME, ""));
    testTblCols.add(new FieldSchema("name", Constants.STRING_TYPE_NAME, ""));
    testTblCols.add(new FieldSchema("birthday", Constants.DATE_TYPE_NAME, ""));
    createTable(client, "testdb1", "TestTbl", testTblCols);

    // Tables with complex types, one table per complex type
    for (String collectionType : Constants.CollectionTypes) {
      String tableName = getComplexTypeTableName(collectionType);
      ArrayList<FieldSchema> complexTblCols = new ArrayList<FieldSchema>();
      complexTblCols.add(new FieldSchema("id", Constants.BIGINT_TYPE_NAME, ""));
      complexTblCols.add(new FieldSchema("name", Constants.STRING_TYPE_NAME, ""));
      complexTblCols.add(new FieldSchema("collection_col", collectionType, ""));
      try {
        createTable(client, "default", tableName, complexTblCols);
      } catch (Exception e) {
        e.printStackTrace();
      }
      createTable(client, "testdb1", tableName, complexTblCols);
    }
  }

  // Create client for test schema.
  public static HiveMetaStoreClient createSchemaAndClient() throws Exception {
    HiveMetaStoreClient client = new HiveMetaStoreClient(new HiveConf(TestSchemaUtils.class));
    createTestSchema(client);
    return client;
  }

  public static String getComplexTypeTableName(String type) {
    return type + "_tbl";
  }

  public static String getAllTypesColumn(PrimitiveType type) {
    return typeToColumnNameMap.get(type);
  }
}
