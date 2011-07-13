// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.catalog;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;

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

  // Create client for test schema.
  public static HiveMetaStoreClient createClient() throws Exception {
    HiveMetaStoreClient client = new HiveMetaStoreClient(new HiveConf(TestSchemaUtils.class));
    return client;
  }

  public static String getComplexTypeTableName(String type) {
    return type + "_tbl";
  }

  public static String getAllTypesColumn(PrimitiveType type) {
    return typeToColumnNameMap.get(type);
  }
}
