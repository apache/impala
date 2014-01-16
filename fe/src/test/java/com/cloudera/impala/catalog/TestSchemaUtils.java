// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.catalog;

import java.util.HashMap;
import java.util.Map;

public class TestSchemaUtils {
  // maps from PrimitiveType to column name
  // in alltypes table
  private static Map<ColumnType, String> typeToColumnNameMap_ =
      new HashMap<ColumnType, String>();
  static {
    typeToColumnNameMap_.put(ColumnType.BOOLEAN, "bool_col");
    typeToColumnNameMap_.put(ColumnType.TINYINT, "tinyint_col");
    typeToColumnNameMap_.put(ColumnType.SMALLINT, "smallint_col");
    typeToColumnNameMap_.put(ColumnType.INT, "int_col");
    typeToColumnNameMap_.put(ColumnType.BIGINT, "bigint_col");
    typeToColumnNameMap_.put(ColumnType.FLOAT, "float_col");
    typeToColumnNameMap_.put(ColumnType.DOUBLE, "double_col");
    typeToColumnNameMap_.put(ColumnType.DATE, "date_col");
    typeToColumnNameMap_.put(ColumnType.DATETIME, "datetime_col");
    typeToColumnNameMap_.put(ColumnType.TIMESTAMP, "timestamp_col");
    typeToColumnNameMap_.put(ColumnType.STRING, "string_col");
  }

  public static String getComplexTypeTableName(String type) {
    return type + "_tbl";
  }

  public static String getAllTypesColumn(ColumnType type) {
    return typeToColumnNameMap_.get(type);
  }
}
