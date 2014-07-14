// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.catalog;

import java.util.HashMap;
import java.util.Map;

public class TestSchemaUtils {
  // maps from PrimitiveType to column name
  // in alltypes table
  private static Map<Type, String> typeToColumnNameMap_ =
      new HashMap<Type, String>();
  static {
    typeToColumnNameMap_.put(Type.BOOLEAN, "bool_col");
    typeToColumnNameMap_.put(Type.TINYINT, "tinyint_col");
    typeToColumnNameMap_.put(Type.SMALLINT, "smallint_col");
    typeToColumnNameMap_.put(Type.INT, "int_col");
    typeToColumnNameMap_.put(Type.BIGINT, "bigint_col");
    typeToColumnNameMap_.put(Type.FLOAT, "float_col");
    typeToColumnNameMap_.put(Type.DOUBLE, "double_col");
    typeToColumnNameMap_.put(Type.DATE, "date_col");
    typeToColumnNameMap_.put(Type.DATETIME, "datetime_col");
    typeToColumnNameMap_.put(Type.TIMESTAMP, "timestamp_col");
    typeToColumnNameMap_.put(Type.STRING, "string_col");
  }

  public static String getComplexTypeTableName(String type) {
    return type + "_tbl";
  }

  public static String getAllTypesColumn(Type type) {
    return typeToColumnNameMap_.get(type);
  }
}
