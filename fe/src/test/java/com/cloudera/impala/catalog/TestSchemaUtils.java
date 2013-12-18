// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.catalog;

import java.util.HashMap;
import java.util.Map;

public class TestSchemaUtils {
  // maps from PrimitiveType to column name
  // in alltypes table
  private final static Map<PrimitiveType, String> typeToColumnNameMap_ =
      new HashMap<PrimitiveType, String>();
  static {
    typeToColumnNameMap_.put(PrimitiveType.BOOLEAN, "bool_col");
    typeToColumnNameMap_.put(PrimitiveType.TINYINT, "tinyint_col");
    typeToColumnNameMap_.put(PrimitiveType.SMALLINT, "smallint_col");
    typeToColumnNameMap_.put(PrimitiveType.INT, "int_col");
    typeToColumnNameMap_.put(PrimitiveType.BIGINT, "bigint_col");
    typeToColumnNameMap_.put(PrimitiveType.FLOAT, "float_col");
    typeToColumnNameMap_.put(PrimitiveType.DOUBLE, "double_col");
    typeToColumnNameMap_.put(PrimitiveType.DATE, "date_col");
    typeToColumnNameMap_.put(PrimitiveType.DATETIME, "datetime_col");
    typeToColumnNameMap_.put(PrimitiveType.TIMESTAMP, "timestamp_col");
    typeToColumnNameMap_.put(PrimitiveType.STRING, "string_col");
  }

  public static String getComplexTypeTableName(String type) {
    return type + "_tbl";
  }

  public static String getAllTypesColumn(PrimitiveType type) {
    return typeToColumnNameMap_.get(type);
  }
}
