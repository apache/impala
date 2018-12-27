// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.impala.catalog;

import java.util.HashMap;
import java.util.Map;

public class TestSchemaUtils {
  // maps from PrimitiveType to column name
  // in alltypes table
  private static Map<Type, String> typeToColumnNameMap_ = new HashMap<>();
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
