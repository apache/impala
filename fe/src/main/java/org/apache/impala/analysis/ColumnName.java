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

package org.apache.impala.analysis;

import com.google.common.base.Preconditions;
import org.apache.impala.thrift.TColumnName;

/**
 * Represents a column name that optionally includes its database name.
 */
public class ColumnName {
  private final TableName tableName_;
  private final String columnName_;

  public ColumnName(TableName tableName, String columnName) {
    Preconditions.checkNotNull(tableName);
    Preconditions.checkNotNull(columnName);
    this.tableName_ = tableName;
    this.columnName_ = columnName;
  }

  public TableName getTableName() { return tableName_; }

  public String getColumnName() { return columnName_; }

  public static String thriftToString(TColumnName colName) {
    return TableName.thriftToString(colName.getTable_name()) + "." +
        colName.getColumn_name();
  }
}
