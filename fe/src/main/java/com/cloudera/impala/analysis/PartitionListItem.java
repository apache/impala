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

package com.cloudera.impala.analysis;

/**
 * Representation of a single column:value element in the PARTITION (...) clause of an insert
 * statement.
 */
public class PartitionListItem {
  // Name of partitioning column.
  private final String colName_;
  // Value of partitioning column. Set to null for dynamic inserts.
  private final LiteralExpr value_;

  public PartitionListItem(String colName, LiteralExpr value) {
    this.colName_ = colName;
    this.value_ = value;
  }

  public String getColName() { return colName_; }
  public LiteralExpr getValue() { return value_; }
}
