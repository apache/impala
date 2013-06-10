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

import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.thrift.TColumnDef;
import com.cloudera.impala.thrift.TColumnDesc;

/**
 * Represents a column definition in a CREATE/ALTER TABLE/VIEW statement.
 * Column definitions in CREATE/ALTER TABLE statements require a column type,
 * whereas column definitions in CREATE/ALTER VIEW statements infer the column type from
 * the corresponding view definition.
 * All column definitions have an optional comment.
 */
public class ColumnDef {
  private final String colName;
  private final String comment;
  // Required in CREATE/ALTER TABLE stmts. Set to NULL in CREATE/ALTER VIEW stmts.
  private PrimitiveType colType;

  public ColumnDef(String colName, PrimitiveType colType, String comment) {
    this.colName = colName;
    this.colType = colType;
    this.comment = comment;
  }

  public void setColType(PrimitiveType colType) { this.colType = colType; }
  public PrimitiveType getColType() { return colType; }
  public String getColName() { return colName; }
  public String getComment() { return comment; }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(colName);
    if (colType != null) sb.append(" " + colType.toString());
    if (comment != null) sb.append(String.format(" COMMENT '%s'", comment));
    return sb.toString();
  }

  public TColumnDef toThrift() {
    TColumnDef colDef = new TColumnDef(
        new TColumnDesc(getColName(), getColType().toThrift()));
    colDef.setComment(getComment());
    return colDef;
  }
}
