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

import org.apache.hadoop.hive.metastore.MetaStoreUtils;

import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TColumn;

/**
 * Represents a column definition in a CREATE/ALTER TABLE/VIEW statement.
 * Column definitions in CREATE/ALTER TABLE statements require a column type,
 * whereas column definitions in CREATE/ALTER VIEW statements infer the column type from
 * the corresponding view definition. All column definitions have an optional comment.
 * Since a column definition refers a column stored in the Metastore, the column name
 * must be valid according to the Metastore's rules (see @MetaStoreUtils).
 */
public class ColumnDesc {
  private final String colName;
  private final String comment;
  // Required in CREATE/ALTER TABLE stmts. Set to NULL in CREATE/ALTER VIEW stmts.
  private PrimitiveType colType;

  public ColumnDesc(String colName, PrimitiveType colType, String comment) {
    this.colName = colName;
    this.colType = colType;
    this.comment = comment;
  }

  public void setColType(PrimitiveType colType) { this.colType = colType; }
  public PrimitiveType getColType() { return colType; }
  public String getColName() { return colName; }
  public String getComment() { return comment; }

  public void analyze() throws AnalysisException {
    // Check whether the column name meets the Metastore's requirements.
    if (!MetaStoreUtils.validateName(colName)) {
      throw new AnalysisException("Invalid column name: " + colName);
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(colName);
    if (colType != null) sb.append(" " + colType.toString());
    if (comment != null) sb.append(String.format(" COMMENT '%s'", comment));
    return sb.toString();
  }

  public TColumn toThrift() {
    TColumn col = new TColumn(
        new TColumn(getColName(), getColType().toThrift()));
    col.setComment(getComment());
    return col;
  }
}
