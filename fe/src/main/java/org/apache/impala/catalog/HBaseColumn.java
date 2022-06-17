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

import org.apache.impala.thrift.TColumn;

// Describes an HBase column mapped to a Hive column (as described in the metastore).
// this.name describes the column name in Hive.
// This class adds the HBase columnFamily and columnQualifier,
// so we can read the column from HBase directly.
public class HBaseColumn extends Column implements Comparable<HBaseColumn> {
  private final String columnFamily_;
  private final String columnQualifier_;
  private final boolean binaryEncoded_;

  public HBaseColumn(String name, String columnFamily, String columnQualifier,
      boolean binaryEncoded, Type type, String comment, int position) {
    super(name, type, comment, position);
    columnFamily_ = CatalogInterners.internString(columnFamily);
    columnQualifier_ = columnQualifier;
    binaryEncoded_ = binaryEncoded;
  }

  public String getColumnFamily() { return columnFamily_; }
  public String getColumnQualifier() { return columnQualifier_; }
  public boolean isBinaryEncoded() { return binaryEncoded_; }
  public boolean isKeyColumn() {
    return columnFamily_.equals(FeHBaseTable.Util.ROW_KEY_COLUMN_FAMILY);
  }

  @Override
  // We order the HBase columns based on columnFamily,columnQualifier.
  // Till IMPALA-886 the backend relied on this, now it is only done for backward
  // compatibility if flag use_hms_column_order_for_hbase_tables=false.
  public int compareTo(HBaseColumn o) {
    int familyCmp = columnFamily_.compareTo(o.columnFamily_);
    if (familyCmp != 0) {
      return familyCmp;
    }
    int qualifierCmp = columnQualifier_.compareTo(o.columnQualifier_);
    return qualifierCmp;
  }

  @Override
  public TColumn toThrift() {
    TColumn colDesc = new TColumn(name_, type_.toThrift());
    if (comment_ != null) colDesc.setComment(comment_);
    colDesc.setCol_stats(getStats().toThrift());
    colDesc.setPosition(position_);
    colDesc.setIs_hbase_column(true);
    colDesc.setColumn_family(columnFamily_);
    colDesc.setColumn_qualifier(columnQualifier_);
    colDesc.setIs_binary(binaryEncoded_);
    return colDesc;
  }
}
