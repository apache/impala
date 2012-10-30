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

package com.cloudera.impala.catalog;

// Describes an HBase column mapped to a Hive column (as described in the metastore).
// this.name describes the column name in Hive.
// This class adds the HBase columnFamily and columnQualifier,
// so we can read the column from HBase directly.
public class HBaseColumn extends Column implements Comparable<HBaseColumn> {
  protected String columnFamily;
  protected String columnQualifier;

  public HBaseColumn(String name, String columnFamily, String columnQualifier, PrimitiveType type,
      int position) {
    super(name, type, position);
    this.columnFamily = columnFamily;
    this.columnQualifier = columnQualifier;
  }

  public String getColumnFamily() {
    return columnFamily;
  }

  public String getColumnQualifier() {
    return columnQualifier;
  }

  @Override
  // We order the HBase columns in the matadata based on columnFamily,columnQualifier,
  // to more easily map slots from HBase's Result.raw() to target slots in the backend.
  public int compareTo(HBaseColumn o) {
    int familyCmp = columnFamily.compareTo(o.columnFamily);
    if (familyCmp != 0) {
      return familyCmp;
    }
    int qualifierCmp = columnQualifier.compareTo(o.columnQualifier);
    return qualifierCmp;
  }
}
