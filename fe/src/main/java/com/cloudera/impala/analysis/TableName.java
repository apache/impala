// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import com.google.common.base.Preconditions;

class TableName {
  private final String db;
  private final String tbl;

  public TableName(String db, String tbl) {
    super();
    Preconditions.checkArgument(db == null || !db.isEmpty());
    this.db = db;
    Preconditions.checkNotNull(tbl);
    this.tbl = tbl;
  }

  public String getDb() {
    return db;
  }

  public String getTbl() {
    return tbl;
  }

  public boolean isEmpty() {
    return tbl.isEmpty();
  }

  @Override
  public String toString() {
    if (db == null) {
      return tbl;
    } else {
      return db + "." + tbl;
    }
  }
}
