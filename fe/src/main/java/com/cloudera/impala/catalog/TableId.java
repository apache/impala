// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.catalog;

import com.cloudera.impala.common.Id;

public class TableId extends Id {
  public TableId() {
    super();
  }

  public TableId(int id) {
    super(id);
  }
}
