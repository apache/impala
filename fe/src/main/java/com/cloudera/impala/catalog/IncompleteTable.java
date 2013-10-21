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

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;

import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.common.JniUtil;
import com.cloudera.impala.thrift.TCatalogObjectType;
import com.cloudera.impala.thrift.TStatus;
import com.cloudera.impala.thrift.TStatusCode;
import com.cloudera.impala.thrift.TTable;
import com.cloudera.impala.thrift.TTableDescriptor;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Represents a table with incomplete metadata. Currently, the only use of the
 * IncompleteTable is for tables that encountered problems loading their table
 * metadata.
 * TODO: This could be extended to also be used for tables that have not yet had
 * their metadata loaded.
 */
public class IncompleteTable extends Table {
  // The cause for the incomplete metadata.
  ImpalaException cause_;

  public IncompleteTable(TableId id, Db db, String name,
      ImpalaException cause) {
    super(id, null, db, name, null);
    Preconditions.checkNotNull(cause);
    cause_ = cause;
  }

  /**
   * Returns the cause (ImpalaException) which led to this table's metadata being
   * incomplete.
   */
  public ImpalaException getCause() { return cause_; }

  @Override
  public TCatalogObjectType getCatalogObjectType() { return TCatalogObjectType.TABLE; }

  @Override
  public int getNumNodes() { throw new IllegalStateException(cause_); }

  @Override
  public TTableDescriptor toThriftDescriptor() {
    throw new IllegalStateException(cause_);
  }

  @Override
  public void load(Table oldValue, HiveMetaStoreClient client,
      org.apache.hadoop.hive.metastore.api.Table msTbl) throws TableLoadingException {
    if (cause_ instanceof TableLoadingException) {
      throw (TableLoadingException) cause_;
    } else {
      throw new TableLoadingException("Table metadata incomplete: ", cause_);
    }
  }

  @Override
  public TTable toThrift() {
    TTable table = new TTable(db.getName(), name);
    table.setId(id.asInt());
    table.setLoad_status(new TStatus(TStatusCode.INTERNAL_ERROR,
        Lists.newArrayList(JniUtil.throwableToString(cause_),
                           JniUtil.throwableToStackTrace(cause_))));
    return table;
  }
}