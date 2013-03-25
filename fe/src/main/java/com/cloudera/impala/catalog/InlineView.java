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

import java.util.List;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;

import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.planner.DataSink;
import com.cloudera.impala.thrift.TTableDescriptor;
import com.google.common.base.Preconditions;

/**
 * A fake catalog representation of an inline view. It's like a table. It has name
 * and columns, but it won't have ids and it shouldn't be converted to Thrift.
 * An inline view is constructed by providing an alias (required) and then adding columns
 * one-by-one.
 */
public class InlineView extends Table {

  /**
   * An inline view only has an alias and columns, but it won't have id, db and owner.
   * @param alias alias of the inline view; must not be null;
   */
  public InlineView(String alias) {
    super(null, null, null, alias, null);
    Preconditions.checkArgument(alias != null);
  }

  /**
   * An inline view has to be constructed by adding columns one by one.
   * @param col Column of the inline view
   */
  public void addColumn(Column col) {
    colsByPos.add(col);
    colsByName.put(col.getName(), col);
  }

  /**
   * This should never be called.
   */
  @Override
  public Table load(HiveMetaStoreClient client,
      org.apache.hadoop.hive.metastore.api.Table msTbl) {
    // An inline view is never loaded
    throw new UnsupportedOperationException("Inline View should never be loaded");
  }

  /**
   * This should never be called.
   */
  @Override
  public TTableDescriptor toThrift() {
    // An inline view never generate Thrift representation.
    throw new UnsupportedOperationException(
        "Inline View should not generate Thrift representation");
  }

  /**
   * This should never be called.
   */
  @Override
  public DataSink createDataSink(List<Expr> partitionKeyExprs, boolean overwrite) {
    // An inlinview should never be a target of a n insert.
    throw new UnsupportedOperationException(
        "Inline View should never be the target of an insert");
  }

}
