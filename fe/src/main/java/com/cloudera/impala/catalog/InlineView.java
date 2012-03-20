// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

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
    super(null, null, alias, null);
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
