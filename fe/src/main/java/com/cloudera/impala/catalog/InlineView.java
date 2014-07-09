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

import com.cloudera.impala.thrift.TCatalogObjectType;
import com.cloudera.impala.thrift.TTableDescriptor;
import com.google.common.base.Preconditions;

/**
 * A fake catalog representation of an inline view that is similar to a table.
 * Inline views originating directly from the query or from a local view can only
 * be referenced by their explicit alias, so their table name is the alias. Inline views
 * instantiated from a catalog view have a db and a table name such that we can generate
 * unqualified and fully-qualified implicit aliases.
 * Fake tables for inline views do not have an id, msTbl or an owner and cannot be
 * converted to Thrift.
 */
public class InlineView extends Table {

  /**
   * C'tor for inline views that only have an explicit alias and not a real table name.
   */
  public InlineView(String alias) {
    super(null, null, null, alias, null);
    Preconditions.checkArgument(alias != null);
  }

  /**
   * C'tor for inline views instantiated from a local or catalog view.
   */
  public InlineView(Table tbl) {
    super(null, null, tbl.getDb(), tbl.getName(), null);
    Preconditions.checkArgument(tbl != null);
  }

  /**
   * This should never be called.
   */
  @Override
  public void load(Table oldValue, HiveMetaStoreClient client,
      org.apache.hadoop.hive.metastore.api.Table msTbl) {
    // An inline view is never loaded
    throw new UnsupportedOperationException("Inline View should never be loaded");
  }

  @Override
  public boolean isVirtualTable() { return true; }
  @Override
  public TCatalogObjectType getCatalogObjectType() { return TCatalogObjectType.VIEW; }

  /**
   * This should never be called.
   */
  @Override
  public TTableDescriptor toThriftDescriptor() {
    // An inline view never generate Thrift representation.
    throw new UnsupportedOperationException(
        "Inline View should not generate Thrift representation");
  }

  @Override
  public int getNumNodes() {
    throw new UnsupportedOperationException("InlineView.getNumNodes() not supported");
  }
}