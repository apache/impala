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

package org.apache.impala.catalog.local;

import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.impala.analysis.QueryStmt;
import org.apache.impala.catalog.FeView;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.catalog.View;
import org.apache.impala.catalog.local.MetaProvider.TableMetaRef;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TImpalaTableType;
import org.apache.impala.thrift.TTableDescriptor;

/**
 * View implementation corresponding to the LocalCatalog.
 *
 * NOTE: this does _not_ implement "local views" in the sense of a view defined
 * in the scope of a query using a WITH expression.
 */
public class LocalView extends LocalTable implements FeView {
  private final QueryStmt queryStmt_;

  public LocalView(LocalDb db, Table msTbl, TableMetaRef ref) {
    super(db, msTbl, ref);

    try {
      queryStmt_ = View.parseViewDef(this);
    } catch (TableLoadingException e) {
      throw new LocalCatalogException(e);
    }
  }

  @Override
  public TImpalaTableType getTableType() {
    return TImpalaTableType.VIEW;
  }

  @Override
  public TTableDescriptor toThriftDescriptor(int tableId,
      Set<Long> referencedPartitions) {
    // Views are always resolved into underlying tables before being serialized.
    throw new IllegalStateException("Cannot call toThriftDescriptor() on a view.");
  }

  // NOTE: "local view" in this context means "a view defined local to a query"
  // whereas "local" in the class name "LocalView" means "view from the
  // LocalCatalog catalog implementation".
  @Override
  public boolean isLocalView() {
    // TODO: the org.apache.impala.catalog.View class is still used for local views
    // even in the LocalCatalog implementation.
    return false;
  }

  @Override
  public QueryStmt getQueryStmt() {
    return queryStmt_;
  }

  @Override
  public List<String> getColLabels() {
    // Explicit column labels are only used by local views.
    // Returning null indicates that the column labels are derived
    // from the underlying statement.
    return null;
  }

  @Override
  public TCatalogObjectType getCatalogObjectType() {
    return TCatalogObjectType.VIEW;
  }
}
