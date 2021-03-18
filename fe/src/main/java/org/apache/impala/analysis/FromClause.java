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

package org.apache.impala.analysis;

import static org.apache.impala.analysis.ToSqlOptions.DEFAULT;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.impala.common.AnalysisException;
import org.apache.impala.util.AcidUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Wraps a list of TableRef instances that form a FROM clause, allowing them to be
 * analyzed independently of the statement using them. To increase the flexibility of
 * the class it implements the Iterable interface.
 */
public class FromClause extends StmtNode implements Iterable<TableRef> {
  private final List<TableRef> tableRefs_;

  private boolean analyzed_ = false;

  public FromClause(List<TableRef> tableRefs) {
    tableRefs_ = Lists.newArrayList(tableRefs);
    // Set left table refs to ensure correct toSql() before analysis.
    for (int i = 1; i < tableRefs_.size(); ++i) {
      tableRefs_.get(i).setLeftTblRef(tableRefs_.get(i - 1));
    }
  }

  public FromClause() { tableRefs_ = new ArrayList<>(); }
  public List<TableRef> getTableRefs() { return tableRefs_; }

  @Override
  public boolean resolveTableMask(Analyzer analyzer) throws AnalysisException {
    boolean hasChanges = false;
    for (int i = 0; i < size(); ++i) {
      TableRef origRef = get(i);
      if (origRef instanceof InlineViewRef) {
        hasChanges |= ((InlineViewRef) origRef).getViewStmt().resolveTableMask(analyzer);
        // Skip local views
        if (!((InlineViewRef) origRef).isCatalogView()) continue;
      }
      TableRef newRef = analyzer.resolveTableMask(origRef);
      if (newRef == origRef) continue;
      set(i, newRef);
      hasChanges = true;
      Preconditions.checkState(newRef.isTableMaskingView(),
          "resolved table mask should be a view");
    }
    return hasChanges;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (analyzed_) return;

    TableRef leftTblRef = null;  // the one to the left of tblRef
    for (int i = 0; i < tableRefs_.size(); ++i) {
      TableRef tblRef = tableRefs_.get(i);
      tblRef = analyzer.resolveTableRef(tblRef);
      tableRefs_.set(i, Preconditions.checkNotNull(tblRef));
      tblRef.setLeftTblRef(leftTblRef);
      tblRef.analyze(analyzer);
      leftTblRef = tblRef;
      if (tblRef instanceof CollectionTableRef) {
        checkTopLevelComplexAcidScan(analyzer, (CollectionTableRef)tblRef);
      }
    }
    analyzed_ = true;
  }

  public void collectFromClauseTableRefs(List<TableRef> tblRefs) {
    collectTableRefs(tblRefs, true);
  }

  public void collectTableRefs(List<TableRef> tblRefs) {
    collectTableRefs(tblRefs, false);
  }

  private void collectTableRefs(List<TableRef> tblRefs, boolean fromClauseOnly) {
    for (TableRef tblRef: tableRefs_) {
      if (tblRef instanceof InlineViewRef) {
        InlineViewRef inlineViewRef = (InlineViewRef) tblRef;
        inlineViewRef.getViewStmt().collectTableRefs(tblRefs, fromClauseOnly);
      } else {
        tblRefs.add(tblRef);
      }
    }
  }

  private void checkTopLevelComplexAcidScan(Analyzer analyzer,
      CollectionTableRef collRef) {
    if (!AcidUtils.isFullAcidTable(
        collRef.getTable().getMetaStoreTable().getParameters())) {
      return;
    }
    if (collRef.getCollectionExpr() == null) {
      analyzer.setHasTopLevelAcidCollectionTableRef();
    }
  }

  @Override
  public FromClause clone() {
    List<TableRef> clone = new ArrayList<>();
    for (TableRef tblRef: tableRefs_) clone.add(tblRef.clone());
    return new FromClause(clone);
  }

  public void reset() {
    for (int i = 0; i < size(); ++i) {
      TableRef origTblRef = get(i);
      if (origTblRef.isResolved() && !(origTblRef instanceof InlineViewRef)) {
        // Replace resolved table refs with unresolved ones.
        TableRef newTblRef = new TableRef(origTblRef);
        // Use the fully qualified raw path to preserve the original resolution.
        // Otherwise, non-fully qualified paths might incorrectly match a local view.
        // TODO(IMPALA-9286): This full qualification preserves analysis state which is
        // contrary to the intended semantics of reset(). We could address this issue by
        // changing the WITH-clause analysis to register local views that have
        // fully-qualified table refs, and then remove the full qualification here.
        newTblRef.rawPath_ = origTblRef.getResolvedPath().getFullyQualifiedRawPath();
        set(i, newTblRef);
      }
      // recurse for views
      get(i).reset();
    }
    this.analyzed_ = false;
  }

  @Override
  public final String toSql() {
    return toSql(DEFAULT);
  }

  @Override
  public String toSql(ToSqlOptions options) {
    StringBuilder builder = new StringBuilder();
    if (!tableRefs_.isEmpty()) {
      builder.append(" FROM ");
      for (int i = 0; i < tableRefs_.size(); ++i) {
        builder.append(tableRefs_.get(i).toSql(options));
      }
    }
    return builder.toString();
  }

  public boolean isEmpty() { return tableRefs_.isEmpty(); }

  @Override
  public Iterator<TableRef> iterator() { return tableRefs_.iterator(); }
  public int size() { return tableRefs_.size(); }
  public TableRef get(int i) { return tableRefs_.get(i); }
  public void set(int i, TableRef tableRef) { tableRefs_.set(i, tableRef); }
  public void add(TableRef t) { tableRefs_.add(t); }
  public void add(int i, TableRef t) { tableRefs_.add(i, t); }
}
