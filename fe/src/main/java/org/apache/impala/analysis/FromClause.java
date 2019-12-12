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

  // Whether we should perform table masking. It will replace each table/view with a
  // masked subquery if there're column masking policies for the user on this table/view.
  // Turned off for CreateView and AlterView statements since they're not actually
  // reading data.
  private boolean doTableMasking_ = true;

  public FromClause(List<TableRef> tableRefs) {
    tableRefs_ = Lists.newArrayList(tableRefs);
    // Set left table refs to ensure correct toSql() before analysis.
    for (int i = 1; i < tableRefs_.size(); ++i) {
      tableRefs_.get(i).setLeftTblRef(tableRefs_.get(i - 1));
    }
  }

  public FromClause() { tableRefs_ = new ArrayList<>(); }
  public List<TableRef> getTableRefs() { return tableRefs_; }

  public void setDoTableMasking(boolean doTableMasking) {
    doTableMasking_ = doTableMasking;
    for (TableRef tableRef : tableRefs_) {
      if (!(tableRef instanceof InlineViewRef)) continue;
      InlineViewRef viewRef = (InlineViewRef) tableRef;
      viewRef.getViewStmt().setDoTableMasking(doTableMasking);
    }
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (analyzed_) return;

    TableRef leftTblRef = null;  // the one to the left of tblRef
    for (int i = 0; i < tableRefs_.size(); ++i) {
      TableRef tblRef = tableRefs_.get(i);
      // Replace non-InlineViewRef table refs with a BaseTableRef or ViewRef.
      tblRef = analyzer.resolveTableRef(tblRef, doTableMasking_);
      tableRefs_.set(i, Preconditions.checkNotNull(tblRef));
      tblRef.setLeftTblRef(leftTblRef);
      tblRef.analyze(analyzer);
      leftTblRef = tblRef;
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

  @Override
  public FromClause clone() {
    List<TableRef> clone = new ArrayList<>();
    for (TableRef tblRef: tableRefs_) clone.add(tblRef.clone());
    return new FromClause(clone);
  }

  /**
   * Unmask, un-resolve and reset all the tableRefs.
   * TableMasking views are created by analysis so we should unwrap them to restore the
   * unmasked TableRef.
   * 'un-resolve' here means replacing resolved tableRefs with unresolved ones. To make
   * sure we get the same results in later resolution, the unresolved tableRefs should
   * use fully qualified paths. Otherwise, non-fully qualified paths might incorrectly
   * match a local view.
   * However, we don't un-resolve views because local views don't have fully qualified
   * paths. Due to this we don't unmask a TableMasking view if the underlying TableRef is
   * a view. TODO(IMPALA-9286): These may make this FromClause still dirty after reset()
   * since it doesn't come back to the state before analyze(). We should introduce fully
   * qualified paths for local views to fix this.
   */
  public void reset() {
    for (int i = 0; i < size(); ++i) {
      unmaskAndUnresolveTableRef(i);
      get(i).reset();   // Reset() recursion happens here for views
    }
    this.analyzed_ = false;
  }

  /**
   * Replace the i-th tableRef with an unmasked and unresolved one if the result is not a
   * view. See more in comments of reset().
   */
  private void unmaskAndUnresolveTableRef(int i) {
    TableRef origTblRef = get(i);
    if (!origTblRef.isResolved()
        || (origTblRef instanceof InlineViewRef && !origTblRef.isTableMaskingView())) {
      return;
    }
    // Unmasked the TableRef if it's an inline view for table masking.
    if (origTblRef.isTableMaskingView()) {
      Preconditions.checkState(origTblRef instanceof InlineViewRef);
      TableRef unMaskedTableRef = ((InlineViewRef) origTblRef).getUnMaskedTableRef();
      if (unMaskedTableRef instanceof InlineViewRef) return;
      // Migrate back the properties (e.g. join ops, hints) since we are going to
      // replace it with an unresolved one.
      origTblRef.migratePropertiesTo(unMaskedTableRef);
      origTblRef = unMaskedTableRef;
    }
    // Replace the resolved TableRef with an unresolved one if it's not a view.
    if (!(origTblRef instanceof InlineViewRef)) {
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
}
