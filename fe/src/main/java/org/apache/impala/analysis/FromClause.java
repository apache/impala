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

import org.apache.impala.analysis.TableRef.ZippingUnnestType;
import org.apache.impala.catalog.Type;
import org.apache.impala.catalog.iceberg.IcebergMetadataTable;
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

  public boolean isAnalyzed() { return analyzed_; }

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

    TableRef firstZippingUnnestRef = null;
    TableRef leftTblRef = null;  // the one to the left of tblRef
    boolean hasJoiningUnnest = false;
    for (int i = 0; i < tableRefs_.size(); ++i) {
      TableRef tblRef = tableRefs_.get(i);
      tblRef = analyzer.resolveTableRef(tblRef);
      tableRefs_.set(i, Preconditions.checkNotNull(tblRef));
      tblRef.setLeftTblRef(leftTblRef);
      tblRef.analyze(analyzer);
      leftTblRef = tblRef;
      if (tblRef instanceof CollectionTableRef) {
        checkIcebergCollectionSupport((CollectionTableRef)tblRef);
        checkTopLevelComplexAcidScan(analyzer, (CollectionTableRef)tblRef);
        if (firstZippingUnnestRef != null && tblRef.isZippingUnnest() &&
            firstZippingUnnestRef.getResolvedPath().getRootTable() !=
            tblRef.getResolvedPath().getRootTable()) {
          throw new AnalysisException("Not supported to do zipping unnest on " +
              "arrays from different tables.");
        }
        if (!tblRef.isZippingUnnest()) {
          hasJoiningUnnest = true;
        } else {
          if (!isPathForArrayType(tblRef)) {
            throw new AnalysisException("Unnest operator is only supported for arrays. " +
                ToSqlUtils.getPathSql(tblRef.getPath()));
          }
          if (firstZippingUnnestRef == null) firstZippingUnnestRef = tblRef;
          analyzer.addZippingUnnestTupleId((CollectionTableRef)tblRef);
          analyzer.increaseZippingUnnestCount();
        }
      }
    }
    if (hasJoiningUnnest && firstZippingUnnestRef != null) {
      throw new AnalysisException(
          "Providing zipping and joining unnests together is not supported.");
    }
    analyzed_ = true;
  }

  private boolean isPathForArrayType(TableRef tblRef) {
    Preconditions.checkNotNull(tblRef);
    Preconditions.checkState(!tblRef.getResolvedPath().getMatchedTypes().isEmpty());
    Type resolvedType =
        tblRef.getResolvedPath().getMatchedTypes().get(
            tblRef.getResolvedPath().getMatchedTypes().size() - 1);
    return resolvedType.isArrayType();
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
    if (collRef.getCollectionExpr() != null) return;
    // Don't do any checks of the collection that came from a view as getTable() would
    // return null in that case.
    if (collRef.getTable() == null) return;
    if (!AcidUtils.isFullAcidTable(
        collRef.getTable().getMetaStoreTable().getParameters())) {
      return;
    }
    analyzer.setHasTopLevelAcidCollectionTableRef();
  }

  private void checkIcebergCollectionSupport(CollectionTableRef tblRef)
      throws AnalysisException {
    Preconditions.checkNotNull(tblRef);
    Preconditions.checkNotNull(tblRef.getDesc());
    Preconditions.checkNotNull(tblRef.getDesc().getPath());
    Preconditions.checkNotNull(tblRef.getDesc().getPath().getRootTable());
    // IMPALA-12853: Collection types in FROM clause for Iceberg Metadata Tables
    if (tblRef.getDesc().getPath().getRootTable() instanceof IcebergMetadataTable) {
      throw new AnalysisException("Querying collection types (ARRAY/MAP) in FROM " +
          "clause is not supported for Iceberg Metadata tables.");
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
        Path oldPath = origTblRef.getResolvedPath();
        if (oldPath.getRootDesc() == null
            || !oldPath.getRootDesc().getType().isCollectionStructType()) {
          newTblRef.rawPath_ = oldPath.getFullyQualifiedRawPath();
        }
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
        TableRef tblRef = tableRefs_.get(i);
        if (tblRef.getZippingUnnestType() ==
            ZippingUnnestType.FROM_CLAUSE_ZIPPING_UNNEST) {
          // Go through all the consecutive table refs for zipping unnest and put them in
          // the same "UNNEST()".
          if (i != 0) builder.append(", ");
          builder.append("UNNEST(");
          boolean first = true;
          while(i < tableRefs_.size() && tblRef.getZippingUnnestType() ==
              ZippingUnnestType.FROM_CLAUSE_ZIPPING_UNNEST) {
            if (!first) builder.append(", ");
            if (first) first = false;
            builder.append(ToSqlUtils.getPathSql(tblRef.getPath()));
            if (++i < tableRefs_.size()) tblRef = tableRefs_.get(i);
          }
          builder.append(")");
        }
        if (i >= tableRefs_.size()) break;
        builder.append(tblRef.toSql(options));
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
