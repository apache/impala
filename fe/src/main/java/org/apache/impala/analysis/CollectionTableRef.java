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

import org.apache.impala.authorization.Privilege;
import org.apache.impala.catalog.FeView;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import com.google.common.base.Preconditions;

/**
 * Reference to a MAP or ARRAY collection type. It can be either being
 * flattened during execution or returned in a select list (as json strings)/
 * TODO: We currently create a new slot in the root tuple descriptor for every
 * relative collection ref, even if they have the same path. The BE currently relies on
 * this behavior for setting collection slots to NULL after they have been unnested
 * inside a SubplanNode. We could instead share the slot and the corresponding item tuple
 * descriptor among all collection table refs with the same path. This change will
 * require decoupling tuple descriptors from table aliases, i.e., a tuple descriptor
 * should be able to back multiple aliases.
 */
public class CollectionTableRef extends TableRef {
  /////////////////////////////////////////
  // BEGIN: Members that need to be reset()

  // Expr that returns the referenced collection. Typically a SlotRef into the
  // parent scan's tuple. Result of analysis. Fully resolved against base tables.
  private Expr collectionExpr_;

  // If true, then the collection won't be flattened.
  private boolean inSelectList_ = false;

  // END: Members that need to be reset()
  /////////////////////////////////////////

  /**
   * Create a CollectionTableRef from the original unresolved table ref as well as
   * its resolved path. Sets table aliases and join-related attributes.
   * If inSelectList is true, then the collection won't be flattened during execution.
   */
  public CollectionTableRef(TableRef tableRef, Path resolvedPath, boolean inSelectList) {
    super(tableRef);
    Preconditions.checkState(resolvedPath.isResolved());
    resolvedPath_ = resolvedPath;
    inSelectList_ = inSelectList;
    // Use the last path element as an implicit alias if no explicit alias was given.
    if (hasExplicitAlias()) return;
    String implicitAlias = rawPath_.get(rawPath_.size() - 1).toLowerCase();
    if (rawPath_.size() > 1) {
      // Use the full path from the root table alias to be able to distinguish
      // among collection columns with the same name.
      aliases_ =
          new String[] { String.join(".", rawPath_).toLowerCase(), implicitAlias };
    } else {
      aliases_ = new String[] { implicitAlias };
    }
  }

  /**
   * C'tor for cloning.
   */
  public CollectionTableRef(CollectionTableRef other) {
    super(other);
    inSelectList_ = other.inSelectList_;
    collectionExpr_ =
        (other.collectionExpr_ != null) ? other.collectionExpr_.clone() : null;
  }

  /**
   * Registers this collection table ref with the given analyzer and adds a slot
   * descriptor for the materialized collection to be populated by parent scan.
   * Also determines whether this collection table ref is correlated or not.
   *
   * If this function is called in the context of analyzing a WITH clause, then
   * no slot is added to the parent descriptor so as to not pollute the analysis
   * state of the parent block (the WITH-clause analyzer is discarded, and the
   * parent analyzer could have an entirely different global state). This is not
   * applied if inSelectList_ is true, as the parent is analysed with the same
   * analyser in that case.
   */
  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (isAnalyzed_) return;
    InlineViewRef sourceView = null;
    if (resolvedPath_.getRootDesc() != null) {
      sourceView = resolvedPath_.getRootDesc().getSourceView();
    }
    if (zippingUnnestType_ == ZippingUnnestType.FROM_CLAUSE_ZIPPING_UNNEST) {
      UnnestExpr.verifyNotInsideStruct(resolvedPath_);

      Type type = resolvedPath_.getMatchedTypes().get(
          resolvedPath_.getMatchedTypes().size() - 1);
      UnnestExpr.verifyContainsNoStruct(type);
    }
    if (sourceView != null && zippingUnnestType_ ==
        ZippingUnnestType.FROM_CLAUSE_ZIPPING_UNNEST) {
      String implicitAlias = rawPath_.get(rawPath_.size() - 1).toLowerCase();
      analyzer.addZippingUnnestTupleId(analyzer.getDescriptor(implicitAlias).getId());
      TableRef existingTableRef = analyzer.getRegisteredTableRef(getUniqueAlias());
      existingTableRef.getDesc().setHidden(false);
    }
    if (sourceView == null || inSelectList_) {
      desc_ = analyzer.registerTableRef(this);
      // Avoid polluting the namespace with collections that back arrays
      // in select list.
      if (inSelectList_) {
        setHidden(true);
        desc_.setHidden(true);
      }
    }

    if (!isRelative() && resolvedPath_.getRootTable() instanceof FeView) {
      throw new AnalysisException("Non-relative collections are currently not " +
          "supported on collections from views.");
    }

    if (isRelative() && sourceView != null && !inSelectList_) {
      // The collection is a column from a view. This means that we must reuse the
      // existing tuple desc created by the view. This is not needed when the
      // collection is in a select list, as the slot refs in the select list are
      // substituted in SelectStmt.resolveInlineViewRefs()
      // TODO: currently we cannot use the same array twice (e.g. self join) in this
      //       case
      SlotDescriptor parentSlotDesc = analyzer.getSlotDescriptor(
          resolvedPath_.getFullyQualifiedRawPath());
      collectionExpr_ = new SlotRef(parentSlotDesc);
      collectionExpr_ =
        collectionExpr_.trySubstitute(sourceView.getBaseTblSmap(), analyzer, true);
      desc_ = ((SlotRef) collectionExpr_).getDesc().getItemTupleDesc();
      // The tuple desc was hidden as it belonged to a collection in select list in
      // a view. Set hidden to false, as now it is in the from clause.
      Preconditions.checkState(desc_.isHidden());
      desc_.setHidden(false);

      analyzer.addCollectionTableRef(getUniqueAlias(), this, desc_);
    }

    if (isRelative() && (!analyzer.hasWithClause() || inSelectList_)) {
      if (sourceView == null || inSelectList_) {
        SlotDescriptor parentSlotDesc = analyzer.registerSlotRef(resolvedPath_);
        parentSlotDesc.setItemTupleDesc(desc_);
        collectionExpr_ = new SlotRef(parentSlotDesc);
      }

      // If unnested then must always be materialized to ensure the correct cardinality
      // after unnesting.
      if (!inSelectList_) analyzer.materializeSlots(collectionExpr_);

      TupleId rootTupleId = resolvedPath_.getRootDesc().getId();
      if (resolvedPath_.isMaskedPath()) {
        // Use tuple id of the table masking view so we can find it in current block or
        // parent blocks. See how such kind of Paths are resolved in
        // Analyzer#resolvePathWithMasking().
        Preconditions.checkState(resolvedPath_.getPathBeforeMasking().isRootedAtTuple());
        rootTupleId = resolvedPath_.getPathBeforeMasking().getRootDesc().getId();
      }
      Analyzer parentAnalyzer = analyzer.findAnalyzer(rootTupleId);
      Preconditions.checkNotNull(parentAnalyzer);
      if (parentAnalyzer != analyzer) {  // Correlated to a TableRef in a parent block.
        TableRef parentRef = parentAnalyzer.getTableRef(rootTupleId);
        Preconditions.checkNotNull(parentRef);
        // InlineViews are currently not supported as a parent ref expects
        // TableMaskingViews.
        Preconditions.checkState(!(parentRef instanceof InlineViewRef)
            || parentRef.isTableMaskingView());
        correlatedTupleIds_.add(resolvedPath_.getRootDesc().getId());
      }
    }
    if (!isRelative()) {
      // Register a table-level privilege request as well as a column-level privilege request
      // for the collection-typed column.
      Preconditions.checkNotNull(resolvedPath_.getRootTable());
      analyzer.registerAuthAndAuditEvent(resolvedPath_.getRootTable(), priv_,
          requireGrantOption_);
      analyzer.registerPrivReq(builder ->
          builder.allOf(Privilege.SELECT)
              .onColumn(desc_.getTableName().getDb(), desc_.getTableName().getTbl(),
                  desc_.getPath().getRawPath().get(0),
                  resolvedPath_.getRootTable().getOwnerUser()).build());
    }
    isAnalyzed_ = true;
    analyzeTableSample(analyzer);
    analyzeHints(analyzer);

    // TODO: For joins on nested collections some join ops can be simplified
    // due to the containment relationship of the parent and child. For example,
    // a FULL OUTER JOIN would become a LEFT OUTER JOIN, or a RIGHT SEMI JOIN
    // would become an INNER or CROSS JOIN.
    analyzeJoin(analyzer);
  }

  @Override
  public boolean isRelative() {
    Preconditions.checkNotNull(resolvedPath_);
    return resolvedPath_.getRootDesc() != null;
  }

  public Expr getCollectionExpr() { return collectionExpr_; };

  @Override
  public boolean isCollectionInSelectList() { return inSelectList_; }

  @Override
  protected CollectionTableRef clone() { return new CollectionTableRef(this); }

  @Override
  public void reset() {
    super.reset();
    collectionExpr_ = null;
    inSelectList_ = false;
  }
}
