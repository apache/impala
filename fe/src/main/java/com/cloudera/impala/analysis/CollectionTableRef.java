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

package com.cloudera.impala.analysis;

import com.cloudera.impala.authorization.Privilege;
import com.cloudera.impala.authorization.PrivilegeRequestBuilder;
import com.cloudera.impala.catalog.Column;
import com.cloudera.impala.common.AnalysisException;
import com.google.common.base.Preconditions;

/**
 * Reference to a MAP or ARRAY collection type that implies its
 * flattening during execution.
 * TODO: We currently create a new slot in the root tuple descriptor for every
 * relative collection ref, even if they have the same path. We should instead
 * share the slot and the corresponding item tuple descriptor among all collection
 * table refs with the same path. This change will require decoupling tuple
 * descriptors from table aliases, i.e., a tuple descriptor should be able to back
 * multiple aliases.
 */
public class CollectionTableRef extends TableRef {
  /////////////////////////////////////////
  // BEGIN: Members that need to be reset()

  // Expr that returns the referenced collection. Typically a SlotRef into the
  // parent scan's tuple. Result of analysis. Fully resolved against base tables.
  private Expr collectionExpr_;

  // END: Members that need to be reset()
  /////////////////////////////////////////

  /**
   * Create a CollectionTableRef for the given collection type from the original
   * unresolved table ref. Sets the explicit alias and the join-related attributes
   * of the new collection table ref from the unresolved table ref.
   */
  public CollectionTableRef(TableRef tableRef) {
    super(tableRef);
    // Use the last path element as an implicit alias if no explicit alias was given.
    if (hasExplicitAlias()) return;
    String implicitAlias = rawPath_.get(rawPath_.size() - 1).toLowerCase();
    aliases_ = new String[] { implicitAlias };
  }

  /**
   * C'tor for cloning.
   */
  public CollectionTableRef(CollectionTableRef other) {
    super(other);
    collectionExpr_ =
        (other.collectionExpr_ != null) ? other.collectionExpr_.clone() : null;
  }

  /**
   * Registers this collection table ref with the given analyzer and adds a slot
   * descriptor for the materialized collection to be populated by parent scan.
   * Also determines whether this collection table ref is correlated or not.
   */
  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (isAnalyzed_) return;
    Preconditions.checkNotNull(getPrivilegeRequirement());
    desc_ = analyzer.registerTableRef(this);
    if (isRelative()) {
      SlotDescriptor parentSlotDesc = analyzer.registerSlotRef(resolvedPath_);
      parentSlotDesc.setItemTupleDesc(desc_);
      collectionExpr_ = new SlotRef(parentSlotDesc);
      // Must always be materialized to ensure the correct cardinality after unnesting.
      analyzer.materializeSlots(collectionExpr_);
      Analyzer parentAnalyzer =
          analyzer.findAnalyzer(resolvedPath_.getRootDesc().getId());
      Preconditions.checkNotNull(parentAnalyzer);
      if (parentAnalyzer != analyzer) {
        TableRef parentRef =
            parentAnalyzer.getTableRef(resolvedPath_.getRootDesc().getId());
        Preconditions.checkNotNull(parentRef);
        // InlineViews are currently not supported as a parent ref.
        Preconditions.checkState(!(parentRef instanceof InlineViewRef));
        correlatedTupleIds_.add(parentRef.getId());
      }
    } else {
      // Register a column-level privilege request for the collection-typed column.
      analyzer.registerPrivReq(new PrivilegeRequestBuilder().
          allOf(Privilege.SELECT).onColumn(desc_.getTableName().getDb(),
          desc_.getTableName().getTbl(), desc_.getPath().getRawPath().get(0))
          .toRequest());
    }
    isAnalyzed_ = true;
    analyzeJoin(analyzer);
  }

  @Override
  public boolean isRelative() {
    Preconditions.checkNotNull(resolvedPath_);
    return resolvedPath_.getRootDesc() != null;
  }

  public Expr getCollectionExpr() { return collectionExpr_; }

  @Override
  protected CollectionTableRef clone() { return new CollectionTableRef(this); }

  @Override
  public void reset() {
    super.reset();
    collectionExpr_ = null;
  }
}
