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

import com.cloudera.impala.catalog.Type;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.Pair;
import com.cloudera.impala.common.Reference;

public abstract class Predicate extends Expr {
  protected boolean isEqJoinConjunct_;

  public Predicate() {
    super();
    isEqJoinConjunct_ = false;
  }

  /**
   * Copy c'tor used in clone().
   */
  protected Predicate(Predicate other) {
    super(other);
    isEqJoinConjunct_ = other.isEqJoinConjunct_;
  }

  public boolean isEqJoinConjunct() { return isEqJoinConjunct_; }
  public void setIsEqJoinConjunct(boolean v) { isEqJoinConjunct_ = v; }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (isAnalyzed_) return;
    super.analyze(analyzer);
    type_ = Type.BOOLEAN;
    // values: true/false/null
    numDistinctValues_ = 3;
  }

  /**
   * Returns true if one of the children is a slotref (possibly wrapped in a cast)
   * and the other children are all constant. Returns the slotref in 'slotRef' and
   * its child index in 'idx'.
   * This will pick up something like "col = 5", but not "2 * col = 10", which is
   * what we want.
   */
  public boolean isSingleColumnPredicate(
      Reference<SlotRef> slotRefRef, Reference<Integer> idxRef) {
    // find slotref
    SlotRef slotRef = null;
    int i = 0;
    for (; i < children_.size(); ++i) {
      slotRef = getChild(i).unwrapSlotRef(false);
      if (slotRef != null) break;
    }
    if (slotRef == null) return false;

    // make sure everything else is constant
    for (int j = 0; j < children_.size(); ++j) {
      if (i == j) continue;
      if (!getChild(j).isConstant()) return false;
    }

    if (slotRefRef != null) slotRefRef.setRef(slotRef);
    if (idxRef != null) idxRef.setRef(Integer.valueOf(i));
    return true;
  }

  public static boolean isEquivalencePredicate(Expr expr) {
    return (expr instanceof BinaryPredicate)
        && ((BinaryPredicate) expr).getOp().isEquivalence();
  }

  /**
   * If predicate is of the form "<slotref> = <slotref>", returns both SlotRefs,
   * otherwise returns null.
   */
  public Pair<SlotId, SlotId> getEqSlots() { return null; }

  /**
   * Returns the SlotRef bound by this Predicate.
   */
  public SlotRef getBoundSlot() { return null; }
}
