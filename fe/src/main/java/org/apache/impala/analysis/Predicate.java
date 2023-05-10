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

import java.util.Optional;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.Pair;
import org.apache.impala.common.Reference;

public abstract class Predicate extends Expr {
  protected boolean isEqJoinConjunct_;
  // true if this predicate has an always_true hint
  protected boolean hasAlwaysTrueHint_;
  // cache prior shouldConvertToCNF checks to avoid repeat tree walking
  // omitted from clone in case cloner plans to mutate the expr
  protected Optional<Boolean> shouldConvertToCNF_ = Optional.empty();
  // Reserve 'SELECTIVITY' hint value from query to replace original selectivity
  // computing in sql analysis phase.
  // Default value is -1.0, means no selectivity hint set.
  // The allowed values is (0,1], 1 means all records are eligible, 0 is not allowed,
  // 0 makes no sense for a query.
  protected double selectivityHint_;

  public Predicate() {
    super();
    isEqJoinConjunct_ = false;
    hasAlwaysTrueHint_ = false;
    selectivityHint_ = -1.0;
  }

  /**
   * Copy c'tor used in clone().
   */
  protected Predicate(Predicate other) {
    super(other);
    isEqJoinConjunct_ = other.isEqJoinConjunct_;
    hasAlwaysTrueHint_ = other.hasAlwaysTrueHint_;
    selectivityHint_ = other.selectivityHint_;
  }

  public void setIsEqJoinConjunct(boolean v) { isEqJoinConjunct_ = v; }
  public void setHasAlwaysTrueHint(boolean v) { hasAlwaysTrueHint_ = v; }

  @Override
  protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
    type_ = Type.BOOLEAN;
    // values: true/false/null
    numDistinctValues_ = 3;
    analyzeHints(analyzer);

    analyzeSelectivityHint(analyzer);
  }

  /**
   * Set selectivity_ if this predicate has a selectivity hint, and value is legal.
   * Otherwise, Impala will print a warning msg, and ignore this hint value.
   */
  protected void analyzeSelectivityHint(Analyzer analyzer) {
    if (selectivityHint_ >= 0) {
      // If we set a negative number in selectivity hint, the query will throw
      // 'Syntax error' exception directly, so the 'selectivityHint_' is always larger
      // than or equal to zero here.
      if (selectivityHint_ == 0 || selectivityHint_ > 1.0) {
        analyzer.addWarning("Invalid selectivity hint value: " + selectivityHint_ +
            ", allowed value should be a double value in (0, 1].");
      } else {
        selectivity_ = selectivityHint_;
      }
    }
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

  private boolean lookupShouldConvertToCNF() {
    for (int i = 0; i < children_.size(); ++i) {
      if (!getChild(i).shouldConvertToCNF()) return false;
    }
    return true;
  }

  /**
   * Return true if this predicate's children should be converted to CNF.
   * Predicates that are considered expensive can override to return false.
   */
  @Override
  public boolean shouldConvertToCNF() {
    if (shouldConvertToCNF_.isPresent()) {
      return shouldConvertToCNF_.get();
    }
    boolean result = lookupShouldConvertToCNF();
    shouldConvertToCNF_ = Optional.of(result);
    return result;
  }

  public static boolean isEquivalencePredicate(Expr expr) {
    return (expr instanceof BinaryPredicate)
        && ((BinaryPredicate) expr).getOp().isEquivalence();
  }

  public static boolean isSqlEquivalencePredicate(Expr expr) {
    return (expr instanceof BinaryPredicate)
        && ((BinaryPredicate) expr).getOp().isSqlEquivalence();
  }

  public static boolean isSingleRangePredicate(Expr expr) {
    return (expr instanceof BinaryPredicate)
        && ((BinaryPredicate) expr).getOp().isSingleRange();
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

  public boolean hasAlwaysTrueHint() { return hasAlwaysTrueHint_; }

  public void setSelectivityHint(double selectivityHint) {
    this.selectivityHint_ = selectivityHint;
  }

  /**
   * Valid selectivity hint is (0,1], return true if hint value is in the range.
   */
  public boolean hasValidSelectivityHint() {
    return selectivityHint_ > 0 && selectivityHint_ <= 1.0;
  }
}
