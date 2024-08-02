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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.apache.impala.service.BackendConfig;

/**
 * Map of expression substitutions: lhs[i] gets substituted with rhs[i].
 * To support expression substitution across query blocks, rhs exprs must already be
 * analyzed when added to this map. Otherwise, analysis of a SlotRef may fail after
 * substitution, e.g., because the table it refers to is in a different query block
 * that is not visible.
 * See Expr.substitute() and related functions for details on the actual substitution.
 */
public final class ExprSubstitutionMap {
  private final static Logger LOG = LoggerFactory.getLogger(ExprSubstitutionMap.class);

  // This essentially implements a LinkedHashMap. So much of the interface relies on
  // interacting with lhs and rhs directly that it currently makes sense to track the
  // HashMap ourselves.
  private List<Expr> lhs_; // left-hand side
  private List<Expr> rhs_; // right-hand side
  private Map<Expr, Expr> substitutions_; // lhs -> rhs

  public ExprSubstitutionMap() {
    this(new ArrayList<>(), new ArrayList<>());
  }

  public ExprSubstitutionMap(List<Expr> lhs, List<Expr> rhs) {
    Preconditions.checkArgument(lhs.size() == rhs.size());
    lhs_ = lhs;
    rhs_ = rhs;
    buildMap();
    verify();
  }

  private void buildMap() {
    // Build lookup map and ensure keys are unique.
    substitutions_ = new HashMap<>();
    List<Integer> toRemove = new ArrayList<>();
    for (int i = 0; i < lhs_.size(); i++) {
      Expr existingVal = substitutions_.putIfAbsent(lhs_.get(i), rhs_.get(i));
      if (existingVal != null) {
        toRemove.add(i);
      }
    }

    Collections.reverse(toRemove);
    for (int i: toRemove) {
      lhs_.remove(i);
      rhs_.remove(i);
    }
  }

  /**
   * Add an expr mapping. The rhsExpr must be analyzed to support correct substitution
   * across query blocks. It is not required that the lhsExpr is analyzed.
   */
  public void put(Expr lhsExpr, Expr rhsExpr) {
    if (substitutions_.containsKey(lhsExpr)) {
      // Some construction, such as AggregateInfo#createDistinctAggInfo, may attempt to
      // add entries that already exist. When that happens, ignore the new entries.
      return;
    }
    Preconditions.checkState(rhsExpr.isAnalyzed(), "Rhs expr must be analyzed.");
    lhs_.add(lhsExpr);
    rhs_.add(rhsExpr);
    substitutions_.put(lhsExpr, rhsExpr);
  }

  /**
   * Returns the expr mapped to lhsExpr or null if no mapping to lhsExpr exists.
   */
  public Expr get(Expr lhsExpr) {
    return substitutions_.get(lhsExpr);
  }

  /**
   * Returns true if the smap contains a mapping for lhsExpr.
   */
  public boolean containsMappingFor(Expr lhsExpr) {
    return substitutions_.containsKey(lhsExpr);
  }

  /**
   * Return a map which is equivalent to applying f followed by g,
   * i.e., g(f()).
   * Always returns a non-null map. LHS is a shallow clone of f+g LHS,
   * RHS are deep-cloned Exprs.
   */
  public static ExprSubstitutionMap compose(ExprSubstitutionMap f, ExprSubstitutionMap g,
      Analyzer analyzer) {
    if (f == null && g == null) return new ExprSubstitutionMap();
    if (f == null) return g;
    if (g == null) return f;
    // f's substitution targets need to be substituted via g
    ExprSubstitutionMap result = new ExprSubstitutionMap(new ArrayList<>(f.lhs_),
        Expr.substituteList(f.rhs_, g, analyzer, false));

    // substitution maps are cumulative: the combined map contains all
    // substitutions from f and g.
    for (int i = 0; i < g.lhs_.size(); i++) {
      // If f contains expr1->fn(expr2) and g contains expr2->expr3,
      // then result must contain expr1->fn(expr3).
      // The check before adding to result.lhs is to ensure that cases
      // where expr2.equals(expr1) are handled correctly.
      // For example f: count(*) -> zeroifnull(count(*))
      // and g: count(*) -> slotref
      // result.lhs must only have: count(*) -> zeroifnull(slotref) from f above,
      // and not count(*) -> slotref from g as well.
      result.put(g.lhs_.get(i), g.rhs_.get(i).clone());
    }
    return result;
  }

  /**
   * Returns the union of two substitution maps. Always returns a non-null map.
   */
  public static ExprSubstitutionMap combine(ExprSubstitutionMap f,
      ExprSubstitutionMap g) {
    if (f == null && g == null) return new ExprSubstitutionMap();
    if (f == null) return g;
    if (g == null) return f;
    return new ExprSubstitutionMap(
      Lists.newArrayList(Iterables.concat(f.lhs_, g.lhs_)),
      Lists.newArrayList(Iterables.concat(f.rhs_, g.rhs_)));
  }

  /**
   * Maps keys based on lhsSmap, retaining original values.
   */
  public void substituteLhs(ExprSubstitutionMap lhsSmap, Analyzer analyzer) {
    lhs_ = Expr.substituteList(lhs_, lhsSmap, analyzer, false);
    buildMap();
    verify();
  }

  public List<Expr> getLhs() { return lhs_; }
  public List<Expr> getRhs() { return rhs_; }

  public int size() { return lhs_.size(); }

  public String debugString() {
    Preconditions.checkState(lhs_.size() == rhs_.size());
    List<String> output = new ArrayList<>();
    for (int i = 0; i < lhs_.size(); ++i) {
      output.add(lhs_.get(i).toSql() + ":" + rhs_.get(i).toSql());
      output.add("(" + lhs_.get(i).debugString() + ":" + rhs_.get(i).debugString() + ")");
    }
    return "smap(" + Joiner.on(" ").join(output) + ")";
  }

  /**
   * Verifies the internal state of this smap: Checks that lhs/rhs and substitutions map
   * are consistent and that all rhs exprs are analyzed.
   */
  private void verify() {
    Preconditions.checkState(lhs_.size() == rhs_.size());
    Preconditions.checkState(substitutions_.size() == lhs_.size());

    // Skip checking every expression on release builds to avoid the overhead.
    if (BackendConfig.INSTANCE.isReleaseBuild()) return;
    for (int i = 0; i < lhs_.size(); i++) {
      Preconditions.checkState(substitutions_.get(lhs_.get(i)).equals(rhs_.get(i)));
      Preconditions.checkState(rhs_.get(i).isAnalyzed());
    }
  }

  public void clear() {
    lhs_.clear();
    rhs_.clear();
    substitutions_.clear();
  }

  /**
   * Performs deep clone on the RHS list. LHS list is a shallow clone.
   */
  @Override
  public ExprSubstitutionMap clone() {
    return new ExprSubstitutionMap(new ArrayList<>(lhs_), Expr.cloneList(rhs_));
  }

  /**
   * Returns whether we are composed from the other map.
   */
  public boolean checkComposedFrom(ExprSubstitutionMap other) {
    // If g is composed from f, then g(x) = h(f(x)), so "f(a) == f(b)" => "g(a) == g(b)".
    for (int i = 0; i < other.lhs_.size() - 1; ++i) {
      for (int j = i + 1; j < other.lhs_.size(); ++j) {
        Expr a = other.lhs_.get(i);
        Expr b = other.lhs_.get(j);
        Expr finalA = get(a);
        Expr finalB = get(b);
        if (finalA == null || finalB == null) {
          if (LOG.isTraceEnabled()) {
            if (finalA == null) {
              LOG.trace("current smap misses item for " + a.debugString());
            }
            if (finalB == null) {
              LOG.trace("current smap misses item for " + b.debugString());
            }
          }
          return false;
        }
        if (other.rhs_.get(i).equals(other.rhs_.get(j)) && !finalA.equals(finalB)) {
          // f(a) == f(b) but g(a) != g(b)
          if (LOG.isTraceEnabled()) {
            LOG.trace(String.format("smap conflicts in substituting %s and %s. Result" +
                " of the base map: %s. Results of current map: %s and %s",
                a.debugString(), b.debugString(), other.rhs_.get(i).debugString(),
                finalA.debugString(), finalB.debugString()));
          }
          return false;
        }
      }
    }
    return true;
  }
}
