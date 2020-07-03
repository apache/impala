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

import org.apache.impala.common.AnalysisException;
import com.google.common.base.Preconditions;


/**
 * Representation of the group by clause within a select statement.
 */
public class GroupByClause {
  private final static Logger LOG = LoggerFactory.getLogger(GroupByClause.class);

  public enum GroupingSetsType {NONE, CUBE, SETS, ROLLUP};

  private final static int GROUPING_SET_LIMIT = 64;

  // Original grouping exprs from the parser. Never analyzed or modified.
  // Non-null.
  private final List<Expr> origGroupingExprs_;

  // The type of grouping set. Non-null.
  private final GroupingSetsType groupingSetsType_;

  // Indices of the expressions in 'origGroupingExprs_' referenced by the GROUPING SETS
  // clause. Non-null iff groupingSetsType is SETS.
  private final List<List<Integer>> groupingSetsList_;

  /////////////////////////////////////////
  // BEGIN: Members that need to be reset()
  boolean isAnalyzed_ = false;

  // ID of each of the distinct grouping sets. Each ID is a bitfield with a bit set if
  // that distinct grouping expr is part of that grouping set. Populated during analysis.
  private final List<Long> groupingIDs_;

  // The list of analyzed expressions in each distinct grouping set. Populated during
  // analysis.
  private final List<List<Expr>> analyzedGroupingSets_;

  // END: Members that need to be reset()
  /////////////////////////////////////////

  /**
   * Constructor for regular GROUP BY, ROLLUP and CUBE.
   */
  GroupByClause(List<Expr> groupingExprs,
      GroupingSetsType groupingSetsType) {
    Preconditions.checkNotNull(groupingSetsType);
    Preconditions.checkState(groupingSetsType != GroupingSetsType.SETS);
    origGroupingExprs_ = groupingExprs;
    groupingSetsType_ = groupingSetsType;
    groupingSetsList_ = null;
    groupingIDs_ = new ArrayList<>();
    analyzedGroupingSets_ = new ArrayList<>();
  }

  /**
   * Constructor for GROUPING SETS
   */
  GroupByClause(List<List<Expr>> groupingSetsList) {
    groupingSetsType_ = GroupingSetsType.SETS;
    groupingIDs_ = new ArrayList<Long>();
    groupingSetsList_ = new ArrayList<>();
    analyzedGroupingSets_ = new ArrayList<>();

    // Extract unique expressions from the list and clone into 'origGroupingExprs_'.
    origGroupingExprs_ = new ArrayList<Expr>();
    for (List<Expr> groupingSetExprs : groupingSetsList) {
      List<Integer> exprIndices = new ArrayList<>();
      groupingSetsList_.add(exprIndices);
      for (Expr e : groupingSetExprs) {
        int idx = origGroupingExprs_.indexOf(e);
        if (idx < 0) {
          idx = origGroupingExprs_.size();
          origGroupingExprs_.add(e);
        }
        exprIndices.add(idx);
      }
    }
  }

  /**
   * Constructor for unanalyzed clone of GROUPING SETS.
   */
  GroupByClause(List<Expr> groupingExprs, List<List<Integer>> groupingSetsList) {
    groupingSetsType_ = GroupingSetsType.SETS;
    groupingIDs_ = new ArrayList<Long>();
    groupingSetsList_ = groupingSetsList;
    origGroupingExprs_ = groupingExprs;
    analyzedGroupingSets_ = new ArrayList<>();
  }

  public List<Expr> getOrigGroupingExprs() { return origGroupingExprs_; }
  public List<List<Expr>> getAnalyzedGroupingSets() {
    Preconditions.checkState(isAnalyzed_);
    return analyzedGroupingSets_;
  }

  /**
   * Add a new grouping ID if it is not already present in 'groupingIDs_'. If it is a new
   * grouping ID, expand the list of expressions it references and append them to
   * 'analyzedGroupingSets_'.
   * @param groupingExprs duplicated list of analyzed grouping exprs
   * @param addtlGroupingExprs additional grouping exprs that will be added to each
   *    grouping set, e.g. correlated columns from a subquery rewrite.
   */
  private void addGroupingID(long id, List<Expr> groupingExprs,
      List<Expr> addtlGroupingExprs) throws AnalysisException {
    Preconditions.checkState(id >= 0 && id < (1L << groupingExprs.size()),
        "bad id: " + id);
    if (groupingIDs_.contains(id)) return;
    if (groupingIDs_.size() >= GROUPING_SET_LIMIT) {
      throw new AnalysisException("Limit of " + GROUPING_SET_LIMIT +
          " grouping sets exceeded");
    }
    groupingIDs_.add(id);

    List<Expr> groupingSet = new ArrayList<>();
    for (int i = 0; i < groupingExprs.size(); ++i) {
      Expr groupingExpr = groupingExprs.get(i);
      if ((id & (1L << i)) != 0) {
        groupingSet.add(groupingExpr);
      } else {
        // Populate NULL slots so that tuple layouts are identical.
        // TODO: it would probably be more efficient to omit these slots and handle it
        // in transposition.
        groupingSet.add(NullLiteral.create(groupingExpr.getType()));
      }
    }
    // The additional grouping expressions are a part of each grouping set.
    groupingSet.addAll(addtlGroupingExprs);
    analyzedGroupingSets_.add(groupingSet);
  }

  public boolean hasGroupingSets() {
    Preconditions.checkState(groupingSetsType_ != null);
    return groupingSetsType_ != GroupingSetsType.NONE;
  }

  public String getTypeString() {
    return hasGroupingSets() ? groupingSetsType_.toString() : "";
  }

  public GroupByClause clone() {
    List<Expr> groupingExprsClone = Expr.cloneList(origGroupingExprs_);
    if (groupingSetsType_ == GroupingSetsType.SETS) {
      // Safe to share 'groupingSetsList_' because it is not mutated.
      return new GroupByClause(groupingExprsClone, groupingSetsList_);
    } else {
      return new GroupByClause(groupingExprsClone, groupingSetsType_);
    }
  }

  /**
   * Analyze the grouping sets and generate grouping IDs for them.
   * Should not be called if hasGroupingSets() is false.
   * @param groupingExprs grouping exprs from SelectStmt, which must line up with the
   *    original grouping exprs, but can have additional exprs appended on end. These
   *    grouping exprs must have ordinals and aliases substituted, i.e. equivalent
   *    expressions in the list must compare equals with Expr.equals().
   */
  public void analyzeGroupingSets(List<Expr> groupingExprs) throws AnalysisException {
    Preconditions.checkState(hasGroupingSets());
    Preconditions.checkState(groupingExprs.size() >= origGroupingExprs_.size());
    Preconditions.checkState(groupingIDs_.isEmpty());
    Preconditions.checkState(analyzedGroupingSets_.isEmpty());

    // Number of grouping expressions in the original clause. 'groupingExprs' may
    // have additional expressions appended at the end as a result of rewriting.
    int numOrigGroupingExprs = origGroupingExprs_.size();

    // Deduplicate the grouping exprs that may be referenced multiple times in the
    // original 'groupingExprs' map. Compute a map of each expr index in
    // 'origGroupingExprs_' to the index of the deduped expr.
    Map<Integer, Integer> firstEquivExpr = new HashMap<>();
    List<Expr> dedupedGroupingSetExprs = new ArrayList<>();
    for (int i = 0; i < numOrigGroupingExprs; ++i) {
      int j = dedupedGroupingSetExprs.indexOf(groupingExprs.get(i));
      if (j == -1) {
        firstEquivExpr.put(i, dedupedGroupingSetExprs.size());
        dedupedGroupingSetExprs.add(groupingExprs.get(i));
      } else {
        firstEquivExpr.put(i, j);
      }
    }

    List<Expr> addtlGroupingExprs =
        groupingExprs.subList(numOrigGroupingExprs, groupingExprs.size());
    Expr.removeDuplicates(addtlGroupingExprs);

    int numGroupingSetExprs = dedupedGroupingSetExprs.size();
    if (numGroupingSetExprs >= (Long.SIZE - 1)) {
      throw new AnalysisException(
        "Number of grouping columns (" + numGroupingSetExprs + ") exceeds " +
        "GROUP BY with " + getTypeString() + " limit of " + (Long.SIZE - 1));
    }

    // Generate the grouping IDs. Each grouping ID is a bitfield tracking which exprs
    // in 'dedupedGroupingSetExprs' are included in that group.
    if (groupingSetsType_ == GroupingSetsType.CUBE) {
      // Enumerate all combinations of bits, i.e. all integers [0, numGroupingSetExprs).
      // E.g. for CUBE(a, b, c), we enumerate 111, 110, 101, 100, 010, 001, 000,
      // meaning the sets (a, b, c), (b, c), (a, c), (c), (b), (a), ().
      for (long id = (1L << numGroupingSetExprs) - 1; id >= 0; id--) {
        addGroupingID(id, dedupedGroupingSetExprs, addtlGroupingExprs);
      }
    } else if (groupingSetsType_ == GroupingSetsType.ROLLUP) {
      Preconditions.checkState(numGroupingSetExprs > 0);
      // Start with all bits set, then unset the bits one-by-one to enumerate all the
      // grouping sets for rollup.
      // E.g. for ROLLUP(a, b, c), we enumerate 111, 011, 001, 000, meaning the sets
      // (a, b, c), (a, b), (a), ().
      long bit = (1L << numGroupingSetExprs);
      long id = bit - 1;
      while (bit != 0) {
        addGroupingID(id, dedupedGroupingSetExprs, addtlGroupingExprs);
        bit >>= 1;
        id &= ~bit;
      }
      Preconditions.checkState(id == 0);
    } else {
      Preconditions.checkState(groupingSetsType_ == GroupingSetsType.SETS);
      for (List<Integer> set : groupingSetsList_) {
        // Construct a mask with a bit set for each expression in the grouping set.
        // We rely on addGroupingID() to ignore duplicate masks.
        long mask = 0;
        for (int origPos : set) {
          Preconditions.checkState(origPos >= 0, "bad pos" + origPos);
          Preconditions.checkState(origPos < numOrigGroupingExprs, "bad pos " + origPos);
          int dedupedPos = firstEquivExpr.get(origPos);
          Preconditions.checkState(dedupedPos >= 0, "bad pos" + dedupedPos);
          Preconditions.checkState(dedupedPos < numGroupingSetExprs,
              "bad pos" + dedupedPos);
          mask |= (1L << dedupedPos);
        }
        addGroupingID(mask, dedupedGroupingSetExprs, addtlGroupingExprs);
      }
    }
    isAnalyzed_ = true;
  }

  /**
   * Reset analysis state from call to analyze().
   */
  void reset() {
    // origGroupingExprs_ not modified during analysis.
    // groupingIDs_ were generated during analysis, so clear them out.
    groupingIDs_.clear();
    analyzedGroupingSets_.clear();
    isAnalyzed_ = false;
  }

  /**
   * Generate SQL for this GROUP BY clause.
   * Note that if additional grouping exprs were appended during rewriting, the resulting
   * SQL may not be parseable by Impala.
   * @param groupingExprs grouping exprs from SelectStmt, which must line up with the
   *    original grouping exprs, but can have additional exprs appended on end.
   */
  public String toSql(List<Expr> groupingExprs, ToSqlOptions options) {
    StringBuilder sb = new StringBuilder();
    sb.append(" GROUP BY ");
    if (groupingSetsType_ == GroupingSetsType.NONE) {
      sb.append(Expr.toSql(groupingExprs, options));
    } else {
      if (groupingSetsType_ == GroupingSetsType.CUBE) {
        // Use the standard syntax even if input was non-standard "WITH CUBE".
        sb.append("CUBE(");
        sb.append(Expr.toSql(origGroupingExprs_, options));
        sb.append(")");
      } else if (groupingSetsType_ == GroupingSetsType.ROLLUP) {
        // Use the standard syntax even if input was non-standard "WITH ROLLUP".
        sb.append("ROLLUP(");
        sb.append(Expr.toSql(origGroupingExprs_, options));
        sb.append(")");
      } else {
        Preconditions.checkState(groupingSetsType_ == GroupingSetsType.SETS);
        sb.append("GROUPING SETS(");
        for (int i = 0; i < groupingSetsList_.size(); ++i) {
          sb.append("(");
          List<Expr> groupingSetExprs = new ArrayList<>();
          for (int exprIdx : groupingSetsList_.get(i)) {
            groupingSetExprs.add(origGroupingExprs_.get(exprIdx));
          }
          sb.append(Expr.toSql(groupingSetExprs, options));
          sb.append(")");
          sb.append((i < groupingSetsList_.size() - 1) ? ", " : "");
        }
        sb.append(")");
        // Any additional grouping exprs are not part of CUBE, ROLLUP or GROUPING SETS
        // clause and should be appended afterwards.
        if (groupingExprs.size() > origGroupingExprs_.size()) {
          sb.append(", ");
          sb.append(Expr.toSql(groupingExprs.subList(
                  origGroupingExprs_.size(), groupingExprs.size()), options));
        }
      }
    }
    return sb.toString();
  }

  public String toString() {
    return this.toSql(origGroupingExprs_, ToSqlOptions.DEFAULT);
  }

  public String debugString() {
    return this.toSql(origGroupingExprs_, ToSqlOptions.SHOW_IMPLICIT_CASTS);
  }
}
