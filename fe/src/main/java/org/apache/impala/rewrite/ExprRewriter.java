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

package org.apache.impala.rewrite;

import java.util.List;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.common.AnalysisException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Lists;

/**
 * Helper class that drives the transformation of Exprs according to a given list of
 * ExprRewriteRules. The rules are applied as follows:
 * - a single rule is applied repeatedly to the Expr and all its children in a bottom-up
 *   fashion until there are no more changes
 * - the rule list is applied repeatedly until no rule has made any changes
 * - the rules are applied in the order they appear in the rule list
 * Keeps track of how many transformations were applied.
 */
public class ExprRewriter {
  private final static Logger LOG = LoggerFactory.getLogger(ExprRewriter.class);
  private int numChanges_ = 0;
  private final List<ExprRewriteRule> rules_;

  public ExprRewriter(List<ExprRewriteRule> rules) {
    rules_ = rules;
  }

  public ExprRewriter(ExprRewriteRule rule) {
    rules_ = Lists.newArrayList(rule);
  }

  public Expr rewrite(Expr expr, Analyzer analyzer) throws AnalysisException {
    // Keep applying the rule list until no rule has made any changes.
    int oldNumChanges;
    Expr rewrittenExpr = expr;
    do {
      oldNumChanges = numChanges_;
      for (ExprRewriteRule rule: rules_) {
        rewrittenExpr = applyRuleRepeatedly(rewrittenExpr, rule, analyzer);
      }
    } while (oldNumChanges != numChanges_);
    return rewrittenExpr;
  }

  /**
   * Applies 'rule' on the Expr tree rooted at 'expr' until there are no more changes.
   * Returns the transformed Expr or 'expr' if there were no changes.
   */
  private Expr applyRuleRepeatedly(Expr expr, ExprRewriteRule rule, Analyzer analyzer)
      throws AnalysisException {
    int oldNumChanges;
    Expr rewrittenExpr = expr;
    do {
      oldNumChanges = numChanges_;
      rewrittenExpr = applyRuleBottomUp(rewrittenExpr, rule, analyzer);
    } while (oldNumChanges != numChanges_);
    return rewrittenExpr;
  }

  /**
   * Applies 'rule' on 'expr' and all its children in a bottom-up fashion.
   * Returns the transformed Expr or 'expr' if there were no changes.
   */
  private Expr applyRuleBottomUp(Expr expr, ExprRewriteRule rule, Analyzer analyzer)
      throws AnalysisException {
    for (int i = 0; i < expr.getChildren().size(); ++i) {
      expr.setChild(i, applyRuleBottomUp(expr.getChild(i), rule, analyzer));
    }
    Expr rewrittenExpr = rule.apply(expr, analyzer);
    if (rewrittenExpr != expr) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("{} transformed {} to {}", rule.getClass().getSimpleName(),
            expr.debugString(), rewrittenExpr.debugString());
      }
      // Ensure the new expression is analyzed, so later rules will evaluate it
      rewrittenExpr.analyze(analyzer);
      ++numChanges_;
    }
    return rewrittenExpr;
  }

  public void rewriteList(List<Expr> exprs, Analyzer analyzer) throws AnalysisException {
    for (int i = 0; i < exprs.size(); ++i) exprs.set(i, rewrite(exprs.get(i), analyzer));
  }

  /**
   * Add numChanges_ of otherRewriter to this rewriter's numChanges_.
   */
  public void addNumChanges(ExprRewriter otherRewriter) {
    numChanges_ += otherRewriter.numChanges_;
  }

  public void reset() { numChanges_ = 0; }
  public boolean changed() { return numChanges_ > 0; }
  public int getNumChanges() { return numChanges_; }
}
