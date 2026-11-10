/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.impala.planner.paimon;

import com.google.common.collect.Lists;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.CompoundPredicate;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.LikePredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.RowType;

import java.util.List;
import java.util.Optional;

/*
 * Predicate extractor used to extract expressions
 * which can be pushed to paimon. these expressions
 * has two category 1. partition predicates  2. others
 * for partition predicate, it is not need to recheck
 * after pushdown, unless it contains LIKE/RLIKE/REGEXP predicates
 * whose pushed predicates are not equivalent to the original ones.
 * for other predicates, we still need to recheck by
 * adding it to postscan expressions.
 * */
public class PredicateExtractor {
  /**
   * ExtractedPredicates hold the predicate pushdown result from PredicateExtractor
   * for given table and expression.
   */
  public static class ExtractedPredicates {
    private final List<Predicate> pushedPredicates_;
    private final List<Expr> pushedExprs_;
    private final List<Predicate> partitionPredicates_;
    private final List<Expr> partitionExprs_;
    private final List<Expr> postScanExprs_;
    private final PaimonJniReadFilter readFilters_;

    public ExtractedPredicates(List<Predicate> pushedPredicates, List<Expr> pushedExprs,
        List<Predicate> partitionPredicates, List<Expr> partitionExprs,
        List<Expr> postScanExprs, PaimonJniReadFilter readFilters) {
      pushedPredicates_ = pushedPredicates;
      pushedExprs_ = pushedExprs;
      partitionPredicates_ = partitionPredicates;
      partitionExprs_ = partitionExprs;
      postScanExprs_ = postScanExprs;
      readFilters_ = readFilters;
    }

    public ExtractedPredicates() {
      pushedPredicates_ = Lists.newArrayList();
      pushedExprs_ = Lists.newArrayList();
      partitionPredicates_ = Lists.newArrayList();
      partitionExprs_ = Lists.newArrayList();
      postScanExprs_ = Lists.newArrayList();
      readFilters_ = PaimonJniReadFilter.empty();
    }

    public List<Predicate> getPushedPredicates() { return pushedPredicates_; }

    public List<Predicate> getPartitionPredicates() { return partitionPredicates_; }

    public List<Expr> getPushedExprs() { return pushedExprs_; }

    public List<Expr> getPartitionExprs() { return partitionExprs_; }

    public List<Expr> getPostScanExprs() { return postScanExprs_; }

    public PaimonJniReadFilter getReadFilters() { return readFilters_; }

    /**
     * Conjuncts whose selectivity is not already reflected in split row counts.
     * Exact partition predicates are omitted. Data filters and partial LIKE
     * predicates stay here: split row counts are file row counts taken before them.
     */
    public List<Expr> getSelectivityConjuncts() { return postScanExprs_; }
  }

  /**
   * Pushes down exprs, and collect exprs that need to be evaluated after scanning.
   */
  public static ExtractedPredicates extractPaimonPredicates(
      Table table, List<Expr> exprs, Analyzer analyzer) {
    if (exprs.isEmpty()) { return new ExtractedPredicates(); }
    RowType predicateRowType = ImpalaPaimonRowTypeUtils.predicateRowType(table);
    ImpalaExprConverter converter = new ImpalaExprConverter(predicateRowType, analyzer);
    List<String> partitionKeys = table.partitionKeys();

    List<Predicate> pushedPredicates = Lists.newArrayList();
    List<Expr> pushedExprs = Lists.newArrayList();
    List<Predicate> partitionPredicates = Lists.newArrayList();
    List<Expr> partitionExprs = Lists.newArrayList();
    List<Expr> postScanExprs = Lists.newArrayList();

    for (Expr expr : flattenConjuncts(exprs)) {
      ImpalaExprConverter.ConverterResult result = converter.convertExpr(expr);
      if (result.isFailed()) {
        postScanExprs.add(expr);
        continue;
      }
      // LIKE/REGEXP approximations are not exact partition pruning. A failed remap must
      // also stay in Impala: partition filters are not re-checked after the scan.
      Optional<Predicate> remapped = expr.contains(LikePredicate.class) ?
          Optional.empty() :
          PaimonJniReadFilter.tryRemapToPartitionRow(
              predicateRowType, partitionKeys, result.getPredicate());
      // Float != / NOT IN must not be data filters. File bounds omit NaN, so Paimon
      // would skip a file of the literal plus NaN. They are exact on partition keys.
      if (result.isPartitionOnly() && !remapped.isPresent()) {
        postScanExprs.add(expr);
        continue;
      }
      pushedPredicates.add(result.getPredicate());
      pushedExprs.add(expr);

      if (remapped.isPresent()) {
        partitionPredicates.add(remapped.get());
        partitionExprs.add(expr);
      } else {
        // Data filters and mixed partition/data filters are re-evaluated in Impala.
        postScanExprs.add(expr);
      }
    }
    PaimonJniReadFilter readFilters =
        PaimonJniReadFilter.fromPushedPredicates(table, pushedPredicates);
    return new ExtractedPredicates(pushedPredicates, pushedExprs, partitionPredicates,
        partitionExprs, postScanExprs, readFilters);
  }

  private static List<Expr> flattenConjuncts(List<Expr> exprs) {
    List<Expr> flat = Lists.newArrayList();
    for (Expr expr : exprs) {
      if (expr instanceof CompoundPredicate
          && ((CompoundPredicate) expr).getOp() == CompoundPredicate.Operator.AND) {
        flat.addAll(expr.getConjuncts());
      } else {
        flat.add(expr);
      }
    }
    return flat;
  }
}
