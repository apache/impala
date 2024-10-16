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

package org.apache.impala.calcite.util;

import com.google.common.collect.Lists;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.SlotDescriptor;
import org.apache.impala.analysis.SlotId;
import org.apache.impala.analysis.StmtMetadataLoader;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.analysis.TupleId;
import org.apache.impala.authorization.AuthorizationFactory;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TQueryCtx;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Impala relies on the Analyzer for various semantic analysis of expressions
 * and plan nodes. Since Hive has already done most of this analysis we want
 * a basic analyzer that allows for analyzing/validating the final physical plan nodes,
 * slots and expressions. This BasicAnalyzer extends the Analyzer and overrides a few
 * methods.
 */
public class SimplifiedAnalyzer extends Analyzer {
  public static final int MAX_IDENTIFIER_LENGTH = 128;

  // List of temporary filter expressions while initializing an Impala plan node.
  private List<Expr> unassignedConjuncts_ = Lists.newArrayList();

  private Set<String> uniqueTableAlias_ = new HashSet<>();

  public SimplifiedAnalyzer(StmtMetadataLoader.StmtTableCache stmtTableCache,
      TQueryCtx queryCtx, AuthorizationFactory authzFactory,
      List<TNetworkAddress> hostLocations) {
    super(stmtTableCache, queryCtx, authzFactory, null);
  }

  /**
   * No need to worry about bound predicates because Calcite takes
   * care of this.
   */
  @Override
  public List<Expr> getBoundPredicates(TupleId destTid, Set<SlotId> ignoreSlots,
      boolean markAssigned) {
    return Lists.newArrayList();
  }

  /**
   * Return unassigned conjuncts. Within the Calcite flow, these unassigned
   * conjuncts will always be in the Filter RelNode on top of the Aggregation
   * node. Before the "init" for Agg is called, the unassigned conjuncts will
   * be assigned all the Exprs in the Filter. After the init is called, the
   * unassigned conjuncts should be set back to an empty list.
   */
  @Override
  public List<Expr> getUnassignedConjuncts(
      List<TupleId> tupleIds, boolean inclOjConjuncts) {
    return unassignedConjuncts_;
  }

  /**
   * See comment for getUnassignedConjuncts.
   */
  public void setUnassignedConjuncts(List<Expr> unassignedConjuncts) {
    this.unassignedConjuncts_ = unassignedConjuncts;
  }

  /**
   * See comment for getUnassignedConjuncts.
   */
  public void clearUnassignedConjuncts() {
    this.unassignedConjuncts_ = Lists.newArrayList();
  }

  /**
   * No need to worry about assigned conjuncts because Calcite takes
   * care of this.
   */
  @Override
  public void markConjunctsAssigned(List<Expr> conjuncts) {
  }

  /**
   * No need to worry about equivalent conjuncts because Calcite takes
   * care of this.
   */
  @Override
  public void createEquivConjuncts(List<TupleId> lhsTids,
      List<TupleId> rhsTids, List<BinaryPredicate> conjuncts) {
  }

  /**
   * No need to worry about equivalent conjuncts because Calcite takes
   * care of this.
   */
  @Override
  public <T extends Expr> void createEquivConjuncts(TupleId tid, List<T> conjuncts,
      Set<SlotId> ignoreSlots) {
  }

  /**
   * Calcite materializes all slot descriptors within every node, so we can
   * always set the field as materialized. This needs to be overridden because
   * this method gets called from inside an aggregation info class.
   */
  @Override
  public SlotDescriptor addSlotDescriptor(TupleDescriptor tupleDesc) {
    SlotDescriptor result = super.addSlotDescriptor(tupleDesc);
    result.setIsMaterialized(true);
    return result;
  }

  /**
   * Given an alias name, check if it is unique based on previously
   * cached names. If not, create a unique name by concatenating
   * it with an integer sequence counter.
   *
   * TODO: IMPALA-13460: Generating a unique alias is a problem because
   * the user provided alias in the query should be used in the explain
   * plan rather than this alias. There is a Calcite bug that has to be
   * fixed. We'd have to generate our own TableScan object underneath their
   * LogicalTableScan that would hold an alias. This TableScan can be
   * generated through their RelBuilder Factory object. But the current
   * code creates the LogicalTableScan directly rather than go through
   * a factory, so that would need to be fixed first.
   */
  public String getUniqueTableAlias(String alias) throws ImpalaException {
    final String base = alias;
    String newAlias = base;
    int i = 0;
    while (uniqueTableAlias_.contains(newAlias)) {
      newAlias = base + "_" + i++;
    }

    // final alias name should conform to the max identifer length
    if (newAlias.length() > MAX_IDENTIFIER_LENGTH) {
      newAlias = newAlias.substring(0, MAX_IDENTIFIER_LENGTH);
      if (uniqueTableAlias_.contains(newAlias)) {
        throw new AnalysisException ("Cannot create a unique identifier since it " +
            "exceeds maximum allowed length");
      }
    }

    uniqueTableAlias_.add(newAlias);
    return newAlias;
  }
}
