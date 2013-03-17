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

package com.cloudera.impala.planner;

import java.util.List;

import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.SlotDescriptor;
import com.cloudera.impala.analysis.SlotRef;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.service.FeSupport;
import com.cloudera.impala.thrift.TExpr;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Representation of a collection of predicates on a single column that can be evaluated
 * at plan generation time against literal values.
 */
public class SingleColumnFilter {
  private final static Logger LOG = LoggerFactory.getLogger(SingleColumnFilter.class);

  // descriptor of referenced column
  private final SlotDescriptor slotDesc;

  // all conjuncts are fully bound by slotId
  private final List<Expr> conjuncts = Lists.newArrayList();

  public SingleColumnFilter(SlotDescriptor slotDesc) {
    this.slotDesc = slotDesc;
  }

  public void addConjunct(Expr conjunct) {
    conjuncts.add(conjunct);
  }

  /**
   * Determines whether for a given constant expr the filter predicates evaluate to
   * true.
   * Does this by substituting valueExpr for SlotRefs of the slotId in all conjuncts,
   * and then calls the backend for evaluation.
   */
  public boolean isTrue(Analyzer analyzer, Expr valueExpr) throws InternalException {
    Preconditions.checkState(valueExpr.isConstant());

    // construct smap
    Expr.SubstitutionMap sMap = new Expr.SubstitutionMap();
    sMap.lhs.add(new SlotRef(slotDesc));
    sMap.rhs.add(valueExpr);

    for (Expr conjunct: conjuncts) {
      Expr literalConjunct = conjunct.clone(sMap);
      Preconditions.checkState(literalConjunct.isConstant());
      // analyze to insert casts, etc.
      try {
        literalConjunct.analyze(analyzer);
      } catch (AnalysisException e) {
        // this should never happen
        throw new InternalException(
            "couldn't analyze predicate " + literalConjunct.toSql(), e);
      }

      // call backend
      TExpr thriftExpr = literalConjunct.treeToThrift();
      TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
      try {
        if (!FeSupport.EvalPredicate(serializer.serialize(thriftExpr))) {
          return false;
        }
      } catch (TException e) {
        // this should never happen
        throw new InternalException(
            "couldn't execute predicate " + literalConjunct.toSql(), e);
      }
    }

    return true;
  }
}
