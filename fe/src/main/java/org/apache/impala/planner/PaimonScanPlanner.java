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

package org.apache.impala.planner;

import com.google.common.base.Preconditions;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.MultiAggregateInfo;
import org.apache.impala.analysis.TableRef;
import org.apache.impala.catalog.paimon.FePaimonTable;
import org.apache.impala.common.ImpalaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * ScanNode factory class for Paimon, currently, only Jni based is supported
 * Will add native scanNode later.
 */
public class PaimonScanPlanner {
  private static final Logger LOG = LoggerFactory.getLogger(PaimonScanPlanner.class);

  private Analyzer analyzer_;
  private PlannerContext ctx_;
  private TableRef tblRef_;
  private List<Expr> conjuncts_;
  private MultiAggregateInfo aggInfo_;

  private FePaimonTable table_;

  public PaimonScanPlanner(Analyzer analyzer, PlannerContext ctx, TableRef paimonTblRef,
      List<Expr> conjuncts, MultiAggregateInfo aggInfo) throws ImpalaException {
    Preconditions.checkState(paimonTblRef.getTable() instanceof FePaimonTable);
    analyzer_ = analyzer;
    ctx_ = ctx;
    tblRef_ = paimonTblRef;
    conjuncts_ = conjuncts;
    aggInfo_ = aggInfo;
    table_ = (FePaimonTable) paimonTblRef.getTable();
  }

  public PlanNode createPaimonScanPlan() throws ImpalaException {
    PaimonScanNode ret = new PaimonScanNode(
        ctx_.getNextNodeId(), tblRef_.getDesc(), conjuncts_, aggInfo_, table_);
    ret.init(analyzer_);
    return ret;
  }
}
