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

import java.util.List;
import org.apache.impala.analysis.Expr;
import org.apache.impala.thrift.TDataSink;
import org.apache.impala.thrift.TDataSinkType;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TQueryOptions;

/**
 * IcebergMergeSink is a specialized MultiDataSink that contains an insert sink for
 * writing data files, and a delete sink for writing delete files. The order of the sinks
 * is fixed, the first sink is always the insert sink, and the second is always the
 * delete sink. It also contains a 'mergeActionExpr_' that belongs to the incoming rows,
 * by evaluating this expression, it can be decided which sink will receive the row.
 */
public class IcebergMergeSink extends MultiDataSink {
  private final List<Expr> mergeActionExpr_;

  public IcebergMergeSink(
      TableSink insertSink, TableSink deleteSink, List<Expr> mergeActionExpr) {
    dataSinks_.add(insertSink);
    dataSinks_.add(deleteSink);
    this.mergeActionExpr_ = mergeActionExpr;
  }

  @Override
  protected String getLabel() { return "MERGE SINK"; }

  @Override
  protected void toThriftImpl(TDataSink tsink) {
    super.toThriftImpl(tsink);
    tsink.setOutput_exprs(Expr.treesToThrift(mergeActionExpr_));
  }

  @Override
  protected TDataSinkType getSinkType() { return TDataSinkType.MERGE_SINK; }
}
