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

import java.util.ArrayList;
import java.util.List;

import org.apache.impala.analysis.Expr;

import org.apache.impala.thrift.TDataSink;
import org.apache.impala.thrift.TDataSinkType;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TQueryOptions;

/**
 * MultiDataSink can aggregate multiple DataSinks that operate on the same source, e.g.
 * on the same SELECT statement of a modify statement. The backend operator will send
 * each row batch to each child data sink. Child data sinks might use different
 * expressions from the same source tuples. E.g. some data sink might write a different
 * subset of columns than other data sinks. One example is Iceberg UPDATEs, which
 * insert records to data files and delete files simultaneously.
 */
public class MultiDataSink extends DataSink {
  private List<DataSink> dataSinks_ = new ArrayList<>();

  public MultiDataSink() {}

  /**
   * This must be called after all child sinks have been added.
   */
  @Override
  public void setFragment(PlanFragment fragment) {
    fragment_ = fragment;
    for (DataSink tsink : dataSinks_) {
      tsink.setFragment(fragment);
    }
  }

  public void addDataSink(DataSink tsink) {
    dataSinks_.add(tsink);
  }

  @Override
  public void computeProcessingCost(TQueryOptions queryOptions) {
    for (int i = 0; i < dataSinks_.size(); ++i) {
      DataSink dsink = dataSinks_.get(i);
      dsink.computeProcessingCost(queryOptions);
      ProcessingCost dsinkCost = dsink.getProcessingCost();
      if (i == 0) {
        processingCost_ = dsinkCost;
      } else {
        processingCost_ = ProcessingCost.sumCost(processingCost_, dsinkCost);
      }
    }
  }

  @Override
  public void computeResourceProfile(TQueryOptions queryOptions) {
    for (int i = 0; i < dataSinks_.size(); ++i) {
      DataSink dsink = dataSinks_.get(i);
      dsink.computeResourceProfile(queryOptions);
      ResourceProfile dsinkProfile = dsink.getResourceProfile();
      if (i == 0) {
        resourceProfile_ = dsinkProfile;
      } else {
        resourceProfile_.combine(dsinkProfile);
      }
    }
  }

  @Override
  public void appendSinkExplainString(String prefix, String detailPrefix,
      TQueryOptions queryOptions, TExplainLevel explainLevel, StringBuilder output) {
    output.append(String.format("%sMULTI DATA SINK\n", prefix));
    for (DataSink dsink : dataSinks_) {
      dsink.appendSinkExplainString(prefix + "|->", detailPrefix + "  ",
          queryOptions, explainLevel, output);
    }
  }

  @Override
  protected String getLabel() {
    return "MULTI DATA SINK";
  }

  @Override
  protected void toThriftImpl(TDataSink tdsink) {
    for (int i = 0; i < dataSinks_.size(); ++i) {
      DataSink dsink = dataSinks_.get(i);
      tdsink.addToChild_data_sinks(dsink.toThrift());
    }
  }

  @Override
  protected TDataSinkType getSinkType() {
    return TDataSinkType.MULTI_DATA_SINK;
  }

  @Override
  public void collectExprs(List<Expr> exprs) {
    for (int i = 0; i < dataSinks_.size(); ++i) {
      DataSink dsink = dataSinks_.get(i);
      dsink.collectExprs(exprs);
    }
  }

  @Override
  public void computeRowConsumptionAndProductionToCost() {
    super.computeRowConsumptionAndProductionToCost();
    fragment_.setFixedInstanceCount(fragment_.getNumInstances());
  }
}
