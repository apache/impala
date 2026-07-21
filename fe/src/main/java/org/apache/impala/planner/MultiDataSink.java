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

import com.google.common.base.Preconditions;

/**
 * MultiDataSink can aggregate multiple DataSinks that operate on the same source, e.g.
 * on the same SELECT statement of a modify statement. The backend operator will send
 * each row batch to each child data sink. Child data sinks might use different
 * expressions from the same source tuples. E.g. some data sink might write a different
 * subset of columns than other data sinks. One example is Iceberg UPDATEs, which
 * insert records to data files and delete files simultaneously.
 */
public class MultiDataSink extends DataSink implements HasQuantityLimit {
  protected final List<DataSink> dataSinks_;

  public MultiDataSink(DataSink... sinks) {
    dataSinks_ = List.of(sinks);
  }

  /**
   * Returns the capped number of instances by delegating to child sinks that implement
   * HasQuantityLimit. Returns the minimum across all such children, or the
   * plan root's instance count if none do.
   */
  @Override
  public int getNumInstances() {
    int numInstances = getFragment().getPlanRoot().getNumInstances();
    for (DataSink sink : dataSinks_) {
      if (sink instanceof HasQuantityLimit hasLimit) {
        numInstances = Math.min(numInstances, hasLimit.getNumInstances());
      }
    }
    return numInstances;
  }

  @Override
  public int getNumNodes() {
    int numNodes = getFragment().getPlanRoot().getNumNodes();
    for (DataSink sink : dataSinks_) {
      if (sink instanceof HasQuantityLimit hasLimit) {
        numNodes = Math.min(numNodes, hasLimit.getNumNodes());
      }
    }
    return numNodes;
  }

  @Override
  public void setFragment(PlanFragment fragment) {
    fragment_ = fragment;
    for (DataSink tsink : dataSinks_) {
      tsink.setFragment(fragment);
    }
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
    output.append(String.format("%s%s\n", prefix, getLabel()));
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
  protected String getLabelDetail() {
    // We expect the real table's sink to be the first element.
    // To maintain this order, a comment has been added above the statements
    // generating this list.
    Preconditions.checkElementIndex(0, dataSinks_.size());
    return dataSinks_.get(0).getLabelDetail();
  }

  @Override
  protected void toThriftImpl(TDataSink tdsink) {
    for (DataSink dsink : dataSinks_) {
      tdsink.addToChild_data_sinks(dsink.toThrift());
    }
  }

  @Override
  protected TDataSinkType getSinkType() {
    return TDataSinkType.MULTI_DATA_SINK;
  }

  @Override
  public void collectExprsForLineage(List<Expr> exprs) {
    for (DataSink dsink : dataSinks_) {
      dsink.collectExprsForLineage(exprs);
    }
  }

  @Override
  public void computeRowConsumptionAndProductionToCost() {
    super.computeRowConsumptionAndProductionToCost();
    fragment_.setFixedInstanceCount(fragment_.getNumInstances());
  }
}
