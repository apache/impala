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

import java.util.Collections;
import java.util.List;

import org.apache.impala.analysis.Expr;
import org.apache.impala.planner.RuntimeFilterGenerator.RuntimeFilter;
import org.apache.impala.thrift.TDataSink;
import org.apache.impala.thrift.TDataSinkType;
import org.apache.impala.thrift.TExecStats;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TQueryOptions;

import com.google.common.base.Preconditions;

/**
 * A DataSink describes the destination of a plan fragment's output rows.
 * The destination could be another plan fragment on a remote machine,
 * or a table into which the rows are to be inserted
 * (i.e., the destination of the last fragment of an INSERT statement).
 */
public abstract class DataSink {

  // Fragment that this DataSink belongs to. Set by the PlanFragment enclosing this sink.
  protected PlanFragment fragment_;

  // resource requirements and estimates for this plan node.
  // set in computeResourceProfile()
  protected ResourceProfile resourceProfile_ = ResourceProfile.invalid();

  // A total processing cost across all instances of this plan node.
  // Set in computeProcessingCost() for a meaningful value.
  protected ProcessingCost processingCost_ = ProcessingCost.invalid();

  /**
   * Return an explain string for the DataSink. Each line of the explain will be prefixed
   * by "prefix".
   */
  public final String getExplainString(String prefix, String detailPrefix,
      TQueryOptions queryOptions, TExplainLevel explainLevel) {
    StringBuilder output = new StringBuilder();
    appendSinkExplainString(prefix, detailPrefix, queryOptions, explainLevel, output);
    if (explainLevel.ordinal() >= TExplainLevel.EXTENDED.ordinal()) {
      output.append(detailPrefix);
      output.append(resourceProfile_.getExplainString());
      if (queryOptions.isCompute_processing_cost()) {
        // Show processing cost total.
        output.append(" cost=");
        if (processingCost_.isValid()) {
          output.append(processingCost_.getTotalCost());
          if (explainLevel.ordinal() >= TExplainLevel.VERBOSE.ordinal()) {
            output.append("\n");
            output.append(processingCost_.getExplainString(detailPrefix, false));
          }
        } else {
          output.append("<invalid>");
        }
      }
      output.append("\n");
    }
    return output.toString();
  }

  /**
   * Append the node-specific lines of the explain string to "output".
   */
  abstract protected void appendSinkExplainString(String prefix, String detailPrefix,
      TQueryOptions queryOptions, TExplainLevel explainLevel, StringBuilder output);

  /**
   * Return a short human-readable name to describe the sink in the exec summary.
   */
  abstract protected String getLabel();


  /**
   * Return runtime filters produced by this sink. Subclasses that use runtime filters
   * must override.
   */
  public List<RuntimeFilter> getRuntimeFilters() { return Collections.emptyList(); }

  /**
   * Construct a thrift representation of the sink.
   */
  protected final TDataSink toThrift() {
    TDataSink tsink = new TDataSink(getSinkType());
    tsink.setLabel(fragment_.getId() + ":" + getLabel());
    TExecStats estimatedStats = new TExecStats();
    estimatedStats.setMemory_used(resourceProfile_.getMemEstimateBytes());
    tsink.setEstimated_stats(estimatedStats);
    Preconditions.checkState(resourceProfile_.isValid());
    tsink.resource_profile = resourceProfile_.toThrift();
    toThriftImpl(tsink);
    return tsink;
  }

  /**
   * Add subclass-specific information to the sink.
   */
  abstract protected void toThriftImpl(TDataSink tsink);

  /**
   * Get the sink type of the subclass.
   */
  abstract protected TDataSinkType getSinkType();

  public void setFragment(PlanFragment fragment) { fragment_ = fragment; }
  public PlanFragment getFragment() { return fragment_; }
  public ResourceProfile getResourceProfile() { return resourceProfile_; }
  public ProcessingCost getProcessingCost() { return processingCost_; }

  public abstract void computeProcessingCost(TQueryOptions queryOptions);

  /**
   * Compute the resource profile for an instance of this DataSink.
   */
  public abstract void computeResourceProfile(TQueryOptions queryOptions);

  /**
   * Set number of rows consumed and produced data fields in processing cost.
   */
  public void computeRowConsumptionAndProductionToCost() {
    Preconditions.checkState(processingCost_.isValid(),
        "Processing cost of DataSink %s:%s is invalid! %s", fragment_.getId(), getLabel(),
        processingCost_);
    long inputOutputCardinality = Math.max(0, fragment_.getPlanRoot().getCardinality());
    processingCost_.setNumRowToConsume(inputOutputCardinality);
    processingCost_.setNumRowToProduce(inputOutputCardinality);
  }

  /**
   * Collect all expressions evaluated by this data sink.
   */
  public abstract void collectExprs(List<Expr> exprs);
}
