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
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TDataSink;
import org.apache.impala.thrift.TDataSinkType;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TPlanRootSink;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.util.ExprUtil;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;


/**
 * Sink for the root of a query plan that produces result rows. Allows coordination
 * between the sender which produces those rows, and the consumer which sends them to the
 * client, despite both executing concurrently.
 */
public class PlanRootSink extends DataSink {
  private static final Logger LOG = Logger.getLogger(PlanRootSink.class);

  // The default estimated memory consumption is 10 mb. Only used if statistics are not
  // available. 10 mb should be sufficient to buffer results from most queries. See
  // IMPALA-4268 for details on how this value was chosen.
  private static final long DEFAULT_RESULT_SPOOLING_ESTIMATED_MEMORY = 10 * 1024 * 1024;

  // One expression per result column for the query.
  private final List<Expr> outputExprs_;

  public PlanRootSink(List<Expr> outputExprs) {
    Preconditions.checkState(outputExprs != null);
    outputExprs_ = outputExprs;
  }

  @Override
  public void appendSinkExplainString(String prefix, String detailPrefix,
      TQueryOptions queryOptions, TExplainLevel explainLevel, StringBuilder output) {
    output.append(String.format("%sPLAN-ROOT SINK\n", prefix));
    if (explainLevel.ordinal() >= TExplainLevel.EXTENDED.ordinal()) {
      output.append(detailPrefix + "output exprs: ")
          .append(Expr.getExplainString(outputExprs_, explainLevel) + "\n");
    }
  }

  @Override
  protected String getLabel() {
    return "ROOT";
  }

  @Override
  public void computeProcessingCost(TQueryOptions queryOptions) {
    if (queryOptions.isSpool_query_results() && queryOptions.getScratch_limit() != 0
        && !BackendConfig.INSTANCE.getScratchDirs().isEmpty()) {
      // The processing cost to buffer these many rows in root.
      long outputCardinality = Math.max(0, fragment_.getPlanRoot().getCardinality());
      processingCost_ = ProcessingCost.basicCost(
          getLabel(), outputCardinality, ExprUtil.computeExprsTotalCost(outputExprs_));
    } else {
      processingCost_ = ProcessingCost.zero();
    }
  }

  /**
   * Computes and sets the {@link ResourceProfile} for this PlanRootSink. If result
   * spooling is disabled, a ResourceProfile is returned with no reservation or buffer
   * sizes, and the estimated memory consumption is 0. Without result spooling, no rows
   * get buffered, and only a single RowBatch is passed to the client at a time. Given
   * that RowBatch memory is currently unreserved, no reservation is necessary.
   *
   * If SPOOL_QUERY_RESULTS is true, then the ResourceProfile sets a min/max resevation,
   * estimated memory consumption, max buffer size, and spillable buffer size. The
   * 'memEstimateBytes' (estimated memory consumption in bytes) is set by taking the
   * estimated number of input rows into the sink and multiplying it by the estimated
   * average row size. The estimated number of input rows is derived from the cardinality
   * of the associated fragment's root node. If the cardinality or the average row size
   * are not available, a default value is used. The minimum reservation is set as 2x of
   * the maximum between default spillable buffer size and MAX_ROW_SIZE (rounded up to
   * nearest power of 2) to account for the read and write pages in the
   * BufferedTupleStream used by the backend plan-root-sink. The maximum reservation is
   * set to the query-level config MAX_PINNED_RESULT_SPOOLING_MEMORY.
   *
   * If SPOOL_QUERY_RESULTS is true but spill is disabled either due to SCRATCH_LIMIT = 0
   * or SCRATCH_DIRS is empty, SPOOL_QUERY_RESULTS will be set to false. A ResourceProfile
   * is returned with no reservation or buffer sizes, and the estimated memory consumption
   * is 0.
   */
  @Override
  public void computeResourceProfile(TQueryOptions queryOptions) {
    if (queryOptions.isSpool_query_results()) {
      // Check if we need to disable result spooling because we can not spill.
      long scratchLimit = queryOptions.getScratch_limit();
      String scratchDirs = BackendConfig.INSTANCE.getScratchDirs();
      if (scratchLimit == 0 || scratchDirs.isEmpty()) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Result spooling is disabled due to unavailability of scratch "
              + "space.");
        }
        queryOptions.setSpool_query_results(false);
        resourceProfile_ = ResourceProfile.noReservation(0);
        return;
      }

      long maxSpoolingMem = queryOptions.getMax_result_spooling_mem();
      if (maxSpoolingMem <= 0) {
        // max_result_spooling_mem = 0 means unbounded. But instead of setting unlimited
        // memory reservation, we fallback to the default max_result_spooling_mem.
        TQueryOptions defaults = new TQueryOptions();
        maxSpoolingMem = defaults.getMax_result_spooling_mem();
        queryOptions.setMax_result_spooling_mem(maxSpoolingMem);
      }

      long bufferSize = queryOptions.getDefault_spillable_buffer_size();
      long maxRowBufferSize = PlanNode.computeMaxSpillableBufferSize(
          bufferSize, queryOptions.getMax_row_size());
      long minMemReservationBytes = 2 * maxRowBufferSize;
      long maxMemReservationBytes = Math.max(maxSpoolingMem, minMemReservationBytes);

      // User might set query option scratch_limit that is lower than either of
      // minMemReservationBytes, maxMemReservationBytes, or
      // max_spilled_result_spooling_mem. We define:
      //
      //   maxAllowedScratchLimit = scratchLimit - maxRowBufferSize
      //
      // If maxAllowedScratchLimit < minMemReservationBytes, we fall back to use
      // BlockingPlanRootSink in the backend by silently disabling result spooling.
      // If maxAllowedScratchLimit < maxMemReservationBytes, we silently lower
      // maxMemReservationBytes, max_result_spooling_mem, and
      // max_spilled_result_spooling_mem accordingly to fit maxAllowedScratchLimit.
      // Otherwise, do nothing.
      //
      // BufferedPlanRootSink may slightly exceed its maxMemReservationBytes when it
      // decides to spill. maxRowBufferSize bytes is subtracted in maxAllowedScratchLimit
      // to give extra space, ensuring spill does not exceed scratch_limit.
      if (scratchLimit > -1) {
        long maxAllowedScratchLimit = scratchLimit - maxRowBufferSize;
        if (maxAllowedScratchLimit < minMemReservationBytes) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("Result spooling is disabled due to low scratch_limit ("
                + scratchLimit + "). Try increasing scratch_limit to >= "
                + (minMemReservationBytes + maxRowBufferSize)
                + " to enable result spooling.");
          }
          queryOptions.setSpool_query_results(false);
          resourceProfile_ = ResourceProfile.noReservation(0);
          return;
        } else if (maxAllowedScratchLimit < maxMemReservationBytes) {
          maxMemReservationBytes = maxAllowedScratchLimit;
          queryOptions.setMax_result_spooling_mem(maxAllowedScratchLimit);
          if (LOG.isTraceEnabled()) {
            LOG.trace("max_result_spooling_mem is lowered to " + maxMemReservationBytes
                + " to fit scratch_limit (" + scratchLimit + ").");
          }
        }

        // If we got here, it means we can use BufferedPlanRootSink with at least
        // minMemReservationBytes in memory. But the amount of memory we can spill to disk
        // may still be limited by scratch_limit. Thus, we need to lower
        // max_spilled_result_spooling_mem as necessary.
        if (maxAllowedScratchLimit < queryOptions.getMax_spilled_result_spooling_mem()) {
          queryOptions.setMax_spilled_result_spooling_mem(maxAllowedScratchLimit);
          if (LOG.isTraceEnabled()) {
            LOG.trace("max_spilled_result_spooling_mem is lowered to "
                + maxAllowedScratchLimit + " to fit scratch_limit ("
                + scratchLimit + ").");
          }
        }
      }

      PlanNode inputNode = fragment_.getPlanRoot();

      long memEstimateBytes;
      if (inputNode.getCardinality() == -1 || inputNode.getAvgRowSize() == -1) {
        memEstimateBytes = DEFAULT_RESULT_SPOOLING_ESTIMATED_MEMORY;
      } else {
        long inputCardinality = Math.max(1L, inputNode.getCardinality());
        memEstimateBytes = (long) Math.ceil(inputCardinality * inputNode.getAvgRowSize());
      }
      memEstimateBytes = Math.min(memEstimateBytes, maxMemReservationBytes);

      resourceProfile_ = new ResourceProfileBuilder()
                             .setMemEstimateBytes(memEstimateBytes)
                             .setMinMemReservationBytes(minMemReservationBytes)
                             .setMaxMemReservationBytes(maxMemReservationBytes)
                             .setMaxRowBufferBytes(maxRowBufferSize)
                             .setSpillableBufferBytes(bufferSize)
                             .build();
    } else {
      resourceProfile_ = ResourceProfile.noReservation(0);
    }
  }

  @Override
  protected void toThriftImpl(TDataSink tsink) {
    TPlanRootSink tPlanRootSink = new TPlanRootSink(resourceProfile_.toThrift());
    tsink.setPlan_root_sink(tPlanRootSink);
    tsink.output_exprs = Expr.treesToThrift(outputExprs_);
  }

  @Override
  protected TDataSinkType getSinkType() {
    return TDataSinkType.PLAN_ROOT_SINK;
  }

  @Override
  public void collectExprs(List<Expr> exprs) {
    exprs.addAll(outputExprs_);
  }

  @Override
  public void computeRowConsumptionAndProductionToCost() {
    super.computeRowConsumptionAndProductionToCost();
    fragment_.setFixedInstanceCount(fragment_.getNumInstances());
  }
}
